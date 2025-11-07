import logging
from datetime import timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from prefect.server.events import actions, triggers
from prefect.server.events.models import automations
from prefect.server.events.schemas.automations import (
    Automation,
    EventTrigger,
    Firing,
    Posture,
    TriggerState,
    TriggeredAction,
)
from prefect.server.events.schemas.events import Event
from prefect.server.models import (
    deployments as deployments_model,
    flow_runs,
    flows,
    workers,
)
from prefect.server.schemas.actions import DeploymentUpdate, WorkPoolCreate
from prefect.server.schemas.core import Deployment, Flow
from prefect.server.utilities.messaging import Message
from prefect.settings import PREFECT_SERVER_DATABASE_CONNECTION_URL
from prefect.types._datetime import now


async def _create_deployment(session: AsyncSession) -> Deployment:
    """Helper: create a minimal deployment with a work pool/queue."""
    wp = await workers.create_work_pool(
        session=session,
        work_pool=WorkPoolCreate(
            name="wp-automations-fk",
            type="None",
            description="",
            base_job_template={},
        ),
    )

    test_flow = await flows.create_flow(session=session, flow=Flow(name="fk-flow"))
    await session.flush()

    dep = await deployments_model.create_deployment(
        session=session,
        deployment=Deployment(
            name="fk-deployment",
            flow_id=test_flow.id,
            paused=False,
            work_queue_id=wp.default_queue_id,
        ),
    )
    assert dep is not None
    await session.commit()
    return Deployment.model_validate(dep, from_attributes=True)


@pytest.mark.parametrize("expect_backend", ["sqlite", "postgres"])
async def test_automation_still_fires_after_deployment_update_without_fk_violations(
    session: AsyncSession, caplog: pytest.LogCaptureFixture, expect_backend: str
):
    # Detect current backend and skip when not matched to provide cross-backend coverage
    db_url = (PREFECT_SERVER_DATABASE_CONNECTION_URL.value() or "sqlite+aiosqlite:///")
    current_backend = (
        "postgres" if db_url.startswith("postgresql+asyncpg") else "sqlite"
    )
    if current_backend != expect_backend:
        pytest.skip(f"Running for backend={current_backend}; expecting {expect_backend}")

    """
    Regression: After a deployment update (redeploy), automations must still fire
    without FK errors or consumer failures, and relationships remain intact.
    """
    # 1) Create deployment and an automation that runs it on a simple event
    dep = await _create_deployment(session)

    auto = await automations.create_automation(
        session,
        Automation(
            name="fire-on-event",
            trigger=EventTrigger(
                expect={"animal.ingested"},
                posture=Posture.Reactive,
                threshold=0,
                within=timedelta(seconds=30),
            ),
            actions=[
                actions.RunDeployment(
                    deployment_id=dep.id,
                    parameters={"k": "v"},
                    job_variables={"mode": "test"},
                )
            ],
            enabled=True,
        ),
    )
    await session.commit()

    # Drive through consumer pipeline: publish event message via triggers.consumer
    e1 = Event(
        occurred=now("UTC"),
        event="animal.ingested",
        resource={"prefect.resource.id": "some.resource"},
        related=[
            {
                "prefect.resource.role": "meal",
                "genus": "Hemerocallis",
                "species": "fulva",
            }
        ],
        id=uuid4(),
    ).receive()
    async with triggers.consumer(periodic_granularity=timedelta(seconds=60)) as handle:
        msg1 = Message(
            data=e1.model_dump_json().encode(),
            attributes={"id": str(e1.id), "event": e1.event},
        )
        await handle(msg1)
    runs = await flow_runs.read_flow_runs(session)
    assert len(runs) == 1
    assert runs[0].deployment_id == dep.id

    # 2) Update the deployment (simulate redeploy) — keep the same id but change fields
    updated = await deployments_model.update_deployment(
        session=session,
        deployment_id=dep.id,
        deployment=DeploymentUpdate(name="fk-deployment-updated"),
    )
    assert updated is True
    await session.commit()

    # Relationships must remain: the automation should still be related to the same deployment resource id
    related = await automations.read_automations_related_to_resource(
        session=session,
        resource_id=f"prefect.deployment.{dep.id}",
    )
    assert any(a.id == auto.id for a in related)

    # 3) Fire again via consumer — schedule another run without FK/consumer errors or restarts
    e2 = Event(
        occurred=now("UTC"),
        event="animal.ingested",
        resource={"prefect.resource.id": "some.resource"},
        related=[
            {
                "prefect.resource.role": "meal",
                "genus": "Hemerocallis",
                "species": "fulva",
            }
        ],
        id=uuid4(),
    ).receive()
    with caplog.at_level(logging.ERROR):
        async with triggers.consumer(periodic_granularity=timedelta(seconds=60)) as handle:
            msg2 = Message(
                data=e2.model_dump_json().encode(),
                attributes={"id": str(e2.id), "event": e2.event},
            )
            await handle(msg2)
    runs = await flow_runs.read_flow_runs(session)
    assert len(runs) == 2
    assert all(r.deployment_id == dep.id for r in runs)

    # Explicitly assert no error logs (e.g., FK violations, consumer failures)
    error_messages = "\n".join(
        f"{rec.name}: {rec.levelname}: {rec.getMessage()}" for rec in caplog.records
        if rec.levelno >= logging.ERROR
    )
    assert "FOREIGN KEY" not in error_messages
    assert "IntegrityError" not in error_messages
    assert "ActionFailed" not in error_messages
    assert "consumer" not in error_messages.lower()

    # 4) Idempotency: sending the same message again should not create a new run
    with caplog.at_level(logging.ERROR):
        async with triggers.consumer(periodic_granularity=timedelta(seconds=60)) as handle:
            dupe = Message(
                data=e2.model_dump_json().encode(),
                attributes={"id": str(e2.id), "event": e2.event},
            )
            await handle(dupe)
    runs_after_dupe = await flow_runs.read_flow_runs(session)
    assert len(runs_after_dupe) == 2
