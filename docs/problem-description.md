# Automations not firing after redeploy; SQL FK violations

## Problem Brief
Event-driven automations in Prefect intermittently stop firing after a deployment is updated. Server logs may show SQL foreign key violations and the condition typically resolves only after a server restart. This suggests stale references or incomplete cache/relationship updates between automations, triggers, and deployments, or cache refresh that races with transaction boundaries. The desired outcome is that automations remain reliable and deterministic across deployment updates on both SQLite and Postgres, without requiring a server restart, and the database always maintains referential integrity. Users should observe that triggers continue to execute actions (e.g., running a deployment) immediately after redeploys, with no consumer errors and no orphaned records.

## Agent Instructions
- Ensure automations continue to fire for event triggers after deployment updates without a server restart.
- Prevent SQL foreign key violations involving automations, related resources, triggers, and deployments; all writes must preserve referential integrity and be idempotent.
- Guarantee cache/registry coherence: any in-memory or notified caches used by the automations engine must only reflect committed state. Notifications and refreshes must not act on partially committed data.
- Make transaction boundaries explicit so readers never act on uncommitted changes. If notifications are used, ensure they are observed/processed only after commit.
- When deployments are updated, maintain correct linkages from automations to deployments; avoid stale identifiers and orphaned rows. Add safe, deterministic repair/cleanup if necessary.
- Preserve existing public APIs/schemas unless strictly required; prefer internal fixes to ordering, caching, or data integrity.
- Add deterministic tests that reproduce: create a deployment and automation → verify it fires → update the deployment → verify it still fires. Assert no FK violations and no consumer failures. Cover SQLite and Postgres where relevant.
- Keep error handling actionable and avoid timing-based flakiness.

## Test Assumptions (optional)
- No new public APIs are required. Behavior is validated through existing public surfaces; tests rely on current types and endpoints.
