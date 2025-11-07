#!/bin/bash
set -euo pipefail

case "${1:-}" in
  base)
    # Run a small subset of existing tests that should pass on the base commit
    uv run pytest -q tests/events/server/triggers/test_basics.py
    ;;
  new)
    # Run the newly added tests that are expected to fail on the base commit
    uv run pytest -q tests/events/server/actions/test_automation_redeploy_fk.py
    ;;
  *)
    echo "Usage: ./test.sh {base|new}" >&2
    exit 1
    ;;
esac
