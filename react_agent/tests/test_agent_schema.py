# ABOUTME: Checks the ReactAgent workflow imports and exports a schema in this flat module layout,
# and that the checked-in ReactAgent.schema.json (read by the web UI) is not stale.

import json
import subprocess
import sys
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


def _export_schema() -> dict:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "temporal_agent_harness.web.cli",
            "schema",
            "workflow:ReactAgentWorkflow",
        ],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def test_schema_exposes_the_ask_handler() -> None:
    schema = _export_schema()

    assert schema["agent"] == "ReactAgent"
    assert "ask" in schema["handlers"]


def test_checked_in_schema_matches_the_workflow() -> None:
    checked_in = json.loads((PROJECT_DIR / "ReactAgent.schema.json").read_text())

    assert checked_in == _export_schema()
