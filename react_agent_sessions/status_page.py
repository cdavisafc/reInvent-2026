# ABOUTME: Local web page showing, per agent session, whether a Worker is polling and whether its
# AgentCore session is alive. Fetches from Temporal and the DynamoDB heartbeat table.

"""Run locally (not part of the runtime): ``uv run python status_page.py [--port 8100]``.

Connects to Temporal the same way the harness CLI does (TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE /
TEMPORAL_API_KEY, or a temporal.toml profile), and to DynamoDB with your AWS credentials and
AWS_REGION. ``GET /api/session-status`` returns the rows as JSON; ``GET /`` renders them and
refreshes every few seconds. session_status.py holds the logic that joins the sources.
"""

import argparse
import asyncio
import os
import time
from contextlib import asynccontextmanager
from dataclasses import asdict

import boto3
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.enums.v1 import EventType, WorkerStatus
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryReverseRequest,
    ListWorkersRequest,
)
from temporalio.client import Client
from temporalio.envconfig import ClientConfig
from temporalio.service import RPCError, RPCStatusCode

from session_status import WorkerSeen, build_rows, index_workers
from workflow import TASK_QUEUE

AGENT_TASK_QUEUE = os.environ.get("REACT_AGENT_TASK_QUEUE", TASK_QUEUE)
HEARTBEAT_TABLE = os.environ.get("SESSION_HEARTBEAT_TABLE", "react-agent-sessions-heartbeats")
# The newest WorkflowTaskStarted is near the end of history; give up after this many events.
HISTORY_EVENTS_TO_SCAN = 500


async def running_agent_workflows(client: Client) -> list[str]:
    query = (
        f"WorkflowType = 'ReactAgent' AND TaskQueue = '{AGENT_TASK_QUEUE}' "
        "AND ExecutionStatus = 'Running'"
    )
    return [wf.id async for wf in client.list_workflows(query)]


async def last_workflow_task_identity(client: Client, workflow_id: str) -> str | None:
    """The identity of the Worker that started this workflow's most recent workflow task."""
    request = GetWorkflowExecutionHistoryReverseRequest(
        namespace=client.namespace,
        execution=WorkflowExecution(workflow_id=workflow_id),
        maximum_page_size=100,
    )
    scanned = 0
    while scanned < HISTORY_EVENTS_TO_SCAN:
        response = await client.workflow_service.get_workflow_execution_history_reverse(request)
        for event in response.history.events:
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_STARTED:
                return event.workflow_task_started_event_attributes.identity
        scanned += len(response.history.events)
        if not response.next_page_token:
            return None
        request.next_page_token = response.next_page_token
    return None


async def agentcore_workers(client: Client) -> dict[str, WorkerSeen] | None:
    """Temporal's view of this queue's Workers, or None when the server doesn't offer ListWorkers."""
    request = ListWorkersRequest(
        namespace=client.namespace, query=f"TaskQueue = '{AGENT_TASK_QUEUE}'", page_size=200
    )
    seen = []
    try:
        while True:
            response = await client.workflow_service.list_workers(request)
            for info in response.workers_info:
                beat = info.worker_heartbeat
                seen.append(
                    WorkerSeen(
                        identity=beat.worker_identity,
                        status=WorkerStatus.Name(beat.status).removeprefix("WORKER_STATUS_"),
                        # Epoch seconds straight from the Timestamp: ToDatetime() is naive UTC,
                        # and .timestamp() on it would read it as local time.
                        heartbeat_time=beat.heartbeat_time.ToNanoseconds() / 1e9
                        if beat.HasField("heartbeat_time")
                        else 0.0,
                    )
                )
            if not response.next_page_token:
                break
            request.next_page_token = response.next_page_token
    except RPCError as err:
        if err.status in (RPCStatusCode.UNIMPLEMENTED, RPCStatusCode.PERMISSION_DENIED):
            return None
        raise
    return index_workers(seen)


def session_heartbeats(table) -> dict[str, dict]:
    items: list[dict] = []
    kwargs: dict = {}
    while True:
        page = table.scan(**kwargs)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return {item["session_id"]: item for item in items}


async def session_status(client: Client, table) -> dict:
    workflow_ids = await running_agent_workflows(client)
    identities, workers, heartbeats = await asyncio.gather(
        asyncio.gather(*(last_workflow_task_identity(client, wf) for wf in workflow_ids)),
        agentcore_workers(client),
        asyncio.to_thread(session_heartbeats, table),
    )
    rows = build_rows(
        workflow_ids, dict(zip(workflow_ids, identities)), workers, heartbeats, time.time()
    )
    return {
        "task_queue": AGENT_TASK_QUEUE,
        "list_workers_available": workers is not None,
        "rows": [asdict(row) for row in rows],
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = await Client.connect(**ClientConfig.load_client_connect_config())
    app.state.table = boto3.resource("dynamodb").Table(HEARTBEAT_TABLE)
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/api/session-status")
async def api_session_status() -> JSONResponse:
    content = await session_status(app.state.client, app.state.table)
    return JSONResponse(content, headers={"Cache-Control": "no-store"})


@app.get("/")
async def index() -> HTMLResponse:
    return HTMLResponse(PAGE)


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Sessions</title>
<style>
  :root { --bg: #ffffff; --fg: #1f2328; --muted: #656d76; --line: #d0d7de;
          --yes: #1a7f37; --no: #8c959f; --warn: #9a6700; }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #0d1117; --fg: #e6edf3; --muted: #8d96a0; --line: #30363d;
            --yes: #3fb950; --no: #6e7681; --warn: #d29922; }
  }
  body { background: var(--bg); color: var(--fg); margin: 0; padding: 16px;
         font: 14px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  .meta { color: var(--muted); margin-bottom: 12px; }
  .wrap { overflow-x: auto; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid var(--line);
           white-space: nowrap; }
  th { color: var(--muted); font-weight: 600; }
  code { font: 12px ui-monospace, SFMono-Regular, Menlo, monospace; }
  .yes { color: var(--yes); font-weight: 600; }
  .no { color: var(--no); }
  .age { color: var(--muted); font-size: 12px; margin-left: 6px; }
  .warn { color: var(--warn); }
  /* Light green and red in both themes; text inside stays dark so it reads on either. */
  :root { --ok-bg: #d8f5dd; --bad-bg: #fbdcdc; --cell-fg: #1f2328; --cell-muted: #57606a; }
  td.ok { background: var(--ok-bg); }
  td.bad { background: var(--bad-bg); }
  td.ok, td.bad { color: var(--cell-fg); }
  td.ok .yes { color: #116329; }
  td.bad .no { color: #a40e26; font-weight: 600; }
  td.ok .age, td.bad .age { color: var(--cell-muted); }
</style>
</head>
<body>
<h1>Agent sessions</h1>
<div class="meta" id="meta">Loading…</div>
<div class="wrap"><table>
  <thead><tr>
    <th>Agent Session</th><th>Agent Ready</th>
    <th>AgentCore session ID</th><th>Session alive</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table></div>
<script>
const age = s => s == null ? "" : `<span class="age">${Math.round(s)}s ago</span>`;
const flag = (on, seconds) => `<span class="${on ? "yes" : "no"}">${on ? "yes" : "no"}</span>${age(seconds)}`;
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);
const cell = v => v == null ? '<span class="no">—</span>' : `<code>${esc(v)}</code>`;
const state = on => on ? "ok" : "bad";

async function refresh() {
  const meta = document.getElementById("meta");
  try {
    const res = await fetch("api/session-status", {cache: "no-store"});
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const data = await res.json();
    const source = data.list_workers_available
      ? "Worker polling from Temporal ListWorkers"
      : '<span class="warn">ListWorkers unavailable: Worker polling is self-reported by the session</span>';
    meta.innerHTML = `Task queue <code>${esc(data.task_queue)}</code> · ${source} · updated ${new Date().toLocaleTimeString()}`;
    document.getElementById("rows").innerHTML = data.rows.map(r => `<tr>
      <td class="${state(r.worker_polling)}">${cell(r.workflow_id)}</td>
      <td class="${state(r.worker_polling)}">${flag(r.worker_polling, r.worker_heartbeat_age)}</td>
      <td class="${state(r.session_alive)}">${cell(r.session_id)}</td>
      <td class="${state(r.session_alive)}">${r.session_id == null ? cell(null) : flag(r.session_alive, r.session_last_seen_age)}</td>
    </tr>`).join("") || '<tr><td colspan="4" class="no">No running agent sessions.</td></tr>';
  } catch (err) {
    meta.innerHTML = `<span class="warn">Refresh failed: ${esc(err.message)}</span>`;
  }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
