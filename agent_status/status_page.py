# ABOUTME: Local web page showing, for each configured agent, its sessions: whether a Worker is
# polling and whether its AgentCore session is alive. Fetches from Temporal and DynamoDB.

"""Run locally (not part of any runtime): ``uv run python status_page.py [--config agents.toml]``.

The config file (agents.toml) names the DynamoDB heartbeat table and lists the agents, each a task
queue and the label its section shows. Connects to Temporal the same way the harness CLI does
(TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE / TEMPORAL_API_KEY, or a temporal.toml profile), and to
DynamoDB with your AWS credentials and AWS_REGION. ``GET /api/session-status`` returns the
sections as JSON; ``GET /`` renders them and refreshes every few seconds. session_status.py holds
the logic that joins the sources.
"""

import argparse
import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path

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

from session_status import (
    AgentConfig,
    AgentSection,
    StatusConfig,
    WorkerSeen,
    build_sections,
    index_workers,
    load_config,
    running_workflows_query,
    session_state,
    short_id,
    worker_state,
    workers_query,
)

DEFAULT_CONFIG = Path(__file__).with_name("agents.toml")
# The newest WorkflowTaskStarted is near the end of history; give up after this many events.
HISTORY_EVENTS_TO_SCAN = 500


async def running_workflows(client: Client, agent: AgentConfig) -> list[str]:
    return [wf.id async for wf in client.list_workflows(running_workflows_query(agent))]


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


async def _list_workers(client: Client, query: str) -> list[WorkerSeen]:
    request = ListWorkersRequest(namespace=client.namespace, query=query, page_size=200)
    seen = []
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
            return seen
        request.next_page_token = response.next_page_token


async def agentcore_workers(
    client: Client, task_queues: list[str]
) -> dict[str, WorkerSeen] | None:
    """Temporal's view of these queues' Workers, or None when the server doesn't offer ListWorkers."""
    try:
        try:
            seen = await _list_workers(client, workers_query(task_queues))
        except RPCError as err:
            # A server that can't combine queues with OR still answers one queue at a time.
            if err.status != RPCStatusCode.INVALID_ARGUMENT:
                raise
            per_queue = await asyncio.gather(
                *(_list_workers(client, workers_query([queue])) for queue in task_queues)
            )
            seen = [worker for workers in per_queue for worker in workers]
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


async def session_status(client: Client, table, config: StatusConfig) -> dict:
    queues = [agent.task_queue for agent in config.agents]
    workflow_lists, workers, heartbeats = await asyncio.gather(
        asyncio.gather(*(running_workflows(client, agent) for agent in config.agents)),
        agentcore_workers(client, queues),
        asyncio.to_thread(session_heartbeats, table),
    )
    workflow_ids = [wf for workflows in workflow_lists for wf in workflows]
    identities = await asyncio.gather(
        *(last_workflow_task_identity(client, wf) for wf in workflow_ids)
    )
    sections = build_sections(
        config.agents,
        dict(zip(queues, workflow_lists)),
        dict(zip(workflow_ids, identities)),
        workers,
        heartbeats,
        time.time(),
    )
    return {
        "list_workers_available": workers is not None,
        "agents": [_section_json(section) for section in sections],
    }


def _section_json(section: AgentSection) -> dict:
    """A section for the page: each row with short forms of its IDs, and the states its icons show."""
    content = asdict(section)
    for row, session_row in zip(content["rows"], section.rows):
        for key in ("workflow_id", "session_id"):
            row[f"{key}_short"] = short_id(row[key]) if row[key] else None  # no session yet
        row["worker_state"] = worker_state(session_row)
        row["session_state"] = session_state(session_row)
    return content


def create_app(config: StatusConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.client = await Client.connect(**ClientConfig.load_client_connect_config())
        app.state.table = boto3.resource("dynamodb").Table(config.heartbeat_table)
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/api/session-status")
    async def api_session_status() -> JSONResponse:
        content = await session_status(app.state.client, app.state.table, config)
        return JSONResponse(content, headers={"Cache-Control": "no-store"})

    @app.get("/")
    async def index() -> HTMLResponse:
        return HTMLResponse(PAGE)

    return app


# Styled after the agent kit UI (temporal-agent-kit/typescript/apps/ui/src/app.css): dark only,
# square corners, hairline borders, uppercase mono labels, mono identifiers. The cells' green and
# red fills are this page's own, for status at a glance.
PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Agent Sessions</title>
<style>
  :root {
    color-scheme: dark;
    --font-mono: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
    --font-sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --surface-0: #0b0b10; --surface-1: #141414; --surface-2: #17171f;
    --text-1: #f8fafc; --text-2: #c7c9d4; --text-3: #8b8d9d; --text-4: #808294;
    --border: rgba(255, 255, 255, 0.1); --border-strong: rgba(255, 255, 255, 0.22);
    --accent: #6173f3; --success: #6ee7b7; --error: #fca5a5; --warning: #fcd34d;
    --label-size: 10px; --label-tracking: 0.13em; --figure-size: 11px;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 16px; background: var(--surface-0); color: var(--text-1);
         font: 13px/1.4 var(--font-sans); -webkit-font-smoothing: antialiased; }
  .label { font: 500 var(--label-size) var(--font-mono); letter-spacing: var(--label-tracking);
           text-transform: uppercase; }
  .figure { font: var(--figure-size) var(--font-mono); font-variant-numeric: tabular-nums; }
  header { display: flex; align-items: baseline; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
  h1 { margin: 0; font-size: 15px; font-weight: 600; }
  #meta { color: var(--text-3); }
  #meta .warn { color: var(--warning); }
  .agents { display: flex; flex-direction: column; gap: 12px; width: 560px; max-width: 100%; }
  details { background: var(--surface-1); border: 1px solid var(--border); }
  summary { display: flex; align-items: center; gap: 8px; padding: 8px 12px; cursor: pointer;
            list-style: none; user-select: none; }
  summary::-webkit-details-marker { display: none; }
  summary::before { content: ""; width: 0; height: 0; border-style: solid; border-width: 4px 0 4px 6px;
                    border-color: transparent transparent transparent var(--text-3);
                    transition: transform 160ms; }
  details[open] summary::before { transform: rotate(90deg); }
  details[open] summary { border-bottom: 1px solid var(--border); }
  summary:hover { background: var(--surface-2); }
  summary:focus-visible { outline: 1px solid var(--accent); outline-offset: -1px; }
  .name { font-size: 13px; font-weight: 600; }
  .queue { color: var(--text-3); }
  .chips { display: flex; gap: 6px; margin-left: auto; padding-left: 16px; }
  .chip { display: inline-flex; align-items: center; gap: 6px; height: 22px; padding: 0 8px;
          border: 1px solid var(--border); color: var(--text-2); white-space: nowrap; }
  /* Fixed layout and the same column widths in every section, so the sections line up. */
  table { border-collapse: collapse; width: 100%; table-layout: fixed; }
  col.agent-session { width: 64%; }
  col.agentcore-session { width: 36%; }
  th { padding: 6px 12px; text-align: left; color: var(--text-3); white-space: nowrap;
       border-bottom: 1px solid var(--border); }
  td { padding: 5px 12px; border-bottom: 1px solid var(--border); }
  tr:last-child td { border-bottom: none; }
  /* Light green and red fills; text inside stays dark so it reads on either. */
  td.ok { background: #d8f5dd; color: #116329; }
  td.bad { background: #fbdcdc; color: #a40e26; }
  /* Rules in the panel colour, so filled cells stay distinct from their neighbours. */
  td.ok, td.bad { border-bottom-color: var(--surface-1); }
  td + td { border-left: 1px solid var(--surface-1); }
  .cell { display: flex; align-items: center; gap: 8px; }
  .id { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
        color: #1f2328; }
  .none { color: inherit; }
  .icon { flex: none; width: 14px; height: 14px; }
  .empty { color: var(--text-4); }
  .pip { display: inline-block; width: 5px; height: 5px; }
  .chip.yes .pip { background: var(--success); }
</style>
</head>
<body>
<header>
  <h1>Agent sessions</h1>
  <span id="meta" class="figure">Loading…</span>
</header>
<div id="agents" class="agents"></div>
<script>
const esc = s => String(s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);
// Right-aligned state icons, in the cell's text colour: active (a bolt), standby (pause), ended (stop).
const ICONS = {
  active: '<path d="M8.5 1 3 9h4l-1 6 5.5-8h-4z" fill="currentColor"/>',
  standby: '<rect x="4" y="3" width="2.5" height="10" fill="currentColor"/><rect x="9.5" y="3" width="2.5" height="10" fill="currentColor"/>',
  ended: '<rect x="4" y="4" width="8" height="8" fill="currentColor"/>',
};
const icon = name => name == null ? ""
  : `<svg class="icon" viewBox="0 0 16 16" role="img" aria-label="${name}"><title>${name}</title>${ICONS[name]}</svg>`;
// Short form on the page; the full ID in the tooltip.
const cell = (full, short, iconName) => `<div class="cell">${full == null
  ? '<span class="id none figure">—</span>'
  : `<span class="id figure" title="${esc(full)}">${esc(short ?? full)}</span>`}${icon(iconName)}</div>`;
const fill = on => on ? "ok" : "bad";

// Which sections the viewer collapsed, by task queue, so a refresh doesn't reopen them.
const COLLAPSED_KEY = "agent-status:collapsed";
function loadCollapsed() {
  try { return new Set(JSON.parse(localStorage.getItem(COLLAPSED_KEY) || "[]")); }
  catch { return new Set(); }
}
const collapsed = loadCollapsed();
function saveCollapsed() {
  try { localStorage.setItem(COLLAPSED_KEY, JSON.stringify([...collapsed])); } catch {}
}

function rowsHtml(rows) {
  return rows.map(r => `<tr>
      <td class="${fill(r.worker_polling)}">${cell(r.workflow_id, r.workflow_id_short, r.worker_state)}</td>
      <td class="${fill(r.session_alive)}">${cell(r.session_id, r.session_id_short, r.session_state)}</td>
    </tr>`).join("") || '<tr><td colspan="2" class="empty">No running agent sessions.</td></tr>';
}

function sectionHtml(agent) {
  const sessions = agent.rows.length;
  const polling = agent.rows.filter(r => r.worker_polling).length;
  const open = collapsed.has(agent.task_queue) ? "" : " open";
  return `<details data-queue="${esc(agent.task_queue)}"${open}>
    <summary>
      <span class="name">${esc(agent.label)}</span>
      <span class="queue figure">${esc(agent.task_queue)}</span>
      <span class="chips">
        <span class="chip label">${sessions} session${sessions === 1 ? "" : "s"}</span>
        <span class="chip label ${polling ? "yes" : ""}">${polling ? '<span class="pip"></span>' : ""}${polling} polling</span>
      </span>
    </summary>
    <div class="wrap"><table>
      <colgroup><col class="agent-session"><col class="agentcore-session"></colgroup>
      <thead><tr>
        <th class="label">Agent session</th><th class="label">AgentCore session</th>
      </tr></thead>
      <tbody>${rowsHtml(agent.rows)}</tbody>
    </table></div>
  </details>`;
}

document.getElementById("agents").addEventListener("toggle", e => {
  const queue = e.target.dataset.queue;
  if (e.target.open) collapsed.delete(queue); else collapsed.add(queue);
  saveCollapsed();
}, true);

async function refresh() {
  const meta = document.getElementById("meta");
  try {
    const res = await fetch("api/session-status", {cache: "no-store"});
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const data = await res.json();
    const source = data.list_workers_available
      ? "polling from ListWorkers"
      : '<span class="warn">ListWorkers unavailable: polling is self-reported</span>';
    meta.innerHTML = `${source} · updated ${new Date().toLocaleTimeString()}`;
    document.getElementById("agents").innerHTML = data.agents.map(sectionHtml).join("");
  } catch (err) {
    meta.innerHTML = `<span class="warn">Refresh failed: ${esc(err.message)}</span>`;
  }
}
// Browsers slow a page's own timers when they decide it isn't being watched (a background tab,
// a covered window, energy saving): Chrome, to once a minute. A dedicated worker's timers aren't
// slowed that way, so the worker sets the pace and the page fetches on each tick.
const REFRESH_MS = 5000;
function startTicking() {
  try {
    const source = `setInterval(() => postMessage("tick"), ${REFRESH_MS});`;
    const ticker = new Worker(URL.createObjectURL(new Blob([source], {type: "text/javascript"})));
    ticker.onmessage = refresh;
  } catch {
    setInterval(refresh, REFRESH_MS);
  }
}
refresh();
startTicking();
// Catch up the moment the page is visible again, in case it was slowed anyway.
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") refresh();
});
</script>
</body>
</html>
"""


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args()
    uvicorn.run(create_app(load_config(args.config)), host=args.host, port=args.port)
