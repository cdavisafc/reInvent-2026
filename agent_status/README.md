# Agent status page

A local page for every agent whose Temporal Workers run on Amazon Bedrock AgentCore Runtime. It
shows one collapsible section per agent, and in each section, for every running agent session,
whether a Worker is polling for it and whether its AgentCore session is alive.

AWS has no API that lists or describes runtime sessions, so each session reports on itself: every
agent project's runtime writes a session heartbeat to one shared DynamoDB table. The page joins
that table with Temporal's view of the agents' workflows and Workers.

## Files

| File | What it is |
|---|---|
| `status_page.py` | The page and its JSON API (FastAPI): fetches from Temporal and DynamoDB |
| `session_status.py` | The config file loader and the logic that joins the sources into sections and rows |
| `agents.toml` | The config: the heartbeat table and the agents to show |
| `session-heartbeats-table.yaml`, `bin/mk-heartbeat-table.sh` | The shared DynamoDB table every runtime writes its heartbeat to |

The heartbeat itself lives in each agent project, as a copy of `session_heartbeat.py`, so it ships
in that project's runtime zip:
[`react_agent_sessions`](../react_agent_sessions/) and [`order_support`](../order_support/).

## Setup

Create the shared table once, in the region your runtimes deploy to. It needs AWS credentials in
your shell (check with `aws sts get-caller-identity`):

```bash
AWS_REGION=us-west-2 ./bin/mk-heartbeat-table.sh
```

It creates the stack and table `agent-session-heartbeats`. To write to it, each runtime needs:
- `SESSION_HEARTBEAT_TABLE=agent-session-heartbeats` in its `envVars`;
- its project's `session-heartbeat-policy.json` under `additionalPolicies` in `agentcore.json`.

## Configure

`agents.toml` names the table and lists the agents, one section each, in order:

```toml
heartbeat_table = "agent-session-heartbeats"

[[agents]]
label = "Concierge"              # the section's title
task_queue = "spike-concierge"   # the queue the agent's Workers poll

[[agents]]
label = "ReAct Agent"
task_queue = "react-agent-sessions"
workflow_type = "ReactAgent"     # optional: only this workflow type on the queue
```

The page checks the file at startup. Every agent needs a `label` and a `task_queue`, and no task
queue may be listed twice.

## Run

Run it as one more local process, with the same `TEMPORAL_*` variables as the harness CLI (or a
`temporal.toml` profile), plus AWS credentials and `AWS_REGION`:

```bash
uv run python status_page.py                      # http://127.0.0.1:8100
uv run python status_page.py --config other.toml --port 8101
```

The page refreshes every 5 seconds. It remembers which sections you collapsed, per browser.

## What it shows

Each section's title shows the agent's label, its task queue, and how many sessions it has and how
many are polling. Its table has one row per running workflow on the queue:

| Column | Source |
|---|---|
| Agent Session | Running workflow IDs on the agent's task queue (Temporal visibility) |
| Agent Ready | Whether a Worker is polling: Temporal `ListWorkers`, from the SDK's worker heartbeats (every 10s) |
| AgentCore session ID | The identity on the workflow's latest workflow task: Workers connect as `agentcore:<session id>` |
| Session alive | The session heartbeat in DynamoDB (every 10s) |

The Agent Session and Agent Ready cells are green while a Worker is polling and red otherwise; the
AgentCore session ID and Session alive cells are green while the session is alive and red otherwise.

The two heartbeats have different lifetimes. The Temporal Worker heartbeat stops when the Worker
drains. The session heartbeat is a separate task in the runtime process. It keeps beating while
the drained microVM waits, and stops only when AgentCore ends the session. Expect a workflow to go:
1. from polling and alive;
2. to not polling but alive (`AGENTCORE_DEBOUNCE_SECONDS` after its last activity);
3. to not alive (`idleRuntimeSessionTimeout` later).

Live sessions no workflow points at, such as the bootstrap invoke from `create-version`, are
listed after the workflows. Each appears in the section for the task queue its heartbeat names.
Heartbeats for queues missing from the config aren't shown.

The session heartbeat never registers an AgentCore async task. If it did, `/ping` would report
`HealthyBusy`, and the session would stay alive, and billed, until `maxLifetime`.

If the server doesn't offer `ListWorkers`, the page says so, and the Agent Ready column falls back
to the session's own report.

## Heartbeat table schema

One item per AgentCore session, keyed by `session_id`:

| Attribute | Meaning |
|---|---|
| `session_id` | The AgentCore runtime session ID (the key) |
| `task_queue` | The queue the session's Worker serves: which section the session belongs to |
| `started_at`, `last_seen` | Epoch seconds: when the heartbeat started, and its latest beat |
| `worker_running`, `worker_identity` | Whether the session's Worker is running, as the session reports it |
| `expires_at` | `last_seen` plus one hour; DynamoDB's TTL deletes the item some time after |

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
```
