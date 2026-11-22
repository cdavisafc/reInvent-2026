# Per-session status display for `react_agent_sessions`

Status: implemented (2026-11-19); awaiting first deploy and the `ListWorkers` check below.

## Goal

A live table with one row per agent-kit (harness) session:

- the agent workflow ID, and whether a Worker is polling for it;
- the AgentCore session ID, and whether that AgentCore session is alive.

The two halves are expected to differ. After the Worker drains (`AGENTCORE_DEBOUNCE_SECONDS`),
the microVM lives on until `idleRuntimeSessionTimeout`. We show that gap and don't close it:
the runtime does **not** call `StopRuntimeSession` on itself.

## The data problem

AWS has no API to get or list AgentCore runtime sessions. The `bedrock-agentcore` client offers
`InvokeAgentRuntime`, `InvokeAgentRuntimeCommand` and `StopRuntimeSession`; its `ListSessions` is
for AgentCore Memory. The CloudWatch `ActiveSession` metric is aggregate, published only when it is
non-zero, and lags 2–4 minutes.

## Decision: split sources

| Data | Source | Why |
|---|---|---|
| Agent sessions | Temporal visibility: running `ReactAgent` workflows on the agent's queue | Temporal owns the workflows |
| Worker polling | Temporal `ListWorkers`, fed by SDK worker heartbeats (`Runtime(worker_heartbeat_interval=10s)`) | Temporal's own view of who is polling |
| Workflow → AgentCore session | Identity on the latest `WorkflowTaskStarted` event (`GetWorkflowExecutionHistoryReverse`). Workers connect with identity `agentcore:<session id>` | Records which session actually ran the workflow |
| AgentCore session alive | Our session heartbeat, in DynamoDB | Nothing else knows |
| Worker running, as the session reports it | The same DynamoDB item | Cross-check, and the fallback when `ListWorkers` is unavailable |

DynamoDB holds only what nothing else knows. Reading Temporal live keeps routing failures
visible. For example, a workflow whose workflow task never triggered an invoke still appears,
because Temporal lists it, even though the runtime never saw it. A design where the runtime
reports everything about itself would hide exactly that case.

Rejected alternatives:

- **Everything self-reported to DynamoDB.** Simpler, but blind to workflows the runtime never sees.
- **Heartbeat as signals to a Temporal monitor workflow.** No AWS infrastructure, but it grows
  history, and it fails along with the Temporal paths being diagnosed.
- **CloudWatch metrics or EMF.** Too much lag for a live view.

## The two heartbeats

```
AgentCore session (microVM + our process)  |==================================| destroyed
  Temporal Worker                            |=========| drains
    SDK worker heartbeat -> Temporal         ^  ^  ^  ^             (column 1)
  Session heartbeat task (ours) -> DynamoDB  ^  ^  ^  ^  ^  ^  ^  ^  ^  ^  ^   (column 2)
```

- `session_heartbeat.SessionHeartbeat` is a process-level asyncio task, started on the first
  invoke that carries a session ID when `SESSION_HEARTBEAT_TABLE` is set. It writes
  `{session_id, started_at, last_seen, expires_at, worker_running, worker_identity}` every 10s,
  and also immediately when the Worker starts or stops. A failed write is logged, never raised.
  `expires_at` (last beat + 1 h) is the table's TTL attribute.
- **It never registers an AgentCore async task.** The SDK's `/ping` reports `HealthyBusy`
  exactly while an async task is registered (`bedrock_agentcore/runtime/app.py`,
  `get_current_ping_status`). A heartbeat that registered one would keep every session alive,
  and billed, until `maxLifetime` (8 h). `tests/test_agentcore_worker.py` checks that the ping is
  `Healthy` while the heartbeat runs and no Worker does.

Freshness rules (`session_status.FRESH_SECONDS = 30`, three missed beats):

- Polling means the Worker's latest `ListWorkers` status is `RUNNING` and its heartbeat is under 30s old.
- Alive means `last_seen` is under 30s old.

## Components

- `react_agent_sessions/session_heartbeat.py`: the heartbeat.
- `react_agent_sessions/agentcore_worker.py`: starts it, flags the Worker state, sets the Worker
  identity and the SDK worker-heartbeat interval.
- `react_agent_sessions/session-heartbeats-table.yaml`, `bin/mk-heartbeat-table.sh`: the table.
  `session-heartbeat-policy.json` grants `dynamodb:PutItem` and is listed in
  `agentcore.json` under `additionalPolicies`.
- `react_agent_sessions/session_status.py`: pure row building and freshness logic.
- `react_agent_sessions/status_page.py`: local FastAPI page on :8100.
  `GET /api/session-status` returns JSON (Grafana's Infinity datasource can read it later), and
  `GET /` refreshes every 5s.

## Open assumptions (check on first deploy)

1. **Confirmed (2026-11-19): the staging server answers `ListWorkers`,** and session-bound
   Workers report `task_queue = react-agent-sessions`, so the `TaskQueue` filter works. Before the
   identity change, every AgentCore Worker reported the SDK default `1@localhost` (PID 1 in the
   microVM), so Workers were indistinguishable. A draining Worker shows status `SHUTTING_DOWN`
   (2) next to its successor's `RUNNING` (1). If a server doesn't offer `ListWorkers`, the page
   falls back to the self-reported column and says so in its header.
2. **AgentCore neither pauses nor keeps alive idle sessions because of the heartbeat.** The check:
   the heartbeat should stop about `idleRuntimeSessionTimeout` (60s) after the Worker's
   "worker drained in …" log line.
   - A stop hours later would mean it keeps sessions alive.
   - A stop before the timeout would mean idle processes are paused.
3. **The reverse-history RPC is available,** and the latest `WorkflowTaskStarted` is within the
   last 500 events.
