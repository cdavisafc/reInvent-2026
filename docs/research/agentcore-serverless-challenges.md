# Gotchas: agents on Temporal Serverless Workers + Amazon Bedrock AgentCore

What we ran into putting the `temporal-agent-harness` ReAct agent (`react_agent/`) on AgentCore
Runtime as a Temporal Serverless Worker, 2026-11-13 to 2026-11-15. Each item says what happens,
how sure we are, the workaround we use, and who would need to fix it.

**Status:**
- **Confirmed (source):** read in the code.
- **Observed:** we hit it in this deployment.
- **Unverified:** inferred, not yet tested.

**Sources read:**
- WCI, the Worker Controller: [temporalio/temporal-auto-scaled-workers](https://github.com/temporalio/temporal-auto-scaled-workers) @ `ce0c39a`.
- Server: [temporalio/temporal](https://github.com/temporalio/temporal) `main`, matching and history services.
- Harness: `temporal-agent-harness` 0.11.0, plus its bundled web UI.
- AgentCore SDK: `bedrock-agentcore` 1.9.x.

## Summary

| # | Gotcha | Status | Fix owner |
|---|---|---|---|
| 1 | Reads that need a Worker (queries) wake it, so looking at an agent costs a cold start | Observed / Confirmed (source) | — (by design; clients must account for it) |
| 2 | Every WCI invoke creates a new AgentCore session (microVM) | Confirmed (source) | WCI |
| 3 | WCI invokes on every unmatched task: bursts on scale-from-zero, spurious scale-outs | Confirmed (source); burst size unmeasured | WCI |
| 4 | WCI scaling settings can't be set for AgentCore | Confirmed (source) | WCI / CLI |
| 5 | Queries only trigger scale-from-zero, never scale-out | Confirmed (source) | Server / WCI |
| 6 | A query must outlast sticky wait + cold start (~30s cap) | Confirmed (source); timing unmeasured | Server / harness |
| 7 | MicroVMs outlive their Worker (default 15 min) | Observed | Us (config) |
| 8 | The debounce counts only activities; a return after idle costs a cold start + replay | Observed (design) | Us |
| 9 | The harness web UI wakes the Worker every 30s while a session is open | Observed | Harness |
| 10 | The harness web UI blocks new sessions when no Worker is polling (rare in practice) | Observed | Harness |
| 11 | The harness streams break with `openai>=3.9` | Observed | Harness |
| 12 | Harness large-payload offload defaults to local disk | Observed | Harness |
| 13 | AgentCore named endpoints are pinned to one runtime version | Observed | AgentCore / us |
| 14 | CodeZip needs OpenTelemetry; the deploy error hides it | Observed | AgentCore CLI |
| 15 | AgentCore Identity API keys can't work for Temporal-invoked runtimes | Confirmed (source) | AgentCore / WCI |
| 16 | Observability gaps: versioned pollers, identical identities, no session listing | Observed | Several |
| 17 | "Failure exceeds size limit" hides the real error | Observed | SDK |
| 18 | The Strands sample keeps the Temporal API key in plain text | Observed | Samples / us |

---

## How waking and scaling work

### 1. Reads that need a Worker (queries) wake it, so looking at an agent costs a cold start
**Observed / Confirmed (source).** Waking on new work is the point of a serverless Worker and
isn't a gotcha. The gotcha is that a **query**, which looks like a cheap read alongside
`describe`, is also new work for a Worker, and now starts compute.
- **Wakes the Worker:**
  - starting a session (Observed);
  - signals and Updates, since each creates a workflow task (expected, but not yet tested for
    Updates, e.g. answering `ask_user`);
  - **queries** (Observed, with `temporal workflow query` and the harness UI).
- **Never wakes it:** `describe`, `list`, history reads, `task-queue describe`.

Queries skip the backlog, so they used to never wake a scaled-to-zero Worker. Server fix
temporalio/temporal#10574 (`75fcd21`, OSS v1.32.0+, live on Temporal Cloud) fires the WCI hook
from `DispatchQueryTask` when no poller has polled in the last
`matching.workerControllerNoPollerHookWindow` (5s). The hook isn't specific to any provider, so
it applies to AgentCore too.

**Impact:** any UI or monitoring tool that *queries* an agent costs a cold start whenever no
Worker is running.

### 2. Every WCI invoke creates a new AgentCore session (microVM)
**Confirmed (source).** `compute_provider/aws_agentcore.go` calls `InvokeAgentRuntime` with no
`RuntimeSessionId`, so AgentCore starts a new microVM on every invoke. The payload is
`{"deploymentName", "buildId"}`, and WCI waits at most 5s for the call to return.

**Impact:**
- A drained-but-alive microVM never gets work again. Temporal doesn't know it exists, and the
  next invoke has a different session ID.
- Our `invoke` guard (`worker already polling`) can never trigger under WCI. It only applies to
  manual invokes that reuse a session ID.
- Returning to a conversation after the Worker drained means a new microVM plus a full history
  replay (see 8).

**Ask (WCI):** a stable session ID per Worker Deployment Version would make the next invoke land
in the warm microVM.

### 3. WCI invokes on every unmatched task
**Confirmed (source); burst size not yet measured.**

AgentCore and Lambda use the `no-sync` algorithm (`scaling_algorithm/no_sync_match.go`).
Scale-from-zero and scale-out are **the same path**:
- **Unmatched task adds:** every task add that wasn't handed straight to a waiting poll
  (`syncMatchNoPoller` or `syncMatchBacklogPresent`) triggers an invoke, subject only to
  `scale_up_cooloff_ms` (**default 100ms**). Matching's hook (`client/hook.go`) batches these
  events per WCI at most every **500ms**, so the real ceiling is about **2 invokes/s** while
  tasks go unmatched.
- **Metrics poll:** every 60s, it invokes when backlog > `scale_up_backlog_threshold` (default
  0). While any backlog remains, it also invokes every `max_worker_lifetime_ms` (default 10 min).

**Impact:**
- **Burst on cold start.** A turn adds many tasks: the session start, model and tool
  activities, the workflow tasks after them, streaming signals. During a 5–10s cold start, that
  can mean many invokes, each a microVM with its own Worker (see 2).
- **Spurious scale-out.** "No poller" means no poll waiting *at that moment*. A task that arrives
  while the one healthy Worker is between polls, or has every slot busy, still triggers a new
  microVM.
- **Scale-in is per Worker.** Each Worker's `ActivityTracker` is independent, and a Worker that
  gets no activity for `AGENTCORE_DEBOUNCE_SECONDS` drains and stops, however busy the others
  are. After a burst, Workers that don't win tasks scale back on their own. Its microVM ends
  `idleRuntimeSessionTimeout` later (see 7).
- **Activities are often local through eager execution.** It's on by default: Python SDK
  `disable_eager_activity_execution=False`, up to 3 reservations per workflow task; server
  `system.enableActivityEagerExecution=true`. When a workflow task schedules activities on the
  same task queue and the Worker has free slots, the server hands them straight back to that
  Worker, without going through matching. Workflow tasks are sticky, so a workflow's activities
  tend to run on the Worker that has it cached. Eager dispatch also never fires the WCI hook,
  which narrows the burst in this item. Activities that aren't dispatched eagerly (retries,
  scheduled beyond the reservation limit, no free slots, or a backlog present) go through the
  queue to any Worker. Not yet observed in our runs: identical Worker identities (see 16) hide
  which Worker ran what.
- **No consolidation.** Matching gives each queued task to whichever poll is waiting. So under steady load
  spread across more Workers than needed, each Worker can keep getting an activity often enough
  never to go idle. The result is several under-used Workers rather than fewer busy ones. This is
  a general property of idle-based scale-in with arbitrary dispatch, not specific to AgentCore.

**To measure:** for one cold-start question, count distinct `sessionId` values on
`polling react-agent as …` log lines. Also check `ActiveSession` (Maximum, 1-minute period).

### 4. WCI scaling settings can't be set for AgentCore
**Confirmed (source).** WCI accepts settings including `scale_up_cooloff_ms`,
`scale_up_backlog_threshold`, `max_worker_lifetime_ms` and `metrics_poll_interval_ms`. The
Temporal CLI only has scaling flags for Cloud Run, so AgentCore and Lambda versions run on the
defaults.

**Ask (WCI / CLI):** expose them. A cooloff longer than a cold start (e.g. 15s) would largely
remove the burst in 3.

### 5. Queries only trigger scale-from-zero, never scale-out
**Confirmed (source).**
- **The trigger:** the query path fires the hook only if no Worker has polled in the last 5s
  (`!HasPollerAfter(now-5s)`). A Worker that's alive but has every slot busy counts as present.
  A query that gets a Worker reports `syncMatchSuccess`.
- **Invisible to the metrics poll:** queries never enter the backlog, so the 60s backlog check
  never sees them.
- **Pinned to one Worker:** queries for a cached workflow go first to that Worker's sticky queue
  (`service/history/api/queryworkflow/api.go`). Per workflow, query capacity is effectively one
  Worker.

**Impact:** a flood of queries against one live Worker, e.g. many dashboard tabs, causes no
scale-up. The queries wait in matching and time out (see 6). They also take workflow-task
capacity away from real work.

### 6. A query must outlast sticky wait + cold start (~30s cap)
**Confirmed (source); timing not yet measured.**
- **No client deadline:** the Python SDK's `query()` defaults to `rpc_timeout=None`, and the
  harness passes none.
- **The 30s cap:** the frontend's internal history client wraps the call in
  `DefaultTimeout = 30s` (`client/history/client.go`). When it expires, the caller gets
  `DeadlineExceeded: "query timed out before a worker could process it"`.
- **How the 30s is spent:**
  1. If the workflow was sticky, history first tries the sticky queue for the Worker's sticky
     schedule-to-start timeout.
  2. If that fails, it clears stickiness and sends the query to the normal queue. Only there
     does the no-poller hook fire.

**Impact:** if sticky wait + AgentCore cold start + Worker start (secrets, connect) exceeds
about 30s, the first query fails even though it woke the Worker. This is probably the
"UI hangs on an idle session" we saw early on.

**Fix ideas:** an explicit `rpc_timeout` or one retry in clients of serverless agents.

### 7. MicroVMs outlive their Worker
**Observed.** An AgentCore session stays alive for `idleRuntimeSessionTimeout` *after* the
Worker drains: 900s by default. Combined with 2, a drained microVM is never reused, so that
time is pure waste. Together with 9, this produced `ActiveSession` ≈ 28 for one agent and no
users.

**Workaround (us):** `idleRuntimeSessionTimeout: 60`, the minimum, set in
`react_agent/agentcore/agentcore.json`. AgentCore counts the session as busy while the Worker
polls and drains, so this only shortens the time after the Worker has finished.

**Further option:** the Worker could end its own session after draining. That needs
`stop_runtime_session` with its own session ID (from `RequestContext.session_id`), the runtime
ARN and the qualifier, plus IAM permission. Untested.

### 8. The debounce counts only activities
**Observed (by design).** Our `ActivityTracker` drains the Worker after
`AGENTCORE_DEBOUNCE_SECONDS` (60) with no activity. It implements only `intercept_activity`, so
only activity start and finish reset the timer. In practice almost every workflow task in this
agent schedules an activity right away (a message or `ask_user` answer → model call), and that
activity keeps the Worker alive. What doesn't count is work that never leads to an activity:
**queries**, and workflow tasks that schedule nothing, such as stream-poll Updates or a `close`
signal.

**Impact:**
- Status queries every 30s (see 9) still lead to a cold start roughly every 60–90s.
- A user who returns after more than 60s idle pays a cold start **and a full history replay**
  on the new Worker (no cache survives).

**Also missed today:** a **timer firing** (its workflow task schedules nothing unless the code
then calls an activity) and **Nexus operations**. The Worker polls three task queue types
(workflow, activity, Nexus), and the tracker sees only one.

**Proposed idle definition (not implemented):** reset on any workflow activation (task, timer,
Update, signal, query), activity task or Nexus task, and hold the drain while activities or
Nexus handlers are in flight. Rename the class `WorkTracker`.

**How to catch each:**
- Activities: `intercept_activity` (today).
- Nexus: `intercept_nexus_operation` on the same worker interceptor.
- Workflow activations: a workflow inbound interceptor's `execute_workflow` runs **once per run**,
  so it misses timers and activity results. Wrapping the workflow runner sees every activation.
  The SDK's poll metrics are another option.
- Queries: `handle_query` on a workflow inbound interceptor. Workflow interceptors run in the
  sandbox, so they can't reach a module-level tracker directly; that needs a passthrough module
  or the runner wrapper.

**Per-work keep-alive (option):** replace the single "changed" event with a "keep alive until"
deadline that each kind of work pushes out by its own amount. For example, 60s after an
activity, Nexus task or workflow activation, but only 10–15s after a query, which is enough for
the UI's next poll. This is our code, so it can vary per work type. AgentCore's
`idleRuntimeSessionTimeout` can't: it is one value per runtime version, and applies after the
Worker drains.

**Options:**
- Raise the debounce to cover typical pauses (e.g. 5 min for a live demo).
- Count queries as activity with a workflow interceptor on `handle_query`, so one Worker stays
  warm while anyone is watching.
- The full idle definition above, with per-work keep-alives.

---

## temporal-agent-harness

### 9. The web UI wakes the Worker every 30s while a session is open
**Observed.** While a session is open in a *visible* tab and idle, the UI calls
`GET /api/status/{session_id}` every `idlePollMs = 30000` (UI bundle, `#Kt`). It pauses while
the tab is hidden. The server answers each call with `AgentClient.get_status()`, a workflow
query (`AGENT_STATUS_QUERY`), so each one can wake the Worker (see 1).

**What we saw:** `TasksAddRate ≈ 0.033/s` on the workflow queue with zero activity, and a new
session about every minute. Killing `temporal-agent-harness serve` stopped it.

**Workaround:** hide or close the tab, or use `client.py`.

**Fix ideas (harness):** don't status-poll an idle session, or poll far less often. Better,
serve status without a Worker, e.g. from search attributes or a memo.

### 10. The web UI blocks new sessions when no Worker is polling
**Observed.** The agent picker checks for pollers with `DescribeTaskQueue` and disables
"new session" when it finds none, which is the normal state for a scaled-to-zero Worker.
`POST /api/sessions` itself doesn't check, so `client.py` works.

**Rare with the current harness.** A poller counts as live if it polled within
`POLLER_FRESHNESS` (2 minutes, `web/task_queue_status.py`). While a session is visible, the UI's
30s status queries (see 9) keep the gap between Workers at about 30s + cold start, well under
that window. It shows up only after 2+ minutes with no Worker polling: the tab was hidden or
closed, a fresh page load, the laptop was asleep. It was seen once on 2026-11-15; closing and
reopening the new-session popup cleared it, because the popup checks again and a Worker had
come up by then. That also shows the UI's default `DescribeTaskQueue` call does see versioned
pollers, unlike the CLI's default mode (see 16).

**Fix idea (harness):** treat a queue backed by a Worker Deployment with a current serverless
version as ready.

### 11. Streams break with `openai>=3.9`
**Observed.** `openai_agents_harness._emit_tool_requested` evaluates `event.name` immediately in
`self._fn_calls.pop(item_id, (item_id, event.name))`. openai 3.9.0 removed `name` from
`ResponseFunctionCallArgumentsDoneEvent`, so every streamed tool call fails the model activity
with `AttributeError`. Still on upstream `main`.

**Workaround:** pin `openai==3.8.0`. `react_agent/tests/test_openai_pin.py` guards the pin.

**Fix (harness):** read the name only when the lookup misses (`getattr(event, "name", None)`).

### 12. Large-payload offload defaults to local disk
**Observed.** `AgentHarnessPlugin` defaults to `local_payload_storage()`. That storage is
ephemeral on AgentCore, and the locally run server can't read it.

**Workaround:** `large_payload_offload=None` on the Worker, so an oversized payload fails
loudly.

**To use S3:** custom launchers would be needed, because `serve` and `session-manager` have no
storage option.

---

## Amazon Bedrock AgentCore

### 13. Named endpoints are pinned to one runtime version
**Observed.** `endpoints.<name>.version` in `agentcore.json` is the version that endpoint
serves, and only `DEFAULT` follows new deploys. So a code change on a named endpoint needs a
version bump and a second deploy.

**Workaround (dev):** point the Worker Deployment Version at `<runtime-arn>/runtime-endpoint/DEFAULT`
with `temporal worker deployment update-version-compute-config`.

**Trade-off:** build ID `1.0.0` then means "whatever was deployed last", so PINNED sessions can
hit non-determinism errors. Releases should use a new build ID with a pinned endpoint.

**Gotcha within the gotcha:** logs are kept per endpoint. Use
`agentcore logs runtime --qualifier DEFAULT` or `--qualifier temporal` to match the endpoint
Temporal invokes.

### 14. CodeZip needs OpenTelemetry, and the deploy error hides it
**Observed.** The runtime starts the entry point via `opentelemetry-instrument`. Without
`aws-opentelemetry-distro`, runtime creation fails, and the CLI reports only
`NoStack: CloudFormationStack object does not hold a stack`. The real error is in the
CloudFormation stack events. The Strands sample only works because `strands-agents` pulls OTEL
in transitively.

A side effect: the distro traces boto3 calls but not Temporal. So AgentCore traces show each
Worker start as "invocation + Secrets Manager `GetSecretValue`".

### 15. AgentCore Identity API keys can't work for Temporal-invoked runtimes
**Confirmed (source, `bedrock-agentcore` SDK).** `requires_api_key` needs a workload access
token. A runtime invoked with AWS SigV4 only gets one when the caller sends
`X-Amzn-Bedrock-AgentCore-Runtime-User-Id`, and WCI doesn't send it.

**Workaround:** secrets come from Secrets Manager (`react_agent/runtime_secrets.py`).

---

## Visibility and tooling

### 16. Hard to see what's actually running
**Observed.**
- **Versioned pollers are hidden by default.** `temporal task-queue describe` shows only the
  unversioned queue. Add `--select-build-id react-agent-python:1.0.0` to see them.
- **Every Worker looks the same.** All AgentCore Workers report identity `1@localhost`, so
  pollers can't be told apart. Fix (us, not yet done): set the Worker `identity` from the
  AgentCore session ID.
- **No CLI for active runtime sessions.** `aws bedrock-agentcore list-sessions` is for
  AgentCore Memory. Use the CloudWatch `ActiveSession` metric with `Maximum` at a 1-minute
  period; `Sum` over a long period overcounts.
- **The poller list lags.** Pollers stay in `task-queue describe` for minutes after a Worker
  stops, so check `LastAccessTime`.

## Temporal SDK and samples

### 17. "Failure exceeds size limit" hides the real error
**Observed.** A tool's `ConnectTimeout`, with chained httpx/httpcore stack traces, exceeded the
failure size limit, so the UI showed only "Failure exceeds size limit". The real error was in a
nested cause.

### 18. The Strands AgentCore sample keeps `TEMPORAL_API_KEY` in plain text in `agentcore.json`
**Observed.** That file is checked in. The sample's README calls it sample-only, but copying the
sample makes it easy to commit a key. **Our `strands_agent/agentcore/agentcore.json` still has
one. Move it to Secrets Manager before the first commit.**

---

## Asks for other teams

- **Serverless Workers / WCI:**
  - stable AgentCore session ID per deployment version (2);
  - expose the `no-sync` scaling settings for AgentCore and Lambda (3, 4);
  - stop invoking while a scale-up is already in flight (3);
  - a scale signal for query load (5).
- **temporal-agent-harness:**
  - the `openai` 3.9 fix (11);
  - status without queries, or no polling of idle sessions (9);
  - serverless-aware readiness in the UI (10);
  - storage options in the CLI (12);
  - query timeouts and retries suited to cold starts (6).
- **AgentCore:** surface CloudFormation failure reasons in the CLI (14).

## Still to test

- Whether Updates wake a scaled-to-zero Worker: answer an `ask_user` after the Worker drains.
- Burst size on one cold start (3).
- Cold-start duration against the ~30s query budget (6).
- Whether eager activity execution actually happens on Temporal Cloud (3). All Workers share
  identity `1@localhost` (16), so history can't show which Worker ran an activity yet.
- The Worker ending its own session with `stop_runtime_session` after draining (7).

## Open review decisions

- Item 3: the reviewer argues over-provisioning is inherent to any autoscaler and scale-in
  corrects it. Proposed: drop 3 as a gotcha, move "burst size" to *Still to test*, and fold the
  100ms `scale_up_cooloff_ms` default into 4. Not yet decided.
- Item 1: kept "for now". The reviewer sees it as Temporal requiring a Worker for queries, not
  as a serverless issue.

## Not a platform issue (for the record)

- The Open-Meteo forecast server `51.81.167.44` accepted connections and then hung (2026-11-13),
  while another server behind the same hostname was healthy. Which one you got depended on DNS.
  Activity retries recovered the turn.
