# temporal-agent-kit on serverless Workers: the gaps we hit

Every problem with temporal-agent-kit (shipped as `temporal-agent-harness`) that we ran into
while running its agents as Temporal Serverless Workers on Amazon Bedrock AgentCore Runtime. Each
entry is short: the details stay in the write-up it links to. Versions: temporal-agent-harness
0.11.0 to 0.11.1, checked against the kit source at 0.12.0 where noted.

## The theme

Most of these aren't bugs. The kit assumes a Worker that is always running, and a serverless
Worker breaks that assumption in two ways:
- **Holding an activity open is expensive.** A serverless Worker drains only once no activity is
  in flight. So an activity that waits holds a compute session, and its bill, for as long as it
  waits.
- **Anything that needs a Worker wakes one.** Each query or update goes to a Worker. With no
  Worker running, the server invokes one, with a cold start and a replay. So a UI that polls keeps
  Workers alive, and checks for "is a Worker polling?" read a scaled-to-zero agent as down.

The fixes are design changes in the kit. Those designs were reasonable for a long-lived Worker.

## Summary

| # | Gap | Kind | Our workaround | Upstream |
|---|---|---|---|---|
| 1 | A subagent turn holds the parent's Worker while the child waits | Always-on assumption | `order_support/subagent_signals.py` | Issue drafted, not filed |
| 2 | The UI wakes the Worker while a session is open | Always-on assumption | Close the session's tab | Not raised |
| 3 | The UI blocks new sessions when no Worker is polling | Always-on assumption | Reopen the new-session dialog | Not raised |
| 4 | Status queries must outlast a cold start | Always-on assumption | None | Not raised |
| 5 | Large payloads go to the Worker's local disk by default | Always-on assumption | `large_payload_offload=None` | Not raised |
| 6 | `subagent_toolset` tools fail from an OpenAI Agents SDK parent | Bug | `order_support/keyword_tools.py` | Not raised |
| 7 | Streams break with `openai>=3.9` | Bug | Pin `openai==3.8.0` | Not raised |
| 8 | The Strands plugin doesn't pass `opentelemetry` through the sandbox | Bug (shows on AgentCore) | `hosted_agents.workflow_runner()` | Not raised |
| 9 | A handler's failure reaches the UI as `str(e)` only | Rough edge | Log the traceback in the handler | Not raised |
| 10 | `serve` ignores changed registries while `session-manager` runs | Rough edge | Terminate `session-manager` | Not raised |
| 11 | The UI timeline drops a reused subagent's actions on later turns | Bug (UI) | Reload the page | Not raised |

"Not raised" means there's no issue on temporal-agent-kit yet. Filing one is public, so each
waits for a go-ahead.

## Assumes an always-on Worker

### 1. A subagent turn holds the parent's Worker while the child waits

The parent drives each subagent turn with the `run_subagent_turn` activity, which sends the
message and then waits, heartbeating, until the child's `turn_end`. When the child waits on a
person (a tool approval), the child's Worker drains but the parent's can't, for up to the
activity's 1-hour timeout.

- **Details:** `order-support-subagent-spike.md`, "Finding: a subagent turn keeps the parent's
  Worker busy while the child waits".
- **Workaround:** `SubagentSignalRunner` in `order_support/subagent_signals.py`. A short activity
  sends the message, the parent waits in its workflow, and the child signals the reply. It
  overrides private harness hooks, so it's pinned to harness 0.11.1. Verified in local tests; not
  yet verified on AgentCore.
- **Upstream:** the proposal is drafted as
  [`agent-kit-issue-subagent-turn-holds-parent-worker.md`](agent-kit-issue-subagent-turn-holds-parent-worker.md).
  Not filed.

### 2. The UI wakes the Worker while a session is open

While a session is open in a visible tab, the UI polls its status with a workflow query every
30s, and reads its event stream, which the workflow answers. Each one needs a Worker, so an open
idle session keeps re-invoking or holding one. Browsers count a tab as visible when it's the front
tab of a hidden window (seen with Island).

- **Details:** `agentcore-serverless-challenges.md`, item 9. The root `README.md` says what keeps
  a Worker running.
- **Workaround:** close the session's tab.
- **Fix idea:** don't poll idle sessions, or serve status without a Worker (search attributes,
  memo).

### 3. The UI blocks new sessions when no Worker is polling

The new-session dialog disables an agent whose queue has had no poller for 2 minutes. For a
scaled-to-zero agent that is normal, not an outage. `POST /api/sessions` doesn't check, so the
API works.

- **Details:** `agentcore-serverless-challenges.md`, item 10.
- **Workaround:** close and reopen the dialog once a Worker is up. Rare while another session is
  open, because of 2.
- **Fix idea:** treat a queue backed by a Worker Deployment with a current serverless version as
  ready.

### 4. Status queries must outlast a cold start

A query to a scaled-to-zero agent waits for an invoke, a cold start and a replay, within a budget
of about 30s.

- **Details:** `agentcore-serverless-challenges.md`, item 6 (shared with the server). Timing not
  yet measured.
- **Fix idea:** query timeouts and retries that allow for a cold start.

### 5. Large payloads go to the Worker's local disk by default

`AgentHarnessPlugin` defaults to `local_payload_storage()`. On AgentCore that disk is ephemeral,
and the locally run `serve` can't read it.

- **Details:** `agentcore-serverless-challenges.md`, item 12.
- **Workaround:** `large_payload_offload=None`, so an oversized payload fails loudly.
- **Fix idea:** storage options (S3) for `serve` and `session-manager`, which have none.

## Bugs

### 6. `subagent_toolset` tools fail from an OpenAI Agents SDK parent

Every generated `<key>_<handler>` tool fails with "takes 1 positional argument but 2 were given":
the tool's signature has two parameters, its body takes one plus `**kwargs`, and the OpenAI
Agents SDK adapter passes arguments positionally. Unchanged in the kit at 0.12.0.

- **Details:** `order-support-subagent-spike.md`, "Finding: harness bug in `subagent_toolset`".
- **Workaround:** `order_support/keyword_tools.py` makes the parameters keyword-only.

### 7. Streams break with `openai>=3.9`

`_emit_tool_requested` reads `event.name`, which openai 3.9.0 removed from
`ResponseFunctionCallArgumentsDoneEvent`. Every streamed tool call fails.

- **Details:** `agentcore-serverless-challenges.md`, item 11.
- **Workaround:** pin `openai==3.8.0`, guarded by `react_agent/tests/test_openai_pin.py`.

### 8. The Strands plugin doesn't pass `opentelemetry` through the sandbox

Strands starts OpenTelemetry spans inside the workflow. On AgentCore, a span processor then
imports `opentelemetry` modules in the sandbox, and one reads `os.environ`, which fails the turn.
Locally there's no such processor, so it only shows on AgentCore.

- **Details:** `order-support-subagent-spike.md`, "Finding: Strands on AgentCore needs
  `opentelemetry` passed through the sandbox".
- **Workaround:** every Worker uses `order_support/hosted_agents.py`'s `workflow_runner()`, a sandbox that passes
  `opentelemetry` through.

## Rough edges

### 9. A handler's failure reaches the UI as `str(e)` only

The harness catches a handler's exception and publishes only `str(e)` as
`message_handler_error`. The workflow history shows no failure. That's what made 8 hard to
find.

- **Details:** `order-support-subagent-spike.md`, the symptom of the `opentelemetry` finding.
- **Workaround:** `order_support/specialist.py` logs the traceback before re-raising.

### 10. `serve` ignores changed registries while `session-manager` runs

`serve` keeps the agent list in the running `session-manager` workflow. Renamed or added agents
don't appear until that workflow is terminated, which ends every running session.

- **Details:** root `README.md`, "Running the demo with every agent".
- **Workaround:** `temporal workflow terminate --workflow-id session-manager`, then restart `serve`.

### 11. The UI timeline drops a reused subagent's actions on later turns

Seen 2026-11-24 in the order-support demo. The Concierge starts Shipping on the first prompt and
reuses it on a later prompt. On that later prompt, the UI's log shows the Shipping subagent, but
the timeline doesn't show any of its actions (tool calls, approvals).

- **The server side is fine.** `order_support/tests/test_subagent_reuse_e2e.py` reads the merged
  stream (`AgentClient.attach`, what `serve` gives the UI). The child's second-turn events are
  there both when resuming from the offset after turn 1, as the UI does after sending a prompt,
  and when replaying from 0. So it isn't `subagent_signals.py` (gap 1) or the stream merge.
- **Likely cause, not confirmed:** `apps/ui/src/lib/state/replayTimeline.ts` nests a child's
  events under a parent turn only for a subagent it has "observed", and a subagent becomes
  observed from its `subagent_started`. That is published once, on the parent's stream, in the
  turn that started the child. A stream resumed for a later turn doesn't carry it, so the
  child's later events may have no subagent to nest under. The log lists events without nesting,
  so it still shows them.
- **Untried check:** reloading the page replays from 0, which includes `subagent_started`. If the
  actions then appear, that confirms the cause.
- **Workaround:** reload the page. Starting a new specialist on each prompt would avoid the gap,
  but it would lose the specialist's conversation, which auto mode judges calls against.
