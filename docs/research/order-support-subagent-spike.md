# Spike: OpenAI Agents SDK parent driving a Strands child on another task queue

Step 1 of `docs/plans/order-support-demo.md`. Code: `order_support/spike/`. Tests:
`order_support/tests/test_spike_*.py`. The harness problems found here are also indexed in
[`agent-kit-serverless-gaps.md`](agent-kit-serverless-gaps.md).

## Question

Can a harness agent built on the OpenAI Agents SDK, polling queue A, drive a harness agent built on
Strands (Claude on Bedrock), polling queue B, through `agent.subagent_toolset`? Specifically:

- with a stub contract class, so the parent never imports Strands;
- with an approval-gated tool inside the child that a person approves;
- on the sessions-wheel temporalio (`1.34.0+sessions`), although the harness declares `>=1.35`.

## Setup

| Piece | File | Queue |
|---|---|---|
| Parent `SpikeConciergeAgent` (OpenAI Agents SDK) | `spike/concierge_workflow.py` | `spike-concierge` |
| Child `SpikeShippingAgent` (Strands, `harness_strands_agent`) | `spike/shipping_workflow.py` | `spike-shipping` |
| Child's tools: `track_shipment` (inherently safe), `expedite_shipment` (gated) | `spike/shipping_tools.py` | |
| Stub contract the parent reflects on | `spike/contracts.py` | |
| Local Workers, one client and plugin set per queue | `spike/worker.py` | both |
| UI registry and schemas | `spike/agents.toml`, `spike/schemas/` | |

The two Workers use separate clients, as the AgentCore runtimes will:

- the concierge's client has the OpenAI Agents plugin and `AgentHarnessPlugin(tools=HUMAN_TOOLS)`;
- the shipping client has `StrandsPlugin(observer_factory=…)` and
  `AgentHarnessPlugin(tools=[*SHIPPING_TOOLS, *HUMAN_TOOLS])`.

## Results (offline, 2026-11-20)

`uv run pytest` runs everything below with scripted models on a local `temporal` dev server, so it
needs no credentials.

| Check | Result |
|---|---|
| Strands plugin imports and builds on `temporalio 1.34.0+sessions` | ✅ No errors. The `>=1.35` declaration is overridden, as in `react_agent_sessions` |
| Stub contract matches the real child's handlers (name, input/output types, description, `model_callable`) | ✅ `test_contract_matches_the_real_shipping_agent` (drift test) |
| Parent module imports no `strands` module | ✅ Checked in a fresh interpreter |
| `subagent_toolset(stub, key=, task_queue=, workflow_type=)` generates `start_shipping`, `shipping_ask`, `stop_shipping` | ✅ |
| Parent approval policy auto-approves those three tools | ✅ `allow_inherently_safe().with_changes(auto_approve_tools=[…])` |
| Parent starts the child on the other queue; the child runs on the Strands Worker | ✅ `subagent_started` |
| Gated `expedite_shipment` in the child: `tool_approval_requested` on the **child's** stream, approved with `tool_approval` against the child's workflow id (from `subagent_started.workflow_id`) | ✅ |
| The child's reply returns to the parent and reaches the parent's final answer | ✅ `subagent_message_sent` → `subagent_reply_received` → `message_handler_end` |
| Different data converters on the two clients (OpenAI payload converter vs Strands) | ✅ No issue crossing the child-workflow boundary |

The parent's stream carries `subagent_started`, `subagent_message_sent` and
`subagent_reply_received`, which are the markers the UI uses to nest a child's stream.

## Finding: harness bug in `subagent_toolset` with OpenAI Agents SDK parents

**Symptom.** Every `shipping_ask` call failed with:
`Tool 'shipping_ask' failed: shipping_ask() takes 1 positional argument but 2 were given`.

**Cause.** `subagent_toolset` gives each generated `<key>_<handler>` tool the signature
`(subagent, message)`, both positional-or-keyword, but its body is
`_send(subagent: str, **model_kwargs)`. The OpenAI Agents SDK adapter builds the call with
`FuncSchema.to_call_args()`, which passes positional-or-keyword parameters **positionally**. So
`message` arrives as a second positional argument that `_send` has no slot for. See
`harness/subagent_toolset.py` (`_make_send_tool`) and `ai_sdks/openai_agents_harness.py`
(`run_tool(tool_call_id, tool_callable, *args, ...)`).

**Scope.** It's in harness 0.11.x and is unchanged in the kit source at 0.12.0
(`temporal_agent_kit/subagent_toolset.py`; `ai_sdks/openai_agents/_kit.py:411`). The only shipped
subagent example (`examples/monty`) calls tools by keyword from code mode, so nothing exercised
this path.

**Workaround (in the spike).** `spike/keyword_tools.py`'s `keyword_only(tools)` re-declares the
generated tools' parameters as keyword-only. The adapter then passes them by name, and the
model-facing schema doesn't change. `test_generated_subagent_tools_take_keyword_arguments`
guards it.

**Upstream fix.** One of these, in `_make_send_tool`:
- make `_send` take `(subagent, <param>)` explicitly; or
- declare the synthesized parameters `KEYWORD_ONLY`.

Worth an issue on temporal-agent-kit. Remove `keyword_only` once a fixed release is in use.

## Results (local Workers against staging, 2026-11-20)

Run as in `order_support/README.md#run-locally`, with real models and activity sessions on.

| Check | Result |
|---|---|
| The Shipping agent runs live: Strands with Claude Opus 5.5 on Bedrock (`global.anthropic.claude-opus-5-5`) | ✅ |
| The kit UI works with the 0.11 `serve` for both agents | ✅ |
| Thought summaries in the UI for the Strands agent | ✅ after a fix, below |

**Thought summaries.** At first the UI's Thought Summary box said "The model reported thinking but
no thought summary is available". Claude Opus 5.5 omits thinking text unless the request asks for
summaries, so Bedrock streamed thinking blocks with only a signature, and the harness's Strands
adapter passed those on as text-less `thought_summary` events. The fix is a request option on the
`BedrockModel` (`spike/hosted_agents.py`):
`additional_request_fields={"thinking": {"type": "adaptive", "display": "summarized"}}`. Bedrock
accepts it and the summaries then appear.

## Deploying to AgentCore

`order_support/README.md#deploy-to-agentcore` deploys the two agents as two AgentCore runtimes
(`concierge`, `shipping`), each its own Worker Deployment, from one entry point
(`agentcore_worker.py`) that picks its agent with `ORDER_SUPPORT_AGENT`. First deployed
2026-11-20.

### Finding: Strands on AgentCore needs `opentelemetry` passed through the sandbox

**Symptom.** On AgentCore only, every Shipping turn failed in the UI with "Cannot access
os.environ.get from inside a workflow". The workflow history showed no failure, because the
harness catches a handler's exception and publishes only `str(e)` as `message_handler_error`.
`spike/shipping_workflow.py` now logs the traceback to the Worker's logs before re-raising.

**Cause.**
1. Strands starts OpenTelemetry spans inside the workflow (`strands/telemetry/tracer.py`).
2. On AgentCore, `bedrock_agentcore` adds a span processor whose `on_start`
   (`bedrock_agentcore/runtime/tracing.py:121`) imports `opentelemetry.baggage` lazily.
3. That import runs inside the workflow, and the Strands plugin's sandbox doesn't pass
   `opentelemetry` through, so the sandbox imports a fresh copy of `opentelemetry.context`.
4. That module's body reads `os.environ` (`OTEL_PYTHON_CONTEXT`), which the sandbox rejects.

Locally there is no such span processor, so the path never runs. The OpenAI concierge doesn't hit
it either, because the OpenAI Agents SDK doesn't start OpenTelemetry spans in the workflow.

**Fix.** Every Worker uses `spike.hosted_agents.workflow_runner()`, a sandbox with `opentelemetry`
passed through; the plugins add their own passthroughs on top.
`tests/test_workflow_sandbox.py` reproduces the failure with a span processor that imports lazily,
the way AgentCore's does.

**Upstream.** Worth raising with temporal-agent-kit: the Strands plugin could pass `opentelemetry`
through itself, since Strands always starts spans in the workflow.

### Finding: a subagent turn keeps the parent's Worker busy while the child waits

**Symptom.** After a Concierge turn that used Shipping, Shipping's Worker drained and its AgentCore
session ended, but the Concierge's Worker kept polling and its session stayed up. Nothing was
being sent: no UI open, no traffic.

**Cause.** Each `shipping_ask` call runs the harness's `run_subagent_turn` activity on the
Concierge's Worker. The activity sends the message to Shipping and then holds open, heartbeating,
until Shipping's turn ends. The Concierge's Worker drains only once no activity is in flight
(`agentcore_worker.ActivityTracker`), so it stays up, and billed, for as long as Shipping's turn
is open. When Shipping waits for a person, for example the expedite approval, Shipping's own
Worker drains, because it waits inside its workflow. The Concierge's activity times out after
1 hour (`DEFAULT_SUBAGENT_START_TO_CLOSE_TIMEOUT`).

This is how the harness works, not a setting: temporal-agent-harness 0.11.1 and the kit source at
0.12.0 (`subagent_activities.py`; `agent_workflow.py`, `AgentWorkflowRunner.run_subagent_turn`)
both block in the activity by design.

**Fix.** Upstream. The proposal, in
[`agent-kit-issue-subagent-turn-holds-parent-worker.md`](agent-kit-issue-subagent-turn-holds-parent-worker.md):
a short activity sends the message, the child signals the parent with the reply when its turn
ends, and the parent waits in its workflow. Then neither agent holds a Worker while a person
decides.

**Local fix (2026-11-22).** `order_support/subagent_signals.py` implements that proposal for the
demo, without changing the kit. Both agents use `SubagentSignalRunner`, an `AgentWorkflowRunner`
subclass:
- **Parent.** It overrides `run_subagent_turn`, copied from harness 0.11.1 with only the blocking
  activity replaced. The short `send_subagent_message` activity sends the message. The workflow
  publishes `subagent_message_sent`, using the child's `accepted_offset` as `from_offset`, then
  waits for the reply signal for that `message_id`, for up to 24h. The FIFO gate,
  `subagent_reply_received` and the `AgentClosed` handling are the harness's own.
- **Child.** It overrides the private `_dispatch_turn` and `_run_participant` hooks. After
  handling a message, and after `turn_end` when it's the last participant, the child signals its
  parent with the reply or the error, plus its stream offset.

Costs and limits:
- A child signals its parent for every message it handles. A person messaging a nested subagent
  directly therefore wakes, and invokes, the parent for a reply it ignores.
- It overrides private harness methods, so it's tied to the locked harness version. The e2e tests
  guard it. `test_concierge_holds_nothing_while_shipping_waits_for_approval` checks that the
  Concierge has no pending activity while Shipping waits for approval. It then finishes the turn on
  a new Concierge Worker. That test failed with the harness's activity.

To see what keeps a Worker up, `agentcore_worker.py` logs each activity's name and workflow when
it starts and finishes.

### Related fix: no environment reads in workflow code

The concierge workflow module used to read `SPIKE_CONCIERGE_STREAM` at import to choose
streaming. The sandbox only restricts `os` while a workflow runs, so it wasn't caught, but a
Worker with a different environment could replay a history differently. The concierge now always
streams, and `test_workflow_modules_do_not_read_the_environment` guards against env reads in
workflow modules.

## Still to verify

Setup and commands: `order_support/README.md` (run locally, or deploy to AgentCore).

In the UI:

1. Open **Order Concierge** and send "Where is order 1042? Expedite it."
2. Check that Shipping appears nested under Concierge, the expedite approval appears on the nested
   Shipping subagent, and approving it there completes the Concierge's answer.

Open questions:

- **UI nesting with the 0.11 `serve` API.** The nesting markers are on the stream, but nesting
  and the in-child approval haven't been confirmed in the kit UI yet.
- **Activity sessions across a parent and child, on AgentCore.** The concierge's
  `run_subagent_turn` activity joins the concierge's session, and the child runs in its own. Each
  `SingleSessionAdmission` Worker serves one workflow, so the child needs its own invoke, which is
  why it is on a separate runtime and queue.
- **An approval after the child's Worker drains, on AgentCore.** The approval should trigger a new
  invoke of the shipping runtime, with a new AgentCore session, and the child workflow has to move
  to that Worker.
- **Both Workers draining while the child waits, on AgentCore (local fix above).** With the
  approval left open, both sessions should end about 60s after the last activity. After approval,
  Shipping's reply signal should invoke the Concierge in a new session, which then finishes the
  answer.

## Other notes

- `uv lock` resolved `temporal-agent-harness` **0.11.1**; `react_agent_sessions` locks 0.11.0. No
  API differences turned up.
- `ResponseBuilders.tool_call` in the harness's OpenAI testing helpers reuses one `call_id`. The
  OpenAI Agents SDK refuses that across calls ("Model reused a completed tool call ID"), so the
  e2e test builds its own tool calls.
