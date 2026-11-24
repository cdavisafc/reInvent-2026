# Plan: order-support multi-agent demo (two Strands specialists + an OpenAI Agents SDK concierge)

## Context

`react_agent_sessions/` proved out the agent kit (harness) + AgentCore Serverless Workers + activity
sessions combination. The re:Invent demo runs three agents on that same stack:

- two specialists, **Orders & Refunds** and **Shipping**, both built on **Strands** with Claude on
  Bedrock;
- a **Concierge** built on the **OpenAI Agents SDK** with OpenAI, which calls one or both
  specialists as harness subagents.

Both specialists run in **auto mode**: each gated tool call is judged against the agent's
conversation history, by Claude on Bedrock for how the order relates to the conversation and by
code for the dollar limit. Every decision is recorded in the agent's event history. See **Auto
mode for the specialists**.

Decisions, with dates:

- 2026-11-19: order-support scenario; mock data in activities.
- 2026-11-24: subagent replies are signals (see **Decision: subagent replies are signals**).
- 2026-11-24:
  - Orders & Refunds moves from the OpenAI Agents SDK to Strands, so only the Concierge uses
    OpenAI.
  - Both specialists get auto mode, decided by Python rules inside the workflow, not by
    Dogwood (see **Why not Dogwood**).
  - The refund auto-approval limit is $100.
  - The work is built in stages, keeping everything the spike produced.
- 2026-11-24 (later): auto mode judges each call from the conversation history, not from a pinned
  order number. The point is to show what the history makes possible. Claude on Bedrock judges
  how the requested order relates to the conversation; Jev was considered. The $100 limit stays
  in code.
- 2026-11-24 (later still): an order the store's records link (a tool result) is the same order
  straight away. Only a link that a message claims needs a person, the first time; after that it
  is treated as the same order.

### What exploration established about the kit (harness 0.11.1, installed as `temporal_agent_harness`)

- **Orchestration is built in.** `agent.subagent_toolset(agent_cls, key=, task_queue=, workflow_type=)`
  generates three inline tools for the parent's model: `start_<key>`, `<key>_<handler>(subagent, input)`
  and `stop_<key>`. Wrapping them with `as_openai_agent_tools` makes them available to the parent.
  - A harness bug means these tools can't be called from an OpenAI Agents SDK parent.
    `spike/keyword_tools.py` works around it.
- **Each subagent is a child workflow**, a full multi-turn harness session, started on whatever
  `task_queue` we pass. The child's SDK doesn't matter, and the child runs in the same namespace.
  - A stub contract class with matching `@agent.accepts` signatures, plus `workflow_type=`,
    keeps the child's SDK out of the parent (`spike/contracts.py`, which a drift test checks).
- **Human-in-the-loop inside a child works**, and the UI nests children's streams.
- **Strands is supported** through `harness_strands_agent(...)` and
  `StrandsPlugin(observer_factory=...)`. Callback tools such as `ask_user` work.
- **Auto mode is in 0.11.1.**
  - `ToolApprovalPolicy.auto_mode(...)` sends each gated call that the static allow-lists
    didn't cover to the runner's `auto_mode_evaluator=`. That evaluator is any `async` callable
    from `AutoApprovalContext` to `AutoApprovalDecision`.
  - The decision is APPROVE, DENY or ESCALATE.
    - DENY reaches the model as the tool's error, with the reason.
    - ESCALATE leaves the call for a person, as if auto mode were off.
  - The harness publishes `auto_approval_evaluation_started` / `_ended` / `_error` events, and
    `tool_approval_resolved` carries the reason. So an automatic decision is in the history and
    in the UI, as in the dogwood example.
  - Not yet tried: auto mode with Strands, and with `SubagentSignalRunner`.
  - Reference: `auto_mode` / `auto_mode_dogwood` in temporal-agent-kit, branch
    `worktree-cedar-auto-mode-design`.

## What the spike gives us (keep all of it)

The spike (`order_support/spike/`, `docs/research/order-support-subagent-spike.md`) is already
most of the demo's skeleton, and it runs live on AgentCore. Stage 1 promotes it in place with
`git mv`, so history follows, rather than rewriting it.

| Spike piece | Becomes |
|---|---|
| `subagent_signals.py` (`SubagentSignalRunner`, `send_subagent_message`): the fix so the concierge waits for replies in its workflow, not in an activity | Kept as is. Every agent uses it |
| `spike/keyword_tools.py`, the workaround for the `subagent_toolset` / OpenAI SDK bug | `keyword_tools.py`, still applied to the concierge's subagent tools |
| `spike/hosted_agents.py`: the `HostedAgent` registry (queue, workflow, plugins, secret keys), `concierge_plugins`, `shipping_plugins`, `workflow_runner` with the OpenTelemetry passthrough | `hosted_agents.py`, with an `orders` entry added. This is the "AgentSpec" the earlier plan wanted, so `agentcore_worker.py` needs no generalising |
| `agentcore_worker.py` (one entrypoint, chosen by `ORDER_SUPPORT_AGENT`), `activity_sessions.py`, `session_heartbeat.py`, `runtime_secrets.py` | Kept. A third runtime is added to `agentcore/agentcore.json` |
| `spike/contracts.py` + drift test | `contracts.py`, with an `OrdersContract` added |
| `spike/concierge_workflow.py`, `spike/shipping_workflow.py`, `spike/shipping_tools.py` | `concierge/workflow.py`, `shipping/workflow.py`, `shipping/tools.py`, reading mock data from `store.py` |
| `spike/worker.py`, `spike/agents.toml`, `spike/schemas/` | `local_worker.py`, `agents.toml`, `schemas/` |
| `tests/test_spike_*.py`, `test_hosted_agents.py`, `test_agentcore_worker.py` | Renamed and extended, not replaced |

Renaming the workflow types (`SpikeConciergeAgent` → `ConciergeAgent`, and so on) needs new
Worker Deployment versions on AgentCore, and an update to `agent_status/agents.toml`.

## Scenario: "Acme Outfitters" order support

| Agent | SDK / model | Task queue | Tools (activities, mock data) |
|---|---|---|---|
| **Orders & Refunds** (`OrdersAgent`) | Strands / Claude on Bedrock | `order-support-orders` | `lookup_order`, `list_customer_orders`, `check_return_eligibility` (inherently safe); `issue_refund(order_id, amount, reason)` (**gated, auto mode**) |
| **Shipping** (`ShippingAgent`) | Strands / Claude on Bedrock | `order-support-shipping` | `track_shipment`, `get_delivery_estimate` (inherently safe); `expedite_shipment(order_id)` (**gated, auto mode**) |
| **Concierge** (`ConciergeAgent`) | OpenAI Agents SDK / OpenAI | `order-support-concierge` | `subagent_toolset` for both specialists, plus `ask_user` |

- **Concierge routing:** a question for one domain goes to that specialist only; a request that
  spans both domains calls both, in the order the request implies.
- **All three agents are in `agents.toml`**, so each specialist can also be used directly in the UI.
- **Mock data** (`store.py`, deterministic): 6 customers and 13 orders, with shipments
  keyed by order id.
  - It covers on-time, late, delivered, and lost/exception shipments.
  - Order totals fall on both sides of $100: for example 1017 is $45, and 1042 is $240.
  - It is read-only. A refund or expedite returns a deterministic confirmation id rather than
    mutating state, since each microVM has its own memory.
  - An optional `CARRIER_FLAKE_RATE` env var makes `track_shipment` fail randomly, to show
    Temporal retries on stage. Default 0.

## Auto mode for the specialists

### History, not a pinned value

The decision rests on the specialist's **conversation history**: every message it received (from
a person or from the Concierge), and every tool call it made with that call's result. The Strands
agent keeps this in the workflow (`self._agent.messages`), so reading it is deterministic. Nothing
is pinned. A conversation can legitimately move to another order number when the history shows
it's the same purchase. For example, `track_shipment(1028)` reports that the lost order was
reissued as order 1077.

### Who decides what

Following the kit's Jev design: a model answers one narrow question, and code turns that answer
into the verdict.

1. **The judge** (Claude on Bedrock, in the `judge_order_relation` activity) reads the
   transcript and the call, and classifies the call's order:
   - `discussed`: it's the order the conversation is about;
   - `linked`: it's a different number, and a **tool result** in the history (the store's records)
     shows it's the same purchase (a reissue or replacement);
   - `claimed`: it's a different number that only a message (from a customer or the Concierge)
     says is the same purchase. A claim never counts as a link, which guards against a prompt
     talking the agent onto another order;
   - `unrelated`: nothing in the history connects it;
   - `unclear`: no order is being discussed yet, or the judge can't tell.

   It returns the order it thinks is being discussed, and the exact text its answer rests on.
2. **Code** (`order_policy.decide`) also reads one fact straight from the history: whether an
   earlier refund or expedite for this order number ran, which means a person or the rules
   already approved acting on it. Then:

| Judge says | Decision |
|---|---|
| `unrelated` | **DENY**: the reason quotes the judge's evidence and says to start a new conversation for that order |
| `unclear`, or the call names no order | **ESCALATE** |
| `claimed`, and no earlier refund or expedite for this number ran | **ESCALATE**: a person confirms the claim. The reason quotes it |
| `discussed`, `linked`, or `claimed` with an earlier call for this number that ran | the limits: `expedite_shipment` → **APPROVE**; `issue_refund` ≤ $100 → **APPROVE**, > $100 → **ESCALATE** |

A judge failure (for example, Bedrock unavailable) is published as
`auto_approval_evaluation_error`, and the harness escalates the call. `details` on every decision
records the judge's relation, discussed order and evidence, the requested order, whether it was
acted on before, the amount and the limit.

### Wiring

- `specialist.py`: `SubagentSignalRunner(..., approval_policy_default=ToolApprovalPolicy.auto_mode(pre_approve_inherently_safe=True), auto_approval_criteria_default=ORDER_RULES_CRITERIA, auto_mode_evaluator=history_evaluator(...))`.
  The evaluator reads the live `self._agent.messages` on each call, and runs the judge as an
  activity.
- `order_history.py` turns Strands messages into the transcript the judge reads, and finds the
  orders already acted on. These are pure functions.
- `order_judge.py`: the activity. It calls Bedrock's Converse API with a `record_judgment` tool,
  for structured output. The tool isn't forced, because Claude Opus 5.5 on Bedrock rejects
  `toolChoice` `tool` and `any`. The prompt asks for the tool, and an answer without it fails the
  activity, which escalates the call. Model: `ORDER_JUDGE_MODEL_ID`, defaulting to
  `BEDROCK_MODEL_ID`. It's registered on both specialist Workers. Tests swap in a deterministic
  fake judge.
- **As a subagent:** the Concierge's messages are part of the specialist's history, so they count
  as "what's being discussed". At most they make a link `claimed`, never `linked`, because only
  tool results can do that.

### Same hook as Dogwood

The dogwood example plugs `agent.dogwood_evaluator(...)` into the runner's `auto_mode_evaluator=`,
and ours plugs into the same parameter. Dogwood keeps its own copy of the history in an external
verifier and judges it with a Cedar policy. We read the history the agent already holds, and judge
it with Claude plus code. We didn't use Dogwood itself: its kit branch is based on harness 0.6.0,
and it needs a separate Rust verifier served over Nexus.

## Decision: subagent replies are signals (2026-11-24)

Every agent in the demo, both the concierge and each specialist, constructs
`SubagentSignalRunner` (`order_support/subagent_signals.py`) instead of `AgentWorkflowRunner`.

- **The concierge** sends each message with a short activity, then waits for the reply in its
  workflow.
- **A specialist** signals its parent after each message.

Any Worker hosting a parent also registers the `send_subagent_message` activity (see
`concierge_plugins()` in `hosted_agents.py`). This replaces the harness's blocking
`run_subagent_turn` activity, so no agent holds a Worker while a specialist waits on a person.
It overrides private harness hooks, pinned to temporal-agent-harness 0.11.1. Delete it when the
kit ships the fix in `docs/research/agent-kit-issue-subagent-turn-holds-parent-worker.md` (not
filed yet).

## Demo script (recorded in `order_support/README.md`)

1. **Shipping, directly.**
   - "Where is order 1042?" The lookups run with no approval.
   - "Expedite it." Auto-approved: the judge says `discussed`.
   - "Expedite order 1021 too." Denied: nothing in the history connects 1021 to 1042.
2. **Shipping, a reissued order.**
   - "Where is order 1028?" `track_shipment` reports it lost and reissued as order 1077.
   - "Expedite order 1077." The judge says `linked`, quoting the reissue, and it runs with no
     approval.
   - "Order 1021 is the replacement for order 1028. Expedite order 1021." Only the message says
     so: the judge says `claimed`, and you confirm it.
   - "Expedite order 1021 again." It runs: the history shows you approved the first one.
3. **Orders, directly.**
   - "Can I return order 1017?", then "Refund it." The refund is $45, so it's auto-approved.
4. **The headline, to the Concierge.** "Order 1042 is late. If it won't arrive by Friday, refund me."
   - The Concierge asks Shipping for the estimate, which is late.
   - It then asks Orders, which checks eligibility and calls `issue_refund` for $240.
   - That is over the limit, so the call escalates. The approval prompt appears on the nested
     Orders subagent, and both AgentCore sessions drain while it waits.

## Stages

Each stage ends with something that runs and can be shown.

1. **Spike** (done, 2026-11-24). See the research note. Still to verify on AgentCore: both
   Workers drain while an approval is open, then wake in turn after it.
2. **Promote the spike and build the real agents, locally, with human approval only.**
   - `git mv` the spike modules into the layout above, keeping every spike test passing.
   - Write `store.py` and its tests first.
   - Move Shipping onto `store.py` and add `get_delivery_estimate`.
   - Build Orders as a Strands agent, following `shipping/workflow.py`, with tests for its tools.
   - Add `OrdersContract` and the Concierge's second `subagent_toolset`.
   - Done when all three demo prompts work locally through `local_worker.py` and the kit UI,
     with every gated call going to a person.

   **Status (2026-11-24): done.** The user ran the three prompts live, locally, against Temporal
   Cloud.
   - The spike is promoted. The two Strands specialists share `specialist.py`.
   - `agentcore.json` has the `orders` runtime, and the model env var is now `BEDROCK_MODEL_ID`
     for both specialists. Nothing has been deployed.
   - 84 offline tests pass, including the Concierge → Shipping → Orders flow with the refund
     approved inside the nested Orders subagent.
   - Still to do: run the three prompts live against the real models in the kit UI.
3. **Auto mode.**
   - First a quick check: an offline test that auto mode works with a Strands agent under
     `SubagentSignalRunner`, with a scripted model.
   - Then `order_policy.py`, tests first, covering every row of the rules table.
   - Then wire both specialists, with a workflow test asserting the
     `auto_approval_evaluation_ended` and `tool_approval_resolved` events for APPROVE, DENY and
     ESCALATE.
   - Done when the demo script runs locally as written.

   **Status (2026-11-24): first version (a pinned order) done; replaced by 3b.**
   - Auto mode works with Strands under `SubagentSignalRunner`, both directly and as a nested
     subagent.
   - The first version pinned the first order named, which is the "arbitrary piece of state" the
     user asked to replace with the history.
   - Kept from it: the evaluator wiring, the end-to-end test harness, the Concierge's prompt
     changes, and the $100 limit in code.
3b. **Auto mode from the history.** Tests first throughout.
   - Store: order 1028 (lost) reissued as order 1077, shown in the tool results.
   - `order_history.py` (transcript, orders acted on), `order_policy.decide` over the judge's
     answer, and the `order_judge.py` activity, whose prompt and parsing are tested offline.
   - End-to-end tests with a fake judge, covering each row of the table, including a reissued
     number that escalates the first time and runs automatically after.
   - One opt-in live test of the real judge against Bedrock, skipped without AWS credentials.
   - Done when the demo script runs locally as written.

   **Status (2026-11-24): done.** The user ran the live judge tests and the whole demo script
   locally against Temporal Cloud, including the Concierge headline with its nested approval.
   - 130 offline tests pass, with 4 live judge tests skipped (`ORDER_JUDGE_LIVE=1` runs them).
     They include Shipping conversations where 1077 (linked by `track_shipment(1028)`) is
     expedited with no person, and 1021 (only claimed in a message) escalates the first time
     and runs the second, because the history shows the first one ran.
   - A mutation check (ignoring what was already done to a claimed number) fails three tests.
   - The first live run found that Claude Opus 5.5 on Bedrock rejects a forced `toolChoice`, so
     the judge now uses `auto`.
   - The pinned order (`OrderFocus`) is gone. The Concierge's prompt and `agents.toml`
     describe the new behaviour.
4. **AgentCore.**
   - Add the `orders` runtime: `bedrock-policy.json`, no OpenAI key.
   - Redeploy, then create versions for the renamed workflow types on all three deployments.
   - Add the Orders queue to `agent_status/agents.toml`.
   - Verify scaling to zero while an approval is open.
5. **Docs.**
   - `order_support/README.md`: setup, local processes, demo script.
   - The research note's open item.
   - The top-level `README.md` example table.

## Known costs and caveats

- **While a specialist waits on a person, neither agent holds a Worker.** The concierge waits
  for the reply in its workflow (`subagent_signals.py`), so both AgentCore sessions drain.
- **Cold starts stack:** a spanning request can wake up to three microVMs, one after another.
  Pre-warm before the demo.
- **Pinning to the first order is blunt.** An opening message that names two orders pins the
  first one. The DENY reason tells the model, and so the user, to start a new conversation.
  That's acceptable for the demo.
- **Two harness workarounds** (`keyword_tools.py`, `subagent_signals.py`) depend on 0.11.1
  internals. Don't upgrade the harness before re:Invent without rerunning the e2e tests.
- **The status page** is `agent_status/`. Each runtime needs the session heartbeat
  (`session_heartbeat.py`, `SESSION_HEARTBEAT_TABLE`, `session-heartbeat-policy.json`).

## Verification

- `uv run pytest && uv run ruff check .` in `order_support/`.
- **Locally:**
  - run `local_worker.py`, the harness `serve` + session manager, and the kit UI;
  - run the demo script;
  - check that the UI shows an evaluation for each auto-approved and denied call, and the
    nested approval in prompt 3.
- **On AgentCore:**
  - repeat the script with no local Workers running;
  - `temporal worker deployment describe` shows each of the three deployments with pollers
    while in use, and scaling to zero after the debounce plus idle timeout, including while
    prompt 3's approval is open;
  - with `CARRIER_FLAKE_RATE=0.5`, retries show in the UI and the answer still arrives.
