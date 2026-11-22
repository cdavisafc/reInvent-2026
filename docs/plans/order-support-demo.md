# Plan: order-support multi-agent demo (Strands + OpenAI Agents SDK + orchestrator)

## Context

`react_agent_sessions/` proved out the agent kit (harness) + AgentCore Serverless Workers + activity
sessions combination. The real re:Invent demo needs three agents on that same stack:

- two specialists, one built on **Strands** and one on the **OpenAI Agents SDK**;
- an **orchestrator** that calls one or both of them when a request needs it.

The user chose the following (2026-11-19):

- **Scenario:** order support.
- **Data:** mock data in activities.
- **Models:** the Strands agent uses Claude on Bedrock; the other two agents use OpenAI.

### What exploration established about the kit (harness 0.11.0, installed as `temporal_agent_harness`)

- **Orchestration is built in.** `agent.subagent_toolset(agent_cls, key=, task_queue=, workflow_type=)`
  (`harness/subagent_toolset.py:283`) generates three inline tools for the parent's model:
  `start_<key>`, `<key>_<handler>(subagent, input)` and `stop_<key>`. Wrapping them with
  `as_openai_agent_tools` makes them available to the parent.
- **Each subagent is a child workflow**, a full multi-turn harness session, started on whatever
  `task_queue` we pass. The parent runs one `run_subagent_turn` activity per child turn; it
  heartbeats and lasts until the child's `turn_end`.
- **The child's SDK is irrelevant**: any harness agent works.
- **Human-in-the-loop inside a child works.** The UI approves against the child's workflow id.
- **The UI already nests subagents.** The client merges the parent's and children's streams
  (`subagent_started` / `subagent_message_sent` / `subagent_reply_received` markers).
- **Same namespace only.** Nexus is a separate feature and isn't needed here.
- **Untested combination:** no shipped example runs a child on another queue or another SDK.
  `subagent_toolset` needs a class to reflect on; a stub with matching `@agent.accepts`
  signatures plus `workflow_type=` avoids importing the child's SDK into the parent.
- **Strands is supported.** `harness_strands_agent(runner, model="bedrock", tools=as_strands_tools(...))`
  is built once in `@agent.init`, and each turn calls `invoke_async`.
  - Worker: `StrandsPlugin(observer_factory=harness_observer_factory)` goes before
    `AgentHarnessPlugin`; it registers `"bedrock"` → `BedrockModel` by default.
  - Callback tools such as `ask_user` work. Strands' own interrupts and plain `@tool` functions
    are refused.
  - Reference: `temporal-agent-kit/python/examples/strands_hello/`.

## Scenario: "Acme Outfitters" order support

| Agent | SDK / model | Task queue | Tools (activities, mock data) |
|---|---|---|---|
| **Orders & Refunds** (`OrdersAgent`) | OpenAI Agents SDK / OpenAI | `order-support-orders` | `lookup_order`, `list_customer_orders`, `check_return_eligibility` (inherently safe), `issue_refund` (**needs human approval**) |
| **Shipping** (`ShippingAgent`) | Strands / Claude on Bedrock | `order-support-shipping` | `track_shipment`, `get_delivery_estimate` (inherently safe), `expedite_shipment` (**needs approval**) |
| **Concierge** (`ConciergeAgent`) | OpenAI Agents SDK / OpenAI | `order-support-concierge` | `subagent_toolset` for both specialists, plus `ask_user` |

- **Concierge routing:** a question for one domain goes to that specialist only; a request that
  spans both domains calls both, in the order the request implies.
- **All three agents are in `agents.toml`**, so each specialist can also be used directly in the UI.
- **Scripted demo prompts** (recorded in the README):
  1. To Shipping directly: "Where is order 1042?"
  2. To Orders directly: "Can I return order 1017?"
  3. To Concierge, the headline: "Order 1042 is late. If it won't arrive by Friday, refund me."
     Concierge sends Shipping for the estimate (late), then Orders for eligibility and
     `issue_refund`. The approval prompt appears on the nested Orders subagent in the UI.
- **Mock data** (`store.py`, deterministic): about 6 customers and 12 orders, with shipments
  keyed by order id. It covers on-time, late, delivered, and lost/exception cases.
  - It is read-only. A refund or expedite returns a deterministic confirmation id rather than
    mutating state, since each microVM has its own memory.
  - An optional `CARRIER_FLAKE_RATE` env var makes `track_shipment` fail randomly, to show
    Temporal retries on stage. Default 0.

## Layout: new example `order_support/`

One uv project and one AgentCore project, with **three runtimes** in `agentcore/agentcore.json`. The
`runtimes` array allows several. Each runtime has its own entrypoint, task queue and Worker
deployment, and all three share the CodeZip.

- **`agentcore_runtime.py`**: the generic machinery, generalised from
  `react_agent_sessions/agentcore_worker.py`. It holds the app, invoke/ping, `ActivityTracker`
  debounce, `ActivitySessionInterceptor` (from `activity_sessions.py`), versioning, identity,
  secrets and session heartbeat. It is parameterised by an `AgentSpec`: task queue, deployment
  name, workflows, tools, extra plugins, and the secret keys needed.
- **Entrypoints:** `orders_worker.py`, `shipping_worker.py`, `concierge_worker.py`. Each builds its
  `AgentSpec` and calls `run_app(spec)`.
- **`orders/`**: `workflow.py`, `tools.py`. Its `workflow.py` follows `react_agent_sessions/workflow.py`.
- **`shipping/`**: `workflow.py`, `tools.py`. Its `workflow.py` follows `strands_hello/workflow.py`.
- **`concierge/workflow.py`**: OpenAI Agents SDK, using `as_openai_agent_tools(runner, [*subagent_toolset(OrdersContract, …), *subagent_toolset(ShippingContract, …), *HUMAN_TOOLS])`.
  The approval policy auto-approves the generated `start_*`, `*_ask` and `stop_*` tools.
- **`contracts.py`**: stub classes with the specialists' `@agent.accepts` signatures plus their
  workflow type names and task queues. The concierge imports this, not the specialists' SDKs.
- **`store.py`** (mock data), **`human_tools.py`** (`ask_user`, copied).
- **Local process:** `local_worker.py` runs all three Workers in one process for local
  development. It reuses each spec without AgentCore.
- **Config:** `agents.toml`, the schema JSONs, and `pyproject.toml`:
  - `temporal-agent-harness[openai-agents,strands-agents]`;
  - the same temporalio sessions-wheel override and `openai==3.8.0` pin as `react_agent_sessions`.
- **Scripts and policies:**
  - `bin/create-runtime.sh` (deploys all three runtimes);
  - `bin/create-versions.sh` (one `create-version` + `set-current-version` per deployment);
  - `put-secrets.sh`, `mk-invoke-role.sh`, `mk-heartbeat-table.sh` (copied);
  - `bedrock-policy.json`: `bedrock:InvokeModel*`, for the shipping runtime only.
- **Tests (TDD):**
  - mock store queries;
  - each tool's behaviour, including the approval flags;
  - the generic runtime: the existing `agentcore_worker` tests, ported to `AgentSpec`;
  - `contracts.py` matches the real specialists' handler signatures, so a drift test fails if
    they diverge;
  - the concierge's tool list contains the generated subagent tools.

## Order of work

1. **Spike the riskiest piece, locally** (record it in `docs/research/order-support-subagent-spike.md`):
   - a minimal OpenAI-SDK parent on queue A calls a Strands child on queue B through
     `subagent_toolset` with a stub class;
   - run it on the sessions-wheel temporalio, against the staging namespace, with local Workers;
   - confirm the UI nests the child, and that an approval inside the child works.

   Also check that the Strands plugin works with temporalio `1.34.0+sessions`; the harness
   declares `>=1.35`.

   **Status (2026-11-20):** passes offline with scripted models (`order_support/tests/test_spike_*.py`).
   It found a harness bug: `subagent_toolset` tools can't be called from an OpenAI Agents SDK
   parent. The workaround is `spike/keyword_tools.py`. The live staging and UI run is still to do.
   See the research note.
2. Mock store and specialists (tests first), run locally with `local_worker.py` and the kit UI.
3. Concierge, run locally end to end with the three demo prompts.
4. Generalise the AgentCore runtime (port the tests), configure three runtimes, deploy, then
   create three versions.
5. Docs:
   - `order_support/README.md` (setup, the four local processes, demo script);
   - `docs/plans/order-support-demo.md` (this plan);
   - the top-level `README.md` example table.

## Known costs and caveats

- **While a specialist works, the concierge's AgentCore session stays busy** (the
  `run_subagent_turn` activity holds it). That's correct, and worth saying on stage.
- **Cold starts stack:** a spanning request can wake up to three microVMs, one after another.
  Pre-warm before the demo.
- **The status page** (`react_agent_sessions/status_page.py`) handles one queue. Making it handle
  several is a follow-up, not part of this plan.

## Verification

- `uv run pytest && uv run ruff check .` in `order_support/`.
- **Locally:**
  - run `local_worker.py`, the harness `serve` + session manager, and the kit UI on :5173;
  - run the three demo prompts;
  - prompt 3 shows Shipping and Orders nested under Concierge, the refund approval inside
    Orders, and the final answer from Concierge.
- **On AgentCore:**
  - repeat the prompts with no local Workers running;
  - `temporal worker deployment describe` shows each of the three deployments with pollers
    while in use, and scaling to zero after the debounce plus idle timeout;
  - with `CARRIER_FLAKE_RATE=0.5`, retries show in the UI and the answer still arrives.
