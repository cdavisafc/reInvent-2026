# Temporal agents on Amazon Bedrock AgentCore

Agents whose Temporal Workers run as
[Temporal Serverless Workers](https://docs.temporal.io/serverless-workers) on
[Amazon Bedrock AgentCore Runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime.html).
Temporal Cloud invokes the runtime when work arrives, and the Worker drains and scales to zero when
it goes idle.

| Example | What it is |
|---|---|
| [`strands_agent/`](strands_agent/) | A Strands agent with the AgentCore Code Interpreter as a tool (from `temporalio/samples-python`) |
| [`react_agent/`](react_agent/) | The temporal-agent-harness ReAct agent (OpenAI Agents SDK): weather tools plus a human-in-the-loop `ask_user` tool |
| [`react_agent_sessions/`](react_agent_sessions/) | The same ReAct agent with prerelease **activity sessions**: a workflow's activities run on its own Worker (one AgentCore session per workflow). Needs locally built temporalio wheels |
| [`order_support/`](order_support/) | An OpenAI Agents SDK concierge calling a Strands (Claude on Bedrock) shipping specialist as a harness subagent, each on its own AgentCore runtime with activity sessions. Currently the spike's minimal agents; needs locally built temporalio wheels |
| [`agent_status/`](agent_status/) | A local status page for all of the above that run on AgentCore with a session heartbeat: one section per agent (task queue, from `agents.toml`) showing whether each session's Worker is polling and whether its AgentCore session is alive. Owns the shared DynamoDB heartbeat table |

Each example is a self-contained uv project and AgentCore project. The CodeZip build zips the
example's directory, so run its scripts from inside it.

## Running the demo with every agent

One UI can list the ReAct agent, the Concierge and Shipping together: `serve` takes several
`agents.toml` registries and merges them. Each agent's Worker runs on AgentCore, so first deploy
`react_agent_sessions/` and `order_support/` as their READMEs describe. Both use the same Temporal
namespace and the shared heartbeat table from `agent_status/`.

With `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_API_KEY` (or a `temporal.toml`
profile) and AWS credentials exported, run each of these in its own terminal:

| Process | Command | Port |
|---|---|---|
| Session manager | `cd order_support && uv run temporal-agent-harness session-manager` | none |
| HTTP API, all three agents | `cd order_support && uv run temporal-agent-harness serve ../react_agent_sessions/agents.toml spike/agents.toml --port 8000` | 8000 |
| UI | `cd temporal-agent-kit/typescript/apps/ui && pnpm dev` | 5173 |
| Status page | `cd agent_status && uv run python status_page.py` | 8100 |
| ReAct terminal client (optional, to answer `ask_user`) | `cd react_agent_sessions && uv run python client.py` | none |

Open http://127.0.0.1:5173. The UI is run from the agent kit's source for now; see
`react_agent_sessions/README.md#running-the-ui-from-the-agent-kit-temporary`. Don't run
`order_support`'s local `spike.worker` alongside: it would compete with AgentCore for the queues.

`serve` keeps the agent list in the running `session-manager` workflow, and ignores the registries
it's given while that workflow runs. The first time you switch to the combined list, or after
changing either registry, run `temporal workflow terminate --workflow-id session-manager`, then
restart `serve`. That ends every running agent session, since they are its children.

`docs/` holds research notes and plans (`docs/plans/react-agent-on-agentcore.md` records the
porting decisions; `docs/plans/react-agent-sessions-on-agentcore.md` the activity-sessions variant).
