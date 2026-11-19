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

Each example is a self-contained uv project and AgentCore project. The CodeZip build zips the
example's directory, so run its scripts from inside it.

`docs/` holds research notes and plans (`docs/plans/react-agent-on-agentcore.md` records the
porting decisions; `docs/plans/react-agent-sessions-on-agentcore.md` the activity-sessions variant).
