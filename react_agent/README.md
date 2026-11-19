# ReAct Agent on Bedrock AgentCore

The [temporal-agent-harness](https://github.com/temporal-community/temporal-agent-harness) ReAct
agent (OpenAI Agents SDK), with its Temporal Worker running as a
[Temporal Serverless Worker](https://docs.temporal.io/serverless-workers) on
[Amazon Bedrock AgentCore Runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime.html).

It's ported from the harness repo's `python/examples/react_agent`. The agent code is the same.
What changed is where the Worker runs: Temporal Cloud invokes the AgentCore runtime when there is
work on the `react-agent` task queue. The Worker polls until it has been idle for a while, then
drains, so the runtime scales to zero between turns.

## What it does

Ask it a question and it chains tools to find the answer:

| Tool | Source | Purpose |
|------|--------|---------|
| `get_ip_address` | icanhazip.com | The Worker's public IP (here, the AgentCore runtime's) |
| `get_location_info` | ip-api.com | City / country / lat-lon for an IP |
| `get_coordinates` | Open-Meteo Geocoding | Lat-lon for a city name |
| `get_weather` | Open-Meteo Forecast | Current temperature (°F), weather code, wind speed |
| `ask_user` | the human (callback tool) | Ask the user a clarifying question and use their answer |

The source example's F1 MCP server is **not** included yet. It's a Node.js stdio server, and the
CodeZip Python runtime has no Node. See `../docs/plans/react-agent-on-agentcore.md`.

## How the pieces run

| Component | Where it runs |
|---|---|
| `ReactAgent` workflow, model activities, tool activities | AgentCore Runtime (`agentcore_worker.py`), started on demand by Temporal Cloud |
| Session manager (`temporal-agent-harness session-manager`) | Your machine |
| Web UI + HTTP API (`temporal-agent-harness serve agents.toml`) | Your machine |
| Terminal client (`client.py`), needed to answer `ask_user` | Your machine |

The local session manager starts each agent session as a child workflow on the `react-agent` task
queue. The Worker Deployment routes that queue to the AgentCore runtime.

`ask_user` parks the turn in the workflow with nothing in flight, so the Worker goes idle and
drains. When you answer, the resulting Update creates a workflow task, and Temporal Cloud invokes
the runtime again.

## Files

| File | Description |
|------|-------------|
| `agentcore_worker.py` | Runtime entry point: a `BedrockAgentCoreApp` that runs the Worker with the OpenAI Agents + harness plugins, then drains when idle |
| `workflow.py` | `ReactAgentWorkflow`, the harness agent |
| `tool_activities.py` | The four weather/geo/IP tools, as harness activity tools |
| `human_tools.py` | The `ask_user` callback tool |
| `runtime_secrets.py` | Loads `TEMPORAL_API_KEY` / `OPENAI_API_KEY` from Secrets Manager at Worker start |
| `client.py` | Terminal client: session picker, answers `ask_user`, chat |
| `agents.toml`, `ReactAgent.schema.json` | Harness registry entry for the web UI, and the agent schema it points at |
| `agentcore/agentcore.json` | AgentCore project config: runtime definition, env vars, lifecycle, endpoint |
| `agentcore/aws-targets.json` | AWS account and region to deploy into |
| `secrets-policy.json` | Grants the runtime's execution role read access to the `react-agent/keys` secret |
| `bin/put-secrets.sh` | Creates/updates that secret from your shell environment |
| `bin/create-runtime.sh` | Creates/updates the runtime with the AgentCore CLI |
| `bin/mk-invoke-role.sh`, `iam-role-for-temporal-agentcore-invoke.yaml` | The role Temporal Cloud assumes to invoke the runtime |

## Prerequisites

The same as `../strands_agent/README.md` (Temporal Cloud namespace, uv, AgentCore CLI 1.x, AWS CLI,
bootstrapped CDK), plus an **OpenAI API key**. Bedrock model access isn't needed: the agent calls
OpenAI.

## Setup

### 1. Store the API keys in Secrets Manager

Secrets stay out of `agentcore.json`, and so out of git. The Worker reads them from the secret
named by `REACT_AGENT_SECRET_ID` (default `react-agent/keys`) when it starts.

```bash
export TEMPORAL_API_KEY=<your-temporal-cloud-api-key>
export OPENAI_API_KEY=<your-openai-api-key>
./bin/put-secrets.sh
```

This needs AWS credentials in your shell (check with `aws sts get-caller-identity`). The script
creates the secret in the region of the `default` target in `agentcore/aws-targets.json`, the
same region the runtime deploys to. Pass a different target as the second argument:
`./bin/put-secrets.sh react-agent/keys <target>`.

### 2. Configure the runtime

- `agentcore/aws-targets.json`: the account and region.
- `envVars` in `agentcore/agentcore.json`: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and if you
  change them, `TEMPORAL_DEPLOYMENT_NAME` / `TEMPORAL_BUILD_ID` / `REACT_AGENT_TASK_QUEUE`.
  `AGENTCORE_DEBOUNCE_SECONDS` sets how long the Worker keeps polling after it goes idle.
  `REACT_AGENT_STREAM=0` switches the agent to non-streaming.
- `lifecycleConfiguration.idleRuntimeSessionTimeout` is 60s, the minimum. AgentCore counts a
  session as busy while the Worker polls and drains, so this only decides how long the
  microVM waits after the Worker finishes before AgentCore ends it. The default, 15 minutes,
  leaves idle microVMs running.

### 3. Deploy the runtime

```bash
./bin/create-runtime.sh
```

This deploys the runtime `ReactAgent_default_temporal_react_worker`. The deploy's stack outputs
give you the runtime ARN.

**Rolling out code changes.** Each deploy that changes the code creates a new runtime version.
Temporal invokes AgentCore's built-in `DEFAULT` endpoint (`<runtime-arn>/runtime-endpoint/DEFAULT`,
step 5), which always serves the latest version, so one `./bin/create-runtime.sh` rolls out new
code. The trade-off: build ID `1.0.0` then means "whatever was deployed last". Sessions already
running under the PINNED versioning behavior can hit non-determinism errors if you change workflow
code under them. That's fine for development. For a release, use a new build ID pointed at a
pinned endpoint.

`agentcore.json` also defines a `temporal` endpoint pinned to `endpoints.temporal.version`. It
isn't used by default. It's there for pinned, per-build-ID rollouts.

### 4. Create the Temporal Cloud invoke role

```bash
EXTERNAL_ID=$(uuidgen)
./bin/mk-invoke-role.sh react-agent-invoke-role "$EXTERNAL_ID" '<agent-runtime-arn>*'
```

The trailing `*` makes the role cover the runtime's endpoints too. The stack's `RoleARN` output is
the invoke role for the next step.

### 5. Create the Worker Deployment Version

```bash
temporal worker deployment create --name react-agent-python

temporal worker deployment create-version \
  --deployment-name react-agent-python --build-id 1.0.0 \
  --aws-agentcore-endpoint-arn "<agent-runtime-arn>/runtime-endpoint/DEFAULT" \
  --aws-agentcore-assume-role-arn <InvokeRoleArn> \
  --aws-agentcore-assume-role-external-id "$EXTERNAL_ID"

temporal worker deployment set-current-version \
  --deployment-name react-agent-python --build-id 1.0.0 --allow-no-pollers
```

Don't skip `set-current-version`: until a version is current, nothing routes to the runtime.

To move an existing version to another endpoint, use the same three `--aws-agentcore-*` flags
with `temporal worker deployment update-version-compute-config`.

### 6. Run the local pieces

Point the harness CLI at the same namespace. It reads `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` /
`TEMPORAL_API_KEY`, or a `temporal.toml` profile. Then run each of these in its own terminal:

```bash
uv run temporal-agent-harness session-manager
uv run temporal-agent-harness serve agents.toml --port 8000
uv run python client.py
```

Open http://localhost:8000 and pick **ReAct Agent**, or use the terminal client. Try *"What's the
weather in Tokyo?"* (the tools chain), then *"What's the weather?"*. The second is ambiguous, so
the agent calls `ask_user`. Only the terminal client can answer it.

Follow the Worker with
`agentcore logs runtime --name temporal_react_worker --qualifier DEFAULT --tail`. Logs are kept
per endpoint, so the qualifier must match the endpoint Temporal invokes.

To see the AgentCore Workers polling, select the build ID. Without it, `describe` shows only the
unversioned queue:

```bash
temporal task-queue describe --task-queue react-agent \
  --select-build-id react-agent-python:1.0.0
```

Count running microVMs with the CloudWatch `ActiveSession` metric (statistic `Maximum`, 1-minute
period). There's no CLI command that lists runtime sessions.

## Development

```bash
uv sync
uv run pytest
```

If you change the agent's handlers, regenerate the schema (a test fails until you do):

```bash
uv run temporal-agent-harness schema workflow:ReactAgentWorkflow -o ReactAgent.schema.json
```

To run the Worker locally against a dev server, leave `REACT_AGENT_SECRET_ID` unset and export
the keys directly:

```bash
OPENAI_API_KEY=... uv run python -c 'import asyncio, agentcore_worker as w; asyncio.run(w.run_worker())'
```

## Notes

- **Large payloads.** The Worker turns off the harness's large-payload offload. The default would
  write to the runtime's ephemeral disk, where the local server can't read it. A payload over
  Temporal's limit fails loudly instead. If you need offload, use the harness's S3 storage on
  every Worker, server and client.
- **`ActivityTracker`** is duplicated from `strands_agent/` on purpose. Each example is zipped and
  deployed on its own.
