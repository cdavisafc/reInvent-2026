# Order support: harness agents on Bedrock AgentCore, with subagents

A concierge agent built on the OpenAI Agents SDK hands work to a shipping specialist built on
Strands (Claude on Bedrock). Each agent is a
[temporal-agent-harness](https://github.com/temporal-community/temporal-agent-harness) workflow on
its own task queue. Each one's Temporal Worker runs as a
[Temporal Serverless Worker](https://docs.temporal.io/serverless-workers) on its own
[Amazon Bedrock AgentCore Runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime.html),
with **activity sessions** (a prerelease Temporal feature).

> **Status.** These are the spike's agents (`spike/`): a minimal concierge and a shipping
> specialist with two stub tools. The full demo is planned in `../docs/plans/order-support-demo.md`;
> the spike's findings are in `../docs/research/order-support-subagent-spike.md`.

## How the pieces run

| Component | Task queue | Where it runs |
|---|---|---|
| `SpikeConciergeAgent` (OpenAI Agents SDK, `gpt-5.6-luna`) | `spike-concierge` | AgentCore runtime `concierge`, Worker Deployment `order-support-concierge` |
| `SpikeShippingAgent` (Strands, Claude Opus 5.5 on Bedrock) | `spike-shipping` | AgentCore runtime `shipping`, Worker Deployment `order-support-shipping` |
| Session manager, web UI + HTTP API (`temporal-agent-harness session-manager` / `serve`) | | Your machine |

The concierge calls the shipping specialist as a harness subagent (`agent.subagent_toolset`), which
starts it as a child workflow on `spike-shipping`. Because that's a different Worker Deployment,
the Temporal server invokes the shipping runtime separately, and the child runs in its own microVM.
While the child works, the concierge's `run_subagent_turn` Activity is in flight, so the concierge
runtime stays up too.

Both runtimes deploy the same code (`agentcore_worker.py`). The `ORDER_SUPPORT_AGENT` env var in
`agentcore/agentcore.json` picks which agent a runtime hosts (`spike/hosted_agents.py`).

## Files

| File | Description |
|------|-------------|
| `agentcore_worker.py` | Runtime entry point: runs one agent's session Worker, then drains when idle |
| `spike/hosted_agents.py` | The agents a Worker can host: task queue, workflow, plugins, required secrets |
| `spike/worker.py` | Runs both Workers locally, in one process |
| `spike/concierge_workflow.py`, `spike/shipping_workflow.py` | The two agents |
| `spike/contracts.py` | The stub class the concierge reflects on to call the shipping agent |
| `spike/shipping_tools.py`, `human_tools.py` | The agents' tools |
| `spike/keyword_tools.py` | Workaround for a harness bug in `subagent_toolset` (see the research note) |
| `subagent_signals.py` | Local fix: the concierge waits for a subagent's reply signal in its workflow instead of in an activity, so its Worker can drain (see Notes) |
| `spike/agents.toml`, `spike/schemas/` | Harness registry for the web UI, and the schemas it points at |
| `activity_sessions.py` | `session_config_for()` and `ActivitySessionInterceptor` |
| `runtime_secrets.py` | Loads the API keys from Secrets Manager when a Worker starts |
| `session_heartbeat.py` | The AgentCore session's liveness heartbeat, written to the shared table in `../agent_status/` (a copy of `../react_agent_sessions/session_heartbeat.py`) |
| `agentcore/agentcore.json`, `agentcore/aws-targets.json` | AgentCore project: the two runtimes, and the AWS account and region |
| `secrets-policy.json` | Grants both runtimes read access to the `order-support/keys` secret |
| `session-heartbeat-policy.json` | Grants both runtimes write access to the shared heartbeat table |
| `iam-role-for-temporal-agentcore-invoke.yaml` | The role the Temporal server assumes to invoke the runtimes |
| `bin/put-secrets.sh` | Creates/updates the `order-support/keys` secret from your shell |
| `bin/create-runtime.sh` | Deploys both runtimes with the AgentCore CLI |
| `bin/runtime-arns.sh` | Lists each runtime's ARN, Worker Deployment and build ID |
| `bin/mk-invoke-role.sh` | Creates/updates the invoke role |
| `bin/create-worker-deployments.sh` | Creates a Worker Deployment Version per runtime and makes it current |

## Prerequisites

The same as `../react_agent_sessions/README.md#prerequisites`: a Temporal namespace with Serverless
Workers and activity sessions, the AgentCore CLI 1.x, the AWS CLI with CDK bootstrapped, the
Temporal CLI, Docker, uv, and the prerelease temporalio wheels
(`../prerelease/build-local-wheels.sh`). Also:

- **An OpenAI API key**, for the concierge.
- **Bedrock model access** to Claude Opus 5.5 (`global.anthropic.claude-opus-5-5`) in the target
  account, for the shipping specialist. The runtime's execution role, which `agentcore deploy`
  creates, already allows `bedrock:InvokeModel*`.

## Run locally

With `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_API_KEY`, `OPENAI_API_KEY`, AWS
credentials and `AWS_REGION` exported, run each in its own terminal:

```bash
uv run python -m spike.worker                                  # both Workers, sessions on
uv run temporal-agent-harness session-manager
uv run temporal-agent-harness serve spike/agents.toml --port 8000
```

Then use the agent kit's UI on :5173 (see `../react_agent_sessions/README.md`).

To list the ReAct agent in the same UI, pass both registries to `serve`; see
[Running the demo with every agent](../README.md#running-the-demo-with-every-agent).

`serve` keeps its agent list in the running `session-manager` workflow. After changing
`spike/agents.toml`, run `temporal workflow terminate --workflow-id session-manager` (this ends
running agent sessions too), then restart `serve`.

## Deploy to AgentCore

Run these from this directory, with AWS credentials in your shell.

### 1. Store the API keys and create the heartbeat table

```bash
export TEMPORAL_API_KEY=<api-key-for-your-temporal-server>
export OPENAI_API_KEY=<your-openai-api-key>
./bin/put-secrets.sh
```

Both runtimes read the secret `order-support/keys`. Each Worker loads only the keys its agent
needs: the shipping Worker needs only `TEMPORAL_API_KEY`.

Then create the DynamoDB table every agent's AgentCore sessions write their liveness heartbeat to,
if it doesn't exist yet (for example from `../react_agent_sessions`):

```bash
AWS_REGION=us-west-2 ../agent_status/bin/mk-heartbeat-table.sh
```

Both runtimes write to it (`SESSION_HEARTBEAT_TABLE` in `agentcore.json`) through
`session-heartbeat-policy.json`.

### 2. Configure and deploy the runtimes

Check `agentcore/aws-targets.json` (account and region) and each runtime's `envVars` in
`agentcore/agentcore.json` (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`). Then:

```bash
./bin/create-runtime.sh
./bin/runtime-arns.sh
```

The deploy installs the Linux wheel from `../prerelease/wheels/`, so it must exist; build it once
with `../prerelease/build-local-wheels.sh`. A rebuild re-locks only `../react_agent_sessions`. After
one, also run `uv lock --no-cache --upgrade-package temporalio && uv sync` here, or `uv` fails with
`Hash mismatch`.

`create-runtime.sh` deploys both runtimes in the CloudFormation stack `AgentCore-OrderSupport-default`.
`runtime-arns.sh` prints each runtime's ARN, Worker Deployment and build ID.

### 3. Create the invoke role

One role covers both runtimes. The invoker ARNs below are the staging cell's; see
`../react_agent_sessions/README.md#4-create-the-invoke-role` for other environments.

```bash
export AWS_REGION=$(python3 -c 'import json; print(json.load(open("agentcore/aws-targets.json"))[0]["region"])')
export TEMPORAL_INVOKER_ROLE_ARNS="arn:aws:iam::031568301006:role/wci-lambda-invoke,arn:aws:iam::815250390068:role/wci-lambda-invoke"
export EXTERNAL_ID=$(uuidgen)
RUNTIME_ARNS=$(./bin/runtime-arns.sh | awk -F'\t' '{ printf "%s%s*", sep, $2; sep = "," }')
./bin/mk-invoke-role.sh order-support-invoke "$EXTERNAL_ID" "$RUNTIME_ARNS" "$TEMPORAL_INVOKER_ROLE_ARNS"

export ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name order-support-invoke \
  --query 'Stacks[0].Outputs[?OutputKey==`RoleARN`].OutputValue' \
  --output text)
```

The trailing `*` on each ARN covers the runtime's endpoints. Keep `EXTERNAL_ID`; it's also the
stack's `AssumeRoleExternalId` parameter if you open a new shell.

### 4. Create the Worker Deployments

With the Temporal CLI pointed at your namespace, and `ROLE_ARN` and `EXTERNAL_ID` exported:

```bash
./bin/create-worker-deployments.sh
```

For each runtime it creates the Worker Deployment (`order-support-concierge`,
`order-support-shipping`), creates or updates version `1.0.0` pointing at the runtime's `DEFAULT`
endpoint, and makes it current. Re-running it is safe.

### 5. Run it

**Stop the local `spike.worker` first**, or it competes with AgentCore for the same task queues. Keep
the session manager and `serve` running locally, as above. Start new sessions in the UI.

Follow each runtime's logs:

```bash
agentcore logs runtime --name concierge --qualifier DEFAULT --tail
agentcore logs runtime --name shipping --qualifier DEFAULT --tail
```

Each Worker logs `<agent> polling <queue> as <deployment>/<build> (session <id>)` when it starts.

To watch each agent's sessions come and go, run the [status page](../agent_status/README.md). Its
`agents.toml` lists Concierge and Shipping:

```bash
cd ../agent_status && uv run python status_page.py      # http://127.0.0.1:8100
```

To see the AgentCore Workers polling:

```bash
temporal task-queue describe --task-queue spike-shipping \
  --select-build-id order-support-shipping:1.0.0
```

**Rolling out code changes.** Re-run `./bin/create-runtime.sh`. The server invokes each runtime's
`DEFAULT` endpoint, which serves the latest version. Build ID `1.0.0` then means "whatever was
deployed last", so a workflow change can break running sessions (they're PINNED). Start new
sessions after a deploy.

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
```

If you change an agent's handlers, regenerate its schema:

```bash
uv run temporal-agent-harness schema spike.concierge_workflow:SpikeConciergeAgentWorkflow \
  -o spike/schemas/SpikeConciergeAgent.schema.json
uv run temporal-agent-harness schema spike.shipping_workflow:SpikeShippingAgentWorkflow \
  -o spike/schemas/SpikeShippingAgent.schema.json
```

## Notes

- **Approvals inside the subagent.** While the shipping agent waits for an approval, neither
  Worker has anything in flight, so both drain after `AGENTCORE_DEBOUNCE_SECONDS` and both microVMs
  end. The approval invokes the shipping runtime. Shipping's reply signal then invokes the
  concierge's, which finishes the answer. The concierge waits for that reply for up to 24 hours.
  - This relies on `subagent_signals.py`, a local fix. The harness's own subagent turn holds an
    activity open on the parent for the whole wait. The proposed upstream fix is in
    `../docs/research/agent-kit-issue-subagent-turn-holds-parent-worker.md`.
  - A subagent signals its parent after every message it handles. So messaging the nested Shipping
    agent directly in the UI also invokes the concierge briefly, which ignores that reply.
- **Large payloads.** Both Workers turn off the harness's large-payload offload, locally too. The
  default would write to the runtime's ephemeral disk, which the local session manager can't read.
