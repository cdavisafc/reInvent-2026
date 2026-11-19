# ReAct Agent on Bedrock AgentCore, with activity sessions

The [temporal-agent-harness](https://github.com/temporal-community/temporal-agent-harness) ReAct
agent (OpenAI Agents SDK), with its Temporal Worker running as a
[Temporal Serverless Worker](https://docs.temporal.io/serverless-workers) on
[Amazon Bedrock AgentCore Runtime](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime.html),
using **activity sessions**, a prerelease Temporal feature.

The Temporal server invokes the AgentCore runtime when there is work on the `react-agent-sessions`
task queue. The Worker polls until it has been idle for a while, then drains, so the runtime scales
to zero between turns. Activity sessions keep each workflow's model and tool calls on the Worker,
and so in the AgentCore microVM, that runs the workflow.

> ⚠️ **Prerelease.** Activity sessions exist only on the unreleased sdk-python branch
> `dev/sticky-activities`. This project installs a locally built temporalio wheel
> (`1.34.0+sessions`). See [Building the wheels](#building-the-wheels).

## What it does

Ask it a question and it chains tools to find the answer:

| Tool | Source | Purpose |
|------|--------|---------|
| `get_ip_address` | icanhazip.com | The Worker's public IP (here, the AgentCore runtime's) |
| `get_location_info` | ip-api.com | City / country / lat-lon for an IP |
| `get_coordinates` | Open-Meteo Geocoding | Lat-lon for a city name |
| `get_weather` | Open-Meteo Forecast | Current temperature (°F), weather code, wind speed |
| `ask_user` | the human (callback tool) | Ask the user a clarifying question and use their answer |

## Activity sessions

Without sessions, each model call and tool call is an Activity that goes through task-queue
matching. Any Worker polling the queue can pick it up. On AgentCore, every invoke starts a new
microVM, so an Activity often runs in a different microVM from the one running its workflow,
which can mean extra cold starts (see `../docs/research/agentcore-serverless-challenges.md`, items 2
and 3).

With sessions:

- **`ActivitySessionInterceptor`** (`activity_sessions.py`) puts every Activity the workflow starts
  on its own task queue into the workflow's session, so it runs on the *same* Worker as the
  workflow. Activities on other task queues, and local activities, are unaffected. A
  `workflow.activity_session(...)` scope in workflow code takes precedence.
- **`SingleSessionAdmission`** keys the Worker to the AgentCore runtime session
  (`context.session_id`). One runtime session serves exactly one workflow for the Worker's
  lifetime.

The interceptor does the same thing as the `session_options=` argument of upstream
`temporalio-openai-agents` (ai-integrations `feature/activity-sessions`). That plugin isn't used
here. This project keeps the harness's vendored OpenAI Agents plugin, whose streaming hooks feed
the web UI and whose MCP wrapper puts MCP tools under harness approval. See
`../docs/plans/react-agent-sessions-on-agentcore.md`.

## How the pieces run

| Component | Where it runs |
|---|---|
| `ReactAgent` workflow, model activities, tool activities | AgentCore Runtime (`agentcore_worker.py`), started on demand by the Temporal server |
| Session manager (`temporal-agent-harness session-manager`) | Your machine |
| Web UI + HTTP API (`temporal-agent-harness serve agents.toml`) | Your machine |
| Terminal client (`client.py`), needed to answer `ask_user` | Your machine |

The local session manager starts each agent session as a child workflow on the
`react-agent-sessions` task queue. The Worker Deployment `react-agent-sessions-python` routes that
queue to the AgentCore runtime.

`ask_user` parks the turn in the workflow with nothing in flight, so the Worker goes idle and
drains. When you answer, the resulting Update creates a workflow task, and the server invokes the
runtime again.

## Files

| File | Description |
|------|-------------|
| `agentcore_worker.py` | Runtime entry point: a `BedrockAgentCoreApp` that runs the session Worker with the OpenAI Agents + harness plugins, then drains when idle |
| `activity_sessions.py` | `session_config_for()` (AgentCore session → `WorkerSessionConfig`) and `ActivitySessionInterceptor` |
| `workflow.py` | `ReactAgentWorkflow`, the harness agent |
| `tool_activities.py` | The four weather/geo/IP tools, as harness activity tools |
| `human_tools.py` | The `ask_user` callback tool |
| `runtime_secrets.py` | Loads `TEMPORAL_API_KEY` / `OPENAI_API_KEY` from Secrets Manager at Worker start |
| `client.py` | Terminal client: session picker, answers `ask_user`, chat |
| `agents.toml`, `ReactAgent.schema.json` | Harness registry entry for the web UI, and the agent schema it points at |
| `agentcore/agentcore.json` | AgentCore project config: runtime definition, env vars, lifecycle, endpoint |
| `agentcore/aws-targets.json` | AWS account and region to deploy into |
| `secrets-policy.json` | Grants the runtime's execution role read access to the `react-agent-sessions/keys` secret |
| `bin/put-secrets.sh` | Creates/updates that secret from your shell environment |
| `bin/create-runtime.sh` | Creates/updates the runtime with the AgentCore CLI |
| `bin/mk-invoke-role.sh`, `iam-role-for-temporal-agentcore-invoke.yaml` | The role the Temporal server assumes to invoke the runtime |
| `../prerelease/build-local-wheels.sh` | Builds the prerelease temporalio wheels (temporary; lives outside this directory, see below) |

## Prerequisites

- **A Temporal server and namespace** that:
  - supports **Serverless Workers** on AgentCore (Worker Deployment Versions with an AgentCore
    compute config);
  - has **sticky activity execution** (worker sessions) enabled;
  - accepts an **API key** over TLS. This is how the Worker authenticates.

  You also need the **IAM role ARNs of that environment's Serverless Worker invoker**. The invoke
  role must trust them (step 4).
- **Python 3.11+** and [uv](https://docs.astral.sh/uv/).
- **The [AgentCore CLI](https://github.com/aws/agentcore-cli) 1.x or later**, which needs Node.js
  20+: `npm install -g @aws/agentcore@latest`. `agentcore.json` uses the 1.x project format,
  which the v0 CLI can't read.
- **The AWS CLI**, configured, and the
  [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) bootstrapped in the
  target account and region (`cdk bootstrap`).
- **AWS permissions for the AgentCore CLI** (S3, IAM, CloudFormation). See
  [Use the AgentCore CLI](https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/runtime-permissions.html#runtime-permissions-cli).
- **The [Temporal CLI](https://docs.temporal.io/cli)**, for creating the Worker Deployment.
- **An OpenAI API key.** You don't need Bedrock model access, because the agent calls OpenAI.
- **Docker** that can run `linux/arm64` containers (native on Apple silicon). It builds the Linux
  temporalio wheel that gets deployed.
- **A Rust toolchain** (`cargo`), for the macOS temporalio wheel. You only need it for local
  `uv sync` and tests.

## Building the wheels

```bash
../prerelease/build-local-wheels.sh
```

The script clones sdk-python `dev/sticky-activities`, with its sdk-core submodule, into
`../prerelease/.wheel-build/`. Then it builds:

- `../prerelease/wheels/temporalio-1.34.0+sessions-…-manylinux2014_aarch64….whl`, in a manylinux
  container. This is what AgentCore runs.
- `../prerelease/wheels/temporalio-1.34.0+sessions-…-macosx_11_0_arm64.whl`, for local
  development.

`../prerelease/wheels/BUILD_INFO` records the commits it built from. To pin a commit, use
`SDK_PYTHON_REF=<sha> ../prerelease/build-local-wheels.sh`. The first build takes several minutes; later builds reuse cached
Docker volumes.

**Rebuilds and `uv.lock`.** The script is safe to run before every deploy. If
`../prerelease/wheels/BUILD_INFO` already records the SDK branch's current commit and the wheels
exist, it exits without building. `FORCE=1` rebuilds anyway.

When it does build, it re-locks this project. Rust builds aren't byte-for-byte reproducible, so
every build changes the wheels' hashes, and `uv run` / `uv sync` fail with `Hash mismatch` until
`uv.lock` is refreshed. If you see that error (for example, after `NO_RELOCK=1`), run:

```bash
uv lock --no-cache --upgrade-package temporalio && uv sync
```

A plain `uv lock` keeps the old hashes. `--upgrade-package` makes uv re-hash the wheel.
`--no-cache` avoids `uv cache clean`, which blocks for as long as any other `uv run` on your machine
holds the shared cache.

**Why the wheels live outside this directory.** `agentcore deploy` zips this whole directory. It
skips only a fixed list of names (`.git`, `.venv`, `__pycache__`, …) and ignores `.gitignore`. Wheels
and the build scratch directory (over 1 GB) kept in here would end up in the runtime zip.

**How `pyproject.toml` finds them.** Through `override-dependencies` entries, one per platform:
`temporalio @ file://${PROJECT_ROOT}/../prerelease/wheels/… ; sys_platform == '…'`.

- An override is needed because temporal-agent-harness declares `temporalio>=1.35.0`.
- A `[tool.uv.sources]` path source isn't enough. In `uv pip install`, an override drops the path
  source, and you get PyPI's temporalio instead.
- The URL form works for `uv lock` / `uv sync`, and for the install `agentcore deploy` runs:
  `uv pip install -r pyproject.toml --python-platform aarch64-manylinux2014 --only-binary :all:`.

**Remove when activity sessions are released:** delete `../prerelease/` (script, wheels, scratch
dir) and the `override-dependencies` entry in `pyproject.toml`. Then raise the `temporalio` floor
to the release.

## Setup

### 1. Store the API keys in Secrets Manager

Secrets stay out of `agentcore.json`, and so out of git. The Worker reads them when it starts, from
the secret named by `REACT_AGENT_SECRET_ID` (default `react-agent-sessions/keys`).

```bash
export TEMPORAL_API_KEY=<api-key-for-your-temporal-server>
export OPENAI_API_KEY=<your-openai-api-key>
./bin/put-secrets.sh
```

This needs AWS credentials in your shell (check with `aws sts get-caller-identity`). The script
creates the secret in the region of the `default` target in `agentcore/aws-targets.json`, which is
the region the runtime deploys to. To use a different target, pass it as the second argument:
`./bin/put-secrets.sh react-agent-sessions/keys <target>`.

### 2. Configure the runtime

- **`agentcore/aws-targets.json`:** the AWS account and region.
- **`envVars` in `agentcore/agentcore.json`:**
  - `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE`: your Temporal server and namespace.
  - `TEMPORAL_DEPLOYMENT_NAME`, `TEMPORAL_BUILD_ID`, `REACT_AGENT_TASK_QUEUE`: change these only if
    you rename things. Step 5 has to match.
  - `AGENTCORE_DEBOUNCE_SECONDS`: how long the Worker keeps polling after it goes idle.
  - `REACT_AGENT_STREAM=0`: switches the agent to non-streaming.
- **`lifecycleConfiguration.idleRuntimeSessionTimeout`** is 60s, the minimum. AgentCore counts a
  session as busy while the Worker polls and drains. So this only decides how long the microVM
  waits, after the Worker finishes, before AgentCore ends it. The default of 15 minutes leaves idle
  microVMs running.

### 3. Build the wheels and deploy the runtime

```bash
../prerelease/build-local-wheels.sh
./bin/create-runtime.sh
```

This deploys the runtime `ReactAgentSessions_default_temporal_worker`, in the CloudFormation stack
`AgentCore-ReactAgentSessions-default`. Export the region and the runtime ARN for the next steps:

```bash
export AWS_REGION=$(python3 -c 'import json; print(json.load(open("agentcore/aws-targets.json"))[0]["region"])')
export RUNTIME_ARN=$(aws cloudformation describe-stacks \
  --stack-name AgentCore-ReactAgentSessions-default \
  --query 'Stacks[0].Outputs[?starts_with(OutputKey, `ApplicationRuntimeTemporalWorkerRuntimeArnOutput`)].OutputValue' \
  --output text)
echo "$RUNTIME_ARN"
```

The output key has a generated suffix, so the query matches it by prefix. The ARN looks like
`arn:aws:bedrock-agentcore:<region>:<account>:runtime/ReactAgentSessions_default_temporal_worker-<id>`.

**Rolling out code changes.** Each deploy that changes the code creates a new runtime version.
The server invokes AgentCore's built-in `DEFAULT` endpoint (step 5), which always serves the latest
version, so one `./bin/create-runtime.sh` rolls out new code.

The trade-off is that build ID `1.0.0` then means "whatever was deployed last". If you change
workflow code under sessions that are already running with the PINNED versioning behavior, they can
hit non-determinism errors. That's fine for development. For a release, use a new build ID pointed
at a pinned endpoint. `agentcore.json` defines a `temporal` endpoint, pinned to
`endpoints.temporal.version`, for that purpose; it isn't used by default.

### 4. Create the invoke role

The Temporal server assumes this role to invoke the runtime. The role only trusts the IAM roles
that your Temporal environment's Serverless Worker invoker runs as, and those differ per
environment:

| Temporal environment | `TEMPORAL_INVOKER_ROLE_ARNS` |
|---|---|
| Staging cell (`*.tmprl-test.cloud`) | `arn:aws:iam::031568301006:role/wci-lambda-invoke,arn:aws:iam::815250390068:role/wci-lambda-invoke` |
| Temporal Cloud production (`*.tmprl.cloud`) | Leave unset; the template's default |

For another environment, ask whoever runs it for its invoker role ARNs. If the role doesn't trust
the right ones, `temporal worker deployment create-version` fails with an auth error, or the server
never invokes the runtime.

```bash
export TEMPORAL_INVOKER_ROLE_ARNS="arn:aws:iam::031568301006:role/wci-lambda-invoke,arn:aws:iam::815250390068:role/wci-lambda-invoke"
export EXTERNAL_ID=$(uuidgen)
./bin/mk-invoke-role.sh react-agent-sessions-invoke "$EXTERNAL_ID" "${RUNTIME_ARN}*" "$TEMPORAL_INVOKER_ROLE_ARNS"

export ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name react-agent-sessions-invoke \
  --query 'Stacks[0].Outputs[?OutputKey==`RoleARN`].OutputValue' \
  --output text)
echo "$ROLE_ARN"
```

`mk-invoke-role.sh` creates the stack, or updates it in place if it already exists, and waits until
CloudFormation finishes. Re-running it with new invoker ARNs, or a new external ID, updates the
existing role; its ARN doesn't change.

The template names the role `Temporal-Cloud-Serverless-Worker-<stack-name>`, and IAM caps role
names at 64 characters, so keep the stack name to 31 characters or fewer.

The trailing `*` on the runtime ARN makes the role cover the runtime's endpoints too.

Keep `EXTERNAL_ID` for the next step; the Worker Deployment Version must pass the same value. If
you open a new shell later, `EXTERNAL_ID` is the stack's `AssumeRoleExternalId` parameter.

### 5. Create the Worker Deployment Version

Point the `temporal` CLI at your server and namespace (`TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` /
`TEMPORAL_API_KEY`, or a `temporal.toml` profile). With `RUNTIME_ARN`, `ROLE_ARN` and
`EXTERNAL_ID` from steps 3 and 4 still exported:

```bash
temporal worker deployment create --name react-agent-sessions-python

temporal worker deployment create-version \
  --deployment-name react-agent-sessions-python --build-id 1.0.0 \
  --aws-agentcore-endpoint-arn "${RUNTIME_ARN}/runtime-endpoint/DEFAULT" \
  --aws-agentcore-assume-role-arn "$ROLE_ARN" \
  --aws-agentcore-assume-role-external-id "$EXTERNAL_ID"

temporal worker deployment set-current-version \
  --deployment-name react-agent-sessions-python --build-id 1.0.0 --allow-no-pollers
```

Don't skip `set-current-version`: until a version is current, nothing routes to the runtime.

To move an existing version to another endpoint, run
`temporal worker deployment update-version-compute-config` with the same three `--aws-agentcore-*`
flags.

### 6. Run the local pieces

The harness CLI reads the same `TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE` / `TEMPORAL_API_KEY`, or a
`temporal.toml` profile. Run each of these in its own terminal:

```bash
uv run temporal-agent-harness session-manager
uv run temporal-agent-harness serve agents.toml --port 8000
uv run python client.py
```

Open http://localhost:8000 and pick **ReAct Agent (activity sessions)**, or use the terminal client.
Try *"What's the weather in Tokyo?"*, which chains the tools. Then try *"What's the weather?"*. It's
ambiguous, so the agent calls `ask_user`, and only the terminal client can answer it.

### Watching it run

Follow the Worker's logs:

```bash
agentcore logs runtime --name temporal_worker --qualifier DEFAULT --tail
```

Logs are kept per endpoint, so the qualifier must match the endpoint the server invokes. Each
Worker logs `polling react-agent-sessions as … (session <id>)` when it starts. With sessions
working, all of a workflow's activities run under that one session ID.

To see the AgentCore Workers polling, select the build ID. Without it, `describe` shows only the
unversioned queue:

```bash
temporal task-queue describe --task-queue react-agent-sessions \
  --select-build-id react-agent-sessions-python:1.0.0
```

To count running microVMs, use the CloudWatch `ActiveSession` metric (statistic `Maximum`, 1-minute
period). No CLI command lists runtime sessions.

## Development

```bash
../prerelease/build-local-wheels.sh   # builds only if the SDK branch moved; re-locks after a build
uv sync
uv run pytest
```

If you change the agent's handlers, regenerate the schema. A test fails until you do.

```bash
uv run temporal-agent-harness schema workflow:ReactAgentWorkflow -o ReactAgent.schema.json
```

To run the Worker locally, use a server that supports worker sessions. Leave `REACT_AGENT_SECRET_ID`
unset and export the keys directly. With no session ID, the Worker serves sessions without
single-session admission.

```bash
OPENAI_API_KEY=... uv run python -c 'import asyncio, agentcore_worker as w; asyncio.run(w.run_worker(None))'
```

## Notes

- **Sessions and `ask_user`.** When the agent parks on `ask_user`, the Worker drains and its
  AgentCore session ends. Your answer triggers a new invoke, which gets a new AgentCore session ID
  and so a new single-session Worker. The workflow has to move to that Worker. Verify this on
  the first deploy.
- **Subagents.** A `SingleSessionAdmission` Worker serves exactly one workflow. A harness subagent
  (a child workflow) on the same task queue therefore needs its own invoke and Worker.
- **Fixed activity slots.** Session Workers need a fixed-size activity slot supplier, hence
  `max_concurrent_activities=ACTIVITY_SLOTS`.
- **Large payloads.** The Worker turns off the harness's large-payload offload. The default would
  write to the runtime's ephemeral disk, which the local session manager can't read. A payload over
  Temporal's size limit fails loudly instead. If you need offload, use the harness's S3 storage on
  every Worker, server and client.
- **`ActivityTracker`** is duplicated across this repo's examples on purpose. Each example is
  zipped and deployed on its own.
