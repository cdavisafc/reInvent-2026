# ReAct agent on AgentCore with activity sessions (`react_agent_sessions/`)

Started 2026-11-17.

## Goal

Run the `react_agent/` harness ReAct agent on a second AgentCore runtime with prerelease
**activity sessions**:

- every Activity a workflow starts runs on the Worker that holds the workflow, so model and tool
  calls stay in the same AgentCore microVM;
- each Worker is keyed to its AgentCore runtime session with `SingleSessionAdmission`, so one
  microVM serves one workflow.

This targets the cross-microVM activity routing in `docs/research/agentcore-serverless-challenges.md`
(items 2 and 3).

Prerelease sources:

- sdk-python `dev/sticky-activities`: `ActivitySessionOptions`, `UseWorkflowSession`,
  `WorkerSessionConfig`, `SingleSessionAdmission`, `workflow.activity_session(...)`,
  `StartActivityInput.session_options`.
- ai-integrations `feature/activity-sessions`: `OpenAIAgentsPlugin(session_options=...)`,
  implemented as a ~57-line workflow outbound interceptor.
- Reference sample: samples-python `feature/activity-sessions`, `bedrock_agentcore/`
  (`build-local-wheels.sh`, `openai_agent/`).

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Which OpenAI Agents plugin | Keep temporal-agent-harness's vendored copy. Reproduce `session_options` with our own interceptor (`activity_sessions.py`). Don't use the ai-integrations plugin wheel | The harness copy has streaming hooks (`observer_factory`, `stream_to_provider`) that feed the web UI's live turn stream. Its runner also requires MCP servers wrapped by `as_harness_mcp_server()` (approval policy plus `tool_start`/`tool_end`/`tool_error` events). Upstream's runner rejects those wrapped servers, and MCP is planned for this agent |
| How temporalio is installed | Locally built wheels, Linux aarch64 (Docker manylinux2014) plus macOS arm64, version `1.34.0+sessions` (built by `prerelease/build-local-wheels.sh`, adapted from the samples-python script). The script and its outputs live outside `react_agent_sessions/`, because `agentcore deploy` zips the project directory, skipping only a fixed list of names and not `.gitignore` | `agentcore deploy` installs with `uv pip install -r pyproject.toml --only-binary :all: --python-platform aarch64-manylinux2014`, so git sources can't work. temporalio has a Rust extension |
| How pyproject points at the wheels | Marker-split direct-URL overrides: `override-dependencies = ["temporalio @ file://${PROJECT_ROOT}/../prerelease/wheels/<linux>.whl ; sys_platform == 'linux'", "… <macos>.whl ; sys_platform == 'darwin'"]`. No `[tool.uv.sources]` | The harness declares `temporalio>=1.35.0`, and the sessions branch (84 commits behind main) is still 1.34.0, so an override is required. Tested 2026-11-17: in `uv pip install`, a version override (`==1.34.0+sessions` or `>=1.34.0`) drops the path source. It either fails to resolve or silently installs PyPI's 1.36.1. The URL override works for `uv lock`, `uv sync`, and the AgentCore install, and keeps the harness's `[opentelemetry]` extra deps |
| Session admission | `SingleSessionAdmission(stable_identity=context.session_id)`; plain `WorkerSessionConfig()` with no session ID (local runs) | Same as the reference sample. AgentCore session = workflow session |
| Activity slots | `max_concurrent_activities=4` | Session Workers require a fixed-size activity slot supplier |
| Names | Task queue `react-agent-sessions`; deployment `react-agent-sessions-python`; AgentCore project `ReactAgentSessions` with runtime `temporal_worker`; web UI key `react-agent-sessions` | Can run alongside `react_agent` in the same AWS account. Workflow type `ReactAgent` is shared. The AgentCore physical name `<project>_<target>_<runtime>` is capped at 48 characters, so the runtime name is short: `ReactAgentSessions_default_temporal_worker` is 42 |
| Temporal server | A different server and namespace from `react_agent`'s Temporal Cloud namespace (2026-11-17). Same auth model: API key + TLS. The server invokes the AgentCore runtime itself (Serverless Workers) | Needs a server with sticky activity execution |
| Secret | Its own: `react-agent-sessions/keys` (`TEMPORAL_API_KEY` for the new server, `OPENAI_API_KEY`) | A different server needs a different API key. Sharing `react-agent/keys` would break `react_agent` |

## Findings: vendored plugin vs upstream (2026-11-17)

Harness 0.11.0 vendors the OpenAI Agents integration at
`temporal_agent_harness.ai_sdks.openai_agents`, and the two have diverged:

- **Only in the harness copy:** `observer_factory` and `stream_to_provider` (streaming into the
  harness turn vocabulary); a harness-MCP check in `_openai_runner.py`; `_nexus_mcp.py` (Durable
  Tools Gateway).
- **Only upstream:** `session_options`, `mcp_servers=` / `mcp_connection_idle_timeout` (MCP v2
  backend), `resolvable_worker_env_vars`, `session.py`, a model provider module. Upstream also
  checks MCP durability by its own types, which rejects harness-wrapped servers.

**For the harness maintainers:** the next sync with upstream should pick up `session_options`, and
upstream would ideally gain hooks the harness can plug into (an observer factory, an MCP wrapper).
Then `activity_sessions.ActivitySessionInterceptor` can be deleted.

## Findings: invoke role trust (2026-11-18)

The target server is in Temporal's **staging cell** (`*.tmprl-test.cloud`). Its Serverless Worker
invoker assumes the customer's invoke role from
`arn:aws:iam::031568301006:role/wci-lambda-invoke` and
`arn:aws:iam::815250390068:role/wci-lambda-invoke`. Production uses five other accounts.

The invoke role template, copied from `react_agent`, hard-coded production's principals. So:

- the server couldn't assume the role;
- the bootstrap invoke after `create-version` never reached the runtime, leaving the version's
  `taskQueuesInfos` empty and the agent's workflow task stuck;
- later, `create-version` failed with an auth error.

**Fix:** the template takes a `TemporalInvokerRoleARNs` parameter (default: production), and
`bin/mk-invoke-role.sh` takes it as an optional fourth argument and creates or updates the stack in
place.

## Risks and open items

- **Server support.** The target server/namespace must support sticky activity execution / worker
  sessions and Serverless Worker invocation of AgentCore.
- **Resuming after `ask_user`.** The Worker drains while the agent waits for an answer. The answer
  triggers a new invoke, so a new AgentCore session ID, and the new Worker uses a different
  `stable_identity`. The workflow has to rebind to it. Verify on the first deploy.
- **Harness on a 1.34.0-based SDK.** The harness declares `>=1.35.0` for a reason. Check with the
  tests and an end-to-end run.
- **Subagents.** A single-session Worker serves one workflow, so a harness subagent (a child
  workflow on the same queue) needs its own invoke. Not used yet.
- **`maxLifetime` (8h)** caps one workflow session's microVM.

## Removing the prerelease scaffolding

Once activity sessions ship in temporalio, and the harness picks up `session_options`:

1. Delete `prerelease/` (script, wheels, scratch dir) and the `override-dependencies`
   entry.
2. Raise the `temporalio` floor.
3. Replace `ActivitySessionInterceptor` with the plugin's `session_options=` argument, once the
   harness supports it.
