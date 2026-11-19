# Plan: ReAct agent on AgentCore Runtime (serverless Temporal worker)

Port `temporal-agent-harness/python/examples/react_agent` into this repo as `react_agent/`, a
sibling of `strands_agent/`, and host its Temporal worker on Amazon Bedrock AgentCore Runtime as
a Temporal Serverless Worker.

## Decisions (2026-11-13)

| Topic | Decision | Why |
|---|---|---|
| Repo layout | Sibling dirs `strands_agent/` and `react_agent/`, each a self-contained uv + AgentCore project | CodeZip zips the whole `codeLocation`, so each runtime needs its own folder |
| F1 MCP server | Dropped for now | It is a Node.js stdio server that shells out to a FastF1 venv; the CodeZip Python runtime has no Node. Revisit as a remote MCP server (e.g. AgentCore Gateway) or a container build |
| Model | Keep OpenAI (`gpt-5.6-luna`) via the OpenAI Agents SDK | Smallest change from the source example |
| Harness dependency | `temporal-agent-harness[openai-agents]` from PyPI | Published (0.11.0); CodeZip build installs it like any dependency |
| Secrets (`TEMPORAL_API_KEY`, `OPENAI_API_KEY`) | One Secrets Manager secret (JSON), read at worker start; runtime role granted read via `additionalPolicies` | Keeps secrets out of git. AgentCore Identity `requires_api_key` was ruled out: it needs a workload access token, which SigV4-invoked runtimes only get with a user-id header Temporal Cloud does not send |
| Large-payload offload | Disabled on the AgentCore worker (`large_payload_offload=None`) | The harness default is local-disk storage, which is ephemeral on AgentCore and unreadable by the locally run server. Weather tool results are tiny; an oversized payload should fail loudly rather than be written somewhere nobody can read |
| Session manager + web server + terminal client | Run locally against the same Temporal Cloud namespace | Only the agent worker needs serverless hosting; the session manager starts the agent as a child workflow on the `react-agent` task queue, which the Worker Deployment routes to AgentCore |
| Idle detection | Same `ActivityTracker` debounce as `strands_agent` (duplicated, not shared) | Each example must stay self-contained to be zipped independently |
| OpenTelemetry | `aws-opentelemetry-distro` is a direct dependency | The CodeZip runtime launches the entry point via `opentelemetry-instrument` and fails runtime creation ("OpenTelemetry instrumentation executable not found") without it. `strands_agent` gets it transitively from `strands-agents` |
| Streaming batch interval | 250ms (source example used 50ms) | Each flush is one publish Signal; fewer history events at a small cost in token-by-token smoothness |
| Endpoint Temporal invokes (2026-11-14) | AgentCore's `DEFAULT` endpoint, for development | It always serves the latest runtime version, so one deploy rolls out code. The pinned `temporal` endpoint required a version bump and a second deploy. Cost: build ID `1.0.0` no longer identifies fixed code. The `temporal` endpoint stays in `agentcore.json` for the Worker Deployment versioning work |
| `idleRuntimeSessionTimeout` (2026-11-14) | 60s (the minimum; the default is 900) | The session counts as busy while the Worker runs, so this only controls how long a drained microVM lingers. Matters because every WCI invoke starts a new microVM (`docs/research/agentcore-serverless-challenges.md`, items 2, 3 and 7) |

## Known limitations

- **No large-payload offload.** Any payload over Temporal's ~2 MB limit fails the call. Lifting
  this means `s3_payload_storage(bucket=...)` on the AgentCore worker *and* on the session manager,
  server, and client — but the packaged `temporal-agent-harness serve` / `session-manager` CLI
  always uses the default local storage, so those would need small custom launchers.
- **`openai` pinned to 3.8.0.** temporal-agent-harness 0.11 (and upstream `main`) reads
  `ResponseFunctionCallArgumentsDoneEvent.name` eagerly in
  `openai_agents_harness._emit_tool_requested` (`self._fn_calls.pop(item_id, (item_id, event.name))`).
  openai 3.9.0 removed that field, so every streamed tool call failed the model activity with
  `AttributeError`. Upstream fix: build the fallback lazily (`getattr(event, "name", None)` only
  when the item id is missing). Drop the pin once that ships; `tests/test_openai_pin.py` guards it.
- **The web UI can't start sessions while the Worker is scaled to zero.** Its agent picker checks
  for live pollers (`DescribeTaskQueue`) and disables "new session" when there are none; opening an
  idle session also hung on workflow queries. Server fix temporalio/temporal#10574 (OSS v1.32.0+)
  makes queries wake serverless Workers, and that is confirmed on Temporal Cloud (2026-11-15). The
  UI's 30s status queries now keep a Worker mostly up, so the picker rarely blocks (challenges
  doc, item 10). Workarounds when it does: reopen the popup, or use `client.py` (the server
  doesn't gate session creation). Proper fix belongs in the harness UI.
- **No F1 / MCP tools** (see table above).
- **`ask_user` is only answerable from the terminal client** (`client.py`); the packaged web UI has
  no callback-fulfilment affordance. Inherited from the source example.
- **`strands_agent/agentcore/agentcore.json` holds a plaintext `TEMPORAL_API_KEY`** in its
  `envVars`. It must not be committed as-is; move it to the same Secrets Manager pattern.

## Verified on first deploy (2026-11-13)

- CodeZip build installs `temporal-agent-harness[openai-agents]` on ARM64 / Python 3.12 (with
  `aws-opentelemetry-distro` added).
- Temporal Cloud invokes the runtime for a new session; the Worker loads keys from Secrets
  Manager, streams model calls, and chains `get_coordinates` -> `get_weather` to a finished turn.
- A tool outage (Open-Meteo forecast host hanging) retried as an Activity and the turn completed
  once the service recovered.
- Code rollouts through the `temporal` endpoint needed `endpoints.temporal.version` bumped: it is
  pinned to a runtime version, and only `DEFAULT` follows the latest. That's why Temporal now
  invokes `DEFAULT` (see the decisions table).

## Open items to verify on first deploy

- Temporal Cloud invokes the runtime for workflow tasks created by an Update (`ask_user` answer
  via `provide_callback_result`) after the worker has scaled to zero.
- The CodeZip build resolves `temporal-agent-harness[openai-agents]` for ARM64 / Python 3.12.
- Streaming (`WorkflowStream` publish signals from the streaming model activity) behaves across a
  worker that may go idle between turns.
- Whether the CodeZip build also installs the `local` dependency group (`temporal-agent-harness[ui]`,
  i.e. FastAPI). Harmless but bloats the zip; if so, move it out of `default-groups`.
