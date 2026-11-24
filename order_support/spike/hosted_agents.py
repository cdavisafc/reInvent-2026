# ABOUTME: The agents a Worker can host, each with its task queue, workflow, client plugins and
# required secrets. Shared by the local Workers (spike/worker.py) and the AgentCore entry point.

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta

from agents import ModelProvider
from botocore.config import Config as BotocoreConfig
from strands.models import Model as StrandsModel
from strands.models.bedrock import BedrockModel
from temporal_agent_harness.ai_sdks.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
)
from temporal_agent_harness.ai_sdks.openai_agents_harness import (
    harness_observer_factory as openai_observer_factory,
)
from temporal_agent_harness.ai_sdks.openai_agents_harness import stream_to_provider
from temporal_agent_harness.ai_sdks.strands_agents import StrandsPlugin
from temporal_agent_harness.ai_sdks.strands_agents_harness import (
    harness_observer_factory as strands_observer_factory,
)
from temporal_agent_harness.plugin import AgentHarnessPlugin
from temporalio.client import Plugin
from temporalio.plugin import SimplePlugin
from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from human_tools import HUMAN_TOOLS
from spike.concierge_workflow import CONCIERGE_TASK_QUEUE, SpikeConciergeAgentWorkflow
from spike.contracts import SHIPPING_TASK_QUEUE
from spike.shipping_tools import SHIPPING_TOOLS
from spike.shipping_workflow import MODEL, SpikeShippingAgentWorkflow
from subagent_signals import send_subagent_message

BEDROCK_MODEL_ID = os.environ.get(
    "SHIPPING_BEDROCK_MODEL_ID", "global.anthropic.claude-opus-5-5"
)


class UnknownAgentError(ValueError):
    """The requested agent isn't one this project can host."""


@dataclass(frozen=True)
class HostedAgent:
    name: str
    task_queue: str
    workflow: type
    plugins: Callable[[], list[Plugin]]
    # The keys a Worker loads from Secrets Manager on AgentCore.
    secret_keys: tuple[str, ...]


def _harness_plugin(tools: list) -> AgentHarnessPlugin:
    # Offload is off: the harness default writes to local disk, which is ephemeral on AgentCore
    # and unreadable by the locally run server. An oversized payload should fail loudly.
    return AgentHarnessPlugin(tools=tools, large_payload_offload=None)


def concierge_plugins(model_provider: ModelProvider | None = None) -> list[Plugin]:
    """The concierge Worker's plugins. Tests pass a scripted ``model_provider``."""
    return [
        OpenAIAgentsPlugin(
            model_provider=model_provider,
            model_params=ModelActivityParameters(
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=30),
                stream_to_provider=stream_to_provider,
                streaming_batch_interval=timedelta(milliseconds=250),
            ),
            observer_factory=openai_observer_factory,
        ),
        # The concierge sends to its subagents with this activity, then waits for their reply
        # signal in the workflow (subagent_signals.py).
        SimplePlugin("order-support-subagent-signals", activities=[send_subagent_message]),
        # Listed last so its data converter layers on top of the OpenAI plugin's.
        _harness_plugin(HUMAN_TOOLS),
    ]


def _bedrock_model() -> BedrockModel:
    # Temporal owns retries, so Botocore must not retry within an activity attempt.
    # Newer Claude models omit thinking text unless asked for summaries; the UI shows them.
    return BedrockModel(
        model_id=BEDROCK_MODEL_ID,
        boto_client_config=BotocoreConfig(read_timeout=120, retries={"max_attempts": 0}),
        additional_request_fields={"thinking": {"type": "adaptive", "display": "summarized"}},
    )


def shipping_plugins(
    model_factory: Callable[[], StrandsModel] = _bedrock_model,
) -> list[Plugin]:
    """The shipping Worker's plugins. Tests pass a scripted ``model_factory``."""
    return [
        StrandsPlugin(
            models={MODEL: model_factory}, observer_factory=strands_observer_factory
        ),
        _harness_plugin([*SHIPPING_TOOLS, *HUMAN_TOOLS]),
    ]


HOSTED_AGENTS = {
    agent.name: agent
    for agent in [
        HostedAgent(
            name="concierge",
            task_queue=CONCIERGE_TASK_QUEUE,
            workflow=SpikeConciergeAgentWorkflow,
            plugins=concierge_plugins,
            secret_keys=("TEMPORAL_API_KEY", "OPENAI_API_KEY"),
        ),
        HostedAgent(
            name="shipping",
            task_queue=SHIPPING_TASK_QUEUE,
            workflow=SpikeShippingAgentWorkflow,
            plugins=shipping_plugins,
            # Bedrock uses the runtime's IAM role, so no model key.
            secret_keys=("TEMPORAL_API_KEY",),
        ),
    ]
}


def hosted_agent(name: str | None) -> HostedAgent:
    """The agent called ``name``; raises UnknownAgentError for anything else."""
    if name not in HOSTED_AGENTS:
        raise UnknownAgentError(
            f"unknown agent {name!r}; expected one of: {', '.join(HOSTED_AGENTS)}"
        )
    return HOSTED_AGENTS[name]


def workflow_runner() -> SandboxedWorkflowRunner:
    """The sandbox for every Worker here, with OpenTelemetry passed through.

    Strands starts spans inside the workflow, and span processors may import OpenTelemetry
    modules on first use; AgentCore's own (bedrock_agentcore.runtime.tracing) does. Without the
    passthrough the sandbox imports a second copy of ``opentelemetry.context``, whose module body
    reads ``os.environ`` and is rejected. The plugins extend this runner with their own
    passthroughs.
    """
    return SandboxedWorkflowRunner(
        restrictions=SandboxRestrictions.default.with_passthrough_modules("opentelemetry")
    )
