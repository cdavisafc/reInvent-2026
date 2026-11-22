# ABOUTME: Tests for choosing which agent a Worker hosts, and that agentcore.json's runtimes
# each name a hosted agent with its own Worker Deployment.

import json
from pathlib import Path

import pytest

from spike.concierge_workflow import CONCIERGE_TASK_QUEUE, SpikeConciergeAgentWorkflow
from spike.contracts import SHIPPING_TASK_QUEUE
from spike.hosted_agents import HOSTED_AGENTS, UnknownAgentError, hosted_agent
from spike.shipping_workflow import SpikeShippingAgentWorkflow

AGENTCORE_JSON = Path(__file__).parent.parent / "agentcore" / "agentcore.json"


def test_each_agent_polls_its_own_queue_with_its_workflow():
    concierge = hosted_agent("concierge")
    shipping = hosted_agent("shipping")

    assert (concierge.task_queue, concierge.workflow) == (
        CONCIERGE_TASK_QUEUE,
        SpikeConciergeAgentWorkflow,
    )
    assert (shipping.task_queue, shipping.workflow) == (
        SHIPPING_TASK_QUEUE,
        SpikeShippingAgentWorkflow,
    )


def test_only_the_concierge_needs_the_openai_key():
    # The shipping child calls Claude on Bedrock with the runtime's IAM role.
    assert "OPENAI_API_KEY" in hosted_agent("concierge").secret_keys
    assert hosted_agent("shipping").secret_keys == ("TEMPORAL_API_KEY",)


@pytest.mark.parametrize("name", [None, "", "orders"])
def test_unknown_agent_fails_naming_the_choices(name):
    with pytest.raises(UnknownAgentError, match="concierge.*shipping"):
        hosted_agent(name)


def _runtime_env(runtime: dict) -> dict[str, str]:
    return {var["name"]: var["value"] for var in runtime["envVars"]}


def test_agentcore_runtimes_each_host_a_known_agent_in_its_own_deployment():
    runtimes = json.loads(AGENTCORE_JSON.read_text())["runtimes"]
    envs = [_runtime_env(runtime) for runtime in runtimes]

    assert sorted(env["ORDER_SUPPORT_AGENT"] for env in envs) == sorted(HOSTED_AGENTS)
    deployments = [env["TEMPORAL_DEPLOYMENT_NAME"] for env in envs]
    assert len(set(deployments)) == len(deployments)
