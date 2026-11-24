# ABOUTME: Tests for choosing which agent a Worker hosts, and that agentcore.json's runtimes
# each name a hosted agent with its own Worker Deployment.

import json
from pathlib import Path

import pytest

from concierge.workflow import CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow
from contracts import ORDERS_TASK_QUEUE, SHIPPING_TASK_QUEUE
from hosted_agents import HOSTED_AGENTS, UnknownAgentError, hosted_agent
from orders.workflow import OrdersAgentWorkflow
from shipping.workflow import ShippingAgentWorkflow

AGENTCORE_JSON = Path(__file__).parent.parent / "agentcore" / "agentcore.json"


@pytest.mark.parametrize(
    ("name", "task_queue", "workflow"),
    [
        ("concierge", CONCIERGE_TASK_QUEUE, ConciergeAgentWorkflow),
        ("shipping", SHIPPING_TASK_QUEUE, ShippingAgentWorkflow),
        ("orders", ORDERS_TASK_QUEUE, OrdersAgentWorkflow),
    ],
)
def test_each_agent_polls_its_own_queue_with_its_workflow(name, task_queue, workflow):
    agent = hosted_agent(name)
    assert (agent.task_queue, agent.workflow) == (task_queue, workflow)


def test_only_the_concierge_needs_the_openai_key():
    # The specialists call Claude on Bedrock with the runtime's IAM role.
    assert "OPENAI_API_KEY" in hosted_agent("concierge").secret_keys
    assert hosted_agent("shipping").secret_keys == ("TEMPORAL_API_KEY",)
    assert hosted_agent("orders").secret_keys == ("TEMPORAL_API_KEY",)


@pytest.mark.parametrize("name", [None, "", "billing"])
def test_unknown_agent_fails_naming_the_choices(name):
    with pytest.raises(UnknownAgentError, match="concierge.*shipping.*orders"):
        hosted_agent(name)


def _runtime_env(runtime: dict) -> dict[str, str]:
    return {var["name"]: var["value"] for var in runtime["envVars"]}


def test_agentcore_runtimes_each_host_a_known_agent_in_its_own_deployment():
    runtimes = json.loads(AGENTCORE_JSON.read_text())["runtimes"]
    envs = [_runtime_env(runtime) for runtime in runtimes]

    assert sorted(env["ORDER_SUPPORT_AGENT"] for env in envs) == sorted(HOSTED_AGENTS)
    deployments = [env["TEMPORAL_DEPLOYMENT_NAME"] for env in envs]
    assert len(set(deployments)) == len(deployments)
