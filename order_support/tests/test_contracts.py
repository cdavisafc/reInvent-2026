# ABOUTME: Checks that need no server: each stub contract matches its real Strands specialist, the
# concierge's generated subagent tools and approval policy, and that the concierge never imports Strands.

import inspect
import subprocess
import sys

import pytest
from temporal_agent_harness.harness.agent_workflow import agent_handlers
from temporalio import workflow

from contracts import (
    ORDERS_WORKFLOW_TYPE,
    SHIPPING_WORKFLOW_TYPE,
    OrdersContract,
    ShippingContract,
)

SUBAGENT_TOOL_NAMES = [
    "start_shipping",
    "shipping_ask",
    "stop_shipping",
    "start_orders",
    "orders_ask",
    "stop_orders",
]


def _signature(cls: type) -> dict[str, tuple]:
    return {
        name: (h.input_type, h.output_type, h.description, h.model_callable)
        for name, h in agent_handlers(cls).items()
    }


def _specialists():
    from orders.workflow import OrdersAgentWorkflow
    from shipping.workflow import ShippingAgentWorkflow

    return [
        (ShippingContract, ShippingAgentWorkflow, SHIPPING_WORKFLOW_TYPE),
        (OrdersContract, OrdersAgentWorkflow, ORDERS_WORKFLOW_TYPE),
    ]


@pytest.mark.parametrize("index", [0, 1], ids=["shipping", "orders"])
def test_contract_matches_the_real_specialist(index):
    contract, real, _ = _specialists()[index]
    assert _signature(contract) == _signature(real)


@pytest.mark.parametrize("index", [0, 1], ids=["shipping", "orders"])
def test_contract_names_the_real_workflow_type(index):
    _, real, workflow_type = _specialists()[index]
    defn = workflow._Definition.from_class(real)
    assert defn is not None and defn.name == workflow_type


def test_concierge_gets_start_ask_stop_tools_for_both_and_auto_approves_them():
    from concierge.workflow import APPROVAL_POLICY, CONCIERGE_TOOLS

    names = [tool.__name__ for tool in CONCIERGE_TOOLS]
    assert names == [*SUBAGENT_TOOL_NAMES, "ask_user"]
    for name in SUBAGENT_TOOL_NAMES:
        assert APPROVAL_POLICY.auto_approves(name, inherently_safe=False)


def test_concierge_module_does_not_import_strands():
    # A fresh interpreter, so modules other tests imported don't count.
    probe = (
        "import sys, concierge.workflow; "
        "print(sorted(m for m in sys.modules if m == 'strands' or m.startswith('strands.')))"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"


def test_generated_subagent_tools_take_keyword_arguments():
    # The OpenAI adapter passes positional-or-keyword parameters positionally, which the
    # generated `<key>_ask` body (subagent, **model_kwargs) can't accept.
    from concierge.workflow import SUBAGENT_TOOLS

    for tool in SUBAGENT_TOOLS:
        kinds = {p.kind for p in inspect.signature(tool).parameters.values()}
        assert kinds <= {inspect.Parameter.KEYWORD_ONLY}, tool.__name__


def test_workflow_modules_do_not_read_the_environment():
    # A workflow must behave the same when replayed on any Worker, so its code can't depend on
    # the Worker's environment. Worker-side config belongs in hosted_agents.py.
    import ast
    from pathlib import Path

    root = Path(__file__).parent.parent
    modules = [
        *root.glob("*/workflow.py"),
        root / "contracts.py",
        root / "specialist.py",
    ]
    assert len(modules) == 5
    for module in modules:
        tree = ast.parse(module.read_text())
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import | ast.ImportFrom)
            for alias in node.names
        } | {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        assert "os" not in imported, module.name
