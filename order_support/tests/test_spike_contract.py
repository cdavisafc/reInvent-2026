# ABOUTME: Spike checks that need no server: the stub contract matches the real Strands child,
# the parent's generated subagent tools and approval policy, and that the parent never imports Strands.

import subprocess
import sys

from temporal_agent_harness.harness.agent_workflow import agent_handlers

from spike.contracts import SHIPPING_WORKFLOW_TYPE, ShippingContract


def _signature(cls: type) -> dict[str, tuple]:
    return {
        name: (h.input_type, h.output_type, h.description, h.model_callable)
        for name, h in agent_handlers(cls).items()
    }


def test_contract_matches_the_real_shipping_agent():
    from spike.shipping_workflow import SpikeShippingAgentWorkflow

    assert _signature(ShippingContract) == _signature(SpikeShippingAgentWorkflow)


def test_contract_names_the_real_workflow_type():
    from temporalio import workflow

    from spike.shipping_workflow import SpikeShippingAgentWorkflow

    defn = workflow._Definition.from_class(SpikeShippingAgentWorkflow)
    assert defn is not None and defn.name == SHIPPING_WORKFLOW_TYPE


def test_concierge_gets_start_ask_stop_tools_and_auto_approves_them():
    from spike.concierge_workflow import APPROVAL_POLICY, CONCIERGE_TOOLS

    names = [tool.__name__ for tool in CONCIERGE_TOOLS]
    assert names == ["start_shipping", "shipping_ask", "stop_shipping", "ask_user"]
    for name in ["start_shipping", "shipping_ask", "stop_shipping"]:
        assert APPROVAL_POLICY.auto_approves(name, inherently_safe=False)


def test_concierge_module_does_not_import_strands():
    # A fresh interpreter, so modules other tests imported don't count.
    probe = (
        "import sys, spike.concierge_workflow; "
        "print(sorted(m for m in sys.modules if m == 'strands' or m.startswith('strands.')))"
    )
    out = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, check=True
    )
    assert out.stdout.strip() == "[]"


def test_generated_subagent_tools_take_keyword_arguments():
    # The OpenAI adapter passes positional-or-keyword parameters positionally, which the
    # generated `<key>_ask` body (subagent, **model_kwargs) can't accept.
    import inspect

    from spike.concierge_workflow import SHIPPING_SUBAGENT_TOOLS

    for tool in SHIPPING_SUBAGENT_TOOLS:
        kinds = {p.kind for p in inspect.signature(tool).parameters.values()}
        assert kinds <= {inspect.Parameter.KEYWORD_ONLY}, tool.__name__


def test_workflow_modules_do_not_read_the_environment():
    # A workflow must behave the same when replayed on any Worker, so its code can't depend on
    # the Worker's environment. Worker-side config belongs in spike/hosted_agents.py.
    import ast
    from pathlib import Path

    spike = Path(__file__).parent.parent / "spike"
    for module in [*spike.glob("*_workflow.py"), spike / "contracts.py"]:
        tree = ast.parse(module.read_text())
        imported = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import | ast.ImportFrom)
            for alias in node.names
        } | {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        assert "os" not in imported, module.name
