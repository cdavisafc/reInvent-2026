# ABOUTME: Human-in-the-loop callback tool(s) shared by the order-support agents.
# ask_user parks in-workflow until an external client returns the user's typed answer.

# NOTE: no `from __future__ import annotations` — the return type is read concretely to build
# the tool's output schema (the client's result is validated against it), like the activity tools.
# A stringized annotation would break that resolution.

from temporal_agent_harness.harness import agent


@agent.callback_tool_defn(inherently_safe=True)
async def ask_user(question: str) -> str:
    """Ask the user a clarifying question and use their typed answer to continue.

    Use this when a request is ambiguous or needs information only the user can provide —
    e.g. which order they mean.

    Args:
        question: The question to put to the user, in plain language.
    """
    # Never runs on the worker: this is a callback tool. When the model calls it, the harness
    # publishes a `callback_requested` event and parks the turn on a durable wait until an
    # attached client (the web UI) returns the user's answer as the result.
    ...  # noqa: PIE790 - the harness requires a callback tool's body to be exactly `...`


# ask_user is a callback tool with no activity body. Each agent adapts it onto its SDK alongside
# its own tools.
HUMAN_TOOLS = [ask_user]
