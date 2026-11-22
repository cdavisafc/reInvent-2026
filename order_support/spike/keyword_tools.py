# ABOUTME: Workaround for harness 0.11/0.12: makes subagent_toolset's generated tools callable from
# the OpenAI Agents SDK adapter, by declaring their parameters keyword-only.

"""Why this exists.

``subagent_toolset`` gives each ``<key>_<handler>`` tool a synthesized signature
``(subagent, <param>)`` with positional-or-keyword parameters, but the function behind it is
``_send(subagent, **model_kwargs)``. The OpenAI Agents SDK adapter (``as_openai_agent_tools``)
builds its call with ``FuncSchema.to_call_args``, which passes positional-or-keyword parameters
positionally, so every call fails with "takes 1 positional argument but 2 were given".

Declaring the parameters keyword-only makes the adapter pass them by name, which ``_send``
accepts. The model-facing schema is unchanged. Remove this once the harness fixes
``subagent_toolset`` (see docs/research/order-support-subagent-spike.md).
"""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any


def keyword_only(
    tools: list[Callable[..., Awaitable[Any]]],
) -> list[Callable[..., Awaitable[Any]]]:
    """Return ``tools`` with every declared parameter made keyword-only (in place)."""
    for tool in tools:
        signature = inspect.signature(tool)
        tool.__signature__ = signature.replace(  # type: ignore[attr-defined]
            parameters=[
                p.replace(kind=inspect.Parameter.KEYWORD_ONLY)
                for p in signature.parameters.values()
            ]
        )
    return tools
