# ABOUTME: Runs the spike's two Workers locally in one process: the OpenAI concierge on its queue and
# the Strands shipping child on another, each with its own client and plugins, as AgentCore will.

"""Local Workers for the subagent spike.

    uv run python -m spike.worker               # with activity sessions (the default)
    uv run python -m spike.worker --no-sessions

Each Worker gets its own client: the concierge's has only the OpenAI Agents plugin, the shipping
child's only the Strands plugin, matching the separate AgentCore runtimes (agentcore_worker.py).

Env vars:
    TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY   the Temporal server (TLS when a key is set)
    OPENAI_API_KEY                                          the concierge's model
    AWS credentials + AWS_REGION                            the shipping child's Bedrock model
    SHIPPING_BEDROCK_MODEL_ID                               default global.anthropic.claude-opus-5-5
"""

import argparse
import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activity_sessions import ActivitySessionInterceptor, session_config_for
from spike.hosted_agents import (
    BEDROCK_MODEL_ID,
    HOSTED_AGENTS,
    HostedAgent,
    workflow_runner,
)

log = logging.getLogger("spike.worker")

# Session Workers need a fixed-size activity slot supplier.
ACTIVITY_SLOTS = 4


async def _connect(agent: HostedAgent) -> Client:
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key,
        tls=bool(api_key),
        plugins=agent.plugins(),
    )


def _worker(client: Client, agent: HostedAgent, sessions: bool) -> Worker:
    session_kwargs = (
        {
            "session_config": session_config_for(None),
            "interceptors": [ActivitySessionInterceptor()],
        }
        if sessions
        else {}
    )
    return Worker(
        client,
        task_queue=agent.task_queue,
        workflows=[agent.workflow],
        workflow_runner=workflow_runner(),
        max_concurrent_activities=ACTIVITY_SLOTS,
        **session_kwargs,
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--no-sessions",
        dest="sessions",
        action="store_false",
        help="run without activity sessions (needs no server support)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    workers = [
        _worker(await _connect(agent), agent, args.sessions)
        for agent in HOSTED_AGENTS.values()
    ]
    log.info(
        "polling %s (sessions %s, Bedrock model %s)",
        " and ".join(agent.task_queue for agent in HOSTED_AGENTS.values()),
        "on" if args.sessions else "off",
        BEDROCK_MODEL_ID,
    )
    await asyncio.gather(*(worker.run() for worker in workers))


if __name__ == "__main__":
    asyncio.run(main())
