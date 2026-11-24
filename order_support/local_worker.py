# ABOUTME: Runs all three order-support Workers locally in one process, each on its own task queue
# with its own client and plugins, as the separate AgentCore runtimes do.

"""Local Workers for the order-support agents.

    uv run python local_worker.py               # with activity sessions (the default)
    uv run python local_worker.py --no-sessions

Each Worker gets its own client: the concierge's has only the OpenAI Agents plugin, each
specialist's only the Strands plugin, matching the separate AgentCore runtimes (agentcore_worker.py).

Env vars:
    TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY   the Temporal server (TLS when a key is set)
    OPENAI_API_KEY                                          the concierge's model
    AWS credentials + AWS_REGION                            the specialists' Bedrock model
    BEDROCK_MODEL_ID                                        default global.anthropic.claude-opus-5-5
"""

import argparse
import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from activity_sessions import ActivitySessionInterceptor, session_config_for
from hosted_agents import (
    BEDROCK_MODEL_ID,
    HOSTED_AGENTS,
    HostedAgent,
    workflow_runner,
)

log = logging.getLogger("local_worker")

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
        ", ".join(agent.task_queue for agent in HOSTED_AGENTS.values()),
        "on" if args.sessions else "off",
        BEDROCK_MODEL_ID,
    )
    await asyncio.gather(*(worker.run() for worker in workers))


if __name__ == "__main__":
    asyncio.run(main())
