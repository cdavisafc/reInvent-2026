# ABOUTME: Liveness heartbeat for the AgentCore session this process runs in, written to a store
# (DynamoDB in production) every few seconds until AgentCore destroys the microVM.

"""Reports "this AgentCore session is alive" from inside the session.

AWS has no API that lists or describes runtime sessions, so the session reports on itself. The
heartbeat is a plain background task owned by the process, not by the Temporal Worker: it keeps
beating after the Worker drains, while the microVM waits for its next invoke, and it stops only
when AgentCore ends the session (idle timeout, max lifetime, or StopRuntimeSession). A fresh
``last_seen`` therefore means the session is alive.

It must never register an AgentCore async task. /ping reports HealthyBusy while one is
registered, which would keep the session (and its bill) alive until maxLifetime.
"""

import asyncio
import logging
import time
from collections.abc import Callable

from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger(__name__)

# How long an item outlives its last beat before DynamoDB's TTL may delete it.
ITEM_TTL_SECONDS = 3600


class SessionHeartbeat:
    """Writes one liveness item per beat; beats every ``interval`` seconds and on Worker changes."""

    def __init__(
        self,
        session_id: str,
        task_queue: str,
        write_item: Callable[[dict], None],
        *,
        interval: float = 10.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.session_id = session_id
        # The agent this session's Worker serves: the status page groups sessions by it.
        self.task_queue = task_queue
        self._write_item = write_item
        self._interval = interval
        self._clock = clock
        self._started_at = int(clock())
        self._worker_running = False
        self._worker_identity: str | None = None
        self._wake = asyncio.Event()
        # Holds the strong reference that asyncio.create_task() does not keep.
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Begin beating in the background. Calling it again does nothing."""
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    def stop(self) -> None:
        """Stop beating. Production never calls this: the heartbeat ends with the microVM."""
        if self._task is not None:
            self._task.cancel()

    def set_worker_running(self, running: bool, identity: str | None = None) -> None:
        """Record the Worker's state and beat now, so the change shows without waiting."""
        self._worker_running = running
        if identity is not None:
            self._worker_identity = identity
        self._wake.set()

    async def beat(self) -> None:
        """Write the current item. A failed write is logged, never raised."""
        now = int(self._clock())
        item: dict = {
            "session_id": self.session_id,
            "task_queue": self.task_queue,
            "started_at": self._started_at,
            "last_seen": now,
            "expires_at": now + ITEM_TTL_SECONDS,
            "worker_running": self._worker_running,
        }
        if self._worker_identity is not None:
            item["worker_identity"] = self._worker_identity
        try:
            # The production writer is a blocking boto3 call; keep it off the event loop.
            await asyncio.to_thread(self._write_item, item)
        except (BotoCoreError, ClientError, OSError):
            log.exception("session heartbeat write failed")

    async def _run(self) -> None:
        while True:
            self._wake.clear()
            await self.beat()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._interval)
            except TimeoutError:
                pass
