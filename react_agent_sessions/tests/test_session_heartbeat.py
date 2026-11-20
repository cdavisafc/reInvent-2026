# ABOUTME: Tests for the AgentCore session liveness heartbeat: the item it writes, its cadence,
# its immediate beat on Worker state changes, and its resilience to failed writes.

import asyncio

from session_heartbeat import SessionHeartbeat


class RecordingWriter:
    """A write_item stand-in that records items and can fail on demand."""

    def __init__(self, fail_first: int = 0) -> None:
        self.items: list[dict] = []
        self.fail_first = fail_first
        self.written = asyncio.Event()

    def __call__(self, item: dict) -> None:
        if self.fail_first:
            self.fail_first -= 1
            raise ConnectionError("dynamodb unreachable")
        self.items.append(item)
        self.written.set()

    async def wait_for(self, count: int) -> None:
        while len(self.items) < count:
            self.written.clear()
            await asyncio.wait_for(self.written.wait(), timeout=1)


async def test_beat_writes_session_liveness_item() -> None:
    writer = RecordingWriter()
    heartbeat = SessionHeartbeat("session-abc", writer, clock=lambda: 1000.4)

    await heartbeat.beat()

    assert writer.items == [
        {
            "session_id": "session-abc",
            "started_at": 1000,
            "last_seen": 1000,
            "expires_at": 1000 + 3600,
            "worker_running": False,
        }
    ]


async def test_worker_state_is_written_with_its_identity() -> None:
    writer = RecordingWriter()
    heartbeat = SessionHeartbeat("session-abc", writer, clock=lambda: 1000)

    heartbeat.set_worker_running(True, "agentcore:session-abc")
    await heartbeat.beat()

    assert writer.items[-1]["worker_running"] is True
    assert writer.items[-1]["worker_identity"] == "agentcore:session-abc"


async def test_beats_repeatedly_on_interval() -> None:
    writer = RecordingWriter()
    heartbeat = SessionHeartbeat("session-abc", writer, interval=0.01)

    heartbeat.start()
    await writer.wait_for(3)

    heartbeat.stop()


async def test_worker_state_change_beats_immediately() -> None:
    writer = RecordingWriter()
    heartbeat = SessionHeartbeat("session-abc", writer, interval=60)

    heartbeat.start()
    await writer.wait_for(1)
    heartbeat.set_worker_running(True, "agentcore:session-abc")
    await writer.wait_for(2)

    assert writer.items[1]["worker_running"] is True
    heartbeat.stop()


async def test_failed_write_does_not_stop_the_heartbeat() -> None:
    writer = RecordingWriter(fail_first=2)
    heartbeat = SessionHeartbeat("session-abc", writer, interval=0.01)

    heartbeat.start()
    await writer.wait_for(1)

    heartbeat.stop()


async def test_start_is_idempotent() -> None:
    heartbeat = SessionHeartbeat("session-abc", RecordingWriter(), interval=60)

    heartbeat.start()
    first = heartbeat._task
    heartbeat.start()

    assert heartbeat._task is first
    heartbeat.stop()
