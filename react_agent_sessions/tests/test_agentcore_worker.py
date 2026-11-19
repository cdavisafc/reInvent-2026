# ABOUTME: Tests for the AgentCore entry point's worker lifecycle: idle debounce and the
# single-worker guard on repeated invocations.

import asyncio
from types import SimpleNamespace

import pytest

import agentcore_worker
from agentcore_worker import ActivityTracker


async def test_activity_tracker_returns_after_idle_debounce() -> None:
    tracker = ActivityTracker()

    await asyncio.wait_for(tracker.wait_until_idle(0.001), timeout=1)


async def test_second_invocation_does_not_start_another_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started: list[str | None] = []
    release = asyncio.Event()

    async def fake_run_worker(session_id: str | None) -> None:
        started.append(session_id)
        await release.wait()

    monkeypatch.setattr(agentcore_worker, "run_worker", fake_run_worker)
    monkeypatch.setattr(agentcore_worker, "_worker", None)
    context = SimpleNamespace(session_id="runtime-session-1")

    first = await agentcore_worker.invoke({}, context)
    await asyncio.sleep(0)
    second = await agentcore_worker.invoke({}, context)

    assert first["message"] == "worker starting"
    assert second["message"] == "worker already polling"
    assert started == ["runtime-session-1"]

    release.set()
    await agentcore_worker._worker
