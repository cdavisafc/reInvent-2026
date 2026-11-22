# ABOUTME: Tests for the AgentCore entry point's worker lifecycle: idle debounce, the
# single-worker guard on repeated invocations, and the session liveness heartbeat.

import asyncio
from types import SimpleNamespace

import pytest
from bedrock_agentcore.runtime.models import PingStatus

import agentcore_worker
from agentcore_worker import ActivityTracker


@pytest.fixture
def heartbeat_items(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Route the session heartbeat to an in-memory list instead of DynamoDB."""
    items: list[dict] = []
    monkeypatch.setattr(agentcore_worker, "HEARTBEAT_TABLE", "heartbeats")
    monkeypatch.setattr(agentcore_worker, "_heartbeat_writer", lambda table: items.append)
    monkeypatch.setattr(agentcore_worker, "_heartbeat", None)
    monkeypatch.setattr(agentcore_worker, "_worker", None)
    yield items
    if agentcore_worker._heartbeat is not None:
        agentcore_worker._heartbeat.stop()


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


async def _settle() -> None:
    """Let background tasks run until they block."""
    for _ in range(5):
        await asyncio.sleep(0)


async def test_invoke_starts_session_heartbeat_once(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    release = asyncio.Event()

    async def fake_run_worker(session_id: str | None) -> None:
        await release.wait()

    monkeypatch.setattr(agentcore_worker, "run_worker", fake_run_worker)
    context = SimpleNamespace(session_id="runtime-session-1")

    await agentcore_worker.invoke({}, context)
    first = agentcore_worker._heartbeat
    release.set()
    await agentcore_worker._worker
    await agentcore_worker.invoke({}, context)
    await agentcore_worker._worker

    assert first is not None
    assert agentcore_worker._heartbeat is first
    assert {item["session_id"] for item in heartbeat_items} == {"runtime-session-1"}
    assert {item["task_queue"] for item in heartbeat_items} == {
        agentcore_worker.WORKER_TASK_QUEUE
    }


async def test_no_heartbeat_without_table_or_session(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    async def fake_run_worker(session_id: str | None) -> None:
        return None

    monkeypatch.setattr(agentcore_worker, "run_worker", fake_run_worker)

    await agentcore_worker.invoke({}, SimpleNamespace(session_id=None))
    await agentcore_worker._worker
    monkeypatch.setattr(agentcore_worker, "HEARTBEAT_TABLE", None)
    await agentcore_worker.invoke({}, SimpleNamespace(session_id="runtime-session-1"))
    await agentcore_worker._worker

    assert agentcore_worker._heartbeat is None


async def test_heartbeat_reports_worker_running_then_stopped_even_on_failure(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    release = asyncio.Event()

    async def failing_run_worker(session_id: str | None) -> None:
        await release.wait()
        raise RuntimeError("worker crashed")

    monkeypatch.setattr(agentcore_worker, "run_worker", failing_run_worker)

    await agentcore_worker.invoke({}, SimpleNamespace(session_id="runtime-session-1"))
    await _settle()
    running = heartbeat_items[-1]
    release.set()
    await agentcore_worker._worker
    await _settle()

    assert running["worker_running"] is True
    assert running["worker_identity"] == "agentcore:runtime-session-1"
    assert heartbeat_items[-1]["worker_running"] is False


async def test_session_goes_idle_while_heartbeat_keeps_beating(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    """The heartbeat must not hold an async task: that would report HealthyBusy and keep the
    AgentCore session (and its bill) alive until maxLifetime."""

    async def fake_run_worker(session_id: str | None) -> None:
        return None

    monkeypatch.setattr(agentcore_worker, "run_worker", fake_run_worker)

    await agentcore_worker.invoke({}, SimpleNamespace(session_id="runtime-session-1"))
    await agentcore_worker._worker
    await _settle()

    assert not agentcore_worker._heartbeat._task.done()
    assert agentcore_worker.app.get_current_ping_status() == PingStatus.HEALTHY


def test_worker_identity_names_the_agentcore_session() -> None:
    assert agentcore_worker.worker_identity("runtime-session-1") == "agentcore:runtime-session-1"
    assert agentcore_worker.worker_identity(None) is None
