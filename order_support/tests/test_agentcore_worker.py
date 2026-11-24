# ABOUTME: Tests for the AgentCore entry point's Worker lifecycle: the idle debounce, the
# single-Worker guard on repeated invocations, the Worker's identity, and the session heartbeat.

import asyncio
from types import SimpleNamespace

import pytest
from bedrock_agentcore.runtime.models import PingStatus

import agentcore_worker
from agentcore_worker import ActivityTracker
from hosted_agents import hosted_agent


@pytest.fixture
def heartbeat_items(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Route the session heartbeat to an in-memory list instead of DynamoDB."""
    items: list[dict] = []
    monkeypatch.setattr(agentcore_worker, "AGENT_NAME", "shipping")
    monkeypatch.setattr(agentcore_worker, "HEARTBEAT_TABLE", "heartbeats")
    monkeypatch.setattr(agentcore_worker, "_heartbeat_writer", lambda table: items.append)
    monkeypatch.setattr(agentcore_worker, "_heartbeat", None)
    monkeypatch.setattr(agentcore_worker, "_worker", None)
    yield items
    if agentcore_worker._heartbeat is not None:
        agentcore_worker._heartbeat.stop()


async def _settle() -> None:
    """Let background tasks run until they block."""
    for _ in range(5):
        await asyncio.sleep(0)


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


async def test_a_failed_worker_releases_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    # A Worker that dies must still complete its async task, or the AgentCore session reports
    # HealthyBusy (and is billed) until maxLifetime.
    async def failing_run_worker(session_id: str | None) -> None:
        raise RuntimeError("worker crashed")

    monkeypatch.setattr(agentcore_worker, "run_worker", failing_run_worker)
    monkeypatch.setattr(agentcore_worker, "_worker", None)

    await agentcore_worker.invoke({}, SimpleNamespace(session_id="runtime-session-1"))
    await agentcore_worker._worker

    assert agentcore_worker.app.get_current_ping_status().value == "Healthy"


def test_worker_identity_names_the_agentcore_session() -> None:
    assert agentcore_worker.worker_identity("runtime-session-1") == "agentcore:runtime-session-1"
    assert agentcore_worker.worker_identity(None) is None


async def test_invoke_starts_one_session_heartbeat_naming_the_agents_queue(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    async def fake_run_worker(session_id: str | None) -> None:
        return None

    monkeypatch.setattr(agentcore_worker, "run_worker", fake_run_worker)
    context = SimpleNamespace(session_id="runtime-session-1")

    await agentcore_worker.invoke({}, context)
    first = agentcore_worker._heartbeat
    await agentcore_worker._worker
    await agentcore_worker.invoke({}, context)
    await agentcore_worker._worker

    assert first is not None
    assert agentcore_worker._heartbeat is first
    assert {item["session_id"] for item in heartbeat_items} == {"runtime-session-1"}
    assert {item["task_queue"] for item in heartbeat_items} == {
        hosted_agent("shipping").task_queue
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


class _SlowDrainWorker:
    """Stands in for a Worker whose shutdown takes a while; records the heartbeat as it starts."""

    def __init__(self, heartbeat_items: list[dict]) -> None:
        self._items = heartbeat_items
        self.reported_at_drain_start: dict | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc) -> None:
        await _settle()
        self.reported_at_drain_start = self._items[-1]


async def test_session_reports_standby_as_soon_as_the_worker_starts_draining(
    monkeypatch: pytest.MonkeyPatch, heartbeat_items: list[dict]
) -> None:
    # A Worker drains only once nothing is in flight, and stops polling as it starts. Its
    # shutdown can take a while; the session must not report a running Worker meanwhile.
    agentcore_worker._start_heartbeat("runtime-session-1")
    agentcore_worker._heartbeat.set_worker_running(True, "agentcore:runtime-session-1")
    await _settle()
    worker = _SlowDrainWorker(heartbeat_items)

    await agentcore_worker.poll_until_idle(worker, ActivityTracker(), debounce=0.001)

    assert worker.reported_at_drain_start["worker_running"] is False
