# ABOUTME: Tests for activity-session wiring: the AgentCore session -> worker session config mapping,
# and the interceptor that places same-queue activities in the workflow's session.

from types import SimpleNamespace
from typing import Any

import pytest
from temporalio import workflow
from temporalio.common import ActivitySessionOptions, UseWorkflowSession
from temporalio.worker import SingleSessionAdmission, WorkerSessionConfig

from activity_sessions import ActivitySessionInterceptor, session_config_for

WORKFLOW_QUEUE = "react-agent-sessions"
IN_SESSION = ActivitySessionOptions(placement=UseWorkflowSession())


def test_session_id_maps_to_single_session_admission() -> None:
    assert session_config_for("runtime-session-123") == WorkerSessionConfig(
        admission=SingleSessionAdmission(stable_identity="runtime-session-123")
    )


@pytest.mark.parametrize("session_id", [None, ""])
def test_missing_session_id_serves_sessions_without_admission(
    session_id: str | None,
) -> None:
    assert session_config_for(session_id) == WorkerSessionConfig(admission=None)


class _RecordingOutbound:
    """The next outbound interceptor in the chain; records the input it is handed."""

    def __init__(self) -> None:
        self.started: list[Any] = []

    def start_activity(self, input: Any) -> str:
        self.started.append(input)
        return "handle"


@pytest.fixture
def outbound(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, _RecordingOutbound]:
    """The session interceptor's outbound, built through the inbound init as the worker does."""
    monkeypatch.setattr(
        workflow, "info", lambda: SimpleNamespace(task_queue=WORKFLOW_QUEUE)
    )
    recorder = _RecordingOutbound()
    inbound_class = ActivitySessionInterceptor().workflow_interceptor_class(
        SimpleNamespace()
    )
    captured: dict[str, Any] = {}

    class _NextInbound:
        def init(self, outbound: Any) -> None:
            captured["outbound"] = outbound

    inbound_class(_NextInbound()).init(recorder)
    return captured["outbound"], recorder


def _start(
    outbound: Any, *, task_queue: str | None, session_options: Any = None
) -> Any:
    activity_input = SimpleNamespace(
        task_queue=task_queue, session_options=session_options
    )
    assert outbound.start_activity(activity_input) == "handle"
    return activity_input


@pytest.mark.parametrize("task_queue", [None, WORKFLOW_QUEUE])
def test_same_queue_activity_joins_workflow_session(
    outbound: tuple[Any, _RecordingOutbound], task_queue: str | None
) -> None:
    interceptor, recorder = outbound

    started = _start(interceptor, task_queue=task_queue)

    assert recorder.started == [started]
    assert started.session_options == IN_SESSION


def test_other_queue_activity_keeps_ordinary_routing(
    outbound: tuple[Any, _RecordingOutbound],
) -> None:
    interceptor, _ = outbound

    started = _start(interceptor, task_queue="some-other-queue")

    assert started.session_options is None


def test_explicit_activity_session_scope_wins(
    outbound: tuple[Any, _RecordingOutbound],
) -> None:
    interceptor, _ = outbound
    explicit = object()

    started = _start(interceptor, task_queue=None, session_options=explicit)

    assert started.session_options is explicit
