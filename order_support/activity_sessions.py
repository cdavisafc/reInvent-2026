# ABOUTME: Activity-session wiring for the AgentCore worker: maps the AgentCore runtime session to a
# worker session config, and places every same-queue activity in the workflow's session.

"""Activity sessions (experimental; unreleased temporalio ``dev/sticky-activities``).

With sessions, the activities a workflow starts — model calls and tool calls — run on the
*same* Worker that holds the workflow, and therefore in the same AgentCore microVM, instead
of going through task-queue matching to whichever Worker polls next.

The interceptor does what upstream ``temporalio-openai-agents``' ``session_options=``
argument does (ai-integrations ``feature/activity-sessions``,
``_activity_session_interceptor.py``). This project keeps temporal-agent-harness's vendored
OpenAI Agents plugin, which has no such argument, so the behavior lives here instead.
"""

from __future__ import annotations

from typing import Any

from temporalio import workflow
from temporalio.common import ActivitySessionOptions, UseWorkflowSession
from temporalio.worker import (
    Interceptor,
    SingleSessionAdmission,
    StartActivityInput,
    WorkerSessionConfig,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

IN_WORKFLOW_SESSION = ActivitySessionOptions(placement=UseWorkflowSession())


def session_config_for(session_id: str | None) -> WorkerSessionConfig:
    """Map the AgentCore runtime session to the Worker's session config.

    With a session id the Worker admits exactly one workflow, bound under that id, so the
    AgentCore session and the workflow session coincide. Without one (e.g. a local run) the
    Worker still serves workflow sessions, just without single-session admission.
    """
    if session_id:
        return WorkerSessionConfig(
            admission=SingleSessionAdmission(stable_identity=session_id)
        )
    return WorkerSessionConfig()


class ActivitySessionInterceptor(Interceptor):
    """Place every non-local activity a workflow starts on its own task queue in its session.

    Activities on other task queues cannot join the session and keep ordinary routing; local
    activities are unaffected. An enclosing ``workflow.activity_session(...)`` scope wins.
    Runs inside the workflow sandbox, so this module imports only ``temporalio``.
    """

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> type[WorkflowInboundInterceptor]:
        return _SessionInbound


class _SessionInbound(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        super().init(_SessionOutbound(outbound))


class _SessionOutbound(WorkflowOutboundInterceptor):
    def start_activity(self, input: StartActivityInput) -> workflow.ActivityHandle[Any]:
        if input.session_options is None and (
            input.task_queue is None or input.task_queue == workflow.info().task_queue
        ):
            input.session_options = IN_WORKFLOW_SESSION
        return self.next.start_activity(input)
