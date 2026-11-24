# ABOUTME: Local fix so a parent agent waits for a subagent's reply in its workflow, not in an activity:
# a short activity sends the message, and the child signals the reply back when its turn ends.

"""Subagent turns that don't hold the parent's Worker.

The harness drives each subagent turn with the ``run_subagent_turn`` activity, which blocks until
the child's turn ends. While a child waits on a person (a tool approval), the parent therefore has
an activity in flight, and a serverless Worker can't drain. See
``../docs/research/agent-kit-issue-subagent-turn-holds-parent-worker.md``.

:class:`SubagentSignalRunner` replaces that, and both the parent and the child agent use it:

- **Parent.** ``run_subagent_turn`` sends the message with the short
  :func:`send_subagent_message` activity, publishes ``subagent_message_sent``, then waits in the
  workflow for the child's reply signal. Nothing is in flight while it waits, so its Worker drains;
  the signal wakes the workflow on whichever Worker picks it up.
- **Child.** After handling a message (and publishing ``turn_end``, when it's the turn's last
  participant), a child workflow signals its parent with the reply or the error. It does this for
  every message; the parent ignores replies to messages it didn't send.

Everything else in the harness's subagent turn is kept: the per-subagent FIFO gate, the
``subagent_reply_received`` marker, and the ``AgentClosed`` handling. It overrides
``AgentWorkflowRunner.run_subagent_turn`` (copied from temporal-agent-harness 0.11.1, with only
the activity replaced) and two private hooks, ``_dispatch_turn`` and ``_run_participant``.
Delete this module once temporal-agent-kit ships an equivalent.
"""

from datetime import timedelta
from typing import Any, Literal

from pydantic import BaseModel
from temporal_agent_harness.harness.agent_client import (
    AgentClient,
    MidTurnRejectedError,
)
from temporal_agent_harness.harness.agent_protocol import (
    AgentMessageReply,
    SubagentMessageSent,
)
from temporal_agent_harness.harness.agent_workflow import AgentWorkflowRunner
from temporalio import activity, workflow
from temporalio.client import WorkflowUpdateFailedError
from temporalio.exceptions import ActivityError, ApplicationError, FailureError
from temporalio.service import RPCError, RPCStatusCode

SUBAGENT_REPLY_SIGNAL = "subagent_turn_reply"
SEND_SUBAGENT_MESSAGE_ACTIVITY = "send_subagent_message"
# Only the send: the child accepts or rejects the message right away.
SUBAGENT_SEND_TIMEOUT = timedelta(seconds=30)
# How long a parent waits for a child's reply. Waiting holds nothing on a Worker, so this can be
# long enough for a person to come back to an approval.
SUBAGENT_REPLY_TIMEOUT = timedelta(hours=24)


class SendSubagentMessageInput(BaseModel):
    child_workflow_id: str
    type: str
    payload: dict[str, Any]


class SubagentReply(BaseModel):
    """What a child signals its parent once it has handled a message."""

    message_id: str
    outcome: Literal["ok", "error"]
    # The handler's reply (``MessageHandlerEnd.output``) when ``outcome`` is "ok".
    output: dict[str, Any] | None = None
    # The handler's error message when ``outcome`` is "error".
    error: str | None = None
    # The child's stream offset once the message was handled: where the next turn's events start.
    next_offset: int


@activity.defn(name=SEND_SUBAGENT_MESSAGE_ACTIVITY)
async def send_subagent_message(req: SendSubagentMessageInput) -> AgentMessageReply:
    """Send one message to a child agent and return its acceptance. Doesn't wait for the reply.

    The update is keyed on this activity's identity, which every retry shares, so a retry after a
    lost acceptance gets the original one back instead of sending the message twice. Rejections
    become the same non-retryable errors as the harness's ``run_subagent_turn``.
    """
    info = activity.info()
    client = AgentClient(activity.client(), req.child_workflow_id)
    try:
        return await client._submit_message(
            req.type,
            req.payload,
            update_id=f"subagent-send:{info.workflow_run_id}:{info.activity_id}",
        )
    except MidTurnRejectedError as e:
        raise ApplicationError(str(e), type="MidTurnRejected", non_retryable=True) from e
    except WorkflowUpdateFailedError as e:
        cause = e.cause
        raise ApplicationError(
            str(cause) if cause else "subagent rejected the message",
            type=getattr(cause, "type", None) or "SubagentSendRejected",
            non_retryable=True,
        ) from e
    except RPCError as e:
        # The parent started this child, so a missing workflow is one that has already ended.
        if e.status != RPCStatusCode.NOT_FOUND:
            raise
        raise ApplicationError(
            f"subagent workflow {req.child_workflow_id!r} is no longer running",
            type="AgentClosed",
            non_retryable=True,
        ) from e


class SubagentSignalRunner(AgentWorkflowRunner):
    """An ``AgentWorkflowRunner`` whose subagent turns wait for a reply signal, not an activity."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Parent: replies from children, by message_id.
        self._subagent_replies: dict[str, SubagentReply] = {}
        # Child: each handled message's outcome, by message_id, until it is signalled.
        self._handled: dict[str, tuple[Literal["ok", "error"], dict[str, Any] | str]] = {}
        workflow.set_signal_handler(SUBAGENT_REPLY_SIGNAL, self._on_subagent_reply)

    def _on_subagent_reply(self, reply: dict[str, Any]) -> None:
        # A plain dict on the wire: the parent's and the child's clients use different data
        # converters (OpenAI Agents vs Strands).
        parsed = SubagentReply.model_validate(reply)
        self._subagent_replies[parsed.message_id] = parsed

    # --- Parent half -----------------------------------------------------------------------

    async def run_subagent_turn(
        self, handle: str, msg_type: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        """Drive one turn of a subagent and return its reply ``output``.

        As the harness's method, except the turn is a short send activity plus a durable wait for
        the child's reply signal, so nothing is in flight on this Worker while the child works.
        """
        inst = self._status.subagent(handle)  # raises UnknownSubagent
        ticket = inst.take_ticket()  # synchronous → FIFO admission in call order
        await workflow.wait_condition(lambda: inst.is_serving(ticket) or self._closed)
        if not inst.is_serving(ticket):
            raise ApplicationError(
                f"agent closed before subagent {handle!r} turn {ticket} could run",
                type="AgentClosed",
                non_retryable=True,
            )
        try:
            if self.current_stream_context is None:
                raise ApplicationError(
                    "run_subagent_turn called with no active turn to publish against",
                    type="NoActiveTurn",
                    non_retryable=True,
                )
            try:
                accepted = await workflow.execute_activity(
                    SEND_SUBAGENT_MESSAGE_ACTIVITY,
                    SendSubagentMessageInput(
                        child_workflow_id=inst.workflow_id, type=msg_type, payload=payload
                    ),
                    start_to_close_timeout=SUBAGENT_SEND_TIMEOUT,
                    result_type=AgentMessageReply,
                )
            except ActivityError as activity_error:
                e = activity_error.cause
                if isinstance(e, ApplicationError) and e.type == "AgentClosed":
                    # Something other than this parent ended the child: drop the handle.
                    self._forget_closed_subagent(inst)
                    raise ApplicationError(
                        f"Subagent {handle!r} has closed and accepts no more messages. "
                        f"Start a new one with start_{inst.agent_key}.",
                        {"subagent_id": handle},
                        type="AgentClosed",
                        non_retryable=True,
                    ) from e
                raise
            # Published in the workflow, so a retried send can't duplicate it. The child's
            # accepted_offset is at or before its turn_started, which is what a client merge needs.
            self.publish(
                SubagentMessageSent(
                    subagent_id=inst.handle,
                    agent_key=inst.agent_key,
                    workflow_id=inst.workflow_id,
                    handler=msg_type,
                    subagent_turn=accepted.turn_number,
                    from_offset=accepted.accepted_offset,
                )
            )

            message_id = accepted.message_id
            try:
                await workflow.wait_condition(
                    lambda: message_id in self._subagent_replies or self._closed,
                    timeout=SUBAGENT_REPLY_TIMEOUT,
                )
            except TimeoutError:
                self._publish_subagent_reply_received(
                    inst, msg_type, accepted.turn_number, outcome="error"
                )
                raise ApplicationError(
                    f"subagent turn {accepted.turn_number} sent no reply within "
                    f"{SUBAGENT_REPLY_TIMEOUT}",
                    {"subagent_turn": accepted.turn_number},
                    type="SubagentNoReply",
                    non_retryable=True,
                ) from None
            reply = self._subagent_replies.pop(message_id, None)
            if reply is None:
                # Woke because this agent closed.
                raise ApplicationError(
                    f"agent closed while waiting for subagent {handle!r}",
                    type="AgentClosed",
                    non_retryable=True,
                )

            inst.last_consumed_offset = reply.next_offset
            if reply.outcome == "error":
                self._publish_subagent_reply_received(
                    inst, msg_type, accepted.turn_number, outcome="error"
                )
                raise ApplicationError(
                    reply.error or "subagent turn failed",
                    {"subagent_turn": accepted.turn_number},
                    type="SubagentTurnError",
                    non_retryable=True,
                )
            self._publish_subagent_reply_received(
                inst, msg_type, accepted.turn_number, outcome="ok"
            )
            return reply.output or {}
        finally:
            inst.release_gate()

    # --- Child half ------------------------------------------------------------------------

    async def _dispatch_turn(self, agent: object, admitted: Any, *, joined: bool) -> BaseModel:
        """Run the handler as the harness does, recording its outcome for the reply signal."""
        try:
            result = await super()._dispatch_turn(agent, admitted, joined=joined)
        except Exception as e:  # the harness surfaces any handler failure; so does the reply
            self._handled[admitted.message_id] = ("error", str(e))
            raise
        self._handled[admitted.message_id] = ("ok", result.model_dump(mode="json"))
        return result

    async def _run_participant(self, agent: object, admitted: Any, *, joined: bool) -> None:
        """Handle the message as the harness does, then signal the parent with the outcome."""
        await super()._run_participant(agent, admitted, joined=joined)
        handled = self._handled.pop(admitted.message_id, None)
        parent = workflow.info().parent
        if handled is None or parent is None:
            return
        outcome, value = handled
        reply = SubagentReply(
            message_id=admitted.message_id,
            outcome=outcome,
            output=value if outcome == "ok" else None,
            error=value if outcome == "error" else None,
            next_offset=self._stream._on_offset(),
        )
        try:
            await workflow.get_external_workflow_handle(parent.workflow_id).signal(
                SUBAGENT_REPLY_SIGNAL, reply.model_dump(mode="json")
            )
        except FailureError:
            # The parent has gone (it would normally have terminated this child with it).
            workflow.logger.warning(
                "could not signal parent %s with the reply to %s",
                parent.workflow_id,
                admitted.message_id,
            )
