# Issue draft (temporal-agent-kit): a subagent turn keeps the parent's Worker busy

Draft of an issue for temporal-agent-kit, from the order-support spike
(`order-support-subagent-spike.md`). Not filed yet. Item 1 in [`agent-kit-serverless-gaps.md`](agent-kit-serverless-gaps.md).

---

## Title

Subagent turns block in an activity, so a parent's Worker can't scale to zero while a subagent waits

## Summary

A parent agent drives each subagent turn through the `run_subagent_turn` activity. The activity
sends the message to the child, then holds open until the child's turn ends. If the child waits
on a person, for example a tool approval, the parent has an activity in flight for that whole time.

With serverless Workers (Temporal Serverless Workers on AWS Lambda or Amazon Bedrock AgentCore
Runtime), a Worker drains once nothing is in flight. So the parent's Worker and its compute stay
up, and are billed, for as long as the child waits: up to the activity's one-hour
`start_to_close_timeout`. The child's own Worker drains normally, because the child is waiting
inside its workflow.

The proposal: the parent waits for the reply inside its workflow, and the child signals the parent
when the turn ends.

## Where it is (temporal-agent-kit 0.12.0, Python)

- `agent_workflow.py`, `AgentWorkflowRunner.run_subagent_turn`: every `<key>_<handler>` tool from
  `subagent_toolset` calls `workflow.execute_activity(RUN_SUBAGENT_TURN_ACTIVITY, ...)` with
  `start_to_close_timeout=DEFAULT_SUBAGENT_START_TO_CLOSE_TIMEOUT` (1 hour) and a heartbeat
  timeout.
- `subagent_activities.py`, `SubagentActivities.run_subagent_turn`: sends the
  `send_agent_message` update (`_submit`), then subscribes to the child's stream with no timeout
  (`_consume_child_turn`) until the turn's `turn_end`, heartbeating at a steady interval.
- The header's DESIGN note says this is deliberate: "we NEVER cap how long we wait for the
  subagent's terminal reply".

The same design is in `temporal-agent-harness` 0.11.1.

## Reproduce

1. A parent agent wires a child with `subagent_toolset`. The child has an approval-gated tool
   (`ToolApprovalPolicy.allow_inherently_safe()` plus a tool that isn't inherently safe).
2. Run each agent's Worker as a serverless Worker that drains once no activity has run for a
   short window. We ran a 60s debounce on AgentCore Runtime, one runtime per agent, on separate
   task queues.
3. Ask the parent for something that makes the child call the gated tool. Don't approve it.

Result: the child's Worker drains after 60s; the parent's Worker never drains, because
`run_subagent_turn` is in flight. Approving within the hour ends the turn, and the parent drains
60s later. Not approving keeps the parent's Worker up for the full hour, and then the turn fails
with an activity timeout.

## Proposal: wait in the parent's workflow; the child signals the reply

Split the turn into a short send and a durable wait:

1. **Send (short activity).** As `_submit` does today: send the `send_agent_message` update to the
   child, keyed on the activity's identity so retries don't send twice, and return once the child
   accepts. The acceptance gives the `message_id`, `turn_id` and `turn_number`. Publish the
   `subagent_message_sent` marker here, or from the workflow after the activity returns.
2. **Reply (signal from the child).** When the child finishes handling a message that came from
   its parent, it signals the parent with `{message_id, turn_number, output}` or the error. The
   child is a real child workflow (`start_child_workflow`), so it can reach the parent through
   `workflow.info().parent` and `workflow.get_external_workflow_handle(...)` with no activity.
   The envelope could carry an explicit `reply_to` instead, so the same works for non-child
   subagents.
3. **Wait (in the workflow).** The parent's `run_subagent_turn` waits with
   `workflow.wait_condition` for the signal carrying its `message_id`. The wait is durable and
   holds nothing on a Worker. A workflow timer can replace the activity's one-hour ceiling, or the
   wait can be uncapped, matching today's "never cap" intent without holding compute.

Points to keep from today's design:
- **Selecting by `message_id`.** A child turn is refcounted: a person's mid-turn message can join
  the turn the parent opened. The signal must carry the `message_id`, and the parent must match on
  it, as `_consume_child_turn` does now.
- **Ordering of the UI markers.** `subagent_reply_received` must still follow the child's
  `turn_end`. The child can send the signal after it publishes `turn_end`. The client-side merge
  gate in `stream_merge/gates.py` would keep working.
- **Errors.** The child's `message_handler_error` for that message becomes an error signal, and
  the parent raises the same non-retryable `SubagentTurnError` the tool layer renders today. A
  turn that ends without a reply for the message becomes `SubagentNoReply`, as now.
- **Child closed or gone.** The parent's wait should also wake when the child workflow closes, so
  a parent never waits forever on a child that can't answer.

What it buys: neither agent holds a Worker while a person decides. When the signal arrives, the
parent's workflow gets a workflow task, the server invokes the parent's serverless Worker, and the
turn continues on whichever Worker picks it up. That is the same path a tool approval already
takes in a single agent after its Worker drains.

## Workaround today

An app can subclass `AgentWorkflowRunner`. It overrides `run_subagent_turn` (send activity, then
wait for a signal) and the child's private `_dispatch_turn` and `_run_participant` hooks (signal
the parent). That's what our demo does (`order_support/subagent_signals.py`). It works, but it
copies harness code and depends on private methods, so it belongs in the kit.
