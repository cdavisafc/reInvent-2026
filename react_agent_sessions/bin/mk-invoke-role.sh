#!/bin/bash
set -euo pipefail

# ABOUTME: Creates or updates the CloudFormation stack for the IAM role Temporal assumes to invoke this runtime.

# Creates (or updates in place) the IAM role that Temporal's Serverless Worker
# invoker assumes to invoke your AgentCore runtime. The External ID is a value
# you choose (e.g. `uuidgen`); pass the same value to `temporal worker
# deployment create-version --aws-agentcore-assume-role-external-id`.
#
# The optional fourth argument is a comma-separated list of the Temporal
# invoker role ARNs the role trusts. It defaults to Temporal Cloud production's
# (see the template). Other Temporal environments, such as the staging cell,
# use their own invoker roles; get them from whoever runs that environment.
#
# This is not the runtime's execution role: `agentcore deploy` creates that one.
# Waits until the stack is created or updated.

USAGE="Usage: mk-invoke-role.sh <stack-name> <external-id> <agent-runtime-arn> [<temporal-invoker-role-arns>]"
STACK_NAME="${1:?$USAGE}"
EXTERNAL_ID="${2:?$USAGE}"
RUNTIME_ARN="${3:?$USAGE}"
INVOKER_ROLE_ARNS="${4:-}"
SAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

PARAMETERS=(
  "AssumeRoleExternalId=$EXTERNAL_ID"
  "AgentRuntimeARNs=$RUNTIME_ARN"
)
if [ -n "$INVOKER_ROLE_ARNS" ]; then
  PARAMETERS+=("TemporalInvokerRoleARNs=$INVOKER_ROLE_ARNS")
fi

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$SAMPLE_DIR/iam-role-for-temporal-agentcore-invoke.yaml" \
  --parameter-overrides "${PARAMETERS[@]}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset
