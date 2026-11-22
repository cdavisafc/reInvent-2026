#!/bin/bash
set -euo pipefail

# ABOUTME: Creates or updates the CloudFormation stack for the DynamoDB table that holds AgentCore session heartbeats.

# Every agent's AgentCore runtimes write one item per session to this one table (each project's
# session_heartbeat.py), and the local status page reads it (status_page.py). Each runtime's
# execution role gets write access through its project's session-heartbeat-policy.json, listed in
# that project's agentcore/agentcore.json.
#
# Run it once, before deploying any runtime. Waits until the stack is created or updated.

USAGE="Usage: mk-heartbeat-table.sh [<stack-name>] [<table-name>]"
STACK_NAME="${1:-agent-session-heartbeats}"
TABLE_NAME="${2:-agent-session-heartbeats}"
STATUS_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ "$#" -gt 2 ]; then
  echo "$USAGE" >&2
  exit 1
fi

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$STATUS_DIR/session-heartbeats-table.yaml" \
  --parameter-overrides "TableName=$TABLE_NAME" \
  --no-fail-on-empty-changeset
