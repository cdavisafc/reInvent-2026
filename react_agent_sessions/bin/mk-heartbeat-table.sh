#!/bin/bash
set -euo pipefail

# ABOUTME: Creates or updates the CloudFormation stack for the DynamoDB table that holds AgentCore session heartbeats.

# The runtime writes one item per AgentCore session to this table (session_heartbeat.py), and
# the local status page reads it (status_page.py). The runtime's execution role gets write
# access through session-heartbeat-policy.json, listed in agentcore/agentcore.json.
#
# Run it before deploying the runtime. Waits until the stack is created or updated.

USAGE="Usage: mk-heartbeat-table.sh [<stack-name>] [<table-name>]"
STACK_NAME="${1:-react-agent-sessions-heartbeats}"
TABLE_NAME="${2:-react-agent-sessions-heartbeats}"
SAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ "$#" -gt 2 ]; then
  echo "$USAGE" >&2
  exit 1
fi

aws cloudformation deploy \
  --stack-name "$STACK_NAME" \
  --template-file "$SAMPLE_DIR/session-heartbeats-table.yaml" \
  --parameter-overrides "TableName=$TABLE_NAME" \
  --no-fail-on-empty-changeset
