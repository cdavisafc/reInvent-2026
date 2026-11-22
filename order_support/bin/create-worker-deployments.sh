#!/bin/bash
set -euo pipefail

# ABOUTME: Creates (or updates) a Temporal Worker Deployment Version per AgentCore runtime and makes
# it current, so the Temporal server invokes each runtime when its task queue has work.

# Needs ROLE_ARN and EXTERNAL_ID (the invoke role from mk-invoke-role.sh) and the Temporal CLI
# pointed at your server and namespace (TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE / TEMPORAL_API_KEY,
# or a temporal.toml profile). Safe to re-run: an existing version gets its compute config updated.

TARGET="${1:-default}"
: "${ROLE_ARN:?export ROLE_ARN (the invoke role) first}"
: "${EXTERNAL_ID:?export EXTERNAL_ID (the external ID of the invoke role) first}"
SAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

RUNTIMES="$("$SAMPLE_DIR/bin/runtime-arns.sh" "$TARGET")"

while IFS=$'\t' read -r -u 3 name arn deployment build_id; do
  echo "== $name: $deployment/$build_id -> $arn" >&2
  compute=(
    --aws-agentcore-endpoint-arn "${arn}/runtime-endpoint/DEFAULT"
    --aws-agentcore-assume-role-arn "$ROLE_ARN"
    --aws-agentcore-assume-role-external-id "$EXTERNAL_ID"
  )
  if ! temporal worker deployment describe --name "$deployment" >/dev/null 2>&1; then
    temporal worker deployment create --name "$deployment"
  fi
  if temporal worker deployment describe-version \
      --deployment-name "$deployment" --build-id "$build_id" >/dev/null 2>&1; then
    temporal worker deployment update-version-compute-config \
      --deployment-name "$deployment" --build-id "$build_id" "${compute[@]}"
  else
    temporal worker deployment create-version \
      --deployment-name "$deployment" --build-id "$build_id" "${compute[@]}"
  fi
  # Until a version is current, nothing routes to the runtime.
  temporal worker deployment set-current-version \
    --deployment-name "$deployment" --build-id "$build_id" --allow-no-pollers --yes
done 3<<< "$RUNTIMES"
