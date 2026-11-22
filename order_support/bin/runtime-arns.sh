#!/bin/bash
set -euo pipefail

# ABOUTME: Prints each AgentCore runtime in agentcore/agentcore.json with its deployed ARN,
# Temporal Worker Deployment name and build ID, one runtime per line, tab-separated.

# Reads the ARNs from the CloudFormation stack `agentcore deploy` creates
# (AgentCore-<project>-<target>). Output columns:
#   <runtime name> <runtime ARN> <TEMPORAL_DEPLOYMENT_NAME> <TEMPORAL_BUILD_ID>

TARGET="${1:-default}"
SAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# One line per runtime: name, CloudFormation output-key prefix, deployment name, build ID, region.
python3 - "$SAMPLE_DIR/agentcore" "$TARGET" <<'PY' |
import json, sys
from pathlib import Path

config_dir, target = Path(sys.argv[1]), sys.argv[2]
project = json.loads((config_dir / "agentcore.json").read_text())
region = next(t["region"] for t in json.loads((config_dir / "aws-targets.json").read_text())
              if t["name"] == target)
stack = f"AgentCore-{project['name']}-{target}"
for runtime in project["runtimes"]:
    env = {var["name"]: var["value"] for var in runtime["envVars"]}
    pascal = "".join(part.capitalize() for part in runtime["name"].split("_"))
    print(runtime["name"], stack, f"ApplicationRuntime{pascal}RuntimeArnOutput",
          env["TEMPORAL_DEPLOYMENT_NAME"], env.get("TEMPORAL_BUILD_ID", "1.0.0"), region,
          sep="\t")
PY
while IFS=$'\t' read -r name stack output_prefix deployment build_id region; do
  arn="$(aws cloudformation describe-stacks --region "$region" --stack-name "$stack" \
    --query "Stacks[0].Outputs[?starts_with(OutputKey, \`$output_prefix\`)].OutputValue" \
    --output text </dev/null)"
  if [ -z "$arn" ] || [ "$arn" = "None" ]; then
    echo "No runtime ARN for '$name' in stack $stack; has it been deployed?" >&2
    exit 1
  fi
  printf '%s\t%s\t%s\t%s\n' "$name" "$arn" "$deployment" "$build_id"
done
