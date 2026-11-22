#!/bin/bash
set -euo pipefail

# ABOUTME: Creates or updates this project's AgentCore runtimes and endpoints with the AgentCore CLI.

# Creates (or updates) the AgentCore runtimes for this project with the AgentCore
# CLI (v1.x), which zips this directory (CodeZip build) and deploys via CDK:
#   https://github.com/aws/agentcore-cli
#
# The runtimes are described by the checked-in agentcore/agentcore.json. Before the
# first run, set your account and region in agentcore/aws-targets.json and your
# Temporal connection details in each runtime's "envVars".

TARGET="${1:-default}"
SAMPLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SAMPLE_DIR"

# uv is easy to miss: the CDK build shells out to it to install dependencies for
# the runtime's platform, and if it is absent the subprocess is killed and the
# CLI reports only "uv install failed ... with exit code null".
for tool in agentcore aws uv; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "'$tool' is required but not installed." >&2
    [ "$tool" = agentcore ] && echo "Install it with: npm install -g @aws/agentcore" >&2
    [ "$tool" = uv ] && echo "Install it from: https://docs.astral.sh/uv/getting-started/installation/" >&2
    exit 1
  fi
done

# agentcore.json uses the v1.x project format ("version": 2); the v0 CLI cannot
# read it.
AGENTCORE_MAJOR="$(agentcore --version | cut -d. -f1)"
if [ "$AGENTCORE_MAJOR" -lt 1 ]; then
  echo "AgentCore CLI 1.x or later is required (found $(agentcore --version))." >&2
  echo "Upgrade it with: npm install -g @aws/agentcore@latest" >&2
  exit 1
fi

if [ ! -d agentcore/cdk ]; then
  echo "Bootstrapping the AgentCore CDK scaffold (one time)..." >&2
  PROJECT_NAME="$(python3 -c 'import json;print(json.load(open("agentcore/agentcore.json"))["name"])')"
  TMP_DIR="$(mktemp -d)"
  trap 'rm -rf "$TMP_DIR"' EXIT
  # `create` writes the project into ./<name> under the current directory.
  (cd "$TMP_DIR" && agentcore create --name "$PROJECT_NAME" --skip-git)
  mv "$TMP_DIR/$PROJECT_NAME/agentcore/cdk" agentcore/cdk
fi

agentcore deploy --target "$TARGET"

echo >&2
echo "Runtime deployed" >&2
