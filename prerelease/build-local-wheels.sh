#!/usr/bin/env bash
# ABOUTME: Builds the unreleased temporalio (activity sessions) wheels that react_agent_sessions installs, into ./wheels/.
# Adapted from temporalio/samples-python@feature/activity-sessions bedrock_agentcore/build-local-wheels.sh.
#
# TEMPORARY: activity sessions (sticky activities) are not yet in a released
# temporalio. Delete this script, the wheels/ directory, and the
# `override-dependencies` entry in pyproject.toml once
# that release exists.
#
# Only temporalio is built. This project keeps temporal-agent-harness's vendored
# OpenAI Agents plugin (streaming hooks, harness MCP wrapper) and adds activity
# sessions with its own interceptor (activity_sessions.py), so the
# ai-integrations plugin wheel is not needed.
#
# Source: by default the script clones sdk-python into the gitignored
# ./.wheel-build/ scratch dir and checks out SDK_PYTHON_REF (branch, tag or SHA).
# Re-runs reuse the clone (fetch + checkout). The sdk-core submodule is
# initialised recursively. Point SDK_PYTHON_DIR at a local checkout instead to
# build from it as-is (only read, never modified).
#
# Why wheels, and why Linux: `agentcore deploy` (CodeZip) packages the runtime
# on this machine by running
#     uv pip install -r pyproject.toml --target <staging> --python-version 3.12 \
#        --python-platform aarch64-manylinux2014 --only-binary :all:
# so temporalio has to exist as a wheel installable on Linux arm64. It has a
# Rust extension and is built in a linux/arm64 manylinux2014 container. A macOS
# arm64 wheel is built too when a Rust toolchain is available; it is only needed
# for local `uv lock` / `uv sync` / tests on macOS, not for deploying.
#
# The build carries the local version label `+sessions` (e.g. 1.34.0+sessions):
# PyPI cannot host local versions, so it can never be confused with a PyPI
# release in uv's cache, a lock, or `pip list`. The label is applied to a
# scratch copy of the sources.
#
# Prerequisites: git, uv, and docker able to run linux/arm64 containers.
# Optional: a Rust toolchain (cargo on PATH or in $HOME/.cargo/bin) on macOS.
#
# Why this lives outside react_agent_sessions/: `agentcore deploy` zips the
# whole project directory, excluding only a fixed list (.git, .venv, ...), not
# .gitignore'd paths. Wheels and the build scratch dir (>1 GB) inside it would
# land in the runtime zip.
#
# Usage: ./build-local-wheels.sh            (from anywhere)
# Env overrides:
#   SDK_PYTHON_REPO   (default: https://github.com/temporalio/sdk-python.git)
#   SDK_PYTHON_REF    branch, tag or SHA (default: dev/sticky-activities)
#   SDK_CORE_REPO     URL for sdk-python's sdk-core submodule (default: the URL
#                     in sdk-python's .gitmodules)
#   SDK_PYTHON_DIR    use this local sdk-python checkout; skips the clone
#   SKIP_LINUX=1 / SKIP_MACOS=1   build a subset
#   MANYLINUX_IMAGE   (default: quay.io/pypa/manylinux2014_aarch64)
#   FORCE=1           rebuild even if wheels/BUILD_INFO shows the same commit
#   NO_RELOCK=1       skip re-locking ../react_agent_sessions after a build
#
# Safe to run before every deploy: if wheels/BUILD_INFO already records the
# resolved commit and the wheels exist, it exits without building.
#
# After a build it re-locks ../react_agent_sessions. Rust builds are not
# reproducible, so every build changes the wheels' hashes and `uv run` /
# `uv sync` fail with "Hash mismatch" until uv.lock is refreshed. The re-lock is
#     uv lock --no-cache --upgrade-package temporalio
# --upgrade-package makes uv re-hash temporalio (a plain `uv lock` keeps the old
# hashes). --no-cache uses a throwaway cache, because cleaning the shared uv
# cache blocks while any other `uv run` holds it. It needs the macOS wheel.
#
# The resolved commit is written to wheels/BUILD_INFO; include it when
# reporting results.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$HERE/wheels"
SCRATCH="$HERE/.wheel-build"
LOCAL_LABEL="sessions"
MANYLINUX_IMAGE="${MANYLINUX_IMAGE:-quay.io/pypa/manylinux2014_aarch64}"
SDK_PYTHON_REPO="${SDK_PYTHON_REPO:-https://github.com/temporalio/sdk-python.git}"
SDK_PYTHON_REF="${SDK_PYTHON_REF:-dev/sticky-activities}"

mkdir -p "$OUT" "$SCRATCH"

# fetch_source <repo> <ref> <dest>: clone <repo> into <dest> (or reuse an
# existing clone), check out <ref> detached, and init submodules recursively.
# A ref naming a remote branch resolves to its freshly fetched tip.
fetch_source() {
  local repo="$1" ref="$2" dest="$3"
  if [[ -d "$dest/.git" ]]; then
    echo "reusing clone $dest (fetching $repo)"
    git -C "$dest" remote set-url origin "$repo"
    git -C "$dest" fetch --tags --force --prune origin
  else
    rm -rf "$dest"
    git clone "$repo" "$dest"
  fi
  local target="$ref"
  if git -C "$dest" rev-parse --verify --quiet "refs/remotes/origin/$ref^{commit}" >/dev/null; then
    target="origin/$ref"
  fi
  if ! git -C "$dest" rev-parse --verify --quiet "$target^{commit}" >/dev/null; then
    echo "error: ref '$ref' not found in $repo" >&2
    exit 1
  fi
  git -C "$dest" -c advice.detachedHead=false checkout --force --detach "$target"
  git -C "$dest" submodule sync --recursive
  if [[ -n "${SDK_CORE_REPO:-}" ]]; then
    git -C "$dest" submodule init -- temporalio/bridge/sdk-core
    git -C "$dest" config submodule.sdk-core.url "$SDK_CORE_REPO"
    if [[ -e "$dest/temporalio/bridge/sdk-core/.git" ]]; then
      git -C "$dest/temporalio/bridge/sdk-core" remote set-url origin "$SDK_CORE_REPO"
    fi
  fi
  git -C "$dest" submodule update --init --recursive --force
}

if [[ "${SKIP_LINUX:-}" == "1" && "${SKIP_MACOS:-}" == "1" ]]; then
  echo "nothing to build (SKIP_LINUX=1 and SKIP_MACOS=1)" >&2
  exit 1
fi

if [[ -n "${SDK_PYTHON_DIR:-}" ]]; then
  SDK_SRC="$(cd "$SDK_PYTHON_DIR" && pwd)"
  SHA="$(git -C "$SDK_SRC" rev-parse HEAD)"
  DIRTY=""
  [[ -z "$(git -C "$SDK_SRC" status --porcelain --untracked-files=no)" ]] || DIRTY=" (uncommitted changes)"
  BUILD_INFO_SOURCE="sdk-python: local checkout $SDK_SRC @ $SHA$DIRTY"
else
  SDK_SRC="$SCRATCH/src/sdk-python"
  fetch_source "$SDK_PYTHON_REPO" "$SDK_PYTHON_REF" "$SDK_SRC"
  SHA="$(git -C "$SDK_SRC" rev-parse HEAD)"
  BUILD_INFO_SOURCE="sdk-python: $SDK_PYTHON_REPO ref=$SDK_PYTHON_REF commit=$SHA
  sdk-core submodule: $(git -C "$SDK_SRC" config submodule.sdk-core.url) commit=$(git -C "$SDK_SRC/temporalio/bridge/sdk-core" rev-parse HEAD)"
fi
echo "sdk-python: $SDK_SRC @ $SHA"

# wheels_current: true when wheels/BUILD_INFO records this exact clean commit
# and every wheel this run would build already exists. Rebuilding then would
# only produce byte-different wheels (Rust builds are not reproducible), which
# breaks the hashes in uv.lock for nothing.
wheels_current() {
  [[ -z "${DIRTY:-}" && -f "$OUT/BUILD_INFO" ]] || return 1
  grep -q -e "commit=$SHA" -e "@ $SHA\$" "$OUT/BUILD_INFO" || return 1
  if [[ "${SKIP_LINUX:-}" != "1" ]]; then
    compgen -G "$OUT/temporalio-*manylinux*.whl" >/dev/null || return 1
  fi
  if [[ "${SKIP_MACOS:-}" != "1" && "$(uname -s)" == "Darwin" ]]; then
    compgen -G "$OUT/temporalio-*macosx*.whl" >/dev/null || return 1
  fi
}

if [[ "${FORCE:-}" != "1" ]] && wheels_current; then
  echo "==> wheels already built from $SHA; nothing to do (FORCE=1 rebuilds)"
  cat "$OUT/BUILD_INFO"
  exit 0
fi

# Copy the SDK sources (tracked files incl. the sdk-core submodule, no build
# output) into $1 and stamp the local version label onto the copy.
stage_sdk() {
  local dest="$1"
  rm -rf "$dest"
  mkdir -p "$dest"
  (cd "$SDK_SRC" && git ls-files -z --recurse-submodules |
    tar -cf - --no-recursion --null -T -) |
    (cd "$dest" && tar -xf -)
  local version
  version="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$dest/pyproject.toml" | head -1)"
  sed -i.bak "s/^version = \"$version\"$/version = \"$version+$LOCAL_LABEL\"/" "$dest/pyproject.toml"
  rm -f "$dest/pyproject.toml.bak"
  echo "staged temporalio $version+$LOCAL_LABEL in $dest"
}

if [[ "${SKIP_LINUX:-}" != "1" ]]; then
  echo "==> temporalio (manylinux2014 aarch64, docker)"
  stage_sdk "$SCRATCH/sdk-linux"
  trap 'rm -rf "$SCRATCH/sdk-linux"' EXIT
  rm -f "$OUT"/temporalio-*manylinux*.whl
  # Named volumes cache rustup, the cargo registry and the target dir across runs.
  docker run --rm --platform linux/arm64 \
    -v "$SCRATCH/sdk-linux:/src" \
    -v "$OUT:/out" \
    -v temporalio-wheel-rustup:/root/.rustup \
    -v temporalio-wheel-cargo:/root/.cargo \
    -v temporalio-wheel-target:/src/temporalio/bridge/target \
    -e CARGO_NET_GIT_FETCH_WITH_CLI=true \
    "$MANYLINUX_IMAGE" bash -euo pipefail -c '
      export PATH="/root/.cargo/bin:/opt/python/cp312-cp312/bin:$PATH"
      command -v rustup >/dev/null || curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
      rustup default stable >/dev/null
      python -m pip install -q "maturin>=1.8.2,<2" protoc-wheel-0
      cd /src
      maturin build --release --compatibility manylinux2014 --interpreter python3.12 --out /out
    '
fi

if [[ "${SKIP_MACOS:-}" != "1" ]]; then
  MACOS_SKIP_HINT="local uv lock/sync and tests on macOS need it; deploying does not"
  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "==> skipping macOS temporalio wheel (not on macOS)"
  elif ! PATH="$HOME/.cargo/bin:$PATH" command -v cargo >/dev/null; then
    echo "==> skipping macOS temporalio wheel: no Rust toolchain (cargo) found on PATH or in \$HOME/.cargo/bin"
    echo "    ($MACOS_SKIP_HINT; install Rust from https://rustup.rs to build it)"
  else
    echo "==> temporalio (macOS arm64)"
    stage_sdk "$SCRATCH/sdk-macos"
    rm -f "$OUT"/temporalio-*macosx*.whl
    # A persistent cargo target dir (outside the staged copy) keeps rebuilds incremental.
    (cd "$SCRATCH/sdk-macos" &&
      PATH="$HOME/.cargo/bin:$PATH" \
      MACOSX_DEPLOYMENT_TARGET=11.0 \
      CARGO_TARGET_DIR="$SCRATCH/macos-target" \
      uv build --wheel --out-dir "$OUT")
    rm -rf "$SCRATCH/sdk-macos"
  fi
fi

{
  echo "built: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "$BUILD_INFO_SOURCE"
  echo "wheels (sha256):"
  (cd "$OUT" && for w in *.whl; do
    [[ -e "$w" ]] || continue
    if command -v sha256sum >/dev/null; then sha256sum "$w"; else shasum -a 256 "$w"; fi
  done)
} >"$OUT/BUILD_INFO"

if [[ "${NO_RELOCK:-}" != "1" ]]; then
  if compgen -G "$OUT/temporalio-*macosx*.whl" >/dev/null || [[ "$(uname -s)" != "Darwin" ]]; then
    echo "==> re-locking ../react_agent_sessions (new wheel hashes)"
    (cd "$HERE/../react_agent_sessions" && uv lock --no-cache --upgrade-package temporalio)
  else
    echo "==> not re-locking: no macOS wheel. Build it (needs cargo), then run in ../react_agent_sessions:"
    echo "    uv lock --no-cache --upgrade-package temporalio"
  fi
fi

echo "==> $OUT/BUILD_INFO:"
cat "$OUT/BUILD_INFO"
