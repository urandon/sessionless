#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd -P)
versions_file="$repo_root/tools/repowise/versions.env"
sandbox_profile="$repo_root/tools/repowise/no-network.sb"
smoke_script="$repo_root/tools/repowise/mcp_smoke.py"
lock_file="$repo_root/tools/repowise/pylock.darwin-arm64.toml"
runtime_root="$repo_root/.local/repowise"
venv_root="$runtime_root/venv"
home_root="$runtime_root/home"
tmp_root="$runtime_root/tmp"
xdg_config_root="$runtime_root/xdg/config"
xdg_cache_root="$runtime_root/xdg/cache"
xdg_data_root="$runtime_root/xdg/data"
downloads_root="$runtime_root/downloads"
evidence_root="$runtime_root/evidence"
pid_root="$runtime_root/pids"
mcp_pid_file="$pid_root/mcp.pid"

# shellcheck disable=SC1090
. "$versions_file"

safe_system_path=/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin
repowise_bin="$venv_root/bin/repowise"
python_bin="$venv_root/bin/python3"
synthetic_path="$venv_root/bin:$safe_system_path"

usage() {
  cat <<'USAGE'
Usage: ./scripts/repowise-local.sh COMMAND

Opt-in local commands:
  install         networked install of the audited RepoWise wheel
  index           build a keyless, offline index for the exact clean HEAD
  update          update the offline index to the exact clean HEAD
  status          show offline RepoWise status for this repository
  doctor          run read-only RepoWise diagnostics offline
  mcp             run foreground stdio MCP with the fixed read-only allowlist
  mcp-smoke       verify MCP startup and exact tool exposure offline
  evaluate        emit local size/version/freshness evidence
  stop            stop the wrapper-managed foreground MCP process, if present
  uninstall-plan  print the exact repo-local paths that uninstall removes
  uninstall       remove those paths after typed confirmation

For uninstall, set REPOWISE_UNINSTALL_CONFIRM=sessionless:<current HEAD>.
USAGE
}

die() {
  printf 'repowise-local: %s\n' "$*" >&2
  exit 1
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

assert_repo_root() {
  actual_root=$(git -C "$repo_root" rev-parse --show-toplevel 2>/dev/null) ||
    die "not a Git checkout: $repo_root"
  actual_root=$(CDPATH= cd -- "$actual_root" && pwd -P)
  test "$actual_root" = "$repo_root" ||
    die "wrapper must target the Sessionless repository root: $repo_root"
}

assert_local_paths_safe() {
  for path in "$repo_root/.local" "$runtime_root" "$repo_root/.repowise"; do
    test ! -L "$path" || die "refusing symlinked repo-local path: $path"
  done
  if test -e "$repo_root/.local"; then
    physical=$(CDPATH= cd -- "$repo_root/.local" && pwd -P) ||
      die "cannot resolve $repo_root/.local"
    test "$physical" = "$repo_root/.local" || die ".local resolves outside the repository"
  fi
  if test -e "$runtime_root"; then
    physical=$(CDPATH= cd -- "$runtime_root" && pwd -P) ||
      die "cannot resolve $runtime_root"
    test "$physical" = "$runtime_root" || die "RepoWise runtime resolves outside the repository"
  fi
  if test -e "$repo_root/.repowise"; then
    test -d "$repo_root/.repowise" || die ".repowise exists but is not a directory"
    physical=$(CDPATH= cd -- "$repo_root/.repowise" && pwd -P) ||
      die "cannot resolve $repo_root/.repowise"
    test "$physical" = "$repo_root/.repowise" || die "RepoWise index resolves outside the repository"
  fi
}

assert_clean_tracked_checkout() {
  git -C "$repo_root" diff --quiet --ignore-submodules -- ||
    die "tracked working tree changes present; commit or stash them before RepoWise analysis"
  git -C "$repo_root" diff --cached --quiet --ignore-submodules -- ||
    die "staged changes present; commit or unstage them before RepoWise analysis"
}

assert_platform() {
  test "$(uname -s)" = "$REPOWISE_PLATFORM_SYSTEM" ||
    die "this evaluation is pinned to $REPOWISE_PLATFORM_SYSTEM"
  test "$(uname -m)" = "$REPOWISE_PLATFORM_MACHINE" ||
    die "this evaluation is pinned to $REPOWISE_PLATFORM_MACHINE"
}

assert_host_python() {
  test -x "$REPOWISE_PYTHON" || die "required Python is missing: $REPOWISE_PYTHON"
  actual_series=$(
    "$REPOWISE_PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")'
  )
  test "$actual_series" = "$REPOWISE_PYTHON_SERIES" ||
    die "expected Python $REPOWISE_PYTHON_SERIES, got $actual_series"
}

prepare_runtime_dirs() {
  umask 077
  mkdir -p \
    "$home_root" "$tmp_root" "$xdg_config_root" "$xdg_cache_root" \
    "$xdg_data_root" "$downloads_root" "$evidence_root" "$pid_root"
}

assert_installed() {
  test -x "$repowise_bin" ||
    die "RepoWise is not installed; run ./scripts/repowise-local.sh install"
  actual_version=$(
    run_sanitized "$python_bin" -c \
      'from importlib.metadata import version; print(version("repowise"))'
  )
  test "$actual_version" = "$REPOWISE_VERSION" ||
    die "expected RepoWise $REPOWISE_VERSION, got $actual_version"
}

assert_no_saved_key() {
  test ! -e "$repo_root/.repowise/.env" ||
    die "refusing to run while .repowise/.env exists; remove the saved provider credential"
}

current_head() {
  git -C "$repo_root" rev-parse HEAD
}

indexed_head() {
  state_file="$repo_root/.repowise/state.json"
  test -f "$state_file" || die "RepoWise index state is missing; run index first"
  run_sanitized "$python_bin" -c \
    'import json,sys; value=json.load(open(sys.argv[1], encoding="utf-8")).get("last_sync_commit"); print(value or "")' \
    "$state_file"
}

assert_fresh_index() {
  expected_head=$(current_head)
  actual_head=$(indexed_head)
  test "$actual_head" = "$expected_head" ||
    die "RepoWise index is stale: indexed ${actual_head:-unknown}, checkout $expected_head; run update"
}

run_sanitized() {
  env -i \
    HOME="$home_root" \
    TMPDIR="$tmp_root" \
    XDG_CONFIG_HOME="$xdg_config_root" \
    XDG_CACHE_HOME="$xdg_cache_root" \
    XDG_DATA_HOME="$xdg_data_root" \
    PATH="$synthetic_path" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    DO_NOT_TRACK=1 \
    REPOWISE_TELEMETRY_DISABLED=1 \
    REPOWISE_SKIP_EDITOR_SETUP=1 \
    REPOWISE_NO_SAVE_KEY=1 \
    REPOWISE_NO_COST_TRACKING=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONNOUSERSITE=1 \
    PIP_CONFIG_FILE=/dev/null \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    GIT_CONFIG_NOSYSTEM=1 \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_TERMINAL_PROMPT=0 \
    NO_COLOR=1 \
    "$@"
}

run_offline() {
  test -x /usr/bin/sandbox-exec || die "sandbox-exec is required for offline analysis"
  run_sanitized /usr/bin/sandbox-exec -f "$sandbox_profile" "$@"
}

install_repowise() {
  assert_platform
  assert_host_python
  test ! -e "$venv_root" || die "local environment already exists: $venv_root"
  prepare_runtime_dirs

  actual_lock_digest=$(sha256_file "$lock_file")
  test "$actual_lock_digest" = "$REPOWISE_LOCK_SHA256" ||
    die "RepoWise lock digest mismatch: expected $REPOWISE_LOCK_SHA256, got $actual_lock_digest"
  actual_package_count=$(grep -c '^\[\[packages\]\]' "$lock_file")
  actual_wheel_count=$(grep -c '^\[\[packages\.wheels\]\]' "$lock_file")
  test "$actual_package_count" = "$REPOWISE_LOCK_PACKAGE_COUNT" ||
    die "RepoWise lock package count mismatch"
  test "$actual_wheel_count" = "$REPOWISE_LOCK_PACKAGE_COUNT" ||
    die "RepoWise lock must select exactly one wheel per package"
  grep -Fq "sha256 = \"$REPOWISE_WHEEL_SHA256\"" "$lock_file" ||
    die "RepoWise top-level wheel hash is absent from the lock"

  run_sanitized "$REPOWISE_PYTHON" -m venv "$venv_root"
  actual_pip_version=$(
    run_sanitized "$REPOWISE_PYTHON" -m pip --version | awk '{print $2}'
  )
  test "$actual_pip_version" = "$REPOWISE_LOCK_PIP_VERSION" ||
    die "lock download requires pip $REPOWISE_LOCK_PIP_VERSION, got $actual_pip_version"
  run_sanitized "$REPOWISE_PYTHON" -m pip download \
    --disable-pip-version-check \
    --only-binary=:all: \
    --no-proxy-env \
    --no-cache-dir \
    --dest "$downloads_root" \
    --requirement "$lock_file"

  set -- "$downloads_root"/*.whl
  test "$#" -eq "$REPOWISE_LOCK_PACKAGE_COUNT" ||
    die "expected $REPOWISE_LOCK_PACKAGE_COUNT locked wheels, got $#"

  # Download is the only networked phase. Installation consumes only the
  # hash-verified, complete wheelhouse and performs no dependency resolution.
  run_offline "$venv_root/bin/python3" -m pip install \
    --disable-pip-version-check \
    --no-index \
    --no-deps \
    "$@"
  run_offline "$venv_root/bin/python3" -m pip check
  run_sanitized "$venv_root/bin/python3" -m pip freeze --all \
    >"$evidence_root/requirements-resolved.txt"
  run_sanitized "$venv_root/bin/python3" --version >"$evidence_root/python-version.txt" 2>&1
  : >"$evidence_root/wheelhouse.sha256"
  for wheel in "$downloads_root"/*.whl; do
    printf '%s  %s\n' "$(sha256_file "$wheel")" "$(basename "$wheel")" \
      >>"$evidence_root/wheelhouse.sha256"
  done
  run_offline "$repowise_bin" --version
}

index_repowise() {
  assert_clean_tracked_checkout
  assert_no_saved_key
  test ! -e "$repo_root/.repowise" ||
    die ".repowise already exists; use update or uninstall it explicitly first"
  run_offline "$repowise_bin" init "$repo_root" \
    --yes \
    --no-prose \
    --mode standard \
    --no-editor-setup \
    --no-save-key \
    --no-workspace \
    --no-seed \
    --no-claude-md \
    --no-agents \
    --no-codex \
    --no-distill-hook \
    --no-cost-tracking \
    --commit-limit 500 \
    --progress json \
    -x .gitcode/ \
    -x .local/
  assert_no_saved_key
  assert_fresh_index
  current_head >"$evidence_root/indexed-head.txt"
}

update_repowise() {
  assert_clean_tracked_checkout
  assert_no_saved_key
  run_offline "$repowise_bin" update "$repo_root" \
    --index-only \
    --no-workspace \
    --no-cost-tracking \
    --no-agents \
    --progress json
  assert_no_saved_key
  assert_fresh_index
  current_head >"$evidence_root/indexed-head.txt"
}

start_mcp() {
  test ! -f "$mcp_pid_file" || die "MCP pid file already exists; run stop first"
  env -i \
    HOME="$home_root" TMPDIR="$tmp_root" \
    XDG_CONFIG_HOME="$xdg_config_root" XDG_CACHE_HOME="$xdg_cache_root" \
    XDG_DATA_HOME="$xdg_data_root" PATH="$synthetic_path" \
    LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    DO_NOT_TRACK=1 REPOWISE_TELEMETRY_DISABLED=1 \
    REPOWISE_SKIP_EDITOR_SETUP=1 REPOWISE_NO_SAVE_KEY=1 \
    REPOWISE_NO_COST_TRACKING=1 PYTHONDONTWRITEBYTECODE=1 \
    PYTHONNOUSERSITE=1 PIP_CONFIG_FILE=/dev/null \
    PIP_DISABLE_PIP_VERSION_CHECK=1 GIT_CONFIG_NOSYSTEM=1 \
    GIT_CONFIG_GLOBAL=/dev/null GIT_TERMINAL_PROMPT=0 NO_COLOR=1 \
    /usr/bin/sandbox-exec -f "$sandbox_profile" \
    "$repowise_bin" mcp "$repo_root" --transport stdio --tools "$REPOWISE_MCP_ALLOWED_TOOLS" &
  mcp_pid=$!
  printf '%s\n' "$mcp_pid" >"$mcp_pid_file"
  trap 'kill "$mcp_pid" 2>/dev/null || true; rm -f "$mcp_pid_file"' EXIT HUP INT TERM
  wait "$mcp_pid"
}

stop_mcp() {
  test -f "$mcp_pid_file" || {
    printf '%s\n' 'RepoWise MCP is not running under this wrapper.'
    return 0
  }
  pid=$(sed -n '1p' "$mcp_pid_file")
  case "$pid" in
    ''|*[!0-9]*) die "invalid MCP pid file: $mcp_pid_file" ;;
  esac
  if ! kill -0 "$pid" 2>/dev/null; then
    rm -f "$mcp_pid_file"
    printf '%s\n' 'Removed stale RepoWise MCP pid file.'
    return 0
  fi
  command_line=$(ps -p "$pid" -o command= 2>/dev/null || true)
  case "$command_line" in
    *"$sandbox_profile"*"$repowise_bin"*) ;;
    *) die "pid $pid is not the expected repo-local sandboxed RepoWise MCP process" ;;
  esac
  kill -TERM "$pid"
  attempts=0
  while kill -0 "$pid" 2>/dev/null && test "$attempts" -lt 50; do
    /bin/sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -KILL "$pid"
    attempts=0
    while kill -0 "$pid" 2>/dev/null && test "$attempts" -lt 20; do
      /bin/sleep 0.1
      attempts=$((attempts + 1))
    done
  fi
  kill -0 "$pid" 2>/dev/null && die "RepoWise MCP process $pid survived stop"
  rm -f "$mcp_pid_file"
}

evaluate_repowise() {
  assert_fresh_index
  index_kib=$(du -sk "$repo_root/.repowise" | awk '{print $1}')
  env_kib=$(du -sk "$venv_root" | awk '{print $1}')
  head=$(current_head)
  version=$(run_offline "$repowise_bin" --version | tr '\n' ' ' | sed 's/[[:space:]]*$//')
  printf '{"schema_version":1,"source_head":"%s","indexed_head":"%s","repowise_version":"%s","environment_kib":%s,"index_kib":%s,"network_policy":"deny","mcp_tools":"%s"}\n' \
    "$head" "$head" "$version" "$env_kib" "$index_kib" "$REPOWISE_MCP_ALLOWED_TOOLS"
}

uninstall_plan() {
  head=$(current_head)
  printf '%s\n' \
    "RepoWise uninstall plan (repo-local only):" \
    "  $repo_root/.repowise" \
    "  $runtime_root" \
    "Confirmation: REPOWISE_UNINSTALL_CONFIRM=sessionless:$head"
}

uninstall_repowise() {
  expected="sessionless:$(current_head)"
  test "${REPOWISE_UNINSTALL_CONFIRM:-}" = "$expected" ||
    die "typed confirmation must equal $expected; run uninstall-plan first"
  test ! -f "$mcp_pid_file" || die "stop the wrapper-managed MCP process before uninstall"
  test "$runtime_root" = "$repo_root/.local/repowise" || die "unsafe runtime path"
  rm -rf "$repo_root/.repowise" "$runtime_root"
  printf '%s\n' 'Removed the repo-local RepoWise index and environment.'
}

assert_repo_root
assert_local_paths_safe
umask 077
command=${1:-help}
test "$#" -le 1 || die "commands do not accept additional arguments"

case "$command" in
  help|-h|--help)
    usage
    ;;
  install)
    install_repowise
    ;;
  uninstall-plan)
    uninstall_plan
    ;;
  uninstall)
    uninstall_repowise
    ;;
  stop)
    stop_mcp
    ;;
  index|update|status|doctor|mcp|mcp-smoke|evaluate)
    assert_platform
    prepare_runtime_dirs
    assert_installed
    assert_clean_tracked_checkout
    assert_no_saved_key
    case "$command" in
      index) index_repowise ;;
      update) update_repowise ;;
      status)
        assert_fresh_index
        run_offline "$repowise_bin" status "$repo_root" --no-workspace --format json
        ;;
      doctor)
        assert_fresh_index
        run_offline "$repowise_bin" doctor "$repo_root" --no-workspace --format json
        ;;
      mcp)
        assert_clean_tracked_checkout
        assert_no_saved_key
        assert_fresh_index
        start_mcp
        ;;
      mcp-smoke)
        assert_clean_tracked_checkout
        assert_no_saved_key
        assert_fresh_index
        run_offline "$python_bin" "$smoke_script" \
          "$repowise_bin" "$repo_root" "$REPOWISE_MCP_ALLOWED_TOOLS"
        ;;
      evaluate) evaluate_repowise ;;
    esac
    ;;
  *)
    usage >&2
    die "unknown command: $command"
    ;;
esac
