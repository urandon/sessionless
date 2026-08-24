#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
wrapper="$repo_root/scripts/repowise-local.sh"
versions_file="$repo_root/tools/repowise/versions.env"
lock_file="$repo_root/tools/repowise/pylock.darwin-arm64.toml"
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-repowise-policy.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

fail() {
	printf 'repowise local policy: %s\n' "$*" >&2
	exit 1
}

require_literal() {
	needle=$1
	file=$2
	message=$3
	grep -F -- "$needle" "$file" >/dev/null || fail "$message"
}

test -f "$wrapper" || fail 'scripts/repowise-local.sh is missing'
test -f "$versions_file" || fail 'the RepoWise version manifest is missing'
test -f "$lock_file" || fail 'the Darwin arm64 RepoWise lock is missing'
test -f "$repo_root/tools/repowise/no-network.sb" || fail 'the post-install network sandbox is missing'
sh -n "$wrapper" || fail 'scripts/repowise-local.sh is not valid POSIX shell'
require_literal 'REPOWISE_VERSION=0.45.0' "$versions_file" 'RepoWise version pin drifted'
require_literal \
	'REPOWISE_UPSTREAM_TAG_COMMIT=e2bb8a2e4eff3d00005a602ac65a8e4be7daa4a3' \
	"$versions_file" 'RepoWise upstream revision pin drifted'
require_literal \
	'REPOWISE_WHEEL_SHA256=c86ec4a505b16dfe0a6df5366aae9908a0a3ef6fabb204c883a6faf94a62492a' \
	"$versions_file" 'RepoWise wheel digest pin drifted'
require_literal \
	'REPOWISE_SDIST_SHA256=4e7fdaf9d837d09ff53454963c0afda7c98e72e8020914094c2bd431f9950ada' \
	"$versions_file" 'RepoWise sdist digest pin drifted'
require_literal \
	'REPOWISE_LOCK_SHA256=dcbd8913e1b6e7f21990c14696143cad8dea66f8c2704147c4c6fafb02cf8dc8' \
	"$versions_file" 'RepoWise platform lock digest pin drifted'
require_literal 'REPOWISE_LOCK_PACKAGE_COUNT=125' "$versions_file" 'RepoWise lock package count drifted'
require_literal 'REPOWISE_LOCK_PIP_VERSION=26.2.1' "$versions_file" 'RepoWise lock pip version drifted'
test "$(shasum -a 256 "$lock_file" | awk '{print $1}')" = \
	'dcbd8913e1b6e7f21990c14696143cad8dea66f8c2704147c4c6fafb02cf8dc8' ||
	fail 'RepoWise platform lock contents do not match the reviewed digest'
test "$(grep -c '^\[\[packages\]\]$' "$lock_file")" -eq 125 ||
	fail 'RepoWise platform lock must contain exactly 125 packages'
test "$(grep -c '^\[\[packages\.wheels\]\]$' "$lock_file")" -eq 125 ||
	fail 'RepoWise platform lock must select exactly one wheel per package'
test "$(grep -Ec '^sha256 = "[0-9a-f]{64}"$' "$lock_file")" -eq 125 ||
	fail 'every locked RepoWise wheel must have one SHA-256 digest'
if grep '^url = ' "$lock_file" | grep -v '^url = "https://files.pythonhosted.org/' >/dev/null; then
	fail 'RepoWise platform lock contains a wheel outside files.pythonhosted.org'
fi
if grep '^name = ".*\.whl"$' "$lock_file" |
	grep -vE '(^name = ".*-(py2\.py3|py3)-none-any\.whl"$|macosx_[0-9_]+_(arm64|universal2)\.whl"$)' >/dev/null; then
	fail 'RepoWise platform lock contains a wheel incompatible with Darwin arm64'
fi
test "$(grep '^name = "' "$lock_file" | sed 's/^name = "//; s/"$//' | sort | uniq -d | wc -l | tr -d ' ')" -eq 0 ||
	fail 'RepoWise platform lock contains duplicate package or wheel names'

# The only repository-owned state roots are ignored. A fresh contributor or CI
# checkout must never see generated Python, index, wiki, cache, or log files.
git -C "$repo_root" check-ignore -q .repowise/probe ||
	fail '.repowise/ is not ignored'
git -C "$repo_root" check-ignore -q .local/repowise/probe ||
	fail '.local/repowise/ is not ignored'

# Validate the effective normal target graph. `make -n` expands prerequisites
# without running recipes; any RepoWise command in its output is an accidental
# dependency even when it was introduced indirectly.
for target in ci test build tools dev-up; do
	plan="$test_root/make-$target.plan"
	make -C "$repo_root" -n "$target" >"$plan" 2>&1 ||
		fail "could not inspect the make $target graph"
	if grep -i 'repowise' "$plan" >/dev/null; then
		printf '%s\n' "unexpected RepoWise command in make $target:" >&2
		grep -in 'repowise' "$plan" >&2
		fail "make $target must not depend on the optional experiment"
	fi
done

# Production dependency and deployment manifests must remain RepoWise-free.
for manifest in \
	go.mod go.sum compose.yaml package.json package-lock.json tools/versions.env \
	web/package.json web/package-lock.json \
	infra/cloudflare/telegram-edge/package.json \
	infra/cloudflare/telegram-edge/package-lock.json; do
	if test -f "$repo_root/$manifest" && grep -i 'repowise' "$repo_root/$manifest" >/dev/null; then
		fail "$manifest must not contain a RepoWise dependency"
	fi
done
if find "$repo_root/scripts" -type f \
	! -name 'repowise-local.sh' ! -name 'test-repowise-local-policy.sh' \
	-exec grep -il 'repowise' {} + 2>/dev/null | grep . >"$test_root/script-hits"; then
	cat "$test_root/script-hits" >&2
	fail 'non-RepoWise scripts must not acquire an implicit RepoWise dependency'
fi
if find "$repo_root" \
	\( -path "$repo_root/.git" -o -path "$repo_root/.build" -o -path '*/node_modules' -o -path "$repo_root/.local" -o -path "$repo_root/.repowise" \) -prune -o \
	\( -name 'Dockerfile' -o -name 'Dockerfile.*' -o -path "$repo_root/.github/workflows/*" \) \
	-type f -exec grep -il 'repowise' {} + 2>/dev/null | grep . >"$test_root/runtime-hits"; then
	cat "$test_root/runtime-hits" >&2
	fail 'RepoWise must not appear in production images or GitHub workflows'
fi

# Installation is the sole network-capable mode. All other modes must be
# implemented by the wrapper, not by direct ad hoc invocations.
for command in install index update status doctor mcp mcp-smoke evaluate stop uninstall-plan uninstall; do
	require_literal "$command)" "$wrapper" "wrapper command '$command' is missing"
done
require_literal 'env -i' "$wrapper" 'RepoWise children must receive an explicit empty-base environment'
require_literal 'sandbox-exec' "$wrapper" 'post-install RepoWise commands must use the OS network sandbox'
require_literal 'tools/repowise/no-network.sb' "$wrapper" 'wrapper must use the repository-owned no-network profile'
require_literal 'pylock.darwin-arm64.toml' "$wrapper" 'wrapper must consume the reviewed platform lock'
require_literal '--requirement' "$wrapper" 'networked download must resolve only the reviewed lock'
require_literal '--no-index' "$wrapper" 'installation must use only the downloaded wheelhouse'
require_literal '--no-deps' "$wrapper" 'installation must not resolve dependencies'
require_literal 'pip check' "$wrapper" 'installed lock must pass pip dependency validation'
require_literal '(deny network*)' "$repo_root/tools/repowise/no-network.sb" 'offline sandbox must deny every network operation'
require_literal '(deny file-write*)' "$repo_root/tools/repowise/no-network.sb" 'offline sandbox must deny ambient filesystem writes'
require_literal '(param "STATE_ROOT")' "$repo_root/tools/repowise/no-network.sb" 'offline sandbox must allow only the ignored index root'
require_literal '(param "LOCAL_ROOT")' "$repo_root/tools/repowise/no-network.sb" 'offline sandbox must allow only the ignored local environment root'
require_literal '-D STATE_ROOT=' "$wrapper" 'wrapper must bind the exact index root into the sandbox profile'
require_literal '-D LOCAL_ROOT=' "$wrapper" 'wrapper must bind the exact local environment root into the sandbox profile'
if grep -E 'run_sanitized .*\$repowise_bin' "$wrapper" >/dev/null; then
	fail 'a non-install RepoWise invocation bypasses the no-network sandbox'
fi

# Synthetic home/state and interpreter isolation prevent host/global writes and
# Python user-site leakage.
for token in \
	'HOME=' 'TMPDIR=' 'XDG_CONFIG_HOME=' 'XDG_CACHE_HOME=' 'XDG_DATA_HOME=' \
	'PYTHONDONTWRITEBYTECODE=1' 'PYTHONNOUSERSITE=1' 'PIP_CONFIG_FILE=/dev/null' \
	'GIT_CONFIG_NOSYSTEM=1' 'GIT_CONFIG_GLOBAL=/dev/null' 'GIT_TERMINAL_PROMPT=0'; do
	require_literal "$token" "$wrapper" "wrapper environment guard '$token' is missing"
done
require_literal '.local/repowise' "$wrapper" 'wrapper local environment/state root is missing'
require_literal '.repowise' "$wrapper" 'wrapper index root is missing'
require_literal 'umask 077' "$wrapper" 'RepoWise state must default to owner-only permissions'

# Defense in depth against telemetry, provider prose, editor/agent integration,
# saved credentials, hooks, and cost tracking.
for token in \
	'DO_NOT_TRACK=1' 'REPOWISE_TELEMETRY_DISABLED=1' \
	'REPOWISE_SKIP_EDITOR_SETUP=1' 'REPOWISE_NO_SAVE_KEY=1' \
	'REPOWISE_NO_COST_TRACKING=1' '--no-prose' '--no-editor-setup' \
	'--no-save-key' '--no-workspace' '--no-seed' '--no-claude-md' \
	'--no-agents' '--no-codex' '--no-distill-hook' '--no-cost-tracking' \
	'--commit-limit' '.gitcode/' '.local/'; do
	require_literal "$token" "$wrapper" "wrapper safety guard '$token' is missing"
done
require_literal '.repowise/.env' "$wrapper" 'wrapper must explicitly reject a RepoWise dotenv file'
require_literal 'last_sync_commit' "$wrapper" 'wrapper must tie index state to an exact commit'
require_literal 'assert_clean_tracked_checkout' "$wrapper" 'wrapper must reject a dirty tracked worktree before indexing'
require_literal 'ls-files --others --exclude-standard' "$wrapper" 'exact-HEAD indexing must reject non-ignored untracked files'
require_literal 'assert_local_paths_safe' "$wrapper" 'cleanup must validate physical repository-local paths'
require_literal 'test ! -L' "$wrapper" 'cleanup must reject symlinked state roots'
require_literal 'REPOWISE_UNINSTALL_CONFIRM=sessionless:' "$wrapper" 'uninstall must require exact typed confirmation'
require_literal 'kill -KILL' "$wrapper" 'stop must have a bounded forced-termination fallback'

# Prove the exact Git primitive used by the wrapper distinguishes an untracked
# source from the two allowed ignored state roots.
fixture="$test_root/untracked-fixture"
mkdir -p "$fixture/internal" "$fixture/.repowise" "$fixture/.local/repowise"
git -C "$fixture" init -q
printf '%s\n' '.repowise/' '.local/repowise/' >"$fixture/.gitignore"
git -C "$fixture" add .gitignore
printf '%s\n' 'package probe' >"$fixture/internal/probe.go"
printf '%s\n' ignored >"$fixture/.repowise/state.json"
printf '%s\n' ignored >"$fixture/.local/repowise/cache"
detected=$(git -C "$fixture" ls-files --others --exclude-standard)
test "$detected" = 'internal/probe.go' ||
	fail 'the clean-check fixture did not isolate the untracked source from ignored RepoWise state'

# Keep MCP on stdio and restricted to the five evaluated read-only capability
# families. The exact names are part of the reviewed experiment contract.
require_literal \
	'REPOWISE_MCP_ALLOWED_TOOLS=get_overview,get_context,get_change_risk,get_health,get_dead_code' \
	"$versions_file" 'MCP allowlist must equal the five reviewed tool families'
require_literal 'REPOWISE_MCP_ALLOWED_TOOLS' "$wrapper" 'wrapper must enforce the pinned MCP allowlist'
require_literal 'stdio' "$wrapper" 'MCP must be explicitly constrained to stdio'
require_literal '<&0 >&1 2>&2 &' "$wrapper" 'foreground MCP must preserve stdio across the POSIX async-list launch'
for tool in get_overview get_context get_change_risk get_health get_dead_code; do
	require_literal "(\"$tool\"" "$repo_root/tools/repowise/mcp_smoke.py" \
		"MCP smoke must call '$tool', not only advertise it"
done

printf '%s\n' 'repowise local policy: passed'
