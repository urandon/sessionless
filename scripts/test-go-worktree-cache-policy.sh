#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/sessionless-go-cache-policy.XXXXXX")
test_root=$(CDPATH= cd -- "$test_root" && pwd -P)
trap 'rm -rf "$test_root"' EXIT HUP INT TERM

fail() {
	printf 'Go worktree cache policy: %s\n' "$*" >&2
	exit 1
}

fixture=$test_root/repository
linked=$test_root/linked
mkdir -p "$fixture"
git -C "$fixture" init -q
git -C "$fixture" -c user.name=Sessionless -c user.email=sessionless@example.invalid \
	commit --allow-empty -q -m seed
git -C "$fixture" worktree add --detach "$linked" HEAD >/dev/null

cache_paths() {
	make --no-print-directory -s -C "$1" -f "$repo_root/Makefile" go-cache-status
}

field() {
	name=$1
	sed -n "s/^${name}=//p"
}

main_paths=$(cache_paths "$fixture")
linked_paths=$(cache_paths "$linked")
main_build_cache=$(printf '%s\n' "$main_paths" | field GOCACHE)
linked_build_cache=$(printf '%s\n' "$linked_paths" | field GOCACHE)
main_module_cache=$(printf '%s\n' "$main_paths" | field GOMODCACHE)
linked_module_cache=$(printf '%s\n' "$linked_paths" | field GOMODCACHE)
main_tmp=$(printf '%s\n' "$main_paths" | field GOTMPDIR)
linked_tmp=$(printf '%s\n' "$linked_paths" | field GOTMPDIR)
main_bin=$(printf '%s\n' "$main_paths" | field BIN_DIR)
linked_bin=$(printf '%s\n' "$linked_paths" | field BIN_DIR)

test "$main_build_cache" = "$fixture/.git/sessionless-go-cache/go-build" ||
	fail "main GOCACHE = $main_build_cache"
test "$main_build_cache" = "$linked_build_cache" ||
	fail 'linked worktree does not share GOCACHE'
test "$main_module_cache" = "$linked_module_cache" ||
	fail 'linked worktree does not share GOMODCACHE'
test "$main_module_cache" = "$fixture/.git/sessionless-go-cache/go-mod" ||
	fail "main GOMODCACHE = $main_module_cache"
test "$main_tmp" != "$linked_tmp" ||
	fail 'linked worktree shares GOTMPDIR'
test "$main_bin" != "$linked_bin" ||
	fail 'linked worktree shares BIN_DIR'

override_root=$test_root/override-cache
override_paths=$(SESSIONLESS_GO_CACHE_ROOT="$override_root" cache_paths "$linked")
override_build_cache=$(printf '%s\n' "$override_paths" | field GOCACHE)
override_module_cache=$(printf '%s\n' "$override_paths" | field GOMODCACHE)
test "$override_build_cache" = "$override_root/go-build" ||
	fail "overridden GOCACHE = $override_build_cache"
test "$override_module_cache" = "$override_root/go-mod" ||
	fail "overridden GOMODCACHE = $override_module_cache"

shared_root=$fixture/.git/sessionless-go-cache
mkdir -p "$shared_root" "$linked/.build" "$test_root/unrelated"
: >"$shared_root/sentinel"
: >"$linked/.build/sentinel"
: >"$test_root/unrelated/sentinel"
make --no-print-directory -s -C "$linked" -f "$repo_root/Makefile" clean
test ! -e "$linked/.build" || fail 'worktree-local clean did not remove .build'
test -f "$shared_root/sentinel" || fail 'worktree-local clean removed the shared cache'
make --no-print-directory -s -C "$fixture" -f "$repo_root/Makefile" go-cache-clean
test ! -e "$shared_root" || fail 'default shared cache was not removed'
test -f "$test_root/unrelated/sentinel" || fail 'cleanup removed unrelated data'

mkdir -p "$override_root"
: >"$override_root/sentinel"
if SESSIONLESS_GO_CACHE_ROOT="$override_root" \
	make --no-print-directory -s -C "$fixture" -f "$repo_root/Makefile" go-cache-clean \
	>"$test_root/unsafe-clean.out" 2>&1; then
	fail 'cleanup accepted an override outside the Git common directory'
fi
test -f "$override_root/sentinel" || fail 'refused cleanup still removed override data'

injected_root='$(touch '"$test_root"'/injected)'
if SESSIONLESS_GO_CACHE_ROOT="$injected_root" \
	make --no-print-directory -s -C "$fixture" -f "$repo_root/Makefile" go-cache-clean \
	>"$test_root/injected-clean.out" 2>&1; then
	fail 'cleanup accepted a shell-substitution cache override'
fi
test ! -e "$test_root/injected" || fail 'cleanup executed an override as shell code'

internal_root=$fixture/.git/refs
if SESSIONLESS_GO_CACHE_ROOT="$internal_root" \
	make --no-print-directory -s -C "$fixture" -f "$repo_root/Makefile" go-cache-clean \
	>"$test_root/internal-clean.out" 2>&1; then
	fail 'cleanup accepted a Git metadata directory as its cache root'
fi
test -d "$internal_root" || fail 'refused cleanup removed Git metadata'

ln -s "$test_root/unrelated" "$shared_root"
if make --no-print-directory -s -C "$fixture" -f "$repo_root/Makefile" go-cache-clean \
	>"$test_root/symlink-clean.out" 2>&1; then
	fail 'cleanup accepted a symlinked cache root'
fi
test -L "$shared_root" || fail 'refused cleanup removed symlinked cache root'
test -f "$test_root/unrelated/sentinel" || fail 'refused cleanup removed symlink target'

printf 'Go worktree cache policy checks passed.\n'
