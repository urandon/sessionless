import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';

// Deliberately checks this repository's fixed, indentation-sensitive format,
// not arbitrary YAML/Make syntax. Structural changes require explicit review.
const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const original = {
  make: read('Makefile'),
  ci: read('.github/workflows/ci.yml'),
  release: read('.github/workflows/release.yml'),
};
const required = `docs-check cloud-web-smoke-test observability-config-check web-ci generate test-ordinary build integration
  image-publication-test image-build-inputs-test
  image-publish-policy-test registry-gc-policy-test release-policy-test
  local-stand-policy-test dockerless-stand-policy-test go-cache-policy-test`.split(/\s+/);
const orderedWords = (value) => value.trim() ? value.trim().split(/\s+/) : [];
const words = (value) => orderedWords(value).sort();
const ciCoreBasePrerequisites = '$(subst test-ordinary,test-ordinary-ci-base,$(CI_CORE_CHECKS))';
const coldGoPrefix = 'ci(cold-go): ';
const coldGoPredicate = "startsWith(github.event.head_commit.message, 'ci(cold-go): ')";
const goJobs = ['verify', 'go-local', 'go-connected', 'ydb', 'ydb-security-isolated', 'local-stand'];
const pinnedGo = `uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
          cache-dependency-path: go.sum
          cache: "\${{ !${coldGoPredicate} }}"`;
const coldGoStepName = 'Prove cold Go caches are absent or empty';
const coldGoShell = `set -eu
test "$GOCACHE" = "$GITHUB_WORKSPACE/.git/sessionless-go-cache/go-build"
test "$GOMODCACHE" = "$GITHUB_WORKSPACE/.git/sessionless-go-cache/go-mod"
test ! -L "$GITHUB_WORKSPACE/.git/sessionless-go-cache"
for cache_dir in "$GOCACHE" "$GOMODCACHE"; do
  test ! -L "$cache_dir"
  if test -e "$cache_dir"; then
    test -d "$cache_dir"
    entries=$(find "$cache_dir" -mindepth 1 -maxdepth 1 -print -quit)
    test -z "$entries"
  fi
done`;
const coldGoStep = `if: "${coldGoPredicate}"\n        shell: bash\n        run: |\n${coldGoShell.split('\n').map(line => `          ${line}`).join('\n')}`;
const connectedTerminal = "go test -race -count=1 -timeout=15m -run='^TestLocalSessionTerminal' $(GO_PACKAGE_PATTERNS)";
const connectedOther = "go test -race -count=1 -timeout=15m -run='^TestLocalSession' -skip='^TestLocalSessionTerminal' $(GO_PACKAGE_PATTERNS)";
const ordinary = [
  "go test -race -count=1 -timeout=15m -skip='^(TestLocal|TestOwned|TestPrivate)' $(GO_PACKAGE_PATTERNS)",
  "go test -race -count=1 -timeout=15m -run='^(TestOwned|TestPrivate)' $(GO_PACKAGE_PATTERNS)",
  "go test -race -count=1 -timeout=15m -run='^TestLocalCompaction' $(GO_PACKAGE_PATTERNS)",
  "go test -race -count=1 -timeout=15m -run='^TestLocal' -skip='^TestLocal(Session|Compaction)' $(GO_PACKAGE_PATTERNS)",
];
const ordinaryTail = [
  "go test -race -count=1 -timeout=15m -run='^TestLocalTerminal' $(GO_PACKAGE_PATTERNS)",
  "go test -race -count=1 -timeout=15m -run='^TestLocal' -skip='^TestLocal(Session|Compaction|Terminal)' $(GO_PACKAGE_PATTERNS)",
];
function recipe(make, name) {
  const matches = [...make.matchAll(new RegExp(`^${name}:[^\\n]*\\n((?:\\t[^\\n]*\\n)*)`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} recipe`);
  return matches[0][1].trim();
}
function targetText(make, name) {
  const matches = [...make.matchAll(new RegExp(`^${name}:([^\\n]*)$`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} target`);
  return matches[0][1].trim();
}
function target(make, name) {
  return words(targetText(make, name));
}
function assignment(make, name) {
  const matches = [...make.matchAll(new RegExp(`^(?:override\\s+)?${name}\\s*[:+?]?=[^\\n]*$`, 'gm'))];
  assert.equal(matches.length, 1, `unique canonical ${name} assignment`);
  assert.ok(matches[0][0].startsWith(`${name} := `), `simple canonical ${name} assignment`);
  return orderedWords(matches[0][0].slice(`${name} := `.length));
}
function job(ci, name) {
  const matches = [...ci.matchAll(new RegExp(`^  ${name}:\\n([\\s\\S]*?)(?=^  [\\w-]+:|$(?![\\s\\S]))`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} job`);
  return matches[0][1];
}
function step(jobText, name) {
  const matches = [...jobText.matchAll(new RegExp(`^      - name: ${name}\\n([\\s\\S]*?)(?=^      - |$(?![\\s\\S]))`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} step`);
  return matches[0][1].trim();
}
function verify({ make, ci, release }) {
  assert.deepEqual(target(make, 'ci'), words('ci-core test-connected attached-worker-security-gate serverless-egress-conformance'));
  assert.deepEqual(assignment(make, 'CI_CORE_CHECKS'), [...required, 'ci-partition-test'], 'complete core prerequisites in original order');
  assert.equal(targetText(make, 'ci-core'), '$(CI_CORE_CHECKS)', 'complete local core variant');
  assert.equal(targetText(make, 'ci-core-base'), ciCoreBasePrerequisites, 'complete CI core variant');
  assert.equal(recipe(make, 'ci-core'), '', 'local core has no bypass recipe');
  assert.equal(recipe(make, 'ci-core-base'), '', 'CI core has no bypass recipe');
  assert.deepEqual(target(make, 'observability-config-check'), ['prepare'], 'offline YAML preparation');
  assert.equal(recipe(make, 'observability-config-check'),
    'go run ./cmd/observability-config validate --root "$(OBSERVABILITY_CONFIG_ROOT)" --profile "$(OBSERVABILITY_PROFILE)"\n\tgo run ./cmd/observability-config canary-validate --input "$(OBSERVABILITY_CANARY_INPUT)"', 'offline YAML catalog check');
  assert.deepEqual(target(make, 'test'), [], 'test must sequence, not parallelize prerequisites');
  assert.equal(recipe(make, 'test'), '$(MAKE) test-ordinary\n\t$(MAKE) test-connected', 'sequential complete local test');
  assert.deepEqual(target(make, 'test-ordinary'), words('prepare fmt-check lint'), 'ordinary preparation');
  assert.deepEqual(target(make, 'test-ordinary-ci-base'), words('prepare fmt-check lint'), 'CI base preparation');
  assert.deepEqual(target(make, 'test-ordinary-ci-tail'), ['prepare'], 'CI tail preparation');
  assert.deepEqual(target(make, 'test-connected'), ['prepare'], 'connected preparation');
  assert.deepEqual(assignment(make, 'GO_PACKAGE_PATTERNS'), words('./cmd/... ./internal/... ./migrations/...'), 'complete Go corpus');
  assert.equal(recipe(make, 'test-ordinary'), ordinary.join('\n\t'), 'exact sequential ordinary partition');
  assert.equal(recipe(make, 'test-ordinary-ci-base'), ordinary.slice(0, 3).join('\n\t'), 'exact CI base partition');
  assert.equal(recipe(make, 'test-ordinary-ci-tail'), ordinaryTail.join('\n\t'), 'exact CI tail partition');
  assert.equal(recipe(make, 'test-connected'), `${connectedTerminal}\n\t${connectedOther}\n\t$(MAKE) test-mcp-offline`, 'exact sequential connected partition');
  assert.deepEqual(target(make, 'test-mcp-offline'), ['prepare'], 'offline MCP preparation');
  assert.equal(recipe(make, 'test-mcp-offline'), [
    'test ! -e internal/localloopback/mcp_offline_fixture.go',
    'test ! -e internal/providerloopsession/mcp_offline_fixture.go',
    'mcp_fixture_format=$$(gofmt -l internal/localloopback/testdata/mcp_offline_fixture.go.txt internal/providerloopsession/testdata/mcp_offline_fixture.go.txt) && test -z "$$mcp_fixture_format"',
    'go build -overlay= -tags= ./internal/localloopback ./internal/providerloopsession',
    "go test -race -count=1 -shuffle=190351 -timeout=5m -tags=sessionless_mcp_offline -overlay=test/mcp-offline-overlay.json -run='^TestLocalSessionCredentialedMCPOffline' ./internal/ydbstore",
  ].join('\n\t'), 'exact additive offline MCP gate');
  assert.deepEqual(target(make, 'attached-worker-security-gate'),
    words('attached-worker-crash-integration attached-worker-active-crash-integration'));
  assert.equal(recipe(make, 'ci-partition-test'), 'node --test scripts/test-ci-partition.mjs\n\tnode --test scripts/test-ci-ydb-readiness.mjs');
  const core = job(ci, 'verify');
  const connected = job(ci, 'go-connected');
  const local = job(ci, 'go-local');
  const security = job(ci, 'ydb-security-isolated');
  assert.equal([...ci.split('\njobs:\n')[1].matchAll(/^  [\w-]+:\n/gm)].length, 10, 'ten required jobs');
  assert.match(core, /^    timeout-minutes: 30$/m);
  assert.doesNotMatch(core, /^    (?:needs|permissions|env):/m, 'core base is parallel, inherits read-only policy');
  assert.match(connected, /^    timeout-minutes: 30$/m, 'connected 30-minute bound');
  assert.match(connected, /^    runs-on: ubuntu-latest$/m);
  assert.doesNotMatch(connected, /^    (?:needs|permissions|env):/m, 'connected is parallel, inherits read-only policy');
  assert.match(ci, /^permissions:\n  contents: read\n/m, 'read-only workflow');
  assert.equal(step(connected, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact connected checkout');
  const actualGoJobs = [...ci.split('\njobs:\n')[1].matchAll(/^  ([\w-]+):\n/gm)]
    .map(match => match[1]).filter(name => /uses: actions\/setup-go@/.test(job(ci, name)));
  assert.deepEqual(actualGoJobs, goJobs, 'all six host Go jobs have the cold-cache contract');
  for (const name of goJobs) {
    const value = job(ci, name);
    assert.equal(step(value, 'Set up pinned Go'), pinnedGo, `${name} exact cold Go cache control`);
    assert.equal(step(value, coldGoStepName), coldGoStep, `${name} exact cold Go freshness guard`);
    assert.ok(value.includes(`      - name: Set up pinned Go\n        ${pinnedGo}\n\n      - name: ${coldGoStepName}\n`), `${name} cold Go guard immediately after setup`);
    assert.equal(value.slice(0, value.indexOf(`      - name: ${coldGoStepName}\n`)).match(/\b(?:run:|make |go (?:test|build|run|env) )/), null, `${name} cold Go guard before repository commands`);
  }
  assert.equal(step(connected, 'Run connected-session tests over all packages'), 'run: make test-connected', 'complete connected invocation');
  assert.equal([...connected.matchAll(/^      - name:/gm)].length, 4, 'bounded connected steps');
  assert.match(local, /^    timeout-minutes: 30$/m, 'local tail 30-minute bound');
  assert.match(local, /^    runs-on: ubuntu-latest$/m);
  assert.doesNotMatch(local, /^    (?:needs|permissions|env):/m, 'local tail is parallel, inherits read-only policy');
  assert.equal(step(local, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact local tail checkout');
  assert.equal(step(local, 'Set up pinned Go'), step(connected, 'Set up pinned Go'), 'same pinned local tail Go/cache setup');
  assert.equal(step(local, 'Run ordinary local-tail tests over all packages'), 'run: make test-ordinary-ci-tail', 'complete local tail invocation');
  assert.equal([...local.matchAll(/^      - name:/gm)].length, 4, 'bounded local tail steps');
  assert.match(ci, /^env:\n  GOCACHE: \$\{\{ github.workspace \}\}\/\.git\/sessionless-go-cache\/go-build\n  GOMODCACHE: \$\{\{ github.workspace \}\}\/\.git\/sessionless-go-cache\/go-mod\n/m, 'shared Go cache policy');
  assert.match(security, /^    timeout-minutes: 25$/m);
  for (const name of ['ydb', 'ydb-security-isolated', 'local-stand', 'terraform']) {
    assert.match(job(ci, name), /^    needs: \[verify, go-connected, go-local\]$/m, `${name} joins all three Go jobs`);
  }
  for (const [name, container] of [['ydb', 'sessionless-ydb'], ['ydb-security-isolated', 'sessionless-security-ydb']]) {
    const text = job(ci, name);
    assert.equal(step(text, 'Wait for YDB Local'), `run: bash scripts/wait-ci-ydb.sh ${container}`, `${name} SQL/storage readiness`);
    assert.ok(text.indexOf('Start pinned YDB Local') < text.indexOf('Wait for YDB Local'));
    assert.ok(text.indexOf('Wait for YDB Local') < text.indexOf('Apply schema twice'));
    assert.equal(step(text, 'Apply schema twice'), 'run: |\n          make migrate-local\n          make migrate-local', `${name} no migration retry`);
  }
  assert.equal(step(core, 'Run repository CI contract'), 'run: make ci-core-base', 'complete CI base invocation');
  const offlineName = 'Run offline worker and egress security contracts';
  assert.equal(step(security, offlineName),
    'run: env -u YDB_CONNECTION_STRING -u YDB_ANONYMOUS_CREDENTIALS make attached-worker-security-gate serverless-egress-conformance');
  assert.ok(security.indexOf(offlineName) > security.indexOf('Set up pinned Go'));
  assert.ok(security.indexOf(offlineName) < security.indexOf('Start pinned YDB Local'));
  assert.equal(step(job(release, 'verify'), 'Run repository verification contract'), 'run: make ci');
  const images = job(ci, 'images');
  const needs = images.match(/^    needs:\n((?:      - [\w-]+\n)+)/m);
  assert.ok(needs, 'explicit image prerequisites');
  assert.deepEqual([...needs[1].matchAll(/- ([\w-]+)/g)].map((m) => m[1]).sort(),
    words('verify go-connected go-local ydb ydb-security-isolated local-stand terraform cloudflare-edge web-browser'));
  for (const name of ['verify', 'go-connected', 'go-local', 'ydb', 'ydb-security-isolated', 'local-stand', 'terraform', 'images']) {
    const value = job(ci, name);
    assert.doesNotMatch(value, /^    (?:if|continue-on-error):/m, `${name} cannot bypass job failure`);
    assert.doesNotMatch(value, /^        continue-on-error:/m, `${name} cannot ignore step failure`);
  }
}

test('local, release and mirrored CI retain the complete partition', () => verify(original));

test('ordered core expansion changes only the ordinary prerequisite for CI', () => {
  const local = assignment(original.make, 'CI_CORE_CHECKS');
  assert.equal(targetText(original.make, 'ci-core'), '$(CI_CORE_CHECKS)');
  assert.equal(targetText(original.make, 'ci-core-base'), ciCoreBasePrerequisites);
  // This exact literal subst is the sole admitted Make transformation, not
  // a general Make evaluator. No other canonical target contains its token.
  const ciBase = local.map(name => name.replaceAll('test-ordinary', 'test-ordinary-ci-base'));
  assert.deepEqual(local, [...required, 'ci-partition-test']);
  assert.deepEqual(ciBase, [...required, 'ci-partition-test'].map(name => name === 'test-ordinary' ? 'test-ordinary-ci-base' : name));
  assert.equal(local.indexOf('test-ordinary') + 1, local.indexOf('build'));
  assert.equal(ciBase.indexOf('test-ordinary-ci-base') + 1, ciBase.indexOf('build'));
  assert.equal(local.indexOf('build') + 1, local.indexOf('integration'));
});

test('CI base and tail preserve the complete local ordinary coverage', () => {
  const commands = name => recipe(original.make, name).split('\n\t');
  const local = commands('test-ordinary');
  const base = commands('test-ordinary-ci-base');
  const tail = commands('test-ordinary-ci-tail');
  assert.deepEqual(base, local.slice(0, 3));
  assert.deepEqual(local, ordinary);
  assert.deepEqual(tail, ordinaryTail);
  assert.equal(new Set([...base, ...tail]).size, 5, 'no duplicate CI command');
  for (const name of [
    'TestLocal', 'TestLocalFuture', 'TestLocalTerminal', 'TestLocalTerminalFuture',
    'TestLocalTerminalCompaction', 'TestLocalTerminalSession', 'TestLocalCompactionTerminal',
    'TestLocalTerminalMCPV2FreshOwnerCompactionAndNextModel',
    'TestLocalPrepared', 'TestLocalPreparedFuture', 'TestLocalCompaction',
    'TestLocalCompactionFuture', 'TestLocalSession', 'TestLocalSessionTerminal',
    'TestOther', 'Example', 'ExampleLocalTerminal', 'FuzzLocalTerminal',
  ]) {
    const originalTail = /^TestLocal/.test(name) && !/^TestLocal(Session|Compaction)/.test(name);
    const terminal = /^TestLocalTerminal/.test(name);
    const remainder = /^TestLocal/.test(name) && !/^TestLocal(Session|Compaction|Terminal)/.test(name);
    assert.equal(Number(terminal) + Number(remainder), Number(originalTail), `${name}: exact original tail union`);
  }
});

test('offline MCP overlay adds only two absent test fixture files', () => {
  const expected = {
    Replace: {
      'internal/localloopback/mcp_offline_fixture.go': 'internal/localloopback/testdata/mcp_offline_fixture.go.txt',
      'internal/providerloopsession/mcp_offline_fixture.go': 'internal/providerloopsession/testdata/mcp_offline_fixture.go.txt',
    },
  };
  assert.deepEqual(JSON.parse(read('test/mcp-offline-overlay.json')), expected);
  for (const [destination, source] of Object.entries(expected.Replace)) {
    assert.equal(existsSync(new URL(`../${destination}`, import.meta.url)), false, 'overlay cannot replace a production file');
    assert.match(read(source), /^\/\/go:build sessionless_mcp_offline\n/m, 'fixture is also explicitly build-tagged');
  }
});
test('connected selectors are disjoint and cover the original complete prefix', () => {
  for (const name of [
    'TestLocalSession', 'TestLocalSessionTerminal',
    'TestLocalSessionTerminalCleanupStorageFaultsConnected',
    'TestLocalSessionRetainedDeletionConnected',
    'TestLocalSessionWorkerPreflightOriginalPositiveReadOnly',
    'TestLocalSessionFutureFamily', 'TestOther', 'ExampleLocalSession', 'FuzzLocalSession',
  ]) {
    const allConnected = /^TestLocalSession/.test(name);
    const terminal = /^TestLocalSessionTerminal/.test(name);
    const other = allConnected && !terminal;
    assert.equal(Number(terminal) + Number(other), Number(allConnected), name);
    assert.equal(Number(!allConnected) + Number(terminal) + Number(other), 1, name);
  }
});
test('ordinary selectors and connected selectors partition the complete test-name corpus', () => {
  const names = [
    'TestOrdinary', 'TestOtherFutureFamily', 'TestLocal', 'TestLocalFutureFamily',
    'TestLocalCompaction', 'TestLocalCompactionSourceDenies',
    'TestLocalCompactionFutureFamily', 'TestLocalPreparedModelTwoCompactionsOriginalChain',
    'TestOwned', 'TestOwnedFutureFamily', 'TestPrivate', 'TestPrivateFutureFamily',
    'TestLocalSession', 'TestLocalSessionFutureFamily', 'TestLocalSessionTerminal',
    'TestLocalSessionCompaction', 'TestLocalSessionTerminalCompaction',
    'Example', 'ExampleLocalSession', 'ExampleLocalCompaction', 'Fuzz', 'FuzzPrivateBytes',
    'FuzzLocalCompaction',
  ];
  for (const name of names) {
    const selected = [
      !/^(?:TestLocal|TestOwned|TestPrivate)/.test(name),
      /^Test(?:Owned|Private)/.test(name),
      /^TestLocalCompaction/.test(name),
      /^TestLocal/.test(name) && !/^TestLocal(Session|Compaction)/.test(name),
      /^TestLocalSessionTerminal/.test(name),
      /^TestLocalSession/.test(name) && !/^TestLocalSessionTerminal/.test(name),
    ];
    assert.equal(selected.filter(Boolean).length, 1, `${name} selected exactly once`);
  }
});
test('cold intent is only the case-insensitive HEAD subject prefix', () => {
  // GitHub expression startsWith is case-insensitive; this is not byte matching
  // or substring matching in a PR description copied into a merge commit body.
  const cold = message => (message ?? '').toLowerCase().startsWith(coldGoPrefix.toLowerCase());
  for (const message of ['ci(cold-go): qualify complete graph', 'CI(COLD-GO): qualify complete graph']) {
    assert.equal(cold(message), true);
  }
  for (const message of [undefined, null, '', 'ci: normal push', 'ci(cold-go):missing space',
    'docs: explain cold intent\n\nci(cold-go): quoted later',
    '!282 Merge CI partition\n\nUse ci(cold-go): only for reviewed cold qualification']) {
    assert.equal(cold(message), false);
  }
});

for (const mode of ['absent', 'empty', 'build-nonempty', 'modules-nonempty', 'wrong-path', 'file', 'symlink', 'parent-symlink', 'find-error']) {
  test(`cold freshness shell ${mode} without clearing caches`, t => {
    const root = mkdtempSync(join(tmpdir(), 'sessionless-ci-cold-'));
    t.after(() => rmSync(root, { recursive: true, force: true })); // Exact owned fixture, never repository/shared caches.
    const cacheRoot = join(root, '.git/sessionless-go-cache');
    const build = join(cacheRoot, 'go-build');
    const modules = join(cacheRoot, 'go-mod');
    let sentinel;
    if (mode !== 'absent') {
      mkdirSync(mode === 'parent-symlink' ? join(root, '.git') : cacheRoot, { recursive: true });
      if (mode === 'file') writeFileSync(build, 'test-owned cache-path file');
      else if (mode === 'symlink') symlinkSync(root, build, 'dir');
      else if (mode !== 'parent-symlink') {
        mkdirSync(build);
        mkdirSync(modules);
        if (mode.endsWith('-nonempty')) {
          sentinel = join(mode === 'build-nonempty' ? build : modules, '.retained');
          writeFileSync(sentinel, 'do not clear this synthetic cache');
        }
      }
      if (mode === 'parent-symlink') {
        symlinkSync(root, cacheRoot, 'dir');
      }
    }
    const script = mode === 'find-error' ? `find() { return 17; }\n${coldGoShell}` : coldGoShell;
    const result = spawnSync('bash', ['-c', script], {
      env: { ...process.env, GITHUB_WORKSPACE: root, GOCACHE: mode === 'wrong-path' ? root : build, GOMODCACHE: modules },
      encoding: 'utf8', timeout: 5000,
    });
    assert.equal(result.error, undefined);
    assert.equal(result.signal, null);
    assert.equal(result.status === 0, mode === 'absent' || mode === 'empty');
    if (mode === 'absent') assert.equal(existsSync(cacheRoot), false, 'guard must not create caches');
    if (sentinel) assert.equal(readFileSync(sentinel, 'utf8'), 'do not clear this synthetic cache');
    if (mode === 'file') assert.equal(readFileSync(build, 'utf8'), 'test-owned cache-path file');
    if (mode === 'find-error') {
      assert.equal(result.status, 17, 'find read failure cannot masquerade as an empty directory');
      assert.ok(existsSync(build) && existsSync(modules), 'failed guard must leave the synthetic caches intact');
    }
  });
}

const mutations = [
  ['missing tail remainder', 'make', `${ordinaryTail[1]}\n`, '', /exact CI tail partition/],
  ['duplicate tail remainder', 'make', ordinaryTail.join('\n\t'), `${ordinaryTail.join('\n\t')}\n\t${ordinaryTail[1]}`, /exact CI tail partition/],
  ['reversed tail halves', 'make', ordinaryTail.join('\n\t'), [...ordinaryTail].reverse().join('\n\t'), /exact CI tail partition/],
  ['tail halves overlap', 'make', ordinaryTail[1], ordinaryTail[1].replace('|Terminal', ''), /exact CI tail partition/],
  ['terminal family gap', 'make', ordinaryTail[0], ordinaryTail[0].replace("^TestLocalTerminal", "^TestLocalTerminalMCP"), /exact CI tail partition/],
  ['missing core check', 'make', 'CI_CORE_CHECKS := docs-check ', 'CI_CORE_CHECKS := ', /complete core prerequisites/],
  ['missing local ordinary core prerequisite', 'make', 'ci-core: $(CI_CORE_CHECKS)', 'ci-core:', /complete local core variant/],
  ['missing CI base prerequisite', 'make', `ci-core-base: ${ciCoreBasePrerequisites}`, 'ci-core-base: $(CI_CORE_CHECKS)', /complete CI core variant/],
  ['extra common prerequisite assignment', 'make', 'ci-core: $(CI_CORE_CHECKS)', 'CI_CORE_CHECKS += test-ordinary\nci-core: $(CI_CORE_CHECKS)', /unique canonical CI_CORE_CHECKS assignment/],
  ['CI core repeats full ordinary suite', 'make', `ci-core-base: ${ciCoreBasePrerequisites}`, 'ci-core-base: $(CI_CORE_CHECKS) test-ordinary', /complete CI core variant/],
  ['local core loses tail', 'make', 'ci-core: $(CI_CORE_CHECKS)', `ci-core: ${ciCoreBasePrerequisites}`, /complete local core variant/],
  ['CI core skips common checks', 'make', `ci-core-base: ${ciCoreBasePrerequisites}`, 'ci-core-base: test-ordinary-ci-base', /complete CI core variant/],
  ['ordinary moved after build and integration', 'make', 'generate test-ordinary build integration', 'generate build integration test-ordinary', /complete core prerequisites in original order/],
  ['shared build and integration reordered', 'make', 'test-ordinary build integration', 'test-ordinary integration build', /complete core prerequisites in original order/],
  ['CI replaces build instead of ordinary', 'make', ciCoreBasePrerequisites, '$(subst build,test-ordinary-ci-base,$(CI_CORE_CHECKS))', /complete CI core variant/],
  ['missing YAML catalog check', 'make', 'cloud-web-smoke-test observability-config-check web-ci', 'cloud-web-smoke-test web-ci', /complete core prerequisites/],
  ['wrong YAML operation', 'make', 'go run ./cmd/observability-config validate --root', 'go run ./cmd/observability-config render --root', /offline YAML catalog check/],
  ['missing canary YAML validation only', 'make', '\tgo run ./cmd/observability-config canary-validate --input "$(OBSERVABILITY_CANARY_INPUT)"\n', '', /offline YAML catalog check/],
  ['wrong canary YAML operation only', 'make', 'go run ./cmd/observability-config canary-validate --input', 'go run ./cmd/observability-config canary-render --input', /offline YAML catalog check/],
  ['wrong canary YAML input only', 'make', 'canary-validate --input "$(OBSERVABILITY_CANARY_INPUT)"', 'canary-validate --input "observability/queries.yaml"', /offline YAML catalog check/],
  ['missing local security', 'make', 'ci: ci-core test-connected attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core test-connected serverless-egress-conformance'],
  ['missing local egress', 'make', 'ci: ci-core test-connected attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core test-connected attached-worker-security-gate'],
  ['missing crash prerequisite', 'make', 'attached-worker-security-gate: attached-worker-crash-integration ', 'attached-worker-security-gate: '],
  ['missing regression gate', 'make', 'go-cache-policy-test ci-partition-test\n', 'go-cache-policy-test\n'],
  ['wrong core invocation', 'ci', 'run: make ci-core-base', 'run: make test', /complete CI base invocation/],
  ['conditional base step', 'ci', '      - name: Run repository CI contract\n', '      - name: Run repository CI contract\n        if: false\n', /complete CI base invocation/],
  ['ignored base failure', 'ci', 'run: make ci-core-base\n', 'run: make ci-core-base || true\n', /complete CI base invocation/],
  ['missing local tail job', 'ci', '  go-local:\n', '  removed-local:\n', /unique go-local job/],
  ['serialized local tail job', 'ci', '  go-local:\n', '  go-local:\n    needs: verify\n', /local tail is parallel/],
  ['conditional local tail job', 'ci', '  go-local:\n', '  go-local:\n    if: false\n', /go-local cannot bypass job failure/],
  ['conditional base job', 'ci', '  verify:\n', '  verify:\n    if: false\n', /verify cannot bypass job failure/],
  ['wrong local tail target', 'ci', 'run: make test-ordinary-ci-tail\n', 'run: make test-ordinary-ci-base\n', /complete local tail invocation/],
  ['conditional local tail step', 'ci', '      - name: Run ordinary local-tail tests over all packages\n', '      - name: Run ordinary local-tail tests over all packages\n        if: false\n', /complete local tail invocation/],
  ['ignored local tail failure', 'ci', 'run: make test-ordinary-ci-tail\n', 'run: make test-ordinary-ci-tail || true\n', /complete local tail invocation/],
  ['local tail gains write permission', 'ci', '  go-local:\n', '  go-local:\n    permissions:\n      contents: write\n', /local tail is parallel/],
  ['wider local tail budget', 'ci', '  go-local:\n    name: Go ordinary local-tail verification\n    runs-on: ubuntu-latest\n    timeout-minutes: 30', '  go-local:\n    name: Go ordinary local-tail verification\n    runs-on: ubuntu-latest\n    timeout-minutes: 60', /local tail 30-minute bound/],
  ['offline environment leak', 'ci', 'env -u YDB_CONNECTION_STRING -u YDB_ANONYMOUS_CREDENTIALS make', 'make'],
  ['conditional offline check', 'ci', '      - name: Run offline worker and egress security contracts\n', '      - name: Run offline worker and egress security contracts\n        if: false\n'],
  ['missing isolated egress', 'ci', 'make attached-worker-security-gate serverless-egress-conformance\n', 'make attached-worker-security-gate\n'],
  ['ignored offline failure', 'ci', 'make attached-worker-security-gate serverless-egress-conformance\n', 'make attached-worker-security-gate serverless-egress-conformance || true\n'],
  ['release loses security', 'release', 'run: make ci\n', 'run: make ci-core\n'],
  ['image loses security', 'ci', '      - ydb-security-isolated\n', ''],
  ['image bypasses failure', 'ci', '  images:\n', '  images:\n    if: always()\n'],
  ['ignored job failure', 'ci', '  verify:\n', '  verify:\n    continue-on-error: true\n'],
  ['wider job budget', 'ci', 'timeout-minutes: 30', 'timeout-minutes: 60'],
  ['missing local connected', 'make', 'ci: ci-core test-connected ', 'ci: ci-core '],
  ['parallel local test', 'make', '\ntest:\n', '\ntest: test-ordinary test-connected\n', /test must sequence/],
  ['missing local connected recipe', 'make', '\t$(MAKE) test-connected\n', '', /sequential complete local test/],
  ['reversed local test', 'make', '\t$(MAKE) test-ordinary\n\t$(MAKE) test-connected\n', '\t$(MAKE) test-connected\n\t$(MAKE) test-ordinary\n', /sequential complete local test/],
  ['missing ordinary formatting', 'make', 'test-ordinary: prepare fmt-check lint', 'test-ordinary: prepare lint', /ordinary preparation/],
  ['incomplete package roots', 'make', './cmd/... ./internal/... ./migrations/...', './internal/...', /complete Go corpus/],
  ['extra package roots assignment', 'make', 'GO_PACKAGE_PATTERNS := ./cmd/... ./internal/... ./migrations/...', 'GO_PACKAGE_PATTERNS := ./cmd/... ./internal/... ./migrations/...\nGO_PACKAGE_PATTERNS += ./foreign/...', /unique canonical GO_PACKAGE_PATTERNS assignment/],
  ['missing ordinary first partition', 'make', `${ordinary[0]}\n\t`, '', /exact sequential ordinary partition/],
  ['missing ordinary second partition', 'make', `\t${ordinary[1]}\n`, '', /exact sequential ordinary partition/],
  ['missing ordinary third partition', 'make', `\t${ordinary[2]}`, '', /exact sequential ordinary partition/],
  ['missing ordinary fourth partition', 'make', `\t${ordinary[3]}\n`, '', /exact sequential ordinary partition/],
  ['reversed ordinary partitions', 'make', ordinary.join('\n\t'), [...ordinary].reverse().join('\n\t'), /exact sequential ordinary partition/],
  ['background ordinary partition', 'make', ordinary[0], `${ordinary[0]} &`, /exact sequential ordinary partition/],
  ['lost ordinary race', 'make', ordinary[0], ordinary[0].replace(' -race', ''), /exact sequential ordinary partition/],
  ['cached ordinary results', 'make', ordinary[1], ordinary[1].replace(' -count=1', ''), /exact sequential ordinary partition/],
  ['wider ordinary binary timeout', 'make', ordinary[2], ordinary[2].replace('-timeout=15m', '-timeout=30m'), /exact sequential ordinary partition/],
  ['incomplete ordinary package corpus', 'make', ordinary[0], ordinary[0].replace('$(GO_PACKAGE_PATTERNS)', './internal/ydbstore'), /exact sequential ordinary partition/],
  ['short ordinary suite', 'make', ordinary[1], ordinary[1].replace(' $(GO_PACKAGE_PATTERNS)', ' -short $(GO_PACKAGE_PATTERNS)'), /exact sequential ordinary partition/],
  ['extra ordinary selector', 'make', ordinary[3], ordinary[3].replace(" -skip='^TestLocal(Session|Compaction)'", " -skip='^TestLocal(Session|Compaction)' -run='^TestLocal'"), /exact sequential ordinary partition/],
  ['ignored ordinary failure', 'make', ordinary[1], `${ordinary[1]} || true`, /exact sequential ordinary partition/],
  ['gap in ordinary selectors', 'make', ordinary[3], ordinary[3].replace("-skip='^TestLocal(Session|Compaction)'", "-skip='^TestLocal'"), /exact sequential ordinary partition/],
  ['compaction gap', 'make', ordinary[2], ordinary[2].replace("^TestLocalCompaction", "^TestLocalCompactionSource"), /exact sequential ordinary partition/],
  ['compaction overlaps remainder', 'make', ordinary[3], ordinary[3].replace("-skip='^TestLocal(Session|Compaction)'", "-skip='^TestLocalSession'"), /exact sequential ordinary partition/],
  ['overlap in ordinary selectors', 'make', ordinary[0], ordinary[0].replace("-skip='^(TestLocal|TestOwned|TestPrivate)'", "-skip='^(TestOwned|TestPrivate)'"), /exact sequential ordinary partition/],
  ['missing terminal half', 'make', `\t${connectedTerminal}\n`, '', /exact sequential connected partition/],
  ['missing other half', 'make', `\t${connectedOther}\n`, '', /exact sequential connected partition/],
  ['missing offline MCP join', 'make', '\t$(MAKE) test-mcp-offline\n', '', /exact sequential connected partition/],
  ['offline MCP replaces production', 'make', 'test ! -e internal/localloopback/mcp_offline_fixture.go', 'true', /exact additive offline MCP gate/],
  ['offline MCP loses race', 'make', 'go test -race -count=1 -shuffle=190351', 'go test -count=1 -shuffle=190351', /exact additive offline MCP gate/],
  ['overlapping connected halves', 'make', connectedOther, connectedOther.replace(" -skip='^TestLocalSessionTerminal'", ''), /exact sequential connected partition/],
  ['gap in terminal half', 'make', connectedTerminal, connectedTerminal.replace("^TestLocalSessionTerminal", '^TestLocalSessionTerminalCleanup'), /exact sequential connected partition/],
  ['gap in other half', 'make', connectedOther, connectedOther.replace("-skip='^TestLocalSessionTerminal'", "-skip='^TestLocalSession'"), /exact sequential connected partition/],
  ['reversed connected halves', 'make', `${connectedTerminal}\n\t${connectedOther}`, `${connectedOther}\n\t${connectedTerminal}`, /exact sequential connected partition/],
  ['parallel connected halves', 'make', connectedTerminal, `${connectedTerminal} &`, /exact sequential connected partition/],
  ['missing connected job', 'ci', '  go-connected:\n', '  removed-connected:\n', /unique go-connected job/],
  ['serialized connected job', 'ci', '  go-connected:\n', '  go-connected:\n    needs: verify\n', /connected is parallel/],
  ['wrong connected target', 'ci', 'run: make test-connected\n', 'run: make test-ordinary\n', /complete connected invocation/],
  ['skipped connected step', 'ci', '      - name: Run connected-session tests over all packages\n', '      - name: Run connected-session tests over all packages\n        if: false\n', /complete connected invocation/],
  ['ignored connected command', 'ci', 'run: make test-connected\n', 'run: make test-connected || true\n', /complete connected invocation/],
  ['connected gains write permission', 'ci', '  go-connected:\n', '  go-connected:\n    permissions:\n      contents: write\n', /connected is parallel/],
  ['image loses connected', 'ci', '      - go-connected\n', ''],
  ['image loses local tail', 'ci', '      - go-local\n', ''],
  ['removed readiness policy', 'make', '\tnode --test scripts/test-ci-ydb-readiness.mjs\n', ''],
  ['liveness replaces SQL health', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', 'run: curl http://localhost:8765/monitoring/cluster', /ydb SQL\/storage readiness/],
  ['wrong readiness owner', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-security-ydb', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', /ydb-security-isolated SQL\/storage readiness/],
];
for (const [index, command] of ordinary.entries()) {
  for (const [name, mutate] of [
    ['background command', value => `${value} &`],
    ['lost race', value => value.replace(' -race', '')],
    ['cached results', value => value.replace(' -count=1', '')],
    ['wider binary timeout', value => value.replace('-timeout=15m', '-timeout=30m')],
    ['incomplete package corpus', value => value.replace('$(GO_PACKAGE_PATTERNS)', './internal/ydbstore')],
    ['short suite', value => value.replace(' $(GO_PACKAGE_PATTERNS)', ' -short $(GO_PACKAGE_PATTERNS)')],
    ['extra selector', value => value.replace(' $(GO_PACKAGE_PATTERNS)', " -run='^TestOther' $(GO_PACKAGE_PATTERNS)")],
    ['ignored failure', value => `${value} || true`],
  ]) mutations.push([`ordinary ${index + 1} ${name}`, 'make', command, mutate(command), /exact sequential ordinary partition/]);
}
for (const [index, command] of ordinary.entries()) {
  const narrowed = index === 0
    ? command.replace('TestLocal|TestOwned|TestPrivate', 'TestLocal|TestOwned')
    : index === 1
      ? command.replace('TestOwned|TestPrivate', 'TestOwned')
      : index === 2
        ? command.replace("-run='^TestLocalCompaction'", "-run='^TestLocalCompactionSource'")
        : command.replace("-run='^TestLocal'", "-run='^TestLocalSession'");
  mutations.push([`ordinary ${index + 1} selector gap`, 'make', command, narrowed, /exact sequential ordinary partition/]);
}
mutations.push(['ordinary fourth selector overlap', 'make', ordinary[3],
  ordinary[3].replace("-skip='^TestLocal(Session|Compaction)'", "-skip='^TestLocalSessionTerminal'"),
  /exact sequential ordinary partition/]);
for (const [half, command] of [['terminal', connectedTerminal], ['other', connectedOther]]) {
  for (const [name, mutate] of [
    ['lost race', value => value.replace(' -race', '')],
    ['cached results', value => value.replace(' -count=1', '')],
    ['wider binary bound', value => value.replace('-timeout=15m', '-timeout=30m')],
    ['incomplete corpus', value => value.replace('$(GO_PACKAGE_PATTERNS)', './internal/ydbstore')],
    ['extra filter', value => value.replace(' $(GO_PACKAGE_PATTERNS)', ' -short $(GO_PACKAGE_PATTERNS)')],
    ['ignored failure', value => `${value} || true`],
  ]) mutations.push([`${half} ${name}`, 'make', command, mutate(command), /exact sequential connected partition/]);
}
for (const [name, commands, prerequisites, expected] of [
  ['test-ordinary-ci-base', ordinary.slice(0, 3), 'prepare fmt-check lint', /exact CI base partition/],
  ['test-ordinary-ci-tail', ordinaryTail, 'prepare', /exact CI tail partition/],
]) {
  const before = `${name}: ${prerequisites}\n\t${commands.join('\n\t')}\n`;
  mutations.push([`${name} missing command`, 'make', before,
    `${name}: ${prerequisites}\n${commands.slice(1).map(command => `\t${command}\n`).join('')}`, expected]);
  mutations.push([`${name} duplicate command`, 'make', before,
    before.replace(commands[0], `${commands[0]}\n\t${commands[0]}`), expected]);
  const overlap = name === 'test-ordinary-ci-base' ? ordinary[3] : ordinary[0];
  mutations.push([`${name} cross-partition overlap`, 'make', before,
    before.replace(commands[0], overlap), expected]);
  for (const [index, command] of commands.entries()) {
    for (const [fault, mutate] of [
      ['lost race', value => value.replace(' -race', '')],
      ['cached results', value => value.replace(' -count=1', '')],
      ['wider binary bound', value => value.replace('-timeout=15m', '-timeout=30m')],
      ['incomplete roots', value => value.replace('$(GO_PACKAGE_PATTERNS)', './internal/ydbstore')],
      ['extra selector', value => value.replace(' $(GO_PACKAGE_PATTERNS)', " -run='^TestOther' $(GO_PACKAGE_PATTERNS)")],
      ['short suite', value => value.replace(' $(GO_PACKAGE_PATTERNS)', ' -short $(GO_PACKAGE_PATTERNS)')],
      ['background command', value => `${value} &`],
      ['ignored failure', value => `${value} || true`],
    ]) mutations.push([`${name} command ${index + 1} ${fault}`, 'make', before, before.replace(command, mutate(command)), expected]);
  }
}
for (const name of ['ydb', 'ydb-security-isolated', 'local-stand', 'terraform']) {
  for (const missing of ['verify', 'go-connected', 'go-local']) {
    mutations.push([`${name} loses ${missing} join`, 'ci',
      `  ${name}:\n${job(original.ci, name)}`,
      `  ${name}:\n${job(original.ci, name).replace('needs: [verify, go-connected, go-local]', `needs: [${['verify', 'go-connected', 'go-local'].filter(value => value !== missing).join(', ')}]`)}`,
      new RegExp(`${name} joins all three Go jobs`)]);
  }
}
for (const name of goJobs) {
  const before = `  ${name}:\n${job(original.ci, name)}`;
  for (const [fault, mutate, expected] of [
    ['missing cache flag', value => value.replace(`          cache: "\${{ !${coldGoPredicate} }}"\n`, ''), /exact cold Go cache control/],
    ['unquoted cache expression', value => value.replace(`cache: "\${{ !${coldGoPredicate} }}"`, `cache: \${{ !${coldGoPredicate} }}`), /exact cold Go cache control/],
    ['unquoted guard expression', value => value.replace(`if: "${coldGoPredicate}"`, `if: ${coldGoPredicate}`), /exact cold Go freshness guard/],
    ['always warm', value => value.replace(`\${{ !${coldGoPredicate} }}`, 'true'), /exact cold Go cache control/],
    ['permanently cold', value => value.replace(`\${{ !${coldGoPredicate} }}`, 'false'), /exact cold Go cache control/],
    ['substring intent', value => value.replaceAll('startsWith(', 'contains('), /exact cold Go cache control/],
    ['different intent', value => value.replaceAll('ci(cold-go): ', 'ci(cold): '), /exact cold Go cache control/],
    ['missing freshness guard', value => value.replace(`      - name: ${coldGoStepName}\n        ${coldGoStep}\n\n`, ''), /unique Prove cold Go caches/],
    ['disabled freshness guard', value => value.replace(`if: "${coldGoPredicate}"`, 'if: false'), /exact cold Go freshness guard/],
    ['wrong freshness path', value => value.replace('test "$GOCACHE" = "$GITHUB_WORKSPACE/.git/sessionless-go-cache/go-build"', 'test -n "$GOCACHE"'), /exact cold Go freshness guard/],
    ['ignored nonempty cache', value => value.replace('test -z "$entries"', 'test -z "$entries" || true'), /exact cold Go freshness guard/],
    ['clears cache', value => value.replace('test -z "$entries"', 'rm -rf "$cache_dir"'), /exact cold Go freshness guard/],
    ['creates cache', value => value.replace('set -eu', 'set -eu\n          mkdir -p "$GOCACHE"'), /exact cold Go freshness guard/],
    ['ignored guard failure', value => value.replace(`shell: bash\n        run: |\n          set -eu`, `continue-on-error: true\n        shell: bash\n        run: |\n          set -eu`), /exact cold Go freshness guard/],
    ['late guard', value => value.replace(`      - name: ${coldGoStepName}\n`, '      - name: Premature repository command\n        run: make build\n\n      - name: '+coldGoStepName+'\n'), /cold Go guard immediately after setup/],
  ]) {
    const after = mutate(before);
    assert.notEqual(after, before, `${name} ${fault} mutation must be effective`);
    mutations.push([`${name} ${fault}`, 'ci', before, after, expected]);
  }
}
for (const [name, file, before, after, expected] of mutations) {
  test(`reject ${name}`, () => {
    assert.ok(original[file].includes(before), `mutation ${name} must change its input`);
    assert.throws(() => verify({ ...original, [file]: original[file].replace(before, after) }), expected);
  });
}
