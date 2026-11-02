import assert from 'node:assert/strict';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

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
const goJobs = ['verify', 'go-local', 'go-connected', 'go-runtime', 'ydb', 'ydb-security-isolated', 'local-stand'];
const goPredecessors = ['verify', 'go-connected', 'go-local', 'go-runtime'];
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
const connectedTerminal = "go test -race -count=1 -timeout=15m -run='^TestLocalSession(Terminal|Worker)' $(GO_PACKAGE_PATTERNS)";
const connectedOther = "go test -race -count=1 -timeout=15m -run='^TestLocalSession' -skip='^TestLocalSession(Terminal|Worker)' $(GO_PACKAGE_PATTERNS)";
const connectedCiTerminal = "go test -race -count=1 -timeout=15m -run='^TestLocalSessionTerminal' $(GO_PACKAGE_PATTERNS)";
const connectedWorker = "go test -race -count=1 -timeout=15m -run='^TestLocalSessionWorker' $(GO_PACKAGE_PATTERNS)";
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
const offlineMcp = [
  "go test -race -count=1 -shuffle=190351 -timeout=5m -tags=sessionless_mcp_offline -overlay=test/mcp-offline-overlay.json -run='^TestLocalSessionCredentialedMCPOfflineWebKnownServiceError' ./internal/ydbstore",
  "go test -race -count=1 -shuffle=190351 -timeout=5m -tags=sessionless_mcp_offline -overlay=test/mcp-offline-overlay.json -run='^TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailure' ./internal/ydbstore",
  "go test -race -count=1 -shuffle=190351 -timeout=5m -tags=sessionless_mcp_offline -overlay=test/mcp-offline-overlay.json -run='^TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailure' ./internal/ydbstore",
  "go test -race -count=1 -shuffle=190351 -timeout=5m -tags=sessionless_mcp_offline -overlay=test/mcp-offline-overlay.json -run='^TestLocalSessionCredentialedMCPOffline' -skip='^TestLocalSessionCredentialedMCPOfflineWeb(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)' ./internal/ydbstore",
];
const generationStepName = 'Prove attached resource generation CAS and exact uint64 persistence';
const localRuntimeStepName = 'Prove local Web runtime and two-message trigger batch';
const localRuntimeGateRecipe = [
  '@test "$(GITHUB_ACTIONS)" = true || { echo \'requires disposable GitHub CI\'; exit 1; }',
  'SESSIONLESS_LOCAL_RUNTIME_YDB_GATE=1 SESSIONLESS_LOCAL_RUNTIME_YDB_DISPOSABLE=1 \\\n\t\tgo test -v -race -count=3 -shuffle=on -timeout=5m -tags=ydbintegration \\\n\t\t-run \'^TestLocalRuntimeYDB\' ./test/ydbintegration',
].join('\n\t');
const generationStoreGateCommand = 'SESSIONLESS_OWNED_CURRENT_YDB_GATE=1 SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE=1 \\\n\t\tgo test -race -count=3 -shuffle=on -timeout=5m \\\n\t\t-run \'^TestAttachedResourceGenerationYDB\' ./internal/ydbstore';
const generationJoinedGateCommand = [
  'SESSIONLESS_OWNED_CURRENT_YDB_GATE=1 SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE=1 \\',
  '\t\tgo test -v -race -count=3 -shuffle=on -timeout=5m -tags=ydbintegration \\',
  "\t\t-run '^TestAttachedResourceGenerationYDBJoined' ./internal/attachedworkerstack",
].join('\n');
const generationGateRecipe = [
  '@test "$(GITHUB_ACTIONS)" = true || { echo \'requires disposable GitHub CI\'; exit 1; }',
  generationStoreGateCommand,
  generationJoinedGateCommand,
].join('\n\t');
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
  assert.deepEqual(target(make, 'test-connected-ci'), ['prepare'], 'CI connected preparation');
  assert.deepEqual(target(make, 'test-connected-worker-ci'), ['prepare'], 'CI worker preparation');
  assert.deepEqual(assignment(make, 'GO_PACKAGE_PATTERNS'), words('./cmd/... ./internal/... ./migrations/...'), 'complete Go corpus');
  assert.equal(recipe(make, 'test-ordinary'), ordinary.join('\n\t'), 'exact sequential ordinary partition');
  assert.equal(recipe(make, 'test-ordinary-ci-base'), ordinary.slice(0, 3).join('\n\t'), 'exact CI base partition');
  assert.equal(recipe(make, 'test-ordinary-ci-tail'), ordinaryTail.join('\n\t'), 'exact CI tail partition');
  assert.equal(recipe(make, 'test-connected'), `${connectedTerminal}\n\t${connectedOther}\n\t$(MAKE) test-mcp-offline`, 'exact sequential connected partition');
  assert.equal(recipe(make, 'test-connected-ci'), `${connectedCiTerminal}\n\t${connectedOther}`, 'exact CI normal connected partition');
  assert.equal(recipe(make, 'test-connected-worker-ci'), connectedWorker, 'exact CI worker connected partition');
  assert.deepEqual(target(make, 'test-mcp-offline'), ['prepare'], 'offline MCP preparation');
  assert.equal(recipe(make, 'test-mcp-offline'), [
    'test ! -e internal/localloopback/mcp_offline_fixture.go',
    'test ! -e internal/providerloopsession/mcp_offline_fixture.go',
    'mcp_fixture_format=$$(gofmt -l internal/localloopback/testdata/mcp_offline_fixture.go.txt internal/providerloopsession/testdata/mcp_offline_fixture.go.txt) && test -z "$$mcp_fixture_format"',
    'go build -overlay= -tags= ./internal/localloopback ./internal/providerloopsession',
    ...offlineMcp,
  ].join('\n\t'), 'exact additive offline MCP gate');
  assert.deepEqual(target(make, 'attached-worker-security-gate'),
    words('attached-worker-crash-integration attached-worker-active-crash-integration'));
  assert.equal(recipe(make, 'ci-partition-test'), 'node --test scripts/test-ci-partition.mjs\n\tnode --test scripts/test-ci-ydb-readiness.mjs');
  const core = job(ci, 'verify');
  const connected = job(ci, 'go-connected');
  const local = job(ci, 'go-local');
  const runtime = job(ci, 'go-runtime');
  const security = job(ci, 'ydb-security-isolated');
  assert.deepEqual([...ci.split('\njobs:\n')[1].matchAll(/^  ([\w-]+):\n/gm)].map(match => match[1]),
    ['verify', 'go-local', 'go-connected', 'go-runtime', 'web-browser', 'ydb', 'ydb-security-isolated', 'local-stand', 'terraform', 'cloudflare-edge', 'images'], 'eleven required jobs');
  assert.match(core, /^    timeout-minutes: 30$/m);
  assert.doesNotMatch(core, /^    (?:needs|permissions|env):/m, 'core base is parallel, inherits read-only policy');
  assert.match(connected, /^    timeout-minutes: 30$/m, 'connected 30-minute bound');
  assert.match(connected, /^    runs-on: ubuntu-latest$/m);
  assert.doesNotMatch(connected, /^    (?:needs|permissions|env):/m, 'connected is parallel, inherits read-only policy');
  assert.match(ci, /^permissions:\n  contents: read\n/m, 'read-only workflow');
  assert.equal(ci.match(/^env:\n((?:  [^\n]*\n)+)/m)?.[1],
    '  GOCACHE: ${{ github.workspace }}/.git/sessionless-go-cache/go-build\n  GOMODCACHE: ${{ github.workspace }}/.git/sessionless-go-cache/go-mod\n', 'only shared Go caches in workflow environment');
  assert.equal(step(connected, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact connected checkout');
  const actualGoJobs = [...ci.split('\njobs:\n')[1].matchAll(/^  ([\w-]+):\n/gm)]
    .map(match => match[1]).filter(name => /uses: actions\/setup-go@/.test(job(ci, name)));
  assert.deepEqual(actualGoJobs, goJobs, 'all seven host Go jobs have the cold-cache contract');
  for (const name of goJobs) {
    const value = job(ci, name);
    assert.equal(step(value, 'Set up pinned Go'), pinnedGo, `${name} exact cold Go cache control`);
    assert.equal(step(value, coldGoStepName), coldGoStep, `${name} exact cold Go freshness guard`);
    assert.equal(value.slice(value.indexOf('    steps:\n'), value.indexOf(`      - name: ${coldGoStepName}\n`)),
      '    steps:\n      - name: Checkout mirrored commit\n        uses: actions/checkout@v6\n\n', `${name} cold Go guard immediately after checkout`);
    assert.ok(value.includes(`      - name: ${coldGoStepName}\n        ${coldGoStep}\n\n      - name: Set up pinned Go\n        ${pinnedGo}\n`), `${name} cold Go guard immediately before setup`);
  }
  assert.equal(step(connected, 'Run connected-session tests over all packages'), 'run: make test-connected-ci', 'complete connected invocation');
  assert.equal([...connected.matchAll(/^      - name:/gm)].length, 4, 'bounded connected steps');
  assert.match(local, /^    timeout-minutes: 30$/m, 'local tail 30-minute bound');
  assert.match(local, /^    runs-on: ubuntu-latest$/m);
  assert.doesNotMatch(local, /^    (?:needs|permissions|env):/m, 'local tail is parallel, inherits read-only policy');
  assert.equal(step(local, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact local tail checkout');
  assert.equal(step(local, 'Set up pinned Go'), step(connected, 'Set up pinned Go'), 'same pinned local tail Go/cache setup');
  assert.equal(step(local, 'Run ordinary local-tail tests over all packages'), 'run: make test-ordinary-ci-tail', 'complete local tail invocation');
  assert.match(runtime, /^    name: Go MCP and connected-worker verification$/m, 'runtime job name');
  assert.match(runtime, /^    timeout-minutes: 30$/m, 'runtime 30-minute bound');
  assert.match(runtime, /^    runs-on: ubuntu-latest$/m, 'runtime platform');
  assert.doesNotMatch(runtime, /^    (?:needs|permissions|env|services|container|defaults|strategy):/m, 'runtime is parallel, offline, inherits read-only policy');
  assert.equal(step(runtime, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact runtime checkout');
  assert.deepEqual([...runtime.matchAll(/^      - name: (.+)$/gm)].map(match => match[1]),
    ['Checkout mirrored commit', coldGoStepName, 'Set up pinned Go', 'Run additive offline MCP connected fixtures', 'Run connected-worker tests over all packages'], 'exact runtime steps and MCP-before-Worker order');
  assert.equal(step(runtime, 'Run connected-worker tests over all packages'), 'run: make test-connected-worker-ci', 'complete worker connected invocation');
  assert.equal([...ci.matchAll(/^        run: make test-connected-worker-ci$/gm)].length, 1, 'one required CI connected Worker execution');
  assert.equal([...ci.matchAll(/\bmake test-connected-worker-ci\b/g)].length, 1, 'no alternate or duplicate CI Worker invocation');
  assert.deepEqual([...core.matchAll(/^      - name: (.+)$/gm)].map(match => match[1]),
    ['Checkout mirrored commit', coldGoStepName, 'Set up pinned Go', 'Set up pinned Node.js', 'Set up pinned npm', 'Run repository CI contract'], 'core-only verification steps');
  assert.equal([...local.matchAll(/^      - name:/gm)].length, 4, 'bounded local tail steps');
  assert.match(ci, /^env:\n  GOCACHE: \$\{\{ github.workspace \}\}\/\.git\/sessionless-go-cache\/go-build\n  GOMODCACHE: \$\{\{ github.workspace \}\}\/\.git\/sessionless-go-cache\/go-mod\n/m, 'shared Go cache policy');
  assert.match(security, /^    timeout-minutes: 25$/m);
  for (const name of ['ydb', 'ydb-security-isolated', 'local-stand', 'terraform']) {
    assert.match(job(ci, name), /^    needs: \[verify, go-connected, go-local, go-runtime\]$/m, `${name} joins all four Go jobs`);
  }
  for (const [name, container] of [['ydb', 'sessionless-ydb'], ['ydb-security-isolated', 'sessionless-security-ydb']]) {
    const text = job(ci, name);
    assert.equal(step(text, 'Wait for YDB Local'), `run: bash scripts/wait-ci-ydb.sh ${container}`, `${name} SQL/storage readiness`);
    assert.ok(text.indexOf('Start pinned YDB Local') < text.indexOf('Wait for YDB Local'));
    assert.ok(text.indexOf('Wait for YDB Local') < text.indexOf('Apply schema twice'));
    assert.equal(step(text, 'Apply schema twice'), 'run: |\n          make migrate-local\n          make migrate-local', `${name} no migration retry`);
  }
  const ydb = job(ci, 'ydb');
  assert.match(ydb, /^    timeout-minutes: 25$/m, 'generation proof preserves existing YDB job bound');
  assert.deepEqual(target(make, 'attached-resource-generation-ydb-gate'), ['prepare']);
  assert.equal(recipe(make, 'attached-resource-generation-ydb-gate'), generationGateRecipe, 'bounded disposable exact generation test selector');
  assert.equal(step(ydb, generationStepName),
    'env:\n          YDB_CONNECTION_STRING: grpc://localhost:2136/local?go_query_mode=data&go_query_bind=declare,numeric\n        run: make attached-resource-generation-ydb-gate', 'genuine data transactions, no fake transaction DSN');
  assert.ok(ydb.indexOf(generationStepName) > ydb.indexOf('Prove inline-output transactional claim and write custody'));
  assert.ok(ydb.indexOf(generationStepName) < ydb.indexOf('Capture failed YDB Local state'));
  assert.deepEqual(target(make, 'local-runtime-ydb-gate'), ['prepare']);
  assert.equal(recipe(make, 'local-runtime-ydb-gate'), localRuntimeGateRecipe, 'bounded disposable local runtime selector');
  assert.equal(step(ydb, localRuntimeStepName),
    'env:\n          YDB_CONNECTION_STRING: grpc://localhost:2136/local?go_query_mode=data&go_query_bind=declare,numeric\n        run: make local-runtime-ydb-gate', 'local runtime genuine data transactions');
  assert.ok(ydb.indexOf(localRuntimeStepName) > ydb.indexOf(generationStepName));
  assert.ok(ydb.indexOf(localRuntimeStepName) < ydb.indexOf('Capture failed YDB Local state'));
  assert.equal(step(core, 'Run repository CI contract'), 'run: make ci-core-base', 'complete CI base invocation');
  const mcpStepName = 'Run additive offline MCP connected fixtures';
  assert.equal(step(runtime, mcpStepName), 'run: make test-mcp-offline', 'required complete CI offline MCP invocation');
  assert.equal([...ci.matchAll(/^        run: make test-mcp-offline$/gm)].length, 1, 'one required CI offline MCP execution');
  assert.equal([...ci.matchAll(/\bmake test-mcp-offline\b/g)].length, 1, 'no alternate or duplicate CI MCP invocation');
  assert.equal([...ci.matchAll(/\bmake ci-core-base\b/g)].length, 1, 'one complete CI core invocation');
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
    words('verify go-connected go-local go-runtime ydb ydb-security-isolated local-stand terraform cloudflare-edge web-browser'));
  for (const name of ['verify', 'go-connected', 'go-local', 'go-runtime', 'ydb', 'ydb-security-isolated', 'local-stand', 'terraform', 'images']) {
    const value = job(ci, name);
    assert.doesNotMatch(value, /^    (?:if|continue-on-error):/m, `${name} cannot bypass job failure`);
    assert.doesNotMatch(value, /^        continue-on-error:/m, `${name} cannot ignore step failure`);
  }
}

test('local, release and mirrored CI retain the complete partition', () => verify(original));

test('generation SQL qualification cannot lose its guards, coverage or real transaction mode', () => {
  for (const make of [
    original.make.replace(generationGateRecipe, generationGateRecipe.replace('@test "$(GITHUB_ACTIONS)" = true', '@true')),
    original.make.replace(generationGateRecipe, generationGateRecipe.replace('SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE=1 ', '')),
    original.make.replace(generationGateRecipe, generationGateRecipe.replace('-count=3', '-count=1')),
    original.make.replace(generationGateRecipe, generationGateRecipe.replace('^TestAttachedResourceGenerationYDB', '^TestOther')),
  ]) assert.throws(() => verify({ ...original, make }));
  const nativeStep = step(job(original.ci, 'ydb'), generationStepName);
  assert.throws(() => verify({ ...original, ci: original.ci.replace(nativeStep, nativeStep.replace('go_query_mode=data&', 'go_query_mode=scripting&go_fake_tx=scripting&')) }));
});

test('local runtime SQL proof retains its finite remote-only contract', () => {
  for (const [from, to] of [
    ['@test "$(GITHUB_ACTIONS)" = true', '@true'],
    ['SESSIONLESS_LOCAL_RUNTIME_YDB_GATE=1 ', ''],
    ['SESSIONLESS_LOCAL_RUNTIME_YDB_DISPOSABLE=1 ', ''],
    [' -v', ''], [' -race', ''], ['-count=3', '-count=1'],
    ['-shuffle=on', '-shuffle=off'], ['-timeout=5m', '-timeout=10m'],
    [' -tags=ydbintegration', ''], ['^TestLocalRuntimeYDB', '^TestOther'],
    ['./test/ydbintegration', './internal/ydbstore'],
  ]) {
    const changed = localRuntimeGateRecipe.replace(from, to);
    assert.notEqual(changed, localRuntimeGateRecipe, `effective mutation ${from}`);
    assert.throws(() => verify({ ...original, make: original.make.replace(localRuntimeGateRecipe, changed) }));
  }
  const nativeStep = step(job(original.ci, 'ydb'), localRuntimeStepName);
  for (const changed of [
    nativeStep.replace('go_query_mode=data&', 'go_query_mode=scripting&go_fake_tx=scripting&'),
    nativeStep.replace('run: make local-runtime-ydb-gate', 'run: make local-runtime-ydb-gate || true'),
    `if: false\n        ${nativeStep}`,
  ]) assert.throws(() => verify({ ...original, ci: original.ci.replace(nativeStep, changed) }));
});

test('joined generation qualification keeps exact selection, order and independent bounds', () => {
  const mutations = [
    ['missing joined command', generationGateRecipe.replace('\n\t' + generationJoinedGateCommand, '')],
    ['missing joined explicit gate', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('SESSIONLESS_OWNED_CURRENT_YDB_GATE=1 ', ''))],
    ['missing joined disposable guard', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE=1 ', ''))],
    ['missing joined race detector', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace(' -race', ''))],
    ['missing joined repeat', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('-count=3', '-count=1'))],
    ['missing joined shuffle', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('-shuffle=on', '-shuffle=off'))],
    ['renewed joined failure budget', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('-timeout=5m', '-timeout=15m'))],
    ['missing joined build tag', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace(' -tags=ydbintegration', ''))],
    ['wrong joined test selector', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('^TestAttachedResourceGenerationYDBJoined', '^TestOther'))],
    ['wrong joined package', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace('./internal/attachedworkerstack', './internal/ydbstore'))],
    ['skipped joined negative', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand.replace(' -tags=ydbintegration', " -tags=ydbintegration -skip='GateRemoval'"))],
    ['ignored joined failure', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand + ' || true')],
    ['parallel joined command', generationGateRecipe.replace(generationJoinedGateCommand,
      generationJoinedGateCommand + ' &')],
    ['joined before SQL qualification', generationGateRecipe.replace(generationStoreGateCommand + '\n\t' + generationJoinedGateCommand,
      generationJoinedGateCommand + '\n\t' + generationStoreGateCommand)],
  ];
  for (const [name, mutated] of mutations) {
    assert.notEqual(mutated, generationGateRecipe, name + ' mutation must change the recipe');
    assert.throws(() => verify({ ...original, make: original.make.replace(generationGateRecipe, mutated) }),
      /bounded disposable exact generation test selector/, name);
  }
});

test('dedicated runtime MCP keeps the complete local gate and required executions', () => {
  const mcpName = 'Run additive offline MCP connected fixtures';
  const mcpStep = `      - name: ${mcpName}\n        run: make test-mcp-offline\n`;
  const coreStep = '      - name: Run repository CI contract\n        run: make ci-core-base\n';
  for (const ci of [
    original.ci.replace(mcpStep, ''),
    original.ci.replace(mcpStep, mcpStep.replace('        run:', '        if: false\n        run:')),
    original.ci.replace(mcpStep, mcpStep.replace('        run:', '        continue-on-error: true\n        run:')),
    original.ci.replace(mcpStep, mcpStep.replace('make test-mcp-offline', 'make test-mcp-offline || true')),
    original.ci.replace(mcpStep, '').replace(coreStep, mcpStep + '\n' + coreStep),
    original.ci.replace('run: make test-connected-ci\n', 'run: make test-connected\n'),
  ]) {
    assert.notEqual(ci, original.ci, 'mutation must change the workflow');
    assert.throws(() => verify({ ...original, ci }));
  }
  const normal = recipe(original.make, 'test-connected-ci');
  for (const changed of [
    connectedOther,
    connectedCiTerminal,
    `${connectedOther}\n\t${connectedCiTerminal}`,
    `${normal}\n\t$(MAKE) test-mcp-offline`,
    normal.replace(connectedCiTerminal, connectedCiTerminal + ' &'),
    normal.replace('-timeout=15m', '-timeout=30m'),
  ]) {
    assert.notEqual(changed, normal);
    const make = original.make.replace(`test-connected-ci: prepare\n\t${normal}`, `test-connected-ci: prepare\n\t${changed}`);
    assert.notEqual(make, original.make, 'mutation must change the CI recipe');
    assert.throws(() => verify({ ...original, make }));
  }
});

test('dedicated runtime Worker remains required, complete and sequential', () => {
  const workerStep = '      - name: Run connected-worker tests over all packages\n        run: make test-connected-worker-ci\n';
  const tailStep = '      - name: Run ordinary local-tail tests over all packages\n        run: make test-ordinary-ci-tail\n';
  const coreStep = '      - name: Run repository CI contract\n        run: make ci-core-base\n';
  const mcpStep = '      - name: Run additive offline MCP connected fixtures\n        run: make test-mcp-offline\n';
  const connectedStep = '      - name: Run connected-session tests over all packages\n        run: make test-connected-ci\n';
  for (const ci of [
    original.ci.replace(workerStep, ''),
    original.ci.replace(workerStep, workerStep.replace('        run:', '        if: false\n        run:')),
    original.ci.replace(workerStep, workerStep.replace('        run:', '        continue-on-error: true\n        run:')),
    original.ci.replace(workerStep, workerStep.replace('make test-connected-worker-ci', 'make test-connected-worker-ci || true')),
    original.ci.replace(workerStep, '').replace(tailStep, workerStep + '\n' + tailStep),
    original.ci.replace(workerStep, '').replace(coreStep, workerStep + '\n' + coreStep),
    original.ci.replace(workerStep, '').replace(mcpStep, workerStep + '\n' + mcpStep),
    original.ci.replace(workerStep, '').replace(connectedStep, connectedStep + '\n' + workerStep),
    original.ci.replace('      - name: Run repository CI contract\n        run: make ci-core-base\n', '      - name: Run repository CI contract\n        run: make ci-core-base\n\n      - name: Duplicate connected-worker execution\n        run: make test-connected-worker-ci\n'),
  ]) {
    assert.notEqual(ci, original.ci);
    assert.throws(() => verify({ ...original, ci }));
  }
  for (const changed of [
    '', connectedWorker + ' &', connectedWorker + ' || true',
    connectedWorker.replace(' -race', ''), connectedWorker.replace('-count=1', '-count=2'),
    connectedWorker.replace('-timeout=15m', '-timeout=30m'),
    connectedWorker.replace('^TestLocalSessionWorker', '^TestLocalSessionWorkerEntry'),
    connectedWorker.replace('$(GO_PACKAGE_PATTERNS)', './internal/ydbstore'),
  ]) {
    const make = original.make.replace(`test-connected-worker-ci: prepare\n\t${connectedWorker}`, `test-connected-worker-ci: prepare\n\t${changed}`);
    assert.notEqual(make, original.make);
    assert.throws(() => verify({ ...original, make }));
  }
});

test('runtime job stays parallel, Go-only, bounded and MCP-before-Worker', () => {
  const runtime = job(original.ci, 'go-runtime');
  const before = `  go-runtime:\n${runtime}`;
  const mcp = '      - name: Run additive offline MCP connected fixtures\n        run: make test-mcp-offline\n';
  const worker = '      - name: Run connected-worker tests over all packages\n        run: make test-connected-worker-ci\n';
  for (const [name, mutate, expected] of [
    ['missing job', () => '', /unique go-runtime job/],
    ['renamed job', value => value.replace('  go-runtime:', '  other-runtime:'), /unique go-runtime job/],
    ['serialized job', value => value.replace('    steps:', '    needs: verify\n    steps:'), /runtime is parallel/],
    ['database environment', value => value.replace('    steps:', '    env:\n      YDB_CONNECTION_STRING: grpc:\/\/localhost:2136/local\n    steps:'), /runtime is parallel/],
    ['write permission', value => value.replace('    steps:', '    permissions:\n      contents: write\n    steps:'), /runtime is parallel/],
    ['service', value => value.replace('    steps:', '    services:\n      database:\n        image: ydbplatform/local-ydb\n    steps:'), /runtime is parallel/],
    ['container', value => value.replace('    steps:', '    container: ubuntu:latest\n    steps:'), /runtime is parallel/],
    ['strategy', value => value.replace('    steps:', '    strategy:\n      fail-fast: false\n    steps:'), /runtime is parallel/],
    ['defaults', value => value.replace('    steps:', '    defaults:\n      run:\n        shell: bash\n    steps:'), /runtime is parallel/],
    ['different platform', value => value.replace('runs-on: ubuntu-latest', 'runs-on: macos-latest'), /runtime platform/],
    ['wider budget', value => value.replace('timeout-minutes: 30', 'timeout-minutes: 45'), /runtime 30-minute bound/],
    ['missing budget', value => value.replace('    timeout-minutes: 30\n', ''), /runtime 30-minute bound/],
    ['Node setup', value => value.replace(mcp, '      - name: Unneeded Node\n        uses: actions/setup-node@v6\n\n' + mcp), /exact runtime steps/],
    ['reversed groups', value => value.replace(mcp + '\n' + worker, worker + '\n' + mcp), /exact runtime steps/],
    ['extra core group', value => value.replace(worker, worker + '\n      - name: Extra core\n        run: make ci-core-base\n'), /exact runtime steps/],
    ['wrong checkout', value => value.replace('actions/checkout@v6', 'actions/checkout@v5'), /cold Go guard immediately after checkout/],
    ['conditional job', value => value.replace('    steps:', '    if: false\n    steps:'), /go-runtime cannot bypass job failure/],
    ['ignored job', value => value.replace('    steps:', '    continue-on-error: true\n    steps:'), /go-runtime cannot bypass job failure/],
  ]) {
    const after = mutate(before);
    assert.notEqual(after, before, `runtime ${name} mutation must be effective`);
    assert.throws(() => verify({ ...original, ci: original.ci.replace(before, after) }), expected, name);
  }
  for (const [name, stepText] of [['MCP', mcp], ['Worker', worker]]) {
    for (const [fault, mutate] of [
      ['step environment', value => value.replace('        run:', '        env:\n          YDB_CONNECTION_STRING: grpc://localhost:2136/local\n        run:')],
      ['background command', value => value.replace(/(make [^\n]+)/, '$1 &')],
      ['ignored failure', value => value.replace(/(make [^\n]+)/, '$1 || true')],
      ['step timeout', value => value.replace('        run:', '        timeout-minutes: 60\n        run:')],
    ]) {
      const changed = mutate(stepText);
      assert.notEqual(changed, stepText, `${name} ${fault} mutation must be effective`);
      assert.throws(() => verify({ ...original, ci: original.ci.replace(stepText, changed) }), /complete .*invocation/, `${name} ${fault}`);
    }
  }
  const leaked = original.ci.replace('env:\n', 'env:\n  YDB_CONNECTION_STRING: grpc://localhost:2136/local\n');
  assert.notEqual(leaked, original.ci);
  assert.throws(() => verify({ ...original, ci: leaked }), /only shared Go caches in workflow environment/);
});

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
function connectedSelections(name) {
  return [
    /^TestLocalSession(Terminal|Worker)/.test(name),
    /^TestLocalSession/.test(name) && !/^TestLocalSession(Terminal|Worker)/.test(name),
  ];
}
function offlineMcpSelections(name) {
  return [
    /^TestLocalSessionCredentialedMCPOfflineWebKnownServiceError/.test(name),
    /^TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailure/.test(name),
    /^TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailure/.test(name),
    /^TestLocalSessionCredentialedMCPOffline/.test(name) && !/^TestLocalSessionCredentialedMCPOfflineWeb(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)/.test(name),
  ];
}
test('offline MCP selectors cover current and future prefix members exactly once', t => {
  for (const name of [
    'TestLocalSessionCredentialedMCPOffline',
    'TestLocalSessionCredentialedMCPOfflineWeb',
    'TestLocalSessionCredentialedMCPOfflineWebKnownServiceError',
    'TestLocalSessionCredentialedMCPOfflineWebKnownServiceErrorFuture',
    'TestLocalSessionCredentialedMCPOfflineWebKnownServiceErrorish',
    'TestLocalSessionCredentialedMCPOfflineWebKnownServiceErro',
    'TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailure',
    'TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailureFuture',
    'TestLocalSessionCredentialedMCPOfflineWebModelAfterMCPFailur',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailure',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureFuture',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureish',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailureKnownServiceError',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCPFailur',
    'TestLocalSessionCredentialedMCPOfflineWebUnknownMCP',
    'TestLocalSessionCredentialedMCPOfflineWebFutureFamily',
    'TestLocalSessionCredentialedMCPOfflineWebish',
    'TestLocalSessionCredentialedMCPOfflineCanonicalTerminalComplete',
    'TestLocalSessionCredentialedMCPOfflineFutureFamily',
    'TestLocalSessionCredentialedMCPOfflineManagerWeb',
    'TestLocalSessionCredentialedMCP', 'TestLocalSessionOther',
    'ExampleLocalSessionCredentialedMCPOffline', 'FuzzLocalSessionCredentialedMCPOffline',
  ]) {
    assert.equal(offlineMcpSelections(name).filter(Boolean).length,
      Number(/^TestLocalSessionCredentialedMCPOffline/.test(name)), name);
  }
  for (const [suffix, partition] of [
    ['WebUnknownMCPFailure', 2], ['WebUnknownMCPFailureFuture', 2],
    ['WebUnknownMCPFailureish', 2], ['WebUnknownMCPFailureKnownServiceError', 2],
    ['WebUnknownMCPFailur', 3], ['WebUnknownMCP', 3], ['WebFutureFamily', 3],
    ['WebKnownServiceErrorUnknownMCPFailure', 0], ['WebModelAfterMCPFailureUnknownMCPFailure', 1],
  ]) {
    const selected = offlineMcpSelections('TestLocalSessionCredentialedMCPOffline' + suffix);
    assert.deepEqual(selected, [0, 1, 2, 3].map(index => index === partition), `${suffix}: exact family, not just union`);
  }
  const counts = [0, 0, 0, 0];
  for (const entry of readdirSync(new URL('../internal/ydbstore/', import.meta.url))) {
    if (!entry.endsWith('_test.go')) continue;
    for (const match of read(`internal/ydbstore/${entry}`).matchAll(/^func ((?:Test|Fuzz)\w+|Example\w*)\(/gm)) {
      const name = match[1];
      const selected = offlineMcpSelections(name);
      assert.equal(selected.filter(Boolean).length,
        Number(/^TestLocalSessionCredentialedMCPOffline/.test(name)), `${entry}:${name}`);
      selected.forEach((value, index) => { counts[index] += Number(value); });
    }
  }
  assert.ok(counts.every(count => count > 0), `actual nonempty offline MCP partitions: ${counts}`);
  t.diagnostic(`actual offline MCP family census F1/F2/F3/complement: ${counts.join('/')}; total=${counts.reduce((sum, value) => sum + value, 0)}`);
});
function ciConnectedSelections(name) {
  return [
    /^TestLocalSessionTerminal/.test(name),
    /^TestLocalSessionWorker/.test(name),
    /^TestLocalSession/.test(name) && !/^TestLocalSession(Terminal|Worker)/.test(name),
  ];
}
test('connected selectors are disjoint and cover the original complete prefix', () => {
  for (const name of [
    'TestLocalSession', 'TestLocalSessionTerminal',
    'TestLocalSessionTerminalCleanupStorageFaultsConnected',
    'TestLocalSessionRetainedDeletionConnected',
    'TestLocalSessionWorker', 'TestLocalSessionWorkerPreflightOriginalPositiveReadOnly',
    'TestLocalSessionWorkerTerminal', 'TestLocalSessionTerminalWorker',
    'TestLocalSessionWorkerFutureFamily',
    'TestLocalSessionFutureFamily', 'TestOther', 'ExampleLocalSession', 'FuzzLocalSession',
  ]) {
    const allConnected = /^TestLocalSession/.test(name);
    const selected = connectedSelections(name).filter(Boolean).length;
    assert.equal(selected, Number(allConnected), name);
    assert.equal(Number(!allConnected) + selected, 1, name);
    assert.equal(ciConnectedSelections(name).filter(Boolean).length, Number(allConnected), `${name}: CI exact union`);
  }
});
test('connected partition covers the actual repository test-name corpus', () => {
  const counts = [0, 0];
  const ciCounts = [0, 0, 0];
  function inspect(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) inspect(path);
      else if (entry.isFile() && entry.name.endsWith('_test.go')) {
        for (const match of readFileSync(path, 'utf8').matchAll(/^func ((?:Test|Fuzz)\w+|Example\w*)\(/gm)) {
          const name = match[1];
          const selected = connectedSelections(name);
          assert.equal(selected.filter(Boolean).length, Number(/^TestLocalSession/.test(name)), `${path}:${name}`);
          selected.forEach((value, index) => { counts[index] += Number(value); });
          const ciSelected = ciConnectedSelections(name);
          assert.equal(ciSelected.filter(Boolean).length, Number(/^TestLocalSession/.test(name)), `${path}:${name}: CI`);
          ciSelected.forEach((value, index) => { ciCounts[index] += Number(value); });
        }
      }
    }
  }
  for (const root of ['cmd', 'internal', 'migrations']) inspect(fileURLToPath(new URL(`../${root}`, import.meta.url)));
  assert.ok(counts.every(count => count > 0), `actual nonempty connected partitions: ${counts}`);
  assert.ok(ciCounts.every(count => count > 0), `actual nonempty CI connected partitions: ${ciCounts}`);
});
test('ordinary selectors and connected selectors partition the complete test-name corpus', () => {
  const names = [
    'TestOrdinary', 'TestOtherFutureFamily', 'TestLocal', 'TestLocalFutureFamily',
    'TestLocalCompaction', 'TestLocalCompactionSourceDenies',
    'TestLocalCompactionFutureFamily', 'TestLocalPreparedModelTwoCompactionsOriginalChain',
    'TestOwned', 'TestOwnedFutureFamily', 'TestPrivate', 'TestPrivateFutureFamily',
    'TestLocalSession', 'TestLocalSessionFutureFamily', 'TestLocalSessionTerminal',
    'TestLocalSessionCompaction', 'TestLocalSessionTerminalCompaction', 'TestLocalSessionWorker',
    'TestLocalSessionWorkerFuture', 'TestLocalSessionWorkerTerminal',
    'Example', 'ExampleLocalSession', 'ExampleLocalCompaction', 'Fuzz', 'FuzzPrivateBytes',
    'FuzzLocalCompaction',
  ];
  for (const name of names) {
    const selected = [
      !/^(?:TestLocal|TestOwned|TestPrivate)/.test(name),
      /^Test(?:Owned|Private)/.test(name),
      /^TestLocalCompaction/.test(name),
      /^TestLocal/.test(name) && !/^TestLocal(Session|Compaction)/.test(name),
      ...connectedSelections(name),
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
  ['overlapping connected partitions', 'make', connectedOther, connectedOther.replace(" -skip='^TestLocalSession(Terminal|Worker)'", ''), /exact sequential connected partition/],
  ['gap in terminal half', 'make', connectedTerminal, connectedTerminal.replace('(Terminal|Worker)', 'Terminal'), /exact sequential connected partition/],
  ['gap in other half', 'make', connectedOther, connectedOther.replace("-skip='^TestLocalSession(Terminal|Worker)'", "-skip='^TestLocalSession'"), /exact sequential connected partition/],
  ['gap in worker family', 'make', connectedTerminal, connectedTerminal.replace('(Terminal|Worker)', '(Terminal|WorkerEntry)'), /exact sequential connected partition/],
  ['reversed connected partitions', 'make', `${connectedTerminal}\n\t${connectedOther}`, `${connectedOther}\n\t${connectedTerminal}`, /exact sequential connected partition/],
  ['parallel connected halves', 'make', connectedTerminal, `${connectedTerminal} &`, /exact sequential connected partition/],
  ['missing connected job', 'ci', '  go-connected:\n', '  removed-connected:\n', /unique go-connected job/],
  ['serialized connected job', 'ci', '  go-connected:\n', '  go-connected:\n    needs: verify\n', /connected is parallel/],
  ['wrong connected target', 'ci', 'run: make test-connected-ci\n', 'run: make test-ordinary\n', /complete connected invocation/],
  ['skipped connected step', 'ci', '      - name: Run connected-session tests over all packages\n', '      - name: Run connected-session tests over all packages\n        if: false\n', /complete connected invocation/],
  ['ignored connected command', 'ci', 'run: make test-connected-ci\n', 'run: make test-connected-ci || true\n', /complete connected invocation/],
  ['connected gains write permission', 'ci', '  go-connected:\n', '  go-connected:\n    permissions:\n      contents: write\n', /connected is parallel/],
  ['image loses connected', 'ci', '      - go-connected\n', ''],
  ['image loses local tail', 'ci', '      - go-local\n', ''],
  ['removed readiness policy', 'make', '\tnode --test scripts/test-ci-ydb-readiness.mjs\n', ''],
  ['liveness replaces SQL health', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', 'run: curl http://localhost:8765/monitoring/cluster', /ydb SQL\/storage readiness/],
  ['wrong readiness owner', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-security-ydb', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', /ydb-security-isolated SQL\/storage readiness/],
];
for (const [index, command] of offlineMcp.entries()) {
  for (const [name, mutate] of [
    ['lost race', value => value.replace(' -race', '')],
    ['cached results', value => value.replace(' -count=1', '')],
    ['lost shuffle', value => value.replace(' -shuffle=190351', '')],
    ['wider binary timeout', value => value.replace('-timeout=5m', '-timeout=10m')],
    ['lost fixture tag', value => value.replace(' -tags=sessionless_mcp_offline', '')],
    ['lost additive overlay', value => value.replace(' -overlay=test/mcp-offline-overlay.json', '')],
    ['incomplete package', value => value.replace('./internal/ydbstore', './internal/worker')],
    ['extra exclusion', value => value.replace(' ./internal/ydbstore', " -skip='Terminal' ./internal/ydbstore")],
    ['short suite', value => value.replace(' ./internal/ydbstore', ' -short ./internal/ydbstore')],
    ['background command', value => `${value} &`],
    ['ignored failure', value => `${value} || true`],
  ]) mutations.push([`offline MCP ${index + 1} ${name}`, 'make', command, mutate(command), /exact additive offline MCP gate/]);
  mutations.push([`offline MCP ${index + 1} missing`, 'make', `\t${command}\n`, '', /exact additive offline MCP gate/]);
  mutations.push([`offline MCP ${index + 1} duplicated`, 'make', command, `${command}\n\t${command}`, /exact additive offline MCP gate/]);
}
mutations.push(['offline MCP reversed', 'make', offlineMcp.join('\n\t'), [...offlineMcp].reverse().join('\n\t'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP overlap', 'make', offlineMcp[3], offlineMcp[3].replace(" -skip='^TestLocalSessionCredentialedMCPOfflineWeb(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)'", ''), /exact additive offline MCP gate/]);
mutations.push(['offline MCP F1 gap', 'make', offlineMcp[0], offlineMcp[0].replace('WebKnownServiceError', 'WebKnownServiceErrorFreshHistory'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP F2 gap', 'make', offlineMcp[1], offlineMcp[1].replace('WebModelAfterMCPFailure', 'WebModelAfterMCPFailureFreshHistory'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP F3 gap', 'make', offlineMcp[2], offlineMcp[2].replace('WebUnknownMCPFailure', 'WebUnknownMCPFailureFreshHistory'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP complement loses F1 exclusion', 'make', offlineMcp[3], offlineMcp[3].replace('Web(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)', 'Web(ModelAfterMCPFailure|UnknownMCPFailure)'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP complement loses F2 exclusion', 'make', offlineMcp[3], offlineMcp[3].replace('Web(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)', 'Web(KnownServiceError|UnknownMCPFailure)'), /exact additive offline MCP gate/]);
mutations.push(['offline MCP complement loses F3 exclusion', 'make', offlineMcp[3], offlineMcp[3].replace('Web(KnownServiceError|ModelAfterMCPFailure|UnknownMCPFailure)', 'Web(KnownServiceError|ModelAfterMCPFailure)'), /exact additive offline MCP gate/]);
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
  for (const missing of goPredecessors) {
    mutations.push([`${name} loses ${missing} join`, 'ci',
      `  ${name}:\n${job(original.ci, name)}`,
      `  ${name}:\n${job(original.ci, name).replace('needs: [verify, go-connected, go-local, go-runtime]', `needs: [${goPredecessors.filter(value => value !== missing).join(', ')}]`)}`,
      new RegExp(`${name} joins all four Go jobs`)]);
  }
}
mutations.push(['image loses direct runtime join', 'ci', '      - go-runtime\n', '']);
for (const name of goJobs) {
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
    ['late guard', value => value.replace(`      - name: ${coldGoStepName}\n`, '      - name: Premature repository command\n        run: make build\n\n      - name: '+coldGoStepName+'\n'), /cold Go guard immediately after checkout/],
    ['guard after setup', value => value.replace(`      - name: ${coldGoStepName}\n        ${coldGoStep}\n\n      - name: Set up pinned Go\n        ${pinnedGo}\n`, `      - name: Set up pinned Go\n        ${pinnedGo}\n\n      - name: ${coldGoStepName}\n        ${coldGoStep}\n`), /cold Go guard immediately after checkout/],
  ]) {
    test(`reject ${name} ${fault}`, () => {
      const before = `  ${name}:\n${job(original.ci, name)}`;
      const after = mutate(before);
      assert.notEqual(after, before, `${name} ${fault} mutation must be effective`);
      assert.throws(() => verify({ ...original, ci: original.ci.replace(before, after) }), expected);
    });
  }
}
for (const [name, file, before, after, expected] of mutations) {
  test(`reject ${name}`, () => {
    assert.ok(original[file].includes(before), `mutation ${name} must change its input`);
    assert.throws(() => verify({ ...original, [file]: original[file].replace(before, after) }), expected);
  });
}
