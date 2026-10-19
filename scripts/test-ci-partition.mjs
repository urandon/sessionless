import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';

// Deliberately checks this repository's fixed, indentation-sensitive format,
// not arbitrary YAML/Make syntax. Structural changes require explicit review.
const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');
const original = {
  make: read('Makefile'),
  ci: read('.github/workflows/ci.yml'),
  release: read('.github/workflows/release.yml'),
};
const required = `docs-check cloud-web-smoke-test web-ci generate test-ordinary build integration
  image-publication-test image-build-inputs-test
  image-publish-policy-test registry-gc-policy-test release-policy-test
  local-stand-policy-test dockerless-stand-policy-test go-cache-policy-test`.split(/\s+/);
const words = (value) => value.trim() ? value.trim().split(/\s+/).sort() : [];
function recipe(make, name) {
  const matches = [...make.matchAll(new RegExp(`^${name}:[^\\n]*\\n((?:\\t[^\\n]*\\n)*)`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} recipe`);
  return matches[0][1].trim();
}
function target(make, name) {
  const matches = [...make.matchAll(new RegExp(`^${name}:([^\\n]*)$`, 'gm'))];
  assert.equal(matches.length, 1, `unique ${name} target`);
  return words(matches[0][1]);
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
  assert.deepEqual(target(make, 'ci-core'), [...required, 'ci-partition-test'].sort());
  assert.deepEqual(target(make, 'test'), [], 'test must sequence, not parallelize prerequisites');
  assert.equal(recipe(make, 'test'), '$(MAKE) test-ordinary\n\t$(MAKE) test-connected', 'sequential complete local test');
  assert.deepEqual(target(make, 'test-ordinary'), words('prepare fmt-check lint'), 'ordinary preparation');
  assert.deepEqual(target(make, 'test-connected'), ['prepare'], 'connected preparation');
  assert.match(make, /^GO_PACKAGE_PATTERNS := \.\/cmd\/\.\.\. \.\/internal\/\.\.\. \.\/migrations\/\.\.\.$/m, 'complete Go corpus');
  assert.equal(recipe(make, 'test-ordinary'), "go test -race -timeout=15m -skip='^TestLocalSession' $(GO_PACKAGE_PATTERNS)", 'exact ordinary command');
  assert.equal(recipe(make, 'test-connected'), "go test -race -timeout=15m -run='^TestLocalSession' $(GO_PACKAGE_PATTERNS)", 'exact connected command');
  assert.deepEqual(target(make, 'attached-worker-security-gate'),
    words('attached-worker-crash-integration attached-worker-active-crash-integration'));
  assert.equal(recipe(make, 'ci-partition-test'), 'node --test scripts/test-ci-partition.mjs\n\tnode --test scripts/test-ci-ydb-readiness.mjs');
  const core = job(ci, 'verify');
  const connected = job(ci, 'go-connected');
  const security = job(ci, 'ydb-security-isolated');
  assert.equal([...ci.split('\njobs:\n')[1].matchAll(/^  [\w-]+:\n/gm)].length, 9, 'nine required jobs');
  assert.match(core, /^    timeout-minutes: 30$/m);
  assert.match(connected, /^    timeout-minutes: 30$/m, 'connected 30-minute bound');
  assert.match(connected, /^    runs-on: ubuntu-latest$/m);
  assert.doesNotMatch(connected, /^    (?:needs|permissions|env):/m, 'connected is parallel, inherits read-only policy');
  assert.match(ci, /^permissions:\n  contents: read\n/m, 'read-only workflow');
  assert.equal(step(connected, 'Checkout mirrored commit'), 'uses: actions/checkout@v6', 'exact connected checkout');
  assert.equal(step(connected, 'Set up pinned Go'), 'uses: actions/setup-go@v6\n        with:\n          go-version-file: go.mod\n          cache-dependency-path: go.sum', 'pinned connected Go');
  assert.equal(step(connected, 'Run connected-session tests over all packages'), 'run: make test-connected', 'complete connected invocation');
  assert.equal([...connected.matchAll(/^      - name:/gm)].length, 3, 'bounded connected steps');
  assert.match(security, /^    timeout-minutes: 25$/m);
  for (const name of ['ydb', 'ydb-security-isolated', 'local-stand', 'terraform']) {
    assert.match(job(ci, name), /^    needs: \[verify, go-connected\]$/m, `${name} joins both Go jobs`);
  }
  for (const [name, container] of [['ydb', 'sessionless-ydb'], ['ydb-security-isolated', 'sessionless-security-ydb']]) {
    const text = job(ci, name);
    assert.equal(step(text, 'Wait for YDB Local'), `run: bash scripts/wait-ci-ydb.sh ${container}`, `${name} SQL/storage readiness`);
    assert.ok(text.indexOf('Start pinned YDB Local') < text.indexOf('Wait for YDB Local'));
    assert.ok(text.indexOf('Wait for YDB Local') < text.indexOf('Apply schema twice'));
    assert.equal(step(text, 'Apply schema twice'), 'run: |\n          make migrate-local\n          make migrate-local', `${name} no migration retry`);
  }
  assert.equal(step(core, 'Run repository CI contract'), 'run: make ci-core');
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
    words('verify go-connected ydb ydb-security-isolated local-stand terraform cloudflare-edge web-browser'));
  for (const name of ['verify', 'go-connected', 'ydb', 'ydb-security-isolated', 'local-stand', 'terraform', 'images']) {
    const value = job(ci, name);
    assert.doesNotMatch(value, /^    (?:if|continue-on-error):/m, `${name} cannot bypass job failure`);
    assert.doesNotMatch(value, /^        continue-on-error:/m, `${name} cannot ignore step failure`);
  }
}

test('local, release and mirrored CI retain the complete partition', () => verify(original));
const mutations = [
  ['missing core check', 'make', 'ci-core: docs-check ', 'ci-core: '],
  ['missing local security', 'make', 'ci: ci-core test-connected attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core test-connected serverless-egress-conformance'],
  ['missing local egress', 'make', 'ci: ci-core test-connected attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core test-connected attached-worker-security-gate'],
  ['missing crash prerequisite', 'make', 'attached-worker-security-gate: attached-worker-crash-integration ', 'attached-worker-security-gate: '],
  ['missing regression gate', 'make', 'go-cache-policy-test ci-partition-test\n', 'go-cache-policy-test\n'],
  ['wrong core invocation', 'ci', 'run: make ci-core', 'run: make test'],
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
  ['lost ordinary race', 'make', "go test -race -timeout=15m -skip=", "go test -timeout=15m -skip=", /exact ordinary command/],
  ['changed ordinary selector', 'make', "-skip='^TestLocalSession'", "-skip='^TestLocal'", /exact ordinary command/],
  ['lost connected race', 'make', "go test -race -timeout=15m -run='^TestLocalSession'", "go test -timeout=15m -run='^TestLocalSession'", /exact connected command/],
  ['wider connected binary bound', 'make', "-timeout=15m -run='^TestLocalSession'", "-timeout=30m -run='^TestLocalSession'", /exact connected command/],
  ['incomplete connected corpus', 'make', "-run='^TestLocalSession' $(GO_PACKAGE_PATTERNS)", "-run='^TestLocalSession' ./internal/ydbstore", /exact connected command/],
  ['extra connected filter', 'make', "-run='^TestLocalSession' $(GO_PACKAGE_PATTERNS)", "-run='^TestLocalSession' -short $(GO_PACKAGE_PATTERNS)", /exact connected command/],
  ['missing connected job', 'ci', '  go-connected:\n', '  removed-connected:\n', /unique go-connected job/],
  ['serialized connected job', 'ci', '  go-connected:\n', '  go-connected:\n    needs: verify\n', /connected is parallel/],
  ['wrong connected target', 'ci', 'run: make test-connected\n', 'run: make test-ordinary\n', /complete connected invocation/],
  ['skipped connected step', 'ci', '      - name: Run connected-session tests over all packages\n', '      - name: Run connected-session tests over all packages\n        if: false\n', /complete connected invocation/],
  ['ignored connected command', 'ci', 'run: make test-connected\n', 'run: make test-connected || true\n', /complete connected invocation/],
  ['connected gains write permission', 'ci', '  go-connected:\n', '  go-connected:\n    permissions:\n      contents: write\n', /connected is parallel/],
  ['image loses connected', 'ci', '      - go-connected\n', ''],
  ['removed readiness policy', 'make', '\tnode --test scripts/test-ci-ydb-readiness.mjs\n', ''],
  ['liveness replaces SQL health', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', 'run: curl http://localhost:8765/monitoring/cluster', /ydb SQL\/storage readiness/],
  ['wrong readiness owner', 'ci', 'run: bash scripts/wait-ci-ydb.sh sessionless-security-ydb', 'run: bash scripts/wait-ci-ydb.sh sessionless-ydb', /ydb-security-isolated SQL\/storage readiness/],
];
for (const name of ['ydb', 'ydb-security-isolated', 'local-stand', 'terraform']) {
  mutations.push([`${name} loses connected join`, 'ci',
    `  ${name}:\n${job(original.ci, name)}`,
    `  ${name}:\n${job(original.ci, name).replace('needs: [verify, go-connected]', 'needs: verify')}`,
    new RegExp(`${name} joins both Go jobs`)]);
}
for (const [name, file, before, after, expected] of mutations) {
  test(`reject ${name}`, () => {
    assert.ok(original[file].includes(before), `mutation ${name} must change its input`);
    assert.throws(() => verify({ ...original, [file]: original[file].replace(before, after) }), expected);
  });
}
