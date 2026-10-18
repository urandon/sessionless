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
const required = `docs-check cloud-web-smoke-test web-ci generate test build integration
  image-publication-test image-build-inputs-test
  image-publish-policy-test registry-gc-policy-test release-policy-test
  local-stand-policy-test dockerless-stand-policy-test go-cache-policy-test`.split(/\s+/);
const words = (value) => value.trim().split(/\s+/).sort();
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
  assert.deepEqual(target(make, 'ci'), words('ci-core attached-worker-security-gate serverless-egress-conformance'));
  assert.deepEqual(target(make, 'ci-core'), [...required, 'ci-partition-test'].sort());
  assert.deepEqual(target(make, 'attached-worker-security-gate'),
    words('attached-worker-crash-integration attached-worker-active-crash-integration'));
  assert.match(make, /^ci-partition-test:\n\tnode --test scripts\/test-ci-partition\.mjs$/m);
  const core = job(ci, 'verify');
  const security = job(ci, 'ydb-security-isolated');
  assert.match(core, /^    timeout-minutes: 30$/m);
  assert.match(security, /^    timeout-minutes: 25$/m);
  assert.match(security, /^    needs: verify$/m);
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
    words('verify ydb ydb-security-isolated local-stand terraform cloudflare-edge web-browser'));
  for (const [name, value] of [['verify', core], ['security', security], ['images', images]]) {
    assert.doesNotMatch(value, /^    (?:if|continue-on-error):/m, `${name} cannot bypass job failure`);
    assert.doesNotMatch(value, /^        continue-on-error:/m, `${name} cannot ignore step failure`);
  }
}

test('local, release and mirrored CI retain the complete partition', () => verify(original));
const mutations = [
  ['missing core check', 'make', 'ci-core: docs-check ', 'ci-core: '],
  ['missing local security', 'make', 'ci: ci-core attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core serverless-egress-conformance'],
  ['missing local egress', 'make', 'ci: ci-core attached-worker-security-gate serverless-egress-conformance', 'ci: ci-core attached-worker-security-gate'],
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
];
for (const [name, file, before, after] of mutations) {
  test(`reject ${name}`, () => {
    assert.ok(original[file].includes(before), `mutation ${name} must change its input`);
    assert.throws(() => verify({ ...original, [file]: original[file].replace(before, after) }));
  });
}
