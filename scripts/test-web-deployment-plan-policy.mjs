import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const repoRoot = fileURLToPath(new URL('../', import.meta.url));
const policy = join(repoRoot, 'scripts/test-web-deployment-policy.sh');
// Independent exact fixtures: do not derive the expected permission set from
// the implementation or an accidentally removed guard could weaken the test.
const required = [
  'module.web.yandex_serverless_container.web',
  'module.foundation.yandex_container_registry_iam_binding.runtime_puller',
  'module.foundation.yandex_ydb_database_iam_binding.runtime_editor',
  'module.foundation.yandex_storage_bucket_iam_binding.runtime_editor',
  'module.foundation.yandex_lockbox_secret_iam_member.web_bff',
  'module.foundation.yandex_lockbox_secret_iam_member.scheduler_ymq["web-bff"]',
  'module.foundation.yandex_kms_symmetric_key_iam_member.runtime_secret_decrypter["web-bff"]',
  'module.foundation.yandex_resourcemanager_folder_iam_member.runtime["web-bff:logging.writer"]',
  'module.foundation.yandex_resourcemanager_folder_iam_member.runtime["web-gateway:logging.writer"]',
];
// Terraform emits starting/running/teardown events as well as one completed
// status. Only terminal records participate in run cardinality:
// https://developer.hashicorp.com/terraform/internals/machine-readable-ui#test-run
const bootstrap = (status = 'pass', run = 'foundation_bootstrap_without_web_payload') => ({
  type: 'test_run', test_run: { run, status, progress: 'complete' },
});
const plan = (addresses = required, run = 'isolated_web_rollout') => ({
  type: 'test_plan', '@testrun': run,
  test_plan: { resource_changes: addresses.map((address) => ({ address, change: { actions: ['create'] } })) },
});

function check(t, records, success) {
  const root = mkdtempSync(join(tmpdir(), 'sessionless-web-plan-policy-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const fixture = join(root, 'terraform-test.jsonl');
  writeFileSync(fixture, `${records.map((record) => JSON.stringify(record)).join('\n')}\n`, { mode: 0o600 });
  // No Terraform, provider, cloud or credential environment is passed through.
  // Only the existing script and jq inspect this finite synthetic JSONL file.
  const result = spawnSync('/bin/sh', [policy, '--plan-json', fixture], {
    cwd: repoRoot,
    env: { PATH: '/usr/bin:/bin', LANG: 'C', LC_ALL: 'C', TMPDIR: root },
    encoding: 'utf8', timeout: 5000, maxBuffer: 32 * 1024,
  });
  assert.ifError(result.error);
  assert.equal(result.signal, null, `policy process signalled: ${result.signal}; ${result.stderr}`);
  assert.equal(result.status, success ? 0 : 1,
    `policy exit=${result.status}; expected ${success ? 'accept' : 'deny'}; stdout=${result.stdout}; stderr=${result.stderr}`);
  if (success) {
    assert.match(result.stdout, /Web deployment policy invariants passed/);
  } else {
    assert.match(result.stderr, /targeted Web plan must retain every runtime permission and exclude publication\/GC and unrelated runtime graphs/);
    assert.doesNotMatch(result.stdout, /Web deployment policy invariants passed/);
  }
}

test('complete target expansion passes', (t) => {
  // Also the minimal cross-version binding regression. jq 1.8 binds an
  // ungrouped boolean prefix into `as $addresses`; jq 1.7 accepts the original.
  // https://github.com/jqlang/jq/blob/jq-1.8.1/NEWS.md#language-changes
  check(t, [bootstrap(), plan()], true);
});

test('starting progress and unrelated log events do not duplicate the completed bootstrap', (t) => {
  check(t, [
    { type: 'version', terraform: '1.14.7' },
    { type: 'test_run', test_run: { run: 'foundation_bootstrap_without_web_payload', progress: 'starting' } },
    { type: 'test_run', test_run: { run: 'foundation_bootstrap_without_web_payload', progress: 'running', elapsed: 1 } },
    bootstrap(),
    { type: 'test_run', test_run: { run: 'foundation_bootstrap_without_web_payload', progress: 'teardown' } },
    bootstrap('pass', 'unrelated_test'), plan(),
  ], true);
});

test('additional unrelated safe address remains allowed', (t) => {
  check(t, [bootstrap(), plan([...required, 'module.web.yandex_api_gateway.web'])], true);
});

const malformedRuns = [
  ['missing bootstrap', [plan()]],
  ['duplicate completed passing bootstrap', [bootstrap(), bootstrap(), plan()]],
  ['completed failed bootstrap', [bootstrap('fail'), plan()]],
  ['completed errored bootstrap', [bootstrap('error'), plan()]],
  ['completed skipped bootstrap', [bootstrap('skip'), plan()]],
  ['duplicate completed pass and fail bootstrap', [bootstrap(), bootstrap('fail'), plan()]],
  ['wrong bootstrap run', [bootstrap('pass', 'other_bootstrap'), plan()]],
  ['missing isolated plan', [bootstrap()]],
  ['duplicate isolated plans', [bootstrap(), plan(), plan()]],
  ['wrong plan run', [bootstrap(), plan(required, 'other_rollout')]],
];
for (const [name, records] of malformedRuns) {
  test(name, (t) => check(t, records, false));
}

for (const address of required) {
  test(`missing required permission/address: ${address}`, (t) => {
    check(t, [bootstrap(), plan(required.filter((candidate) => candidate !== address))], false);
  });
}

const forbidden = [
  ['runtime module', 'module.runtime.yandex_serverless_container.worker'],
  ['edge module', 'module.edge.yandex_api_gateway.edge'],
  ['OIDC federation', 'module.foundation.yandex_iam_workload_identity_oidc_federation.github'],
  ['federated credential', 'module.foundation.yandex_iam_workload_identity_federated_credential.github'],
  ['repository IAM', 'module.foundation.yandex_container_repository_iam_binding.publisher'],
  ['repository publication/GC graph', 'module.foundation.yandex_container_repository.runtime["web-bff"]'],
  ['queue provisioner key', 'module.foundation.yandex_iam_service_account_static_access_key.queue_provisioner'],
];
for (const [name, address] of forbidden) {
  test(`forbidden address class: ${name}`, (t) => check(t, [bootstrap(), plan([...required, address])], false));
}
