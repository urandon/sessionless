import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

const repo = fileURLToPath(new URL('../', import.meta.url));
const destination = 'e2300000000000000000';
const foundation = readFileSync(join(repo, 'infra/terraform/modules/foundation/main.tf'), 'utf8');
const handoff = foundation.match(/removed\s*\{\s*from\s*=\s*yandex_logging_group\.application\s*lifecycle\s*\{\s*destroy\s*=\s*false\s*\}\s*\}/)?.[0];

test('native destination is explicit and only the old owned group is retained', () => {
  assert.ok(handoff, 'explicit non-destructive handoff is required');
  assert.doesNotMatch(foundation, /(?:resource|data)\s+"yandex_logging_group"/);
  assert.match(readFileSync(join(repo, 'infra/terraform/modules/foundation/outputs.tf'), 'utf8'), /output "log_group_id" \{ value = var\.runtime_log_group_id \}/);
  assert.doesNotMatch(readFileSync(join(repo, 'infra/terraform/cloud-dev/variables.tf'), 'utf8'), /variable "log_retention"/);
  assert.doesNotMatch(readFileSync(join(repo, 'scripts/cloud-terraform.sh'), 'utf8'), /-refresh=false|state rm.*logging/);
  const workflow = readFileSync(join(repo, '.github/workflows/ci.yml'), 'utf8');
  const terraformJob = workflow.match(/^  terraform:\n([\s\S]*?)(?=^  [a-z][a-z-]*:|$(?![\s\S]))/m)?.[1];
  assert.ok(terraformJob, 'Terraform CI job must remain present');
  assert.match(terraformJob, /uses: hashicorp\/setup-terraform@v4\n\s+with:\n\s+terraform_version: 1\.15\.5\n(?:\s*#.*\n)*\s+terraform_wrapper: false\n/,
    'isolated tests require the actual pinned CLI, not the Actions output wrapper');
});

function command(binary, args, cwd, env, timeout = 10000) {
  return new Promise((resolveCommand, reject) => {
    const child = spawn(binary, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '', bytes = 0;
    const timer = setTimeout(() => child.kill('SIGTERM'), timeout);
    const collect = (chunk) => {
      bytes += chunk.length;
      if (bytes > 512 * 1024) child.kill('SIGTERM');
      else output += chunk.toString();
    };
    child.stdout.on('data', collect);
    child.stderr.on('data', collect);
    child.on('error', (error) => { clearTimeout(timer); reject(error); });
    child.on('close', (status, signal) => {
      clearTimeout(timer);
      resolveCommand({ status, signal, output });
    });
  });
}

test('pinned real orphan planner retains state without refresh; destructive control refreshes', { timeout: 90000 }, async (t) => {
  const root = mkdtempSync(join(tmpdir(), 'sessionless-native-log-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  // Exercise the real common orphan-planner branch with its built-in local
  // provider. No Yandex adapter, credential, network or provider mock is needed.
  // Actual Yandex wiring is checked separately in the full mocked plan below.
  const header = 'terraform { required_version = "= 1.15.5" }\n';
  writeFileSync(join(root, 'main.tf'), `${header}resource "terraform_data" "application" { input = "synthetic-retained-data" }\n`, { mode: 0o600 });
  const env = {
    PATH: process.env.PATH, LANG: 'C', LC_ALL: 'C',
    TF_CLI_CONFIG_FILE: process.platform === 'win32' ? 'NUL' : '/dev/null',
    TF_IN_AUTOMATION: '1', CHECKPOINT_DISABLE: '1', AWS_EC2_METADATA_DISABLED: 'true',
  };
  const binary = process.env.TERRAFORM || 'terraform';
  const init = await command(binary, ['init', '-backend=false', '-input=false'], root, env);
  assert.equal(init.signal, null, init.output);
  assert.equal(init.status, 0, init.output);
  const seed = await command(binary, ['apply', '-input=false', '-auto-approve', '-no-color'], root, env);
  assert.equal(seed.signal, null, seed.output);
  assert.equal(seed.status, 0, seed.output);
  const oldState = readFileSync(join(root, 'terraform.tfstate'), 'utf8');
  const config = header + handoff.replace('yandex_logging_group.application', 'terraform_data.application');
  writeFileSync(join(root, 'main.tf'), config, { mode: 0o600 });
  const plan = await command(binary, ['plan', '-input=false', '-no-color', '-out=retained.tfplan'], root, env);
  assert.equal(plan.signal, null, plan.output);
  assert.equal(plan.status, 0, plan.output);
  assert.doesNotMatch(plan.output, /Refreshing state/);
  assert.equal(readFileSync(join(root, 'terraform.tfstate'), 'utf8'), oldState, 'plan must not change even the synthetic state');
  const show = await command(binary, ['show', '-json', 'retained.tfplan'], root, env);
  assert.equal(show.status, 0, show.output);
  const changes = JSON.parse(show.output).resource_changes;
  assert.equal(changes.length, 1);
  assert.equal(changes[0].address, 'terraform_data.application');
  assert.deepEqual(changes[0].change.actions, ['forget']);
  assert.equal(changes[0].change.before.input, 'synthetic-retained-data');
  // Causal control: the destructive branch refreshes and plans deletion. Neither
  // removal plan is applied; the original built-in fixture remains unchanged.
  writeFileSync(join(root, 'main.tf'), config.replace('destroy = false', 'destroy = true'), { mode: 0o600 });
  const control = await command(binary, ['plan', '-input=false', '-no-color', '-out=destructive.tfplan'], root, env);
  assert.equal(control.signal, null, control.output);
  assert.equal(control.status, 0, control.output);
  assert.match(control.output, /Refreshing state/);
  const destructive = await command(binary, ['show', '-json', 'destructive.tfplan'], root, env);
  assert.equal(destructive.status, 0, destructive.output);
  assert.deepEqual(JSON.parse(destructive.output).resource_changes[0].change.actions, ['delete']);
  assert.equal(readFileSync(join(root, 'terraform.tfstate'), 'utf8'), oldState);
});

test('actual mocked full plan preserves INFO destination for all six containers and both gateways', () => {
  const path = process.env.NATIVE_LOG_TEST_PLAN_JSON;
  assert.ok(path, 'run through make terraform-ci or supply the actual Terraform test JSONL');
  const records = readFileSync(path, 'utf8').trim().split('\n').map((line) => JSON.parse(line));
  const plans = records.filter((r) => r.type === 'test_plan' && r['@testrun'] === 'web_deployment_plan');
  assert.equal(plans.length, 1);
  const resources = plans[0].test_plan.resource_changes;
  const expected = [
    'module.edge.yandex_api_gateway.api',
    'module.runtime.yandex_serverless_container.control["blue"]',
    'module.runtime.yandex_serverless_container.control["green"]',
    'module.runtime.yandex_serverless_container.reconciler',
    'module.runtime.yandex_serverless_container.telegram_sender',
    'module.runtime.yandex_serverless_container.worker',
    'module.web.yandex_api_gateway.web', 'module.web.yandex_serverless_container.web',
  ];
  for (const address of expected) {
    const rows = resources.filter((r) => r.address === address);
    assert.equal(rows.length, 1, address);
    const logs = rows[0].change.after.log_options;
    assert.equal(logs.length, 1, address);
    assert.equal(logs[0].log_group_id, destination, address);
    assert.equal(logs[0].min_level, 'INFO', address);
    assert.notEqual(logs[0].disabled, true, address);
    assert.equal(logs[0].folder_id, null, 'no silent default-folder route');
  }
  assert.ok(!resources.some((r) => r.type === 'yandex_logging_group'), 'no legacy managed resource or lookup');
});
