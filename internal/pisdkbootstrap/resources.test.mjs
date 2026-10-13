// UNIT CONTRACT ONLY: these spies are not Pi constructors or SDK conformance.
import test from 'node:test';
import assert from 'node:assert/strict';
import { createLiteralCredentialStore, createFixedSettings, createFixedResourceLoader, createFixedBootstrapResources } from './resources.mjs';

function fixture() {
  const calls = [];
  const runtime = {};
  const sdk = {
    createExtensionRuntime() { calls.push(['extensions']); return runtime; },
    SettingsManager: { inMemory(settings) { calls.push(['settings', settings]); return { settings }; } },
    SessionManager: { inMemory(cwd) { calls.push(['session', cwd]); return { cwd }; } },
    InMemoryModelsStore: class { constructor() { calls.push(['models', this]); } },
    ModelRuntime: { async create(options) { calls.push(['runtime', options]); return { options }; } },
  };
  const controller = new AbortController();
  const model = Object.freeze({ id: 'fixture', provider: 'openai', api: 'openai-responses', input: Object.freeze(['text', 'image']) });
  const input = { cwd: '/fixture/work', agentDir: '/fixture/agent', systemPrompt: 'Fixed fixture prompt', model, key: 'fixture-not-a-secret', timeoutMs: 1234, signal: controller.signal };
  return { calls, runtime, sdk, controller, input };
}

test('literal readonly credential contract and secret-free metadata', async () => {
  for (const key of ['!echo fixture', '$FIXTURE_KEY', 'ordinary-fixture']) {
    const { store, dispose } = createLiteralCredentialStore('openai', key, new AbortController().signal);
    try {
      assert.deepEqual(await store.read('openai'), { type: 'api_key', key });
      assert.equal(await store.read('foreign'), undefined);
      assert.deepEqual(await store.list(), [{ providerId: 'openai', type: 'api_key' }]);
      assert.equal(JSON.stringify(store).includes(key), false);
      let invoked = false;
      await assert.rejects(store.modify('openai', async () => { invoked = true; }), /operation_denied/);
      await assert.rejects(store.delete('openai'), /operation_denied/);
      assert.equal(invoked, false);
      assert.equal((await store.read('openai')).key, key);
      assert.throws(() => { store.read = () => undefined; }, TypeError);
    } finally { dispose(); }
    dispose();
    await assert.rejects(store.read('openai'), /operation_denied/);
    await assert.rejects(store.list(), /operation_denied/);
  }
});

test('original and per-operation cancellation deny every credential operation', async () => {
  for (const original of [true, false]) {
    const c = new AbortController(); const operation = new AbortController();
    const { store, dispose } = createLiteralCredentialStore('openai', 'fixture', c.signal);
    (original ? c : operation).abort();
    const options = { signal: operation.signal };
    try {
      await assert.rejects(store.read('openai', options)); await assert.rejects(store.list(options));
      await assert.rejects(store.modify('openai', () => assert.fail('mutation invoked'), options));
      await assert.rejects(store.delete('openai', options));
    } finally { dispose(); }
  }
});

test('fixed settings: exact nested retry, timeout, image and discovery choices', () => {
  const s = createFixedSettings(1234);
  assert.deepEqual(s.retry, { enabled: false, maxRetries: 0, provider: { maxRetries: 0, maxRetryDelayMs: 0, timeoutMs: 1234 } });
  assert.equal(s.provider, undefined);
  assert.deepEqual(s.images, { blockImages: false, autoResize: false });
  assert.deepEqual(s.compaction, { enabled: false }); assert.equal(s.transport, 'sse');
  for (const name of ['defaultTools', 'packages', 'extensions', 'skills', 'prompts', 'themes']) {
    assert.deepEqual(s[name], []); assert.throws(() => s[name].push('ambient'), TypeError);
  }
  for (const name of ['enableAnalytics', 'enableInstallTelemetry', 'enableSkillCommands']) assert.equal(s[name], false);
  assert.throws(() => { s.retry.provider.maxRetries = 1; }, TypeError);
  for (const v of [undefined, null, 0, -1, 1.1, NaN, Infinity, 2_147_483_648, '1234']) assert.throws(() => createFixedSettings(v), /input_invalid/);
  assert.equal(createFixedSettings(2_147_483_647).retry.provider.timeoutMs, 2_147_483_647);
});

test('resources remain empty; discovery denies; genuine runtime stays bindable', async () => {
  const runtime = {}; let created = 0;
  const r = createFixedResourceLoader('literal /not/a/prompt/file', () => { created++; return runtime; });
  assert.equal(created, 1); assert.equal(r.getExtensions().runtime, runtime);
  runtime.sendMessage = () => {}; assert.equal(r.getExtensions().runtime.sendMessage, runtime.sendMessage);
  assert.deepEqual(r.getExtensions().extensions, []); assert.deepEqual(r.getExtensions().errors, []);
  assert.deepEqual(r.getSkills(), { skills: [], diagnostics: [] });
  assert.deepEqual(r.getPrompts(), { prompts: [], diagnostics: [] });
  assert.deepEqual(r.getThemes(), { themes: [], diagnostics: [] });
  assert.deepEqual(r.getAgentsFiles(), { agentsFiles: [] });
  assert.equal(r.getSystemPrompt(), 'literal /not/a/prompt/file'); assert.equal(r.getSystemPromptSource(), undefined);
  assert.deepEqual(r.getAppendSystemPrompt(), []); assert.deepEqual(r.getAppendSystemPromptSources(), []);
  assert.throws(() => r.getExtensions().extensions.push({}), TypeError);
  assert.throws(() => r.extendResources({ skillPaths: [{ path: '/ambient' }] }), /operation_denied/);
  await assert.rejects(r.reload(), /operation_denied/);
});

test('constructor spies: fixed memory services, model and noTools all', async () => {
  const f = fixture(); const result = await createFixedBootstrapResources(f.sdk, f.input);
  try {
    assert.deepEqual(f.calls.map(([name]) => name), ['extensions', 'settings', 'session', 'models', 'runtime']);
    const o = f.calls.find(([name]) => name === 'runtime')[1];
    assert.deepEqual(Object.keys(o).sort(), ['allowModelNetwork', 'credentials', 'modelsPath', 'modelsStore', 'refreshOnCreate', 'signal']);
    assert.equal(o.modelsPath, null); assert.equal(o.allowModelNetwork, false); assert.equal(o.refreshOnCreate, false);
    assert.equal(o.signal, f.input.signal); assert.ok(o.modelsStore instanceof f.sdk.InMemoryModelsStore);
    assert.equal((await o.credentials.read('openai')).key, f.input.key);
    const s = result.sessionOptions;
    assert.equal(s.cwd, f.input.cwd); assert.equal(s.agentDir, f.input.agentDir); assert.equal(s.model, f.input.model);
    assert.deepEqual(s.scopedModels, [{ model: f.input.model, thinkingLevel: 'off' }]);
    assert.equal(s.thinkingLevel, 'off'); assert.equal(s.noTools, 'all');
    assert.deepEqual(s.tools, []); assert.deepEqual(s.customTools, []);
    assert.equal(s.prompt, undefined); assert.equal(result.session, undefined);
    assert.throws(() => { s.noTools = undefined; }, TypeError);
  } finally { result.dispose(); }
  await assert.rejects(f.calls.find(([name]) => name === 'runtime')[1].credentials.read('openai'), /operation_denied/);
});

test('constructor spies: invalid selected inputs deny before factory effects', async () => {
  const changes = [{ cwd: '.' }, { agentDir: undefined }, { signal: undefined }, { model: undefined },
    { model: { api: 'openai-responses', id: 'fixture', provider: 'openai', input: ['image'] } },
    { model: Object.freeze({ api: 'other', id: 'fixture', provider: 'openai', input: Object.freeze(['image']) }) },
    { model: Object.freeze({ api: 'openai-responses', id: 'fixture', provider: 'openai', input: Object.freeze(['text']) }) },
    { model: Object.freeze({ api: 'openai-responses', id: 'fixture', provider: 'openai', input: Object.freeze(['image']), samplingParams: {} }) },
    { key: '' }, { timeoutMs: 0 }];
  for (const change of changes) {
    const f = fixture(); await assert.rejects(createFixedBootstrapResources(f.sdk, { ...f.input, ...change }), /input_invalid/);
    assert.deepEqual(f.calls, []);
  }
});

test('constructor spies: failure retires key and suppresses raw SDK diagnostics', async () => {
  for (const cancellation of [true, false]) {
    const f = fixture(); let retained;
    f.sdk.ModelRuntime.create = async o => { retained = o.credentials;
      if (cancellation) { f.controller.abort(); return {}; }
      throw new Error('RAW-fixture-key-or-runtime-diagnostic'); };
    await assert.rejects(createFixedBootstrapResources(f.sdk, f.input), { message: 'pi_bootstrap_construction_failed' });
    await assert.rejects(retained.read('openai')); await assert.rejects(retained.list());
  }
});

test('inherited overrides and model accessors deny without factory or getter effects', async () => {
  for (const name of ['headers', 'samplingParams']) {
    const f = fixture();
    const model = Object.freeze(Object.assign(Object.create({ [name]: {} }), f.input.model));
    await assert.rejects(createFixedBootstrapResources(f.sdk, { ...f.input, model }), /input_invalid/);
    assert.deepEqual(f.calls, []);
  }
  const f = fixture();
  let read = false;
  const model = { ...f.input.model };
  Object.defineProperty(model, 'api', { get() { read = true; return 'openai-responses'; } });
  Object.freeze(model);
  await assert.rejects(createFixedBootstrapResources(f.sdk, { ...f.input, model }), /input_invalid/);
  assert.equal(read, false);
  assert.deepEqual(f.calls, []);
  const nullPrototype = Object.freeze(Object.assign(Object.create(null), f.input.model));
  const restored = await createFixedBootstrapResources(f.sdk, { ...f.input, model: nullPrototype });
  restored.dispose();
});

test('model input cannot inject a method or array-prototype callback into vision checking', async () => {
  for (const ownMethod of [true, false]) {
    const f = fixture(); let invoked = false;
    const input = ['text'];
    if (ownMethod) input.includes = () => { invoked = true; return true; };
    else Object.setPrototypeOf(input, { get includes() { invoked = true; return () => true; } });
    Object.freeze(input);
    const model = Object.freeze({ ...f.input.model, input });
    await assert.rejects(createFixedBootstrapResources(f.sdk, { ...f.input, model }), /input_invalid/);
    assert.equal(invoked, false);
    assert.deepEqual(f.calls, []);
  }
});
