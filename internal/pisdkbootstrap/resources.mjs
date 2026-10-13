// Private construction helpers for Pi 6c4f3602. No SDK imports or entrypoint.
// The original prepared supervisor must qualify the real SDK import graph,
// process environment, profile, allocations and physical send BEFORE use.
// These helpers supply no credential, model, lifecycle or execution authority.
import { isAbsolute } from 'node:path';

const empty = () => Object.freeze([]);
const deny = () => { throw new Error('pi_bootstrap_operation_denied'); };
const invalid = () => { throw new Error('pi_bootstrap_input_invalid'); };

function checkSignal(signal) {
  if (!(signal instanceof AbortSignal) || signal.aborted) invalid();
}

// CredentialStore, not AuthStorage: key bytes are literal, including ! and $.
// Missing providers retain the upstream store contract (undefined); the real
// driver must separately seal the selected model and deny ambient auth fallback.
export function createLiteralCredentialStore(providerId, key, signal) {
  checkSignal(signal);
  if (typeof providerId !== 'string' || !providerId || typeof key !== 'string' || !key) invalid();
  let retainedKey = key;
  let disposed = false;
  const check = (options) => {
    checkSignal(signal);
    if (disposed || options?.signal?.aborted) deny();
  };
  const store = Object.freeze({
    async read(id, options) {
      check(options);
      return id === providerId ? Object.freeze({ type: 'api_key', key: retainedKey }) : undefined;
    },
    async list(options) {
      check(options);
      return Object.freeze([Object.freeze({ providerId, type: 'api_key' })]);
    },
    async modify(_id, _fn, options) { check(options); deny(); },
    async delete(_id, options) { check(options); deny(); },
  });
  return Object.freeze({
    store,
    dispose() {
      disposed = true;
      retainedKey = undefined;
      // Removes this reference; JavaScript strings cannot promise zeroization.
    },
  });
}

// timeoutMs is a bound supplied by the original prepared authority, NOT a
// fresh model timeout. Its true monotonic deadline remains enforced externally.
export function createFixedSettings(timeoutMs) {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2_147_483_647) invalid();
  return Object.freeze({
    compaction: Object.freeze({ enabled: false }),
    retry: Object.freeze({
      enabled: false,
      maxRetries: 0,
      provider: Object.freeze({ maxRetries: 0, maxRetryDelayMs: 0, timeoutMs }),
    }),
    images: Object.freeze({ blockImages: false, autoResize: false }),
    transport: 'sse',
    defaultTools: empty(),
    packages: empty(), extensions: empty(), skills: empty(), prompts: empty(), themes: empty(),
    enableSkillCommands: false,
    enableAnalytics: false,
    enableInstallTelemetry: false,
  });
}

// The factory MUST be the qualified SDK's genuine createExtensionRuntime.
// Pi bindCore mutates that runtime; freezing it would break actual construction.
// No extension factory, package discovery, resource path or reload is accepted.
export function createFixedResourceLoader(systemPrompt, createExtensionRuntime) {
  if (typeof systemPrompt !== 'string' || !systemPrompt || typeof createExtensionRuntime !== 'function') invalid();
  const runtime = createExtensionRuntime();
  if (!runtime || typeof runtime !== 'object') invalid();
  return Object.freeze({
    getExtensions: () => Object.freeze({ extensions: empty(), errors: empty(), runtime }),
    getSkills: () => Object.freeze({ skills: empty(), diagnostics: empty() }),
    getPrompts: () => Object.freeze({ prompts: empty(), diagnostics: empty() }),
    getThemes: () => Object.freeze({ themes: empty(), diagnostics: empty() }),
    getAgentsFiles: () => Object.freeze({ agentsFiles: empty() }),
    getSystemPrompt: () => systemPrompt,
    getSystemPromptSource: () => undefined,
    getAppendSystemPrompt: empty,
    getAppendSystemPromptSources: empty,
    extendResources: deny,
    async reload() { deny(); },
  });
}

// Construct options for genuine createAgentSession, without calling it or
// exposing prompt/queue/command APIs. sdk is a private namespace assembled from
// qualified genuine SDK exports, never an extension or caller-selected module.
// A real call still needs PI_OFFLINE=1 in a closed child environment, native
// import confinement, stream guards and ALL original #203 eligibility gates.
export async function createFixedBootstrapResources(sdk, input) {
  const { cwd, agentDir, systemPrompt, model, key, timeoutMs, signal } = input ?? {};
  checkSignal(signal);
  if (typeof cwd !== 'string' || !isAbsolute(cwd) || typeof agentDir !== 'string' || !isAbsolute(agentDir)) invalid();
  if (!model || typeof model !== 'object' || !Object.isFrozen(model) ||
      ![Object.prototype, null].includes(Object.getPrototypeOf(model)) ||
      'samplingParams' in model || 'headers' in model ||
      Object.values(Object.getOwnPropertyDescriptors(model)).some(d => !Object.hasOwn(d, 'value'))) invalid();
  if (!['api', 'provider', 'id', 'input'].every(name => Object.hasOwn(model, name)) ||
      model.api !== 'openai-responses' ||
      typeof model.provider !== 'string' || !model.provider || typeof model.id !== 'string' || !model.id ||
      !Array.isArray(model.input) || !Object.isFrozen(model.input) ||
      Object.getPrototypeOf(model.input) !== Array.prototype ||
      model.input.length < 1 || model.input.length > 2 ||
      Reflect.ownKeys(model.input).length !== model.input.length + 1) invalid();
  let vision = false;
  for (let index = 0; index < model.input.length; index++) {
    const descriptor = Object.getOwnPropertyDescriptor(model.input, String(index));
    if (!descriptor || !Object.hasOwn(descriptor, 'value') ||
        !['text', 'image'].includes(descriptor.value)) invalid();
    if (descriptor.value === 'image') vision = true;
  }
  if (!vision) invalid();
  const settings = createFixedSettings(timeoutMs);
  if (typeof sdk?.SettingsManager?.inMemory !== 'function' || typeof sdk?.SessionManager?.inMemory !== 'function' ||
      typeof sdk?.InMemoryModelsStore !== 'function' || typeof sdk?.ModelRuntime?.create !== 'function' ||
      typeof sdk?.createExtensionRuntime !== 'function') invalid();
  const credential = createLiteralCredentialStore(model.provider, key, signal);
  try {
    const resourceLoader = createFixedResourceLoader(systemPrompt, sdk.createExtensionRuntime);
    const settingsManager = sdk.SettingsManager.inMemory(settings);
    const sessionManager = sdk.SessionManager.inMemory(cwd);
    const modelsStore = new sdk.InMemoryModelsStore();
    const modelRuntime = await sdk.ModelRuntime.create({
      credentials: credential.store, modelsPath: null, modelsStore,
      allowModelNetwork: false, refreshOnCreate: false, signal,
    });
    checkSignal(signal);
    if (!settingsManager || !sessionManager || !modelRuntime) invalid();
    return Object.freeze({
      sessionOptions: Object.freeze({
        cwd, agentDir, modelRuntime, settingsManager, sessionManager, resourceLoader,
        model, scopedModels: Object.freeze([Object.freeze({ model, thinkingLevel: 'off' })]),
        thinkingLevel: 'off', tools: empty(), noTools: 'all', customTools: empty(),
      }),
      dispose: credential.dispose,
    });
  } catch {
    credential.dispose();
    // Do not leak raw SDK/auth diagnostics or credential-bearing exceptions.
    throw new Error('pi_bootstrap_construction_failed');
  }
}
