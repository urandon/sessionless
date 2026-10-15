// Artifact layout: runner/{bootstrap,boundary}.mjs beside the exact built pi/
// tree. Imports intentionally select the genuine pinned Pi SDK, not an injected
// imitation. The Go prepared owner must verify the full artifact and profile
// before supervising this private entrypoint; this module grants no eligibility.
import { Buffer } from 'node:buffer';
import { createFixedBootstrapResources } from '../../pisdkbootstrap/resources.mjs';
import { createPhysicalFetchOwner } from '../../pisdktransport/request.mjs';
import { createFixedRequestContract } from './request_contract.mjs';

const denied = () => new Error('prepared Pi invocation denied');
let invocationClaimed = false; // One private invocation per supervised child.

function equalJSON(left, right) {
  if (left === right) return true;
  if (!left || !right || typeof left !== 'object' || typeof right !== 'object') return false;
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  const a = Object.keys(left).filter(key => left[key] !== undefined);
  const b = Object.keys(right).filter(key => right[key] !== undefined);
  return a.length === b.length && a.every(key => Object.hasOwn(right, key) && equalJSON(left[key], right[key]));
}

// profile/messages are private, complete, source-associated preallocated output
// from canonical Go validation, not arbitrary public SDK options. The exact
// prospective allocation/context certificate is a prerequisite at the Go
// pre-Issue boundary; onPayload cannot retroactively certify those allocations.
export async function runPreparedPi({ profile, messages, cwd, agentDir, apiKey, signal, parentUse, physicalFetch }) {
  // Consume before any await/preflight: failure and concurrent/repeated calls
  // cannot create a second in-memory session or renew this process's permit.
  if (invocationClaimed) throw denied();
  invocationClaimed = true;
  let session, resources, boundary;
  let stage = 'configuration'; // fixed trusted diagnostic, never raw SDK data
  try {
    if (!(signal instanceof AbortSignal) || signal.aborted || typeof physicalFetch !== 'function' ||
        typeof cwd !== 'string' || !cwd.startsWith('/') || typeof apiKey !== 'string' || apiKey.length === 0 ||
        typeof agentDir !== 'string' || !agentDir.startsWith('/') || !Object.isFrozen(profile) ||
        typeof profile.systemPrompt !== 'string' || profile.systemPrompt.length === 0 ||
        !Number.isSafeInteger(profile.maxOutputTokens) || profile.maxOutputTokens < 16 ||
        !Number.isSafeInteger(profile.timeoutMs) || profile.timeoutMs < 1 ||
        !Object.isFrozen(profile.limits) || !Object.isFrozen(profile.admittedRequest) ||
        typeof profile.admittedRequest.body !== 'string' || profile.admittedRequest.body.includes(apiKey) || profile.systemPrompt.includes(apiKey) ||
        !Array.isArray(messages) || messages.length === 0 || !Object.isFrozen(messages)) throw denied();
    const model = profile.model;
    if (!model || !Object.isFrozen(model) || model.api !== 'openai-responses' ||
        typeof model.provider !== 'string' || !model.provider || typeof model.id !== 'string' || !model.id ||
        typeof model.baseUrl !== 'string' || !model.baseUrl.startsWith('https://') ||
        !Array.isArray(model.input) || !model.input.includes('image') ||
        model.headers !== undefined || model.samplingParams !== undefined) throw denied();
    const expected = profile.admittedRequest.request;
    if (expected.model !== model.id || expected.max_output_tokens !== profile.maxOutputTokens ||
        profile.admittedRequest.endpoint !== `${model.baseUrl.replace(/\/$/, '')}/responses`) throw denied();
    // These snapshots/copy reservations originate before Issue. This bootstrap
    // does not parse/stringify a second expected body or invent a full-P pool.
    const fixedPayload = createFixedRequestContract({ limits: profile.requestContractLimits, expectedRequest: expected });
    stage = 'physical-boundary';
    boundary = createPhysicalFetchOwner({ limits: profile.limits, admittedRequest: profile.admittedRequest,
      originalSignal: signal, parentUse, physicalFetch });
    // Fixed genuine SDK graph, never a caller-selected module or factory. The
    // private SPU2 entry admits complete Init and reads its one owned file
    // before calling this function. Merely importing bootstrap imports no SDK.
    stage = 'sdk-import';
    signal.throwIfAborted();
    const [{ AgentSession, convertToLlm, createExtensionRuntime, ModelRuntime, SessionManager, SettingsManager },
      { Agent }, { InMemoryModelsStore }] = await Promise.all([
      import('../pi/packages/coding-agent/dist/index.js'),
      import('../pi/packages/agent/dist/index.js'),
      import('../pi/packages/ai/dist/index.js'),
    ]);
    signal.throwIfAborted();
    stage = 'resources';
    resources = await createFixedBootstrapResources({ AgentSession, convertToLlm, createExtensionRuntime, ModelRuntime,
      SessionManager, SettingsManager, InMemoryModelsStore }, {
      cwd, agentDir, systemPrompt: profile.systemPrompt, model, key: apiKey, timeoutMs: profile.timeoutMs, signal,
    });
    const { modelRuntime, settingsManager, sessionManager, resourceLoader } = resources.sessionOptions;
    stage = 'session';
    // The public factory's noTools disables active tools but still constructs
    // every builtin definition. The genuine constructor's documented override
    // avoids creating that registry at all; no private SDK state is mutated.
    const agent = new Agent({ initialState: { systemPrompt: profile.systemPrompt,
      model, thinkingLevel: 'off', tools: [], messages: [] }, convertToLlm,
      streamFn: () => { throw denied(); } });
    session = new AgentSession({ agent, cwd, modelRuntime, scopedModels: [],
      baseToolsOverride: {}, initialActiveToolNames: [], allowedToolNames: [], customTools: [],
      resourceLoader, sessionManager, settingsManager });
    if (session.getAllTools().length !== 0 || session.getActiveToolNames().length !== 0 ||
        session.agent.state.messages.length !== 0 || session.agent.state.tools.length !== 0) throw denied();
    // AgentSession's custom-prompt builder appends cwd. The first turn reads
    // public Agent state; the genuine next-turn hook later reads its own base
    // prompt. Retain that hook and snapshot, replacing only admitted prompt
    // bytes through the supported public seam, not private _baseSystemPrompt.
    session.agent.state.systemPrompt = profile.systemPrompt;
    const refreshTurn = session.agent.prepareNextTurnWithContext;
    if (typeof refreshTurn !== 'function') throw denied();
    session.agent.prepareNextTurnWithContext = async (turn, linkedSignal) => {
      const snapshot = await refreshTurn(turn, linkedSignal);
      if (!snapshot?.context || !equalJSON(snapshot.model, model) || snapshot.thinkingLevel !== 'off' ||
          (snapshot.context.tools?.length ?? 0) !== 0 || session.getAllTools().length !== 0 ||
          snapshot.context.messages !== turn.context.messages) throw denied();
      return { ...snapshot, context: { ...snapshot.context, systemPrompt: profile.systemPrompt } };
    };
    let streams = 0, payloads = 0;
    // Replace only the public trusted stream seam, retaining the real Agent
    // loop, ModelRuntime auth resolution, standard provider, SDK and Pi parser.
    session.agent.streamFunction = (selected, context, options) => {
      if (++streams !== 1 || signal.aborted || !equalJSON(selected, model) ||
          context.systemPrompt !== profile.systemPrompt || (context.tools?.length ?? 0) !== 0 ||
          !equalJSON(context.messages, messages)) throw denied();
      const linked = options?.signal ? AbortSignal.any([signal, options.signal]) : signal;
      return modelRuntime.stream(model, context, {
        apiKey, env: {}, signal: linked, transport: 'sse', cacheRetention: profile.cacheRetention,
        maxTokens: profile.maxOutputTokens, timeoutMs: profile.timeoutMs, maxRetries: 0,
        toolChoice: 'none',
        temperature: profile.temperature, reasoningEffort: profile.reasoningEffort, reasoningSummary: profile.reasoningSummary,
        onPayload: payload => {
          if (++payloads !== 1 || linked.aborted) throw denied();
          return fixedPayload(payload);
        },
        fetch: boundary.fetch,
      });
    };
    const abort = () => session.agent.abort();
    signal.addEventListener('abort', abort, { once: true });
    try {
      stage = 'standard-prompt';
      signal.throwIfAborted();
      // Typed batch preserves adjacent source labels and original image blocks;
      // Session.prompt(text, images) would reorder those positions.
      await session.agent.prompt(messages);
    } finally {
      signal.removeEventListener('abort', abort);
    }
    const result = session.agent.state.messages.at(-1);
    stage = 'final';
    if (signal.aborted || streams !== 1 || payloads !== 1 || result?.role !== 'assistant' ||
        result.stopReason !== 'stop' || result.content.length !== 1 || result.content[0].type !== 'text' ||
        Buffer.byteLength(result.content[0].text) > profile.limits.structured_final_bytes || result.content[0].text.includes(apiKey)) throw denied();
    // Exact provider-authored final bytes only. The same supervised Go producer
    // must still parse the strict final, bind observations and complete ordinary
    // WriteBack/Release before #200's upload; this is not a READY carrier.
    return result.content[0].text;
  } catch {
    throw new Error(`prepared Pi ${stage} denied`); // only fixed stage, no raw cause/key/task
  } finally {
    session?.dispose();
    boundary?.dispose();
    resources?.dispose();
  }
}
