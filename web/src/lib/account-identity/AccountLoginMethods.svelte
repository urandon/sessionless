<script lang="ts">
  import { onDestroy, onMount, tick, untrack } from 'svelte';
  import { resolve } from '$app/paths';
  import { ApiError, CanonicalApiClient } from '$lib/api/client';
  import {
    isIdentityOperationID,
    providerLabel,
    validateIdentityAuthorizationURL,
    type AccountLoginMethods,
    type AccountLoginMethod,
    type IdentityOperationView,
    type IdentityOperationStart,
  } from './contract';

  type AccountClient = Pick<
    CanonicalApiClient,
    | 'getAccountLoginMethods'
    | 'getIdentityOperationOutcome'
    | 'startIdentityOperation'
    | 'continueIdentityOperation'
    | 'confirmIdentityOperation'
    | 'cancelIdentityOperation'
  >;
  let {
    client = new CanonicalApiClient(),
    operationId,
    onOperationIdChange = () => undefined,
    onNavigate = (url: string) => globalThis.location.assign(url),
  }: {
    client?: AccountClient;
    operationId?: string;
    onOperationIdChange?: (id: string) => void;
    onNavigate?: (url: string) => void;
  } = $props();

  const id = $props.id();
  let methods = $state<AccountLoginMethods | null>(null);
  let operation = $state<IdentityOperationView | null>(null);
  let currentID = $state('');
  const reloginReturn = $derived(
    isIdentityOperationID(currentID)
      ? '/account/login-methods?' + new URLSearchParams({ operation_id: currentID }).toString()
      : '/account/login-methods',
  );
  let loading = $state(true);
  let busy = $state(false);
  let uncertain = $state(false);
  let error = $state('');
  let announcement = $state('');
  let heading: HTMLHeadingElement;
  let operationHeading = $state<HTMLHeadingElement>();
  let errorSummary = $state<HTMLDivElement>();
  let alive = true;
  let generation = 0;
  let scopeClient: AccountClient | undefined;
  let scopeAbort = new AbortController();
  const pending = $derived(
    operation !== null && operation.phase !== undefined && operation.phase !== 'terminal',
  );
  const canUseOperation = $derived(
    !loading &&
      !busy &&
      !uncertain &&
      operation !== null &&
      operation.outcome !== 'expired' &&
      methods?.revision === operation.before_revision,
  );

  function present(version: number, signal: AbortSignal): boolean {
    return alive && generation === version && !signal.aborted;
  }
  function newScope(): { version: number; signal: AbortSignal } {
    scopeAbort.abort();
    scopeAbort = new AbortController();
    generation += 1;
    return { version: generation, signal: scopeAbort.signal };
  }
  function explanation(cause: unknown, mutation = false): string {
    const code = cause instanceof ApiError ? cause.code : 'temporarily_unavailable';
    switch (code) {
      case 'unauthenticated':
        return 'Sign in again through a remaining sign-in method, then return to this operation to read its stored outcome. A lost replacement cookie cannot be restored here.';
      case 'access_denied':
      case 'csrf_failed':
        return 'Your current session or permission no longer authorizes this action. Sign in again and refresh; no request will be replayed.';
      case 'conflict':
        return 'This change conflicts with current account data. No other account details are available. Refresh before starting a new explicit operation.';
      case 'stale_revision':
        return 'Your sign-in methods changed. Refresh their current revision before starting a new explicit operation.';
      case 'last_method':
        return 'The final usable sign-in method cannot be removed. Keep a different enabled method linked.';
      case 'proof_required':
        return 'The required fresh provider checks are incomplete. Refresh the stored operation; do not reuse an old provider callback.';
      case 'proof_unavailable':
        return 'The provider proof is unavailable or was already consumed. Refresh status, then start a new explicit operation if needed. Do not exchange the code again.';
      case 'provider_unavailable':
        return 'This provider is unavailable for the requested change. Refresh the stored operation before trying a new explicit operation.';
      case 'expired':
        return 'The original operation or proof window expired. Refresh its stored outcome; expiry is not a success receipt.';
      case 'outcome_unavailable':
        return 'No retained outcome is available. This does not prove that the change failed. Sign in again and check current sign-in methods.';
      case 'not_found':
        return 'Account sign-in settings are unavailable in this deployment.';
      case 'invalid_request':
        return 'This operation reference or request is invalid. Open sign-in settings again without an invalid reference.';
      case 'rate_limited':
        return 'Too many requests. Wait before explicitly refreshing status; nothing will be retried automatically.';
      default:
        return mutation
          ? 'The request outcome is unknown and may already be stored. Do not repeat it. Refresh status; if the replacement cookie was lost, sign in again through a remaining method and read this operation.'
          : 'Sign-in settings could not be read. Current permissions and operation outcome are unknown. Refresh explicitly when ready.';
    }
  }
  async function focusError(version: number, signal: AbortSignal): Promise<void> {
    await tick();
    if (present(version, signal)) errorSummary?.focus();
  }
  async function focusOperation(version: number, signal: AbortSignal): Promise<void> {
    await tick();
    if (present(version, signal)) operationHeading?.focus();
  }
  async function read(api: AccountClient, reference: string, explicit: boolean): Promise<void> {
    const { version, signal } = newScope();
    loading = true;
    busy = false;
    error = '';
    methods = null;
    operation = null;
    if (reference && !isIdentityOperationID(reference)) {
      error = explanation(new ApiError('invalid_request', '', 400));
      loading = false;
      if (explicit) await focusError(version, signal);
      return;
    }
    try {
      const [rows, view] = await Promise.all([
        api.getAccountLoginMethods(signal),
        reference ? api.getIdentityOperationOutcome(reference, signal) : Promise.resolve(null),
      ]);
      if (!present(version, signal)) return;
      methods = rows;
      operation = view;
      uncertain = false;
      if (explicit)
        announcement = 'Current stored status read. No provider check or change was replayed.';
    } catch (cause) {
      if (!present(version, signal)) return;
      error = explanation(cause);
      if (explicit) await focusError(version, signal);
    } finally {
      if (present(version, signal)) loading = false;
    }
  }

  $effect(() => {
    const reference = operationId ?? '';
    const api = client;
    untrack(() => {
      // The page echoes a successful Start's ID into its local query. That is
      // the SAME scope, not a new load that discards the returned pending view.
      if (scopeClient === api && reference === currentID) return;
      scopeClient = api;
      currentID = reference;
      uncertain = false;
      announcement = '';
      void read(api, reference, false);
    });
  });
  onMount(() => heading.focus());
  onDestroy(() => {
    alive = false;
    generation += 1;
    scopeAbort.abort();
  });

  function allowed(row: AccountLoginMethod, action: 'link' | 'unlink'): boolean {
    return (
      !loading &&
      !busy &&
      !uncertain &&
      (!pending || operation?.allowed_actions.length === 0) &&
      row.allowed_actions.includes(action)
    );
  }
  async function start(row: AccountLoginMethod, kind: 'link' | 'unlink'): Promise<void> {
    if (!methods || !allowed(row, kind)) return;
    const input: IdentityOperationStart =
      kind === 'link'
        ? { kind, target_provider: row.provider, expected_revision: methods.revision }
        : { kind, method_handle: row.method_handle!, expected_revision: methods.revision };
    const version = generation,
      signal = scopeAbort.signal,
      api = client;
    busy = true;
    error = '';
    announcement = '';
    try {
      const view = await api.startIdentityOperation(input, signal);
      if (!present(version, signal)) return;
      currentID = view.operation_id;
      operation = view;
      onOperationIdChange(currentID);
      announcement =
        'Operation started. No sign-in method has changed. Continue a provider check only when ready.';
      await focusOperation(version, signal);
    } catch (cause) {
      if (!present(version, signal)) return;
      uncertain = true;
      error = explanation(cause, true);
      await focusError(version, signal);
    } finally {
      if (present(version, signal)) busy = false;
    }
  }
  async function action(kind: 'continue' | 'confirm' | 'cancel'): Promise<void> {
    const view = operation;
    if (!view?.before_revision || !canUseOperation || !view.allowed_actions.includes(kind)) return;
    const version = generation,
      signal = scopeAbort.signal,
      api = client;
    busy = true;
    error = '';
    announcement = '';
    let storedResult = false;
    try {
      if (kind === 'continue') {
        const result = await api.continueIdentityOperation(
          view.operation_id,
          view.before_revision,
          signal,
        );
        if (!present(version, signal)) return;
        const url = validateIdentityAuthorizationURL(
          result.authorization_url,
          globalThis.location.origin,
        );
        onNavigate(url);
        announcement =
          'Provider navigation requested. Return here to read the stored operation; no confirmation has been submitted.';
      } else {
        const result =
          kind === 'confirm'
            ? await api.confirmIdentityOperation(view.operation_id, view.before_revision, signal)
            : await api.cancelIdentityOperation(view.operation_id, view.before_revision, signal);
        if (!present(version, signal)) return;
        operation = result;
        storedResult = true;
        announcement =
          'The server returned a stored outcome. Other Web sessions may need to sign in again after a successful change.';
        await focusOperation(version, signal);
        if (!present(version, signal)) return;
        // A successful write does not grant new row capabilities. Read them
        // again using the actual rotated cookie, never recompute eligibility.
        methods = null;
        const rows = await api.getAccountLoginMethods(signal);
        if (present(version, signal)) methods = rows;
      }
    } catch (cause) {
      if (!present(version, signal)) return;
      uncertain = true;
      error = storedResult
        ? `The stored outcome above was returned, but current methods could not be refreshed. ${explanation(cause)}`
        : explanation(cause, true);
      if (
        cause instanceof ApiError &&
        ['unauthenticated', 'access_denied', 'csrf_failed'].includes(cause.code)
      )
        methods = null;
      await focusError(version, signal);
    } finally {
      if (present(version, signal)) busy = false;
    }
  }
</script>

<svelte:head><meta name="referrer" content="no-referrer" /></svelte:head>
<section class="account-methods" aria-labelledby={`${id}-heading`} aria-busy={loading || busy}>
  <header>
    <h1 id={`${id}-heading`} bind:this={heading} tabindex="-1">Login methods</h1>
    <button
      type="button"
      class="button"
      disabled={busy || loading}
      onclick={() => void read(client, currentID, true)}>Refresh status</button
    >
  </header>
  <p>
    Manage sign-in to your existing Sessionless account. Linking does not merge another account,
    transfer its data, or enable Telegram messaging.
  </p>
  <p role="status" aria-live="polite" aria-atomic="true">{announcement}</p>
  {#if error}
    <div bind:this={errorSummary} role="alert" tabindex="-1" class="account-error">
      <h2>Action needs attention</h2>
      <p>{error}</p>
      <a href={resolve(`/login?return_to=${encodeURIComponent(reloginReturn)}`)}>Sign in again</a>
    </div>
  {/if}
  {#if loading}<p>Reading sign-in methods and stored status…</p>{/if}
  {#if methods}
    <p>Current sign-in revision: <span class="revision">{methods.revision}</span></p>
    <ul class="method-list" aria-label="Your sign-in methods">
      {#each methods.methods as method (method.provider)}
        <li>
          <h2>{providerLabel[method.provider]}</h2>
          <p>
            {method.linked
              ? method.current
                ? 'Linked · current sign-in'
                : 'Linked'
              : 'Not linked'}{!method.enabled ? ' · unavailable in this deployment' : ''}
          </p>
          {#if method.allowed_actions.includes('link')}
            <p>
              Linking requires a new check with your current provider and a separate check with {providerLabel[
                method.provider
              ]}, then local confirmation.
            </p>
            <button
              type="button"
              class="button"
              disabled={!allowed(method, 'link')}
              onclick={() => void start(method, 'link')}
              >Start linking {providerLabel[method.provider]}</button
            >
          {:else if method.allowed_actions.includes('unlink') && method.remaining_provider}
            <p>
              Keep signing in with {providerLabel[method.remaining_provider]}. A fresh check with
              that remaining method and local confirmation are required. Other Web sessions will
              need sign-in again.
            </p>
            <button
              type="button"
              class="button"
              disabled={!allowed(method, 'unlink')}
              onclick={() => void start(method, 'unlink')}
              >Start unlinking {providerLabel[method.provider]}</button
            >
          {:else}
            <p class="muted">
              {method.linked
                ? 'The server offers no removal action for this method. A different enabled sign-in method is required.'
                : 'The server offers no linking action for this provider.'}
            </p>
          {/if}
        </li>
      {/each}
    </ul>
  {/if}
  {#if operation}
    <section aria-labelledby={`${id}-operation`} class="operation">
      <h2 id={`${id}-operation`} bind:this={operationHeading} tabindex="-1">Stored operation</h2>
      {#if operation.outcome === 'outcome_unavailable'}
        <p>
          No retained outcome is available. This does not prove the change failed or permit replay.
          Sign in again through a remaining method and check current sign-in methods.
        </p>
        <a href={resolve(`/login?return_to=${encodeURIComponent(reloginReturn)}`)}>Sign in again</a>
      {:else if operation.phase === 'terminal'}
        <p>
          <strong
            >{operation.outcome === 'linked'
              ? 'Sign-in method linked.'
              : operation.outcome === 'unlinked'
                ? 'Sign-in method unlinked.'
                : operation.outcome === 'cancelled'
                  ? 'Operation cancelled.'
                  : operation.outcome === 'expired'
                    ? 'Operation expired.'
                    : 'The operation did not complete the requested change.'}</strong
          >
        </p>
        {#if operation.outcome === 'linked' || operation.outcome === 'unlinked'}<p>
            Your existing account and resource ownership remain the same. Other Web sessions need to
            sign in again.
          </p>{:else if operation.outcome !== 'cancelled'}<p>
            Refresh current methods before a new explicit attempt. Do not reuse a consumed provider
            code.
          </p>{/if}
        {#if operation.completed_at}<p>
            Recorded at <time datetime={operation.completed_at}>{operation.completed_at}</time>.
          </p>{/if}
        {#if operation.after_revision}<p>
            Stored revision: <span class="revision">{operation.after_revision}</span>.
          </p>{/if}
      {:else if operation.outcome === 'expired'}
        <p>
          The pending operation expired. This is not a committed success or a terminal receipt.
          Refresh status; never replay an old provider callback.
        </p>
      {:else}
        <p>
          {operation.kind === 'link' ? 'Link' : 'Unlink'}
          {operation.target_provider ? providerLabel[operation.target_provider] : 'sign-in method'} ·
          {operation.phase === 'source_proof'
            ? operation.kind === 'unlink'
              ? 'Check the remaining sign-in method'
              : 'Check your existing account first'
            : operation.phase === 'target_proof'
              ? 'Check the target provider separately'
              : 'Review the final local confirmation'}.
        </p>
        {#if operation.expires_at}<p>
            The original window ends at <time datetime={operation.expires_at}
              >{operation.expires_at}</time
            >. Refresh does not extend it.
          </p>{/if}
        {#if operation.phase === 'confirm'}<p>
            Confirm only this unchanged change to your existing account. No account merge or
            resource transfer occurs. Other Web sessions will need to sign in again.
          </p>{/if}
        {#if operation.allowed_actions.length === 0}<p>
            The server currently offers no continuation. This read is not permission to restart a
            provider exchange. Refresh status or start a new explicit operation from current data.
          </p>{/if}
        <div class="actions">
          {#if operation.allowed_actions.includes('continue')}<button
              type="button"
              class="button primary"
              disabled={!canUseOperation}
              onclick={() => void action('continue')}>Continue provider check</button
            >{/if}
          {#if operation.allowed_actions.includes('confirm')}<button
              type="button"
              class="button primary"
              disabled={!canUseOperation}
              onclick={() => void action('confirm')}
              >Confirm {operation.kind === 'link' ? 'link' : 'unlink'}</button
            >{/if}
          {#if operation.allowed_actions.includes('cancel')}<button
              type="button"
              class="button"
              disabled={!canUseOperation}
              onclick={() => void action('cancel')}>Cancel operation</button
            >{/if}
        </div>
      {/if}
    </section>
  {/if}
</section>

<style>
  .account-methods {
    max-width: 52rem;
    overflow-wrap: anywhere;
  }
  header,
  .actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 1rem;
    justify-content: space-between;
  }
  .method-list {
    list-style: none;
    padding: 0;
    display: grid;
    gap: 1rem;
  }
  .method-list li,
  .operation,
  .account-error {
    padding: 1rem;
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    background: var(--paper);
  }
  .method-list h2 {
    margin-top: 0;
  }
  .operation {
    margin-block: 1rem;
  }
  .account-error {
    border-color: var(--accent);
  }
  .muted {
    color: var(--muted);
  }
  .revision {
    font-variant-numeric: tabular-nums;
  }
  button {
    min-height: 44px;
    max-width: 100%;
    white-space: normal;
  }
  :is(h1, h2, .account-error):focus-visible {
    outline: 3px solid var(--focus);
    outline-offset: 3px;
  }
  @media (max-width: 32rem) {
    header {
      align-items: stretch;
    }
    .actions {
      flex-direction: column;
      align-items: stretch;
    }
  }
</style>
