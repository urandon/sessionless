<script module lang="ts">
  import type {
    CanonicalApiClient,
    ConditionalResult,
    Identity,
    SessionPage,
    SessionSummary,
  } from '$lib/api/client';
  import type { components } from '$lib/api/generated';

  type SessionStatus = components['schemas']['SessionStatus'];

  export interface DashboardApi {
    getIdentity: CanonicalApiClient['getIdentity'];
    listSessions: CanonicalApiClient['listSessions'];
    selectTenant: CanonicalApiClient['selectTenant'];
    createSession: CanonicalApiClient['createSession'];
    setSessionArchived: CanonicalApiClient['setSessionArchived'];
    logout: CanonicalApiClient['logout'];
  }

  type ViewState = 'loading' | 'ready' | 'unauthenticated' | 'access-denied' | 'error';
</script>

<script lang="ts">
  import { onMount } from 'svelte';
  import { resolve } from '$app/paths';

  import { ApiError } from '$lib/api/client';

  let {
    client,
    initialStatus = 'active',
  }: { client: DashboardApi; initialStatus?: SessionStatus } = $props();

  let view = $state<ViewState>('loading');
  let identity = $state<Identity | undefined>();
  let sessions = $state<SessionSummary[]>([]);
  let status = $state<SessionStatus>('active');
  let pendingAction = $state<string | undefined>();
  let errorMessage = $state('');
  let createKey: string | undefined;
  const archiveKeys: Record<string, { desired: boolean; key: string } | undefined> = {};

  const activeTenant = $derived(identity?.tenants.find((tenant) => tenant.active));

  onMount(() => {
    status = initialStatus;
    void load();
  });

  async function load(): Promise<void> {
    view = 'loading';
    errorMessage = '';
    try {
      const [nextIdentity, page] = await Promise.all([
        client.getIdentity(),
        client.listSessions({ status, limit: 50 }),
      ]);
      identity = nextIdentity;
      applySessionPage(page);
      view = 'ready';
    } catch (error) {
      showFailure(error);
    }
  }

  async function changeStatus(nextStatus: SessionStatus): Promise<void> {
    if (status === nextStatus || pendingAction) return;
    status = nextStatus;
    pendingAction = 'status';
    errorMessage = '';
    try {
      applySessionPage(await client.listSessions({ status, limit: 50 }));
    } catch (error) {
      showFailure(error);
    } finally {
      pendingAction = undefined;
    }
  }

  async function changeTenant(event: Event): Promise<void> {
    const tenantId = (event.currentTarget as HTMLSelectElement).value;
    if (!tenantId || tenantId === activeTenant?.tenant_id || pendingAction) return;
    pendingAction = 'tenant';
    errorMessage = '';
    try {
      identity = await client.selectTenant(tenantId);
      applySessionPage(await client.listSessions({ status, limit: 50 }));
    } catch (error) {
      showFailure(error);
    } finally {
      pendingAction = undefined;
    }
  }

  async function createSession(): Promise<void> {
    if (pendingAction) return;
    pendingAction = 'create';
    errorMessage = '';
    createKey ??= newIdempotencyKey();
    try {
      const created = await client.createSession({ idempotency_key: createKey });
      createKey = undefined;
      globalThis.location.assign(
        resolve('/sessions/[sessionId]', { sessionId: created.session_id }),
      );
    } catch (error) {
      showFailure(error);
      pendingAction = undefined;
    }
  }

  async function toggleArchived(session: SessionSummary): Promise<void> {
    if (pendingAction) return;
    pendingAction = session.session_id;
    errorMessage = '';
    const desired = session.status === 'active';
    let mutation = archiveKeys[session.session_id];
    if (!mutation || mutation.desired !== desired) {
      mutation = { desired, key: newIdempotencyKey() };
      archiveKeys[session.session_id] = mutation;
    }
    try {
      await client.setSessionArchived(session.session_id, {
        archived: desired,
        idempotency_key: mutation.key,
      });
      delete archiveKeys[session.session_id];
      applySessionPage(await client.listSessions({ status, limit: 50 }));
    } catch (error) {
      showFailure(error);
    } finally {
      pendingAction = undefined;
    }
  }

  async function logout(): Promise<void> {
    if (pendingAction) return;
    pendingAction = 'logout';
    errorMessage = '';
    try {
      await client.logout();
      globalThis.location.assign(resolve('/login'));
    } catch (error) {
      showFailure(error);
      pendingAction = undefined;
    }
  }

  function applySessionPage(result: ConditionalResult<SessionPage>): void {
    if (result.state === 'fresh') sessions = result.data.items;
  }

  function showFailure(error: unknown): void {
    if (error instanceof ApiError && error.code === 'unauthenticated') {
      view = 'unauthenticated';
      return;
    }
    if (error instanceof ApiError && error.code === 'access_denied') {
      view = 'access-denied';
      return;
    }
    errorMessage =
      error instanceof ApiError ? error.message : 'Sessionless is temporarily unavailable.';
    view = identity ? 'ready' : 'error';
  }

  function newIdempotencyKey(): string {
    return `web-${crypto.randomUUID()}`;
  }
</script>

{#if view === 'loading'}
  <section class="panel loading-state" aria-live="polite" aria-busy="true">
    <p>Loading your workspace…</p>
  </section>
{:else if view === 'unauthenticated'}
  <section class="hero" aria-labelledby="page-title">
    <div>
      <p class="eyebrow">Canonical conversations</p>
      <h1 id="page-title">Your sessions, in one place</h1>
      <p class="lede">
        Sign in to continue a conversation across Web, Telegram, and future frontends without
        changing its history.
      </p>
    </div>
    <!-- eslint-disable-next-line svelte/no-navigation-without-resolve -- Go BFF auth route -->
    <a class="button primary" href="/auth/telegram/start?return_to=%2F">Continue with Telegram</a>
  </section>
{:else if view === 'access-denied'}
  <section class="narrow panel" aria-labelledby="access-title">
    <p class="eyebrow">Access unavailable</p>
    <h1 id="access-title">No workspace is linked to this account</h1>
    <p>Signing in proves identity but never creates tenant membership.</p>
    <!-- eslint-disable-next-line svelte/no-navigation-without-resolve -- query-bearing recovery route -->
    <a class="button primary" href={`${resolve('/login')}?auth_error=access_denied`}
      >Recovery steps</a
    >
  </section>
{:else if view === 'error'}
  <section class="narrow panel" aria-labelledby="error-title">
    <p class="eyebrow">Unable to load</p>
    <h1 id="error-title">Your workspace is temporarily unavailable</h1>
    <p role="alert">{errorMessage}</p>
    <button class="button primary" type="button" onclick={() => void load()}>Try again</button>
  </section>
{:else}
  <section class="dashboard-heading" aria-labelledby="page-title">
    <div>
      <p class="eyebrow">Canonical conversations</p>
      <h1 id="page-title">Sessions</h1>
      <p class="lede">Signed in through {identity?.provider}.</p>
    </div>
    <div class="account-actions">
      <label for="tenant">Workspace</label>
      <select
        id="tenant"
        value={activeTenant?.tenant_id}
        disabled={pendingAction !== undefined}
        onchange={(event) => void changeTenant(event)}
      >
        {#each identity?.tenants ?? [] as tenant (tenant.tenant_id)}
          <option value={tenant.tenant_id}>{tenant.tenant_id} · {tenant.role}</option>
        {/each}
      </select>
      <button
        class="button quiet"
        type="button"
        disabled={pendingAction !== undefined}
        onclick={() => void logout()}>Sign out</button
      >
    </div>
  </section>

  {#if errorMessage}
    <div class="error-banner" role="alert">{errorMessage}</div>
  {/if}

  <section class="panel" aria-labelledby="session-list-title">
    <h2 id="session-list-title" class="visually-hidden">Session list</h2>
    <div class="panel-heading dashboard-toolbar">
      <div class="tabs" aria-label="Session status">
        <button
          type="button"
          class:active={status === 'active'}
          aria-pressed={status === 'active'}
          disabled={pendingAction !== undefined}
          onclick={() => void changeStatus('active')}>Active</button
        >
        <button
          type="button"
          class:active={status === 'archived'}
          aria-pressed={status === 'archived'}
          disabled={pendingAction !== undefined}
          onclick={() => void changeStatus('archived')}>Archived</button
        >
      </div>
      <button
        class="button primary"
        type="button"
        disabled={pendingAction !== undefined}
        onclick={() => void createSession()}
      >
        {pendingAction === 'create' ? 'Creating…' : 'New session'}
      </button>
    </div>

    {#if sessions.length === 0}
      <div class="empty-state">
        <h2>No {status} sessions</h2>
        <p>
          {status === 'active'
            ? 'Create a session to start a canonical conversation.'
            : 'Archived sessions will appear here.'}
        </p>
      </div>
    {:else}
      <ul class="session-list">
        {#each sessions as session (session.session_id)}
          <li>
            <a
              class="session-link"
              href={resolve('/sessions/[sessionId]', { sessionId: session.session_id })}
            >
              <strong>{session.title || 'Untitled session'}</strong>
              <span>{session.preview || 'No preview yet'}</span>
              <small>Updated {new Date(session.updated_at).toLocaleString()}</small>
            </a>
            <button
              class="button quiet"
              type="button"
              disabled={pendingAction !== undefined}
              aria-label={`${session.status === 'active' ? 'Archive' : 'Unarchive'} ${session.title || 'untitled session'}`}
              onclick={() => void toggleArchived(session)}
            >
              {pendingAction === session.session_id
                ? 'Saving…'
                : session.status === 'active'
                  ? 'Archive'
                  : 'Unarchive'}
            </button>
          </li>
        {/each}
      </ul>
    {/if}
  </section>
{/if}
