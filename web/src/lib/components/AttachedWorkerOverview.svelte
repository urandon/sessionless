<script module lang="ts">
  import type { AttachedWorkerList, CanonicalApiClient } from '$lib/api/client';

  export interface AttachedWorkerOverviewApi {
    listAttachedWorkers: CanonicalApiClient['listAttachedWorkers'];
  }

  type ViewState = 'loading' | 'ready' | 'error';
</script>

<script lang="ts">
  import { onMount } from 'svelte';
  import { resolve } from '$app/paths';

  import { ApiError } from '$lib/api/client';
  import { labelState } from '$lib/attached-worker/presentation';
  import AttachedWorkerTime from './AttachedWorkerTime.svelte';
  import AttachedWorkerWarnings from './AttachedWorkerWarnings.svelte';

  let { client }: { client: AttachedWorkerOverviewApi } = $props();

  let view = $state<ViewState>('loading');
  let page = $state<AttachedWorkerList | undefined>();
  let loadingMore = $state(false);
  let errorMessage = $state('');
  let updateAnnouncement = $state('');

  onMount(() => void load());

  async function load(): Promise<void> {
    view = 'loading';
    errorMessage = '';
    try {
      page = await client.listAttachedWorkers({ limit: 20 });
      view = 'ready';
    } catch (error) {
      errorMessage = publicFailure(error);
      view = 'error';
    }
  }

  async function loadMore(): Promise<void> {
    if (!page?.has_more || !page.next_worker_id || loadingMore) return;
    loadingMore = true;
    errorMessage = '';
    updateAnnouncement = '';
    try {
      const next = await client.listAttachedWorkers({
        afterWorkerId: page.next_worker_id,
        limit: 20,
      });
      const existing = new Set(page.items.map((item) => item.worker.worker_id));
      const appended = next.items.filter((item) => !existing.has(item.worker.worker_id));
      page = { ...next, items: [...page.items, ...appended] };
      updateAnnouncement = `${appended.length} more ${appended.length === 1 ? 'worker' : 'workers'} loaded.`;
    } catch (error) {
      errorMessage = publicFailure(error);
    } finally {
      loadingMore = false;
    }
  }

  function publicFailure(error: unknown): string {
    if (error instanceof ApiError && error.code === 'unauthenticated') {
      return 'Sign in again to view attached workers.';
    }
    return error instanceof ApiError
      ? error.message
      : 'Attached workers are temporarily unavailable.';
  }
</script>

<svelte:head>
  <title>Attached workers · Sessionless</title>
  <meta name="description" content="Inspect owner-scoped attached-worker status and evidence." />
</svelte:head>

<section class="heading" aria-labelledby="worker-page-title">
  <div>
    <p class="eyebrow">Owner-managed compute</p>
    <h1 id="worker-page-title">Attached workers</h1>
    <p class="lede">
      Current control-plane truth and bounded observations are shown separately. This view cannot
      change worker state.
    </p>
  </div>
</section>

{#if view === 'loading'}
  <section class="panel loading-state" aria-live="polite" aria-busy="true">
    <p>Loading attached workers…</p>
  </section>
{:else if view === 'error'}
  <section class="panel error-state" aria-labelledby="worker-error-title">
    <h2 id="worker-error-title">Attached workers cannot be loaded</h2>
    <p role="alert">{errorMessage}</p>
    <button class="button primary" type="button" onclick={() => void load()}>Try again</button>
  </section>
{:else if !page || page.items.length === 0}
  <section class="panel empty-state" aria-labelledby="empty-workers-title">
    <h2 id="empty-workers-title">No attached workers</h2>
    <p>No owner-scoped worker is visible in the active workspace.</p>
  </section>
{:else}
  <section class="worker-content" aria-labelledby="worker-list-title">
    <div class="list-heading">
      <h2 id="worker-list-title">Worker overview</h2>
      <p>Latest page evaluated <AttachedWorkerTime value={page.evaluated_at} /></p>
    </div>
    {#if errorMessage}<p class="error-banner" role="alert">{errorMessage}</p>{/if}
    <ul class="worker-list">
      {#each page.items as item (item.worker.worker_id)}
        <li class="worker-card">
          <div class="card-heading">
            <div>
              <h3>
                <a
                  href={resolve('/workers/[workerId]', {
                    workerId: item.worker.worker_id,
                  })}>{item.worker.display_name}</a
                >
              </h3>
              <code>{item.worker.worker_id}</code>
            </div>
            <span class="state-text">{labelState(item.worker.observed_state)}</span>
          </div>
          <dl>
            <div>
              <dt>This worker evaluated</dt>
              <dd><AttachedWorkerTime value={item.evaluated_at} /></dd>
            </div>
            <div>
              <dt>Desired state</dt>
              <dd>{labelState(item.worker.desired_state)}</dd>
            </div>
            <div>
              <dt>Observed state</dt>
              <dd>{labelState(item.worker.observed_state)}</dd>
            </div>
            <div>
              <dt>Connection</dt>
              <dd>{labelState(item.connectivity.state)}</dd>
            </div>
            <div>
              <dt>Contact freshness</dt>
              <dd>{labelState(item.connectivity.freshness)}</dd>
            </div>
            <div>
              <dt>Attempt</dt>
              <dd>{labelState(item.execution_state)}</dd>
            </div>
          </dl>
          <AttachedWorkerWarnings
            codes={item.observation_warnings}
            label={`Warnings for ${item.worker.display_name}`}
          />
        </li>
      {/each}
    </ul>
    {#if page.has_more}
      <button
        class="button quiet more"
        type="button"
        disabled={loadingMore}
        onclick={() => void loadMore()}
      >
        {loadingMore ? 'Loading…' : 'Load more workers'}
      </button>
    {/if}
    <p class="visually-hidden" aria-live="polite">{updateAnnouncement}</p>
  </section>
{/if}

<style>
  .heading {
    margin-bottom: 2rem;
  }

  .worker-content {
    display: grid;
    gap: 1rem;
  }

  .list-heading,
  .card-heading {
    display: flex;
    align-items: start;
    justify-content: space-between;
    gap: 1rem;
  }

  .list-heading p,
  h3,
  dd {
    margin: 0;
  }

  .worker-list {
    display: grid;
    gap: 1rem;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  .worker-card {
    display: grid;
    gap: 1.2rem;
    border: 1px solid var(--line);
    border-radius: 1rem;
    padding: clamp(1rem, 3vw, 1.5rem);
    background: var(--paper);
  }

  h3 {
    font-size: 1.35rem;
  }

  h3 a {
    text-underline-offset: 0.25rem;
  }

  code {
    font-size: 0.75rem;
    overflow-wrap: anywhere;
  }

  .state-text {
    border: 1px solid var(--line);
    border-radius: 999px;
    padding: 0.35rem 0.65rem;
    font-weight: 750;
  }

  dl {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(9rem, 1fr));
    gap: 0.75rem;
    margin: 0;
  }

  dl > div {
    min-width: 0;
  }

  dt {
    color: var(--muted);
    font-size: 0.78rem;
    font-weight: 750;
  }

  dd {
    margin-top: 0.2rem;
    overflow-wrap: anywhere;
  }

  .more {
    justify-self: center;
  }

  .error-state {
    display: grid;
    gap: 1rem;
    justify-items: start;
  }

  .error-state p {
    margin: 0;
  }

  @media (max-width: 42rem) {
    .list-heading,
    .card-heading {
      align-items: stretch;
      flex-direction: column;
    }

    .state-text {
      width: fit-content;
    }
  }
</style>
