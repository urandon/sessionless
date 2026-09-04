<script lang="ts">
  import type { AttachedWorkerAvailableAction } from '$lib/api/client';
  import { explainAction, explainUnavailable } from '$lib/attached-worker/presentation';

  let { actions }: { actions: readonly AttachedWorkerAvailableAction[] } = $props();
</script>

{#if actions.length === 0}
  <p class="empty">No controls are exposed by this read model.</p>
{:else}
  <ul class="action-list" aria-label="Worker controls">
    {#each actions as action, index (`${action.code}-${index}`)}
      {@const unavailable = action.enabled
        ? {
            title: 'Read-only interface',
            description: 'This interface does not execute worker controls.',
          }
        : explainUnavailable(action.reason_code)}
      <li>
        <div>
          <strong>{explainAction(action.code)}</strong>
          <code>{action.code}</code>
        </div>
        <p><span class="status">Unavailable</span> — {unavailable.title}</p>
        <small>{unavailable.description}</small>
        {#if action.reason_code}<code class="reason-code">{action.reason_code}</code>{/if}
      </li>
    {/each}
  </ul>
{/if}

<style>
  .action-list {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 17rem), 1fr));
    gap: 0.75rem;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  li {
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    padding: 0.9rem;
    background: #f5f3ed;
  }

  li > div {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
  }

  p {
    margin: 0.65rem 0 0.25rem;
  }

  small,
  .empty {
    color: var(--muted);
  }

  code {
    font-size: 0.7rem;
    overflow-wrap: anywhere;
  }

  .reason-code {
    display: block;
    margin-top: 0.5rem;
  }

  .status {
    font-weight: 800;
  }
</style>
