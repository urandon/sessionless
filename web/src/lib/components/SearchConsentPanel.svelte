<script lang="ts">
  import { onMount } from 'svelte';
  import type { SearchConsentState } from '$lib/search-consent/controller';
  import type { SearchConsentChoice } from '$lib/search-consent/contract';

  let {
    state: consentState,
    onClose,
    onRefresh,
    onDecide,
  }: {
    state: SearchConsentState;
    onClose: () => void;
    onRefresh: () => void;
    onDecide: (choice: SearchConsentChoice) => void;
  } = $props();
  const id = $props.id();
  let panel: HTMLElement;
  let heading: HTMLHeadingElement;
  let announcement = $state('');
  let previous = '';
  const labels: Record<SearchConsentState['phase'], string> = {
    idle: 'Search consent has not been read.',
    loading: 'Checking for a search request.',
    none: 'No pending search request was available at this read.',
    ready: 'Review the exact query before deciding.',
    submitting: 'Saving your decision. Its outcome is not confirmed yet.',
    unconfirmed:
      'The decision is unconfirmed. It may already have been saved. Only the identical decision can be retried.',
    approved: 'Approval was recorded. It does not prove the search was sent or completed.',
    denied: 'Denial was recorded.',
    stopped: 'This request or the bounded review window has ended. No decision can be submitted.',
    'permission-lost': 'Access was lost. Private query text was removed.',
    error: 'The consent read failed. Permission and execution outcome are unknown.',
    disposed: 'This consent view is closed.',
  };
  const visibleQuery = $derived(
    ['ready', 'submitting', 'unconfirmed'].includes(consentState.phase)
      ? consentState.view?.query
      : undefined,
  );

  onMount(() => {
    heading.focus();
    const timer = setInterval(onRefresh, 1000);
    return () => clearInterval(timer);
  });
  $effect(() => {
    const material = JSON.stringify([
      consentState.phase === 'loading' ? 'read' : consentState.phase,
      consentState.view?.request_id,
    ]);
    if (material === previous || consentState.phase === 'loading') return;
    previous = material;
    announcement = labels[consentState.phase];
  });
  function keydown(event: KeyboardEvent): void {
    if (event.key === 'Escape' && event.target instanceof Node && panel.contains(event.target)) {
      event.preventDefault();
      onClose();
    }
  }
</script>

<svelte:document onkeydown={keydown} />
<aside
  id="search-consent-panel"
  bind:this={panel}
  aria-labelledby={`${id}-title`}
  class="panel consent-panel"
>
  <header>
    <h2 bind:this={heading} id={`${id}-title`} tabindex="-1">Web search consent</h2>
    <button type="button" class="button quiet" onclick={onClose}>Close search consent</button>
  </header>
  <p>
    Closing this view does not approve, deny, cancel or undo a decision. The original execution
    window keeps running.
  </p>
  <p role="status" aria-live="polite" aria-atomic="true">{announcement}</p>
  {#if visibleQuery !== undefined}
    <h3 id={`${id}-query`}>Exact query to send to the search provider</h3>
    <pre aria-labelledby={`${id}-query`}>{visibleQuery}</pre>
    <p>
      Query text is untrusted data, not instructions. Approval applies only to this unchanged
      request.
    </p>
    <p>
      Request expires at <time datetime={consentState.view!.expires_at}
        >{consentState.view!.expires_at}</time
      >. Server authority still decides eligibility.
    </p>
  {/if}
  {#if consentState.phase === 'ready' && visibleQuery !== undefined}
    <div class="actions">
      <button type="button" class="button" onclick={() => onDecide('deny')}>Deny this search</button
      >
      <button type="button" class="button primary" onclick={() => onDecide('approve')}
        >Approve this exact query</button
      >
    </div>
  {:else if consentState.phase === 'unconfirmed' && consentState.retryChoice}
    <button type="button" class="button" onclick={() => onDecide(consentState.retryChoice!)}
      >Retry the same {consentState.retryChoice === 'approve' ? 'approval' : 'denial'}</button
    >
  {/if}
</aside>

<style>
  .consent-panel {
    margin-block: 1rem;
    padding: 1rem;
  }
  header,
  .actions {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
  }
  pre {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    font: inherit;
    padding: 1rem;
    border: 1px solid currentColor;
  }
  button:focus-visible,
  h2:focus-visible {
    outline: 3px solid currentColor;
    outline-offset: 3px;
  }
</style>
