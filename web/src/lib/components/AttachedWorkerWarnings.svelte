<script lang="ts">
  import { explainReason } from '$lib/attached-worker/presentation';

  let { codes, label = 'Observation warnings' }: { codes: readonly string[]; label?: string } =
    $props();
</script>

{#if codes.length === 0}
  <p class="empty">No warnings were reported.</p>
{:else}
  <ul class="warning-list" aria-label={label}>
    {#each codes as code, index (`${code}-${index}`)}
      {@const explanation = explainReason(code)}
      <li>
        <strong>{explanation.title}</strong>
        <span>{explanation.description}</span>
        <code>{code}</code>
      </li>
    {/each}
  </ul>
{/if}

<style>
  .warning-list {
    display: grid;
    gap: 0.75rem;
    margin: 0;
    padding: 0;
    list-style: none;
  }

  li {
    display: grid;
    gap: 0.2rem;
    border-left: 0.25rem solid #a8501d;
    padding: 0.65rem 0.8rem;
    background: #fff8ed;
  }

  span,
  .empty {
    color: var(--muted);
  }

  code {
    width: fit-content;
    font-size: 0.75rem;
    overflow-wrap: anywhere;
  }
</style>
