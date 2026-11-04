<script lang="ts">
  import { validateAnswerCitations } from '$lib/citations/contract';

  let { manifest, runId }: { manifest: unknown; runId: unknown } = $props();
  const id = $props.id();
  const state = $derived.by(() => {
    try {
      return { value: validateAnswerCitations(manifest, runId), invalid: false };
    } catch {
      return { value: undefined, invalid: true };
    }
  });
</script>

{#if state.invalid}
  <p class="citation-note" role="status">Source references are unavailable.</p>
{:else if state.value && state.value.sources.length > 0}
  <section aria-labelledby={`${id}-title`} class="answer-citations">
    <h3 id={`${id}-title`}>Search excerpts referenced by this answer</h3>
    <p class="citation-note">
      These links do not establish that the pages were read or that the answer’s claims are
      supported.
    </p>
    <ol>
      {#each state.value.sources as source, index (source.source_id)}
        <li>
          <a
            href={source.url}
            target="_blank"
            rel="external noopener noreferrer"
            referrerpolicy="no-referrer"
            data-sveltekit-preload-data="off"
            data-sveltekit-preload-code="off"
            aria-label={`Source ${index + 1}: ${source.title} (opens in a new tab)`}
            >{source.title}</a
          >
          {#if source.title_truncated}<span class="citation-note"> (title abbreviated)</span>{/if}
          <div class="citation-note">
            Retrieved: <time datetime={source.retrieved_at}>{source.retrieved_at}</time>
            {#if source.published_at}<span>
                · Published: <time datetime={source.published_at}>{source.published_at}</time></span
              >{/if}
          </div>
        </li>
      {/each}
    </ol>
  </section>
{/if}

<style>
  .answer-citations {
    margin-top: 1rem;
    overflow-wrap: anywhere;
  }
  h3 {
    font-size: 0.95rem;
  }
  ol {
    padding-left: 1.5rem;
  }
  li + li {
    margin-top: 0.75rem;
  }
  .citation-note {
    color: var(--text-muted, #626a78);
    font-size: 0.85rem;
  }
</style>
