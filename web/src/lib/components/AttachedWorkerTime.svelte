<script lang="ts">
  import { exactUTCTime, relativeTime, validTimestamp } from '$lib/attached-worker/presentation';

  let { value, relativeTo }: { value?: string; relativeTo?: string } = $props();
  let relative = $derived(value && relativeTo ? relativeTime(value, relativeTo) : '');
</script>

{#if validTimestamp(value)}
  <time datetime={value}>{exactUTCTime(value)}</time>{#if relative}
    <span> ({relative})</span>
  {/if}
{:else}
  <span>Not recorded</span>
{/if}
