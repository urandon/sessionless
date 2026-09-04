<script lang="ts">
  import { exactUTCTime, validTimestamp } from '$lib/attached-worker/presentation';

  let { value }: { value?: string } = $props();
</script>

{#if validTimestamp(value)}
  <time datetime={value}>{exactUTCTime(value)}</time>
{:else}
  <span>Not recorded</span>
{/if}
