<script lang="ts">
  import { page } from '$app/state';

  import { CanonicalApiClient } from '$lib/api/client';
  import SessionDetail from '$lib/components/SessionDetail.svelte';

  const client = new CanonicalApiClient();
  const sessionId = $derived(page.params.sessionId ?? '');
</script>

<svelte:head>
  <title>Session · Sessionless</title>
</svelte:head>

<SessionDetail {client} {sessionId} />
