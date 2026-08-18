<script lang="ts">
  import { page } from '$app/state';

  import { CanonicalApiClient } from '$lib/api/client';
  import SessionDashboard from '$lib/components/SessionDashboard.svelte';

  const client = new CanonicalApiClient();
  const initialStatus = $derived(
    page.url.searchParams.get('status') === 'archived' ? 'archived' : 'active',
  );
</script>

<svelte:head>
  <title>Sessions · Sessionless</title>
  <meta
    name="description"
    content="Work with canonical Sessionless conversations from any supported frontend."
  />
</svelte:head>

<SessionDashboard {client} {initialStatus} />
