<script lang="ts">
  import { page } from '$app/state';

  import { CanonicalApiClient } from '$lib/api/client';
  import AttachedWorkerDetail from '$lib/components/AttachedWorkerDetail.svelte';

  const client = new CanonicalApiClient();
  const workerId = $derived(page.params.workerId ?? '');
</script>

<AttachedWorkerDetail {client} {workerId} />
