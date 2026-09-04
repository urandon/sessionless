<script lang="ts">
  import { CanonicalApiClient } from '$lib/api/client';
  import AttachedWorkerOverview from '$lib/components/AttachedWorkerOverview.svelte';

  const client = new CanonicalApiClient();
</script>

<AttachedWorkerOverview {client} />
