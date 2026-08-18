<script lang="ts">
  import '../styles.css';
  import AppShell from '$lib/components/AppShell.svelte';

  let { children } = $props();
</script>

<AppShell>
  {@render children()}
</AppShell>
