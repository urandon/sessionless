<script lang="ts">
  import { resolve } from '$app/paths';
  import type { Snippet } from 'svelte';

  let { children }: { children?: Snippet } = $props();
</script>

<!-- eslint-disable-next-line svelte/no-navigation-without-resolve -- same-document focus target -->
<a class="skip-link" href="#main-content">Skip to content</a>
<div class="app-shell">
  <header class="site-header">
    <a class="brand" href={resolve('/')} aria-label="Sessionless home">
      <span class="brand-mark" aria-hidden="true">S</span>
      <span>Sessionless</span>
    </a>
    <nav aria-label="Primary navigation">
      <a href={resolve('/')}>Sessions</a>
    </nav>
  </header>
  <main id="main-content" tabindex="-1">
    {@render children?.()}
  </main>
</div>
