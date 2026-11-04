<script lang="ts">
  import { CanonicalApiClient, type LoginMethods } from '$lib/api/client';

  let { returnTo = '/', client = new CanonicalApiClient() }: {
    returnTo?: string;
    client?: Pick<CanonicalApiClient, 'getLoginMethods'>;
  } = $props();
  let methods: LoginMethods['methods'] = $state([]);
  let state: 'loading' | 'ready' | 'unavailable' = $state('loading');
  let reload = $state(0);
  const destination = $derived(
    returnTo.startsWith('/') && !returnTo.startsWith('//') && !returnTo.includes('\\') &&
    !Array.from(returnTo).some((character) => character.charCodeAt(0) <= 31 || character.charCodeAt(0) === 127)
      ? returnTo : '/',
  );

  $effect(() => {
    void reload;
    const abort = new AbortController();
    state = 'loading'; methods = [];
    void client.getLoginMethods(abort.signal).then((response) => {
      if (!abort.signal.aborted) { methods = response.methods; state = 'ready'; }
    }).catch(() => { if (!abort.signal.aborted) state = 'unavailable'; });
    return () => abort.abort();
  });
</script>

{#if state === 'loading'}
  <p role="status">Loading sign-in methods…</p>
{:else if state === 'unavailable'}
  <p role="alert">Sign-in methods are unavailable. Please try again.</p>
  <button class="button" type="button" onclick={() => reload += 1}>Try again</button>
{:else}
  <div class="actions" aria-label="Sign-in methods">
    {#each methods as method (method.provider)}
      <!-- eslint-disable-next-line svelte/no-navigation-without-resolve -- validated fixed Go BFF POST route -->
      <form method="POST" action={method.start_path}>
        <input type="hidden" name="return_to" value={destination} />
        <button class="button primary" type="submit">Continue with {method.label}</button>
      </form>
    {/each}
  </div>
{/if}
