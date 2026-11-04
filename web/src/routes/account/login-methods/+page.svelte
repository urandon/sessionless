<script lang="ts">
  import { replaceState } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { page } from '$app/state';
  import { CanonicalApiClient } from '$lib/api/client';
  import AccountLoginMethods from '$lib/account-identity/AccountLoginMethods.svelte';
  import { isIdentityOperationID } from '$lib/account-identity/contract';

  const client = new CanonicalApiClient();
  const references = $derived(page.url.searchParams.getAll('operation_id'));
  const operationId = $derived(references.length === 1 ? references[0] : undefined);
  const validReference = $derived(
    page.url.hash === '' &&
      references.length <= 1 &&
      (references.length === 0 || isIdentityOperationID(operationId)) &&
      [...page.url.searchParams.keys()].every((key) => key === 'operation_id'),
  );

  function onOperationIdChange(id: string): void {
    if (!isIdentityOperationID(id)) return;
    // The sole local selector is opaque and never grants operation authority.
    // No provider state, code, nonce or browser secret enters local routing.
    replaceState(resolve(`/account/login-methods?operation_id=${encodeURIComponent(id)}`), {});
  }
</script>

<svelte:head>
  <title>Sign-in methods · Sessionless</title>
  <meta name="referrer" content="no-referrer" />
</svelte:head>

{#if validReference}
  <AccountLoginMethods {client} {operationId} {onOperationIdChange} />
{:else}
  <section class="narrow panel" aria-labelledby="account-reference-title">
    <p class="eyebrow">Account</p>
    <h1 id="account-reference-title">Invalid operation reference</h1>
    <p>
      This URL cannot identify an account operation. Open your sign-in methods to check their
      status.
    </p>
    <a class="button" href={resolve('/account/login-methods')}>Open sign-in methods</a>
  </section>
{/if}
