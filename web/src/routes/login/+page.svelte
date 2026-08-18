<script lang="ts">
  import { resolve } from '$app/paths';
  import { page } from '$app/state';

  const accessDenied = $derived(page.url.searchParams.get('auth_error') === 'access_denied');
  const temporarilyUnavailable = $derived(
    page.url.searchParams.get('auth_error') === 'temporarily_unavailable',
  );
  const returnTo = $derived(safeReturnTo(page.url.searchParams.get('return_to')));
  const authStart = $derived(`/auth/telegram/start?return_to=${encodeURIComponent(returnTo)}`);

  function safeReturnTo(candidate: string | null): string {
    if (
      candidate?.startsWith('/') &&
      !candidate.startsWith('//') &&
      !candidate.includes('\\') &&
      !Array.from(candidate).some((character) => {
        const code = character.charCodeAt(0);
        return code <= 31 || code === 127;
      })
    ) {
      return candidate;
    }
    return '/';
  }
</script>

<svelte:head>
  <title
    >{accessDenied || temporarilyUnavailable ? 'Access unavailable' : 'Sign in'} · Sessionless</title
  >
</svelte:head>

<section class="narrow panel" aria-labelledby="login-title">
  {#if accessDenied}
    <p class="eyebrow">Access unavailable</p>
    <h1 id="login-title">This Telegram account has no Sessionless workspace yet</h1>
    <p>
      Initialize your workspace through the Sessionless Telegram bot, then return here and sign in
      again. Signing in never creates tenant access by itself.
    </p>
  {:else if temporarilyUnavailable}
    <p class="eyebrow">Temporarily unavailable</p>
    <h1 id="login-title">Sign-in could not be completed safely</h1>
    <p>The security audit could not be recorded. Please try again shortly.</p>
  {:else}
    <p class="eyebrow">Welcome back</p>
    <h1 id="login-title">Sign in to Sessionless</h1>
    <p>
      Telegram verifies your identity. Workspace access still comes from Sessionless membership.
    </p>
  {/if}
  <div class="actions">
    <!-- eslint-disable-next-line svelte/no-navigation-without-resolve -- Go BFF auth route -->
    <a class="button primary" href={authStart}>Continue with Telegram</a>
    <a class="button" href={resolve('/')}>Back to sessions</a>
  </div>
</section>
