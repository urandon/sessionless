import { expect, test } from './fixtures/canonical-api';

for (const providers of [['yandex'], ['telegram'], ['yandex', 'telegram']] as const) {
  test(`ordinary configured login chooser ${providers.join(',')}`, async ({ canonicalApi, page }) => {
    canonicalApi.auth = 'unauthenticated';
    canonicalApi.loginProviders = [...providers];
    await page.goto('/login?return_to=%2Fsessions%2Fprior');
    for (const provider of providers) {
      const label = provider === 'yandex' ? 'Yandex' : 'Telegram';
      const form = page.locator(`form[action="/auth/login/${provider}/start"]`);
      await expect(form.getByRole('button', { name: `Continue with ${label}` })).toBeVisible();
      await expect(form).toHaveAttribute('method', 'POST');
      await expect(form.locator('input[name="return_to"]')).toHaveValue('/sessions/prior');
    }
    await expect(page.locator('form')).toHaveCount(providers.length);
    expect(canonicalApi.requests.filter((request) => request.method !== 'GET')).toEqual([]);
    const selected = providers[0];
    await page.getByRole('button', { name: `Continue with ${selected === 'yandex' ? 'Yandex' : 'Telegram'}` }).click();
    await expect(page.getByRole('heading', { name: 'Controlled provider navigation' })).toBeVisible();
    const starts = canonicalApi.requestsFor('POST', `/auth/login/${selected}/start`);
    expect(starts).toHaveLength(1);
    expect(starts[0].headers.origin).toBe('http://127.0.0.1:4173');
    expect(starts[0].headers['content-type']).toContain('application/x-www-form-urlencoded');
    expect(new URLSearchParams(String(starts[0].body)).get('return_to')).toBe('/sessions/prior');
    expect(canonicalApi.requests.some((request) => /identity-operations|active-tenant/.test(request.url))).toBe(false);
  });
}
for (const mode of ['error', 'foreign', 'duplicate'] as const) {
  test(`login metadata ${mode} stays unavailable without default provider`, async ({ canonicalApi, page }) => {
    canonicalApi.auth = 'unauthenticated';
    if (mode === 'error') canonicalApi.loginMetadataError = true;
    if (mode === 'foreign') canonicalApi.loginMetadata = { methods: [{ provider: 'telegram', label: 'Telegram', start_path: 'https://outside.example' }] };
    if (mode === 'duplicate') canonicalApi.loginMetadata = { methods: [
      { provider: 'telegram', label: 'Telegram', start_path: '/auth/login/telegram/start' },
      { provider: 'telegram', label: 'Telegram', start_path: '/auth/login/telegram/start' },
    ] };
    await page.goto('/login');
    await expect(page.getByRole('alert')).toContainText('Sign-in methods are unavailable');
    await expect(page.locator('form')).toHaveCount(0);
    expect(canonicalApi.requests.filter((request) => request.method !== 'GET')).toEqual([]);
  });
}
