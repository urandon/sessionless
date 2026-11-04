import AxeBuilder from '@axe-core/playwright';
import type { Page, Route } from '@playwright/test';
import type { components } from '../src/lib/api/generated';

import { csrfToken, expect, test } from './fixtures/canonical-api';

type View = components['schemas']['IdentityOperationView'];
type Methods = components['schemas']['AccountLoginMethods'];
const settings = '/account/login-methods';
const methodsPath = '/api/web/v1/account/login-methods';
const operationsPath = '/api/web/v1/account/identity-operations';
const operationId = 'opaque-browser-operation';
const before = '9007199254740993';
const after = '9007199254740994';

// These route-controlled browser tests qualify presentation and request behavior
// only. They do not impersonate genuine BFF, provider or disposable-YDB proof.
class IdentityPresentationFixture {
  readonly calls: {
    method: string;
    path: string;
    body: unknown;
    headers: Record<string, string>;
  }[] = [];
  readonly navigations: string[] = [];
  linked = false;
  phase: View['phase'] = 'source_proof';
  kind: 'link' | 'unlink' = 'link';
  outcome: View['outcome'];
  confirmLost = false;
  unavailable = false;

  methods(): Methods {
    return {
      revision: this.outcome === 'linked' || this.outcome === 'unlinked' ? after : before,
      methods: [
        {
          provider: 'yandex',
          method_handle: 'own-yandex',
          linked: true,
          current: true,
          enabled: true,
          allowed_actions: this.linked ? ['unlink'] : [],
          ...(this.linked ? { remaining_provider: 'telegram' as const } : {}),
        },
        {
          provider: 'telegram',
          linked: this.linked,
          current: false,
          enabled: true,
          allowed_actions: this.linked ? ['unlink'] : ['link'],
          ...(this.linked
            ? { method_handle: 'own-telegram', remaining_provider: 'yandex' as const }
            : {}),
        },
      ],
    };
  }

  view(): View {
    if (this.unavailable)
      return { operation_id: operationId, outcome: 'outcome_unavailable', allowed_actions: [] };
    return {
      operation_id: operationId,
      kind: this.kind,
      phase: this.phase,
      before_revision: before,
      expires_at: '2099-01-01T00:10:00Z',
      ...(this.phase === 'terminal'
        ? {
            outcome: this.outcome,
            completed_at: '2099-01-01T00:05:00Z',
            ...(this.outcome === 'linked' || this.outcome === 'unlinked'
              ? { after_revision: after }
              : {}),
          }
        : { target_provider: 'telegram' as const }),
      allowed_actions:
        this.phase === 'terminal'
          ? []
          : [this.phase === 'confirm' ? 'confirm' : 'continue', 'cancel'],
    };
  }

  async install(page: Page): Promise<void> {
    await page.route('https://oauth.yandex.ru/**', (route) => this.providerNavigation(route));
    await page.route('https://oauth.telegram.org/**', (route) => this.providerNavigation(route));
    await page.route('**/api/web/v1/account/**', async (route) => {
      const request = route.request();
      const path = new URL(request.url()).pathname;
      const method = request.method();
      const body: unknown = request.postData() ? request.postDataJSON() : undefined;
      const headers = request.headers();
      this.calls.push({ method, path, body, headers });
      if (method !== 'GET') {
        expect(headers['x-sessionless-csrf']).toBe(csrfToken);
        expect(headers['content-type']).toContain('application/json');
        expect(headers.origin).toBe('http://127.0.0.1:4173');
      }
      const json = (value: unknown, status = 200) =>
        route.fulfill({
          status,
          contentType: 'application/json',
          headers: { 'Cache-Control': 'no-store' },
          body: JSON.stringify(value),
        });
      if (path === methodsPath && method === 'GET') return json(this.methods());
      if (path === operationsPath && method === 'POST') {
        this.kind = (body as { kind: 'link' | 'unlink' }).kind;
        return json(this.view());
      }
      if (path === `${operationsPath}/${operationId}` && method === 'GET')
        return json(this.view(), this.unavailable ? 410 : 200);
      if (path === `${operationsPath}/${operationId}/continue` && method === 'POST') {
        const yandex = this.phase === 'source_proof';
        const url = new URL(
          yandex ? 'https://oauth.yandex.ru/authorize' : 'https://oauth.telegram.org/auth',
        );
        const values: Record<string, string> = {
          client_id: 'fixture-client',
          redirect_uri: `http://127.0.0.1:4173${yandex ? '/auth/login/callback' : '/auth/telegram/callback'}`,
          response_type: 'code',
          state: 'opaque-provider-state',
          code_challenge: 'A'.repeat(43),
          code_challenge_method: 'S256',
          scope: yandex ? 'login:info' : 'openid profile',
          ...(yandex ? { force_confirm: 'yes' } : { max_age: '0', nonce: 'opaque-provider-nonce' }),
        };
        for (const [key, value] of Object.entries(values)) url.searchParams.set(key, value);
        return json({ authorization_url: url.href });
      }
      if (path === `${operationsPath}/${operationId}/confirm` && method === 'POST') {
        if (this.confirmLost) return route.abort('failed');
        this.phase = 'terminal';
        this.outcome ??= this.kind === 'link' ? 'linked' : 'unlinked';
        if (this.outcome === 'linked') this.linked = true;
        if (this.outcome === 'unlinked') this.linked = false;
        return json(this.view());
      }
      if (path === `${operationsPath}/${operationId}/cancel` && method === 'POST') {
        this.phase = 'terminal';
        this.outcome = 'cancelled';
        return json(this.view());
      }
      throw new Error(`Unexpected controlled identity request ${method} ${path}`);
    });
  }

  async providerNavigation(route: Route): Promise<void> {
    // Fulfill before any DNS/TLS/provider connection. No real OAuth is invoked.
    expect(route.request().headers().referer).toBeUndefined();
    this.navigations.push(new URL(route.request().url()).origin);
    await route.fulfill({
      status: 200,
      contentType: 'text/html',
      body: '<html lang="en"><head><title>Controlled provider</title></head><body><main><h1>Controlled provider check</h1></main></body></html>',
    });
  }

  mutations(path: string): number {
    return this.calls.filter((call) => call.method === 'POST' && call.path === path).length;
  }
}

test.describe('account identity presentation (controlled HTTP, not joined SQL proof)', () => {
  test('link requires explicit source, target and local confirmation; refresh performs GET only', async ({
    page,
    canonicalApi,
  }) => {
    void canonicalApi;
    const fixture = new IdentityPresentationFixture();
    await fixture.install(page);
    await page.goto(settings);
    await expect(page.getByRole('heading', { name: 'Login methods', exact: true })).toBeVisible();
    expect(fixture.calls.every((call) => call.method === 'GET')).toBe(true);
    await page.getByRole('button', { name: 'Start linking Telegram', exact: true }).click();
    await expect(page).toHaveURL(`${settings}?operation_id=${operationId}`);
    await expect(
      page.getByRole('heading', { name: 'Stored operation', exact: true }),
    ).toBeFocused();
    expect(fixture.calls.find((call) => call.path === operationsPath)?.body).toEqual({
      kind: 'link',
      target_provider: 'telegram',
      expected_revision: before,
    });
    await page.reload();
    await expect(page.getByRole('button', { name: 'Continue provider check' })).toBeVisible();
    expect(fixture.mutations(operationsPath)).toBe(1);
    expect(fixture.mutations(`${operationsPath}/${operationId}/continue`)).toBe(0);
    await page.getByRole('button', { name: 'Continue provider check' }).click();
    await expect(page.getByRole('heading', { name: 'Controlled provider check' })).toBeVisible();
    fixture.phase = 'target_proof';
    await page.goto(`${settings}?operation_id=${operationId}`);
    await page.getByRole('button', { name: 'Continue provider check' }).click();
    await expect(page.getByRole('heading', { name: 'Controlled provider check' })).toBeVisible();
    expect(fixture.navigations).toEqual(['https://oauth.yandex.ru', 'https://oauth.telegram.org']);
    fixture.phase = 'confirm';
    await page.goto(`${settings}?operation_id=${operationId}`);
    await expect(page.getByRole('button', { name: 'Confirm link', exact: true })).toBeVisible();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(0);
    await page.getByRole('button', { name: 'Confirm link', exact: true }).click();
    await expect(page.getByText(/sign-in method linked/i)).toBeVisible();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(1);
    await page.reload();
    await expect(page.getByText(/sign-in method linked/i)).toBeVisible();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(1);
    const stored = await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }));
    expect(stored.local).toEqual([]);
    // Pinned SvelteKit stores its own scroll/snapshot bookkeeping on navigation.
    // It must not persist this feature's private DTOs, selectors or OAuth data.
    for (const [key] of stored.session)
      expect(['sveltekit:scroll', 'sveltekit:snapshot']).toContain(key);
    for (const forbidden of [
      operationId,
      before,
      after,
      'own-yandex',
      'own-telegram',
      'opaque-provider-',
      'authorization_url',
      'code_challenge',
      'client_id',
    ]) {
      expect(JSON.stringify(stored)).not.toContain(forbidden);
    }
  });

  test('unlink names the server remaining method and cancel never confirms', async ({
    page,
    canonicalApi,
  }) => {
    void canonicalApi;
    const fixture = new IdentityPresentationFixture();
    fixture.linked = true;
    await fixture.install(page);
    await page.goto(settings);
    await page.getByRole('button', { name: 'Start unlinking Telegram', exact: true }).click();
    expect(fixture.calls.find((call) => call.path === operationsPath)?.body).toEqual({
      kind: 'unlink',
      method_handle: 'own-telegram',
      expected_revision: before,
    });
    await expect(page.getByText(/Keep signing in with Yandex\./)).toBeVisible();
    await expect(
      page.getByText('Other Web sessions will need sign-in again.', { exact: false }).first(),
    ).toBeVisible();
    await page.getByRole('button', { name: 'Cancel operation' }).click();
    await expect(page.getByText(/cancelled/i)).toBeVisible();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(0);
    expect(fixture.linked).toBe(true);
  });

  test('lost confirmation is unknown, refresh does not resend, and 410 is not success', async ({
    page,
    canonicalApi,
  }) => {
    void canonicalApi;
    const fixture = new IdentityPresentationFixture();
    fixture.phase = 'confirm';
    fixture.confirmLost = true;
    await fixture.install(page);
    await page.goto(`${settings}?operation_id=${operationId}`);
    await page.getByRole('button', { name: 'Confirm link', exact: true }).click();
    await expect(page.getByRole('alert')).toBeFocused();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(1);
    const returnTo = `${settings}?operation_id=${operationId}`;
    const recovery = page.getByRole('link', { name: 'Sign in again', exact: true });
    await expect(recovery).toHaveAttribute(
      'href',
      `/login?return_to=${encodeURIComponent(returnTo)}`,
    );
    await recovery.click();
    await expect(page.getByRole('button', { name: 'Continue with Yandex' })).toBeVisible();
    await expect(page.locator('form input[name="return_to"]')).toHaveValue(returnTo);
    // Re-entry is simulated; no claim that a genuine OAuth login happened.
    await page.goto(returnTo);
    await expect(
      page.getByRole('heading', { name: 'Stored operation', exact: true }),
    ).toBeVisible();
    expect(fixture.mutations(`${operationsPath}/${operationId}/confirm`)).toBe(1);
    fixture.unavailable = true;
    await page.reload();
    await expect(page.getByText(/No retained outcome is available/)).toBeVisible();
    await expect(page.getByRole('button', { name: 'Confirm link', exact: true })).toHaveCount(0);
    await expect(page.getByText(/sign-in method linked/i)).toHaveCount(0);
    expect(fixture.calls.filter((call) => call.method === 'POST')).toHaveLength(1);
  });

  test('invalid or duplicate local selectors make no account API calls', async ({
    page,
    canonicalApi,
  }) => {
    void canonicalApi;
    const fixture = new IdentityPresentationFixture();
    await fixture.install(page);
    for (const query of [
      'operation_id=a&operation_id=b',
      'operation_id=',
      'operation_id=a&code=private',
      'operation_id=%2Fforeign',
      'operation_id=valid#code=private',
      'operation_id=valid#state=private',
    ]) {
      await page.goto(`${settings}?${query}`);
      await expect(
        page.getByRole('heading', { name: 'Invalid operation reference' }),
      ).toBeVisible();
    }
    expect(fixture.calls).toHaveLength(0);
  });

  test('@a11y account methods and confirmation remain keyboard usable at 320px and 200% text', async ({
    page,
    canonicalApi,
  }, testInfo) => {
    void canonicalApi;
    const fixture = new IdentityPresentationFixture();
    fixture.phase = 'confirm';
    await fixture.install(page);
    await page.setViewportSize({ width: 320, height: 844 });
    await page.goto(`${settings}?operation_id=${operationId}`);
    await expect(page.getByRole('button', { name: 'Confirm link', exact: true })).toBeVisible();
    await page.addStyleTag({ content: 'html { font-size: 200%; }' });
    const results = await new AxeBuilder({ page }).analyze();
    expect(
      results.violations.filter((item) => item.impact === 'serious' || item.impact === 'critical'),
    ).toEqual([]);
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    ).toBe(true);
    await page.screenshot({
      path: testInfo.outputPath('account-320px-200percent-text.png'),
      fullPage: true,
    });
    const confirm = page.getByRole('button', { name: 'Confirm link', exact: true });
    await page.getByRole('heading', { name: 'Login methods', exact: true }).focus();
    for (let step = 0; step < 16; step += 1) {
      await page.keyboard.press('Tab');
      if (await confirm.evaluate((element) => element === element.ownerDocument.activeElement))
        break;
    }
    await expect(confirm).toBeFocused();
    await expect(confirm).toHaveCSS('outline-style', 'solid');
    await page.keyboard.press('Enter');
    await expect(page.getByText(/sign-in method linked/i)).toBeVisible();
  });
});
