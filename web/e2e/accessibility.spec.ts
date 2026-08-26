import AxeBuilder from '@axe-core/playwright';
import type { Page } from '@playwright/test';

import { expect, sessionId, test, workerId } from './fixtures/canonical-api';

async function expectNoSeriousOrCriticalViolations(page: Page): Promise<void> {
  const results = await new AxeBuilder({ page }).analyze();
  const blocking = results.violations.filter(
    (violation) => violation.impact === 'serious' || violation.impact === 'critical',
  );
  expect(blocking, JSON.stringify(blocking, undefined, 2)).toEqual([]);
}

test.describe('accessible states', () => {
  test('@a11y dashboard, login, and access-denied states pass axe', async ({
    canonicalApi,
    page,
  }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);

    await page.goto('/login');
    await expect(page.getByRole('heading', { name: 'Sign in to Sessionless' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);

    canonicalApi.auth = 'access-denied';
    await page.goto('/');
    await expect(
      page.getByRole('heading', { name: 'No workspace is linked to this account' }),
    ).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);
  });

  test('@a11y session, empty, and error states pass axe', async ({ canonicalApi, page }) => {
    await page.goto(`/sessions/${sessionId}`);
    await expect(page.getByRole('heading', { name: 'Launch planning' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);

    canonicalApi.emptySessions = true;
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'No active sessions' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);

    canonicalApi.auth = 'error';
    await page.goto('/');
    await expect(
      page.getByRole('heading', { name: 'Your workspace is temporarily unavailable' }),
    ).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);
  });

  test('@a11y loading state is announced and passes axe', async ({ canonicalApi, page }) => {
    void canonicalApi;
    let releaseIdentity: () => void = () => undefined;
    const identityReleased = new Promise<void>((resolve) => {
      releaseIdentity = resolve;
    });
    await page.route('**/api/web/v1/me', async (route) => {
      await identityReleased;
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ user_id: 'user-one', provider: 'telegram', tenants: [] }),
      });
    });

    await page.goto('/');
    const loading = page.getByText('Loading your workspace…');
    await expect(loading).toBeVisible();
    await expect(loading.locator('..')).toHaveAttribute('aria-busy', 'true');
    await expectNoSeriousOrCriticalViolations(page);
    releaseIdentity();
  });

  test('@a11y attached-worker detail and explicit diagnostics pass axe', async ({
    canonicalApi,
    page,
  }) => {
    void canonicalApi;
    await page.goto(`/workers/${workerId}`);
    await expect(page.getByRole('heading', { name: 'Studio Mac' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);

    const load = page.getByRole('button', { name: 'Load redacted diagnostics' });
    await load.focus();
    await page.keyboard.press('Enter');
    await expect(page.getByLabel('Redacted diagnostic JSON')).toBeVisible();
    await expect(page.getByRole('region', { name: 'Loaded redacted diagnostics' })).toBeFocused();
    await page.keyboard.press('Tab');
    await expect(page.getByRole('region', { name: 'Identity diagnostic facts' })).toBeFocused();
    for (let index = 0; index < 5; index += 1) await page.keyboard.press('Tab');
    await expect(page.getByRole('region', { name: 'Governance diagnostic facts' })).toBeFocused();
    await page.keyboard.press('Tab');
    await expect(page.getByLabel('Redacted diagnostic JSON')).toBeFocused();
    await page.keyboard.press('Tab');
    await expect(page.getByRole('button', { name: 'Copy redacted diagnostics' })).toBeFocused();
    await expect(page.getByRole('region', { name: 'Execution diagnostic facts' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);
  });

  test('@a11y keyboard path exposes focus, labels, and the composer', async ({
    canonicalApi,
    page,
  }) => {
    void canonicalApi;
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();

    await page.keyboard.press('Tab');
    await expect(page.getByRole('link', { name: 'Skip to content' })).toBeFocused();
    await page.keyboard.press('Enter');
    await expect(page.locator('main')).toBeFocused();
    await expect(page.getByLabel('Workspace')).toBeVisible();

    await page.goto(`/sessions/${sessionId}`);
    await expect(page.getByRole('textbox', { name: 'Message' })).toBeVisible();
    await expect(page.getByLabel('Attach files')).toBeVisible();
    await expect(page.getByRole('form', { name: 'Message composer' })).toBeVisible();
    await expectNoSeriousOrCriticalViolations(page);
  });
});
