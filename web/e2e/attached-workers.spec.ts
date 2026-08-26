import { expect, test, workerId } from './fixtures/canonical-api';

test.describe('attached-worker diagnostics', () => {
  test('loads and copies only after explicit owner action without mutation or polling', async ({
    canonicalApi,
    page,
  }) => {
    await page.addInitScript(() => {
      Object.defineProperty(navigator, 'clipboard', {
        configurable: true,
        value: {
          writeText(value: string) {
            (window as typeof window & { __copied?: string }).__copied = value;
            return Promise.resolve();
          },
        },
      });
    });

    await page.goto('/workers');
    await page.getByRole('link', { name: /Studio Mac/ }).click();
    await expect(page.getByRole('heading', { name: 'Studio Mac' })).toBeVisible();
    expect(
      canonicalApi.requestsFor('GET', `/api/web/v1/attached-workers/${workerId}/diagnostics`),
    ).toHaveLength(0);

    await page.getByRole('button', { name: 'Load redacted diagnostics' }).click();
    const report = page.getByLabel('Redacted diagnostic JSON');
    await expect(report).toBeVisible();
    await expect(page.getByText('canonical_terminal')).toBeVisible();
    await page.getByRole('button', { name: 'Copy redacted diagnostics' }).click();
    await expect(page.getByRole('status')).toContainText('copied');
    const copied = await page.evaluate(
      () => (window as typeof window & { __copied?: string }).__copied ?? '',
    );
    expect(copied).toBe(await report.inputValue());
    expect(JSON.parse(copied)).toMatchObject({ version: 1, worker_id: workerId });
    expect(copied).not.toContain('public-fingerprint');

    expect(
      canonicalApi.requestsFor('GET', `/api/web/v1/attached-workers/${workerId}/diagnostics`),
    ).toHaveLength(1);
    expect(
      canonicalApi.requests.filter(
        (request) =>
          new URL(request.url).pathname.includes('/attached-workers') && request.method !== 'GET',
      ),
    ).toEqual([]);
  });

  test('collapses unavailable diagnostics and sanitizes selector mismatch', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.diagnosticsMode = 'not-found';
    await page.goto(`/workers/${workerId}`);
    await page.getByRole('button', { name: 'Load redacted diagnostics' }).click();
    await expect(
      page.getByText('Diagnostics are unavailable for this worker in the active workspace.'),
    ).toBeVisible();

    canonicalApi.diagnosticsMode = 'mismatch';
    await page.getByRole('button', { name: 'Try again' }).click();
    await expect(page.getByText('Diagnostics are temporarily unavailable.').first()).toBeVisible();
    await expect(page.getByText('worker-other')).toHaveCount(0);
  });
});
