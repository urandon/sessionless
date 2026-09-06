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

    canonicalApi.diagnosticsMode = 'error';
    await page.getByRole('button', { name: 'Try again' }).click();
    await expect(page.getByText('Diagnostics are temporarily unavailable.').first()).toBeVisible();
  });

  test('renders bounded empty and error overview states without polling', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.workerMode = 'empty';
    await page.goto('/workers');
    await expect(page.getByRole('heading', { name: 'No attached workers' })).toBeVisible();
    expect(canonicalApi.requestsFor('GET', '/api/web/v1/attached-workers')).toHaveLength(1);

    canonicalApi.workerMode = 'error';
    await page.reload();
    await expect(
      page.getByRole('heading', { name: 'Attached workers cannot be loaded' }),
    ).toBeVisible();
    await expect(page.getByRole('alert')).toContainText('Sessionless is temporarily unavailable.');
    expect(canonicalApi.requestsFor('GET', '/api/web/v1/attached-workers')).toHaveLength(2);
  });

  test('renders production-reachable baseline, stale, and retained replay evidence', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.diagnosticsMode = 'baseline';
    await page.goto(`/workers/${workerId}`);
    await page.getByRole('button', { name: 'Load redacted diagnostics' }).click();
    await expect(page.getByText('connection_state').locator('..').locator('..')).toContainText(
      'Unknown',
    );
    await expect(page.getByText('attempt_state').locator('..').locator('..')).toContainText('None');
    await expect(page.getByText('capability_missing')).toBeVisible();

    canonicalApi.diagnosticsMode = 'stale';
    await page.reload();
    await page.getByRole('button', { name: 'Load redacted diagnostics' }).click();
    await expect(page.getByText('last_contact').locator('..').locator('..')).toContainText(
      'Freshness: Expired',
    );
    await expect(page.getByText('presence_expired')).toBeVisible();

    canonicalApi.diagnosticsMode = 'replay';
    await page.reload();
    await page.getByRole('button', { name: 'Load redacted diagnostics' }).click();
    await expect(page.getByText('attempt_state').locator('..').locator('..')).toContainText(
      'Retired',
    );
    await expect(page.getByText('worker_terminal').locator('..').locator('..')).toContainText(
      'Received',
    );
    await expect(page.getByText('canonical_terminal').locator('..').locator('..')).toContainText(
      'Committed',
    );
  });
});
