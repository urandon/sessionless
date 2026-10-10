import AxeBuilder from '@axe-core/playwright';
import type { Page } from '@playwright/test';

import { explanation } from '../src/lib/run-explanation/test-fixtures';
import { expect, sessionId, test } from './fixtures/canonical-api';

async function installRun(page: Page): Promise<void> {
  await page.route(`**/api/web/v1/sessions/${sessionId}/events*`, (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        items: [
          {
            event_id: 'event-run',
            sequence: 1,
            run_id: 'run-1',
            kind: 'user_message',
            content: { text: 'Explain this execution' },
            created_at: '2026-10-10T12:00:00Z',
          },
        ],
      }),
      headers: { 'X-Sessionless-Poll-After-Ms': '15000' },
    }),
  );
  await page.route(`**/api/web/v1/sessions/${sessionId}/runs*`, (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ items: [] }),
      headers: { 'X-Sessionless-Poll-After-Ms': '15000' },
    }),
  );
  await page.route('**/api/web/v1/runs/run-1/explanation', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ ...explanation(), session_id: sessionId }),
      headers: { 'Cache-Control': 'no-store' },
    }),
  );
}

test('explanation uses explicit canonical event correlation without a loaded Run; keyboard preserves draft and focus', async ({
  canonicalApi,
  page,
}, info) => {
  await installRun(page);
  await page.goto(`/sessions/${sessionId}`);
  const composer = page.getByRole('textbox', { name: 'Message' });
  await composer.fill('Keep this unsent draft');
  const trigger = page.getByRole('button', { name: 'Explain run' });
  await expect(trigger).toBeEnabled();
  const computeReads = canonicalApi.requestsFor(
    'GET',
    `/api/web/v1/sessions/${sessionId}/compute`,
  ).length;
  const identityReads = canonicalApi.requestsFor('GET', '/api/web/v1/me').length;
  await trigger.focus();
  await page.keyboard.press('Enter');
  const panel = page.getByRole('complementary', { name: 'Run explanation' });
  const title = panel.getByRole('heading', { name: 'Run explanation', exact: true });
  await expect(title).toBeFocused();
  await expect(
    panel.getByRole('heading', { name: 'Last recorded admission decision' }),
  ).toBeVisible();
  await expect(panel.getByText('Expired at the server read time', { exact: true })).toBeVisible();
  expect(canonicalApi.requestsFor('GET', `/api/web/v1/sessions/${sessionId}/compute`)).toHaveLength(
    computeReads,
  );
  expect(canonicalApi.requestsFor('GET', '/api/web/v1/me')).toHaveLength(identityReads);
  expect(canonicalApi.requests.some((request) => request.method !== 'GET')).toBe(false);
  await panel.screenshot({ path: info.outputPath('drawer-recorded.png') });
  await page.keyboard.press('Escape');
  await expect(panel).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await expect(composer).toHaveValue('Keep this unsent draft');
});

test('@a11y explanation unknown coverage is usable at 320px and 200% zoom, without modal containment or hover', async ({
  canonicalApi,
  page,
}, info) => {
  void canonicalApi;
  await installRun(page);
  await page.route('**/api/web/v1/runs/run-1/explanation', (route) =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        ...explanation(),
        session_id: sessionId,
        admission: { availability: 'unknown' },
        terminal: { availability: 'not_applicable' },
        attempt: { availability: 'unknown' },
        attached: { availability: 'unknown' },
        coverage: {
          admission: 'unknown',
          terminal: 'not_applicable',
          attempt: 'unknown',
          operational: 'unknown',
        },
      }),
    }),
  );
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.setViewportSize({ width: 320, height: 800 });
  await page.goto(`/sessions/${sessionId}`);
  await page.getByRole('button', { name: 'Explain run' }).click();
  const panel = page.getByRole('complementary', { name: 'Run explanation' });
  await expect(panel.getByText(/Partial evidence:/)).toBeVisible();
  await expect(panel.getByRole('region', { name: 'Attached receipt observation' })).toContainText(
    'Managed remote-process observation is unknown',
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= document.documentElement.clientWidth,
    ),
  ).toBe(true);
  const results = await new AxeBuilder({ page }).analyze();
  expect(
    results.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical'),
    JSON.stringify(results.violations),
  ).toEqual([]);
  await panel.screenshot({ path: info.outputPath('drawer-320.png') });
  await page.setViewportSize({ width: 640, height: 900 });
  await page.evaluate(() => {
    document.body.style.zoom = '2';
  });
  await expect(panel.getByRole('button', { name: 'Close explanation' })).toBeVisible();
  await expect(panel.getByRole('button', { name: 'Refresh explanation' })).toBeEnabled();
  await panel.getByRole('button', { name: 'Refresh explanation' }).click();
  await expect(panel.getByRole('button', { name: 'Refresh explanation' })).toBeFocused();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= document.documentElement.clientWidth,
    ),
  ).toBe(true);
  // It is a nonmodal panel: moving directly to the primary composer is permitted.
  await page.getByRole('textbox', { name: 'Message' }).focus();
  await expect(page.getByRole('textbox', { name: 'Message' })).toBeFocused();
  await panel.screenshot({ path: info.outputPath('drawer-200-percent.png') });
  await panel.getByRole('button', { name: 'Close explanation' }).click();
  await expect(page.getByRole('button', { name: 'Explain run' })).toBeFocused();
});

test('opaque explanation denial clears evidence without treating read-only compute denial as lost transcript access', async ({
  canonicalApi,
  page,
}) => {
  void canonicalApi;
  await installRun(page);
  await page.route(`**/api/web/v1/sessions/${sessionId}/compute`, (route) =>
    route.fulfill({
      status: 404,
      contentType: 'application/json',
      body: JSON.stringify({
        error: { code: 'not_found', message: 'Unavailable', request_id: 'fixture-compute-denied' },
      }),
    }),
  );
  await page.route('**/api/web/v1/runs/run-1/explanation', (route) =>
    route.fulfill({
      status: 404,
      contentType: 'application/json',
      body: JSON.stringify({
        error: {
          code: 'not_found',
          message: 'Unavailable',
          request_id: 'fixture-explanation-denied',
        },
      }),
    }),
  );
  await page.goto(`/sessions/${sessionId}`);
  const trigger = page.getByRole('button', { name: 'Explain run' });
  await trigger.click();
  const panel = page.getByRole('complementary', { name: 'Run explanation' });
  await expect(panel.locator('.read-warning')).toContainText(
    'Previously read evidence was removed',
  );
  await expect(panel.getByRole('heading', { name: 'Canonical Run', exact: true })).toHaveCount(0);
  await expect(trigger).toBeDisabled();
  await expect(page.getByText('Explain this execution', { exact: true })).toBeVisible();
  await expect(page.getByRole('textbox', { name: 'Message' })).toBeDisabled();
});
