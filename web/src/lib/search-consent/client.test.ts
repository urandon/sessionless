import { describe, expect, it, vi } from 'vitest';
import { CanonicalApiClient } from '../api/client';

const locator = {
  sessionId: 'ses-1',
  runId: 'run-1',
  attemptId: 'attempt-1',
  requestId: 'request-1',
};
const summary = {
  request_id: 'request-1',
  revision: 1,
  status: 'pending',
  expires_at: '2026-10-20T01:00:00Z',
};

describe('private consent HTTP client', () => {
  it('uses closed same-origin no-store routes and exact CSRF mutation body', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ session_id: 'ses-1', run_id: 'run-1' }))
      .mockResolvedValueOnce(Response.json({ ...summary, query: 'exact query' }))
      .mockResolvedValueOnce(Response.json({ ...summary, status: 'approved', revision: 2 }));
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'csrf' });
    await client.discoverSearchConsent('ses-1', 'run-1');
    await client.getSearchConsent(locator);
    await client.decideSearchConsent(locator, 'approve');
    expect(request.mock.calls[0]?.[0]).toBe(
      '/api/web/v1/sessions/ses-1/runs/run-1/search-consents/current',
    );
    expect(request.mock.calls[1]?.[0]).toBe(
      '/api/web/v1/sessions/ses-1/runs/run-1/attempts/attempt-1/search-consents/request-1',
    );
    for (const [, options] of request.mock.calls) {
      expect(options).toMatchObject({
        cache: 'no-store',
        credentials: 'same-origin',
        referrerPolicy: 'no-referrer',
        redirect: 'error',
      });
      expect(new Headers(options?.headers).has('Authorization')).toBe(false);
      expect(new Headers(options?.headers).has('If-None-Match')).toBe(false);
    }
    expect(new Headers(request.mock.calls[0]?.[1]?.headers).has('X-Sessionless-CSRF')).toBe(false);
    expect(request.mock.calls[2]?.[1]?.body).toBe('{"expected_revision":1,"choice":"approve"}');
    expect(new Headers(request.mock.calls[2]?.[1]?.headers).get('X-Sessionless-CSRF')).toBe('csrf');
  });

  it('does not send without CSRF or after caller cancellation', async () => {
    const request = vi.fn<typeof fetch>();
    const token = vi.fn().mockReturnValue(undefined);
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: token });
    await expect(client.decideSearchConsent(locator, 'approve')).rejects.toMatchObject({
      code: 'csrf_failed',
      status: 403,
    });
    const abort = new AbortController();
    abort.abort();
    token.mockClear();
    await expect(
      client.decideSearchConsent(locator, 'approve', abort.signal),
    ).rejects.toMatchObject({ name: 'AbortError' });
    expect(token).not.toHaveBeenCalled();
    expect(request).not.toHaveBeenCalled();
  });

  it('rejects overlarge, foreign, private-metadata and noncanonical responses', async () => {
    for (const value of [
      { ...summary, query: 'x'.repeat(9000) },
      { ...summary, query: 'private', request_id: 'foreign' },
      { ...summary, query: 'private', receipt: 'secret' },
    ]) {
      const client = new CanonicalApiClient({
        fetch: vi.fn().mockResolvedValue(Response.json(value)),
      });
      await expect(client.getSearchConsent(locator)).rejects.toThrow();
    }
    const client = new CanonicalApiClient({
      fetch: vi
        .fn()
        .mockResolvedValue(new Response('secret query and provider error', { status: 503 })),
    });
    await expect(client.getSearchConsent(locator)).rejects.toMatchObject({
      message: 'Search consent is unavailable.',
      status: 503,
    });
  });

  it('cancels a stalled request at its finite transport deadline', async () => {
    vi.useFakeTimers();
    try {
      const request = vi.fn<typeof fetch>().mockImplementation(
        (_url, options) =>
          new Promise((_resolve, reject) => {
            options?.signal?.addEventListener(
              'abort',
              () => reject(new DOMException('timeout', 'AbortError')),
              { once: true },
            );
          }),
      );
      const client = new CanonicalApiClient({ fetch: request });
      const result = expect(client.getSearchConsent(locator)).rejects.toMatchObject({
        status: 503,
        message: 'Search consent is unavailable.',
      });
      await vi.advanceTimersByTimeAsync(10000);
      await result;
      expect(request.mock.calls[0]?.[1]?.signal?.aborted).toBe(true);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
