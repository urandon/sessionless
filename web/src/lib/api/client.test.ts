import { describe, expect, it, vi } from 'vitest';

import { ApiError, CanonicalApiClient } from './client';

describe('CanonicalApiClient', () => {
  it('uses same-origin credentials and the CSRF header for mutations', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json(
        {
          session_id: 'ses-1',
          status: 'active',
          last_sequence: 0,
          created_at: '2026-08-18T00:00:00Z',
          updated_at: '2026-08-18T00:00:00Z',
        },
        { status: 201 },
      ),
    );
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'csrf value' });

    await client.createSession({ idempotency_key: 'create-1' });

    expect(request).toHaveBeenCalledOnce();
    const [path, options] = request.mock.calls[0] ?? [];
    expect(path).toBe('/api/web/v1/sessions');
    expect(options?.credentials).toBe('same-origin');
    const headers = new Headers(options?.headers);
    expect(headers.get('X-Sessionless-CSRF')).toBe('csrf value');
    expect(headers.get('Content-Type')).toBe('application/json');
  });

  it('does not send a CSRF header on reads', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json({ user_id: 'usr-1', provider: 'telegram', tenants: [] }));
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'csrf' });

    await client.getIdentity();

    const [, options] = request.mock.calls[0] ?? [];
    expect(new Headers(options?.headers).has('X-Sessionless-CSRF')).toBe(false);
  });

  it('fails closed before a mutation when the CSRF cookie is unavailable', async () => {
    const request = vi.fn<typeof fetch>();
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => undefined });

    await expect(client.logout()).rejects.toMatchObject({ code: 'csrf_failed', status: 403 });
    expect(request).not.toHaveBeenCalled();
  });

  it('keeps only the bounded public error envelope', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json(
        {
          error: {
            code: 'conflict',
            message: 'The request conflicts with current state.',
            request_id: 'req-1',
            internal_secret: 'must-not-escape',
          },
        },
        { status: 409 },
      ),
    );
    const client = new CanonicalApiClient({ fetch: request });

    const failure = await client.getIdentity().catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(ApiError);
    expect(failure).toMatchObject({ code: 'conflict', requestId: 'req-1', status: 409 });
    expect(JSON.stringify(failure)).not.toContain('must-not-escape');
  });

  it('replaces malformed or raw server errors with a safe generic error', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response('provider token: secret', { status: 502 }));
    const client = new CanonicalApiClient({ fetch: request });

    await expect(client.getIdentity()).rejects.toMatchObject({
      code: 'temporarily_unavailable',
      message: 'Sessionless is temporarily unavailable. Please try again.',
    });
  });

  it('stops reading an oversized streamed error body at the public envelope limit', async () => {
    const cancel = vi.fn();
    let pulls = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        pulls += 1;
        if (pulls === 1) {
          controller.enqueue(new Uint8Array(64 * 1024 + 1));
        } else {
          controller.enqueue(new TextEncoder().encode('provider-secret-that-must-not-be-read'));
          controller.close();
        }
      },
      cancel,
    });
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(body, { status: 502 }));
    const client = new CanonicalApiClient({ fetch: request });

    await expect(client.getIdentity()).rejects.toMatchObject({
      code: 'temporarily_unavailable',
    });
    expect(cancel).toHaveBeenCalledOnce();
    expect(pulls).toBeLessThan(3);
  });

  it('rejects an oversized success body without buffering it all', async () => {
    const cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array(8 * 1024 * 1024 + 1));
      },
      cancel,
    });
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(body, { status: 200 }));
    const client = new CanonicalApiClient({ fetch: request });

    await expect(client.getIdentity()).rejects.toMatchObject({
      code: 'temporarily_unavailable',
      message: 'Sessionless returned an invalid response. Please try again.',
    });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it('preserves conditional polling metadata without parsing a 304 body', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(null, {
        status: 304,
        headers: { 'X-Sessionless-Poll-After-Ms': '1750' },
      }),
    );
    const client = new CanonicalApiClient({ fetch: request });

    await expect(client.getRun('run/1', '"etag"')).resolves.toEqual({
      state: 'not-modified',
      pollAfterMs: 1750,
    });
    expect(request.mock.calls[0]?.[0]).toBe('/api/web/v1/runs/run%2F1');
    expect(new Headers(request.mock.calls[0]?.[1]?.headers).get('If-None-Match')).toBe('"etag"');
  });

  it('maps fetch failures without retaining the thrown transport detail', async () => {
    const request = vi.fn<typeof fetch>().mockRejectedValue(new Error('signed-url=secret'));
    const client = new CanonicalApiClient({ fetch: request });

    const failure = await client.getIdentity().catch((error: unknown) => error);
    expect(failure).toMatchObject({ code: 'temporarily_unavailable', status: 503 });
    expect(JSON.stringify(failure)).not.toContain('signed-url');
  });
});
