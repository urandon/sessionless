import { describe, expect, it, vi } from 'vitest';

import { ApiError, CanonicalApiClient } from './client';
import { barrier, explanation } from '../run-explanation/test-fixtures';

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

  it('reads bounded attached-worker pages without mutation headers or polling', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(
      Response.json({
        version: 1,
        evaluated_at: '2026-08-26T08:00:00Z',
        items: [],
        has_more: false,
      }),
    );
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'csrf' });

    await client.listAttachedWorkers({ afterWorkerId: 'worker:one', limit: 20 });

    expect(request.mock.calls[0]?.[0]).toBe(
      '/api/web/v1/attached-workers?after_worker_id=worker%3Aone&limit=20',
    );
    const options = request.mock.calls[0]?.[1];
    expect(options?.method).toBeUndefined();
    expect(new Headers(options?.headers).has('X-Sessionless-CSRF')).toBe(false);
    expect(new Headers(options?.headers).has('If-None-Match')).toBe(false);
  });

  it('escapes attached-worker selectors for detail and diagnostics reads', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ version: 1 }))
      .mockResolvedValueOnce(Response.json({ version: 1 }));
    const client = new CanonicalApiClient({ fetch: request });

    await client.getAttachedWorker('worker:one');
    await client.getAttachedWorkerDiagnostics('worker:one');

    expect(request.mock.calls[0]?.[0]).toBe('/api/web/v1/attached-workers/worker%3Aone');
    expect(request.mock.calls[1]?.[0]).toBe(
      '/api/web/v1/attached-workers/worker%3Aone/diagnostics',
    );
    for (const [, options] of request.mock.calls) {
      expect(new Headers(options?.headers).has('X-Sessionless-CSRF')).toBe(false);
    }
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

describe('Run explanation transport', () => {
  it.each(['9007199254740991.1', '1e0', '18446744073709551615', '-0'])(
    'rejects inexact/noncanonical integer token %s before numeric rounding',
    async (token) => {
      const raw = JSON.stringify(explanation()).replace(
        '"decision_revision":1',
        `"decision_revision":${token}`,
      );
      const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(raw));
      await expect(
        new CanonicalApiClient({ fetch: request }).getRunExplanation('run-1', 'ses-1'),
      ).rejects.toMatchObject({ code: 'temporarily_unavailable' });
    },
  );

  it('honors an HTTP-date Retry-After using the injected time authority', async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(
      new Response('private-provider-secret', {
        status: 429,
        headers: { 'Retry-After': 'Sat, 10 Oct 2026 12:00:12 GMT' },
      }),
    );
    const client = new CanonicalApiClient({
      fetch: request,
      now: () => Date.parse('2026-10-10T12:00:00Z'),
    });
    const error = await client
      .getRunExplanation('run-1', 'ses-1')
      .catch((failure: unknown) => failure);
    expect(error).toMatchObject({ retryAfterMs: 12000 });
    expect(JSON.stringify(error)).not.toContain('private-provider-secret');
  });

  it('maps transport detail to a content-free error and does not fetch an already aborted read', async () => {
    const request = vi.fn<typeof fetch>().mockRejectedValue(new Error('secret-token'));
    const client = new CanonicalApiClient({ fetch: request });
    const error = await client
      .getRunExplanation('run-1', 'ses-1')
      .catch((failure: unknown) => failure);
    expect(error).toMatchObject({ status: 503, code: 'temporarily_unavailable' });
    expect(JSON.stringify(error)).not.toContain('secret-token');
    const abort = new AbortController();
    abort.abort();
    await expect(client.getRunExplanation('run-1', 'ses-1', abort.signal)).rejects.toMatchObject({
      name: 'AbortError',
    });
    expect(request).toHaveBeenCalledOnce();
  });

  it('uses selector-only no-store GET and local exact Session/Run correlation', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(Response.json(explanation(), { headers: { ETag: 'ignored' } }));
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'must-not-send' });
    await expect(client.getRunExplanation('run-1', 'ses-1')).resolves.toEqual(explanation());
    expect(request.mock.calls[0]?.[0]).toBe('/api/web/v1/runs/run-1/explanation');
    const options = request.mock.calls[0]?.[1];
    expect(options).toMatchObject({
      method: 'GET',
      cache: 'no-store',
      credentials: 'same-origin',
      referrerPolicy: 'no-referrer',
    });
    expect(options?.body).toBeUndefined();
    expect([...new Headers(options?.headers).keys()]).toEqual(['accept']);
  });

  it.each([401, 403, 404, 429, 503, 304])(
    'rejects status %i without reading or keeping its body',
    async (status) => {
      const cancel = vi.fn();
      const body = status === 304 ? null : new ReadableStream<Uint8Array>({ cancel });
      const request = vi
        .fn<typeof fetch>()
        .mockResolvedValue(new Response(body, { status, headers: { 'Retry-After': '12' } }));
      const failure = await new CanonicalApiClient({ fetch: request })
        .getRunExplanation('run-1', 'ses-1')
        .catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(ApiError);
      expect(failure).toMatchObject({
        status,
        retryAfterMs: 12000,
        message: 'Run evidence is unavailable. Please try again.',
      });
      if (body) expect(cancel).toHaveBeenCalledOnce();
    },
  );

  it.each(['-1', 'Infinity', 'NaN', '1e309', '999999999999999999999', '1.5'])(
    'ignores non-finite/invalid Retry-After %s',
    async (retry) => {
      const request = vi
        .fn<typeof fetch>()
        .mockResolvedValue(new Response(null, { status: 429, headers: { 'Retry-After': retry } }));
      await expect(
        new CanonicalApiClient({ fetch: request }).getRunExplanation('run-1', 'ses-1'),
      ).rejects.toMatchObject({ retryAfterMs: undefined });
    },
  );

  it('stops a streamed explanation body at 16 KiB without relying on Content-Length', async () => {
    const cancel = vi.fn();
    let pulls = 0;
    const body = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          pulls++;
          controller.enqueue(new Uint8Array(4096));
        },
        cancel,
      },
      { highWaterMark: 0 },
    );
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(body));
    await expect(
      new CanonicalApiClient({ fetch: request }).getRunExplanation('run-1', 'ses-1'),
    ).rejects.toMatchObject({ code: 'temporarily_unavailable' });
    expect(pulls).toBe(5);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it('rejects Content-Length over 16 KiB before reading', async () => {
    const pull = vi.fn(),
      cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>({ pull, cancel }, { highWaterMark: 0 });
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response(body, { headers: { 'Content-Length': '16385' } }));
    await expect(
      new CanonicalApiClient({ fetch: request }).getRunExplanation('run-1', 'ses-1'),
    ).rejects.toBeInstanceOf(ApiError);
    expect(pull).not.toHaveBeenCalled();
    expect(cancel).toHaveBeenCalledOnce();
  });

  it('accepts the exact 16 KiB body ceiling', async () => {
    const raw = JSON.stringify(explanation());
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response(raw + ' '.repeat(16384 - raw.length)));
    await expect(
      new CanonicalApiClient({ fetch: request }).getRunExplanation('run-1', 'ses-1'),
    ).resolves.toEqual(explanation());
  });

  it('aborts during a blocked body read and cancels the stream', async () => {
    const started = barrier<void>(),
      cancel = vi.fn();
    const body = new ReadableStream<Uint8Array>(
      {
        pull() {
          started.resolve();
        },
        cancel,
      },
      { highWaterMark: 0 },
    );
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(body));
    const abort = new AbortController();
    const read = new CanonicalApiClient({ fetch: request }).getRunExplanation(
      'run-1',
      'ses-1',
      abort.signal,
    );
    await started.promise;
    abort.abort();
    await expect(read).rejects.toMatchObject({ name: 'AbortError' });
    expect(cancel).toHaveBeenCalledOnce();
  });

  it('does not restore a success when fetch ignores its aborted signal', async () => {
    const pending = barrier<Response>(),
      request = vi.fn<typeof fetch>().mockReturnValue(pending.promise);
    const abort = new AbortController();
    const read = new CanonicalApiClient({ fetch: request }).getRunExplanation(
      'run-1',
      'ses-1',
      abort.signal,
    );
    abort.abort();
    pending.resolve(Response.json(explanation()));
    await expect(read).rejects.toMatchObject({ name: 'AbortError' });
  });

  it.each([
    [
      'private envelope field',
      (v: Record<string, unknown>) => {
        v.owner_user_id = 'secret';
      },
    ],
    [
      'private nested field',
      (v: Record<string, unknown>) => {
        (v.attached as Record<string, unknown>).worker_id = 'secret';
      },
    ],
    [
      'foreign Run',
      (v: Record<string, unknown>) => {
        v.run_id = 'run-other';
      },
    ],
    [
      'foreign Session',
      (v: Record<string, unknown>) => {
        v.session_id = 'ses-other';
      },
    ],
    [
      'unknown variant fields',
      (v: Record<string, unknown>) => {
        v.admission = { availability: 'unknown', reason_code: 'admitted' };
      },
    ],
    [
      'unsafe revision',
      (v: Record<string, unknown>) => {
        (v.admission as Record<string, unknown>).decision_revision = Number.MAX_SAFE_INTEGER + 1;
      },
    ],
    [
      'wrong coverage',
      (v: Record<string, unknown>) => {
        (v.coverage as Record<string, unknown>).operational = 'unknown';
      },
    ],
    [
      'invented freshness',
      (v: Record<string, unknown>) => {
        (v.attached as Record<string, unknown>).freshness = 'within_lease';
      },
    ],
    [
      'invalid enum',
      (v: Record<string, unknown>) => {
        (v.admission as Record<string, unknown>).reason_code = 'raw-provider-cause';
      },
    ],
    [
      'invalid UTC',
      (v: Record<string, unknown>) => {
        v.read_at = '2026-10-10T12:00:10+03:00';
      },
    ],
    [
      'invalid calendar',
      (v: Record<string, unknown>) => {
        v.read_at = '2026-02-30T12:00:10Z';
      },
    ],
  ])('fails closed for %s without retaining body values', async (_name, mutate) => {
    const v = explanation() as unknown as Record<string, unknown>;
    mutate(v);
    const request = vi.fn<typeof fetch>().mockResolvedValue(Response.json(v));
    const error = await new CanonicalApiClient({ fetch: request })
      .getRunExplanation('run-1', 'ses-1')
      .catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(ApiError);
    expect(JSON.stringify(error)).not.toContain('secret');
    expect(JSON.stringify(error)).not.toContain('raw-provider-cause');
  });
});
