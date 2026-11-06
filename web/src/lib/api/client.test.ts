import { describe, expect, it, vi } from 'vitest';

import { ApiError, CanonicalApiClient, parseLoginMethods } from './client';
import { barrier, explanation } from '../run-explanation/test-fixtures';

describe('CanonicalApiClient account identity methods', () => {
  it('retains a safe server request reference through account error handling', async () => {
    const client = new CanonicalApiClient({
      fetch: vi.fn<typeof fetch>().mockResolvedValue(
        Response.json(
          {
            error: {
              code: 'temporarily_unavailable',
              message: 'private upstream detail',
              request_id: 'req_synthetic244',
            },
          },
          { status: 503 },
        ),
      ),
      readCSRFToken: () => 'synthetic-csrf',
    });
    const error = await client
      .continueIdentityOperation('op_synthetic', '1')
      .catch((cause: unknown) => cause);
    expect(error).toMatchObject({
      code: 'temporarily_unavailable',
      requestId: 'req_synthetic244',
      status: 503,
    });
    expect((error as Error).message).not.toContain('private upstream');
  });
  const revision = '9007199254740993';
  const operationId = 'opaque:operation-1';
  const rows = {
    revision,
    methods: [
      {
        provider: 'yandex',
        linked: true,
        current: true,
        enabled: true,
        method_handle: 'own-yandex',
        allowed_actions: [],
      },
      {
        provider: 'telegram',
        linked: false,
        current: false,
        enabled: true,
        allowed_actions: ['link'],
      },
    ],
  };
  const pending = {
    operation_id: operationId,
    kind: 'link',
    phase: 'target_proof',
    target_provider: 'telegram',
    before_revision: revision,
    expires_at: '2026-11-04T12:10:00Z',
    allowed_actions: ['continue', 'cancel'],
  };
  const terminal = {
    operation_id: operationId,
    kind: 'link',
    phase: 'terminal',
    before_revision: revision,
    after_revision: '9007199254740994',
    outcome: 'linked',
    expires_at: pending.expires_at,
    completed_at: '2026-11-04T12:01:00Z',
    allowed_actions: [],
  };
  function continuation(): string {
    const params = new URLSearchParams({
      client_id: '100000',
      redirect_uri: `${globalThis.location.origin}/auth/telegram/callback`,
      response_type: 'code',
      state: 'state',
      code_challenge: 'A'.repeat(43),
      code_challenge_method: 'S256',
      scope: 'openid profile',
      max_age: '0',
      nonce: 'nonce',
    });
    return `https://oauth.telegram.org/auth?${params}`;
  }
  it('uses canonical no-store reads, no mutation CSRF, and exact owned outcome binding', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(rows))
      .mockResolvedValueOnce(Response.json(pending));
    const csrf = vi.fn();
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: csrf });
    expect(await client.getAccountLoginMethods()).toEqual(rows);
    expect(await client.getIdentityOperationOutcome(operationId)).toEqual(pending);
    expect(request.mock.calls.map(([path]) => path)).toEqual([
      '/api/web/v1/account/login-methods',
      '/api/web/v1/account/identity-operations/opaque%3Aoperation-1',
    ]);
    expect(csrf).not.toHaveBeenCalled();
    for (const [, options] of request.mock.calls) {
      expect(options).toMatchObject({
        method: 'GET',
        cache: 'no-store',
        redirect: 'error',
        credentials: 'same-origin',
        referrerPolicy: 'no-referrer',
      });
      expect(new Headers(options?.headers).has('X-Sessionless-CSRF')).toBe(false);
    }
  });
  it('sends each explicit mutation once with a STRING revision and current CSRF; never submits a confirmation as continuation', async () => {
    const cancelled = { ...terminal, outcome: 'cancelled', after_revision: undefined };
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(pending))
      .mockResolvedValueOnce(Response.json({ authorization_url: continuation() }))
      .mockResolvedValueOnce(Response.json(terminal))
      .mockResolvedValueOnce(Response.json(cancelled));
    const csrf = vi.fn().mockReturnValue('current-csrf');
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: csrf });
    await client.startIdentityOperation({
      kind: 'link',
      target_provider: 'telegram',
      expected_revision: revision,
    });
    expect(request).toHaveBeenCalledTimes(1);
    await client.continueIdentityOperation(operationId, revision);
    expect(request).toHaveBeenCalledTimes(2);
    await client.confirmIdentityOperation(operationId, revision);
    await client.cancelIdentityOperation(operationId, revision);
    expect(csrf).toHaveBeenCalledTimes(4);
    expect(request.mock.calls.map(([path]) => String(path).split('/').at(-1))).toEqual([
      'identity-operations',
      'continue',
      'confirm',
      'cancel',
    ]);
    for (const [, options] of request.mock.calls) {
      expect(options).toMatchObject({
        method: 'POST',
        cache: 'no-store',
        redirect: 'error',
        credentials: 'same-origin',
        referrerPolicy: 'no-referrer',
      });
      expect(new Headers(options?.headers).get('X-Sessionless-CSRF')).toBe('current-csrf');
      expect(JSON.parse(String(options?.body)).expected_revision).toBe(revision);
    }
  });
  it('parses minimal outcome 410 as a view, but not unknown 503, malformed 410 or a known expired error envelope', async () => {
    const absent = {
      operation_id: operationId,
      outcome: 'outcome_unavailable',
      allowed_actions: [],
    };
    const request = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json(absent, { status: 410 }))
      .mockResolvedValueOnce(Response.json(absent, { status: 503 }))
      .mockResolvedValueOnce(Response.json({ ...absent, phase: 'terminal' }, { status: 410 }))
      .mockResolvedValueOnce(
        Response.json(
          { error: { code: 'expired', message: 'private upstream', request_id: 'safe-request' } },
          { status: 410 },
        ),
      );
    const client = new CanonicalApiClient({ fetch: request });
    expect(await client.getIdentityOperationOutcome(operationId)).toEqual(absent);
    for (let i = 0; i < 2; i += 1)
      await expect(client.getIdentityOperationOutcome(operationId)).rejects.toMatchObject({
        code: 'temporarily_unavailable',
        status: 503,
      });
    await expect(client.getIdentityOperationOutcome(operationId)).rejects.toMatchObject({
      code: 'expired',
      status: 410,
      message: 'Login methods are unavailable.',
    });
  });
  it('bounds bytes/MIME, rejects crossed views and does not report a pending confirm response as success', async () => {
    for (const response of [
      new Response(' '.repeat(8193), { headers: { 'Content-Type': 'application/json' } }),
      new Response(JSON.stringify(rows), { headers: { 'Content-Type': 'text/html' } }),
      Response.json({ ...pending, operation_id: 'foreign-operation' }),
      Response.json({ ...pending, subject: 'private' }),
    ]) {
      const client = new CanonicalApiClient({
        fetch: vi.fn<typeof fetch>().mockResolvedValue(response),
      });
      await expect(client.getIdentityOperationOutcome(operationId)).rejects.toMatchObject({
        code: 'temporarily_unavailable',
      });
    }
    const client = new CanonicalApiClient({
      fetch: vi.fn<typeof fetch>().mockResolvedValue(Response.json(pending)),
      readCSRFToken: () => 'csrf',
    });
    await expect(client.confirmIdentityOperation(operationId, revision)).rejects.toMatchObject({
      code: 'temporarily_unavailable',
    });
  });
  it('rejects invalid selectors/revisions and missing CSRF before fetch and never retries an unknown mutation', async () => {
    const request = vi
      .fn<typeof fetch>()
      .mockRejectedValue(new Error('private provider/network details'));
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'csrf' });
    expect(() => client.getIdentityOperationOutcome('../foreign')).toThrow();
    expect(() => client.continueIdentityOperation(operationId, '01')).toThrow();
    expect(request).not.toHaveBeenCalled();
    await expect(
      client.startIdentityOperation({
        kind: 'link',
        target_provider: 'telegram',
        expected_revision: revision,
      }),
    ).rejects.toMatchObject({
      code: 'temporarily_unavailable',
      message: 'Login methods are unavailable.',
    });
    expect(request).toHaveBeenCalledTimes(1);
    await expect(
      new CanonicalApiClient({
        fetch: request,
        readCSRFToken: () => undefined,
      }).cancelIdentityOperation(operationId, revision),
    ).rejects.toMatchObject({ code: 'csrf_failed' });
    expect(request).toHaveBeenCalledTimes(1);
  });
  it('aborts discarded scope before reads/mutations and bounds a stalled body without retry', async () => {
    const request = vi.fn<typeof fetch>();
    const csrf = vi.fn();
    const aborted = new AbortController();
    aborted.abort();
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: csrf });
    await expect(client.getAccountLoginMethods(aborted.signal)).rejects.toMatchObject({
      name: 'AbortError',
    });
    await expect(
      client.confirmIdentityOperation(operationId, revision, aborted.signal),
    ).rejects.toMatchObject({ name: 'AbortError' });
    expect(request).not.toHaveBeenCalled();
    expect(csrf).not.toHaveBeenCalled();
    vi.useFakeTimers();
    try {
      const cancel = vi.fn();
      const response = new Response(new ReadableStream({ cancel }), {
        headers: { 'Content-Type': 'application/json' },
      });
      const stalled = vi.fn<typeof fetch>().mockResolvedValue(response);
      const outcome = new CanonicalApiClient({ fetch: stalled })
        .getAccountLoginMethods()
        .catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(15000);
      expect(await outcome).toMatchObject({ code: 'temporarily_unavailable' });
      expect(cancel).toHaveBeenCalledOnce();
      expect(stalled).toHaveBeenCalledOnce();
    } finally {
      vi.useRealTimers();
    }
  });
});

describe('CanonicalApiClient', () => {
  it('reads bounded closed login metadata without effects or CSRF', async () => {
    const methods = [
      { provider: 'yandex', label: 'Yandex', start_path: '/auth/login/yandex/start' },
      { provider: 'telegram', label: 'Telegram', start_path: '/auth/login/telegram/start' },
    ];
    const request = vi.fn<typeof fetch>().mockResolvedValue(Response.json({ methods }));
    const csrf = vi.fn();
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken: csrf });
    expect(await client.getLoginMethods()).toEqual({ methods });
    expect(request).toHaveBeenCalledWith(
      '/auth/login/methods',
      expect.objectContaining({
        method: 'GET',
        credentials: 'same-origin',
        cache: 'no-store',
        redirect: 'error',
      }),
    );
    expect(csrf).not.toHaveBeenCalled();
  });
  it('denies malformed/foreign/duplicate methods without a default provider', () => {
    const valid =
      '{"methods":[{"provider":"telegram","label":"Telegram","start_path":"/auth/login/telegram/start"}]}';
    expect(parseLoginMethods(valid).methods).toHaveLength(1);
    for (const raw of [
      valid.replace('"methods":', '"methods":[],"methods":'),
      valid.replace('"label":', '"label":"Telegram","label":'),
      valid.replace('"telegram"', '"foreign"'),
      valid.replace('/auth/login/telegram/start', 'https://outside.example'),
      valid.replace('"Telegram"', '"Yandex"'),
      valid.replace('"methods"', '"Methods"'),
      '{"methods":[]}',
      '{"methods":[null]}',
    ])
      expect(() => parseLoginMethods(raw)).toThrow();
  });
  it('rejects metadata errors, wrong MIME and oversized bytes without parsing private error text', async () => {
    for (const response of [
      new Response('private-error', { status: 503 }),
      new Response('{}', { headers: { 'Content-Type': 'text/html' } }),
      new Response('x'.repeat(4097), { headers: { 'Content-Type': 'application/json' } }),
    ]) {
      const client = new CanonicalApiClient({
        fetch: vi.fn<typeof fetch>().mockResolvedValue(response),
      });
      await expect(client.getLoginMethods()).rejects.toMatchObject({
        code: 'temporarily_unavailable',
        message: 'Sign-in methods are unavailable. Please try again.',
      });
    }
  });
  it('forwards one captured signal to message/archive/upload/commit/capability requests and rejects already discarded scope before fetch', async () => {
    const request = vi.fn<typeof fetch>().mockImplementation(async () => Response.json({}));
    const readCSRFToken = vi.fn().mockReturnValue('csrf');
    const client = new CanonicalApiClient({ fetch: request, readCSRFToken });
    const abort = new AbortController();
    const operations = [
      () =>
        client.setSessionArchived(
          'ses-1',
          { archived: true, idempotency_key: 'archive-key' },
          abort.signal,
        ),
      () =>
        client.createMessage(
          'ses-1',
          { text: 'draft', idempotency_key: 'message-key' },
          abort.signal,
        ),
      () =>
        client.createUpload(
          {
            session_id: 'ses-1',
            idempotency_key: 'upload-key',
            name: 'a.txt',
            media_type: 'text/plain',
            size: 3,
            sha256: 'a'.repeat(64),
            content_md5: 'kAFQmDzST7DWlj99KOF/cg==',
          },
          abort.signal,
        ),
      () => client.commitUpload('up-1', abort.signal),
      () => client.getAttachmentCapability('ses-1', 1, 0, abort.signal),
    ];
    for (const operation of operations) await operation();
    expect(request).toHaveBeenCalledTimes(5);
    for (const [, options] of request.mock.calls) expect(options?.signal).toBe(abort.signal);
    request.mockClear();
    readCSRFToken.mockClear();
    abort.abort();
    for (const operation of operations)
      await expect(operation()).rejects.toMatchObject({ name: 'AbortError' });
    expect(request).not.toHaveBeenCalled();
    expect(readCSRFToken).not.toHaveBeenCalled();
  });

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
