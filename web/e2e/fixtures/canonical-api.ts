import { expect, test as base, type Page, type Request, type Route } from '@playwright/test';

export const csrfToken = 'e2e-csrf-token';
export const sessionId = 'session-alpha';
export const objectOrigin = 'https://objects.sessionless.test';

type AuthMode = 'authenticated' | 'unauthenticated' | 'access-denied' | 'error';

export interface CapturedRequest {
  method: string;
  url: string;
  headers: Record<string, string>;
  body?: unknown;
}

interface Gate {
  started: Promise<CapturedRequest>;
  release: () => void;
}

interface PendingGate {
  started: (request: CapturedRequest) => void;
  released: Promise<void>;
  release: () => void;
}

export class CanonicalApiFixture {
  readonly requests: CapturedRequest[] = [];
  auth: AuthMode = 'authenticated';
  activeTenant = 'tenant-one';
  sessionStatus: 'active' | 'archived' = 'active';
  emptySessions = false;
  emptyHistory = false;

  #page: Page;
  #messageCreated = false;
  #uploadGate?: PendingGate;

  constructor(page: Page) {
    this.#page = page;
  }

  async install(): Promise<void> {
    await this.#page.context().addCookies([
      {
        name: '__Host-sessionless-csrf',
        value: csrfToken,
        domain: '127.0.0.1',
        path: '/',
        secure: true,
        sameSite: 'Strict',
      },
    ]);
    await this.#page.route(`${objectOrigin}/**`, (route) => this.#handleObject(route));
    await this.#page.route('**/api/web/v1/**', (route) => this.#handleApi(route));
    await this.#page.route('**/auth/logout', (route) => this.#handleLogout(route));
  }

  pauseNextUpload(): Gate {
    let markStarted: (request: CapturedRequest) => void = () => undefined;
    let release: () => void = () => undefined;
    const started = new Promise<CapturedRequest>((resolve) => {
      markStarted = resolve;
    });
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    this.#uploadGate = { started: markStarted, released, release };
    return { started, release };
  }

  findRequest(method: string, pathname: string): CapturedRequest | undefined {
    return this.requests.find((request) => {
      const url = new URL(request.url);
      return request.method === method && url.pathname === pathname;
    });
  }

  requestsFor(method: string, pathname: string): CapturedRequest[] {
    return this.requests.filter((request) => {
      const url = new URL(request.url);
      return request.method === method && url.pathname === pathname;
    });
  }

  async expectMutation(method: string, pathname: string): Promise<CapturedRequest> {
    await expect
      .poll(() => this.findRequest(method, pathname), { message: `${method} ${pathname}` })
      .not.toBeUndefined();
    const request = this.findRequest(method, pathname);
    expect(request).toBeDefined();
    expect(request?.headers['x-sessionless-csrf']).toBe(csrfToken);
    expect(request?.headers['content-type']).toContain('application/json');
    return request as CapturedRequest;
  }

  async #handleLogout(route: Route): Promise<void> {
    const captured = await this.#capture(route.request());
    this.requests.push(captured);
    await route.fulfill({ status: 204 });
  }

  async #handleObject(route: Route): Promise<void> {
    const request = await this.#capture(route.request());
    this.requests.push(request);
    const gate = this.#uploadGate;
    if (gate) {
      this.#uploadGate = undefined;
      gate.started(request);
      await gate.released;
    }
    await route.fulfill({
      status: 200,
      headers: {
        'Access-Control-Allow-Origin': 'http://127.0.0.1:4173',
        'Access-Control-Allow-Methods': 'PUT, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Content-MD5, X-Upload-Token',
      },
      body: '',
    });
  }

  async #handleApi(route: Route): Promise<void> {
    const request = route.request();
    const captured = await this.#capture(request);
    this.requests.push(captured);
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();

    if (this.auth === 'access-denied') return this.#error(route, 403, 'access_denied');
    if (this.auth === 'unauthenticated' && path !== '/api/web/v1/me') {
      return this.#error(route, 401, 'unauthenticated');
    }

    if (path === '/api/web/v1/me' && method === 'GET') {
      if (this.auth === 'unauthenticated') return this.#error(route, 401, 'unauthenticated');
      if (this.auth === 'error') return this.#error(route, 503, 'temporarily_unavailable');
      return this.#json(route, this.#identity());
    }

    if (path === '/api/web/v1/active-tenant' && method === 'POST') {
      const body = captured.body as { tenant_id?: string } | undefined;
      this.activeTenant = body?.tenant_id === 'tenant-two' ? 'tenant-two' : 'tenant-one';
      return this.#json(route, this.#identity());
    }

    if (path === '/api/web/v1/sessions' && method === 'GET') {
      const requestedStatus = url.searchParams.get('status') ?? 'active';
      const visible = !this.emptySessions && requestedStatus === this.sessionStatus;
      return this.#json(route, { items: visible ? [this.#session()] : [] }, 200, {
        ETag: '"sessions-v1"',
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}` && method === 'GET') {
      return this.#json(route, this.#session(), 200, {
        ETag: '"session-v1"',
        'X-Sessionless-Poll-After-Ms': '500',
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}/events` && method === 'GET') {
      const after = Number(url.searchParams.get('after_sequence') ?? 0);
      const items = this.emptyHistory
        ? []
        : after > 0
          ? this.#messageCreated && after < 4
            ? this.#newEvents().filter((event) => event.sequence > after)
            : []
          : this.#initialEvents();
      return this.#json(route, { items }, 200, {
        ETag: this.#messageCreated ? '"events-v2"' : '"events-v1"',
        'X-Sessionless-Poll-After-Ms': '500',
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}/runs` && method === 'GET') {
      const items = this.#messageCreated ? [this.#run('run-new', 'event-3')] : [];
      return this.#json(route, { items }, 200, {
        ETag: this.#messageCreated ? '"runs-v2"' : '"runs-v1"',
        'X-Sessionless-Poll-After-Ms': '500',
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}/compute` && method === 'GET') {
      return this.#json(route, {
        availability: 'ready',
        connection: {
          provider: 'fixture-ai',
          entitlement: 'active',
          quota: 'available',
          observed_at: '2026-08-18T10:00:00Z',
        },
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}/archive` && method === 'POST') {
      const body = captured.body as { archived?: boolean } | undefined;
      this.sessionStatus = body?.archived ? 'archived' : 'active';
      return this.#json(route, this.#session());
    }

    if (path === '/api/web/v1/uploads' && method === 'POST') {
      return this.#json(route, {
        upload_id: 'upload-1',
        method: 'PUT',
        url: `${objectOrigin}/uploads/upload-1?signature=one-time-secret`,
        headers: {
          'Content-Type': 'text/plain',
          'Content-MD5': '9cpoqhUMFcdsvuGtlAybug==',
          'X-Upload-Token': 'object-capability-secret',
        },
        expires_at: '2026-08-18T10:05:00Z',
      });
    }

    if (path === '/api/web/v1/uploads/upload-1/commit' && method === 'POST') {
      return this.#json(route, {
        upload_id: 'upload-1',
        name: 'fixture.txt',
        media_type: 'text/plain',
        size: 12,
      });
    }

    if (path === `/api/web/v1/sessions/${sessionId}/messages` && method === 'POST') {
      this.#messageCreated = true;
      return this.#json(route, {
        session_id: sessionId,
        event_id: 'event-3',
        sequence: 3,
        run_id: 'run-new',
        created: true,
        compute: {
          provider: 'fixture-ai',
          entitlement: 'active',
          quota: 'available',
          observed_at: '2026-08-18T10:00:00Z',
        },
      });
    }

    return this.#error(route, 404, 'not_found');
  }

  async #capture(request: Request): Promise<CapturedRequest> {
    let body: unknown;
    const raw = request.postData();
    if (raw) {
      try {
        body = JSON.parse(raw) as unknown;
      } catch {
        body = raw;
      }
    }
    return {
      method: request.method(),
      url: request.url(),
      headers: await request.allHeaders(),
      body,
    };
  }

  async #json(
    route: Route,
    body: unknown,
    status = 200,
    headers: Record<string, string> = {},
  ): Promise<void> {
    await route.fulfill({
      status,
      contentType: 'application/json',
      headers,
      body: JSON.stringify(body),
    });
  }

  async #error(route: Route, status: number, code: string): Promise<void> {
    await this.#json(
      route,
      {
        error: {
          code,
          message:
            code === 'temporarily_unavailable'
              ? 'Sessionless is temporarily unavailable.'
              : code === 'unauthenticated'
                ? 'Sign in to continue.'
                : 'This account does not have access.',
          request_id: `fixture-${code}`,
        },
      },
      status,
    );
  }

  #identity(): unknown {
    return {
      user_id: 'user-one',
      provider: 'telegram',
      tenants: [
        { tenant_id: 'tenant-one', role: 'owner', active: this.activeTenant === 'tenant-one' },
        { tenant_id: 'tenant-two', role: 'member', active: this.activeTenant === 'tenant-two' },
      ],
    };
  }

  #session(): Record<string, unknown> {
    return {
      session_id: sessionId,
      status: this.sessionStatus,
      title: 'Launch planning',
      preview: 'What should we ship next?',
      last_sequence: this.#messageCreated ? 4 : 2,
      created_at: '2026-08-18T09:00:00Z',
      updated_at: '2026-08-18T10:00:00Z',
      ...(this.sessionStatus === 'archived' ? { archived_at: '2026-08-18T10:01:00Z' } : {}),
    };
  }

  #initialEvents(): Array<Record<string, unknown>> {
    return [
      {
        event_id: 'event-1',
        sequence: 1,
        kind: 'user_message',
        content: { text: 'What should we ship next?' },
        created_at: '2026-08-18T09:00:00Z',
      },
      {
        event_id: 'event-2',
        sequence: 2,
        kind: 'assistant_message',
        content: { text: 'Ship the canonical WebUI.' },
        created_at: '2026-08-18T09:00:05Z',
      },
    ];
  }

  #newEvents(): Array<Record<string, unknown> & { sequence: number }> {
    return [
      {
        event_id: 'event-3',
        sequence: 3,
        kind: 'user_message',
        content: { text: 'Attach the fixture report.' },
        created_at: '2026-08-18T10:00:00Z',
      },
      {
        event_id: 'event-4',
        sequence: 4,
        kind: 'assistant_message',
        content: { text: 'The report is queued for review.' },
        created_at: '2026-08-18T10:00:02Z',
      },
    ];
  }

  #run(runId: string, triggerEventId: string): Record<string, unknown> {
    return {
      run_id: runId,
      session_id: sessionId,
      trigger_event_id: triggerEventId,
      subscription_connection_id: 'connection-one',
      provider: 'fixture-ai',
      status: 'succeeded',
      created_at: '2026-08-18T10:00:00Z',
      updated_at: '2026-08-18T10:00:02Z',
      finished_at: '2026-08-18T10:00:02Z',
    };
  }
}

interface Fixtures {
  canonicalApi: CanonicalApiFixture;
}

export const test = base.extend<Fixtures>({
  canonicalApi: async ({ page }, use) => {
    const fixture = new CanonicalApiFixture(page);
    await fixture.install();
    await use(fixture);
  },
});

export { expect } from '@playwright/test';
