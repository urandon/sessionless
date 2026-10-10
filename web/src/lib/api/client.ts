import type { components } from './generated';
import { parseRunExplanationText } from '../run-explanation/validate';

type Schemas = components['schemas'];

export type Identity = Schemas['Identity'];
export type TenantPage = Schemas['TenantPage'];
export type SessionPage = Schemas['SessionPage'];
export type SessionSummary = Schemas['SessionSummary'];
export type EventPage = Schemas['EventPage'];
export type SessionEvent = Schemas['SessionEvent'];
export type Attachment = Schemas['Attachment'];
export type RunPage = Schemas['RunPage'];
export type Run = Schemas['Run'];
export type RunExplanationV1 = Schemas['RunExplanationV1'];
export type ComputeStatus = Schemas['ComputeStatus'];
export type CreateMessageResponse = Schemas['CreateMessageResponse'];
export type UploadIntent = Schemas['UploadIntent'];
export type UploadCommit = Schemas['UploadCommit'];
export type DownloadCapability = Schemas['DownloadCapability'];
export type ArtifactCapability = Schemas['ArtifactCapability'];
export type AttachedWorkerList = Schemas['AttachedWorkerListV1'];
export type AttachedWorkerSummary = Schemas['AttachedWorkerSummaryV1'];
export type AttachedWorkerReadModel = Schemas['AttachedWorkerReadModelV1'];
export type AttachedWorkerDiagnostics = Schemas['AttachedWorkerDiagnosticsV1'];
export type AttachedWorkerDiagnosticCohort = Schemas['AttachedWorkerDiagnosticCohortV1'];
export type AttachedWorkerDiagnosticCode = Schemas['AttachedWorkerDiagnosticCodeV1'];
export type AttachedWorkerReasonCode = Schemas['AttachedWorkerReasonCodeV1'];
export type AttachedWorkerActionCode = Schemas['AttachedWorkerActionCodeV1'];
export type AttachedWorkerActionUnavailableCode = Schemas['AttachedWorkerActionUnavailableCodeV1'];
export type AttachedWorkerAvailableAction = Schemas['AttachedWorkerAvailableActionV1'];

export type PublicErrorCode = Schemas['PublicError']['code'];

const csrfCookieName = '__Host-sessionless-csrf';
const csrfHeaderName = 'X-Sessionless-CSRF';
const maxErrorBodyBytes = 64 * 1024;
const maxSuccessBodyBytes = 8 * 1024 * 1024;
const safeErrorCodes = new Set<PublicErrorCode>([
  'invalid_request',
  'unauthenticated',
  'access_denied',
  'csrf_failed',
  'not_found',
  'conflict',
  'payload_too_large',
  'rate_limited',
  'temporarily_unavailable',
]);

export class ApiError extends Error {
  readonly code: PublicErrorCode;
  readonly requestId?: string;
  readonly status: number;
  readonly retryAfterMs?: number;

  constructor(
    code: PublicErrorCode,
    message: string,
    status: number,
    requestId?: string,
    retryAfterMs?: number,
  ) {
    super(message);
    this.name = 'ApiError';
    this.code = code;
    this.requestId = requestId;
    this.status = status;
    this.retryAfterMs = retryAfterMs;
  }
}

export type ConditionalResult<T> =
  | { state: 'fresh'; data: T; etag?: string; pollAfterMs?: number }
  | { state: 'not-modified'; pollAfterMs?: number };

export interface ApiClientOptions {
  fetch?: typeof globalThis.fetch;
  readCSRFToken?: () => string | undefined;
  now?: () => number;
}

export interface ListSessionsQuery {
  status?: Schemas['SessionStatus'];
  cursor?: string;
  limit?: number;
  etag?: string;
}

export interface ListEventsQuery {
  cursor?: string;
  afterSequence?: number;
  limit?: number;
  etag?: string;
  signal?: AbortSignal;
}

export interface ListRunsQuery {
  cursor?: string;
  limit?: number;
  etag?: string;
  signal?: AbortSignal;
}

export interface ListAttachedWorkersQuery {
  afterWorkerId?: string;
  limit?: number;
}

interface RequestOptions extends RequestInit {
  mutation?: boolean;
}

export class CanonicalApiClient {
  readonly #fetch: typeof globalThis.fetch;
  readonly #readCSRFToken: () => string | undefined;
  readonly #now: () => number;

  constructor(options: ApiClientOptions = {}) {
    this.#fetch = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.#readCSRFToken = options.readCSRFToken ?? readCSRFCookie;
    this.#now = options.now ?? Date.now;
  }

  getIdentity(signal?: AbortSignal): Promise<Identity> {
    return this.#json<Identity>('/api/web/v1/me', { signal });
  }

  listTenants(): Promise<TenantPage> {
    return this.#json<TenantPage>('/api/web/v1/tenants');
  }

  selectTenant(tenantId: string): Promise<Identity> {
    return this.#json<Identity>('/api/web/v1/active-tenant', {
      method: 'POST',
      mutation: true,
      body: JSON.stringify({ tenant_id: tenantId } satisfies Schemas['SelectTenantRequest']),
    });
  }

  async logout(): Promise<void> {
    await this.#json<void>('/auth/logout', { method: 'POST', mutation: true });
  }

  listSessions(query: ListSessionsQuery = {}): Promise<ConditionalResult<SessionPage>> {
    const params = new URLSearchParams();
    setParam(params, 'status', query.status);
    setParam(params, 'cursor', query.cursor);
    setParam(params, 'limit', query.limit);
    return this.#conditional<SessionPage>(withQuery('/api/web/v1/sessions', params), query.etag);
  }

  createSession(request: Schemas['CreateSessionRequest']): Promise<SessionSummary> {
    return this.#json<SessionSummary>('/api/web/v1/sessions', {
      method: 'POST',
      mutation: true,
      body: JSON.stringify(request),
    });
  }

  getSession(
    sessionId: string,
    etag?: string,
    signal?: AbortSignal,
  ): Promise<ConditionalResult<SessionSummary>> {
    return this.#conditional<SessionSummary>(
      `/api/web/v1/sessions/${selector(sessionId)}`,
      etag,
      signal,
    );
  }

  listEvents(
    sessionId: string,
    query: ListEventsQuery = {},
  ): Promise<ConditionalResult<EventPage>> {
    const params = new URLSearchParams();
    setParam(params, 'cursor', query.cursor);
    setParam(params, 'after_sequence', query.afterSequence);
    setParam(params, 'limit', query.limit);
    return this.#conditional<EventPage>(
      withQuery(`/api/web/v1/sessions/${selector(sessionId)}/events`, params),
      query.etag,
      query.signal,
    );
  }

  listRuns(sessionId: string, query: ListRunsQuery = {}): Promise<ConditionalResult<RunPage>> {
    const params = new URLSearchParams();
    setParam(params, 'cursor', query.cursor);
    setParam(params, 'limit', query.limit);
    return this.#conditional<RunPage>(
      withQuery(`/api/web/v1/sessions/${selector(sessionId)}/runs`, params),
      query.etag,
      query.signal,
    );
  }

  setSessionArchived(
    sessionId: string,
    request: Schemas['ArchiveSessionRequest'],
    signal?: AbortSignal,
  ): Promise<SessionSummary> {
    return this.#json<SessionSummary>(`/api/web/v1/sessions/${selector(sessionId)}/archive`, {
      method: 'POST',
      mutation: true,
      body: JSON.stringify(request),
      signal,
    });
  }

  createMessage(
    sessionId: string,
    request: Schemas['CreateMessageRequest'],
    signal?: AbortSignal,
  ): Promise<CreateMessageResponse> {
    return this.#json<CreateMessageResponse>(
      `/api/web/v1/sessions/${selector(sessionId)}/messages`,
      { method: 'POST', mutation: true, body: JSON.stringify(request), signal },
    );
  }

  getComputeStatus(sessionId: string, signal?: AbortSignal): Promise<ComputeStatus> {
    return this.#json<ComputeStatus>(`/api/web/v1/sessions/${selector(sessionId)}/compute`, {
      signal,
    });
  }

  createUpload(
    request: Schemas['CreateUploadRequest'],
    signal?: AbortSignal,
  ): Promise<UploadIntent> {
    return this.#json<UploadIntent>('/api/web/v1/uploads', {
      method: 'POST',
      mutation: true,
      body: JSON.stringify(request),
      signal,
    });
  }

  commitUpload(uploadId: string, signal?: AbortSignal): Promise<UploadCommit> {
    const request: Schemas['CommitUploadRequest'] = { upload_id: uploadId };
    return this.#json<UploadCommit>(`/api/web/v1/uploads/${selector(uploadId)}/commit`, {
      method: 'POST',
      mutation: true,
      body: JSON.stringify(request),
      signal,
    });
  }

  getRun(runId: string, etag?: string, signal?: AbortSignal): Promise<ConditionalResult<Run>> {
    return this.#conditional<Run>(`/api/web/v1/runs/${selector(runId)}`, etag, signal);
  }

  // expectedSessionId is local correlation, never an authorization/query selector.
  async getRunExplanation(
    runId: string,
    expectedSessionId: string,
    signal?: AbortSignal,
  ): Promise<RunExplanationV1> {
    let response: Response;
    const aborted = () => {
      if (signal?.aborted) throw new DOMException('Request aborted.', 'AbortError');
    };
    aborted();
    try {
      response = await this.#fetch(`/api/web/v1/runs/${selector(runId)}/explanation`, {
        method: 'GET',
        cache: 'no-store',
        credentials: 'same-origin',
        referrerPolicy: 'no-referrer',
        headers: { Accept: 'application/json' },
        signal,
      });
    } catch {
      aborted();
      throw explanationError(503);
    }
    if (signal?.aborted) void response.body?.cancel().catch(() => undefined);
    aborted();
    const retryAfterMs = finiteRetryAfter(response.headers, this.#now());
    if (response.status !== 200) {
      // Never retain a private body, server message, ETag, or 304 cache result.
      try {
        await response.body?.cancel();
      } catch {
        /* best-effort */
      }
      aborted();
      throw explanationError(response.status, retryAfterMs);
    }
    try {
      const body = await readBoundedText(response, 16 * 1024, signal);
      aborted();
      return parseRunExplanationText(body, runId, expectedSessionId);
    } catch {
      aborted();
      throw explanationError(503, retryAfterMs);
    }
  }

  getAttachmentCapability(
    sessionId: string,
    sequence: number,
    index: number,
    signal?: AbortSignal,
  ): Promise<DownloadCapability> {
    return this.#json<DownloadCapability>(
      `/api/web/v1/sessions/${selector(sessionId)}/events/${boundedInteger(sequence)}/attachments/${boundedInteger(index)}`,
      { signal },
    );
  }

  getArtifactCapability(
    sessionId: string,
    runId: string,
    manifestId: string,
    index: number,
  ): Promise<ArtifactCapability> {
    return this.#json<ArtifactCapability>(
      `/api/web/v1/sessions/${selector(sessionId)}/runs/${selector(runId)}/artifact-manifests/${selector(manifestId)}/artifacts/${boundedInteger(index)}`,
    );
  }

  listAttachedWorkers(query: ListAttachedWorkersQuery = {}): Promise<AttachedWorkerList> {
    const params = new URLSearchParams();
    setParam(params, 'after_worker_id', query.afterWorkerId);
    setParam(params, 'limit', query.limit);
    return this.#json<AttachedWorkerList>(withQuery('/api/web/v1/attached-workers', params));
  }

  getAttachedWorker(workerId: string): Promise<AttachedWorkerReadModel> {
    return this.#json<AttachedWorkerReadModel>(
      `/api/web/v1/attached-workers/${selector(workerId)}`,
    );
  }

  getAttachedWorkerDiagnostics(workerId: string): Promise<AttachedWorkerDiagnostics> {
    return this.#json<AttachedWorkerDiagnostics>(
      `/api/web/v1/attached-workers/${selector(workerId)}/diagnostics`,
    );
  }

  async #conditional<T>(
    path: string,
    etag?: string,
    signal?: AbortSignal,
  ): Promise<ConditionalResult<T>> {
    const headers = new Headers();
    if (etag) headers.set('If-None-Match', etag);
    const response = await this.#request(path, { headers, signal });
    const pollAfterMs = parsePollAfter(response.headers);
    if (response.status === 304) return { state: 'not-modified', pollAfterMs };
    return {
      state: 'fresh',
      data: await parseSuccess<T>(response),
      etag: response.headers.get('ETag') ?? undefined,
      pollAfterMs,
    };
  }

  async #json<T>(path: string, options: RequestOptions = {}): Promise<T> {
    return parseSuccess<T>(await this.#request(path, options));
  }

  async #request(path: string, options: RequestOptions): Promise<Response> {
    if (!path.startsWith('/')) throw new TypeError('API path must be same-origin.');
    if (options.signal?.aborted) throw new DOMException('Request aborted.', 'AbortError');

    const { mutation, ...requestOptions } = options;
    const headers = new Headers(requestOptions.headers);
    headers.set('Accept', 'application/json');
    if (requestOptions.body !== undefined) headers.set('Content-Type', 'application/json');
    if (mutation) {
      const csrf = this.#readCSRFToken();
      if (!csrf) throw new ApiError('csrf_failed', 'Please reload the page and try again.', 403);
      headers.set(csrfHeaderName, csrf);
    }

    let response: Response;
    try {
      response = await this.#fetch(path, {
        ...requestOptions,
        headers,
        credentials: 'same-origin',
        referrerPolicy: 'no-referrer',
      } as RequestInit);
    } catch {
      if (requestOptions.signal?.aborted) throw new DOMException('Request aborted.', 'AbortError');
      throw new ApiError(
        'temporarily_unavailable',
        'Sessionless is temporarily unavailable. Please try again.',
        503,
      );
    }
    if (!response.ok && response.status !== 304) throw await parsePublicError(response);
    return response;
  }
}

function readCSRFCookie(): string | undefined {
  if (typeof document === 'undefined') return undefined;
  for (const part of document.cookie.split(';')) {
    const [name, ...rest] = part.trim().split('=');
    if (name === csrfCookieName) {
      try {
        return decodeURIComponent(rest.join('='));
      } catch {
        return undefined;
      }
    }
  }
  return undefined;
}

async function parseSuccess<T>(response: Response): Promise<T> {
  if (response.status === 204) return undefined as T;
  try {
    return JSON.parse(await readBoundedText(response, maxSuccessBodyBytes)) as T;
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error;
    throw new ApiError(
      'temporarily_unavailable',
      'Sessionless returned an invalid response. Please try again.',
      503,
    );
  }
}

async function parsePublicError(response: Response): Promise<ApiError> {
  const generic = new ApiError(
    'temporarily_unavailable',
    'Sessionless is temporarily unavailable. Please try again.',
    response.status,
  );
  let body: string;
  try {
    body = await readBoundedText(response, maxErrorBodyBytes);
  } catch {
    return generic;
  }

  try {
    const parsed = JSON.parse(body) as unknown;
    if (!isRecord(parsed) || !isRecord(parsed.error)) return generic;
    const { code, message, request_id: requestId } = parsed.error;
    if (
      typeof code !== 'string' ||
      !safeErrorCodes.has(code as PublicErrorCode) ||
      typeof message !== 'string' ||
      message.trim().length === 0 ||
      message.length > 512 ||
      typeof requestId !== 'string' ||
      requestId.length === 0 ||
      requestId.length > 512
    ) {
      return generic;
    }
    return new ApiError(code as PublicErrorCode, message, response.status, requestId);
  } catch {
    return generic;
  }
}

async function readBoundedText(
  response: Response,
  limit: number,
  signal?: AbortSignal,
): Promise<string> {
  const contentLength = response.headers.get('Content-Length');
  if (contentLength && /^\d+$/.test(contentLength) && Number(contentLength) > limit) {
    try {
      await response.body?.cancel();
    } catch {
      // Cancellation is best-effort; the body is still never read by this client.
    }
    throw new RangeError('Response body exceeds the allowed size.');
  }
  if (!response.body) return '';

  const reader = response.body.getReader();
  const cancel = () => {
    void reader.cancel().catch(() => undefined);
  };
  signal?.addEventListener('abort', cancel, { once: true });
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      if (signal?.aborted) {
        cancel();
        throw new DOMException('Request aborted.', 'AbortError');
      }
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > limit) {
        await reader.cancel();
        throw new RangeError('Response body exceeds the allowed size.');
      }
      chunks.push(value);
    }
  } finally {
    signal?.removeEventListener('abort', cancel);
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder('utf-8', { fatal: true }).decode(body);
}

function explanationError(status: number, retryAfterMs?: number): ApiError {
  const code: PublicErrorCode =
    status === 401
      ? 'unauthenticated'
      : status === 403
        ? 'access_denied'
        : status === 404
          ? 'not_found'
          : status === 429
            ? 'rate_limited'
            : 'temporarily_unavailable';
  return new ApiError(
    code,
    'Run evidence is unavailable. Please try again.',
    status,
    undefined,
    retryAfterMs,
  );
}

function finiteRetryAfter(headers: Headers, now: number): number | undefined {
  const raw = headers.get('Retry-After');
  if (!raw) return undefined;
  let ms: number;
  if (/^\d+$/.test(raw)) ms = Number(raw) * 1000;
  else {
    // Accept HTTP-date, not Date.parse's permissive numeric/date guesses.
    if (!/^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(raw))
      return undefined;
    const at = Date.parse(raw);
    if (!Number.isFinite(at) || new Date(at).toUTCString() !== raw || !Number.isFinite(now))
      return undefined;
    ms = Math.max(0, at - now);
  }
  return Number.isSafeInteger(ms) ? ms : undefined;
}

function parsePollAfter(headers: Headers): number | undefined {
  const precise = headers.get('X-Sessionless-Poll-After-Ms');
  if (precise && /^\d+$/.test(precise)) return Number(precise);
  const seconds = headers.get('Retry-After');
  if (seconds && /^\d+$/.test(seconds)) return Number(seconds) * 1000;
  return undefined;
}

function setParam(params: URLSearchParams, name: string, value: string | number | undefined): void {
  if (value !== undefined && value !== '') params.set(name, String(value));
}

function withQuery(path: string, params: URLSearchParams): string {
  const query = params.toString();
  return query ? `${path}?${query}` : path;
}

function selector(value: string): string {
  if (!value) throw new TypeError('Resource selector must not be empty.');
  return encodeURIComponent(value);
}

function boundedInteger(value: number): string {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError('Resource index must be a non-negative safe integer.');
  }
  return String(value);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
