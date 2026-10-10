import { ApiError, type RunExplanationV1 } from '../api/client';
import { validateRunExplanation } from './validate';

export interface RunExplanationScope {
  webIdentity: string;
  tenantId: string;
  sessionId: string;
  runId: string;
}

export interface RunExplanationState {
  phase: 'idle' | 'loading' | 'ready' | 'refresh-failed' | 'permission-lost' | 'disposed';
  evidence?: RunExplanationV1;
  // Browser read failures are not server evidence/lease freshness.
  refreshError?: { code: ApiError['code']; status: number };
  nextRefreshAt: number;
}

export interface RunExplanationControllerOptions {
  read: (
    runId: string,
    expectedSessionId: string,
    signal: AbortSignal,
  ) => Promise<RunExplanationV1>;
  now: () => number;
}

// Memory-only, caller-driven lifecycle. No storage, timer, API probes or mutation.
export class RunExplanationController {
  #scope?: RunExplanationScope;
  #visible = true;
  #disposed = false;
  #generation = 0;
  #abort?: AbortController;
  #flight?: Promise<void>;
  #rateScope?: string;
  #state: RunExplanationState = { phase: 'idle', nextRefreshAt: 0 };
  #listeners = new Set<(state: RunExplanationState) => void>();
  readonly #options: RunExplanationControllerOptions;

  constructor(options: RunExplanationControllerOptions) {
    this.#options = options;
  }

  snapshot(): RunExplanationState {
    return structuredClone(this.#state);
  }

  subscribe(listener: (state: RunExplanationState) => void): () => void {
    if (this.#disposed) return () => undefined;
    this.#listeners.add(listener);
    listener(this.snapshot());
    return () => this.#listeners.delete(listener);
  }

  setScope(scope?: RunExplanationScope): void {
    if (this.#disposed) return;
    if (
      scope &&
      this.#scope &&
      Object.keys(scope).every(
        (key) =>
          scope[key as keyof RunExplanationScope] ===
          this.#scope?.[key as keyof RunExplanationScope],
      )
    )
      return;
    this.#discard();
    this.#scope = scope ? { ...scope } : undefined;
    if (scope) {
      const rateScope = JSON.stringify([scope.webIdentity, scope.tenantId]);
      if (rateScope !== this.#rateScope) this.#state.nextRefreshAt = 0;
      this.#rateScope = rateScope;
    }
    this.#publish();
  }

  // Call with false on hidden/unmount; call setScope(undefined) on permission loss.
  setVisible(visible: boolean): void {
    if (this.#disposed || visible === this.#visible) return;
    this.#visible = visible;
    if (!visible) this.#discard();
    this.#publish();
  }

  refresh(): Promise<void> {
    if (this.#disposed || !this.#visible || !this.#scope) return Promise.resolve();
    if (this.#flight) return this.#flight;
    const now = this.#options.now();
    if (now < this.#state.nextRefreshAt) return Promise.resolve();
    const scope = { ...this.#scope },
      generation = ++this.#generation;
    const abort = new AbortController();
    this.#abort = abort;
    this.#state = { phase: 'loading', evidence: this.#state.evidence, nextRefreshAt: now + 5000 };
    // Microtask start ensures one-flight is installed before read or listeners reenter.
    const flight = Promise.resolve().then(async () => {
      if (!this.#current(generation)) return;
      try {
        const evidence = await this.#options.read(scope.runId, scope.sessionId, abort.signal);
        if (!this.#current(generation)) return;
        validateRunExplanation(evidence, scope.runId, scope.sessionId);
        this.#state = {
          phase: 'ready',
          evidence: structuredClone(evidence),
          nextRefreshAt: this.#state.nextRefreshAt,
        };
      } catch (error) {
        if (!this.#current(generation)) return;
        const status = error instanceof ApiError ? error.status : 503;
        const code = error instanceof ApiError ? error.code : 'temporarily_unavailable';
        const delay = error instanceof ApiError ? error.retryAfterMs : undefined;
        if (delay !== undefined && Number.isSafeInteger(delay) && delay >= 0) {
          this.#state.nextRefreshAt = Math.max(
            this.#state.nextRefreshAt,
            Math.min(Number.MAX_SAFE_INTEGER, this.#options.now() + delay),
          );
        }
        if ([401, 403, 404].includes(status)) {
          this.#discard();
          this.#scope = undefined;
          this.#state.phase = 'permission-lost';
        } else {
          this.#state.phase = 'refresh-failed';
        }
        this.#state.refreshError = { code, status };
        if (this.#state.phase === 'permission-lost') this.#publish();
      } finally {
        if (this.#current(generation)) {
          this.#flight = undefined;
          this.#abort = undefined;
        }
        // Discard already notified. An obsolete response must not publish even an empty state.
        if (this.#current(generation)) this.#publish();
      }
    });
    this.#flight = flight;
    this.#publish();
    return flight;
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#discard();
    this.#scope = undefined;
    this.#rateScope = undefined;
    this.#disposed = true;
    this.#state.phase = 'disposed';
    this.#publish();
    this.#listeners.clear();
  }

  #current(generation: number): boolean {
    return !this.#disposed && this.#visible && !!this.#scope && generation === this.#generation;
  }

  #discard(): void {
    ++this.#generation;
    this.#abort?.abort();
    this.#abort = undefined;
    this.#flight = undefined;
    this.#state = { phase: 'idle', nextRefreshAt: this.#state.nextRefreshAt };
  }

  #publish(): void {
    for (const listener of this.#listeners) listener(this.snapshot());
  }
}
