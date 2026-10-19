import { ApiError } from '../api/client';
import {
  validateConsentDiscovery,
  validateConsentSummary,
  type SearchConsentChoice,
  type SearchConsentDiscovery,
  type SearchConsentLocator,
  type SearchConsentSummary,
  type SearchConsentView,
} from './contract';

export interface SearchConsentScope {
  webIdentity: string;
  tenantId: string;
  sessionId: string;
  runId: string;
}
export interface SearchConsentState {
  phase:
    | 'idle'
    | 'loading'
    | 'none'
    | 'ready'
    | 'submitting'
    | 'unconfirmed'
    | 'approved'
    | 'denied'
    | 'stopped'
    | 'permission-lost'
    | 'error'
    | 'disposed';
  view?: SearchConsentView;
  retryChoice?: SearchConsentChoice;
  nextPollAt: number;
}
export interface SearchConsentOptions {
  now: () => number;
  discover: (
    sessionId: string,
    runId: string,
    signal: AbortSignal,
  ) => Promise<SearchConsentDiscovery>;
  read: (locator: SearchConsentLocator, signal: AbortSignal) => Promise<SearchConsentView>;
  decide: (
    locator: SearchConsentLocator,
    choice: SearchConsentChoice,
    signal: AbortSignal,
  ) => Promise<SearchConsentSummary>;
}

interface Budget {
  end: number;
  next: number;
  rounds: number;
  gets: number;
  posts: number;
  // Opaque unresolved write identity, never query text or a receipt. Display
  // disposal must not turn an ambiguous decision into permission to switch it.
  unresolved?: { locator: SearchConsentLocator; choice: SearchConsentChoice; expiresAt: string };
}

// Volatile browser state only. The original worker/Store deadlines and current
// authorization remain authoritative. Reopening cannot renew these UI bounds.
export class SearchConsentController {
  readonly #options: SearchConsentOptions;
  readonly #budgets = new Map<string, Budget>();
  readonly #listeners = new Set<(state: SearchConsentState) => void>();
  #scope?: SearchConsentScope;
  #budget?: Budget;
  #locator?: SearchConsentLocator;
  #visible = true;
  #disposed = false;
  #generation = 0;
  #abort?: AbortController;
  #flight?: Promise<void>;
  #state: SearchConsentState = { phase: 'idle', nextPollAt: 0 };

  constructor(options: SearchConsentOptions) {
    this.#options = options;
  }
  snapshot(): SearchConsentState {
    return structuredClone(this.#state);
  }
  subscribe(listener: (state: SearchConsentState) => void): () => void {
    if (this.#disposed) return () => undefined;
    this.#listeners.add(listener);
    listener(this.snapshot());
    return () => this.#listeners.delete(listener);
  }

  setScope(scope?: SearchConsentScope): void {
    if (this.#disposed) return;
    if (JSON.stringify(scope) === JSON.stringify(this.#scope)) return;
    this.#discard();
    this.#scope = scope ? { ...scope } : undefined;
    this.#budget = undefined;
    if (scope) {
      const key = JSON.stringify([scope.webIdentity, scope.tenantId, scope.sessionId, scope.runId]);
      this.#budget = this.#budgets.get(key);
      if (!this.#budget && this.#budgets.size < 50) {
        this.#budget = { end: this.#options.now() + 120000, next: 0, rounds: 0, gets: 0, posts: 0 };
        this.#budgets.set(key, this.#budget);
      }
      // Never evict an old Run and recreate a fresh polling window for it.
      if (!this.#budget) this.#state.phase = 'stopped';
    }
    this.#publish();
  }

  setVisible(visible: boolean): void {
    if (this.#disposed || visible === this.#visible) return;
    this.#visible = visible;
    if (!visible) this.#discard();
    this.#publish();
  }

  refresh(): Promise<void> {
    if (!this.#available()) return Promise.resolve();
    const budget = this.#budget!;
    if (this.#state.view && this.#options.now() >= Date.parse(this.#state.view.expires_at)) {
      this.#discard();
      this.#state.phase = 'stopped';
      this.#publish();
      return Promise.resolve();
    }
    if (this.#options.now() >= budget.end || budget.rounds >= 24 || budget.gets >= 48) {
      this.#discard();
      this.#state.phase = 'stopped';
      this.#publish();
      return Promise.resolve();
    }
    if (this.#flight) return this.#flight;
    if (this.#options.now() < budget.next) return Promise.resolve();
    budget.rounds++;
    budget.next = this.#options.now() + 5000;
    const scope = { ...this.#scope! };
    const unresolved = budget.unresolved;
    if (unresolved && this.#options.now() >= Date.parse(unresolved.expiresAt)) {
      delete budget.unresolved;
      this.#discard();
      this.#state.phase = 'stopped';
      this.#publish();
      return Promise.resolve();
    }
    const retryChoice = unresolved?.choice;
    const pinned = unresolved ? { ...unresolved.locator } : undefined;
    if (retryChoice) this.#state.retryChoice = retryChoice;
    return this.#start('loading', async (generation, signal) => {
      if (retryChoice && pinned) {
        // An ambiguous decision stays tied to its exact request. Discovery of
        // another request, or a transient read failure, cannot change its choice.
        this.#get(budget);
        const current = await this.#options.read(pinned, signal);
        if (!this.#current(generation)) return;
        if (current.expires_at !== unresolved!.expiresAt) throw new TypeError('Consent changed.');
        this.#locator = pinned;
        this.#accept(current, pinned, retryChoice);
        return;
      }
      this.#get(budget);
      const discovered = validateConsentDiscovery(
        await this.#options.discover(scope.sessionId, scope.runId, signal),
        scope.sessionId,
        scope.runId,
      );
      if (!this.#current(generation)) return;
      if (!discovered.pending) {
        this.#locator = undefined;
        this.#state = { phase: 'none', nextPollAt: budget.next };
        return;
      }
      const pending = discovered.pending;
      if (this.#options.now() >= Date.parse(pending.expires_at)) {
        this.#locator = undefined;
        this.#state = { phase: 'stopped', nextPollAt: budget.next };
        return;
      }
      const locator = {
        sessionId: scope.sessionId,
        runId: scope.runId,
        attemptId: pending.attempt_id,
        requestId: pending.request_id,
      };
      this.#get(budget);
      const value = await this.#options.read(locator, signal);
      if (!this.#current(generation)) return;
      if (value.expires_at !== pending.expires_at) throw new TypeError('Consent changed.');
      this.#locator = locator;
      this.#accept(value, locator);
    });
  }

  decide(choice: SearchConsentChoice): Promise<void> {
    if (
      !this.#available() ||
      this.#flight ||
      !this.#locator ||
      !this.#state.view ||
      !['ready', 'unconfirmed'].includes(this.#state.phase)
    )
      return Promise.resolve();
    const budget = this.#budget!,
      locator = { ...this.#locator },
      value = this.#state.view;
    if (this.#state.retryChoice && choice !== this.#state.retryChoice) return Promise.resolve();
    if (
      this.#options.now() >= budget.end ||
      this.#options.now() >= Date.parse(value.expires_at) ||
      budget.posts >= 4 ||
      budget.gets >= 48
    ) {
      this.#discard();
      this.#state.phase = 'stopped';
      this.#publish();
      return Promise.resolve();
    }
    budget.posts++;
    budget.unresolved = { locator: { ...locator }, choice, expiresAt: value.expires_at };
    return this.#start('submitting', async (generation, signal) => {
      try {
        const response = await this.#options.decide(locator, choice, signal);
        if (!this.#current(generation)) return;
        const confirmed = validateConsentSummary(response, locator.requestId);
        if (
          confirmed.status !== (choice === 'approve' ? 'approved' : 'denied') ||
          confirmed.expires_at !== value.expires_at
        )
          throw new TypeError('Decision not confirmed.');
        this.#state = { phase: confirmed.status, nextPollAt: budget.next };
        delete budget.unresolved;
      } catch (error) {
        if (!this.#current(generation)) return;
        if (this.#permissionLoss(error)) {
          this.#deny();
          return;
        }
        // A failed POST response cannot prove rollback. Recover via a read,
        // never another automatic write. Explicit retry retains the same choice.
        this.#state = {
          phase: 'unconfirmed',
          view: value,
          retryChoice: choice,
          nextPollAt: budget.next,
        };
        try {
          this.#get(budget);
          const current = await this.#options.read(locator, signal);
          if (!this.#current(generation)) return;
          if (current.expires_at !== value.expires_at)
            throw new TypeError('Consent changed.', { cause: error });
          this.#accept(current, locator, choice);
        } catch (readError) {
          if (this.#current(generation) && this.#permissionLoss(readError)) this.#deny();
          // Unknown remains unconfirmed; private server messages never enter state.
        }
      }
    });
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#discard();
    this.#scope = undefined;
    this.#budget = undefined;
    this.#disposed = true;
    this.#state.phase = 'disposed';
    this.#publish();
    this.#listeners.clear();
    this.#budgets.clear();
  }

  #start(
    phase: SearchConsentState['phase'],
    work: (generation: number, signal: AbortSignal) => Promise<void>,
  ): Promise<void> {
    const generation = ++this.#generation;
    const abort = new AbortController();
    this.#abort = abort;
    this.#state = { ...this.#state, phase, nextPollAt: this.#budget!.next };
    const flight = Promise.resolve().then(async () => {
      if (!this.#current(generation)) return;
      try {
        await work(generation, abort.signal);
      } catch (error) {
        if (!this.#current(generation)) return;
        if (this.#permissionLoss(error)) this.#deny();
        else if (this.#state.retryChoice) this.#state.phase = 'unconfirmed';
        else this.#state = { phase: 'error', nextPollAt: this.#budget!.next };
      } finally {
        if (this.#current(generation)) {
          this.#flight = undefined;
          this.#abort = undefined;
          if (this.#options.now() >= this.#budget!.end)
            this.#state = { phase: 'stopped', nextPollAt: this.#budget!.next };
          this.#publish();
        }
      }
    });
    this.#flight = flight;
    this.#publish();
    return flight;
  }

  #get(budget: Budget): void {
    if (budget.gets >= 48 || this.#options.now() >= budget.end)
      throw new RangeError('Read budget exhausted.');
    budget.gets++;
  }
  #accept(
    value: SearchConsentView,
    locator: SearchConsentLocator,
    retryChoice?: SearchConsentChoice,
  ): void {
    validateConsentSummary(value, locator.requestId, true);
    if (value.status !== 'pending') delete this.#budget!.unresolved;
    const phase =
      value.status === 'pending'
        ? this.#options.now() < Date.parse(value.expires_at)
          ? retryChoice
            ? 'unconfirmed'
            : 'ready'
          : 'stopped'
        : ['approved', 'denied'].includes(value.status)
          ? (value.status as 'approved' | 'denied')
          : 'stopped';
    this.#state = {
      phase,
      nextPollAt: this.#budget!.next,
      ...(['ready', 'unconfirmed'].includes(phase)
        ? { view: structuredClone(value), retryChoice }
        : {}),
    };
  }
  #available(): boolean {
    return !this.#disposed && this.#visible && !!this.#scope && !!this.#budget;
  }
  #current(generation: number): boolean {
    if (!this.#available() || generation !== this.#generation) return false;
    if (
      this.#options.now() >= this.#budget!.end ||
      (this.#state.view && this.#options.now() >= Date.parse(this.#state.view.expires_at))
    ) {
      this.#discard();
      this.#state.phase = 'stopped';
      this.#publish();
      return false;
    }
    return true;
  }
  #permissionLoss(error: unknown): boolean {
    return error instanceof ApiError && [401, 403, 404].includes(error.status);
  }
  #deny(): void {
    this.#discard();
    this.#scope = undefined;
    this.#state.phase = 'permission-lost';
    this.#publish();
  }
  #discard(): void {
    ++this.#generation;
    this.#abort?.abort();
    this.#abort = undefined;
    this.#flight = undefined;
    this.#locator = undefined;
    this.#state = { phase: 'idle', nextPollAt: this.#budget?.next ?? 0 };
  }
  #publish(): void {
    for (const listener of this.#listeners) listener(this.snapshot());
  }
}
