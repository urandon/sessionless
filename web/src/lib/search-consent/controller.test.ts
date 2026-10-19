import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '../api/client';
import { SearchConsentController, type SearchConsentScope } from './controller';
import type { SearchConsentView } from './contract';

const epoch = Date.parse('2026-10-20T00:00:00Z');
const scope: SearchConsentScope = {
  webIdentity: 'owner',
  tenantId: 'ten-1',
  sessionId: 'ses-1',
  runId: 'run-1',
};
const view: SearchConsentView = {
  request_id: 'request-1',
  revision: 1,
  status: 'pending',
  expires_at: new Date(epoch + 60000).toISOString(),
  query: ' exact <script>untrusted</script>\nquery ',
};
function barrier<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function fixture() {
  let now = epoch;
  const discover = vi.fn().mockResolvedValue({
    session_id: 'ses-1',
    run_id: 'run-1',
    pending: { attempt_id: 'attempt-1', request_id: 'request-1', expires_at: view.expires_at },
  });
  const read = vi.fn().mockResolvedValue(view);
  const decide = vi.fn().mockResolvedValue({
    request_id: 'request-1',
    revision: 2,
    status: 'approved',
    expires_at: view.expires_at,
  });
  const controller = new SearchConsentController({ now: () => now, discover, read, decide });
  controller.setScope(scope);
  return {
    controller,
    discover,
    read,
    decide,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe('search consent lifecycle', () => {
  it.each(['hide', 'scope', 'failed-response'] as const)(
    'retains ambiguous decision identity across %s and reopening',
    async (lifecycle) => {
      const f = fixture(),
        pending = barrier<{
          request_id: string;
          revision: number;
          status: 'approved';
          expires_at: string;
        }>();
      await f.controller.refresh();
      if (lifecycle === 'failed-response')
        f.decide.mockRejectedValue(new ApiError('temporarily_unavailable', 'unknown', 503));
      else f.decide.mockReturnValue(pending.promise);
      const first = f.controller.decide('approve');
      await Promise.resolve();
      if (lifecycle === 'failed-response') await first;
      if (lifecycle === 'scope') {
        f.controller.setScope({ ...scope, runId: 'run-other' });
        f.controller.setScope(scope);
      } else {
        f.controller.setVisible(false);
        f.controller.setVisible(true);
      }
      f.advance(5000);
      await f.controller.refresh();
      expect(f.controller.snapshot()).toMatchObject({
        phase: 'unconfirmed',
        retryChoice: 'approve',
      });
      await f.controller.decide('deny');
      expect(f.decide).toHaveBeenCalledTimes(1);
      if (lifecycle !== 'failed-response') {
        pending.resolve({
          request_id: 'request-1',
          revision: 2,
          status: 'approved',
          expires_at: view.expires_at,
        });
        await first;
        expect(f.controller.snapshot().phase).toBe('unconfirmed');
      }
    },
  );

  it('discovers only in canonical scope and displays the exact private read', async () => {
    const f = fixture();
    await f.controller.refresh();
    expect(f.discover).toHaveBeenCalledWith('ses-1', 'run-1', expect.any(AbortSignal));
    expect(f.read).toHaveBeenCalledWith(
      { sessionId: 'ses-1', runId: 'run-1', attemptId: 'attempt-1', requestId: 'request-1' },
      expect.any(AbortSignal),
    );
    expect(f.controller.snapshot()).toMatchObject({ phase: 'ready', view });
  });

  it('coalesces reads and obeys cadence without publishing spinner-only private updates', async () => {
    const f = fixture(),
      pending = barrier<SearchConsentView>();
    f.read.mockReturnValue(pending.promise);
    const first = f.controller.refresh(),
      second = f.controller.refresh();
    expect(second).toBe(first);
    await Promise.resolve();
    await Promise.resolve();
    pending.resolve(view);
    await first;
    await f.controller.refresh();
    expect(f.discover).toHaveBeenCalledTimes(1);
  });

  it.each(['hide', 'scope', 'dispose'] as const)(
    'fences a late private read after %s',
    async (action) => {
      const f = fixture(),
        pending = barrier<SearchConsentView>();
      f.read.mockReturnValue(pending.promise);
      const flight = f.controller.refresh();
      await Promise.resolve();
      await Promise.resolve();
      if (action === 'hide') f.controller.setVisible(false);
      if (action === 'scope') f.controller.setScope({ ...scope, runId: 'run-other' });
      if (action === 'dispose') f.controller.dispose();
      pending.resolve(view);
      await flight;
      expect(f.controller.snapshot().view).toBeUndefined();
      expect(f.decide).not.toHaveBeenCalled();
    },
  );

  it('expires without extending its window on hide/reopen', async () => {
    const f = fixture();
    await f.controller.refresh();
    f.controller.setVisible(false);
    f.advance(60001);
    f.controller.setVisible(true);
    await f.controller.refresh();
    expect(f.controller.snapshot().phase).toBe('stopped');
    expect(f.controller.snapshot()).not.toHaveProperty('view');
    expect(f.read).toHaveBeenCalledTimes(1);
    await f.controller.decide('approve');
    expect(f.decide).not.toHaveBeenCalled();
  });

  it('clears private material on permission loss, not on ordinary transport errors', async () => {
    const f = fixture();
    await f.controller.refresh();
    f.advance(5000);
    f.read.mockRejectedValue(
      new ApiError('temporarily_unavailable', 'private must not be retained in errors', 503),
    );
    await f.controller.refresh();
    expect(f.controller.snapshot().phase).toBe('error');
    expect(f.controller.snapshot().view).toBeUndefined();
    f.advance(5000);
    f.discover.mockRejectedValue(new ApiError('access_denied', 'private', 403));
    await f.controller.refresh();
    expect(f.controller.snapshot().phase).toBe('permission-lost');
    expect(f.controller.snapshot().view).toBeUndefined();
  });

  it('one explicit decision prevents conflicting clicks and late mutation responses after disposal', async () => {
    const f = fixture(),
      pending = barrier<{
        request_id: string;
        revision: number;
        status: 'approved';
        expires_at: string;
      }>();
    await f.controller.refresh();
    f.decide.mockReturnValue(pending.promise);
    const flight = f.controller.decide('approve');
    await f.controller.decide('deny');
    await Promise.resolve();
    expect(f.decide).toHaveBeenCalledTimes(1);
    f.controller.setVisible(false);
    pending.resolve({
      request_id: 'request-1',
      revision: 2,
      status: 'approved',
      expires_at: view.expires_at,
    });
    await flight;
    expect(f.controller.snapshot().view).toBeUndefined();
    expect(f.controller.snapshot().phase).not.toBe('approved');
  });

  it('recovers committed-but-lost response by canonical GET, never a second automatic POST', async () => {
    const f = fixture();
    await f.controller.refresh();
    f.decide.mockRejectedValue(new ApiError('temporarily_unavailable', 'unknown', 503));
    f.read.mockResolvedValue({ ...view, status: 'approved', revision: 2 });
    await f.controller.decide('approve');
    expect(f.controller.snapshot().phase).toBe('approved');
    expect(f.controller.snapshot()).not.toHaveProperty('view');
    expect(f.decide).toHaveBeenCalledTimes(1);
    expect(f.read).toHaveBeenCalledTimes(2);
  });

  it('keeps an unresolved decision unconfirmed and permits only an explicit identical retry', async () => {
    const f = fixture();
    await f.controller.refresh();
    f.decide.mockRejectedValue(new ApiError('temporarily_unavailable', 'unknown', 503));
    await f.controller.decide('approve');
    expect(f.controller.snapshot()).toMatchObject({ phase: 'unconfirmed', retryChoice: 'approve' });
    await f.controller.decide('deny');
    expect(f.decide).toHaveBeenCalledTimes(1);
    f.decide.mockResolvedValue({
      request_id: 'request-1',
      revision: 2,
      status: 'approved',
      expires_at: view.expires_at,
    });
    await f.controller.decide('approve');
    expect(f.decide).toHaveBeenCalledTimes(2);
    expect(f.controller.snapshot().phase).toBe('approved');
  });

  it('bounds absent-request polling and never turns absence into approval', async () => {
    const f = fixture();
    f.discover.mockResolvedValue({ session_id: 'ses-1', run_id: 'run-1' });
    for (let i = 0; i < 60; i++) {
      await f.controller.refresh();
      f.advance(5000);
    }
    expect(f.discover.mock.calls.length).toBeGreaterThan(0);
    expect(f.discover.mock.calls.length).toBeLessThanOrEqual(24);
    expect(f.read).not.toHaveBeenCalled();
    expect(f.decide).not.toHaveBeenCalled();
    expect(f.controller.snapshot().phase).toBe('stopped');
  });

  it('keeps an ambiguous choice across automatic reads and read failures', async () => {
    const f = fixture();
    await f.controller.refresh();
    f.decide.mockRejectedValue(new ApiError('temporarily_unavailable', 'unknown', 503));
    await f.controller.decide('approve');
    f.advance(5000);
    await f.controller.refresh();
    expect(f.controller.snapshot()).toMatchObject({ phase: 'unconfirmed', retryChoice: 'approve' });
    f.advance(5000);
    f.read.mockRejectedValue(new ApiError('temporarily_unavailable', 'unknown', 503));
    await f.controller.refresh();
    expect(f.controller.snapshot()).toMatchObject({ phase: 'unconfirmed', retryChoice: 'approve' });
    await f.controller.decide('deny');
    expect(f.decide).toHaveBeenCalledTimes(1);
    expect(f.discover).toHaveBeenCalledTimes(1);
  });

  it('removes a pending query at expiry and suppresses an obsolete POST acknowledgement', async () => {
    const f = fixture(),
      pending = barrier<{
        request_id: string;
        revision: number;
        status: 'approved';
        expires_at: string;
      }>();
    await f.controller.refresh();
    f.decide.mockReturnValue(pending.promise);
    const flight = f.controller.decide('approve');
    await Promise.resolve();
    f.advance(60001);
    await f.controller.refresh();
    expect(f.controller.snapshot().phase).toBe('stopped');
    expect(f.controller.snapshot().view).toBeUndefined();
    pending.resolve({
      request_id: 'request-1',
      revision: 2,
      status: 'approved',
      expires_at: view.expires_at,
    });
    await flight;
    expect(f.controller.snapshot().phase).toBe('stopped');
  });

  it('permits a second distinct request only within the original Run review window', async () => {
    const f = fixture();
    await f.controller.refresh();
    await f.controller.decide('approve');
    f.advance(65000);
    const second = {
      ...view,
      request_id: 'request-2',
      query: 'second exact query',
      expires_at: new Date(epoch + 120000).toISOString(),
    };
    f.discover.mockResolvedValue({
      session_id: 'ses-1',
      run_id: 'run-1',
      pending: { attempt_id: 'attempt-1', request_id: 'request-2', expires_at: second.expires_at },
    });
    f.read.mockResolvedValue(second);
    await f.controller.refresh();
    expect(f.controller.snapshot()).toMatchObject({ phase: 'ready', view: second });
    f.advance(55001);
    await f.controller.refresh();
    expect(f.controller.snapshot().phase).toBe('stopped');
    await f.controller.decide('approve');
    expect(f.decide).toHaveBeenCalledTimes(1);
  });
});
