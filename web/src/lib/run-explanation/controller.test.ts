import { describe, expect, it, vi } from 'vitest';
import { ApiError, type RunExplanationV1 } from '../api/client';
import { RunExplanationController, type RunExplanationScope } from './controller';
import { barrier, explanation } from './test-fixtures';

const initialScope: RunExplanationScope = {
  webIdentity: 'usr-1:web-session-1',
  tenantId: 'ten-1',
  sessionId: 'ses-1',
  runId: 'run-1',
};

function setup() {
  let now = 1000;
  const read = vi
    .fn<(run: string, session: string, signal: AbortSignal) => Promise<RunExplanationV1>>()
    .mockImplementation(async () => explanation());
  const controller = new RunExplanationController({ read, now: () => now });
  controller.setScope(initialScope);
  return {
    controller,
    read,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

describe('RunExplanationController', () => {
  it('retains cadence/backoff across explicit permission-scope discard and reselect', async () => {
    const { controller, read, advance } = setup();
    read.mockRejectedValueOnce(new ApiError('rate_limited', 'safe', 429, undefined, 12000));
    await controller.refresh();
    controller.setScope(undefined);
    controller.setScope(initialScope);
    advance(11999);
    await controller.refresh();
    expect(read).toHaveBeenCalledOnce();
    advance(1);
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(2);
    controller.dispose();
  });

  it('fences a queued read before it starts when hidden synchronously', async () => {
    const { controller, read } = setup();
    const request = controller.refresh();
    controller.setVisible(false);
    await request;
    expect(read).not.toHaveBeenCalled();
    expect(controller.snapshot().evidence).toBeUndefined();
    controller.dispose();
  });

  it('has one flight and a five-second floor without scheduling refreshes', async () => {
    const { controller, read, advance } = setup();
    const pending = barrier<RunExplanationV1>(),
      started = barrier<void>();
    read.mockImplementation(() => {
      started.resolve();
      return pending.promise;
    });
    const first = controller.refresh();
    expect(controller.refresh()).toBe(first);
    await started.promise;
    advance(5000);
    expect(controller.refresh()).toBe(first);
    expect(read).toHaveBeenCalledOnce();
    pending.resolve(explanation());
    await first;
    read.mockImplementation(async () => explanation());
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(2);
    advance(4999);
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(2);
    advance(1);
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(3);
    advance(60000);
    expect(read).toHaveBeenCalledTimes(3);
    controller.dispose();
  });

  it('keeps a server backoff across Runs in the same identity/tenant', async () => {
    const { controller, read, advance } = setup();
    read.mockRejectedValueOnce(new ApiError('rate_limited', 'safe', 429, undefined, 12000));
    await controller.refresh();
    expect(controller.snapshot()).toMatchObject({ phase: 'refresh-failed', nextRefreshAt: 13000 });
    controller.setScope({ ...initialScope, runId: 'run-2' });
    advance(11999);
    await controller.refresh();
    expect(read).toHaveBeenCalledOnce();
    advance(1);
    read.mockImplementation(async () => ({ ...explanation(), run_id: 'run-2' }));
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(2);
    controller.dispose();
  });

  it('does not replace server evidence freshness on an ordinary temporary refresh failure', async () => {
    const { controller, read, advance } = setup();
    await controller.refresh();
    advance(5000);
    read.mockRejectedValueOnce(new Error('private-transport-detail'));
    await controller.refresh();
    expect(controller.snapshot()).toMatchObject({
      phase: 'refresh-failed',
      evidence: explanation(),
      refreshError: { code: 'temporarily_unavailable', status: 503 },
    });
    expect(controller.snapshot().evidence?.attached.freshness).toBe('expired');
    expect(JSON.stringify(controller.snapshot())).not.toContain('private-transport-detail');
    controller.dispose();
  });

  it.each([401, 403, 404])(
    'clears evidence and stops on permission/resource loss %i',
    async (status) => {
      const { controller, read, advance } = setup();
      await controller.refresh();
      advance(5000);
      read.mockRejectedValueOnce(
        new ApiError(
          status === 401 ? 'unauthenticated' : status === 403 ? 'access_denied' : 'not_found',
          'safe',
          status,
        ),
      );
      await controller.refresh();
      expect(controller.snapshot()).toMatchObject({
        phase: 'permission-lost',
        refreshError: { status },
      });
      expect(controller.snapshot().evidence).toBeUndefined();
      advance(60000);
      await controller.refresh();
      expect(read).toHaveBeenCalledTimes(2);
      controller.dispose();
    },
  );

  it.each(['hidden', 'unmount', 'dispose', 'permission'])(
    'aborts and fences late success after %s even if reader ignores abort',
    async (action) => {
      const { controller, read, advance } = setup();
      await controller.refresh();
      advance(5000);
      const pending = barrier<RunExplanationV1>(),
        started = barrier<void>();
      let signal: AbortSignal | undefined;
      read.mockImplementation((_run, _session, supplied) => {
        signal = supplied;
        started.resolve();
        return pending.promise;
      });
      const refresh = controller.refresh();
      await started.promise;
      if (action === 'hidden' || action === 'unmount') controller.setVisible(false);
      else if (action === 'dispose') controller.dispose();
      else controller.setScope(undefined);
      expect(signal?.aborted).toBe(true);
      expect(controller.snapshot().evidence).toBeUndefined();
      pending.resolve(explanation());
      await refresh;
      expect(controller.snapshot().evidence).toBeUndefined();
      advance(60000);
      await controller.refresh();
      expect(read).toHaveBeenCalledTimes(2);
      controller.dispose();
    },
  );

  it.each(['webIdentity', 'tenantId', 'sessionId', 'runId'] as const)(
    'keys private memory by %s and fences old generation',
    async (field) => {
      const { controller, read, advance } = setup();
      const pending = barrier<RunExplanationV1>(),
        started = barrier<void>();
      read.mockImplementationOnce(() => {
        started.resolve();
        return pending.promise;
      });
      const old = controller.refresh();
      await started.promise;
      const scope = { ...initialScope, [field]: `${initialScope[field]}-other` };
      controller.setScope(scope);
      expect(controller.snapshot().evidence).toBeUndefined();
      advance(5000);
      read.mockImplementation(async () => ({
        ...explanation(),
        run_id: scope.runId,
        session_id: scope.sessionId,
      }));
      await controller.refresh();
      pending.resolve(explanation());
      await old;
      expect(controller.snapshot().evidence).toMatchObject({
        run_id: scope.runId,
        session_id: scope.sessionId,
      });
      expect(read.mock.calls[0]?.[2].aborted).toBe(true);
      controller.dispose();
    },
  );

  it.each(['run_id', 'session_id'] as const)(
    'rejects mismatched %s even from a custom reader',
    async (field) => {
      const { controller, read } = setup();
      read.mockResolvedValueOnce({ ...explanation(), [field]: 'foreign' });
      await controller.refresh();
      expect(controller.snapshot()).toMatchObject({
        phase: 'refresh-failed',
        refreshError: { status: 503 },
      });
      expect(controller.snapshot().evidence).toBeUndefined();
      controller.dispose();
    },
  );

  it('does not let snapshots/listeners mutate private memory and unsubscribes/disposes cleanly', async () => {
    const { controller, read, advance } = setup();
    const listener = vi.fn((state) => {
      if (state.evidence) state.evidence.run_id = 'mutated';
    });
    const unsubscribe = controller.subscribe(listener);
    await controller.refresh();
    const copy = controller.snapshot();
    if (copy.evidence) copy.evidence.run_id = 'changed';
    expect(controller.snapshot().evidence?.run_id).toBe('run-1');
    unsubscribe();
    const count = listener.mock.calls.length;
    controller.dispose();
    controller.dispose();
    controller.setScope(initialScope);
    advance(5000);
    await controller.refresh();
    expect(listener).toHaveBeenCalledTimes(count);
    expect(read).toHaveBeenCalledOnce();
  });

  it('blocks requests while hidden and permits explicit visible refresh without retaining old evidence', async () => {
    const { controller, read, advance } = setup();
    await controller.refresh();
    controller.setVisible(false);
    advance(5000);
    await controller.refresh();
    expect(read).toHaveBeenCalledOnce();
    controller.setVisible(true);
    expect(controller.snapshot().evidence).toBeUndefined();
    expect(read).toHaveBeenCalledOnce();
    await controller.refresh();
    expect(read).toHaveBeenCalledTimes(2);
    controller.dispose();
  });
});
