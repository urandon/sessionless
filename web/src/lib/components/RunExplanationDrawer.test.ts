import { fireEvent, render, screen, within } from '@testing-library/svelte';
import { tick } from 'svelte';
import { describe, expect, it, vi } from 'vitest';
import type { RunExplanationState } from '$lib/run-explanation/controller';
import { explanation } from '$lib/run-explanation/test-fixtures';
import { validateRunExplanation } from '$lib/run-explanation/validate';
import RunExplanationDrawer from './RunExplanationDrawer.svelte';

function ready(evidence = explanation()): RunExplanationState {
  validateRunExplanation(evidence, evidence.run_id, evidence.session_id);
  return { phase: 'ready', evidence, nextRefreshAt: 0 };
}

function props(state = ready()) {
  return { state, onClose: vi.fn(), onRefresh: vi.fn(), refreshDisabled: false };
}

describe('RunExplanationDrawer', () => {
  it('focuses the heading once and exposes a nonmodal complementary panel with native controls', async () => {
    const input = props();
    const view = render(RunExplanationDrawer, input);
    const heading = screen.getByRole('heading', { name: 'Run explanation' });
    const panel = screen.getByRole('complementary', { name: 'Run explanation' });
    expect(heading).toHaveFocus();
    expect(panel).not.toHaveAttribute('aria-modal');
    expect(screen.queryByRole('dialog')).not.toBeInTheDocument();
    const refresh = screen.getByRole('button', { name: 'Refresh explanation' });
    refresh.focus();
    await fireEvent.click(refresh);
    expect(input.onRefresh).toHaveBeenCalledTimes(1);
    const newer = explanation();
    newer.read_at = '2026-10-10T12:00:20Z';
    await view.rerender({ ...input, state: ready(newer) });
    expect(refresh).toHaveFocus();
    await fireEvent.keyDown(refresh, { key: 'Escape' });
    expect(input.onClose).toHaveBeenCalledTimes(1);
    await fireEvent.click(screen.getByRole('button', { name: 'Close explanation' }));
    expect(input.onClose).toHaveBeenCalledTimes(2);
    await fireEvent.keyDown(document.body, { key: 'Escape' });
    expect(input.onClose).toHaveBeenCalledTimes(2);
    // Nonmodal navigation does not contain focus inside this panel.
    const outside = document.createElement('button');
    document.body.append(outside);
    try {
      outside.focus();
      await tick();
      expect(outside).toHaveFocus();
    } finally {
      outside.remove();
    }
  });

  it('separates authoritative state, selected Attempt and server-expired receipt evidence', () => {
    render(RunExplanationDrawer, props());
    const run = screen.getByRole('region', { name: 'Canonical Run' });
    expect(within(run).getByText('Running')).toBeInTheDocument();
    expect(within(run).getByText('run-1')).toBeInTheDocument();
    expect(within(run).getByText('2026-10-10T12:00:00Z')).toBeInTheDocument();
    const admission = screen.getByRole('region', { name: 'Last recorded admission decision' });
    expect(admission).toHaveTextContent('Admitted — Admission was recorded.');
    expect(admission).toHaveTextContent('Last recorded decision only');
    expect(admission).toHaveTextContent('2026-10-10T12:00:01Z');
    const attempt = screen.getByRole('region', { name: 'Selected Attempt' });
    expect(attempt).toHaveTextContent('att-1');
    expect(attempt).toHaveTextContent('not a physical-process heartbeat');
    const attached = screen.getByRole('region', { name: 'Attached receipt observation' });
    expect(attached).toHaveTextContent('A claim was recorded.');
    expect(attached).toHaveTextContent('Expired at the server read time');
    expect(within(attached).getByText('Lease valid until')).toBeInTheDocument();
    expect(within(attached).getByText('2026-10-10T12:00:10Z')).toBeInTheDocument();
    expect(attached).toHaveTextContent('separate from canonical completion');
    expect(screen.getByRole('region', { name: 'Per-source coverage' })).toHaveTextContent(
      'Recorded',
    );
    expect(screen.getByRole('region', { name: 'Canonical terminal reason' })).toHaveTextContent(
      'Not applicable',
    );
    expect(
      screen.queryByRole('button', { name: /stop|retry|compute|probe/i }),
    ).not.toBeInTheDocument();
  });

  it('keeps all unknown groups explicit, including an authoritative terminal Run without its reason', () => {
    const value = explanation();
    value.status = 'succeeded';
    value.finished_at = value.updated_at;
    value.admission = { availability: 'unknown' };
    value.terminal = { availability: 'unknown' };
    value.attempt = { availability: 'unknown' };
    value.attached = { availability: 'unknown' };
    value.coverage = {
      admission: 'unknown',
      terminal: 'unknown',
      attempt: 'unknown',
      operational: 'unknown',
    };
    render(RunExplanationDrawer, props(ready(value)));
    expect(screen.getByRole('region', { name: 'Canonical Run' })).toHaveTextContent('Succeeded');
    expect(screen.getByText(/Partial evidence:/)).toBeInTheDocument();
    for (const name of [
      'Last recorded admission decision',
      'Canonical terminal reason',
      'Selected Attempt',
      'Attached receipt observation',
    ]) {
      expect(screen.getByRole('region', { name })).toHaveTextContent('Unknown:');
    }
    expect(
      within(screen.getByRole('region', { name: 'Per-source coverage' })).getAllByText('Unknown'),
    ).toHaveLength(4);
    expect(screen.getByRole('region', { name: 'Attached receipt observation' })).toHaveTextContent(
      'Managed remote-process observation is unknown',
    );
  });

  it.each([
    ['admitted', 'Admission was recorded.'],
    ['subscription_attention_required', 'Subscription attention was required.'],
    ['capacity_draining', 'Capacity was draining.'],
    ['quota_reset_pending', 'Quota reset was pending.'],
    ['quota_exhausted_reset_unknown', 'Quota was exhausted; reset time was unknown.'],
    ['runtime_limit_exceeded', 'The runtime limit was exceeded.'],
    ['turn_limit_exceeded', 'The turn limit was exceeded.'],
    ['input_limit_exceeded', 'The input limit was exceeded.'],
    ['context_limit_exceeded', 'The context limit was exceeded.'],
    ['artifact_limit_exceeded', 'The artifact limit was exceeded.'],
    ['capacity_busy', 'Capacity was busy.'],
    ['workspace_queue_limit', 'The workspace queue limit was reached.'],
    ['workspace_active_run_limit', 'The workspace active Run limit was reached.'],
  ] as const)('uses fixed historical admission copy for %s', (reason, copy) => {
    const value = explanation();
    value.admission.reason_code = reason;
    value.admission.outcome = reason === 'admitted' ? 'admitted' : 'denied';
    value.status = reason === 'admitted' ? 'running' : 'quota_blocked';
    render(RunExplanationDrawer, props(ready(value)));
    const region = screen.getByRole('region', { name: 'Last recorded admission decision' });
    expect(region).toHaveTextContent(copy);
    expect(region).toHaveTextContent('not a current entitlement guarantee');
    expect(region).toHaveTextContent('2026-10-10T12:00:01Z');
    expect(region).not.toHaveTextContent(reason === 'admitted' ? 'denied' : reason);
  });

  it.each([
    ['execution_failed', 'An execution phase failure was recorded.'],
    ['result_persistence_failed', 'A result persistence failure was recorded.'],
    ['canonical_cancelled', 'Canonical cancellation was recorded.'],
    ['unclassified_failure', 'A failure notice was recorded; its detailed cause is unknown.'],
  ] as const)(
    'uses fixed terminal phase copy for %s without guessing a root cause',
    (reason, copy) => {
      const value = explanation();
      value.status = reason === 'canonical_cancelled' ? 'cancelled' : 'failed';
      value.finished_at = value.updated_at;
      value.terminal = {
        availability: 'recorded',
        reason_code: reason,
        observed_at: value.finished_at,
        event_id: 'evt-1',
        event_sequence: 1,
      };
      value.coverage.terminal = 'recorded';
      render(RunExplanationDrawer, props(ready(value)));
      const region = screen.getByRole('region', { name: 'Canonical terminal reason' });
      expect(region).toHaveTextContent(copy);
      expect(region).toHaveTextContent('not an independently diagnosed root cause');
      expect(region).not.toHaveTextContent(reason);
      // Historical admission is not rewritten as the failed execution's cause.
      expect(
        screen.getByRole('region', { name: 'Last recorded admission decision' }),
      ).toHaveTextContent('Admitted');
    },
  );

  it.each([
    ['offer_recorded', 'An offer was recorded.', 'within_lease'],
    ['claim_recorded', 'A claim was recorded.', 'expired'],
    [
      'terminal_candidate_recorded',
      'A terminal candidate was recorded, not canonical completion.',
      'expired',
    ],
    ['terminal_commit_recorded', 'A terminal commit receipt was recorded.', 'durable'],
    [
      'fenced_outcome_unknown',
      'A fenced receipt was recorded; the physical outcome is unknown.',
      'durable',
    ],
    ['receipt_retired', 'The receipt was retired.', 'durable'],
  ] as const)(
    'uses closed attached fact copy for %s and only server freshness',
    (fact, copy, freshness) => {
      const value = explanation();
      value.attached.fact = fact;
      value.attached.freshness = freshness;
      if (freshness === 'durable') delete value.attached.valid_until;
      if (freshness === 'within_lease') value.attached.valid_until = '2026-10-10T12:00:30Z';
      render(RunExplanationDrawer, props(ready(value)));
      const region = screen.getByRole('region', { name: 'Attached receipt observation' });
      expect(region).toHaveTextContent(copy);
      expect(region).toHaveTextContent(
        freshness === 'durable'
          ? 'Durable historical receipt'
          : freshness === 'within_lease'
            ? 'Within lease at the server read time'
            : 'Expired at the server read time',
      );
      expect(region).not.toHaveTextContent(fact);
      if (freshness === 'durable')
        expect(within(region).queryByText('Lease valid until')).not.toBeInTheDocument();
      expect(screen.getByRole('region', { name: 'Canonical Run' })).toHaveTextContent('Running');
    },
  );

  it('keeps failed refresh distinct from original recorded evidence and never steals focus', async () => {
    const input = props();
    const view = render(RunExplanationDrawer, input);
    const close = screen.getByRole('button', { name: 'Close explanation' });
    close.focus();
    await view.rerender({
      ...input,
      state: {
        ...input.state,
        phase: 'refresh-failed',
        refreshError: { code: 'temporarily_unavailable', status: 503 },
      },
    });
    expect(screen.getByText(/Evidence refresh failed/)).toHaveTextContent('original read time');
    expect(screen.getByText(/Previously read at/)).toHaveTextContent('2026-10-10T12:00:10Z');
    expect(screen.getByRole('region', { name: 'Canonical Run' })).toHaveTextContent('Running');
    expect(close).toHaveFocus();
    expect(screen.getByRole('status')).toHaveTextContent('Refresh failed');
  });

  it('shows initial loading and failed reads without fabricating any evidence', async () => {
    const input = props({ phase: 'loading', nextRefreshAt: 0 });
    const view = render(RunExplanationDrawer, input);
    expect(screen.getByRole('button', { name: 'Refresh explanation' })).toBeDisabled();
    expect(screen.getByText(/Execution details are not yet available/)).toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Canonical Run' })).not.toBeInTheDocument();
    await view.rerender({
      ...input,
      state: {
        phase: 'refresh-failed',
        nextRefreshAt: 0,
        refreshError: { code: 'rate_limited', status: 429 },
      },
      refreshDisabled: true,
    });
    expect(screen.getByText(/Refresh was rate limited/)).toHaveTextContent(
      'no successful read is available',
    );
    screen.getByRole('button', { name: 'Refresh explanation' }).click();
    expect(input.onRefresh).not.toHaveBeenCalled();
  });

  it('removes even accidentally retained evidence and disables refresh after permission loss', async () => {
    const input = props();
    const view = render(RunExplanationDrawer, input);
    await view.rerender({
      ...input,
      state: {
        ...input.state,
        phase: 'permission-lost',
        refreshError: { code: 'access_denied', status: 403 },
      },
    });
    expect(
      screen.getByText(/Run evidence is no longer available in this scope/),
    ).toBeInTheDocument();
    expect(screen.queryByText('run-1')).not.toBeInTheDocument();
    expect(screen.queryByText('att-1')).not.toBeInTheDocument();
    expect(screen.queryByRole('region', { name: 'Canonical Run' })).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh explanation' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Close explanation' })).toBeEnabled();
  });

  it('announces material changes but does not repeat facts for read_at or spinner-only changes', async () => {
    const input = props();
    const view = render(RunExplanationDrawer, input);
    const status = screen.getByRole('status');
    const initial = status.textContent;
    const changes: string[] = [];
    const observer = new MutationObserver(() => changes.push(status.textContent ?? ''));
    observer.observe(status, { subtree: true, childList: true, characterData: true });
    try {
      await view.rerender({ ...input, state: { ...input.state, phase: 'loading' } });
      const newer = explanation();
      newer.read_at = '2026-10-10T12:00:20Z';
      await view.rerender({ ...input, state: ready(newer) });
      await tick();
      expect(status.textContent).toBe(initial);
      expect(changes).toEqual([]);
      newer.status = 'failed';
      newer.finished_at = newer.updated_at;
      newer.terminal = { availability: 'unknown' };
      newer.coverage.terminal = 'unknown';
      await view.rerender({ ...input, state: ready(newer) });
      expect(status).toHaveTextContent('Canonical Run: Failed');
      expect(status).toHaveTextContent('Coverage is partial');
      expect(changes).toHaveLength(1);
    } finally {
      observer.disconnect();
    }
  });

  it('announces a changed recorded reason even when the canonical blocked status is unchanged', async () => {
    const value = explanation();
    value.status = 'quota_blocked';
    value.admission.outcome = 'denied';
    value.admission.reason_code = 'quota_reset_pending';
    const input = props(ready(value));
    const view = render(RunExplanationDrawer, input);
    expect(screen.getByRole('status')).toHaveTextContent('Quota reset was pending.');
    const changed = structuredClone(value);
    changed.admission.reason_code = 'capacity_busy';
    await view.rerender({ ...input, state: ready(changed) });
    expect(screen.getByRole('status')).toHaveTextContent('Capacity was busy.');
    expect(screen.getByRole('status')).not.toHaveTextContent('Quota reset was pending.');
  });
});
