import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';

import { attachedWorkerList, attachedWorkerSummary } from '$lib/attached-worker/test-fixtures';
import AttachedWorkerOverview from './AttachedWorkerOverview.svelte';

describe('AttachedWorkerOverview', () => {
  it('keeps current state, freshness, attempt, and every warning separate', async () => {
    const listAttachedWorkers = vi.fn().mockResolvedValue(attachedWorkerList());

    render(AttachedWorkerOverview, { client: { listAttachedWorkers } });

    expect(await screen.findByRole('heading', { name: 'Studio Mac' })).toBeInTheDocument();
    expect(listAttachedWorkers).toHaveBeenCalledWith({ limit: 20 });
    expect(screen.getByText('Contact freshness')).toBeInTheDocument();
    expect(screen.getByText('Cancel Requested')).toBeInTheDocument();
    expect(screen.getByText('Isolation is unsupported')).toBeInTheDocument();
    expect(screen.getByText('isolation_unsupported')).toBeInTheDocument();
    expect(screen.getByText('Quota is zero')).toBeInTheDocument();
    expect(screen.getByText('quota_zero')).toBeInTheDocument();
    expect(screen.queryByText(/healthy/i)).not.toBeInTheDocument();

    const evaluated = document.querySelector('time');
    expect(evaluated).toHaveAttribute('datetime', '2026-08-26T08:00:00.123456Z');
    expect(evaluated).toHaveTextContent('2026-08-26 08:00:00.123456 UTC');
  });

  it('loads bounded pages only after an explicit action', async () => {
    const first = {
      ...attachedWorkerList(),
      has_more: true,
      next_worker_id: 'worker-one',
    };
    const second = attachedWorkerList([
      attachedWorkerSummary('worker-one'),
      attachedWorkerSummary('worker-two'),
    ]);
    second.evaluated_at = '2026-08-26T08:01:00Z';
    second.items[0]!.evaluated_at = '2026-08-26T08:01:00Z';
    second.items[1]!.evaluated_at = '2026-08-26T08:01:00Z';
    const listAttachedWorkers = vi.fn().mockResolvedValueOnce(first).mockResolvedValueOnce(second);

    render(AttachedWorkerOverview, { client: { listAttachedWorkers } });

    const loadMore = await screen.findByRole('button', { name: 'Load more workers' });
    expect(listAttachedWorkers).toHaveBeenCalledTimes(1);

    await fireEvent.click(loadMore);
    await screen.findByRole('heading', { name: 'Build server' });
    await waitFor(() => expect(listAttachedWorkers).toHaveBeenCalledTimes(2));
    expect(listAttachedWorkers).toHaveBeenLastCalledWith({
      afterWorkerId: 'worker-one',
      limit: 20,
    });
    expect(screen.getByText('1 more worker loaded.')).toBeInTheDocument();
    expect(screen.getAllByRole('heading', { name: 'Studio Mac' })).toHaveLength(1);
    expect(
      document.querySelector('time[datetime="2026-08-26T08:00:00.123456Z"]'),
    ).toBeInTheDocument();
    expect(document.querySelector('time[datetime="2026-08-26T08:01:00Z"]')).toBeInTheDocument();
  });

  it('renders a bounded empty state without polling', async () => {
    const listAttachedWorkers = vi.fn().mockResolvedValue(attachedWorkerList([]));

    render(AttachedWorkerOverview, { client: { listAttachedWorkers } });

    expect(await screen.findByRole('heading', { name: 'No attached workers' })).toBeInTheDocument();
    await Promise.resolve();
    expect(listAttachedWorkers).toHaveBeenCalledTimes(1);
  });

  it('renders a safe failure with an explicit retry only', async () => {
    const listAttachedWorkers = vi.fn().mockRejectedValue(new Error('private-token'));

    render(AttachedWorkerOverview, { client: { listAttachedWorkers } });

    expect(
      await screen.findByRole('heading', { name: 'Attached workers cannot be loaded' }),
    ).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent(
      'Attached workers are temporarily unavailable.',
    );
    expect(screen.queryByText('private-token')).not.toBeInTheDocument();
    expect(listAttachedWorkers).toHaveBeenCalledTimes(1);
  });
});
