import { render, screen, within } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';

import { ApiError } from '$lib/api/client';
import { attachedWorkerDetail } from '$lib/attached-worker/test-fixtures';
import AttachedWorkerDetail from './AttachedWorkerDetail.svelte';

describe('AttachedWorkerDetail', () => {
  it('renders exactly six evidence cohorts without collapsing orthogonal states', async () => {
    const model = attachedWorkerDetail();
    model.connectivity.last_failure = {
      state: 'recorded',
      code: 'transport_unavailable',
      occurred_at: '2026-08-25T22:00:00Z',
      operation: 'poll',
      retry_class: 'retryable',
    };
    const getAttachedWorker = vi.fn().mockResolvedValue(model);

    render(AttachedWorkerDetail, {
      client: { getAttachedWorker },
      workerId: 'worker-one',
    });

    expect(await screen.findByRole('heading', { name: 'Studio Mac' })).toBeInTheDocument();
    expect(getAttachedWorker).toHaveBeenCalledWith('worker-one');

    const cohortHeadings = [
      'Identity and ownership',
      'Readiness and isolation',
      'Connectivity and presence',
      'Eligibility, capability, and capacity',
      'Execution and recovery',
      'Policy, lifecycle, and governance',
    ];
    for (const name of cohortHeadings) {
      expect(screen.getByRole('heading', { name })).toBeInTheDocument();
    }
    expect(document.querySelectorAll('section.cohort')).toHaveLength(6);
    expect(
      screen.getByRole('complementary', { name: 'All observation warnings' }),
    ).toBeInTheDocument();

    expect(screen.getByRole('heading', { name: 'Current observation' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Last daemon failure' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Current connection' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Last transport failure' })).toBeInTheDocument();
    expect(screen.getByText('Connection state').nextElementSibling).toHaveTextContent('Online');
    expect(screen.getByText('transport_unavailable')).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Advertised capability' })).toBeInTheDocument();
    expect(screen.getByRole('heading', { name: 'Admission preview' })).toBeInTheDocument();
    expect(screen.getByText('Not Evaluated')).toBeInTheDocument();

    expect(screen.getAllByText('Unsupported')).toHaveLength(2);
    expect(screen.getByText('Network Boundary, Process Boundary')).toBeInTheDocument();
    expect(screen.getByText('Quota remaining').nextElementSibling).toHaveTextContent('0');
    expect(screen.getByText('Zero')).toBeInTheDocument();
  });

  it('shows cancellation, process, worker terminal, and canonical evidence independently', async () => {
    render(AttachedWorkerDetail, {
      client: { getAttachedWorker: vi.fn().mockResolvedValue(attachedWorkerDetail()) },
      workerId: 'worker-one',
    });

    await screen.findByRole('heading', { name: 'Studio Mac' });
    const execution = screen
      .getByRole('heading', { name: 'Execution and recovery' })
      .closest('section');
    expect(execution).not.toBeNull();
    const view = within(execution as HTMLElement);

    expect(view.getByRole('heading', { name: 'Cancellation request' })).toBeInTheDocument();
    expect(view.getByText('Requested:').parentElement).toHaveTextContent('2026-08-26 07:59:50 UTC');
    expect(view.getByText('Acknowledgement deadline:').parentElement).toHaveTextContent(
      '2026-08-26 08:00:10 UTC',
    );
    expect(view.getByRole('heading', { name: 'Cancellation acknowledgement' })).toBeInTheDocument();
    expect(view.getByText('Pending')).toBeInTheDocument();
    expect(view.getAllByText('Revision 3')).toHaveLength(2);
    expect(view.getByText('Acknowledged:').parentElement).toHaveTextContent('Not recorded');

    expect(view.getByRole('heading', { name: 'Process observation' })).toBeInTheDocument();
    expect(view.getByText('Attempt:').parentElement).toHaveTextContent('attempt-one');
    expect(view.getByText('Lease generation: 5')).toBeInTheDocument();
    expect(view.getByText('Fence:').parentElement).toHaveTextContent('sha256:fence');
    expect(view.getByText('Source: Worker Report')).toBeInTheDocument();
    expect(view.getByText('Freshness: Fresh')).toBeInTheDocument();

    expect(view.getByRole('heading', { name: 'Worker terminal evidence' })).toBeInTheDocument();
    expect(view.getByText('Sequence: 11')).toBeInTheDocument();
    expect(view.getByText('Evidence:').parentElement).toHaveTextContent('sha256:terminal');
    expect(view.getByRole('heading', { name: 'Canonical terminal commit' })).toBeInTheDocument();
    expect(view.getByText('Not Committed')).toBeInTheDocument();
    expect(view.getByText('Committed:').parentElement).toHaveTextContent('Not recorded');
  });

  it('keeps disabled controls inert and renders their stable reason', async () => {
    render(AttachedWorkerDetail, {
      client: { getAttachedWorker: vi.fn().mockResolvedValue(attachedWorkerDetail()) },
      workerId: 'worker-one',
    });

    await screen.findByRole('heading', { name: 'Studio Mac' });
    expect(screen.queryByRole('button', { name: 'Revoke worker' })).not.toBeInTheDocument();
    const revoke = screen.getByText('Revoke worker').closest('li');
    expect(revoke).not.toBeNull();
    expect(
      within(revoke as HTMLElement).getByText('control_contract_unavailable'),
    ).toBeInTheDocument();
    expect(revoke).toHaveTextContent('Unavailable — Control contract is unavailable');
    expect(screen.getByText('Not Acknowledged')).toBeInTheDocument();
  });

  it.each(['unknown', 'zero', 'exhausted'])(
    'renders the quota state %s without inference',
    async (state) => {
      const model = attachedWorkerDetail();
      model.resource.quota.state = state;
      if (state !== 'zero') delete model.resource.quota.remaining;

      render(AttachedWorkerDetail, {
        client: { getAttachedWorker: vi.fn().mockResolvedValue(model) },
        workerId: 'worker-one',
      });

      await screen.findByRole('heading', { name: 'Studio Mac' });
      expect(screen.getByText('Quota state').nextElementSibling).toHaveTextContent(
        state.charAt(0).toUpperCase() + state.slice(1),
      );
    },
  );

  it('renders the owner-safe not-found response without exposing a locator reason', async () => {
    render(AttachedWorkerDetail, {
      client: {
        getAttachedWorker: vi
          .fn()
          .mockRejectedValue(
            new ApiError('not_found', 'The requested resource is not available.', 404),
          ),
      },
      workerId: 'worker-other',
    });

    expect(
      await screen.findByRole('heading', { name: 'This worker cannot be opened' }),
    ).toBeInTheDocument();
    expect(screen.queryByText('not_found')).not.toBeInTheDocument();
  });
});
