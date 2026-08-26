import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { ApiError } from '$lib/api/client';
import { serializeDiagnosticBundleV1 } from '$lib/attached-worker/diagnostics';
import { attachedWorkerDiagnostics } from '$lib/attached-worker/test-fixtures';
import AttachedWorkerDiagnostics from './AttachedWorkerDiagnostics.svelte';

describe('AttachedWorkerDiagnostics', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('loads only after an explicit gesture and renders bounded evidence without inference', async () => {
    const getAttachedWorkerDiagnostics = vi.fn().mockResolvedValue(attachedWorkerDiagnostics());
    render(AttachedWorkerDiagnostics, {
      client: { getAttachedWorkerDiagnostics },
      workerId: 'worker-one',
    });

    expect(getAttachedWorkerDiagnostics).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    expect(await screen.findByText('Redacted diagnostics loaded.')).toBeInTheDocument();
    expect(screen.getByRole('region', { name: 'Loaded redacted diagnostics' })).toHaveFocus();
    expect(getAttachedWorkerDiagnostics).toHaveBeenCalledTimes(1);
    expect(getAttachedWorkerDiagnostics).toHaveBeenCalledWith('worker-one');
    expect(screen.getAllByRole('region', { name: /diagnostic facts$/ })).toHaveLength(6);
    expect(screen.getByText('canonical_terminal')).toBeInTheDocument();
    expect(screen.getByText('Not Evaluated')).toBeInTheDocument();
    expect(screen.getAllByText('Freshness: Fresh')).toHaveLength(3);
    expect(screen.getAllByText('Not recorded').length).toBeGreaterThan(0);
    expect(screen.getByText('quota_zero')).toBeInTheDocument();
  });

  it('copies only the exact visible allowlisted bundle and never performs another request', async () => {
    const diagnostics = attachedWorkerDiagnostics() as ReturnType<
      typeof attachedWorkerDiagnostics
    > & {
      private_token?: string;
    };
    diagnostics.private_token = 'must-not-leak';
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText },
    });
    const getAttachedWorkerDiagnostics = vi.fn().mockResolvedValue(diagnostics);
    render(AttachedWorkerDiagnostics, {
      client: { getAttachedWorkerDiagnostics },
      workerId: 'worker-one',
    });

    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    const report = await screen.findByLabelText('Redacted diagnostic JSON');
    const expected = serializeDiagnosticBundleV1(diagnostics);
    expect(report).toHaveValue(expected);
    expect(expected).not.toContain('must-not-leak');

    await fireEvent.click(screen.getByRole('button', { name: 'Copy redacted diagnostics' }));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(expected));
    expect(screen.getByRole('status')).toHaveTextContent('Redacted diagnostics copied.');
    expect(getAttachedWorkerDiagnostics).toHaveBeenCalledTimes(1);
  });

  it('keeps the report selectable and announces a sanitized clipboard failure', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: { writeText: vi.fn().mockRejectedValue(new Error('secret clipboard detail')) },
    });
    render(AttachedWorkerDiagnostics, {
      client: {
        getAttachedWorkerDiagnostics: vi.fn().mockResolvedValue(attachedWorkerDiagnostics()),
      },
      workerId: 'worker-one',
    });

    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    await screen.findByLabelText('Redacted diagnostic JSON');
    await fireEvent.click(screen.getByRole('button', { name: 'Copy redacted diagnostics' }));

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Copy failed. The redacted JSON remains available for manual selection.',
    );
    expect(screen.queryByText('secret clipboard detail')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Redacted diagnostic JSON')).toHaveValue(
      serializeDiagnosticBundleV1(attachedWorkerDiagnostics()),
    );
  });

  it('downloads the same bounded bytes under a static filename and revokes the object URL', async () => {
    const diagnostics = attachedWorkerDiagnostics();
    const createObjectURL = vi.fn().mockReturnValue('blob:diagnostics');
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: createObjectURL });
    Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: revokeObjectURL });
    let download = '';
    vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (
      this: HTMLAnchorElement,
    ) {
      download = this.download;
    });
    render(AttachedWorkerDiagnostics, {
      client: { getAttachedWorkerDiagnostics: vi.fn().mockResolvedValue(diagnostics) },
      workerId: 'worker-one',
    });

    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    await screen.findByLabelText('Redacted diagnostic JSON');
    await fireEvent.click(screen.getByRole('button', { name: 'Download redacted JSON' }));

    expect(download).toBe('sessionless-attached-worker-diagnostics-v1.json');
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    const blob = createObjectURL.mock.calls[0]?.[0] as Blob;
    expect(await blob.text()).toBe(serializeDiagnosticBundleV1(diagnostics));
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:diagnostics');
    expect(screen.getByRole('status')).toHaveTextContent('Redacted diagnostics downloaded.');
  });

  it('collapses owner-safe denial and rejects a mismatched worker selector', async () => {
    const denied = vi
      .fn()
      .mockRejectedValue(new ApiError('access_denied', 'private policy detail', 403));
    const view = render(AttachedWorkerDiagnostics, {
      client: { getAttachedWorkerDiagnostics: denied },
      workerId: 'worker-one',
    });
    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    expect(
      await screen.findByText(
        'Diagnostics are unavailable for this worker in the active workspace.',
      ),
    ).toBeInTheDocument();
    expect(screen.queryByText('private policy detail')).not.toBeInTheDocument();

    view.unmount();
    const mismatched = attachedWorkerDiagnostics();
    mismatched.worker_id = 'worker-other';
    render(AttachedWorkerDiagnostics, {
      client: { getAttachedWorkerDiagnostics: vi.fn().mockResolvedValue(mismatched) },
      workerId: 'worker-one',
    });
    await fireEvent.click(screen.getByRole('button', { name: 'Load redacted diagnostics' }));
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'Diagnostics are temporarily unavailable.',
    );
    expect(screen.queryByText('worker-other')).not.toBeInTheDocument();
  });
});
