import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';

import {
  ApiError,
  type ConditionalResult,
  type EventPage,
  type RunPage,
  type SessionSummary,
} from '$lib/api/client';
import SessionDetail, { type SessionDetailApi } from './SessionDetail.svelte';

const session: SessionSummary = {
  session_id: 'ses-1',
  status: 'active',
  title: 'Canonical planning',
  last_sequence: 1,
  created_at: '2026-08-18T00:00:00Z',
  updated_at: '2026-08-18T01:00:00Z',
};

function fresh<T>(data: T): ConditionalResult<T> {
  return { state: 'fresh', data, etag: '"v1"', pollAfterMs: 15000 };
}

function api(overrides: Partial<SessionDetailApi> = {}): SessionDetailApi {
  return {
    getSession: vi.fn().mockResolvedValue(fresh(session)),
    listEvents: vi.fn().mockResolvedValue(
      fresh<EventPage>({
        items: [
          {
            event_id: 'evt-1',
            sequence: 1,
            kind: 'assistant_message',
            content: { text: '<img src=x onerror=alert(1)> Safe text' },
            created_at: '2026-08-18T01:00:00Z',
          },
        ],
      }),
    ),
    listRuns: vi.fn().mockResolvedValue(fresh<RunPage>({ items: [] })),
    getComputeStatus: vi.fn().mockResolvedValue({
      availability: 'ready',
      connection: {
        provider: 'openai',
        entitlement: 'active',
        quota: 'available',
        observed_at: '2026-08-18T01:00:00Z',
      },
    }),
    setSessionArchived: vi.fn().mockResolvedValue({ ...session, status: 'archived' }),
    createUpload: vi.fn(),
    commitUpload: vi.fn(),
    createMessage: vi.fn().mockResolvedValue({
      session_id: 'ses-1',
      event_id: 'evt-2',
      sequence: 2,
      run_id: 'run-1',
      created: true,
      compute: {
        provider: 'openai',
        entitlement: 'active',
        quota: 'available',
        observed_at: '2026-08-18T01:00:00Z',
      },
    }),
    getAttachmentCapability: vi.fn(),
    ...overrides,
  } as SessionDetailApi;
}

describe('SessionDetail', () => {
  it('shows bounded canonical history as escaped text and compute/quota status', async () => {
    const client = api();

    render(SessionDetail, { client, sessionId: 'ses-1' });

    expect(
      await screen.findByRole('heading', { name: 'Canonical planning', level: 1 }),
    ).toBeInTheDocument();
    expect(screen.getByText('<img src=x onerror=alert(1)> Safe text')).toBeInTheDocument();
    expect(document.querySelector('img')).toBeNull();
    expect(screen.getByRole('status', { name: '' })).toHaveTextContent('openai · quota available');
    expect(client.listEvents).toHaveBeenCalledWith(
      'ses-1',
      expect.objectContaining({ limit: 100 }),
    );
  });

  it('keeps authorized history available while disabling mutations for a read-only participant', async () => {
    const client = api({
      getComputeStatus: vi
        .fn()
        .mockRejectedValue(
          new ApiError('not_found', 'The requested resource is not available.', 404),
        ),
    });

    render(SessionDetail, { client, sessionId: 'ses-1' });

    expect(
      await screen.findByRole('heading', { name: 'Canonical planning', level: 1 }),
    ).toBeInTheDocument();
    expect(screen.getByText('<img src=x onerror=alert(1)> Safe text')).toBeInTheDocument();
    expect(screen.getByText(/Read-only access/)).toBeInTheDocument();
    expect(screen.getByLabelText('Message')).toBeDisabled();
    expect(screen.queryByRole('button', { name: 'Archive' })).not.toBeInTheDocument();
  });

  it('disables sending when the authoritative compute quota is exhausted', async () => {
    const client = api({
      getComputeStatus: vi.fn().mockResolvedValue({
        availability: 'ready',
        connection: {
          provider: 'openai',
          entitlement: 'active',
          quota: 'exhausted',
          observed_at: '2026-08-18T01:00:00Z',
        },
      }),
    });

    render(SessionDetail, { client, sessionId: 'ses-1' });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'hello' } });

    expect(screen.getByText('Compute quota is exhausted.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Send' })).toBeDisabled();
  });

  it('keeps the message idempotency key across an explicit retry', async () => {
    const createMessage = vi
      .fn()
      .mockRejectedValueOnce(new Error('transport detail'))
      .mockResolvedValueOnce({
        session_id: 'ses-1',
        event_id: 'evt-2',
        sequence: 2,
        run_id: 'run-1',
        created: true,
        compute: {
          provider: 'openai',
          entitlement: 'active',
          quota: 'available',
          observed_at: '2026-08-18T01:00:00Z',
        },
      });
    render(SessionDetail, { client: api({ createMessage }), sessionId: 'ses-1' });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'retry me' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Send' }));

    const retry = await screen.findByRole('button', { name: 'Retry send' });
    await fireEvent.click(retry);
    await waitFor(() => expect(createMessage).toHaveBeenCalledTimes(2));

    expect(createMessage.mock.calls[0]?.[1]?.idempotency_key).toBe(
      createMessage.mock.calls[1]?.[1]?.idempotency_key,
    );
    expect(createMessage.mock.calls[0]?.[1]).toMatchObject({ text: 'retry me' });
  });

  it('uses fresh upload and message idempotency keys after editing a failed submission', async () => {
    const createUpload = vi
      .fn()
      .mockResolvedValueOnce({
        upload_id: 'up-1',
        method: 'PUT',
        url: 'https://objects.example/one',
        headers: {},
        expires_at: '2026-08-18T02:00:00Z',
      })
      .mockResolvedValueOnce({
        upload_id: 'up-2',
        method: 'PUT',
        url: 'https://objects.example/two',
        headers: {},
        expires_at: '2026-08-18T02:00:00Z',
      });
    const commitUpload = vi
      .fn()
      .mockResolvedValueOnce({
        upload_id: 'up-1',
        name: 'note.txt',
        media_type: 'text/plain',
        size: 3,
      })
      .mockResolvedValueOnce({
        upload_id: 'up-2',
        name: 'note.txt',
        media_type: 'text/plain',
        size: 3,
      });
    const createMessage = vi
      .fn()
      .mockRejectedValueOnce(new Error('ambiguous response'))
      .mockResolvedValueOnce({
        session_id: 'ses-1',
        event_id: 'evt-2',
        sequence: 2,
        run_id: 'run-1',
        created: true,
        compute: {
          provider: 'openai',
          entitlement: 'active',
          quota: 'available',
          observed_at: '2026-08-18T01:00:00Z',
        },
      });
    render(SessionDetail, {
      client: api({ createUpload, commitUpload, createMessage }),
      sessionId: 'ses-1',
      hashFileFn: vi.fn().mockResolvedValue({
        sha256: 'a'.repeat(64),
        contentMD5: 'kAFQmDzST7DWlj99KOF/cg==',
      }),
      putUploadFn: vi.fn().mockResolvedValue(undefined),
    });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'first' } });
    await fireEvent.change(screen.getByLabelText('Attach files'), {
      target: { files: [new File(['abc'], 'note.txt', { type: 'text/plain' })] },
    });
    await fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await screen.findByRole('button', { name: 'Retry send' });

    await fireEvent.input(composer, { target: { value: 'changed' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Send' }));
    await waitFor(() => expect(createMessage).toHaveBeenCalledTimes(2));

    expect(createUpload).toHaveBeenCalledTimes(2);
    expect(createUpload.mock.calls[0]?.[0].idempotency_key).not.toBe(
      createUpload.mock.calls[1]?.[0].idempotency_key,
    );
    expect(createMessage.mock.calls[0]?.[1]?.idempotency_key).not.toBe(
      createMessage.mock.calls[1]?.[1]?.idempotency_key,
    );
    expect(createMessage.mock.calls[1]?.[1]).toMatchObject({
      text: 'changed',
      upload_ids: ['up-2'],
    });
  });

  it('archives through a fresh idempotent mutation and updates the composer state', async () => {
    const setSessionArchived = vi.fn().mockResolvedValue({ ...session, status: 'archived' });
    render(SessionDetail, { client: api({ setSessionArchived }), sessionId: 'ses-1' });

    await fireEvent.click(await screen.findByRole('button', { name: 'Archive' }));

    await waitFor(() => expect(setSessionArchived).toHaveBeenCalledOnce());
    expect(setSessionArchived.mock.calls[0]?.[1]).toMatchObject({ archived: true });
    expect(screen.getByLabelText('Message')).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Unarchive' })).toBeInTheDocument();
  });

  it('aborts participant reads and polling when navigation unmounts the view', async () => {
    let signal: AbortSignal | undefined;
    const getSession = vi.fn((_sessionId: string, _etag?: string, requestSignal?: AbortSignal) => {
      signal = requestSignal;
      return Promise.resolve(fresh(session));
    });
    const view = render(SessionDetail, {
      client: api({ getSession }),
      sessionId: 'ses-1',
    });
    await screen.findByRole('heading', { name: 'Canonical planning', level: 1 });

    expect(signal?.aborted).toBe(false);
    view.unmount();
    expect(signal?.aborted).toBe(true);
  });
});
