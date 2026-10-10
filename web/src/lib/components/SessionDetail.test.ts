import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import { tick } from 'svelte';

import {
  ApiError,
  type ConditionalResult,
  type EventPage,
  type Identity,
  type RunPage,
  type SessionSummary,
} from '$lib/api/client';
import SessionDetail, { type SessionDetailApi } from './SessionDetail.svelte';
import { barrier, explanation } from '$lib/run-explanation/test-fixtures';

const identity: Identity = {
  user_id: 'usr-1',
  provider: 'yandex',
  tenants: [{ tenant_id: 'ten-1', role: 'owner', active: true }],
};

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
    getIdentity: vi.fn().mockResolvedValue(identity),
    getRunExplanation: vi.fn().mockResolvedValue(explanation()),
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

describe('SessionDetail explanation lifecycle', () => {
  it('keeps default builds free of the drawer and additional identity reads', async () => {
    const client = withRun();
    render(SessionDetail, { client, sessionId: 'ses-1' });
    expect(await screen.findByText('Explain this execution')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Explain run' })).not.toBeInTheDocument();
    expect(client.getIdentity).not.toHaveBeenCalled();
    expect(client.getRunExplanation).not.toHaveBeenCalled();
  });

  function withRun(overrides: Partial<SessionDetailApi> = {}): SessionDetailApi {
    return api({
      listEvents: vi.fn().mockResolvedValue(
        fresh<EventPage>({
          items: [
            {
              event_id: 'evt-1',
              sequence: 1,
              kind: 'user_message',
              content: { text: 'Explain this execution' },
              created_at: '2026-10-10T12:00:00Z',
            },
          ],
        }),
      ),
      listRuns: vi.fn().mockResolvedValue(
        fresh<RunPage>({
          items: [
            {
              run_id: 'run-1',
              session_id: 'ses-1',
              trigger_event_id: 'evt-1',
              subscription_connection_id: 'con-1',
              status: 'running',
              created_at: '2026-10-10T12:00:00Z',
              updated_at: '2026-10-10T12:00:01Z',
            },
          ],
        }),
      ),
      ...overrides,
    });
  }

  it('opens only the explanation read and preserves draft/files and return focus', async () => {
    const client = withRun();
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'unsent private draft' } });
    await fireEvent.change(screen.getByLabelText('Attach files'), {
      target: { files: [new File(['abc'], 'draft.txt', { type: 'text/plain' })] },
    });
    const trigger = screen.getByRole('button', { name: 'Explain run' });
    await fireEvent.click(trigger);
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    expect(client.getRunExplanation).toHaveBeenCalledWith(
      'run-1',
      'ses-1',
      expect.any(AbortSignal),
    );
    expect(client.getIdentity).toHaveBeenCalledOnce();
    expect(client.getComputeStatus).toHaveBeenCalledOnce();
    expect(client.createMessage).not.toHaveBeenCalled();
    expect(client.createUpload).not.toHaveBeenCalled();
    expect(client.getAttachmentCapability).not.toHaveBeenCalled();
    await fireEvent.click(screen.getByRole('button', { name: /Close/ }));
    expect(composer).toHaveValue('unsent private draft');
    expect(screen.getByText('draft.txt')).toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });

  it('gives a read-only participant explanation without compute-write authority', async () => {
    const client = withRun({
      getComputeStatus: vi.fn().mockRejectedValue(new ApiError('not_found', 'Unavailable', 404)),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    expect(screen.getByLabelText('Message')).toBeDisabled();
    expect(client.createMessage).not.toHaveBeenCalled();
  });

  it('requires exactly one real active tenant without hiding the transcript', async () => {
    const client = withRun({
      getIdentity: vi.fn().mockResolvedValue({
        ...identity,
        tenants: [
          { tenant_id: 'ten-1', role: 'owner', active: true },
          { tenant_id: 'ten-2', role: 'owner', active: true },
        ],
      }),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    expect(await screen.findByText('Explain this execution')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Explain run' })).toBeDisabled();
    expect(client.getRunExplanation).not.toHaveBeenCalled();
  });

  it('retains the identity+tenant cadence across close and reopen, without polling explanation', async () => {
    let now = 1000;
    const client = withRun();
    render(SessionDetail, {
      client,
      sessionId: 'ses-1',
      explanationEnabled: true,
      explanationNow: () => now,
    });
    const trigger = await screen.findByRole('button', { name: 'Explain run' });
    await fireEvent.click(trigger);
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    await fireEvent.click(screen.getByRole('button', { name: /Close/ }));
    await fireEvent.click(trigger);
    expect(client.getRunExplanation).toHaveBeenCalledOnce();
    now = 6000;
    await fireEvent.click(screen.getByRole('button', { name: /Refresh/ }));
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledTimes(2));
  });

  it('discards a closed drawer response even when a transport ignores abort', async () => {
    const pending = barrier<ReturnType<typeof explanation>>();
    const client = withRun({ getRunExplanation: vi.fn().mockReturnValue(pending.promise) });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    const signal = vi.mocked(client.getRunExplanation).mock.calls[0]?.[2];
    await fireEvent.click(screen.getByRole('button', { name: /Close/ }));
    expect(signal?.aborted).toBe(true);
    pending.resolve(explanation());
    await pending.promise;
    expect(screen.queryByText('Last recorded admission decision')).not.toBeInTheDocument();
  });

  it('never restores permission from cached identity after an opaque explanation denial', async () => {
    const client = withRun({
      getRunExplanation: vi.fn().mockRejectedValue(new ApiError('not_found', 'Unavailable', 404)),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Explain run' })).toBeDisabled());
    await fireEvent.click(screen.getByRole('button', { name: /Close/ }));
    expect(screen.getByRole('button', { name: 'Explain run' })).toBeDisabled();
    expect(screen.getByText('Explain this execution')).toBeInTheDocument();
    expect(client.getIdentity).toHaveBeenCalledOnce();
  });

  it('discards an in-flight explanation and identity on unmount', async () => {
    const pending = barrier<ReturnType<typeof explanation>>();
    const client = withRun({ getRunExplanation: vi.fn().mockReturnValue(pending.promise) });
    const view = render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    const signal = vi.mocked(client.getRunExplanation).mock.calls[0]?.[2];
    const authSignal = vi.mocked(client.getIdentity).mock.calls[0]?.[0];
    view.unmount();
    expect(signal?.aborted).toBe(true);
    expect(authSignal?.aborted).toBe(true);
    pending.resolve(explanation());
    await pending.promise;
    expect(screen.queryByText('Last recorded admission decision')).not.toBeInTheDocument();
  });

  it('discards hidden responses without restoring a drawer or losing a recoverable draft', async () => {
    const pending = barrier<ReturnType<typeof explanation>>();
    const client = withRun({ getRunExplanation: vi.fn().mockReturnValue(pending.promise) });
    const hidden = vi.spyOn(document, 'hidden', 'get').mockReturnValue(false);
    const view = render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    try {
      const composer = await screen.findByLabelText('Message');
      await fireEvent.input(composer, { target: { value: 'Keep while hidden' } });
      await fireEvent.click(screen.getByRole('button', { name: 'Explain run' }));
      await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
      const signal = vi.mocked(client.getRunExplanation).mock.calls[0]?.[2];
      hidden.mockReturnValue(true);
      await fireEvent(document, new Event('visibilitychange'));
      expect(signal?.aborted).toBe(true);
      pending.resolve(explanation());
      await pending.promise;
      expect(screen.queryByRole('complementary')).not.toBeInTheDocument();
      hidden.mockReturnValue(false);
      await fireEvent(document, new Event('visibilitychange'));
      await waitFor(() => expect(client.getIdentity).toHaveBeenCalledTimes(2));
      expect(client.getRunExplanation).toHaveBeenCalledOnce();
      expect(screen.getByLabelText('Message')).toHaveValue('Keep while hidden');
    } finally {
      view.unmount();
      hidden.mockRestore();
    }
  });

  it('discards old private drafts and evidence on an observed tenant scope change', async () => {
    const getIdentity = vi
      .fn()
      .mockResolvedValueOnce(identity)
      .mockResolvedValue({
        ...identity,
        tenants: [{ tenant_id: 'ten-2', role: 'owner', active: true }],
      });
    const client = withRun({ getIdentity });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'Old workspace private draft' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Explain run' }));
    await screen.findByText('Last recorded admission decision');
    await fireEvent(document, new Event('visibilitychange'));
    await waitFor(() => expect(client.getSession).toHaveBeenCalledTimes(2));
    expect(await screen.findByLabelText('Message')).toHaveValue('');
    expect(screen.queryByRole('complementary')).not.toBeInTheDocument();
    expect(client.getRunExplanation).toHaveBeenCalledOnce();
    expect(client.createMessage).not.toHaveBeenCalled();
  });

  it('clears private state on actual identity read denial rather than retrying it as a network error', async () => {
    const client = withRun({
      getIdentity: vi
        .fn()
        .mockResolvedValueOnce(identity)
        .mockRejectedValue(new ApiError('unauthenticated', 'Sign in required', 401)),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await screen.findByText('Last recorded admission decision');
    await fireEvent(document, new Event('visibilitychange'));
    expect(
      await screen.findByRole('heading', { name: 'Continue to this conversation' }),
    ).toBeInTheDocument();
    expect(screen.queryByText('Explain this execution')).not.toBeInTheDocument();
    expect(screen.queryByRole('complementary')).not.toBeInTheDocument();
  });

  it('preserves the draft on a recoverable explanation transport failure', async () => {
    const client = withRun({
      getRunExplanation: vi
        .fn()
        .mockRejectedValue(new ApiError('temporarily_unavailable', 'Unavailable', 503)),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    const composer = await screen.findByLabelText('Message');
    await fireEvent.input(composer, { target: { value: 'Recoverable private draft' } });
    await fireEvent.click(screen.getByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(client.getRunExplanation).toHaveBeenCalledOnce());
    expect(composer).toHaveValue('Recoverable private draft');
    expect(screen.getByRole('button', { name: 'Explain run' })).toBeEnabled();
    expect(client.createMessage).not.toHaveBeenCalled();
  });

  it('never derives a Run selector from an unlinked notice payload', async () => {
    const client = withRun({
      listEvents: vi.fn().mockResolvedValue(
        fresh<EventPage>({
          items: [
            {
              event_id: 'generic-notice',
              sequence: 1,
              kind: 'system_notice',
              content: { text: 'run-1 is mentioned but not correlated' },
              created_at: '2026-10-10T12:00:00Z',
            },
          ],
        }),
      ),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await screen.findByText('run-1 is mentioned but not correlated');
    expect(screen.queryByRole('button', { name: 'Explain run' })).not.toBeInTheDocument();
    expect(client.getRunExplanation).not.toHaveBeenCalled();
  });

  it('returns focus to the conversation heading when both trigger and composer are disabled', async () => {
    const client = withRun({
      getComputeStatus: vi.fn().mockRejectedValue(new ApiError('not_found', 'Unavailable', 404)),
      getRunExplanation: vi.fn().mockRejectedValue(new ApiError('not_found', 'Unavailable', 404)),
    });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Explain run' }));
    await waitFor(() => expect(screen.getByRole('button', { name: 'Explain run' })).toBeDisabled());
    await fireEvent.click(screen.getByRole('button', { name: 'Close explanation' }));
    expect(screen.getByRole('heading', { name: 'Canonical planning' })).toHaveFocus();
  });

  function switchedScope(overrides: Partial<SessionDetailApi> = {}): SessionDetailApi {
    return withRun({
      getIdentity: vi
        .fn()
        .mockResolvedValueOnce(identity)
        .mockResolvedValue({
          ...identity,
          tenants: [{ tenant_id: 'ten-2', role: 'owner', active: true }],
        }),
      getSession: vi
        .fn()
        .mockResolvedValueOnce(fresh(session))
        .mockResolvedValue(fresh({ ...session, title: 'New authorized scope' })),
      ...overrides,
    });
  }

  async function observeSwitch(): Promise<void> {
    await fireEvent(document, new Event('visibilitychange'));
    await screen.findByRole('heading', { name: 'New authorized scope' });
    await waitFor(() => expect(screen.getByLabelText('Message')).toBeEnabled());
  }

  it('never restores a discarded private title from an old archive response', async () => {
    const pending = barrier<SessionSummary>();
    const client = switchedScope({ setSessionArchived: vi.fn().mockReturnValue(pending.promise) });
    render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
    await fireEvent.click(await screen.findByRole('button', { name: 'Archive' }));
    await waitFor(() => expect(client.setSessionArchived).toHaveBeenCalledOnce());
    const signal = vi.mocked(client.setSessionArchived).mock.calls[0]?.[2];
    await observeSwitch();
    expect(signal?.aborted).toBe(true);
    pending.resolve({ ...session, title: 'OLD PRIVATE TITLE', status: 'archived' });
    await pending.promise;
    await tick();
    expect(screen.getByRole('heading', { name: 'New authorized scope' })).toBeInTheDocument();
    expect(screen.queryByText('OLD PRIVATE TITLE')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Archive' })).toBeEnabled();
  });

  it.each(['compute', 'hash', 'intent', 'put', 'commit', 'message'] as const)(
    'fences a discarded file submission paused at %s, without follow-up effects or new-scope state overwrite',
    async (stage) => {
      const release = barrier<void>();
      const digests = { sha256: 'a'.repeat(64), contentMD5: 'kAFQmDzST7DWlj99KOF/cg==' };
      const intent = {
        upload_id: 'up-old',
        method: 'PUT' as const,
        url: 'https://objects.example/private',
        headers: {},
        expires_at: '2026-10-10T12:30:00Z',
      };
      const hashFileFn = vi.fn(async () => {
        if (stage === 'hash') await release.promise;
        return digests;
      });
      const putUploadFn = vi.fn(async () => {
        if (stage === 'put') await release.promise;
      });
      const createUpload = vi
        .fn<SessionDetailApi['createUpload']>()
        .mockImplementation(async () => {
          if (stage === 'intent') await release.promise;
          return intent;
        });
      const commitUpload = vi
        .fn<SessionDetailApi['commitUpload']>()
        .mockImplementation(async () => {
          if (stage === 'commit') await release.promise;
          return { upload_id: 'up-old', name: 'old.txt', media_type: 'text/plain', size: 3 };
        });
      const createMessage = vi
        .fn<SessionDetailApi['createMessage']>()
        .mockImplementation(async () => {
          if (stage === 'message') await release.promise;
          return {
            session_id: 'ses-1',
            event_id: 'old-created',
            sequence: 2,
            run_id: 'old-run',
            created: true,
            compute: {
              provider: 'openai',
              entitlement: 'active',
              quota: 'available',
              observed_at: '2026-10-10T12:00:00Z',
            },
          };
        });
      let computeReads = 0;
      const getComputeStatus = vi
        .fn<SessionDetailApi['getComputeStatus']>()
        .mockImplementation(async () => {
          if (computeReads++ === 1 && stage === 'compute') await release.promise;
          return {
            availability: 'ready',
            connection: {
              provider: 'openai',
              entitlement: 'active',
              quota: 'available',
              observed_at: '2026-10-10T12:00:00Z',
            },
          };
        });
      const client = switchedScope({ getComputeStatus, createUpload, commitUpload, createMessage });
      render(SessionDetail, {
        client,
        sessionId: 'ses-1',
        explanationEnabled: true,
        hashFileFn,
        putUploadFn,
      });
      await fireEvent.input(await screen.findByLabelText('Message'), {
        target: { value: 'OLD PRIVATE DRAFT' },
      });
      await fireEvent.change(screen.getByLabelText('Attach files'), {
        target: { files: [new File(['abc'], 'old.txt', { type: 'text/plain' })] },
      });
      await fireEvent.click(screen.getByRole('button', { name: 'Send' }));
      const paused = {
        compute: getComputeStatus,
        hash: hashFileFn,
        intent: createUpload,
        put: putUploadFn,
        commit: commitUpload,
        message: createMessage,
      }[stage];
      await waitFor(() => expect(paused).toHaveBeenCalledTimes(stage === 'compute' ? 2 : 1));
      const originalSignal = getComputeStatus.mock.calls[1]?.[1];
      await observeSwitch();
      expect(originalSignal?.aborted).toBe(true);
      await fireEvent.input(screen.getByLabelText('Message'), {
        target: { value: 'New scope draft' },
      });
      release.resolve();
      await release.promise;
      await tick();
      expect(createUpload).toHaveBeenCalledTimes(['compute', 'hash'].includes(stage) ? 0 : 1);
      expect(putUploadFn).toHaveBeenCalledTimes(
        ['compute', 'hash', 'intent'].includes(stage) ? 0 : 1,
      );
      expect(commitUpload).toHaveBeenCalledTimes(['commit', 'message'].includes(stage) ? 1 : 0);
      expect(createMessage).toHaveBeenCalledTimes(stage === 'message' ? 1 : 0);
      expect(screen.getByLabelText('Message')).toHaveValue('New scope draft');
      expect(screen.queryByText('old.txt')).not.toBeInTheDocument();
      expect(screen.queryByText('Message sent. Waiting for the agent…')).not.toBeInTheDocument();
      if (stage === 'intent') expect(intent.url).toBe('');
    },
  );

  it('never starts a discarded attachment download from a late capability response', async () => {
    const pending = barrier<Awaited<ReturnType<SessionDetailApi['getAttachmentCapability']>>>();
    const client = switchedScope({
      listEvents: vi.fn().mockResolvedValue(
        fresh<EventPage>({
          items: [
            {
              event_id: 'evt-old',
              sequence: 1,
              kind: 'assistant_message',
              created_at: '2026-10-10T12:00:00Z',
              content: {
                attachments: [{ name: 'old-download.txt', media_type: 'text/plain', size: 3 }],
              },
            },
          ],
        }),
      ),
      getAttachmentCapability: vi.fn().mockReturnValue(pending.promise),
    });
    const downloadCapabilityFn = vi.fn().mockResolvedValue(undefined);
    render(SessionDetail, {
      client,
      sessionId: 'ses-1',
      explanationEnabled: true,
      downloadCapabilityFn,
    });
    await fireEvent.click(await screen.findByRole('button', { name: /old-download.txt/ }));
    await waitFor(() => expect(client.getAttachmentCapability).toHaveBeenCalledOnce());
    const signal = vi.mocked(client.getAttachmentCapability).mock.calls[0]?.[3];
    await observeSwitch();
    expect(signal?.aborted).toBe(true);
    const capability = {
      method: 'GET' as const,
      url: 'https://objects.example/private',
      headers: {},
      expires_at: '2026-10-10T12:30:00Z',
    };
    pending.resolve(capability);
    await pending.promise;
    await tick();
    expect(downloadCapabilityFn).not.toHaveBeenCalled();
    expect(capability.url).toBe('');
  });

  it.each(['initial', 'refresh'] as const)(
    'does not replace new-scope compute with a stale %s read',
    async (stage) => {
      const pending = barrier<Awaited<ReturnType<SessionDetailApi['getComputeStatus']>>>();
      const ready = {
        availability: 'ready' as const,
        connection: {
          provider: 'openai',
          entitlement: 'active' as const,
          quota: 'available' as const,
          observed_at: '2026-10-10T12:00:00Z',
        },
      };
      const exhausted = {
        ...ready,
        connection: { ...ready.connection, quota: 'exhausted' as const },
      };
      const getComputeStatus = vi.fn<SessionDetailApi['getComputeStatus']>();
      if (stage === 'initial') getComputeStatus.mockReturnValueOnce(pending.promise);
      else getComputeStatus.mockResolvedValueOnce(exhausted).mockReturnValueOnce(pending.promise);
      getComputeStatus.mockResolvedValue(ready);
      const client = switchedScope({ getComputeStatus });
      render(SessionDetail, { client, sessionId: 'ses-1', explanationEnabled: true });
      if (stage === 'initial') await waitFor(() => expect(getComputeStatus).toHaveBeenCalledOnce());
      else {
        await fireEvent.click(await screen.findByRole('button', { name: /^Refresh$/ }));
        await waitFor(() => expect(getComputeStatus).toHaveBeenCalledTimes(2));
      }
      const signal = getComputeStatus.mock.calls[stage === 'initial' ? 0 : 1]?.[1];
      await observeSwitch();
      expect(signal?.aborted).toBe(true);
      pending.resolve(exhausted);
      await pending.promise;
      await tick();
      await fireEvent.input(screen.getByLabelText('Message'), {
        target: { value: 'New scope message' },
      });
      expect(screen.getByRole('button', { name: 'Send' })).toBeEnabled();
      expect(screen.queryByRole('button', { name: /^Refresh$/ })).not.toBeInTheDocument();
    },
  );
});
