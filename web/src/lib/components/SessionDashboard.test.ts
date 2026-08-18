import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';

import type { ConditionalResult, SessionPage } from '$lib/api/client';
import SessionDashboard, { type DashboardApi } from './SessionDashboard.svelte';

const identity = {
  user_id: 'usr-1',
  provider: 'telegram',
  tenants: [
    { tenant_id: 'ten-1', role: 'owner' as const, active: true },
    { tenant_id: 'ten-2', role: 'member' as const, active: false },
  ],
};

function fresh(items: SessionPage['items']): ConditionalResult<SessionPage> {
  return { state: 'fresh', data: { items } };
}

function api(overrides: Partial<DashboardApi> = {}): DashboardApi {
  return {
    getIdentity: vi.fn().mockResolvedValue(identity),
    listSessions: vi.fn().mockResolvedValue(
      fresh([
        {
          session_id: 'ses-1',
          status: 'active',
          title: 'Planning',
          preview: 'Next steps',
          last_sequence: 2,
          created_at: '2026-08-18T00:00:00Z',
          updated_at: '2026-08-18T01:00:00Z',
        },
      ]),
    ),
    selectTenant: vi.fn().mockResolvedValue(identity),
    createSession: vi.fn(),
    setSessionArchived: vi.fn().mockResolvedValue(undefined),
    logout: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  } as DashboardApi;
}

describe('SessionDashboard', () => {
  it('loads identity and canonical sessions into an accessible workspace', async () => {
    const client = api();

    render(SessionDashboard, { client });

    expect(await screen.findByRole('heading', { name: 'Sessions', level: 1 })).toBeInTheDocument();
    expect(screen.getByLabelText('Workspace')).toHaveValue('ten-1');
    expect(screen.getByRole('link', { name: /Planning/ })).toHaveAttribute(
      'href',
      '/sessions/ses-1',
    );
    expect(client.listSessions).toHaveBeenCalledWith({ status: 'active', limit: 50 });
  });

  it('switches tenant through the typed mutation and refreshes the session projection', async () => {
    const selected = {
      ...identity,
      tenants: identity.tenants.map((tenant) => ({
        ...tenant,
        active: tenant.tenant_id === 'ten-2',
      })),
    };
    const selectTenant = vi.fn().mockResolvedValue(selected);
    const listSessions = vi.fn().mockResolvedValue(fresh([]));
    const client = api({ selectTenant, listSessions });

    render(SessionDashboard, { client });
    const selector = await screen.findByLabelText('Workspace');
    await fireEvent.change(selector, { target: { value: 'ten-2' } });

    await waitFor(() => expect(selectTenant).toHaveBeenCalledWith('ten-2'));
    expect(listSessions).toHaveBeenLastCalledWith({ status: 'active', limit: 50 });
    expect(selector).toHaveValue('ten-2');
  });

  it('archives a session and reloads the active list', async () => {
    const setSessionArchived = vi.fn().mockResolvedValue(undefined);
    const client = api({ setSessionArchived });

    render(SessionDashboard, { client });
    await fireEvent.click(await screen.findByRole('button', { name: 'Archive Planning' }));

    await waitFor(() => expect(setSessionArchived).toHaveBeenCalledOnce());
    expect(setSessionArchived.mock.calls[0]?.[0]).toBe('ses-1');
    expect(setSessionArchived.mock.calls[0]?.[1]).toMatchObject({ archived: true });
    expect(client.listSessions).toHaveBeenCalledTimes(2);
  });

  it('preserves archive idempotency across an ambiguous retry', async () => {
    const setSessionArchived = vi
      .fn()
      .mockRejectedValueOnce(new Error('ambiguous transport failure'))
      .mockResolvedValueOnce(undefined);
    const client = api({ setSessionArchived });

    render(SessionDashboard, { client });
    const archive = await screen.findByRole('button', { name: 'Archive Planning' });
    await fireEvent.click(archive);
    await waitFor(() => expect(setSessionArchived).toHaveBeenCalledTimes(1));
    await fireEvent.click(archive);
    await waitFor(() => expect(setSessionArchived).toHaveBeenCalledTimes(2));

    expect(setSessionArchived.mock.calls[0]?.[1]?.idempotency_key).toBe(
      setSessionArchived.mock.calls[1]?.[1]?.idempotency_key,
    );
  });
});
