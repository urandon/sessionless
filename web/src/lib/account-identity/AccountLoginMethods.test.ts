import { fireEvent, render, screen, waitFor, within } from '@testing-library/svelte';
import { flushSync, tick } from 'svelte';
import { describe, expect, it, vi } from 'vitest';
import { ApiError, type CanonicalApiClient } from '$lib/api/client';
import {
  parseAccountLoginMethods,
  parseIdentityOperation,
  type IdentityProvider,
  type IdentityOperationView,
} from './contract';
import AccountLoginMethods from './AccountLoginMethods.svelte';

const id = 'opaque:operation-1';
function rows(
  current: IdentityProvider = 'yandex',
  both = false,
  revision = '7',
  disabled?: IdentityProvider,
) {
  return parseAccountLoginMethods(
    JSON.stringify({
      revision,
      methods: (['yandex', 'telegram'] as const).map((provider) => {
        const linked = both || provider === current;
        const remaining = provider === 'yandex' ? 'telegram' : 'yandex';
        const unlink = both && disabled !== remaining;
        const link = !linked && disabled !== provider;
        return {
          provider,
          linked,
          current: current === provider,
          enabled: disabled !== provider,
          ...(linked ? { method_handle: `own-${provider}` } : {}),
          allowed_actions: unlink ? ['unlink'] : link ? ['link'] : [],
          ...(unlink ? { remaining_provider: remaining } : {}),
        };
      }),
    }),
  );
}
function pending(
  reference = id,
  phase = 'source_proof',
  kind = 'link',
  target = 'telegram',
  revision = '7',
): IdentityOperationView {
  return parseIdentityOperation(
    JSON.stringify({
      operation_id: reference,
      kind,
      phase,
      target_provider: target,
      before_revision: revision,
      expires_at: '2026-11-04T12:10:00Z',
      allowed_actions: [phase === 'confirm' ? 'confirm' : 'continue', 'cancel'],
    }),
  );
}
function terminal(outcome = 'linked', kind = 'link'): IdentityOperationView {
  return parseIdentityOperation(
    JSON.stringify({
      operation_id: id,
      kind,
      phase: 'terminal',
      outcome,
      before_revision: '7',
      ...(outcome === 'linked' || outcome === 'unlinked' ? { after_revision: '8' } : {}),
      expires_at: '2026-11-04T12:10:00Z',
      completed_at: '2026-11-04T12:01:00Z',
      allowed_actions: [],
    }),
  );
}
function authorization(): string {
  return (
    'https://oauth.telegram.org/auth?' +
    new URLSearchParams({
      client_id: '100000',
      redirect_uri: `${globalThis.location.origin}/auth/telegram/callback`,
      response_type: 'code',
      state: 'state',
      code_challenge: 'A'.repeat(43),
      code_challenge_method: 'S256',
      scope: 'openid profile',
      max_age: '0',
      nonce: 'nonce',
    })
  );
}
function api(current: IdentityProvider = 'yandex') {
  return {
    getAccountLoginMethods: vi
      .fn<CanonicalApiClient['getAccountLoginMethods']>()
      .mockResolvedValue(rows(current)),
    getIdentityOperationOutcome: vi
      .fn<CanonicalApiClient['getIdentityOperationOutcome']>()
      .mockImplementation(async (reference) => pending(reference)),
    startIdentityOperation: vi
      .fn<CanonicalApiClient['startIdentityOperation']>()
      .mockImplementation(async (input) =>
        pending(
          id,
          'source_proof',
          input.kind,
          input.kind === 'link' ? input.target_provider : 'yandex',
          input.expected_revision,
        ),
      ),
    continueIdentityOperation: vi
      .fn<CanonicalApiClient['continueIdentityOperation']>()
      .mockResolvedValue({ authorization_url: authorization() }),
    confirmIdentityOperation: vi
      .fn<CanonicalApiClient['confirmIdentityOperation']>()
      .mockResolvedValue(terminal()),
    cancelIdentityOperation: vi
      .fn<CanonicalApiClient['cancelIdentityOperation']>()
      .mockResolvedValue(terminal('cancelled')),
  };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe('AccountLoginMethods presentation', () => {
  for (const action of ['confirm', 'cancel'] as const) {
    it(`drops the old ${action} follow-up after scope changes inside outcome focus`, async () => {
      const client = api(),
        next = api();
      client.getIdentityOperationOutcome.mockResolvedValue(pending(id, 'confirm'));
      next.getAccountLoginMethods.mockResolvedValue(rows('telegram', false, '9'));
      next.getIdentityOperationOutcome.mockResolvedValue(
        pending('next-operation', 'source_proof', 'link', 'yandex', '9'),
      );
      const view = render(AccountLoginMethods, { client, operationId: id });
      const button = await screen.findByRole('button', {
        name: action === 'confirm' ? 'Confirm link' : 'Cancel operation',
      });
      const oldReadSignal = client.getAccountLoginMethods.mock.calls[0]![0] as AbortSignal;
      let changed: Promise<void> | undefined;
      screen.getByRole('heading', { name: 'Stored operation' }).addEventListener(
        'focus',
        () => {
          changed = view.rerender({ client: next, operationId: 'next-operation' });
          flushSync();
        },
        { once: true },
      );
      await fireEvent.click(button);
      await waitFor(() =>
        expect(next.getIdentityOperationOutcome).toHaveBeenCalledWith(
          'next-operation',
          expect.any(AbortSignal),
        ),
      );
      expect(changed).toBeDefined();
      await changed;
      await waitFor(() =>
        expect(screen.getByText('9', { selector: '.revision' })).toBeInTheDocument(),
      );
      expect(oldReadSignal.aborted).toBe(true);
      expect(client.getAccountLoginMethods).toHaveBeenCalledOnce();
      expect(next.getAccountLoginMethods).toHaveBeenCalledOnce();
      expect(screen.getByRole('list', { name: 'Your sign-in methods' })).toHaveTextContent(
        'Telegram',
      );
      expect(screen.queryByText('Sign-in method linked.')).not.toBeInTheDocument();
    });
  }
  for (const reference of [undefined, id]) {
    it(`keeps only the safe local receipt reference through sign-in and re-enters with GET only: ${reference ?? 'settings'}`, async () => {
      const client = api();
      client.getAccountLoginMethods.mockRejectedValue(
        new ApiError('unauthenticated', 'private detail', 401),
      );
      const view = render(AccountLoginMethods, { client, operationId: reference });
      await screen.findByRole('alert');
      const link = screen.getByRole('link', { name: 'Sign in again' });
      const login = new URL(link.getAttribute('href')!, 'https://account.example.com');
      expect(login.origin).toBe('https://account.example.com');
      expect(login.pathname).toBe('/login');
      expect([...login.searchParams.keys()]).toEqual(['return_to']);
      const returning = new URL(login.searchParams.get('return_to')!, login.origin);
      expect(returning.pathname).toBe('/account/login-methods');
      expect(returning.searchParams.get('operation_id')).toBe(reference ?? null);
      expect([...returning.searchParams.keys()]).toEqual(reference ? ['operation_id'] : []);
      await view.unmount();
      const reentry = api();
      reentry.getIdentityOperationOutcome.mockResolvedValue(terminal());
      render(AccountLoginMethods, {
        client: reentry,
        operationId: returning.searchParams.get('operation_id') ?? undefined,
      });
      if (reference) {
        expect(await screen.findByText('Sign-in method linked.')).toBeInTheDocument();
        expect(reentry.getIdentityOperationOutcome).toHaveBeenCalledWith(
          reference,
          expect.any(AbortSignal),
        );
      } else {
        await screen.findByRole('list', { name: 'Your sign-in methods' });
        expect(reentry.getIdentityOperationOutcome).not.toHaveBeenCalled();
      }
      expect(reentry.startIdentityOperation).not.toHaveBeenCalled();
      expect(reentry.continueIdentityOperation).not.toHaveBeenCalled();
      expect(reentry.confirmIdentityOperation).not.toHaveBeenCalled();
      expect(reentry.cancelIdentityOperation).not.toHaveBeenCalled();
    });
  }
  for (const current of ['yandex', 'telegram'] as const) {
    it(`reads only on mount/refresh for ${current}, keeps server actions and starts separately`, async () => {
      const client = api(current),
        onOperationIdChange = vi.fn(),
        onNavigate = vi.fn();
      const target = current === 'yandex' ? 'Telegram' : 'Yandex';
      const view = render(AccountLoginMethods, { client, onOperationIdChange, onNavigate });
      expect(screen.getByRole('heading', { name: 'Login methods' })).toHaveFocus();
      await screen.findByRole('button', { name: `Start linking ${target}` });
      expect(client.getAccountLoginMethods).toHaveBeenCalledOnce();
      expect(client.getIdentityOperationOutcome).not.toHaveBeenCalled();
      expect(client.startIdentityOperation).not.toHaveBeenCalled();
      expect(client.continueIdentityOperation).not.toHaveBeenCalled();
      expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
      const refresh = screen.getByRole('button', { name: 'Refresh status' });
      refresh.focus();
      await fireEvent.click(refresh);
      await waitFor(() => expect(client.getAccountLoginMethods).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(refresh).toBeEnabled());
      expect(refresh).toHaveFocus();
      await fireEvent.click(screen.getByRole('button', { name: `Start linking ${target}` }));
      const continuation = await screen.findByRole('button', { name: 'Continue provider check' });
      await waitFor(() => expect(continuation).toBeEnabled());
      expect(client.startIdentityOperation).toHaveBeenCalledWith(
        { kind: 'link', target_provider: target.toLowerCase(), expected_revision: '7' },
        expect.any(AbortSignal),
      );
      expect(onOperationIdChange).toHaveBeenCalledWith(id);
      expect(client.continueIdentityOperation).not.toHaveBeenCalled();
      expect(onNavigate).not.toHaveBeenCalled();
      expect(screen.getByRole('heading', { name: 'Stored operation' })).toHaveFocus();
      await view.rerender({ client, operationId: id, onOperationIdChange, onNavigate });
      await tick();
      expect(client.getIdentityOperationOutcome).not.toHaveBeenCalled();
      expect(client.getAccountLoginMethods).toHaveBeenCalledTimes(2);
    });
  }
  for (const disabled of ['yandex', 'telegram'] as const) {
    for (const linked of [false, true])
      it(`shows ${disabled} linked=${linked} in a one-enabled-provider mode without inventing eligibility`, async () => {
        const current = disabled === 'yandex' ? 'telegram' : 'yandex';
        const client = api(current);
        client.getAccountLoginMethods.mockResolvedValue(rows(current, linked, '7', disabled));
        render(AccountLoginMethods, { client });
        const list = await screen.findByRole('list', { name: 'Your sign-in methods' });
        const disabledRow = within(list)
          .getByRole('heading', { name: disabled === 'yandex' ? 'Yandex' : 'Telegram' })
          .closest('li')!;
        expect(disabledRow).toHaveTextContent('unavailable in this deployment');
        expect(disabledRow).toHaveTextContent(linked ? 'Linked' : 'Not linked');
        if (!linked) expect(within(disabledRow).queryByRole('button')).not.toBeInTheDocument();
        const currentRow = within(list)
          .getByRole('heading', { name: current === 'yandex' ? 'Yandex' : 'Telegram' })
          .closest('li')!;
        expect(
          within(currentRow).queryByRole('button', { name: /Start unlinking/ }),
        ).not.toBeInTheDocument();
        expect(client.startIdentityOperation).not.toHaveBeenCalled();
      });
  }
  it('preserves max uint64 decimal revision without frontend arithmetic', async () => {
    const client = api();
    client.getAccountLoginMethods.mockResolvedValue(rows('yandex', false, '18446744073709551615'));
    render(AccountLoginMethods, { client });
    await fireEvent.click(await screen.findByRole('button', { name: 'Start linking Telegram' }));
    await waitFor(() =>
      expect(client.startIdentityOperation).toHaveBeenCalledWith(
        { kind: 'link', target_provider: 'telegram', expected_revision: '18446744073709551615' },
        expect.any(AbortSignal),
      ),
    );
  });
  it('navigates only after an explicit continuation and never confirms a provider callback automatically', async () => {
    const client = api(),
      onNavigate = vi.fn();
    render(AccountLoginMethods, { client, operationId: id, onNavigate });
    const button = await screen.findByRole('button', { name: 'Continue provider check' });
    expect(client.continueIdentityOperation).not.toHaveBeenCalled();
    expect(onNavigate).not.toHaveBeenCalled();
    await fireEvent.click(button);
    await waitFor(() => expect(onNavigate).toHaveBeenCalledWith(authorization()));
    expect(client.continueIdentityOperation).toHaveBeenCalledTimes(1);
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
    expect(document.body).not.toHaveTextContent('client_id');
  });
  it('does not navigate a foreign continuation URL even from a discarded/malformed client result', async () => {
    const client = api(),
      onNavigate = vi.fn();
    client.continueIdentityOperation.mockResolvedValue({
      authorization_url: 'https://outside.example.com/?code=private',
    });
    render(AccountLoginMethods, { client, operationId: id, onNavigate });
    await fireEvent.click(await screen.findByRole('button', { name: 'Continue provider check' }));
    expect(await screen.findByRole('alert')).not.toHaveTextContent('outside.example.com');
    expect(onNavigate).not.toHaveBeenCalled();
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
  });
  it('requires separate final confirmation, displays pruned receipt and rereads capabilities with the rotated cookie', async () => {
    const client = api();
    client.getIdentityOperationOutcome.mockResolvedValue(pending(id, 'confirm'));
    client.getAccountLoginMethods
      .mockResolvedValueOnce(rows())
      .mockResolvedValue(rows('yandex', true, '8'));
    render(AccountLoginMethods, { client, operationId: id });
    const confirm = await screen.findByRole('button', { name: 'Confirm link' });
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
    expect(
      screen.queryByRole('button', { name: 'Continue provider check' }),
    ).not.toBeInTheDocument();
    await fireEvent.click(confirm);
    expect(await screen.findByText('Sign-in method linked.')).toBeInTheDocument();
    await waitFor(() => expect(client.getAccountLoginMethods).toHaveBeenCalledTimes(2));
    expect(client.confirmIdentityOperation).toHaveBeenCalledWith(id, '7', expect.any(AbortSignal));
    expect(screen.getByRole('heading', { name: 'Stored operation' })).toHaveFocus();
    expect(screen.queryByRole('button', { name: 'Confirm link' })).not.toBeInTheDocument();
    expect(screen.getByText(/Other Web sessions need to sign in again/)).toBeInTheDocument();
  });
  it('names the server-selected remaining method, uses own handle, and cancels only on explicit click', async () => {
    const client = api();
    client.getAccountLoginMethods.mockResolvedValue(rows('yandex', true));
    client.cancelIdentityOperation.mockResolvedValue(terminal('cancelled', 'unlink'));
    render(AccountLoginMethods, { client });
    const start = await screen.findByRole('button', { name: 'Start unlinking Yandex' });
    expect(start.closest('li')).toHaveTextContent('Keep signing in with Telegram');
    expect(start.closest('li')).toHaveTextContent('Other Web sessions will need sign-in again');
    await fireEvent.click(start);
    const cancel = await screen.findByRole('button', { name: 'Cancel operation' });
    expect(client.startIdentityOperation).toHaveBeenCalledWith(
      { kind: 'unlink', method_handle: 'own-yandex', expected_revision: '7' },
      expect.any(AbortSignal),
    );
    expect(client.cancelIdentityOperation).not.toHaveBeenCalled();
    await waitFor(() => expect(cancel).toBeEnabled());
    await fireEvent.click(cancel);
    expect(await screen.findByText('Operation cancelled.')).toBeInTheDocument();
    expect(client.cancelIdentityOperation).toHaveBeenCalledWith(id, '7', expect.any(AbortSignal));
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
  });
  it('confirms an unlink only from an explicit server-confirm phase, not from the provider return', async () => {
    const client = api();
    client.getAccountLoginMethods.mockResolvedValue(rows('yandex', true));
    client.getIdentityOperationOutcome.mockResolvedValue(
      pending(id, 'confirm', 'unlink', 'yandex'),
    );
    client.confirmIdentityOperation.mockResolvedValue(terminal('unlinked', 'unlink'));
    render(AccountLoginMethods, { client, operationId: id });
    const confirm = await screen.findByRole('button', { name: 'Confirm unlink' });
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
    await fireEvent.click(confirm);
    expect(await screen.findByText('Sign-in method unlinked.')).toBeInTheDocument();
    expect(client.confirmIdentityOperation).toHaveBeenCalledWith(id, '7', expect.any(AbortSignal));
    expect(client.continueIdentityOperation).not.toHaveBeenCalled();
  });
  it('recovers an unknown confirm by pure receipt read, not a repeated POST or provider exchange', async () => {
    const client = api();
    client.getIdentityOperationOutcome
      .mockResolvedValueOnce(pending(id, 'confirm'))
      .mockResolvedValue(terminal());
    client.confirmIdentityOperation.mockRejectedValue(
      new ApiError('temporarily_unavailable', 'private lost response', 503),
    );
    render(AccountLoginMethods, { client, operationId: id });
    await fireEvent.click(await screen.findByRole('button', { name: 'Confirm link' }));
    const alert = await screen.findByRole('alert');
    expect(alert).toHaveTextContent('outcome is unknown');
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Refresh status' })).toBeEnabled(),
    );
    await fireEvent.click(screen.getByRole('button', { name: 'Refresh status' }));
    expect(await screen.findByText('Sign-in method linked.')).toBeInTheDocument();
    expect(client.confirmIdentityOperation).toHaveBeenCalledOnce();
    expect(client.continueIdentityOperation).not.toHaveBeenCalled();
    expect(screen.queryByRole('button', { name: 'Confirm link' })).not.toBeInTheDocument();
  });
  it('shows a genuine missing 410 view separately from unknown read failure and pending expiry', async () => {
    const client = api();
    client.getIdentityOperationOutcome.mockResolvedValue(
      parseIdentityOperation(
        JSON.stringify({ operation_id: id, outcome: 'outcome_unavailable', allowed_actions: [] }),
        id,
        true,
      ),
    );
    const view = render(AccountLoginMethods, { client, operationId: id });
    expect(await screen.findByText(/No retained outcome is available/)).toHaveTextContent(
      'does not prove',
    );
    expect(screen.getByRole('link', { name: 'Sign in again' })).toHaveAttribute(
      'href',
      `/login?return_to=${encodeURIComponent('/account/login-methods?' + new URLSearchParams({ operation_id: id }).toString())}`,
    );
    expect(client.startIdentityOperation).not.toHaveBeenCalled();
    client.getIdentityOperationOutcome.mockResolvedValue(
      parseIdentityOperation(
        JSON.stringify({
          ...pending('expired-operation'),
          outcome: 'expired',
          allowed_actions: [],
        }),
      ),
    );
    await view.rerender({ client, operationId: 'expired-operation' });
    expect(await screen.findByText(/The pending operation expired/)).toHaveTextContent(
      'not a committed success',
    );
    expect(screen.queryByText(/Recorded at/)).not.toBeInTheDocument();
    client.getIdentityOperationOutcome.mockRejectedValue(
      new ApiError('temporarily_unavailable', 'private error', 503),
    );
    await view.rerender({ client, operationId: 'unknown-operation' });
    expect(await screen.findByRole('alert')).toHaveTextContent('outcome are unknown');
    expect(document.body).not.toHaveTextContent('No retained outcome is available');
    expect(document.body).not.toHaveTextContent('private error');
  });
  for (const [code, phrase] of [
    ['stale_revision', 'methods changed'],
    ['last_method', 'final usable'],
    ['conflict', 'conflicts with current'],
    ['proof_unavailable', 'already consumed'],
    ['provider_unavailable', 'provider is unavailable'],
    ['expired', 'window expired'],
    ['unauthenticated', 'lost replacement cookie'],
    ['temporarily_unavailable', 'outcome is unknown'],
  ] as const) {
    it(`uses fixed safe ${code} recovery without resubmitting`, async () => {
      const client = api();
      client.startIdentityOperation.mockRejectedValue(
        new ApiError(code, 'private subject/token/upstream detail', 503),
      );
      render(AccountLoginMethods, { client });
      await fireEvent.click(await screen.findByRole('button', { name: 'Start linking Telegram' }));
      const alert = await screen.findByRole('alert');
      expect(alert).toHaveTextContent(phrase);
      expect(alert).not.toHaveTextContent('private subject');
      await waitFor(() => expect(alert).toHaveFocus());
      expect(client.startIdentityOperation).toHaveBeenCalledOnce();
      expect(client.continueIdentityOperation).not.toHaveBeenCalled();
      expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
      expect(screen.getByRole('button', { name: 'Start linking Telegram' })).toBeDisabled();
      await fireEvent.click(screen.getByRole('button', { name: 'Refresh status' }));
      await waitFor(() => expect(client.getAccountLoginMethods).toHaveBeenCalledTimes(2));
      expect(client.startIdentityOperation).toHaveBeenCalledOnce();
    });
  }
  it('aborts old reads and ignores late responses after operation reference changes', async () => {
    const client = api(),
      old = deferred<IdentityOperationView>();
    client.getIdentityOperationOutcome.mockImplementationOnce(() => old.promise);
    const view = render(AccountLoginMethods, { client, operationId: id });
    await waitFor(() => expect(client.getIdentityOperationOutcome).toHaveBeenCalledOnce());
    const signal = client.getIdentityOperationOutcome.mock.calls[0]![1] as AbortSignal;
    await view.rerender({ client, operationId: 'next-operation' });
    await screen.findByRole('button', { name: 'Continue provider check' });
    expect(signal.aborted).toBe(true);
    old.resolve(terminal());
    await tick();
    expect(screen.queryByText('Sign-in method linked.')).not.toBeInTheDocument();
    expect(client.confirmIdentityOperation).not.toHaveBeenCalled();
  });
  it('does not publish an operation ID or navigate after unmount/changed-scope pending mutations', async () => {
    const client = api(),
      response = deferred<IdentityOperationView>(),
      onOperationIdChange = vi.fn();
    client.startIdentityOperation.mockReturnValue(response.promise);
    const view = render(AccountLoginMethods, { client, onOperationIdChange });
    await fireEvent.click(await screen.findByRole('button', { name: 'Start linking Telegram' }));
    const signal = client.startIdentityOperation.mock.calls[0]![1] as AbortSignal;
    await view.rerender({ client, operationId: 'next-operation', onOperationIdChange });
    await waitFor(() =>
      expect(client.getIdentityOperationOutcome).toHaveBeenCalledWith(
        'next-operation',
        expect.any(AbortSignal),
      ),
    );
    response.resolve(pending());
    await tick();
    expect(signal.aborted).toBe(true);
    expect(onOperationIdChange).not.toHaveBeenCalled();
    await view.unmount();
    const other = api(),
      navigation = deferred<{ authorization_url: string }>(),
      onNavigate = vi.fn();
    other.continueIdentityOperation.mockReturnValue(navigation.promise);
    const next = render(AccountLoginMethods, { client: other, operationId: id, onNavigate });
    await fireEvent.click(await screen.findByRole('button', { name: 'Continue provider check' }));
    await next.unmount();
    navigation.resolve({ authorization_url: authorization() });
    await tick();
    expect(onNavigate).not.toHaveBeenCalled();
  });
});
