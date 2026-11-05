import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import { parseLoginMethods } from '$lib/api/client';
import LoginMethods from './LoginMethods.svelte';

describe('LoginMethods', () => {
  for (const providers of [['yandex'], ['telegram'], ['yandex', 'telegram']] as const) {
    it(`renders only configured normal POST forms: ${providers.join(',')}`, async () => {
      const methods = providers.map((provider) => ({
        provider,
        label: provider === 'yandex' ? 'Yandex' : 'Telegram',
        start_path: `/auth/login/${provider}/start`,
      }));
      const client = {
        getLoginMethods: vi.fn().mockResolvedValue(parseLoginMethods(JSON.stringify({ methods }))),
      };
      render(LoginMethods, { client, returnTo: '/sessions/one' });
      for (const provider of providers) {
        const label = provider === 'yandex' ? 'Yandex' : 'Telegram';
        const button = await screen.findByRole('button', { name: `Continue with ${label}` });
        const form = button.closest('form');
        expect(form).toHaveAttribute('method', 'POST');
        expect(form).toHaveAttribute('action', `/auth/login/${provider}/start`);
        expect(form?.querySelector('input[name="return_to"]')).toHaveValue('/sessions/one');
      }
      expect(screen.queryAllByRole('button')).toHaveLength(providers.length);
      expect(client.getLoginMethods).toHaveBeenCalledOnce();
    });
  }
  it('shows unavailable with no fallback and retries only on explicit action', async () => {
    const client = { getLoginMethods: vi.fn().mockRejectedValue(new Error('private upstream')) };
    render(LoginMethods, { client });
    expect(await screen.findByRole('alert')).toHaveTextContent('Sign-in methods are unavailable');
    expect(screen.queryByText(/private upstream/)).not.toBeInTheDocument();
    expect(document.querySelector('form')).toBeNull();
    expect(client.getLoginMethods).toHaveBeenCalledOnce();
    await fireEvent.click(screen.getByRole('button', { name: 'Try again' }));
    await screen.findByRole('alert');
    expect(client.getLoginMethods).toHaveBeenCalledTimes(2);
  });
  for (const returnTo of [
    'https://outside.example',
    '//outside.example',
    '/\\outside',
    '/unsafe\npath',
  ]) {
    it(`refuses unsafe return ${JSON.stringify(returnTo)}`, async () => {
      const client = {
        getLoginMethods: vi
          .fn()
          .mockResolvedValue(
            parseLoginMethods(
              '{"methods":[{"provider":"telegram","label":"Telegram","start_path":"/auth/login/telegram/start"}]}',
            ),
          ),
      };
      render(LoginMethods, { client, returnTo });
      const button = await screen.findByRole('button', { name: 'Continue with Telegram' });
      expect(button.closest('form')?.querySelector('input')).toHaveValue('/');
    });
  }
});
