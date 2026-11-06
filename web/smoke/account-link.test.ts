import { readFileSync, statSync } from 'node:fs';
import { fireEvent, render, screen, waitFor } from '@testing-library/svelte';
import { afterEach, expect, it, vi } from 'vitest';
import { CanonicalApiClient } from '../src/lib/api/client';
import {
  parseIdentityContinuation,
  parseIdentityOperation,
} from '../src/lib/account-identity/contract';
import AccountLoginMethods from '../src/lib/account-identity/AccountLoginMethods.svelte';

afterEach(() => vi.unstubAllGlobals());

it('navigates directly to Telegram using actual Go/BFF response bytes and never auto-confirms or replays on return', async () => {
  const path = process.env.SESSIONLESS_IDENTITY_CONTINUATION_FIXTURE;
  if (!path)
    throw new Error('Run make account-link-smoke-test to generate the Go/BFF fixture first.');
  expect(statSync(path).size).toBeLessThan(32 * 1024);
  const fixture: { origin: string; methods: string; start: string; continuation: string } =
    JSON.parse(readFileSync(path, 'utf8'));
  vi.stubGlobal('location', { origin: fixture.origin });
  const started = parseIdentityOperation(fixture.start);
  const continuation = parseIdentityContinuation(fixture.continuation, fixture.origin);
  expect(started.kind).toBe('link');
  expect(started.phase).toBe('target_proof');
  expect(new URL(continuation.authorization_url).origin).toBe('https://oauth.telegram.org');
  const operations = '/api/web/v1/account/identity-operations';
  const operationPath = `${operations}/${encodeURIComponent(started.operation_id)}`;
  const request = vi.fn<typeof fetch>().mockImplementation(async (path, options) => {
    const method = options?.method ?? 'GET';
    let body: string;
    if (method === 'GET' && path === '/api/web/v1/account/login-methods') body = fixture.methods;
    else if (method === 'GET' && path === operationPath) body = fixture.start;
    else if (method === 'POST' && path === operations) body = fixture.start;
    else if (method === 'POST' && path === `${operationPath}/continue`) body = fixture.continuation;
    else
      throw new Error('Unexpected request: this smoke never calls providers or confirms a link.');
    return new Response(body, {
      status: 200,
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
    });
  });
  const client = new CanonicalApiClient({ fetch: request, readCSRFToken: () => 'synthetic-csrf' });
  const onNavigate = vi.fn();
  const view = render(AccountLoginMethods, { client, onNavigate });
  await fireEvent.click(await screen.findByRole('button', { name: 'Link Telegram', exact: true }));
  await waitFor(() =>
    expect(onNavigate).toHaveBeenCalledExactlyOnceWith(continuation.authorization_url),
  );
  const mutations = () => request.mock.calls.filter(([, options]) => options?.method === 'POST');
  expect(mutations().map(([path]) => path)).toEqual([operations, `${operationPath}/continue`]);
  expect(JSON.parse(String(mutations()[1]![1]?.body))).toEqual({
    expected_revision: started.before_revision,
  });
  expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  expect(screen.queryByText('Stored operation')).not.toBeInTheDocument();
  await view.unmount();
  render(AccountLoginMethods, { client, onNavigate, operationId: started.operation_id });
  await screen.findByRole('button', { name: 'Continue to Telegram', exact: true });
  await fireEvent.click(screen.getByRole('button', { name: 'Refresh status' }));
  await waitFor(() => expect(screen.getByRole('button', { name: 'Refresh status' })).toBeEnabled());
  expect(mutations()).toHaveLength(2);
  expect(onNavigate).toHaveBeenCalledOnce();
});
