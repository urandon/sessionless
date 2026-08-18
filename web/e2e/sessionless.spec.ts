import { expect, csrfToken, objectOrigin, sessionId, test } from './fixtures/canonical-api';

test.describe('authentication boundaries', () => {
  test('offers login without granting tenant access and renders access-denied recovery', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.auth = 'unauthenticated';
    await page.goto('/');

    await expect(page.getByRole('heading', { name: 'Your sessions, in one place' })).toBeVisible();
    const signIn = page.getByRole('link', { name: 'Continue with Telegram' });
    await expect(signIn).toHaveAttribute('href', '/auth/telegram/start?return_to=%2F');

    await page.goto('/login?auth_error=access_denied');
    await expect(
      page.getByRole('heading', {
        name: 'This Telegram account has no Sessionless workspace yet',
      }),
    ).toBeVisible();
    await expect(page.getByText('Signing in never creates tenant access by itself.')).toBeVisible();
  });

  test('does not reveal whether an inaccessible deep-linked session exists', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.auth = 'access-denied';
    await page.goto(`/sessions/${sessionId}`);

    await expect(
      page.getByRole('heading', { name: 'This conversation cannot be opened' }),
    ).toBeVisible();
    await expect(
      page.getByText(/may not exist, or your active workspace is not a participant/),
    ).toBeVisible();
  });

  test('preserves an unauthenticated session deep link through the login start URL', async ({
    canonicalApi,
    page,
  }) => {
    canonicalApi.auth = 'unauthenticated';
    await page.goto(`/sessions/${sessionId}`);

    const signIn = page.getByRole('link', { name: 'Sign in' });
    await expect(signIn).toHaveAttribute('href', `/login?return_to=%2Fsessions%2F${sessionId}`);
    await signIn.click();
    await expect(page.getByRole('link', { name: 'Continue with Telegram' })).toHaveAttribute(
      'href',
      `/auth/telegram/start?return_to=%2Fsessions%2F${sessionId}`,
    );
  });
});

test.describe('canonical session workflow', () => {
  test('switches tenant and archives then restores a session with CSRF protection', async ({
    canonicalApi,
    page,
  }) => {
    await page.goto('/');
    await expect(page.getByRole('heading', { name: 'Sessions' })).toBeVisible();
    await expect(page.getByRole('link', { name: /Launch planning/ })).toBeVisible();

    await page.getByLabel('Workspace').selectOption('tenant-two');
    const tenantRequest = await canonicalApi.expectMutation('POST', '/api/web/v1/active-tenant');
    expect(tenantRequest.body).toEqual({ tenant_id: 'tenant-two' });
    await expect(page.getByLabel('Workspace')).toHaveValue('tenant-two');

    await page.getByRole('button', { name: 'Archive Launch planning' }).click();
    const archiveRequest = await canonicalApi.expectMutation(
      'POST',
      `/api/web/v1/sessions/${sessionId}/archive`,
    );
    expect(archiveRequest.body).toMatchObject({ archived: true });
    await expect(page.getByRole('heading', { name: 'No active sessions' })).toBeVisible();

    await page.getByRole('button', { name: 'Archived' }).click();
    await page.getByRole('button', { name: 'Unarchive Launch planning' }).click();
    await expect
      .poll(
        () =>
          canonicalApi
            .requestsFor('POST', `/api/web/v1/sessions/${sessionId}/archive`)
            .map((request) => request.body),
        { message: 'archive and unarchive mutations' },
      )
      .toContainEqual(expect.objectContaining({ archived: false }));
    await expect(page.getByRole('heading', { name: 'No archived sessions' })).toBeVisible();
  });

  test('opens a deep link, sends from the keyboard, and polls a bounded event sequence', async ({
    canonicalApi,
    page,
  }) => {
    await page.goto(`/sessions/${sessionId}`);
    await expect(page.getByRole('heading', { name: 'Launch planning' })).toBeVisible();
    await expect(page.getByRole('region', { name: 'Conversation history' })).toContainText(
      'Ship the canonical WebUI.',
    );

    const composer = page.getByRole('textbox', { name: 'Message' });
    await composer.fill('Attach the fixture report.');
    await composer.press(process.platform === 'darwin' ? 'Meta+Enter' : 'Control+Enter');

    const messageRequest = await canonicalApi.expectMutation(
      'POST',
      `/api/web/v1/sessions/${sessionId}/messages`,
    );
    expect(messageRequest.body).toMatchObject({ text: 'Attach the fixture report.' });
    await expect(page.getByText('The report is queued for review.')).toBeVisible();
    await expect(page.getByText('Run succeeded')).toBeVisible();

    await expect
      .poll(() =>
        canonicalApi
          .requestsFor('GET', `/api/web/v1/sessions/${sessionId}/events`)
          .some((request) => new URL(request.url).searchParams.get('after_sequence') === '2'),
      )
      .toBe(true);
    await expect
      .poll(() =>
        canonicalApi
          .requestsFor('GET', `/api/web/v1/sessions/${sessionId}`)
          .some((request) => request.headers['if-none-match'] === '"session-v1"'),
      )
      .toBe(true);
  });

  test('uploads through an expiring object capability before committing the message', async ({
    canonicalApi,
    page,
  }) => {
    await page.goto(`/sessions/${sessionId}`);
    await expect(page.getByRole('heading', { name: 'Launch planning' })).toBeVisible();

    const upload = canonicalApi.pauseNextUpload();
    await page.getByLabel('Attach files').setInputFiles({
      name: 'fixture.txt',
      mimeType: 'text/plain',
      buffer: Buffer.from('fixture body'),
    });
    await page.getByRole('textbox', { name: 'Message' }).fill('Attach the fixture report.');
    await page.getByRole('button', { name: 'Send' }).click();

    const objectRequest = await upload.started;
    expect(objectRequest.method).toBe('PUT');
    expect(objectRequest.url).toBe(`${objectOrigin}/uploads/upload-1?signature=one-time-secret`);
    expect(objectRequest.headers['content-type']).toBe('text/plain');
    expect(objectRequest.headers['content-md5']).toBe('9cpoqhUMFcdsvuGtlAybug==');
    expect(objectRequest.headers['x-upload-token']).toBe('object-capability-secret');
    await expect(
      page.getByRole('progressbar', { name: 'Upload progress for fixture.txt' }),
    ).toBeVisible();
    upload.release();

    const uploadRequest = await canonicalApi.expectMutation('POST', '/api/web/v1/uploads');
    expect(uploadRequest.body).toMatchObject({
      session_id: sessionId,
      name: 'fixture.txt',
      media_type: 'text/plain',
      size: 12,
      sha256: 'ca260f20e9412d1ac5e1e30014e8592c75e07ad93446586497e04863084b52a3',
      content_md5: '9cpoqhUMFcdsvuGtlAybug==',
    });
    await canonicalApi.expectMutation('POST', '/api/web/v1/uploads/upload-1/commit');
    const messageRequest = await canonicalApi.expectMutation(
      'POST',
      `/api/web/v1/sessions/${sessionId}/messages`,
    );
    expect(messageRequest.body).toMatchObject({ upload_ids: ['upload-1'] });

    const browserStorage = await page.evaluate(() => ({
      local: Object.fromEntries(Object.entries(localStorage)),
      session: Object.fromEntries(Object.entries(sessionStorage)),
    }));
    expect(JSON.stringify(browserStorage)).not.toContain('one-time-secret');
    expect(JSON.stringify(browserStorage)).not.toContain('object-capability-secret');
    expect(JSON.stringify(browserStorage)).not.toContain(csrfToken);
  });

  test('logs out through the protected BFF route', async ({ canonicalApi, page }) => {
    await page.goto('/');
    await page.getByRole('button', { name: 'Sign out' }).click();
    await expect(page).toHaveURL('/login');
    await expect(page.getByRole('heading', { name: 'Sign in to Sessionless' })).toBeVisible();

    await expect.poll(() => canonicalApi.findRequest('POST', '/auth/logout')).not.toBeUndefined();
    expect(canonicalApi.findRequest('POST', '/auth/logout')?.headers['x-sessionless-csrf']).toBe(
      csrfToken,
    );
  });
});
