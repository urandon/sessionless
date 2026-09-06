#!/usr/bin/env node

import { createReadStream } from 'node:fs';
import { access, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { extname, resolve, sep } from 'node:path';

const host = '127.0.0.1';
const port = Number.parseInt(process.env.SESSIONLESS_README_PREVIEW_PORT ?? '4174', 10);
const webRoot = resolve('web/build');
const sessionId = 'session-alpha';

if (!Number.isInteger(port) || port < 1 || port > 65535) {
  throw new Error('SESSIONLESS_README_PREVIEW_PORT must be an integer from 1 to 65535');
}
await access(resolve(webRoot, '200.html'));

const session = {
  session_id: sessionId,
  status: 'active',
  title: 'Launch planning',
  preview: 'What should we ship next?',
  last_sequence: 2,
  created_at: '2026-08-18T09:00:00Z',
  updated_at: '2026-08-18T10:00:00Z',
};
const events = [
  {
    event_id: 'event-1',
    sequence: 1,
    kind: 'user_message',
    content: { text: 'What should we ship next?' },
    created_at: '2026-08-18T09:00:00Z',
  },
  {
    event_id: 'event-2',
    sequence: 2,
    kind: 'assistant_message',
    content: { text: 'Ship the canonical WebUI.' },
    created_at: '2026-08-18T09:00:05Z',
  },
];

const contentTypes = new Map([
  ['.css', 'text/css; charset=utf-8'],
  ['.html', 'text/html; charset=utf-8'],
  ['.ico', 'image/x-icon'],
  ['.js', 'text/javascript; charset=utf-8'],
  ['.json', 'application/json; charset=utf-8'],
  ['.png', 'image/png'],
  ['.svg', 'image/svg+xml'],
  ['.webp', 'image/webp'],
]);

function sendJSON(response, body, status = 200, extraHeaders = {}) {
  response.writeHead(status, {
    'Cache-Control': 'no-store',
    'Content-Type': 'application/json; charset=utf-8',
    ...extraHeaders,
  });
  response.end(JSON.stringify(body));
}

function sendError(response, status, code) {
  sendJSON(response, { error: { code, message: code.replaceAll('_', ' '), request_id: `readme-${code}` } }, status);
}

function handleAPI(pathname, response) {
  if (pathname === '/api/web/v1/me') {
    sendJSON(response, {
      user_id: 'user-one',
      provider: 'telegram',
      tenants: [
        { tenant_id: 'tenant-one', role: 'owner', active: true },
        { tenant_id: 'tenant-two', role: 'member', active: false },
      ],
    });
    return true;
  }
  if (pathname === '/api/web/v1/sessions') {
    sendJSON(response, { items: [session] }, 200, { ETag: '"sessions-readme-v1"' });
    return true;
  }
  if (pathname === `/api/web/v1/sessions/${sessionId}`) {
    sendJSON(response, session, 200, {
      ETag: '"session-readme-v1"',
      'X-Sessionless-Poll-After-Ms': '60000',
    });
    return true;
  }
  if (pathname === `/api/web/v1/sessions/${sessionId}/events`) {
    sendJSON(response, { items: events }, 200, {
      ETag: '"events-readme-v1"',
      'X-Sessionless-Poll-After-Ms': '60000',
    });
    return true;
  }
  if (pathname === `/api/web/v1/sessions/${sessionId}/runs`) {
    sendJSON(response, { items: [] }, 200, {
      ETag: '"runs-readme-v1"',
      'X-Sessionless-Poll-After-Ms': '60000',
    });
    return true;
  }
  if (pathname === `/api/web/v1/sessions/${sessionId}/compute`) {
    sendJSON(response, {
      availability: 'ready',
      connection: {
        provider: 'fixture-ai',
        entitlement: 'active',
        quota: 'available',
        observed_at: '2026-08-18T10:00:00Z',
      },
    });
    return true;
  }
  return false;
}

const server = createServer(async (request, response) => {
  if (request.method !== 'GET') {
    sendError(response, 405, 'read_only_preview');
    return;
  }

  const url = new URL(request.url ?? '/', `http://${host}:${port}`);
  if (url.pathname.startsWith('/api/web/v1/')) {
    if (!handleAPI(url.pathname, response)) sendError(response, 404, 'not_found');
    return;
  }

  const requested = url.pathname === '/' ? '200.html' : url.pathname.slice(1);
  let target = resolve(webRoot, requested);
  if (!target.startsWith(`${webRoot}${sep}`) && target !== webRoot) {
    sendError(response, 404, 'not_found');
    return;
  }

  try {
    const targetStat = await stat(target);
    if (!targetStat.isFile()) throw new Error('not a file');
  } catch {
    target = resolve(webRoot, '200.html');
  }

  response.writeHead(200, {
    'Cache-Control': 'no-store',
    'Content-Type': contentTypes.get(extname(target)) ?? 'application/octet-stream',
  });
  createReadStream(target).pipe(response);
});

server.listen(port, host, () => {
  console.log(`Sessionless README visual preview: http://${host}:${port}/sessions/${sessionId}`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
