const WEBHOOK_PATH = "/telegram/webhook";
const TELEGRAM_SECRET_HEADER = "X-Telegram-Bot-Api-Secret-Token";
const MAX_UPDATE_BYTES = 1024 * 1024;
const HANDOFF_TIMEOUT_MS = 5000;

function response(status, message) {
  return new Response(message, {
    status,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "text/plain; charset=utf-8",
    },
  });
}

async function digest(value) {
  const encoded = new TextEncoder().encode(value);
  return new Uint8Array(await crypto.subtle.digest("SHA-256", encoded));
}

async function secretsEqual(actual, expected) {
  if (typeof actual !== "string" || typeof expected !== "string") {
    return false;
  }

  const [actualDigest, expectedDigest] = await Promise.all([
    digest(actual),
    digest(expected),
  ]);
  let different = 0;
  for (let index = 0; index < actualDigest.length; index += 1) {
    different |= actualDigest[index] ^ expectedDigest[index];
  }
  return different === 0;
}

function isJSON(contentType) {
  return /^application\/json(?:\s*;|$)/i.test(contentType);
}

function isTelegramUpdate(body) {
  try {
    const value = JSON.parse(new TextDecoder().decode(body));
    return (
      value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      Number.isSafeInteger(value.update_id)
    );
  } catch {
    return false;
  }
}

async function readBodyBounded(request, limit) {
  if (request.body === null) {
    return new Uint8Array();
  }

  const reader = request.body.getReader();
  const chunks = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      total += value.byteLength;
      if (total > limit) {
        await reader.cancel("request body exceeds the configured limit");
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

export async function handle(request, env, fetchImpl = fetch) {
  const url = new URL(request.url);
  if (url.pathname !== WEBHOOK_PATH) {
    return response(404, "not found");
  }
  if (request.method !== "POST") {
    return response(405, "method not allowed");
  }
  if (!env.TELEGRAM_WEBHOOK_SECRET || !env.YANDEX_WORKFLOW_URL) {
    return response(503, "edge is not configured");
  }

  const suppliedSecret = request.headers.get(TELEGRAM_SECRET_HEADER);
  if (!(await secretsEqual(suppliedSecret, env.TELEGRAM_WEBHOOK_SECRET))) {
    return response(401, "unauthorized");
  }
  if (!isJSON(request.headers.get("Content-Type") || "")) {
    return response(415, "application/json required");
  }

  const declaredLength = request.headers.get("Content-Length");
  if (declaredLength !== null) {
    const parsedLength = Number(declaredLength);
    if (!Number.isSafeInteger(parsedLength) || parsedLength < 0) {
      return response(400, "invalid content length");
    }
    if (parsedLength > MAX_UPDATE_BYTES) {
      return response(413, "update too large");
    }
  }

  const body = await readBodyBounded(request, MAX_UPDATE_BYTES);
  if (body === null) {
    return response(413, "update too large");
  }
  if (!isTelegramUpdate(body)) {
    return response(400, "invalid Telegram update");
  }

  let upstream;
  try {
    upstream = await fetchImpl(env.YANDEX_WORKFLOW_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body.buffer,
      signal: AbortSignal.timeout(HANDOFF_TIMEOUT_MS),
    });
  } catch {
    return response(502, "workflow handoff failed");
  }
  if (!upstream.ok) {
    return response(502, "workflow handoff failed");
  }

  let acknowledgement;
  try {
    acknowledgement = await upstream.json();
  } catch {
    return response(502, "invalid workflow acknowledgement");
  }
  if (
    acknowledgement === null ||
    typeof acknowledgement !== "object" ||
    typeof acknowledgement.executionId !== "string" ||
    acknowledgement.executionId.length === 0
  ) {
    return response(502, "invalid workflow acknowledgement");
  }

  return new Response(null, {
    status: 204,
    headers: { "Cache-Control": "no-store" },
  });
}

export default {
  fetch(request, env) {
    return handle(request, env);
  },
};
