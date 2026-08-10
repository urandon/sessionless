import assert from "node:assert/strict";
import test from "node:test";

import { handle } from "../src/worker.js";

const endpoint = "https://dev-api-sessionless.triborg.dev/telegram/webhook";
const env = {
  TELEGRAM_WEBHOOK_SECRET: "telegram-secret",
  YANDEX_WORKFLOW_URL: "https://workflows.invalid/private-capability",
};
const update = '{"update_id":42,"message":{"text":"hello"}}';

function request(options = {}) {
  return new Request(options.url || endpoint, {
    method: options.method || "POST",
    headers: {
      "Content-Type": options.contentType || "application/json",
      "X-Telegram-Bot-Api-Secret-Token": options.secret ?? env.TELEGRAM_WEBHOOK_SECRET,
      ...options.headers,
    },
    body: options.method === "GET" ? undefined : options.body ?? update,
  });
}

function acceptedExecution(assertion) {
  return async (url, init) => {
    assertion?.(url, init);
    return Response.json({ executionId: "workflow-execution-id" });
  };
}

test("accepts a Telegram update and preserves the body", async () => {
  const result = await handle(
    request(),
    env,
    acceptedExecution((url, init) => {
      assert.equal(url, env.YANDEX_WORKFLOW_URL);
      assert.equal(init.method, "POST");
      assert.equal(init.headers["Content-Type"], "application/json");
      assert.equal(new TextDecoder().decode(init.body), update);
      assert.equal("X-Telegram-Bot-Api-Secret-Token" in init.headers, false);
    }),
  );

  assert.equal(result.status, 204);
});

test("rejects an unknown path and method", async () => {
  assert.equal((await handle(request({ url: `${endpoint}/other` }), env)).status, 404);
  assert.equal((await handle(request({ method: "GET" }), env)).status, 405);
});

test("fails closed when bindings are missing", async () => {
  assert.equal((await handle(request(), {}, acceptedExecution())).status, 503);
});

test("rejects a missing or incorrect Telegram secret", async () => {
  assert.equal((await handle(request({ secret: "wrong" }), env)).status, 401);
  const withoutSecret = request();
  withoutSecret.headers.delete("X-Telegram-Bot-Api-Secret-Token");
  assert.equal((await handle(withoutSecret, env)).status, 401);
});

test("rejects non-JSON and malformed updates", async () => {
  assert.equal(
    (await handle(request({ contentType: "text/plain" }), env)).status,
    415,
  );
  assert.equal((await handle(request({ body: "not-json" }), env)).status, 400);
  assert.equal((await handle(request({ body: '{"message":{}}' }), env)).status, 400);
});

test("rejects oversized updates from declared and actual length", async () => {
  const declared = request({ headers: { "Content-Length": "1048577" } });
  assert.equal((await handle(declared, env)).status, 413);

  const actual = request({ body: `{"update_id":42,"data":"${"x".repeat(1048576)}"}` });
  assert.equal((await handle(actual, env)).status, 413);
});

test("turns upstream errors and invalid acknowledgements into retries", async () => {
  const rejected = await handle(request(), env, async () => new Response(null, { status: 503 }));
  assert.equal(rejected.status, 502);

  const invalid = await handle(request(), env, async () => Response.json({}));
  assert.equal(invalid.status, 502);

  const failed = await handle(request(), env, async () => {
    throw new Error("network failure");
  });
  assert.equal(failed.status, 502);
});
