#!/usr/bin/env node

import { appendFileSync } from "node:fs";
import { spawnSync } from "node:child_process";

function required(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function includesAudience(claim, expected) {
  return Array.isArray(claim) ? claim.includes(expected) : claim === expected;
}

const audience = required("YANDEX_OIDC_AUDIENCE");
const sourceSHA = required("IMAGE_PUBLISH_SOURCE_SHA");
if (!/^[0-9a-f]{40}$/.test(sourceSHA)) {
  throw new Error("IMAGE_PUBLISH_SOURCE_SHA must be a full lowercase commit SHA");
}
let oidcToken;
if (process.env.REGISTRY_OIDC_TEST_TOKEN && process.env.GITHUB_ACTIONS !== "true") {
  oidcToken = process.env.REGISTRY_OIDC_TEST_TOKEN;
} else {
  const requestURL = new URL(required("ACTIONS_ID_TOKEN_REQUEST_URL"));
  requestURL.searchParams.set("audience", audience);
  const oidcResponse = await fetch(requestURL, {
    headers: {
      Authorization: `Bearer ${required("ACTIONS_ID_TOKEN_REQUEST_TOKEN")}`,
    },
  });
  if (!oidcResponse.ok) {
    throw new Error(`GitHub OIDC token request failed with HTTP ${oidcResponse.status}`);
  }
  oidcToken = (await oidcResponse.json()).value;
}
if (!oidcToken) {
  throw new Error("GitHub OIDC response did not contain a token");
}

const tokenParts = oidcToken.split(".");
if (tokenParts.length !== 3) {
  throw new Error("GitHub OIDC token is not a JWT");
}
const claims = JSON.parse(Buffer.from(tokenParts[1], "base64url").toString("utf8"));

if (claims.iss !== "https://token.actions.githubusercontent.com") {
  throw new Error("unexpected GitHub OIDC issuer");
}
if (!includesAudience(claims.aud, audience)) {
  throw new Error("unexpected GitHub OIDC audience");
}
if (claims.repository !== required("GITHUB_REPOSITORY") ||
    claims.repository !== "urandon/sessionless") {
  throw new Error("unexpected GitHub OIDC repository");
}
if (claims.ref !== "refs/heads/main" || required("GITHUB_REF") !== "refs/heads/main") {
  throw new Error("image publication is restricted to refs/heads/main");
}
if (claims.sha !== sourceSHA || required("GITHUB_SHA") !== sourceSHA) {
  throw new Error("image publication identity differs from the verified source SHA");
}
if (claims.event_name !== "workflow_dispatch" ||
    required("GITHUB_EVENT_NAME") !== "workflow_dispatch") {
  throw new Error("image publication requires an explicit workflow_dispatch event");
}

const safeClaims = {
  issuer: claims.iss,
  audience: claims.aud,
  subject: claims.sub,
  repository: claims.repository,
  repository_id: claims.repository_id,
  repository_owner_id: claims.repository_owner_id,
  ref: claims.ref,
  sha: claims.sha,
  event_name: claims.event_name,
};
const safeClaimsJSON = JSON.stringify(safeClaims, null, 2);
console.log(safeClaimsJSON);
if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(
    process.env.GITHUB_STEP_SUMMARY,
    `### Verified GitHub OIDC claims\n\n\`\`\`json\n${safeClaimsJSON}\n\`\`\`\n`,
  );
}

if (process.env.OIDC_CLAIM_ONLY === "1") {
  process.exit(0);
}

if (claims.sub !== required("YANDEX_EXPECTED_OIDC_SUBJECT")) {
  throw new Error("GitHub OIDC subject differs from the Terraform-bound subject");
}

const exchangeBody = new URLSearchParams({
  grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
  requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
  audience: required("YANDEX_IMAGE_PUBLISHER_SERVICE_ACCOUNT_ID"),
  subject_token: oidcToken,
  subject_token_type: "urn:ietf:params:oauth:token-type:id_token",
});
const exchangeResponse = await fetch("https://auth.yandex.cloud/oauth/token", {
  method: "POST",
  headers: { "Content-Type": "application/x-www-form-urlencoded" },
  body: exchangeBody,
});
if (!exchangeResponse.ok) {
  throw new Error(`Yandex token exchange failed with HTTP ${exchangeResponse.status}`);
}
const accessToken = (await exchangeResponse.json()).access_token;
if (!accessToken) {
  throw new Error("Yandex token exchange did not return an access token");
}

console.log(`::add-mask::${oidcToken}`);
console.log(`::add-mask::${accessToken}`);
const login = spawnSync(
  "docker",
  ["login", "--username", "iam", "--password-stdin", "cr.yandex"],
  { input: `${accessToken}\n`, encoding: "utf8", stdio: ["pipe", "inherit", "inherit"] },
);
if (login.error) {
  throw login.error;
}
if (login.status !== 0) {
  throw new Error(`docker login failed with exit code ${login.status}`);
}
