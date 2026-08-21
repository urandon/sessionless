#!/usr/bin/env node

import { appendFileSync } from "node:fs";

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

const oidcToken = (await oidcResponse.json()).value;
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
if (claims.repository !== required("GITHUB_REPOSITORY")) {
  throw new Error("unexpected GitHub OIDC repository");
}
if (claims.ref !== "refs/heads/main") {
  throw new Error("registry cleanup is restricted to refs/heads/main");
}
if (claims.sub !== required("YANDEX_EXPECTED_OIDC_SUBJECT")) {
  throw new Error("GitHub OIDC subject differs from the Terraform-bound subject");
}

const exchangeBody = new URLSearchParams({
  grant_type: "urn:ietf:params:oauth:grant-type:token-exchange",
  requested_token_type: "urn:ietf:params:oauth:token-type:access_token",
  audience: required("YANDEX_SERVICE_ACCOUNT_ID"),
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
if (!accessToken || /[\r\n]/.test(accessToken)) {
  throw new Error("Yandex token exchange did not return a valid access token");
}

console.log(`::add-mask::${oidcToken}`);
console.log(`::add-mask::${accessToken}`);
appendFileSync(required("GITHUB_ENV"), `YC_TOKEN=${accessToken}\n`);
appendFileSync(required("GITHUB_ENV"), `YDB_ACCESS_TOKEN_CREDENTIALS=${accessToken}\n`);
console.log("exchanged the verified GitHub OIDC identity for a masked Yandex IAM token");
