# WebUI authentication threat model

## Scope and assets

This model covers the implemented Go BFF, Telegram OIDC callback, and YDB auth
records, plus the planned same-origin WebUI, canonical-session API, and
direct-to-Object-Storage upload flow. Telegram message ingress, worker sandbox escape, provider-subscription
automation, and general tenant administration are covered by their own tracks.

Protected assets are tenant memberships, canonical sessions/events, uploaded
content, opaque web sessions, OIDC client credentials and tokens, invitation
capabilities, audit integrity, and the ability to enqueue AI work.

## Attacker assumptions

- A remote attacker controls a website, browser requests, URL/query/body
  fields, guessed opaque IDs, and uploaded bytes.
- A user may have membership in several tenants and may be suspended while a
  browser session remains open.
- Queue delivery, retries, callbacks, and browser requests may repeat or race.
- Logs, browser history, analytics, Terraform state, and object metadata are
  potential secondary disclosure channels.
- YDB and Object Storage IAM policies are necessary but do not replace
  application tenant checks.

## Threats, controls, and executable owners

| Threat | Required controls | Executable evidence owner |
| --- | --- | --- |
| Forged or replayed OIDC callback | browser-bound one-time state digest; PKCE S256; nonce; exact redirect; signature and allowed-algorithm verification; exact issuer/audience/time checks | WEB-01 claim/challenge tests; WEB-02 provider and concurrent-consumption tests |
| Authorization-code or token leakage | server-side exchange; immediate clean redirect; no-store; no-referrer; structured redaction; no browser token storage | WEB-02 HTTP/log tests; WEB-04 browser-storage tests; WEB-06 cloud log scan |
| Login/session fixation | at least 256 random bits; digest-only storage; rotate after login and tenant switch; revoke previous digest in the same transaction | WEB-01 rotation invariants; WEB-02 persistence tests; WEB-06 replay test |
| Stolen or stale session replay | Secure HttpOnly `__Host-` cookie; idle and absolute expiry; revocation; membership security-version recheck on every request | WEB-01 authorization matrix; WEB-02 expiry/revoke tests; WEB-06 browser E2E |
| Authentication becomes authorization | OIDC may create/refresh external identity only; active membership is mandatory; no implicit tenant | WEB-01 enrollment tests; WEB-02 unknown-identity test |
| Invitation theft/replay/race | digest-only one-time secret; short expiry; optional provider/subject binding; serializable consume plus membership create | WEB-01 grant/replay contract; WEB-02 two-consumer YDB test |
| Development bootstrap becomes a production backdoor | exact `cloud-dev` guard; metadata credentials; typed confirmation; required operator/reason; atomic redacted audit; no secret CLI args | WEB-01 validation; WEB-02 CLI tests; WEB-06 deployment-policy check |
| Tenant switch or resource IDOR | browser tenant is a selector; session resolves user and active membership; every session/run/upload checks tenant plus participant; indistinguishable denied/not-found response | WEB-01 two-user/two-tenant matrix; WEB-03 API negatives; WEB-06 E2E |
| CSRF | exact HTTPS Origin; session-bound CSRF digest; `SameSite` cookies; no state-changing GET; same-origin BFF | WEB-01 CSRF tests; WEB-02 handler tests; WEB-04 browser tests |
| Malicious or cross-tenant upload | server-generated tenant key; short expiry; expected size/digest/media type; commit uses storage-observed metadata; reauthorization; staging retention; later content inspection | WEB-01 intent tests; WEB-03 storage/inspection tests; WEB-06 cross-tenant E2E |
| Presigned upload URL leakage | capability response no-store/no-referrer; URL redaction; no analytics or persistent browser storage | WEB-03 log tests; WEB-04 browser tests; WEB-06 cloud log scan |
| XSS steals readable CSRF or performs actions | strict CSP; no unsafe inline script; escaped rendering; exact Origin plus server-side authorization; HttpOnly session | WEB-04 component/browser tests; WEB-06 deployed-header check |
| Cache exposes another user | no-store on personalized/auth/capability responses; cache keys never rely on untrusted tenant selectors | WEB-02/03 HTTP tests; WEB-06 gateway test |
| Secret disclosure through operations | Lockbox references outside Terraform state; metadata credentials; structured field allowlist; raw tokens/secrets forbidden in queue and audit records | WEB-02 redaction tests; WEB-05 Terraform review; WEB-06 secret scan |

## Security invariants

1. No request reaches a canonical application port until the stored web session
   and current membership have both been validated.
2. No OIDC claim, Telegram username, cookie field, request tenant, object key,
   or opaque resource ID is sufficient authorization by itself.
3. Membership suspension, revocation, or security-version change invalidates
   outstanding sessions without waiting for their TTL.
4. Challenge, invitation, tenant-switch rotation, and upload commit each have
   one serializable winner under concurrency.
5. Canonical event creation consumes only a committed, authorized upload
   intent; an uploaded object alone is not product history.
6. Provider tokens and Sessionless bearer secrets never enter canonical events,
   browser-readable persistent storage, queue envelopes, logs, audit payloads,
   Terraform state, or command arguments.

## Residual risks and gates

- Telegram JWKS or authorization endpoints may be temporarily unavailable.
  Login fails closed; an already authorized first-party session continues only
  until its own expiry/revocation checks fail.
- A presigned URL is a bearer capability until expiry. Keep expiry and allowed
  operation minimal, bind exact object metadata, and verify at commit.
- Content inspection rules are not frozen in WEB-01. WEB-03 must choose and
  test them before attachment uploads are enabled in cloud-dev.
- CSP correctness depends on the final Svelte bundle and upload endpoints.
  WEB-04/06 must inspect the rendered/deployed application, not only constants.
- Production operator bootstrap is intentionally unresolved and cannot reuse
  the `cloud-dev` exception.
