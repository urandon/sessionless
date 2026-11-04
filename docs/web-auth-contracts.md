# Web authentication and API contracts

This document freezes the WEB-01 contracts. WEB-02 implements the Go BFF,
Telegram OIDC adapter, YDB auth tables, and operator bootstrap described here.
WEB-07 (#168/#171) adds the Yandex OAuth variant below; ordinary #239 login
adds explicit one- or two-provider discovery and form starts without changing
membership, session or API authority. The canonical resource
API and browser application remain WEB-03 and WEB-04.

The WebUI is a projection over canonical Sessionless sessions and events.
The operator explicitly enables Yandex ID, Telegram OIDC, or both, with no
default provider. Provider accounts, chats, usernames and transport identifiers
do not grant product access.

## Trust boundaries

```mermaid
flowchart LR
    Browser["Untrusted browser"] -->|"opaque cookie + exact Origin + CSRF"| BFF["Go Web BFF"]
    BFF -->|"code + PKCE verifier"| OIDC["Selected Yandex OAuth or Telegram OIDC"]
    BFF -->|"digests and resolved IDs"| Auth["YDB auth records"]
    BFF -->|"authorized tenant + session"| Core["Canonical application ports"]
    BFF -->|"short-lived upload capability"| Storage["Object Storage"]
    Core --> YDB["YDB canonical sessions/events"]

    classDef untrusted fill:#ffe5e5,stroke:#a00;
    class Browser untrusted;
```

The browser may supply resource and tenant selectors. The BFF treats them as
untrusted input and resolves authority from the current web session, an active
tenant membership, and (for session resources) a session participant record.
Authentication alone never creates a tenant or membership.

## Selected Yandex OAuth variant (#171)

`WEB_LOGIN_PROVIDERS` must be exactly `yandex`, `telegram`, or `yandex,telegram`.
Missing/empty, duplicate, reversed or unknown selections deny startup, as does
the presence of retired `WEB_LOGIN_PROVIDER` even if empty. Each selected client
must be complete; there is no credential fallback. Terraform uses the matching
`web_login_providers` value. Yandex is Authorization Code + S256 PKCE at
`POST /auth/login/yandex/start` with the fixed `/auth/login/callback`; Telegram
starts at `POST /auth/login/telegram/start` and retains `/auth/telegram/callback`.
Both callbacks derive from the exact `WEB_BASE_URL` and are issuer/client-bound.

The `/login` chooser consumes methods metadata with labels exactly `Yandex` and
`Telegram` and the two fixed start paths, never arbitrary provider URLs. Starts
accept only the bounded URL-encoded `return_to` form and exact Origin, not a
provider field, query selector or automatic GET fallback. The old
`GET /auth/login/start` and `GET /auth/telegram/start` are removed. Callback
provider/client/issuer attribution comes from the bound path and stored
challenge, not a global default or an empty-provider legacy inference.

Operator migration must remove the singular environment/TF settings, complete
every enabled client, and create a new Web Lockbox version outside Terraform
with `yandex-login-client-secret` and/or `telegram-oidc-client-secret`. There is
no shared `oidc-client-secret` fallback. Pin image/revision and secret version
and co-land gateway/UI/BFF changes under separate rollout authority; see the
[migration checklist](development.md#ordinary-login-provider-configuration).
Metadata-GET/Origin-POST smoke checks each selected method without following
redirects; it is not registration, deployment, a completed callback or linking
proof.

The adapter pins `https://oauth.yandex.ru/authorize`,
`https://oauth.yandex.ru/token` and `https://login.yandex.ru/info?format=json`.
It requests only `login:info`, exchanges the code privately with client
authentication, and uses the transient access token only in the account API's
`Authorization: OAuth` header. The returned numeric `id` is accepted only with
an exact configured `client_id`. No profile email, name, login or `psuid`
selects or links a canonical user. Tokens do not cross `OAuthIdentityProvider`.

This is not OIDC: no ID token, JWKS or nonce is assumed. Its durable single-use
challenge binds browser, selected provider, client, callback and PKCE verifier.
Consume occurs before exchange, with one total bounded token/account timeout, no
redirect following or automatic token retry, and strict bounded JSON parsing.
Existing sessions still require a fresh active membership and security version.
Telegram's nonce/signature/JWKS rules below apply to its OIDC path only.

Official contracts: [Yandex code exchange](https://yandex.ru/dev/id/doc/ru/codes/code-url)
and [account information](https://yandex.ru/dev/id/doc/ru/user-information).
See [configuration and proof](web-bff.md#yandex-provider-configuration) and
[development](development.md#yandex-id-login). Explicit dual-proof identity
linking is tracked under [#172](https://gitcode.com/urandon/sessionless/issues/172)/#239;
account settings and joined browser acceptance remain #240.

The [dual-provider account-linking design](design/dual-provider-account-linking.md)
under [#237](https://gitcode.com/urandon/sessionless/issues/237) specifies
simultaneous-provider configuration, dual-proof linking and revision-fenced
unlinking. Ordinary provider configuration alone does not activate linking.
The optional `IdentityAccountServiceV2` mounts the explicit account-operation
routes only when a trusted caller supplies an already V2-configured Store and
the identical immutable provider manifest. Normal startup supplies no such
service; this is not a public partial V2 installer or migration path. Registered
client compatibility, complete native acceptance and the separately authorized
writer cutover remain rollout requirements. No automatic matching or merging
of accounts, tenant enrollment or grants follow from either login provider.

Account reads authenticate the current canonical session without refreshing
idle activity. Link/unlink mutations require exact Origin, cookie/header CSRF
equality, an opaque positive decimal-string revision and the original operation
browser cookie; the owning transaction rechecks actual authority. The provider
callback uses its stored closed purpose, never cookie presence or a LOGIN
decoder fallback. A cross-provider top-level GET may omit the Strict CSRF
cookie: the proof consume transaction derives that digest from the exact current
original session and still requires its separate Lax operation binding.

The original fifteen-second sensitive deadline includes purpose lookup, SQL,
provider exchange and proof recording. Consumed challenges are scrubbed before
exchange and never reset. Refresh reads a retained same-user outcome, including
after legitimate relogin; it never exchanges a code, repeats confirmation or
remints a lost replacement cookie. Known proof failure pruning and its closed,
content-free audit are atomic; unknown storage/commit/cancellation results do
not manufacture a receipt. Provider-unavailable does not mean an OAuth code was
unused or can be exchanged again.

## Telegram OIDC flow

When Telegram is selected, the flow is Authorization Code with PKCE `S256`. Telegram documents
the authorization, token, and JWKS endpoints and requires server-side ID-token
validation. OAuth security guidance recommends PKCE for confidential web
clients as well as public clients, transaction-specific state/nonce values,
and exact registered redirect URI matching:

- [Telegram Login OIDC](https://core.telegram.org/bots/telegram-login)
- [OpenID Connect Core 1.0](https://openid.net/specs/openid-connect-core-1_0.html)
- [RFC 7636: PKCE](https://www.rfc-editor.org/rfc/rfc7636)
- [RFC 9700: OAuth 2.0 Security Best Current Practice](https://www.rfc-editor.org/rfc/rfc9700)

```mermaid
sequenceDiagram
    actor User
    participant Browser
    participant BFF
    participant YDB
    participant Telegram

    User->>BFF: GET /login and GET /auth/login/methods
    User->>BFF: POST /auth/login/telegram/start (Origin + form)
    BFF->>YDB: store state digest, browser-binding digest, nonce, verifier, expiry
    BFF-->>Browser: browser-binding cookie + Telegram redirect
    Browser->>Telegram: code request with state, nonce, PKCE S256
    Telegram-->>BFF: code + state
    BFF->>YDB: atomically consume challenge by state digest and browser binding
    BFF->>Telegram: server-side code exchange with verifier and client secret
    Telegram-->>BFF: signed ID token
    BFF->>BFF: verify signature, algorithm, iss, aud, exp, iat, nonce
    BFF->>YDB: resolve/create external identity only
    BFF->>YDB: resolve explicit enrollment grant and active memberships
    alt no membership or grant
        BFF-->>Browser: 403 access_denied
    else active membership
        BFF->>YDB: store opaque-session and CSRF digests
        BFF-->>Browser: __Host- cookies; redirect without code/state
    end
```

### Protocol defaults

- Production issuer is exactly `https://oauth.telegram.org`; audience is the
  configured Bot ID. The redirect URI is one exact HTTPS URI. Loopback HTTP is
  allowed only in an explicitly local environment.
- State, nonce, browser-binding, session, invitation, and CSRF secrets contain
  at least 32 random bytes. Application tables store SHA-256 digests, not raw
  bearer values.
- The PKCE verifier is 43–128 RFC 7636 unreserved characters. Only `S256` is
  accepted.
- The requested scopes are exactly `openid profile`; phone access is not
  requested by the MVP.
- A login challenge expires after 10 minutes and is consumed exactly once in a
  serializable transaction before code exchange. It is bound to the browser
  that initiated the login.
- The MVP pins the Telegram client to `RS256`. A configured algorithm change is
  a reviewed deployment change, not a value accepted from the token header.
- JWKS responses have a bounded 10-minute cache. An unknown `kid` triggers one
  synchronous refresh; if the key or allowed algorithm remains unknown, login
  fails closed.
- The callback response clears code/state-bearing URLs with an immediate local
  redirect and sends `Cache-Control: no-store` and a restrictive Referrer
  Policy. Authorization codes, tokens, cookies, raw state/nonce, PKCE verifiers,
  invitation secrets, CSRF values, and upload URLs are redacted from logs.

`internal/domain.OIDCLoginChallenge`, `OIDCIdentityClaims`, and
`internal/ports.OIDCProvider` encode these boundaries. The OIDC adapter returns
verified identity claims only; provider tokens cannot cross the port.

## Identity, membership, and enrollment

An `ExternalSubject(provider, subject)` maps immutably to one internal
`UserID`. A successful verified provider exchange may create or refresh this mapping. It may
not create a tenant membership by itself.

An active `TenantMembership(tenant_id, user_id)` is the authorization source.
Roles are `owner`, `member`, and `viewer`; statuses are `active`, `suspended`,
and `revoked`. Every security-relevant membership mutation increments a
positive `security_version`, invalidating web sessions issued against the old
version.

Enrollment sources are evaluated in this order:

1. an active membership corroborated by an existing authorized frontend
   binding;
2. a one-time tenant invitation;
3. an explicitly audited development bootstrap grant.

There is no fourth or implicit fallback. Invitations store a secret digest,
expiry, role, and optional provider/subject restriction. Consumption and
membership creation are one serializable transaction; expiry, subject mismatch,
replay, and competing consumption fail closed.

The development bootstrap exists only for `cloud-dev`. `make web-bootstrap`
exposes it as an operator-only command with these requirements:

- use the normal YDB metadata/environment credential chain; never accept a YDB
  IAM token, invitation secret, or service-account key on the command line;
- require an already verified external identity or an explicitly supplied
  internal user ID plus provider/subject match;
- require the exact environment, tenant, role, operator identity, human reason,
  and typed confirmation through an interactive prompt or standard input;
- atomically create the membership and append a redacted audit record;
- be idempotent only for the exact same user, tenant, role, operator, and reason;
- refuse every environment except `cloud-dev`.

The optional first-identity mode also requires exact provider/subject
confirmation. Identity, membership and audited grant commit atomically. A
subject already owned by another user, or a target user with any different
external identity, is rejected; operator bootstrap is not an account-link path.

This procedure does not depend on a Telegram webhook. General membership
administration and production bootstrap are outside the MVP contract.

## First-party web session

The browser receives at least 256 bits of opaque session material. YDB stores
only its digest, the authenticated subject, internal user, active tenant,
membership security version, CSRF digest, and lifecycle timestamps.

| Property | Default / invariant |
| --- | --- |
| Cookie | `__Host-sessionless`; `Secure`; `HttpOnly`; `SameSite=Lax`; `Path=/`; no `Domain` |
| Idle expiry | 12 hours, enforced server-side |
| Absolute expiry | 7 days, enforced server-side |
| Rotation | mandatory after login and every tenant switch |
| Revocation | logout, membership suspension/revocation/version change, operator action |
| Tenant switch | selected tenant must have a fresh active membership; old digest is revoked atomically |
| Browser storage | no access/ID token, client secret, provider credential, or Sessionless bearer token |

Every request resolves the session digest by a bounded point lookup, then
rechecks expiry, revocation, membership status, user/tenant equality, role, and
security version. A browser-supplied `tenant_id` is only a tenant-switch
selector. It never overrides `active_tenant_id` in the stored session.

Every authenticated mutation also requires an exact normalized HTTPS `Origin` and a
session-bound CSRF value in `X-Sessionless-CSRF`. The readable
`__Host-sessionless-csrf` cookie uses `Secure`, `SameSite=Strict`, `Path=/`, and
no `Domain`; YDB stores its digest with the HttpOnly session.

## Same-origin API

The DTOs live in `internal/webcontract`. They intentionally contain no
authoritative user, role, or tenant field, except that
`POST /api/web/v1/active-tenant` carries the tenant selector that must be
resolved to a membership before session rotation.

| Method and path | Request selector/body | Authorization and result |
| --- | --- | --- |
| `GET /login` | none | chooser, not an automatic provider redirect |
| `GET /auth/login/methods` | no query/body | read-only `{methods:[{provider,label,start_path}]}`; max two known entries in configured order, no DB/provider calls |
| `POST /auth/login/yandex/start` | exact Origin; bounded URL-encoded form with one optional local `return_to` | enabled Yandex/browser/client-bound challenge; redirect |
| `POST /auth/login/telegram/start` | same form/Origin contract | enabled Telegram/browser/client-bound challenge; redirect |
| `GET /auth/login/callback` | exactly one of `code` or `error`, plus `state` | consume Yandex/client/browser-bound challenge; verify account; resolve membership; stable denial without provider details |
| `GET /auth/telegram/callback` | exactly one of `code` or `error`, plus `state` | consume Telegram/client/browser-bound challenge; verify claims; resolve enrollment; on failure redirect to stable `access_denied`, or `temporarily_unavailable` when durable failure audit cannot be written; never expose provider details |
| `POST /auth/logout` | none | CSRF; revoke current digest; clear cookies |
| `GET /api/web/v1/me` | none | resolved identity and memberships |
| `GET /api/web/v1/tenants` | none | active memberships only |
| `POST /api/web/v1/active-tenant` | `tenant_id` selector | active membership; rotate session |
| `GET /api/web/v1/sessions` | bounded cursor/limit | active tenant plus participant read grant |
| `POST /api/web/v1/sessions` | idempotency key | membership write grant; create canonical session |
| `GET /api/web/v1/sessions/{session_id}/events` | `after_sequence` or cursor, bounded limit | participant read grant; ordered canonical events |
| `GET /api/web/v1/sessions/{session_id}/compute` | session selector | participant read grant; credential-free compute selection and quota projection |
| `POST /api/web/v1/sessions/{session_id}/archive` | desired state/idempotency | participant write grant |
| `POST /api/web/v1/sessions/{session_id}/messages` | text, up to 8 committed upload IDs, idempotency key | participant write grant; canonical ingestion port |
| `POST /api/web/v1/uploads` | session selector, idempotency key, name, media type, size, SHA-256 | participant write grant; short-lived intent |
| `POST /api/web/v1/uploads/{upload_id}/commit` | matching upload selector | reauthorize and verify storage metadata |
| `GET /api/web/v1/runs/{run_id}` | run selector | run tenant/session participant read grant |
| `GET /api/web/v1/sessions/{session_id}/events/{sequence}/attachments/{index}` | canonical event and attachment selectors | participant read grant; short-lived exact-object download capability |

Resource-not-found and unauthorized-resource responses have the same public
shape so tenant/session ID probing cannot distinguish them. Errors use:

```json
{
  "error": {
    "code": "stable_machine_code",
    "message": "safe user-facing message",
    "request_id": "opaque-correlation-id"
  }
}
```

The initial status mapping is `400 invalid_request`, `401 unauthenticated`,
`403 access_denied|csrf_failed`, `404 not_found`, `409 conflict`, `413
payload_too_large`, `429 rate_limited`, and `503 temporarily_unavailable`.
Responses never echo credentials, provider payloads, internal object keys, or
authorization failure details.

### Audit contract

Append a redacted audit event for login success/failure, logout, tenant switch,
session rotation/revocation, invitation consumption, development bootstrap,
CSRF rejection, and privileged session/upload mutations. Each event contains a
stable action/code, occurred-at time, request ID, actor/user when resolved,
tenant when resolved, selected resource IDs, and the membership security
version used for authorization. Pre-authentication failures use a provider plus
one-way subject fingerprint rather than the raw provider token or claims body.

Audit records never contain authorization codes, ID/access tokens, client
secrets, cookies, state/nonce/verifier values, invitation or CSRF secrets,
upload URLs, request bodies, message content, file content, or raw error
objects. Audit persistence participates in the corresponding state transaction
where the action mutates YDB; best-effort logging is not an audit substitute.

## Upload-intent contract

Large browser uploads go directly to Object Storage through a short-lived
capability URL. The stored intent is bound to tenant, user, target canonical
session, object key, name, media type, expected size, expected SHA-256, expiry,
and one-time status. Its object key is generated server-side under:

```text
tenants/<tenant-id>/uploads/<upload-id>/...
```

Commit reauthorizes the current web session and target session, obtains object
metadata from Object Storage, and compares tenant, exact key, size, and digest.
The browser cannot commit a different key or tenant by changing its JSON. An
expired or already committed intent fails closed; abandoned objects are
retention-controlled staging data and never become canonical events.

Presigned URLs are capabilities: responses containing them use `no-store`, and
logs, audit payloads, analytics, browser-persistent storage, and referrers must
not contain them. WEB-03 defines allowed media types, maximum object size,
malware/content inspection, and the final event attachment projection.

## Browser response policy

WEB-02/04 must set at least:

- `Strict-Transport-Security: max-age=31536000; includeSubDomains` after the
  delegated development hostname is HTTPS-only;
- `Content-Security-Policy` with `default-src 'self'`, an exact
  `connect-src 'self' <WEB_OBJECT_STORAGE_ORIGIN>`, `object-src 'none'`,
  `base-uri 'none'`, and `frame-ancestors 'none'`; narrowly list only verified
  Telegram/OIDC navigation and Object Storage upload origins; wildcard
  capability origins are rejected at startup;
- `X-Content-Type-Options: nosniff`;
- `Referrer-Policy: no-referrer` on auth/capability responses and
  `strict-origin-when-cross-origin` elsewhere;
- `Cache-Control: no-store` on auth, identity, tenant, mutation, and upload
  capability responses; fingerprinted static assets may be immutable.

The BFF is same-origin. It does not enable credentialed wildcard CORS.

## Ownership of follow-up verification

- WEB-02: signature/JWKS behavior, challenge and invitation concurrency,
  identity immutability, session rotation/revocation, membership-version
  invalidation, and audited bootstrap.
- WEB-03: resource-level IDOR checks, canonical operation authorization,
  upload storage verification, bounded pagination, and error-shape parity.
- WEB-04: browser storage, CSP, cookie, and CSRF integration tests.
- WEB-06: two-user/two-tenant browser E2E, replay/fixation negatives, cloud
  headers, secret/log scanning, and incident procedures.

The focused threat model and executable-test mapping are in
[web-threat-model.md](web-threat-model.md).
