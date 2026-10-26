# Dual-provider login and explicit account linking

Version: 0.1.0. Status: proposed engineering design; not implemented or enabled.
Owners: [#172](https://gitcode.com/urandon/sessionless/issues/172),
[research/design #237](https://gitcode.com/urandon/sessionless/issues/237),
[WebUI epic #29](https://gitcode.com/urandon/sessionless/issues/29).

## Outcome and boundaries

One canonical Sessionless user can sign in through Yandex or Telegram after
explicitly linking both identities. Linking preserves UserID, memberships,
canonical sessions/history and owned resources. It never copies or merges
another user's data. Support Yandex-only, Telegram-only and both-provider
deployments. This follow-up is not another gate for the two-track AI MVP demo.

Only one active subject per provider and at most two active login methods are
supported. Passwords, a general provider catalog, account merging, provider
token custody and tenant provisioning are out of scope. Telegram **login** does
not enroll a Telegram chat or reassign bot actors, tenants or frontend bindings.
An already separate Telegram-owned account cannot be absorbed by this flow.

## Research and decisions

The following are documented capabilities, not observed live-provider tests.
The owning issue retains the research evidence and later acceptance receipts.

| Source | Finding and Sessionless decision |
| --- | --- |
| [Yandex authorization code](https://yandex.ru/dev/id/doc/en/codes/code-url), [account information](https://yandex.com/dev/id/doc/en/user-information) | Use S256 code exchange and account API `id` with exact `client_id`; keep tokens server-side. `force_confirm=yes` requests consent/account selection, not a guarantee of another password or MFA. No invented Yandex OIDC claims. |
| [Telegram Login](https://core.telegram.org/bots/telegram-login) | Reuse code/S256, registered callbacks and verified ID token. The documented identity is issuer-qualified `sub`; no separate UserInfo endpoint. Keep minimal `openid profile`, no phone/bot-message scope. Password age is not token issuance time. |
| [OAuth BCP, sections 2.1 and 4.4](https://www.rfc-editor.org/rfc/rfc9700.html), [OIDC Core, sections 2 and 3.1.2.1](https://openid.net/specs/openid-connect-core-1_0.html) | Bind browser, provider, client, purpose and callback; use distinct provider callbacks plus S256/state and Telegram nonce. `max_age` requires verified `auth_time`; `iat` is not a substitute. |
| [Auth0 account linking](https://auth0.com/docs/manage-users/user-accounts/user-account-linking) | Adopt explicit user initiation and proof of both accounts. Reject its primary/secondary profile-merge model: here another canonical user's subject is a conflict, not a merge candidate. |
| [Keycloak client-initiated linking](https://www.keycloak.org/docs/26.8.1/server_development/#client-initiated-account-linking) | Adopt configured-provider/account-settings initiation and bound browser authorization. Do not add an identity broker, external token retrieval or its legacy custom linking protocol. |
| [Supabase identity linking](https://supabase.com/docs/guides/auth/auth-identity-linking) | Adopt the manual link/settings pattern and protection of the final method. Reject email-driven automatic linking: Sessionless neither requires unique email nor trusts it as account ownership. |

These adoption/rejection choices are Sessionless engineering decisions. They do
not assert that the compared products share our storage or authorization model.

### Existing code and missing guarantees

- [BFF construction](../../internal/webbff/handler.go) requires exactly one
  OAuth/OIDC adapter; [deployment configuration](../../cmd/web-bff/main.go)
  selects one `WEB_LOGIN_PROVIDER`. Existing adapters do not implement two buttons.
- [Identity resolution and Web authorization](../../internal/ydbstore/web_auth.go)
  write forward/reverse identity rows, but Web sessions are checked against
  membership, not the current identity mapping. There is no canonical global
  user-status/security-version record; do not pretend membership revision is one.
- [Domain auth types](../../internal/domain/web_auth.go) have one-use login
  challenges, but no linking purpose, operation result or identity revision.
  Telegram claims expose issuance, not authentication time. [OAuth](../../internal/ports/oauth.go)
  and [OIDC](../../internal/ports/web.go) ports need explicit purpose/proof metadata.
- [Compute choice](../../internal/ydbstore/compute_choice_auth.go),
  [run explanation](../../internal/ydbstore/run_explanation.go),
  [search-consent authority](../../internal/ydbstore/web_search_consent_authority.go)
  and other transaction-local Web readers must enforce the new fence themselves.
  A BFF-only check or revoking one cookie leaves authorization races.
- [Telegram ingress](../../internal/ydbstore/operations.go) currently expects
  deterministic Telegram UserID. It must not rewrite linked identity ownership
  or reactivate an unlinked method. Cross-account messaging enrollment is separate.

## Closed provider configuration and routes

Define one canonical `WEB_LOGIN_PROVIDERS`: `yandex`, `telegram`, or
`yandex,telegram` in that order. Reject duplicates, other values and simultaneous
use of the old singular setting. Explicitly migrate deployment configuration;
do not implement silent old-name reads. Either single-provider configuration is
a supported product mode, not an alias. Enabled providers require their complete
pinned client configuration; missing credentials fail startup, not fallback.

The browser selects only an enabled enum. No issuer, endpoint, client, callback,
scopes, UserID or ownership selector is accepted from it. Adapters remain private.
Keep exact callbacks: Yandex `/auth/login/callback`, Telegram
`/auth/telegram/callback`. They dispatch using the stored provider/purpose/client
binding, never the incoming provider parameter or profile email. Compare actual
callback with the registered one before token exchange, not only after it.

| Operation | Proposed interface and authority |
| --- | --- |
| Discover login methods | `GET /auth/login/methods`: enabled enum/label/start path only, no secrets. |
| Start login | `POST /auth/login/{provider}/start`: exact Origin; browser-bound one-use challenge; safe local return path. Top-level form works without third-party JS. |
| Account settings | `GET /api/web/v1/account/login-methods`: current Web authorization; own method handles, provider, linked/current/enabled states, revision and allowed actions; no raw external subjects. |
| Start link | `POST /api/web/v1/account/identity-operations`: `{kind:link,target_provider,expected_revision}`; current cookie, exact Origin and CSRF. Source subject is resolved by server. |
| Start unlink | Same path: `{kind:unlink,method_handle,expected_revision}`. Resolve exact own method; source proof must use a method that remains after removal. |
| Continue provider proof | `POST /api/web/v1/account/identity-operations/{id}/continue`: original cookie/browser/CSRF and current operation phase; returns only a pinned authorization URL. |
| Confirm change | `POST /api/web/v1/account/identity-operations/{id}/confirm`: explicit confirmation, original cookie/browser/CSRF, exact revision and complete fresh proofs; transaction below. |
| Read outcome | `GET /api/web/v1/account/identity-operations/{id}`: valid current session for SAME user; minimal bounded result only. Operation ID alone is not authority. |

OAuth callbacks are redirect exceptions to Origin/CSRF headers, not exceptions
to one-use state, matching browser cookie, provider/client/purpose/callback and
original session checks. All auth/settings responses are `no-store`; callbacks
immediately redirect to a fixed local settings/login URL without code/state.
Use existing restrictive referrer/CSP policy; no popup SDK or relaxed COOP needed.

## Proof and operation protocol

Use a server-generated 256-bit operation ID, independent one-use OAuth state
and browser secret, and the existing opaque Sessionless session/CSRF cookies.
Keep a separate `__Host-sessionless-identity-operation` HttpOnly/Secure/Lax
browser-binding cookie; it is not a new account session. A different operation
in the same browser invalidates the earlier browser-bound continuation.

An operation is fixed to original UserID, session digest, authenticated subject,
active tenant/membership revision, expected link-set revision, configuration
revision, kind and exact target. It lasts at most ten minutes; each provider
challenge expires no later than that original deadline. Each proof must finish
after its own challenge creation and remain at most five minutes old at commit.
Never extend operation/proof lifetime by a refresh or database retry.

1. Authorize existing session and membership. Derive exact own identities from
   canonical rows; persist the operation. Cookie possession alone is not proof.
2. Perform a NEW source-provider authorization roundtrip. For link, prove the
   exact current authenticating subject. For unlink, prove an exact remaining
   enabled method (which may differ from the current login method). Never create
   an identity, rotate login user or grant membership during this proof.
3. For link, independently authorize the target provider using a second one-use
   state/PKCE/nonce. Keep verified subject privately in this operation, not in
   browser DTOs. Reject a target already belonging to another user without
   exposing that user's ID, profile, memberships or history.
4. Show a local confirmation naming provider/actions and the preservation of
   existing data. The confirmation cannot select a different target or revision.
5. Commit atomically, invalidate old login sessions by identity revision and
   rotate the initiating cookie to a new session bound to a surviving method.

Yandex proofs require new S256 exchange plus exact account API response and
`force_confirm=yes`; they establish fresh provider authorization, NOT a measured
password/MFA age. Telegram proofs request `max_age=0`; verify signed `auth_time`
against challenge start (existing bounded clock skew) and signed nonce as well
as issuer/audience/expiry/issuance. Missing/old `auth_time` denies the sensitive
operation. Its standard semantics are [OIDC Core](https://openid.net/specs/openid-connect-core-1_0.html#AuthRequest);
the Telegram documentation does not independently demonstrate this behavior.
The actual registered Telegram profile therefore needs a bounded compatibility
check before enabling linking. Never fall back to `iat`, cached token or ordinary
login success; ordinary login need not enforce sensitive-operation freshness.

This is dual provider proof, not a new password/MFA subsystem or a claim that
the providers have identical assurance. Preserve their distinct proof kinds in
the private record. If stronger assurance is required later, version the policy.

## Storage: reuse existing authorities, no new table

Extend the existing versioned identity/challenge/session records. The
[entity registry](../architecture/entity-registry.md) records proposed changes
separately from applied schema. Do not introduce user, link, grant, operation,
receipt, token-vault or session-index tables for this bounded feature.

### Identity revision and active set

Define explicit `ExternalIdentityV2` in existing `external_identities.record`:
version, original provider/subject/UserID/creation time, `state` (`active` or
`unlinked`), positive uint64 `security_revision`, updated time. Forward ownership
never changes. `external_identities_by_user` indexes ACTIVE methods only; it is a
projection, not authorization. Read the user prefix with LIMIT 3, reject more
than two, duplicate providers or mismatched/missing forward rows. Validate the
current authenticating forward row is included. All active rows share one
positive link-set revision; no separate account revision row is necessary.

Every link/unlink transaction reads that bounded range and all active forward
rows, then writes every surviving/added active forward row with the SAME next
revision. Set next to checked `max(active revision,target tombstone revision)+1`.
Reject overflow, stale expectation and inconsistent revisions. This shared write
set serializes concurrent changes, including opposing unlinks. It prevents old
session/proof reuse without scanning memberships or sessions. The initial
single-identity resolver creates active revision 1; it cannot attach to an
existing user or reactivate an unlinked subject.

Unlink removes only the reverse projection and marks the canonical forward row
unlinked. Keep that minimal ownership tombstone without an operational TTL:
ordinary login must deny, not create a new user or adopt the tombstone. Explicit
proof may relink the subject only to the SAME owner with a strictly greater
revision. A different unused subject may later occupy the same provider slot.
This protects immutable ownership and revision ABA, but means local unlink is
not account deletion or identity erasure. Future account-deletion policy owns
that separate decision; no raw provider tokens/profile attributes are retained.

### Sessions and all authorization consumers

`WebSessionV2` snapshots the exact active authenticating subject's revision.
Every session create, authorize, tenant switch, reauthentication and every
transaction-local Web authorization point-reads that forward identity and
checks active state, SAME UserID and exact revision, alongside unchanged
session/expiry/CSRF/membership/participant checks. No process-local cache can
renew or substitute this fact. A committed link/unlink bumps all remaining
identities, invalidating every pre-change Web session across tenants; canonical
conversation sessions and memberships are untouched. Rotate only the initiating
Web session, using a surviving proved method after unlink.

Cover direct readers and strict JSON field/read allowlists in compute choice,
run explanation, Web search consent and owned canonical authority paths. Preserve
their statement/materialization budgets; account for the added exact point read,
not a blanket budget increase. Retired preflight authorization cannot authorize
a later mutation: repeat the identity check inside the owning transaction.
There is no new global user suspension state here; identity/session/membership
revocation is enforced and remains explicitly distinct from a future account ban.

### Challenges, receipts and audit

Reuse `oidc_login_challenges` with explicit closed V2 tagged records: ordinary
login challenge, provider-proof challenge and identity-operation. Do not coerce
old records into a new purpose. Store parent reference/phase, original scope,
proof completion metadata and terminal result in those existing bounded records.
At most one operation and two proof challenges are needed per link; unlink
needs one proof. Keep each record <=8KiB, reject duplicate/unknown JSON members.
No code, access/refresh/ID token or authorization URL is durable. PKCE verifier
and nonce are temporary server-only fields, cleared immediately on consume.

Consume each proof challenge BEFORE exchange. Bind child to exact parent phase
and revision; the callback cannot advance another operation. Each provider
exchange is single-shot with an overall ten-second deadline, bounded bodies and
no redirects/retries. Consume then crash or ambiguous provider response means
that proof is unavailable: start a NEW explicit operation, never exchange again.

Add `delete_after` to the SAME challenge table and move its physical TTL from
`expires_at` to that column in a reviewed migration. Logical validity remains
`expires_at` and never depends on timely TTL deletion. Proof-only records expire
at their challenge deadline; clear temporary secrets earlier on consumption.
Operation records retain a minimal terminal result for 24 hours from original
creation; expiry/failure similarly removes proof/subject material. After that,
absence is `outcome_unavailable`, never proof of non-commit. No status read can
re-authorize a mutation or turn a receipt into a fresh link.

Write a content-free success audit in existing `audit_events` in the SAME
mutation transaction: action/provider/UserID/tenant, before/after revision and
operation-ID digest, no subject/email/name/token/cookie/PKCE. Reuse existing
retention policy. Extend the existing typed security-event enum for necessary
redacted denials in `web_security_audit_events`; an audit failure fails the
sensitive mutation rather than committing an unaudited change.

## Exact commit and ambiguous outcomes

The final serializable transaction reads the operation by exact key, validates
all original cookie/browser/current identity/membership/config/revision/proof
facts and checks expiry using the owning database clock. For link, point-read
target forward row: absent or same-owner unlinked may proceed; another owner,
another active provider slot or changed reservation is conflict. Never use
`ResolveOrCreateExternalIdentity` as a grant to attach an arbitrary subject.

For unlink, resolve exact own active target and require a DIFFERENT active,
enabled, fully configured remaining method whose source proof is current.
Usable is not simply a count of reverse rows; provider outages are not health
guarantees, but disabled/misconfigured methods cannot satisfy this check.
Recheck original configuration revision at commit. Then update forward/reverse
rows/revisions, revoke original cookie, create the rotated Web session, record
terminal outcome and append audit atomically. No membership/grant/resource
writes occur. Concurrency conflicts cannot repeat provider exchanges; a bounded
database retry must reread and revalidate the SAME immutable operation, deadline
and prepared output, never synthesize another ID or proof.

If commit response is lost, do not repeat exchange or mutation speculatively.
Read exact retained operation outcome under current same-user authorization.
If the old cookie was invalidated but its replacement was lost, the user signs
in again through a remaining method, then reads the receipt. This is intentional
recovery, not an old-cookie status bypass. A repeated confirm may return an
already committed SAME result only after current authorization; it cannot
restore an invalid cookie or resurrect a method. Pending/expired/missing is not
success or permission to replay. A new explicit attempt starts from current data.

## UX and failure behavior

Login shows one named button or both Yandex/Telegram buttons in stable order.
Account settings show two provider rows: linked/current, not linked, or linked
but unavailable in this deployment. Show no other user's data. A link action
explains two provider checks; unlink names the remaining sign-in method and
warns that other Web sessions will need sign-in again. Preserve return-to only
as an allowlisted local route, never a provider/user-supplied external redirect.

Use accessible labelled controls, keyboard navigation, visible focus and live
status/error announcements under the existing [design rules](../design-rules.md).
No silent callback-side attachment before final local confirmation. Cancel,
provider denial or timeout leaves identity/resource ownership unchanged.

Closed public outcomes: `linked`, `unlinked`, `cancelled`, `conflict`,
`stale_revision`, `proof_required`, `proof_unavailable`, `last_method`,
`provider_unavailable`, `expired`, `outcome_unavailable`, `access_denied`.
HTTP 409 denotes conflict/stale/last-method; 401/403 deny authorization, 410
denotes expired/unavailable operation, 503 denotes temporary provider/audit
failure. No raw upstream error or target-owner existence details are returned.
On refresh read the existing operation; do not auto-start OAuth or resubmit POST.

## Upgrade, enablement and rollback

This document changes no schema/runtime. Implementation uses explicit V2
record readers/writers and a separately authorized maintenance transition, not
silent defaults for old JSON. Keep feature off until all identity writers,
session consumers and direct transaction readers are upgraded together.

Inventory current forward/reverse identities with bounded maintenance batches;
validate unique ownership and at most one active subject/provider. Report
inconsistencies/duplicates without deleting or merging them. Backfill revision 1
only after correspondence is established. Stop old identity/session writers,
including bootstrap and Telegram ingress, during cutover; expire outstanding V1
challenges and revoke V1 Web sessions. Do not infer V2 authority from a V1 cookie.
No migration application/backfill/credential access is authorized by design work.

Telegram ingress may use an already matching canonical identity but must deny
its existing deterministic-user mismatch without reallocating actors/history;
it must never reactivate a tombstone. Account linking cannot be enabled with an
old ingress writer capable of overwriting these rules. Audit all helper callers.

Roll out both-provider discovery first with linking/unlinking disabled; then
enable sensitive operations only after source/YDB/browser acceptance and an
explicit registered-client compatibility/canary plan. No change to current
cloud credentials/registrations follows from a merged document. Disabling the
feature stops NEW operations but preserves V2 authorization, linked login and
receipt reads. Never roll back to V1 readers that ignore revisions/tombstones.
Emergency action is deny new auth/mutations with an operator recovery runbook,
not deleting identity rows or silently selecting another provider.

## Verification and implementation handoff

Use injected clocks, explicit barriers and isolated fixtures under the
[Go testing contract](../testing-best-practices.md). Fake providers prove
source behavior, not real registration, forced authentication or reachability.

| Family | Required evidence |
| --- | --- |
| Configuration/login | Three allowed modes, both buttons, missing/duplicate/unknown config denied; provider/client/callback/state/nonce mix-up; old singular setting explicitly rejected. |
| Continuity | Both initial directions, link, sign out, login through EACH method; exact unchanged UserID/memberships/history/files/resources, equal numeric provider subjects isolated. |
| Proof | New source plus independent target, stale/wrong/consumed/revoked proof, Yandex consent vs password distinction; Telegram missing/old auth_time denied; no reused code/token. |
| Transactions | Genuine disposable-YDB unique target race, opposing unlink race, link versus ordinary login/bootstrap/ingress, stale revision/overflow/tombstone ABA, atomic audit+projection+cookie outcome. |
| Revocation | Old cookies denied across tenants by main BFF AND direct compute/explanation/consent/canonical transactions; currently suspended/revoked membership, unlinked identity and missing row deny. |
| Recovery | Lost exchange/commit/browser response, expired/missing receipt, relogin after lost rotated cookie, refresh without repeated effect; expired rows remain logically denied before TTL. |
| UX/security | Settings link/unlink/cancel, last enabled method, changed configuration, safe conflict, no disclosure/log secrets, keyboard/mobile/a11y and two-owner resource isolation. |

After independent research/design acceptance, register real bounded children
and link their checklist under #172:

1. Versioned identity/session/challenge authority and storage cutover source:
   forward/reverse semantics, all transactional consumers, receipts/audit/TTL,
   bootstrap/ingress invariants and genuine YDB tests; update entity registry.
2. Fixed dual-provider BFF/config and purpose-bound proof orchestration,
   consuming child 1; fake-provider positive/negative tests and runbook.
3. Login/account-settings UI and joined browser acceptance, consuming 1–2;
   both-direction/subsequent-login/continuity/error/a11y tests.

Each child retains parent/design/dependencies/acceptance and scoped authority,
independent exact-artifact review, proportionate checks and all exact-head CI.
Research/design delivery does not close #172, claim both providers work live,
or authorize registrations, shared migrations, real credentials or deployment.
