# Development

## Prerequisites

All supported versions are declared in `tools/versions.env`. Run:

```sh
make tools
```

The check is exact and fails with an actionable list when a tool is absent or
has drifted. The current foundation expects Go, Node.js, npm, Docker Compose, Docker Buildx, Terraform,
Yandex Cloud CLI (`yc`), YDB CLI, and Goose. Cloud tools are validated here even
though the first local process only needs Go and Docker.

| Tool | Pinned version | Installation source |
| --- | ---: | --- |
| Go | 1.26.8 | [go.dev/dl](https://go.dev/dl/) |
| Node.js | 24.19.0 | [nodejs.org downloads](https://nodejs.org/en/download) |
| npm | 11.17.0 | `npm install --global npm@11.17.0` |
| Docker Compose | 5.3.1 | [Docker Compose install](https://docs.docker.com/compose/install/) |
| Docker Buildx | 0.36.0 | [Docker Buildx install](https://docs.docker.com/build/install-buildx/) |
| Terraform | 1.15.5 | [HashiCorp releases](https://releases.hashicorp.com/terraform/1.15.5/) |
| Yandex Cloud CLI | 1.22.0 | [Yandex Cloud CLI install](https://yandex.cloud/en/docs/cli/operations/install-cli) |
| YDB CLI | 2.33.0 | [YDB CLI downloads](https://ydb.tech/docs/en/downloads/ydb-cli) |
| Goose | 3.27.1 | [Goose releases](https://github.com/pressly/goose/releases/tag/v3.27.1) |

For Goose, the reproducible Go installation command is:

```sh
go install github.com/pressly/goose/v3/cmd/goose@v3.27.1
```

The vendor installers for `yc` and YDB may install a newer release. Run
`make tools` afterward; update `tools/versions.env` in a reviewed change instead
of silently using mixed versions.

Do not use a checked-in file for credentials. `.env.example` contains safe
defaults and an empty token slot only. On macOS, a developer can place a token
in Keychain once:

```sh
security add-generic-password -a "$USER" -s sessionless.telegram-bot-token -w
```

Inject it into the process environment for the current shell:

```sh
export TELEGRAM_BOT_TOKEN="$(security find-generic-password -a "$USER" -s sessionless.telegram-bot-token -w)"
```

On other systems, use the OS credential store or a password manager that can
export into the child process environment. Never write service-account JSON or
subscription credentials into this repository.

## Fast path

### Attached execution composition (default off)

The normal Web BFF can use the existing attached scheduler admission path with
`WEB_ATTACHED_EXECUTION_ENABLED=true` and `WEB_ATTACHED_RESOURCE_PINS` containing
a JSON array of `sessionlessharness.AttachedResourcePin` values. Each entry pins
the tenant, owner, subscription resource, worker, enrollment generation,
capability and policy digests, and an explicit expiring harness-binding
template. The template has no run, attempt or placement digest; ingress fills
those from the canonical request. Configuration is bounded to 64 entries and
64 KiB, rejects unknown fields, and cannot silently fall back to managed work.
Enrollment rotation requires replacing the pin. Live worker authority is checked
before ingress and again by the existing atomic `AdmitDispatch` transaction.

`ATTACHED_WORKER_CONTROL_ENABLED=true` with an explicit
`ATTACHED_WORKER_CONTROL_AUDIENCE` mounts authenticated challenge, attach,
exchange, sealed-input and output-receipt routes in the normal control API.
This requires the existing YDB and Object Storage configuration, but not a
Telegram token or webhook secret. With both Telegram and attached control off,
the control API remains health-only. Both flags default to false; malformed or
partial enabled configuration fails startup.

Canonical context windows are retained and sealed using the same event/snapshot
codecs as the managed worker. Materialization verifies the exact session,
snapshot, event window, trigger, immutable references and size limits. Terminal
publication uses a ready server-owned receipt, not caller-supplied completion
material. This composition does **not** activate a real provider or grant a
credential: the production sealed-input constructor remains credentialless.
The credential-bearing provider proof uses only the integration-test adapter;
real-provider activation remains the separately reviewed #133 workstream.

```sh
make web-ci
make generate
make test
make build
make integration
```

`make web-ci` performs a lockfile-only `npm ci`, checks generated OpenAPI types
for drift, runs Prettier, ESLint, Svelte/TypeScript checks, unit/component tests,
and builds the static WebUI. The build is copied into the Go Web BFF embed tree
only after stale generated assets are removed. Node.js and npm are exact pins;
use `make web-tools` when only the Web toolchain needs validation. The separately
gated `make web-browser-install` installs pinned Chromium, and
`make web-browser-test` runs the Playwright end-to-end and axe accessibility
suites. CI adds Playwright's Linux system dependencies through the same target.

`make test` checks formatting, runs `go vet`, unit tests, and the race detector.
The repository-wide rules for deterministic clocks, isolation, cleanup,
diagnostics, repeated execution, and exact-commit CI evidence are in
[testing-best-practices.md](testing-best-practices.md).
`make build` writes every component declared by the Makefile to `.build/bin`.
This includes the control plane, Web BFF, local fixtures, isolated worker, and
operator-only schema, reset, deployment-lock, and Web bootstrap commands. The
Makefile is the authoritative component inventory; documentation deliberately
does not duplicate a count that drifts as slices are added.

### Worktrees and Go caches

`make` resolves the repository's Git common directory and shares
`GOCACHE` and `GOMODCACHE` below `.git/sessionless-go-cache` across every
linked worktree. Go's build cache is content-addressed and safe for concurrent
Go commands; the module cache is also normally shared by the Go toolchain.

`GOTMPDIR`, `.build/bin`, and `.build/dockerless` stay inside each
worktree. They may contain in-progress files, commit-specific binaries, process
metadata, logs, and mutable service data and must not be shared.

Inspect the resolved paths with:

```sh
make go-cache-status
```

`make clean` removes only the current worktree's `.build`. After all Go
commands in every linked worktree have stopped, `make go-cache-clean` removes
the default shared cache. The cleanup target refuses
`SESSIONLESS_GO_CACHE_ROOT` overrides (including those inside the Git common
directory) and symlinked cache roots; it never passes an override to its shell
cleanup command.
Existing checkout-local caches are not migrated automatically.

The fast `make ci` contract uses fake registry fixtures to verify immutable
publication failures and deterministic manifest/receipt separation. The real
container identity gate is intentionally separate because it performs ten cold
builds:

```sh
make image-reproducibility-test
```

It requires a running Docker daemon (Colima is supported), creates two
temporary digest-pinned BuildKit builders and a pinned loopback registry, builds
all five images twice from `git archive HEAD`, and compares config, diff-ID,
layer, and manifest identities. Cleanup removes only those uniquely named
temporary resources. CI runs this gate on every mirrored commit and retains the
second verified set for trusted-main publication.

The Go builder is the Docker Official Image `golang:1.26.8-alpine`, pinned to
OCI index `sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628`.
The reviewed `linux/amd64` child manifest is
`sha256:6e5de3f5b9fb7e30b8bb2ffe8dcbcbdaa2990f0f31267456eabe83f870a623be`,
published from `docker-library/golang` revision
`f47489bcbda87966b421340c536f39a34d00b45f` on 2026-09-01. These values are
recorded in `build/images.env`; `make image-build-inputs-test` rejects drift
between that image tag, its index provenance, `tools/versions.env`, `go.mod`,
and both Dockerfile defaults. Before its first build,
`make image-reproducibility-test` resolves the immutable index and rejects any
`linux/amd64` child digest, source URL/revision, or image-version annotation
that differs from this reviewed record.

The bounded Codex App Server feasibility evidence, stable protocol subset,
subscription-auth boundary, and still-open cloud/policy gates are documented
in [codex-subscription-worker.md](codex-subscription-worker.md). The selected
integration surface, credential locality, and production gates are recorded in
[codex-integration-surface.md](codex-integration-surface.md). That Phase A
client is intentionally not wired into worker product state yet and never
falls back to API-key billing.

The opt-in credential-free SDK/App Server/exec comparator, exact artifact
provenance, sanitized aggregate schema, and explicit operator-consent boundary
are in [codex-surface-measurement.md](codex-surface-measurement.md). Its local
Python environment and Codex binaries are research inputs and are not installed
or invoked by normal developer or CI targets.

The current memory, tooling/MCP, attached-worker, AI-resource, metering,
skills/automation, analytics, administration, and evaluation research is
indexed in [research/README.md](research/README.md). Those reports preserve
evidence, alternatives, open questions, and proposed epic decomposition; they
are not production contracts and do not close their research issues by
themselves.

The implemented AW-01 owner-scoped identity, enrollment, generation, and
deny-first revocation boundary is documented in
[attached-worker-identity.md](attached-worker-identity.md). It is a domain and
persistence contract only; it does not start a daemon or enable remote work.

The feature-disabled AW-03 bootstrap, immediate heartbeat transport, and
presence persistence boundary is documented in
[attached-worker-transport.md](attached-worker-transport.md). It does not
claim reconnect, dispatch, long polling, or cloud wake-up.

The AW-05a Go daemon core, exact process-supervision contract, credential
finalization order, and explicit unsupported-isolation boundary are documented
in [attached-worker-daemon.md](attached-worker-daemon.md). No developer command
or production binary enables it yet.

The feature-disabled AW-05b OCI isolation profile and its explicit opt-in
real-engine matrix are documented in
[attached-worker-oci.md](attached-worker-oci.md). Run only against a reviewed,
explicit local engine endpoint with `make attached-worker-oci-integration`;
the ordinary test/CI path uses deterministic fake-client coverage.

The #132 local execution-stack assembly verifies the manifest-pinned Docker
CLI and harness artifacts, reconciles installation-owned OCI residue, and
composes the supervisor with the existing credential runner. It remains a
library boundary: `attached-worker run` does not call it and ordinary tests do
not contact an engine or provider.

The #137 [service package staging and local control](attached-worker-packaging.md)
adds a single lease-holding, still feature-disabled `attached-worker serve`
mode, a versioned permission-bound local status/doctor/drain/stop endpoint,
and exact launchd/systemd-user/rootless-container artifact plan/apply/rollback
receipts. `make attached-worker-build` builds only its native binary;
`make attached-worker-package-test` repeats its focused race/shuffle tests.
The opt-in `make attached-worker-crash-integration` builds that exact binary,
starts a test-owned synthetic/denied-credential service, kills its exact PID,
and verifies authenticated idle reconnect and lease retirement after restart.
`make attached-worker-active-crash-integration` forks the command dispatch in
a test binary with an injected clock, kills its active attempt owner, and
verifies that restart fences the non-idle checkpoint without replaying input.
The shipped binary retains the 15-minute minimum heartbeat interval; this
bounded test does not claim exact-binary active-crash coverage.
These fixtures need no provider credentials, Docker engine, OS service
registration, or cloud resources; ordinary `make test` skips both process-kill
fixtures.
The #79 `make attached-worker-security-gate` makes the two-owner transport
collision/secret-theft and cloned-identity reconnect checks, sealed-input
owner and credential denials,
HTTP client/poller response-loss fencing (no retry before reconciliation while
the peer continues), in-flight active-heartbeat settlement before terminal
reporting (or fail-closed cleanup timeout), protocol cancel/revoke fencing, CLI attempt-root/sentinel
checks, and both
crash/restart fixtures non-optional in `make ci`. The YDB CI job separately
runs `make attached-worker-security-ydb-gate` after migration: two distinct
owners in one tenant hold live claimed attempts under a deliberately colliding
worker ID and identity key, cross-owner sealed-input requests are denied, and
revoking one worker does not revoke the other's claim. Tagged releases also
require this exact-YDB gate before image publication. The YDB
target needs `YDB_CONNECTION_STRING` and credentials appropriate for the
already migrated test database. Neither gate enables real provider access.
The bounded #79/#166 gate is now closed; rollout-platform isolation and egress
remain #133's responsibility, not a new exhaustive #79 reopening condition.
CI also runs the focused joined-provider gate first, then the complete security
gate, in its own job with a newly started YDB Local alongside the full YDB
integration job. Both jobs must pass before runtime images are checked. The
provider-first order distinguishes failure on a fresh database from contention
or accumulated state after other integration cases without dropping coverage.
The tagged YDB gate includes `TestAW07TwoActivatedProviderDaemonReceipts`:
two activated daemons with colliding worker IDs use distinct test-only
subscription resources, credential generations, sealed inputs, credential
file mounts, receipt publication, and canonical terminal commits. For a
focused local diagnosis against migrated YDB, run
`make attached-worker-joined-provider-ydb-gate`. The fake provider issues no
real secret and the fake OCI client cannot execute a provider call.
The race-enabled joined YDB fixture uses a bounded 45-second per-exchange
budget for Docker-backed CI contention; shipped activation retains its
15-second operation and HTTP request timeouts.
The YDB gate also holds an owner-A terminal pending, revokes A, and verifies
that server-side materialization cannot turn the stale terminal evidence into
canonical run finalization while owner B remains authorized. The terminal
commit checks the current unrevoked owner-scoped worker and connection head in
the same transaction as the canonical write; an already committed terminal may
still be replayed idempotently.
The same gate reconnects an idle owner A while owner B retains a claimed job,
then proves that A's old bearer and generation stay fenced after A claims a
new job and after A is revoked; B's claim remains authorized throughout.
It also runs a joined TLS/YDB response-loss test: A's presence update commits
before its HTTP response is dropped, B progresses through the same control
plane, A requires reconciliation without replay, and revoking A does not
revoke B's sealed-input authority. This response-loss case alone is not the
two-daemon process, artifact, and provider-resource canary proof. The same
YDB gate now drives two claimed owners through the real HTTPS sealed-input
endpoint with separate context and artifact bytes. A's artifact read is held
across revocation: B remains readable, while A's post-read authority check
discards the held bytes. Cross-owner bearer borrowing is denied before any
object read. This sealed-input case alone does not prove the two-daemon
process or provider-resource boundaries.
The gate additionally repeats this held-read/revocation case with A and B as
separate test-owned OS client processes. Each child receives only an
owner-scoped HTTPS bearer and request, not inherited YDB or object-store
credentials: a cross-owner request cannot open an object, B reads its own
context and artifact while A is held, and A receives no material after
revocation. This covers the network/process boundary of sealed-input
delivery. By itself it is not an OS sandbox, credential-path,
activated-daemon OCI, or provider-resource proof.
The YDB gate also starts two separately activated daemon processes with
colliding worker locator and identity key against the same TLS/YDB control
plane. Each claims its own job and reads its own sealed context and artifact.
The synthetic OCI client is test-owned, so this proves transport-to-daemon
composition, exact owner-specific object reads, active A revocation without
stopping B, B cancellation-to-pending-terminal behavior, exact digest
fail-closure, and attempt-root/sentinel cleanup, not a real OCI or provider
resource boundary. The joined provider test above adds the test credential and
receipt path and completed the narrowed #79 gate. It does not claim a real
provider turn or an exhaustive rootless two-owner crash/reconnect matrix.
The receipt YDB gate separately pins distinct test-only provider resource IDs
and credential generations to the two owners. It rejects a successful receipt
without credential-release evidence, then proves an owner-A canonical receipt
and TerminalAck leave owner B's run untouched. This is a storage and protocol
authority check by itself. The joined provider test above now exercises the
activated test-only credential lifecycle. Actual rollout-platform egress and
isolation proof remains required by #133 before enabling real credentials.
The opt-in `make attached-worker-native-integration` exercises one exact
test-owned launchd or systemd user-service lifecycle. The separate opt-in
`make attached-worker-rootless-integration` runs the same lifecycle on Linux
against an already provisioned rootless Docker engine and preloaded immutable
execution-base image; the pinned CI gate provisions those inputs. See
[attached-worker-packaging.md](attached-worker-packaging.md) for the
registration/start/inspect/drain/stop contract and the #137 evidence.
Staging does not register or start an OS service or container. With no explicit
private activation profile, the foreground owner remains feature-disabled.
The opt-in #165 synthetic/denied-credential path is described in
[attached-worker-packaging.md](attached-worker-packaging.md); it never enables
provider credentials or provider calls. #137/#165 are closed, including the
explicit activated rootless service path using narrowly configured host-engine
authority. The default unactivated rootless service remains offline. This
synthetic platform proof does not enable a real provider or remove #133's
rollout-specific egress/isolation gate.

The owner-facing AW-06a information architecture, read-model safety boundary,
and control-action gates are documented in
[attached-worker-ux.md](attached-worker-ux.md). WebUI and CLI implementations
must consume that contract rather than deriving lifecycle state client-side.

The minimum #78/#170 [private owner onboarding](attached-worker-onboarding.md)
uses `make attached-worker-admin ARGS="..."` with existing operator YDB
authority and typed confirmation, and `make attached-worker-setup ARGS="..."`
on the owner host. It retains a private pending identity before claim delivery,
registers one exact owner resource through production APIs, and completes local
state only from the matching receipt. No SQL fixture edits, service startup,
provider credentials or activation are implied. Eligibility remains unknown;
lost responses replay the same private grant/claim/rotation, not a new key.

The provider-neutral local credential binding, invocation handle, secure
materialization, crash recovery, write-back, and deny-first revocation contract
is documented in [credential-lifecycle.md](credential-lifecycle.md). Phase B0
is also intentionally not activated in worker runtime while #18 and #13 remain
open.

The feature-disabled serverless authority, local isolation supervisor, and
attested provider-egress/credential composition boundaries are documented in
[serverless-harness.md](serverless-harness.md),
[serverless-isolation.md](serverless-isolation.md),
[serverless-egress.md](serverless-egress.md), and the PR-03d
[Yandex substrate evidence plan](yandex-serverless-substrate.md). None registers
a concrete cloud launcher, provider proxy, secret backend, or production route.

The feature-disabled native [direct OpenRouter reference backend](direct-openrouter.md)
pins one non-streaming Chat Completions request and strict observed-route
response contract. Its tests use only a local fake boundary; no production HTTP
boundary, key lookup, DNS request, or provider call is enabled.

The [native provider composition](provider-composition.md) accepts four explicit
pinned drivers and assembles their disabled registrations below the existing
exact-match harness registry. It performs no profile or executable discovery,
does not select a default backend, and is not wired into `worker-runtime`.

Run `make provider-conformance` for the credential-free provider registry
matrix. It performs vet plus repeated race-enabled tests over strict fixtures,
including the native feature-disabled Codex/OpenRouter, OpenCode/OpenRouter,
Pi/OpenRouter, and direct OpenRouter profiles plus their closed composition. It
reads no provider secret, starts no provider
process, performs no network call, and does not enable Codex, OpenCode, Pi, or
direct OpenRouter. A generic fake result reports native backend protocol as
`skipped`, even when its exact registry tuple passes.

Deployment-aware cleanup of those immutable registry images is a separate,
fenced operational workflow. Its evidence bridge, dry-run/delete controls, and
audit reports are documented in [registry-gc.md](registry-gc.md). Never replace
that workflow with an age-only or tag-count cleanup.

## Local stack

The bounded Apple Silicon source-build spike for YDB `25.3.1.25` reached a
conditional no-go. Do not replace the pinned Linux container with an
unvalidated native binary; see the [native macOS YDB evidence and rerun
conditions](research/native-macos-ydb.md).

Start and initialize the complete local stand:

```sh
make dev-up
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8081/healthz
```

`make dev-up` first starts pinned YDB Local, Silo (a MinIO fork), ElasticMQ, and the
deterministic Telegram fake. It waits for the infrastructure endpoints,
creates the local bucket, and applies the embedded YDB migrations. Only after
that schema barrier does it start the control API, queue-driven Telegram
sender, and queue-driven reconciler, then idempotently loads the synthetic Telegram
fixture. A fresh-volume YDB storage-pool initialization is retried without
starting schema consumers. Its `YDB_MIGRATION_MAX_ATTEMPTS` bound defaults to
60. Once HTTP monitoring is live, only the exact SDK `failed to dial` timeout
for loopback `localhost`, `127.0.0.1`, or `[::1]` is also retryable; its
independent `YDB_LOCAL_DIAL_MAX_ATTEMPTS` bound defaults to 3 and covers raw or
slog-escaped quotes. Boot-storage markers take precedence. Generic deadlines,
remote endpoints, authentication/configuration errors, and DDL failures remain
fail-fast. Neither readiness path resets or deletes local data. The stand does
not require cloud credentials or a real Telegram token.

The worker is intentionally not kept alive by the default Compose profile.
Local mode consumes at most one queue message and exits; cloud mode serves one
bounded trigger-delivered batch per HTTP request. After an admitted local run
is present:

```sh
make worker-once
```

This starts the isolated `worker-runtime` profile, consumes at most one queue
message with the deterministic harness, stores checkpoints/artifacts/results,
then exits. An empty queue is a successful no-op. Scratch is a private tmpfs
and the container runs read-only as the distroless nonroot user.

The control API uses the YDB SDK single-connection balancer only inside the
Compose stand. This keeps the client on the Docker-resolvable `ydb-local`
endpoint instead of replacing it with YDB Local's host-facing discovery
address. Cloud deployments retain normal endpoint discovery and balancing.

## Web authentication development

For release scope, see the [WebUI-first MVP delivery plan](mvp-delivery-plan.md).
The MVP login selected in #168/#171 is Yandex ID OAuth, independent of Telegram
and bot setup. The existing Telegram OIDC configuration remains the default for
backward compatibility; selecting Yandex is explicit and never falls back to
Telegram when credentials are absent or verification fails.

### Yandex ID login

Set `WEB_LOGIN_PROVIDER=yandex`, `YANDEX_LOGIN_CLIENT_ID` to the registered
application's client ID, and inject `YANDEX_LOGIN_CLIENT_SECRET` from the
operator's credential store. `WEB_BASE_URL` is the exact public HTTPS origin;
register its `/auth/login/callback` URL with Yandex ID. Browser sign-in starts at
`/auth/login/start`. Request only the `login:info` permission, not email or avatar
access. The BFF uses these pinned production endpoints:

| Operation | Endpoint |
| --- | --- |
| Browser authorization | `https://oauth.yandex.ru/authorize` |
| Server-side code exchange | `https://oauth.yandex.ru/token` |
| Verified account information | `https://login.yandex.ru/info?format=json` |

This is Authorization Code OAuth with S256 PKCE, not OIDC: there is no Yandex
ID-token/JWKS/nonce verification path. The callback consumes a single-use,
browser-bound challenge with the selected provider and client binding. The BFF
exchanges the code privately, then calls the account API with the transient
access token in the `Authorization: OAuth` header. It accepts the JSON account
`id` only after `client_id` matches the configured application exactly. The
canonical identity is `(yandex, id)`; email, login and display name never select
the user. Access/refresh tokens are not persisted or sent to the browser.
See the official [code exchange](https://yandex.ru/dev/id/doc/ru/codes/code-url)
and [account API](https://yandex.ru/dev/id/doc/ru/user-information) contracts.

For cloud configuration, select Terraform `web_login_provider = "yandex"` and
set non-secret `yandex_login_client_id`. Load the secret with the existing
[Web Lockbox loading workflow](cloud-development.md), using the matching
`WEB_LOGIN_PROVIDER=yandex` in the loader environment. The historical Lockbox
key `oidc-client-secret` stores the selected login secret; Terraform maps that
key to `YANDEX_LOGIN_CLIENT_SECRET` for Yandex or `TELEGRAM_OIDC_CLIENT_SECRET`
for Telegram. The key name does not make Yandex an OIDC provider. Never put the
client secret in Terraform variables/state, command arguments, checked-in
files, container images or logs.

Successful provider verification alone grants no tenant access. Pilot access
requires a separate audited operator invitation or the existing
[cloud-development membership bootstrap](web-bff.md#audited-cloud-development-bootstrap)
for the canonical Sessionless user. A new identity without active membership
cannot create a Web session, read another user's sessions or inherit quota.
Equal Telegram/Yandex subject strings or emails are not account links. Explicit
late linking of two proved accounts is [post-MVP #172](https://gitcode.com/urandon/sessionless/issues/172),
not an MVP prerequisite.

For operator-assisted first access in `cloud-dev`, the existing
`make web-bootstrap` accepts optional `WEB_BOOTSTRAP_EXTERNAL_PROVIDER=yandex`
and `WEB_BOOTSTRAP_EXTERNAL_SUBJECT` with the operator-verified canonical numeric
Yandex account `id` (not its email or username). Supply the existing target
`WEB_BOOTSTRAP_USER_ID`, tenant, role, operator and reason as documented in the
bootstrap runbook. The typed confirmation becomes exactly
`BOOTSTRAP <user> INTO <tenant> FOR yandex:<id>`. Identity provisioning, membership
and its audit are one transaction. A subject owned by another user or a target
user with any different external identity is rejected; this is first-identity
provisioning, not a privileged shortcut for late linking. Without both optional
variables, the original bootstrap still requires an existing external identity.
Neither mode is available in production or accepts authority-bearing arguments.

`YANDEX_LOGIN_AUTHORIZATION_ENDPOINT`, `YANDEX_LOGIN_TOKEN_ENDPOINT` and
`YANDEX_LOGIN_INFO_ENDPOINT` overrides are accepted only for explicit loopback
fixtures when `SESSIONLESS_ENVIRONMENT=local`; they are not cloud proxy knobs.
Repository adapter/BFF/YDB/browser checks use synthetic accounts, not a real
provider login. Registered-client callback, Yandex endpoint reachability from
the deployed runtime and cloud browser smoke remain #34/#35 rollout evidence;
access to the Yandex Cloud console does not prove these endpoints reachable.

### Existing Telegram fixture

The Web BFF and Telegram-shaped OIDC fixture are separate Go processes. The
fixture generates an ephemeral RS256 key at process start and refuses to start
unless `SESSIONLESS_ENVIRONMENT=local`. Production and cloud-development
processes selecting Telegram use its real issuer and receive the client secret
from the process environment or Lockbox. `WEB_LOGIN_PROVIDER=telegram` (or an
empty selector) preserves the legacy `/auth/telegram/callback` registration.
These fixture values do not configure Yandex login.

Build the binaries and run the credential-free repository checks:

```sh
make build
make test
```

Secure browser cookies and exact-origin checks are never weakened for local
development. A manual browser flow therefore needs a local HTTPS reverse proxy
for `https://web.localhost`; the fixture endpoints may remain loopback HTTP and
are accepted only when the BFF itself runs in the local environment. See
[web-bff.md](web-bff.md) for the route contract, environment variables,
bootstrap procedure, and threat boundary.

The Web canonical API additionally needs the existing Object Storage and
scheduler-wake queue coordinates. Local static S3 credentials use Silo in the
Compose stand and MinIO in the native dockerless stand;
cloud deployments set `S3_IAM_METADATA_CREDENTIALS=true` so both exact-object
operations and short-lived Yandex Object Storage capabilities use the workload
service account. `SESSION_API_ID_HMAC_KEY` must be at least 32 bytes and stable
across replicas because it derives upload, event, run, and dispatch identities.
`WEB_MAX_UPLOAD_BYTES` configures a positive upload limit (default 32 MiB), and
`WEB_ALLOWED_MCP_SERVERS` is an optional comma-separated allowlist copied into
Web-created jobs. `WEB_OBJECT_STORAGE_ORIGIN` is the exact browser-facing
origin of every direct upload/download capability and is added to CSP
`connect-src`; wildcards, paths, credentials, query strings, and non-HTTPS
origins are rejected. Only exact loopback HTTP is accepted when
`SESSIONLESS_ENVIRONMENT=local` (for example `http://localhost:9000`).

The direct upload sequence is intent, exact presigned `PUT`, commit, and then
message submission. Reusing an idempotency key retries the same logical
operation. Poll point runs with the returned ETag and delay headers, and use
`after_sequence` to project newly appended events. Do not persist capability
URLs or include them in logs, test snapshots, or browser analytics.

Run the adapter contracts and stop the stack:

```sh
make migrate-local
make local-integration
make e2e-local
make dev-down
```

Normal stop/start preserves the YDB and Object Storage named volumes. ElasticMQ
and the Telegram fake are intentionally ephemeral transport fixtures. The
complete topology, endpoint table, local-only credentials, persistence test,
and Apple Silicon requirements are documented in
[local-development-stand.md](local-development-stand.md).
The `e2e-local` target starts the stand when needed, builds the isolated worker,
and executes the two-tenant product flow and recovery scenarios documented in
[local-e2e.md](local-e2e.md).

After the YDB monitoring endpoint is ready, apply or inspect the schema:

```sh
export YDB_CONNECTION_STRING='grpc://127.0.0.1:2136/local?go_query_mode=scripting&go_fake_tx=scripting&go_query_bind=declare,numeric'
export YDB_ANONYMOUS_CREDENTIALS=1
make migrate-local
make migration-status
make partition-status
make ydb-integration
```

The repository-owned migration binary embeds the SQL set and uses Goose as a
library. It adds a YDB-backed fenced lock, pre-execution checksums, one
idempotent DDL operation per file, and a forward-only production policy. See
`migrations/ydb/README.md` for crash repair and `docs/ydb-state-store.md` for
keys and transaction procedures.

`make partition-status` emits the live primary keys, partition settings, counts,
and contract drift as JSON. The bucketed ready/expiry expand/backfill/cutover
procedure is documented in
[ydb-partitioning.md](ydb-partitioning.md). `make partition-backfill` is a
deployment migration command, not a normal serving operation.

Local defaults use `YDB_ANONYMOUS_CREDENTIALS=1`. Cloud deployments use the
YDB environment credential chain and metadata credentials; do not place access
tokens in the connection string or command line.

To delete local Compose volumes, use the guarded command:

```sh
CONFIRM_LOCAL_RESET=sessionless-dev make dev-reset
```

The reset target affects only the fixed `sessionless-dev` Compose project. It
does not remove source directories or arbitrary Docker resources.

After a reviewed pre-production migration-baseline rebase, inspect and execute
the separately guarded cloud-dev application-data reset:

```sh
make cloud-app-reset-plan
CONFIRM_CLOUD_APP_RESET='reset-sessionless-cloud-dev:<folder-id>:<artifact-bucket>' \
  make cloud-app-reset
```

The command resolves its target from the selected Terraform state and preserves
cloud infrastructure and unrelated object prefixes. The complete prerequisites,
typed-confirmation derivation, and preservation boundary are documented in
[cloud-development.md](cloud-development.md). It is not a production migration
or an ordinary deployment step.

Single-session archive, legal hold, bounded dry-run, and exact-object deletion
are documented in [session-lifecycle.md](session-lifecycle.md). Use only the
Make targets in that runbook; the destructive command requires the digest of
the resolved inventory and has no prefix-delete mode.

## Images and CI

```sh
make images
make ci
```

`make ci` includes the deterministic WebUI checks and static build before Go
verification and embedding. Go package commands use explicit repository package
roots so they never descend into `web/node_modules`; a layout guard fails if a
new project Go root is added without joining that inventory.

The control plane uses a small distroless runtime. The worker has a separate
Dockerfile. Its deterministic harness validates lifecycle behavior now; a
later decision can add OpenCode, Codex, Claude, Hermes, or another CLI without
expanding the webhook/control-plane attack surface.

GitCode is the source of truth for branches and merge requests. Its push mirror
replicates every commit to `github.com/urandon/sessionless`, where GitHub Actions
runs `make ci` and `make images` for every mirrored branch or tag push. The
workflow is `.github/workflows/ci.yml`; a GitHub pull request is not required.

Ordinary branch and `main` CI never requests a GitHub OIDC token and never
contacts Yandex Container Registry. It still builds every runtime image twice
in independent clean rooms and uploads deterministic reproducibility evidence.
Publishing requires an explicit `Publish runtime images` workflow dispatch on
the exact current, converged GitCode/GitHub `main` SHA with a matching typed
confirmation. That separate job repeats the clean-room proof before requesting
OIDC and publishing the five immutable images. Publication creates a deployment
manifest; it does not deploy Terraform or a runtime revision. See
[cloud-development.md](cloud-development.md).

When reviewing a GitCode merge request, match the GitHub Actions run to the
GitCode head commit SHA. Automatic propagation of that status back into the
GitCode merge-request UI is a separate integration; until it exists, this SHA
check is the merge gate.

Publishing GitHub release artifacts back into GitCode is intentionally outside
this CI workflow and tracked separately in
[issue #15](https://gitcode.com/urandon/sessionless/issues/15). Branch CI does
not receive a GitCode publication token.

Tag-driven GitHub Releases use a separate protected workflow and a dedicated
Yandex identity. The tag formats, GitCode/GitHub provenance checks, environment
gate, five-image asset contract, and same-tag retry procedure are documented in
[releases.md](releases.md).

Cloud development environment procedures are documented in
[cloud-development.md](cloud-development.md). They use separate bootstrap and
environment state, a folder-scoped external budget gate, immutable image
digests with guarded commit-SHA tags, Lockbox payload injection outside
Terraform, and blue/green API Gateway promotion.
