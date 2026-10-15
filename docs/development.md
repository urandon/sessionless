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

### Entity registry and state storage

Maintain the [entity registry](architecture/entity-registry.md) when table,
primary-key, consumer or retention decisions change. It separates observed
physical schema from the logical durability decisions in
[state storage simplification](design/state-storage-simplification.md).
Run `make entity-registry-check` and `make entity-registry-test`; both also run
through `make docs-check`. These commands read source migrations and registry
metadata only. They neither connect to YDB nor authorize schema application.
Reports and unresolved reduction decisions belong in
[#205](https://gitcode.com/urandon/sessionless/issues/205), not migration comments
or the original task description.

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
real attached-provider activation remains the separately reviewed #133
workstream. It is required for the selected attached MVP path, independently of
managed implementation. #176/#180 own the agent-harness minimum/design;
#175 owns managed composition and #90/#92 its platform and rollout proof.

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

### Attempt-output upload checks (default off)

Run the credential-free Go data/signer/receipt checks with shared Go caches:

```sh
make attached-worker-output-upload-test
make attached-worker-output-control-client-test
```

The suite covers closed upload-plan phases, immutable scope/key/digests,
irreversible ordinal spending, noncanonical deletion inventory and a separate
worker-output conditional-object signer. The bounded upload-control wire and
unregistered HTTP adapter use the actual connection bearer binder and the
existing admission/claim/read-only-reconciliation methods. HTTP integration
tests use synthetic store results; they are not executed YDB transactions or
current Source/provider authorization. Reconciliation cannot refund a claim or
infer writer quiescence from expiry. The signer uses explicit server-owned
HTTPS storage configuration, exact metadata/checksum/precondition binding and
the original fixed cutoff; it does not reuse a browser upload grant. Fake
AWS/IAM signing responses test bounded parsing and fail-closed drift, not actual
platform signing, remote conditional writes or native persistence races.

This is an unfinished #200 implementation stream, not a separately accepted
helper or enabled media route. The server control returns custody metadata with
no capability; applied/replayed is not signing or PUT authority. The closed
wire reserves five RPC shapes. The separate control-client check covers a
per-instance finite schedule (one admit, two original ordinal claims, two
read-only reconciliations, with failed/lost slots spent), exact original head
and immutable plan comparison, and test-owned TLS loopback transport. The
private HTTP factory is shared with the existing Pi phase client: explicit TLS
and failure/header bounds, no proxy, redirect, compression or pooled/HTTP2
replay. Existing Pi client checks run alongside the new client checks. Both
recipes clear native/YDB opt-ins and use shared Go caches; neither launches the
local stand. These are not a whole HTTP/TLS/native allocation certificate.
The real private producer/exporter must own exactly one such client and fund
all its work on the SAME original ledger before Issue; constructing a client
or returning metadata is not provenance or permission. The real private exporter,
genuine current Source/profile authorization before signing, complete
original-ledger/control/deadline envelope,
single-send transport/alias retirement, canonical receipt/ACK continuation and
normal #201 download path remain necessary. Unknown writes cannot be settled by
expiry/readback, a config flag or a caller-populated Boolean. No local YDB,
SDK/model/provider, credentials or cloud operation is enabled by this command.

### Offline attached transport experiment preparation

The [#129 Phase-0 plan](attached-worker-transport-experiment.md) prepares
three 24-hour observation cohorts and their proposed limits. Run
`make attached-worker-experiment-plan` to print schedule bounds from the
canonical poller, and `make attached-worker-experiment-test` for the offline
validator and existing cadence regressions. The planner neither contacts a
control plane nor starts a worker; its result is always `draft_not_authorized`.
It is a development command, excluded from deployed component/release assets.
No measured cost or approved execution manifest is supplied by this target.

### Private Pi bootstrap helper checks (default off)

`make pi-sdk-bootstrap-test` verifies the repo-owned embedded construction
helpers for [#203](https://gitcode.com/urandon/sessionless/issues/203). The Node
unit suite imports only the helper and built-in modules: no third-party install,
SDK import, credentials, native addons, worker/child processes or network.
Constructor spies verify argument contracts, not genuine SDK conformance.
The Go race test checks embedded bytes, digest and independent caller copies.
Use `make pi-sdk-bootstrap-test PI_SDK_BOOTSTRAP_TEST_COUNT=20` for its bounded
race/shuffle repetition, with the same repository-shared Go caches.
The target also runs in `make ci`, using its normal pinned development Node.
Development Node is not automatically the eligible Pi serving runtime.
The process runs `node:test` directly, without test-discovery child processes.
Its Node permission flags restrict filesystem/child/worker/addon access; they
are not a production isolation proof or a Node 24 network boundary. This unit
suite imports and calls no networking API. See the exact
[Node 24.19 permission limitations](https://nodejs.org/download/release/v24.19.0/docs/api/permissions.html).

These helpers use a literal read-only CredentialStore, in-memory factories,
fixed retry/image/settings choices and an empty ResourceLoader. They expose
construction options, not a prompt/CLI/RPC API or enabled adapter. They do not
import Pi or erase its eager clipboard/Jiti/Undici initialization. The original
supervisor must still qualify the real SDK graph and closed `PI_OFFLINE=1`
environment, guard selected model/auth and actual physical send, reserve full
canonical/media allocations, and enforce every accepted
[Pi profile gate](design/pi-sdk-attached-media-profile.md). Missing-provider
credential reads return `undefined` per upstream; ambient fallback must be
denied by the real driver, not inferred from these helper tests. Disposal drops
the helper's retained key reference; JavaScript string zeroization is not claimed.

### Private Pi SSE framing checks (default off)

`make pi-sdk-transport-test` tests the embedded byte-framing predecessor of
the Pi physical transport, using the same pinned development Node and shared
Go caches. It also runs in `make ci`. Use
`make pi-sdk-transport-test PI_SDK_TRANSPORT_TEST_COUNT=20` for bounded Go
race/shuffle repetition. The Node unit suite imports only the repo-owned
framer, response plumbing, closed frame policy and built-in assertions; it performs no SDK imports
or network calls.

The fixed-arena framer emits complete original frames with the exact SDK
LF/LF, CR/CR and CRLF/CRLF delimiters, independently of fragment boundaries.
It enforces the existing canonical native profile's body/chunk/line/event,
event-count and explicit input/output copy budgets. Incomplete EOF, overflow,
reentry or early consumer return closes the framer; delivered byte copies are
independent of later arena reuse. This removes repeated fragment-prefix
concatenation before the genuine SDK parser, without replacing that parser.

This primitive does **not** validate UTF-8/JSON, reject tool/effect frames,
enforce an HTTP request or cancel a response reader. Its trusted owner must
validate each complete frame before delivery and preserve bounded downstream
backpressure. The explicit copy budget covers only arena writes and emitted
copies, not the complete SDK/V8/Pi/Undici allocation ledger; arena clearing
also performs bounded work. Unit fixture values are not serving limits.
Full allocation admission before conversion/Issue, exact postauth request,
irreversible one-send, abortable whole-response/effect guard, genuine SDK/Pi
composition, media/lifecycle/output and actual platform gates remain required
by the [Pi profile design](design/pi-sdk-attached-media-profile.md).
No adapter registration or activation is enabled by these tests or assets.

The same target checks a private guarded-response module using genuine
in-memory WHATWG `Response`/`ReadableStream`, not an HTTP/provider fixture.
It bounds normalized headers, accepts status200/SSE/identity encoding only,
delivers one original validated frame per pull with no read-ahead, and stops
the owned reader and private fetch controller on denial, overflow, terminal,
cancellation or incomplete EOF. Cancellation is never held by an upstream
cancel promise. Invalid entry configuration has typed errors and cleans up
available genuine owned response/controller capabilities without stealing an
externally locked reader.

The caller must prearm the original parent signal from its original monotonic
deadline and bind the supplied private controller to the same physical fetch;
the SDK provides only its borrowed signal, not a mutable controller. Unit
abort observations prove propagation, not that integration or deadline origin.
The response asset requires a fixed synchronous callback; it does not itself
choose an effect policy or establish the callback's authority. Responses uses
[typed semantic events](https://developers.openai.com/api/docs/guides/streaming-responses);
the exact pinned SDK/Pi source remains authoritative for the separate policy.
Preallocated HTTP headers/chunks, decompression and the complete native
allocation ledger remain wider qualification requirements. No real SDK
execution, request/send authorization or profile activation is established.

The private `createResponsesFramePolicy` asset checks every semantic message
inside each original frame before forwarding any bytes. It uses fatal UTF-8,
SDK-compatible line/field semantics and complete JSON grammar with decoded-key
duplicate, depth and cumulative node checks before object parsing. Its closed
19-event subset admits message/refusal/reasoning forms, not tool/effect calls
or nested tool items in terminal responses. It preserves original bytes for the
genuine SDK/Pi parsers instead of replacing them. Line-leading BOMs, nonempty
annotations/logprobs and compound terminal-plus-DONE messages are deliberately
outside this subset. A DONE marker or EOF cannot manufacture completion;
incomplete is a terminal condition, not successful final-output evidence.

The parent supplies deeply frozen, bounded admitted request echoes and the
existing canonical native limits, without defaults or alternative wire names.
Known neutral omitted/null echoes, optional matched `store:false` and bounded
default obfuscation on four safe delta families are explicit compatibility
cases, not permissive unknown-key handling or identity/spending authority.
An explicitly selected `service_tier` must echo the same literal value in this
lane. Resolving an `auto` request to an actual serving tier is not qualified by
this equality check; it needs a separately bound mapping and spending policy.
Tests include mixed semantic delimiters, pending CR across original frames,
ordinary schema positives, nested effect denials and real in-memory response
host cleanup. Their source-derived decoder oracle is not a genuine SDK run.

The policy shares explicit component copy credit after reserving the framer's
bounded writes/copies and arena clear. This does not certify complete parsed
object/SDK/Pi/native allocation, immediate garbage collection or process
residency. Parent pre-Issue reservation/model fit, actual postauth one-send,
same-fetch/original-deadline provenance, genuine SDK composition, strict
same-run final/custody and platform/profile activation remain required. Stage
the bound framing, response and policy assets together; embed digest tests also
check each accessor against its exact expected path and independent copies.

The same target exercises the private `createPhysicalFetchOwner` using complete
synthetic admitted requests and in-memory transport capabilities. Its only
options are `limits`, `admittedRequest`, `originalSignal`, `parentUse` and
`physicalFetch`. It captures the exact serialized body, endpoint, method and
normalized headers before awaiting authorization, spends its one-shot latch on
first entry, and keeps one physical controller linked throughout the response.
The owner constructs the fixed frame policy internally; it does not accept an
external validator with another independent full budget.

Known logical request, header, echo, JSON and response copies share checked
allocation, copy-work and node reservations. Downstream framing, response and
policy modules receive the same reduced canonical limits; the original limits
remain immutable. These counters do not bound native objects, V8/GC, SDK or
network allocations, and cannot retroactively reserve conversions before Issue.
Fixtures neither choose serving numbers nor authorize a smaller original input.

The isolated `parentUse.authorizePhysicalSend()` capability takes no arguments;
its synthetic test implementation is not a current source authorization or a
production binding. Actual parent comparison, phase transport, original cutoff,
complete pre-Issue model-fit/allocation proof and credential lifecycle remain
mandatory before genuine SDK composition or registration. No SDK/model/HTTP
call, profile activation or full adapter completion is proved by this target.
Stage the bound framing, response, policy and request assets together.

### Isolated Pi phase-wire checks (non-serving)

`internal/pisdkphasewire` contains a disconnected Go codec for the complete
canonical phase request, seven-key applied/one-key denied reply, original
31-field attempt head and exact seconds/nanoseconds time representation. Its
ordered byte grammar rejects unknown, reordered, escaped and noncanonical
fields before bounded owned copies. Encoding premeasures one exact allocation.
The private process-frame codec accepts only canonical SPU2, replacing its
non-serving SPU1 predecessor without aliases. Header inspection validates all
17 bytes, explicit per-kind/body/frame bounds, platform-safe totals and fixed
sender direction before payload allocation. A future stream owner must reject
the wrong peer at this preflight. Complete UTF-8 Init, physical-request and
final bodies remain byte-exact; use metadata is binary status plus uint64
relative nanoseconds, not a Source reply or serialized Go clock. Complete-frame
decoding is not an actual duplex stream pump or an Init/final schema validator.

Run the focused package with the pinned Go toolchain and repository-shared
caches, using `go test -race -count=1 -shuffle=on ./internal/pisdkphasewire`.
These ordinary unit tests are also included by `make test`. Tests distinguish
conservative syntax ceilings from constructor-backed structural fixtures.

The process-frame helper reports only checked successful wire totals, including
one retire: parent stdin I+60, child stdout B+L+163, total I+B+L+223 for positive
actual Init/request/final byte lengths. These are not the predecessor's logical
copy/node totals, reservations, native/GC bounds or serving limits. The complete
original producer/copy/node ledger must still be established before Issue.
Explicit structural caps, a decoded applied reply or exact byte comparison grant nothing. There is
no source/HTTP/IPC pump, runner, credential or provider registration. Authentic
same-RPC/current-owner phase production, source-owned full pre-Issue projection,
shared/native fit, original deadline/cutoff bridge and cross-language parity
remain required by the [Pi profile](design/pi-sdk-attached-media-profile.md).

### Prepared Pi duplex checks (default-off)

The explicit `Supervisor.PreparePiStdioV1` / `RunPreparedPi` library arm uses
the same validated Supervisor process lifecycle and SPU2, not a second runner.
Its Go arenas are reserved on the original ledger before the original API
Issue boundary; a different ledger, late plan, substituted model-use context
or executable/profile mismatch denies before launch. The original complete
Init must still come from a qualified producer; an opaque UTF-8 fixture does
not implement that producer or certify full native fit.

One owned pump serializes Init/use and assembles child request/final frames
after expected-peer/order/header preflight. It retains the original Go F,
subtracts elapsed work before the one relative reply, and cancels/force-stops
the same supervised attempt rather than granting model time during TERM grace.
All pump/Source/timer work joins before return. Source-call cancellation is
separate from the original immutable-Finalize/retirement context. Actual stdout
count/digest and separately extracted final DATA do not mint a #200 producer,
READY receipt or credential-finalization result.

The focused ordinary process checks are
`go test -race -count=1 -shuffle=on -run '^TestPiStdio' ./internal/attachedworkerdaemon`.
They use synthetic Source/profile/Init and a local test executable, including
fragmentation, exact maximum request, first-over, malformed/order failures,
held Source/init-write cancellation and the original API file lifecycle.
They are not Pi SDK/native clock/egress proof. Real Init/schema/all-copy ledger,
same-JS guarded fetch/clock conversion, private final custody and genuine
Source/admission/READY/terminal remain required before serving registration.

The private Node child half in `internal/pisdktransport/stdio.mjs` speaks the
same SPU2 over owned standard streams. `make pi-sdk-transport-test` includes
credential-free child/actual-fetch-guard controls: full original body/P, exact
BigInt nanoseconds, full IPC elapsed subtraction, delayed actual-use assertion,
one-shot failure, Retire/EOF, fragmented/backpressured streams and cancellation.
Its fixed arena and bounded wire copies are not a full native allocation or
clock-fit certificate. Disposal joins actual pending writes before clearing
borrowed payloads; a nonconforming injected writer is not made quiescent by
detaching a timeout. Successful Final is still DATA, not a private producer.

The ordinary `TestPiStdioNodeSameSupervisor` also runs the actual pinned Node
24.19.0 process through the same Supervisor and canonical Go codec, with an
empty replacement environment and exact module-only Node filesystem permission.
This uses a synthetic Source/profile/opaque Init and no SDK/model/key/network;
it is not real platform isolation, closed Init/bootstrap, full native fit or
provider eligibility. The full Init producer and genuine Pi bootstrap linkage
remain unfinished, rather than being inferred from the transport tests.

### Complete Pi request-data producer checks (non-serving)

`internal/pisdkmedia` projects the original canonical snapshot plus tail through
the existing Codex media association kernel into quoted typed Pi task data and
one independently expected complete Responses body. Historical text, text/JSON
files (including their original number lexemes), assistant summary and original
PNG/JPEG bytes remain in order; image blocks follow their exact source label.
This is not a replacement Pi agent, SDK, parser, request client or native session.

One explicit logical ledger funds bounded source prerequisites before validation,
Clone and kernel work (stage A), then measures those funded data (stage B), then
atomically reserves the combined remaining typed output, one expected emission,
declared SDK/string/header/phase copies and response/echo demands (stage C).
Early missing prerequisites or failed stage-A reservation precede Clone. Late
aggregate overflow may have lawfully funded Clone/kernel work but returns no
partial plan and does no further unfunded emission or Issue. Cumulative copy and
node charges never reset/refund; logical live reservation release does not prove
native object reclamation. The original 96MiB Go validator ceiling is reported
separately, not debited as a child-P memory certificate. Response A/D/N values
reuse the accepted physical owner's equations and the joint remaining nodes,
not fresh independent full-P pools. All fixture limits are synthetic DATA.
The caller context is required and retained unchanged, checked before funded
work, at parser nodes and original block visits; no deadline or grace is created.
This does not interrupt the existing synchronous canonical codec or establish
the separately missing hard Go-F/JavaScript physical-controller bridge.

The closed ordered Go emitter preserves ECMAScript property ordering (including
array-index keys), escaping, safe integer values and original file contents as
strings. Unsupported fractional/exponent schema numbers deny rather than claim
unqualified binary64 parity; such numbers inside quoted files remain unchanged.
The SDK-free `request_contract.mjs` checks the actual complete own payload
against the immutable expected projection, permits only source undefined cache
omissions, freezes the checked source-owned tree and adds only fixed truncation
and strict-schema fields. Its one-shot result is DATA, never authorization.

Run `go test -race -count=1 -shuffle=on ./internal/pisdkmedia ./internal/codexexec`
with the pinned Go and shared caches. `make pi-sdk-body-producer-test` is the
separate pinned-Node pure unit CI gate. It imports only the new data contract;
it must not glob or run the adjacent bootstrap/boundary/SDK tests. Tests include
the 3000x1000 plus small-image cohort, repeated names, complete snapshot/tail
equivalence, staged accounting, byte/node/depth first-over cases and builtin
`JSON.stringify` differential goldens. No credential, SDK import/constructor,
provider call, native YDB process or network is needed for these checks.

`PrepareInitV1` extends that **same** ledger before Issue. Its closed Init
contains the complete original typed messages/timestamps/images, the one emitted
expected body, canonical P and its domain-separated digest, explicit selected
model/catalog DATA (including fractional costs/tiers), schema, options and eleven
nonsecret headers. Caller config pointers/slices are not retained. The exact
immutable filesystem gives the future credential locator's byte bound without
creating a key/file; its worst JSON escaping and the final arena are reserved
before Issue. The canonical Init API consumes a concrete
`providercredential.PreparedImmutableAPIFileV1`, prepared by the SAME
`InvocationService` from its original Issue request and namespace, rather than
requiring an inaccessible private file object or caller fingerprint.
Only the exact handle/materialization inside its active synchronous file-only
callback may fill the once-spent slot; canonical invocation/resource scope is
compared before Init preparation. No direct-file legacy entry point remains.
Finalize/Release remain with the original parent handle.

The service publishes a private original-file record only during that callback.
Its single-verifier gate avoids reentering the already-held per-handle mutex,
rejects concurrent/reentrant verification rather than queuing, revokes on every
callback return/error/panic and joins an existing verifier before unlocking.
The authentic immutable descriptor/namespace/fingerprint verifier still runs;
no planner/binding/secret backend is queried to resolve this bridge. This local
record grants no Source/currentness or spending permission. An explicit trusted
composition may supply its existing `PinnedSecretFileSystem` in InvocationConfig
instead of ScratchRoot (never both), and closes it only after all consumers and
original handles have joined/released. Local tests exercise real
Issue/Materialize/Finalize/Release with a public literal and isolated fake stores;
they do not borrow a real configured account or subscription.

The private SDK-free `native/init.mjs` reuses the existing strict JSON grammar,
rejects duplicate decoded keys/unknown fields/invalid UTF8/types/limits, checks
complete body/message/image/schema consistency and freezes its independently
parsed DATA. Outer Init, nested P and expected-body parsing share one prospectively
reserved copy/node partition, not new full-P pools. Logical payload bounds are
not V8/native-object, SDK, timer/platform or provider eligibility measurements.
`TestCompleteInitPinnedNodeOriginalCohort` drives that actual decoder with both
original history/snapshot-tail cohorts and malformed controls, under the same
pinned Node and replacement empty environment. It does not import any SDK,
read a real credential or run adjacent opt-in functional/native tests. The final
delayed-bootstrap/sole-runner and actual Source/native fit integration still
must consume these mechanics; this source boundary is not an activated adapter.

The private `native/entry.mjs` consumes complete Init over SPU2 before reading
the one file-only immutable API delivery or importing the fixed genuine Pi SDK
graph. Ten closed decimal arguments are prepared and payload-funded before
Issue from the same original ledger; no JSON config graph, Source selector,
credential, refreshed deadline or caller-selected module is passed in argv.
The bounded reader requires a canonical regular single-link owner-only file,
checks identity before/after bounded reads, rejects non-visible-ASCII bytes and
clears its raw buffer on exit. These checks do not replace native read-only
isolation, immutable parent verification, heap bounds or provider-only egress.
Fake-transport harness assets are test-only, not embedded runner assets.

`go test -race -count=3 -shuffle=on -run '^TestPreparedEntrySDKFreeControls$'
./internal/pisdkmedia` tests SDK-free entry import, malformed Init before any
file/SDK/HTTP effect, closed arguments and file-reader rejection controls in an
isolated namespace with no SDK/account tree. Ordinary package tests also check
prospective argv/reader payload accounting and ownership. With the existing
exact local Pi build and pinned Node prerequisites only, the explicit command
`go test -tags=pi_sdk_functional -race -count=1 -run
'^TestPreparedPiNativeStdioEntry$' -timeout=110s ./internal/pisdkmedia` exercises
real lifecycle file delivery, complete original history/snapshot-tail/two-image
Init, SPU2, genuine Pi/standard Responses SDK/parser and exact structured final.
Only a public noncredential literal, synthetic parent allowance and fake HTTP
sink are used; it performs no account discovery, model call, install or rebuild.
The strict parsed final is DATA, not the private same-run custody carrier owned
by #200. Direct-child testing and file checks do not certify Source/native fit
or the full production composition.

`PreparePiExecutionV1` implements the existing `PreparedProcessRunner` seam,
not another invocation/credential owner. The original complete producer prepares
the request/Init, exact expected request metadata and process argv, then this
component's original-ledger stdio plan and independent process snapshot before
Issue. During the existing synchronous Materialize callback, it verifies the
same head/profile/body/model/schema/headers/ledger/argv/environment, fills only
the concrete original Init locator, and invokes the same `Supervisor.RunPreparedPi`
with the original private physical-send capability and owner context. A public
`Authorize` implementation is not an alternative capability. All failures spend
the executor; Close cannot clear active process/collection buffers or rearm it.

The original InvocationRunner still owns callback unlock, immutable-finalize
phase, actual Finalize and independent Release. Its independently owned
parent/child data copies may be cleared at return; the original full producer
must retain the separate borrowed request/Init and this executor's observation
through `FinalData` and `Close`, then clear its originals. FinalData is one-shot
strict parsed DATA gated by this actual observation and successful result DATA,
not private finalization/custody evidence manufactured from public flags.

The same API `InvocationRunner` separately owns a private pending-result slot;
`FinalData` is not its authority. A genuine supervisor observation must belong
to that exact original send, plan, head, profile, schema and accumulated ledger.
Private result storage is funded before Issue. Capturing raw data before borrowed
buffers clear is not a completed producer: immutable result eligibility follows
actual process/pipe joins, callback unlock, fresh immutable Finalize, unconditional
Release and successful `RetireOriginalAPIStaging` on the original retirement
context. The trusted parent must retire its actual owned resources; an arbitrary
nil-error callback is not production provenance. Void `Custody.Clear` only
relinquishes input buffers and cannot prove staging cleanup.
Returned preparation resources also need retirement when admission validation
rejects them, including an original deadline expiring after preparation. This
does not authorize clearing caller-aliased buffers or restarting cleanup grace.

Receipt retries use the original owner's retained immutable result and never
rerun the provider. Foreign identity, replaced public result flags and incomplete
cleanup cannot create a candidate. Declared text/JSON artifact contents remain
private; their success receipt is denied until the genuine bounded pre-Put
capability, upload and custody implementation exists. They are not silently
omitted or converted into fabricated object references. Local owner retirement
tests are not the authenticated journal/TerminalAck join required by #201.
The genuine full parent and ordinary Stack/preflight Pi selection remain required;
this private custody work does not enable a serving profile or complete #200.

With the existing exact local SDK/Node prerequisites only,
`go test -tags=pi_sdk_functional -race -count=1 -run
'^TestPreparedPiSameOriginalSupervisor$' -timeout=150s ./internal/pisdkmedia`
exercises the NEW connection through the actual original InvocationRunner,
concrete executor, Supervisor, credential service and genuine Pi/standard SDK.
History/snapshot-tail, postvalidation model/schema/header/head/ledger/file/argv/
environment substitution, forged public send, callback cancellation, Source
denial, Close during active send, post-run raw-final mutation and failed Release
are distinct controls. Staging retirement failure and private result identity,
retry, no-second-Run and local retirement checks cover the same real credential
service composition; a synthetic parent still does not prove Source/native fit.
Source transactions, OS isolation and HTTP remain explicit synthetic test
collaborators; a passing test does not establish a serving profile or egress.
Ordinary `TestPreparedPiExecutionBeforeIssueOwnershipAndDenials` launches nothing
and checks pre-Issue association, independent metadata, exact live cleanup and
combined first-over denial on the original ledger.

Missing genuine model/compat/header/source-owned profile, tokenizer/output-clamp
and full native-fit evidence, currentness, original S/hard cutoff, credential
cleanup and genuine standard SDK composition remain unavailable. Ordinary
registration remains disabled. The full [Pi profile](design/pi-sdk-attached-media-profile.md)
and both MVP tracks remain required; passing these units is not their completion.

### Explicit Pi phase-session client checks (default off)

`NewPiPhaseSessionSourceFactory` composes the existing sealed-input and receipt
channels with fixed `/attached-worker/v1/pi-api-use` metadata on the same HTTPS
origin and accepted Session/bearer. Existing constructors and normal activation
are unchanged. The new client requires explicit positive HTTP timeout, header
and TLS bounds; it copies trust and uses a private H1-only nonpooled transport,
with redirects, decompression and request-body replay disabled. Each phase call
performs one `Do`, uses the closed phase codec and checks the complete static
tuple/binding/policy. Session retirement cancels pending work and clears its
owned bearer before a blocked exchange delegate can delay cleanup.

Run `go test -race -count=1 -shuffle=on ./internal/attachedworkersealedinput`
with pinned Go and shared caches. The new cases use test-owned loopback TLS and
channel barriers, not a live service/provider. Client timeout is an existing
failure bound, not a new phase permission window; original caller context is
retained without computing W/L/S/F. Header/body arenas and static equality are
not complete native/shared-ledger admission or authenticated current-source
proof. No serving registration is enabled. The Store source and current-policy
transaction checks described below do not themselves supply the original
private parent or a qualified native catalog. Original
phase/deadline/credential/SDK/custody integration and
the full [Pi profile gates](design/pi-sdk-attached-media-profile.md) remain open.

### Pi owner policy and original transaction checks (default off)

The optional authenticated Web BFF policy service records exact owner-confirmed
terms as an immutable decision plus a monotone current mapping. Revocation and
replacement never upgrade an already sealed job to a new mapping. An absent
qualified native/tariff catalog denies new authorization; API credential
ingestion or internally matching hashes do not replace owner consent.

Original attached dispatch admission checks the current policy and canonical
Session/member/participant/deletion inside its transaction, before even a cold
scheduler-slot write. Direct offer checks precede lease allocation. The original
phase source additionally binds to the original execution connection, not a
successor reconnect. New upload admission/ordinal claims, canonical-copy start
or resume, READY and semantic success recheck their stored original job and
current policy before new effects. No additional quota or publication ledger
is created. Failure/cancellation cleanup and exact already committed results
retain their existing predicates: read-only upload reconciliation and historical
READY/ACK replay are not new model or data-publication permission.

The ordinary Go suite includes actual Store callers with a synthetic SQL driver
and catalog, first-write sentinels, a conformance-machine pending terminal,
revocation/replacement controls, READY commit-loss and restored originals.
These tests establish caller ordering and fail-closed results, not native YDB
isolation, installed native/tariff qualification, a real model turn or either
MVP route's activation. Use `make test` with repository-shared Go caches;
native integration remains separately gated.

### Original Pi input acquisition (default off)

`NewPiBoundMaterializer` explicitly selects the canonical API-input arm using
an already selected immutable local profile. The ordinary materializer remains
API-denying. The original Adapter creates one private accounting owner before
claim/request input copies; the authenticated Session source carries it into
the existing HTTP load. Limits never come from the response.

The HTTP arm reserves its fixed MIN(profile canonical limit, inherited 1 MiB,
transport cap) arena plus overflow sentinel before reading. A closed preflight
rejects unknown, duplicate, case-aliased or escaped field names, malformed
base64/UTF-8/integers, excess nesting and collections before typed decoding.
Decoded strings, collections, copy work and the complete re-encoded seal use
the same accumulated ledger. These are logical payload/work bounds, not a
certificate for Go, SDK or native heap fit.

Canonical data moves through materializer and adapter without synthetic stdin
or another Verified-input clone. The existing API runner receives the original
owner separately from its independent input partitions; a different caller
context, entry anchor or ledger is rejected before credential Issue. Temporary
HTTP/materialization contexts do not replace the original invocation lifetime.
Source reading and materialization are each single-use, including failures.

The original process keeps the exact manifest-pinned executable, argv prefix,
environment and read roots, with empty stdin. The API parent appends exactly
the ten already-funded values from the same Init plan to independent owned
parent/child argv; no optional full-argv or suffix-only matching is accepted.
The runner checks the complete prefix and all other process fields before
Issue, together with canonical positive decimal values and their inherited
profile limits. The child differs only by the reserved credential read-root
slot. Matching this DATA is not evidence of genuine Init or native fit; the
fixed trusted composition must supply those independently.

Input cleanup waits for the actual borrower or late materializer to return.
The active request's digest buffers remain charged through ambiguous terminal
acknowledgement and retire only with the original exact ACK and active slot.
This does not fund all future receipt/upload/TLS representations or supply the
genuine native-qualified private parent, SDK transport or exporter. The full
profile, source-currentness, output and activation gates remain required;
neither constructor nor a synthetic local test enables a worker profile.

### Connected Pi preparation and retirement (default off)

`attachedworkersealedinput.PiPreparedOwnerV1` composes the original acquired
input with the real Go request projection, independent parent/child DATA
partitions, credential-file plan, Init plan and prepared supervisor stdio.
The acquisition captures the concrete authenticated Session reader at its
first read. Another Session factory with identical origin or binding cannot
substitute for that reader, and the moved input has one preparation slot.
This association proves local custody, not fresh source or provider authority.

All invocation-owned input graphs, request buffers and process arrays use the
same original ledger. Collection counts and a traversal reservation precede
clone validation; byte/copy/node and retained-buffer reservations precede
their allocations. Parent and child retain the complete canonical history,
artifacts and original images in independent copies. The shared runtime-owned
credential filesystem is borrowed, not recreated or closed per invocation.
Preparing its immutable file plan does not read a key or issue credentials.

The composition deliberately returns an owned partial record together with
`pisdkmedia.ErrMissingProducer`, even after every Go preparation stage succeeds.
There is no installed native/model/complete-context fit, egress or conservative
safety-slack producer yet. No structural profile, synthetic budget, successful
preparation or future Applied phase reply may replace that qualification.
Its phase observer and process-run arm therefore also deny. The ordinary
stack does not select this composition; it is not a runnable Pi profile.

The original API invocation runner retires complete and partial preparations
on rejection, before phase authorization, Issue or process launch. Retirement
uses one bounded cleanup context, joins prepared execution before clearing its
buffers, removes transferred aliases, and retires only this preparation's live
debit. It preserves caller input, source/control reservations and any later
runner/output debit; accumulated copy work and node usage are never refunded.
Credential-free Go tests exercise the real acquisition/preparation/retirement
composition with a test-owned TLS source and a launcher that must never run.
They do not prove native SDK execution, full allocation fit, a provider turn,
output export/receipt/ACK, activation or the complete MVP lifecycle.

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
is intentionally not activated in worker runtime until the selected profile's
credential/isolation/rollout evidence passes. Telegram rollout #18 is not a
managed-cloud MVP prerequisite.

The feature-disabled serverless authority, local isolation supervisor, and
attested provider-egress/credential composition boundaries are documented in
[serverless-harness.md](serverless-harness.md),
[serverless-isolation.md](serverless-isolation.md),
[serverless-egress.md](serverless-egress.md), and the PR-03d
[Yandex substrate evidence plan](yandex-serverless-substrate.md). None registers
a concrete cloud launcher, provider proxy, secret backend, or production route.
The [current MVP plan](mvp-delivery-plan.md) requires this managed cloud path
without user-maintained compute alongside the required attached route. #176
research and #180 accepted design precede linked implementation of the bounded
loop, compaction, useful constrained MCP and web search; #175 joins that harness
into normal Web/control/worker flow, including admitted file/image processing.
#90/#92 remain managed evidence gates; #129/#133 independently gate attached
rollout. This scope correction does not enable runtime, authorize live calls or
import ambient credentials.

For a Dockerless, credential-free check of the prepared authority-to-egress
seam, run:

```sh
make serverless-egress-conformance
```

This runs vet, uncached race/shuffle package suites and 50 repeated joined
cases using the real registry, invocation capability and PR-03c boundary with
test-only credential/proxy ports. It does not contact an external provider,
enable a runtime profile or prove normal worker credential custody, canonical
terminal commit, cloud isolation or billed cost. See the
[substrate evidence boundary](yandex-serverless-substrate.md) for what remains.
The same target is included in `make ci`.

The feature-disabled native [direct OpenRouter reference backend](direct-openrouter.md)
pins one non-streaming Chat Completions request and strict observed-route
response contract. Its tests use only a local fake boundary; no production HTTP
boundary, key lookup, DNS request, or provider call is enabled.

The [native provider composition](provider-composition.md) accepts five explicit
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

Cloud preflight is Dockerless: it checks the cloud toolchain, billing gate and
Terraform configuration, not a local container engine. `make terraform-ci`
includes a credential-free regression with Docker absent from its command path.
It also checks Terraform's actual mocked, targeted Web plan: Web readiness must
wait for its registry, YDB, Object Storage, Lockbox, KMS and logging permissions,
without pulling publication/GC identity activation or unrelated runtime modules
into that graph. This is a dependency boundary, not permission to skip the
reviewed full deployment plan or to use targeting for ordinary deployments.

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

The selected-provider cloud smoke has a credential-free local entry point:

```sh
make cloud-web-smoke-test
```

It runs Node.js standard-library fixtures with fake curl/yc clients and checks
positive redirects, fail-closed configuration/response handling, redaction and
temporary-file cleanup. No Docker, YDB, cloud token or provider login is needed.
Repository CI runs the same target under the pinned Node.js version. The live
`make cloud-web-smoke` command requires separately approved cloud rollout
authority and explicit provider/public client configuration; see the
[cloud runbook](cloud-development.md#9-verify-the-yandex-foundation). A passing
login-start check does not prove an actual account callback or logged-in session.

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

Run explanation storage (#187) has a separate true-transaction proof target.
The read-only conversation explanation drawer (#141) is absent in default WebUI
builds. `VITE_RUN_EXPLANATION_ENABLED=true make web-stage` opts the frontend build
in only after the deployment's writer/schema cutover receipt has been checked
and the BFF's separately guarded explanation route has been enabled. The build
flag is not evidence of deployment readiness, does not enable the BFF, and does
not authorize migration or cloud activation. Rollback rebuilds without the flag
and disables the route; canonical conversations remain usable. Playwright opts
in against its intercepted canonical API fixtures, not a live deployment.

The drawer is a bounded nonmodal in-flow panel, not a new primary page. Opening
focuses its heading once; Escape from inside or Close returns to the trigger,
enabled composer, or conversation heading. Its close/refresh header stays
reachable while the secondary evidence scrolls. Refresh never resets input or
focus. Explain uses an authorized event's optional `run_id` or the exact loaded
Run's `trigger_event_id`; generic notices and adjacent events are never guessed.

| Display or control | Authorized source and meaning |
| --- | --- |
| Canonical Run, timestamps | `RunExplanationV1.run_id/status/created_at/updated_at`; only canonical status denotes completion |
| Admission | `admission` closed reason/outcome and observation time; last recorded decision, not current entitlement |
| Terminal reason | `terminal` closed supported reason; unknown is not diagnosed from raw notice payloads |
| Selected Attempt | `attempt` exact selector/number/state/update time; not a process heartbeat or history |
| Attached receipt/freshness | `attached` closed fact and server assessment at `read_at`; not physical liveness or canonical completion |
| Partial/unknown coverage | `coverage` per source; never inferred as success, readiness, or complete history |
| Explain/Refresh | Existing bounded explanation GET only; no compute refresh, repair, capability, provider or command calls |
| Close | Local disposal/focus only, not cancellation of execution |

State is memory-only and fenced by actual `/me` user/provider, exactly one active
tenant, Session, Run and request generation. Identity is read during ordinary
view initialization/visibility revalidation, not drawer open/refresh (the
existing identity route can renew session idle time). There is no new identity
poller or invented browser-session epoch. Hidden/unmounted/closed views discard
pending explanation responses, including transports that ignore abort. An
observed scope change clears old private drafts/history; actual canonical read
authorization loss clears private view state. Opaque explanation denial removes
only explanation authority, leaving separately authorized transcript access.
Compute-write denial does not revoke participant reads. Recoverable transport
failures retain drafts; retained evidence is explicitly historical. The shared
controller enforces single-flight and a five-second identity/tenant floor across
close/reopen/Run selection, with server backoff and no explanation polling.
Existing send/upload/archive/compute/download operations capture their original
view generation and AbortSignal too: after observed authorization change or
unmount, every asynchronous continuation checks scope before the next request,
state update, capability use or focus change. Old uploads never adopt a new
scope's signal. Already submitted operations may have committed; disposal is
not rollback, and no automatic resubmission or undo is attempted.

Against an explicitly approved disposable YDB only, use:

```sh
YDB_CONNECTION_STRING='grpc://127.0.0.1:2136/local?go_query_mode=data&go_query_bind=declare,numeric' \
YDB_ANONYMOUS_CREDENTIALS=1 make run-explanation-ydb-gate
```

The target runs only `TestRunExplanationYDB` fixtures with race detection,
three uncached shuffled repeats and a five-minute timeout. It rejects any
fake-transaction option or non-data mode; the legacy scripting suite skips
these serialized proofs instead of claiming fake commits prove atomicity.
Fixtures apply embedded test schema only in that disposable database, use
random test-scoped tenant identities, and register bounded cleanup immediately.
No route, provider, runtime or production migration is activated by this test.

For the canonical monetary successor, `make admission-cost-ydb-gate` uses the
same explicit true-data DSN and disposable-database requirement. It exercises
the actual serializable deployment backfill: earlier markers are insufficient,
each dispatch/job/effect backlog denies the transition, dry-run writes no
monetary receipt, and an empty-backlog commit records database time. A repeated
backfill preserves that exact receipt even after new jobs exist. The test
refuses every source inventory traversed by that full backfill before changing
schema state. It restores all six exact prior receipt rows, including the
lifecycle-index receipt and original absence; the serving-marker fixture also
restores both rows it touches. A nested cleanup regression verifies prior
absence and timestamp. Only random owned backlog fixtures are removed; the
gate never resets existing tenant data. It does not certify a
real deployment or authorize old writers to remain active during cutover.

`make partition-status` emits the live primary keys, partition settings, counts,
and contract drift as JSON. The bucketed ready/expiry expand/backfill/cutover
procedure is documented in
[ydb-partitioning.md](ydb-partitioning.md). `make partition-backfill` is a
deployment migration command for lifecycle indexes, not a normal serving
operation. Before deploying v2-only ready/expiry code with retained work, use
the compatible dual-writing predecessor pinned in
[#206 execution comments](https://gitcode.com/urandon/sessionless/issues/206)
to backfill those four projections and verify their tuples. The current command
does not scan their legacy tables; its lifecycle marker does not prove this
separate ready/expiry handoff. No environment operation is authorized by a
source or unit-test pass.

`make ready-expiry-repair` plans or applies one authorized ready/expiry v2
metadata repair from retained source records. It does not perform a batch
backfill or repeat an external effect. Follow
[the operator procedure](ready-expiry-repair.md) for exact selectors, source and
output limits, deletion fences and ambiguous-commit handling.
`make ready-expiry-repair-test` runs offline CLI/store race tests without opening
a database.

Pending migrations 00126–00129 retire exactly four legacy discovery indexes.
`make migrate-local` applies pending destructive definitions; it is not an
inventory-only command. Retained environments must satisfy the
[coverage/drain and rollback requirements](../migrations/ydb/README.md#legacy-readyexpiry-retirement)
and have separate environment-operation authority first. The repository-only
`make ready-expiry-retirement-ydb-gate` executes existing/missing four-table
controls only in explicitly authorized disposable GitHub CI. Local author
validation compiles that integration package without executing it.

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
