# Shared external harness execution boundary

Version: 0.5.0. The 0.1.0 architecture is the independently reviewed engineering design for
[#215](https://gitcode.com/urandon/sessionless/issues/215), using the accepted
[#214 comparison](https://gitcode.com/urandon/sessionless/issues/214#note_194345569).
Owner: #13; attached delivery: #72/#133; managed delivery: #176/#175.
Primary engineering acceptance is limited to this architecture and
decomposition; implementation and exact-profile activation remain separate.
The selected-profile integration delta below is independently reviewed and
accepted as bounded, disabled engineering work under #133. It names weaker
native-envelope guarantees explicitly, not as V1 equivalence or satisfaction
of the original #199 criteria. Governing policy admission, complete artifacts
and actual native/serving-profile qualification remain separate; no serving
artifact is retroactively qualified or activated.

Run a ready-made harness as a contained native process. Reuse the existing Go
supervisor, credential lifecycle and canonical result pipeline. Add only the
external egress companion needed by the selected native client; do not mediate
its SDK, reconstruct its requests or replace its model client. The managed
Sessionless agent retains its own Go loop and inference transport.

The first candidate is a tool-free, owner-local Codex exec profile on an
explicit Linux-rootless installation. It needs one workload container and an
attempt-scoped trusted host companion, not a standing service or a mandatory
two-container pod. This architecture is conditional on exact native packaging
and enforcement proof. No native egress profile is enabled by this document.

## Ownership and trust boundaries

```text
Sessionless control plane
  canonical Session, admission, Attempt, lease/fence, durable terminal
        |
        | authenticated accepted attempt and bounded canonical input
        v
Existing attached coordinator and Go supervisor   [trusted owner host]
  | credential lifecycle, current authority, output/receipt finalization
  | controls actual workload and companion lifetime
  |
  +-- native egress companion                    [trusted host process]
  |     exact attempt policy -> DNS/IP filter -> fixed peer connection
  |                        ^
  |                        | private attempt Unix socket, opaque TLS bytes
  +-- OCI workload ------------------------------ [contained process tree]
        tiny TCP-to-Unix byte relay on container loopback
        pinned native Codex exec -> its own model client and native loop
        read-only admitted input; private bounded scratch; exact auth file

Managed Sessionless Go loop -> existing direct provider-session transport
  shares authority/lifecycle principles, not this native tunnel or CLI loop
```

The host supervisor and launcher are trusted, as in the existing attached
threat model. Neither a hostile owner host nor its rootless engine is made
trustworthy by an attestation digest. The workload gets no host engine socket,
host network, helper control port, YDB/blob/queue authority or sibling mounts.
The companion has no authority to allocate attempts, read canonical sessions,
mint credentials, finalize results or change provider/resource selection.

The socket is a bounded connection capability, not a generic host-filesystem
mount. Only the exact socket inode is exposed; the parent remains private to
the trusted host. Socket permissions must work with the exact rootless UID
mapping without making its parent discoverable to other host users. Inspect
and prove this relationship; do not grant broader mounts or privileges to make
an incompatible engine pass.

## Selected topology and alternatives

| Option | Decision |
| --- | --- |
| Existing network-none OCI profile unchanged | Keep for its existing use. It grants no native provider egress. |
| Network-none workload plus explicit Unix conduit to a host companion | Selected Linux-rootless candidate. No workload NIC, direct DNS or alternate route; the one conduit is separately admitted. |
| Workload with unrestricted NIC and proxy environment | Reject. A native client or descendant can bypass the proxy. |
| Same-UID helper beside the native process | Not a credential/process boundary. A byte relay may live here only because it holds no secret or host authority. |
| Separate helper container inside an engine VM | Optional portability successor when the selected host conduit is unavailable. Requires its own socket, lifecycle and resource proof; not an automatic Darwin implementation. |
| New public broker service, Docker-in-Docker or hosted third-party agent loop | Reject for this slice. No demonstrated need for their authentication, privileges, lifecycle or operational state. |

Docker documents that `network none` leaves container loopback, and that bind
sources belong to the daemon host, not necessarily the client host. This makes
the Unix conduit a Linux-host candidate, not proof that a macOS socket works
through a VM filesystem share. See [none networking](https://docs.docker.com/engine/network/drivers/none/)
and [bind mounts](https://docs.docker.com/engine/storage/bind-mounts/).

## Minimum versioned contract delta

Preserve `IsolationProfile.Validate`, the existing network-denied launcher and
all v1 serving paths. Even with `--network none`, a mounted conduit creates
effective egress: reporting `NetworkDenied=true` would be false. Introduce one
explicit `IsolationProfileV2` successor with a closed network policy:

- `denied`: no conduit or outbound route, preserving v1 semantics;
- `native_connect_unix.v1`: the exact selected companion/relay artifacts,
  original attempt scope, socket identity, policy digest and finite limits.

The successor reuses existing filesystem, process, disk and workload lifecycle
interfaces. It is selected only by an explicit new profile binding and
matching constructor. Old constructors reject it; there is no v1-to-v2
coercion, permissive boolean, legacy decoder or automatic startup selection.

Its launch envelope must explicitly distinguish the native executable from the
small pinned bootstrap/relay process bundle and the exact Unix socket mount
from regular read-only input files. V1 `LaunchSpec`/`WorkloadAttestation` cannot
represent those facts and must reject this profile. The V2 envelope seals every
bundle member, native argv and mount; no generic writable mount or caller path
is added to the old read-roots list. The launcher creates the socket and retains
its original handle/inode; a matching string path is not producer evidence.
The bootstrap only starts the relay and native child, reports bounded local
readiness and joins their exits. It owns no model, retry, credential lifecycle,
output interpretation or attempt state machine. Its identity must not be
misreported as the Codex executable in the existing single-process attestation.

The reviewed launcher owns both the workload and actual companion handle.
Preparation installs them atomically from the caller's perspective: any
failure closes owned connections, stops created processes and removes exact
owned objects. The returned boundary retains the existing
`Command`, `AttestedWorkload`, `GracefulStop`, `ForceStop`, `Alive`, `Release`
responsibilities. `Alive=false` and successful release require both members to
be quiescent, including relays and accepted connections, not merely outer CLI
exit. Use the same supervisor execution owner rather than a parallel runner.

The companion is a private Go component launched under that owner. Its factory
receives the genuine accepted attached Session/Attempt and current authority
projection, not a caller-authored approval DTO. It seals tenant/owner/resource
revision/generation, enrollment/connection generation, worker, Run/Attempt,
lease/fence, reservation/physical claim, artifact/policy and expiry. Keep the
current attached authority checks at preparation, opening each connection and
finalization; cancel/fence/revoke observation retires the actual original
handle. Copying fields or setting a readiness flag cannot mint a handle.

Control remains through the existing supervisor's private Go ownership and
bounded process lifecycle. Native data uses standard HTTP CONNECT followed by
opaque bytes. No per-model-call IPC, SDK phase-authorizer, request mirror,
substituted fetch, TLS interception or second response parser is introduced.

## Native network policy

The first conduit accepts only strict bounded CONNECT to one of the profile's
explicit provider origins on port 443. The relay supplies no destination or
policy authority. Reject absolute HTTP requests, SOCKS, arbitrary Unix paths,
IP-literal targets, userinfo, alternate ports, malformed/ambiguous headers,
proxy chaining and unsupported protocol upgrades before dialing.

The trusted companion resolves the exact admitted hostname, rejects the whole
answer set if any address is private, loopback, link-local, metadata, multicast
or otherwise forbidden, and pins one literal permitted address for that
connection. Each new connection repeats this check under the same original
deadline and finite total connection/byte budget. No resolver or route comes
from ambient proxy environment. DNS, direct-IP/UDP/IPv6 access, alternate
interfaces, host gateways and other sockets are denied to the workload outside
the conduit. Missing companion or relay means failure, never direct egress.

The native client owns end-to-end TLS certificate/name validation. The
companion does not claim a verified TLS peer, encrypted HTTP method/path,
model, response semantics or exact request/token count. CONNECT and byte limits
cannot establish those facts. Same-origin requests and encrypted redirects
are opaque; a new destination still needs a separately permitted CONNECT.
Do not feed tunnel observations into `ProxyAttestationV1` or #189/#190
per-operation direct-HTTP authority. Those contracts remain distinct.

For the selected exact native artifact, #133 must qualify its configured
provider-origin set, TLS validation and native retry/tool limits. No observed
competitor hostname is a policy allowlist. If a governing model, data, token,
effect or price policy requires evidence that this native interface cannot
provide or conservatively bound before execution, the profile is ineligible.
Do not replace the missing fact with a native log, byte count or optimistic
budget estimate. In particular, this tunnel does not satisfy the managed
per-model-operation source/spending contract.

## Credentials and native capabilities

Retain the original prepared runner's single
`Issue -> Materialize -> Execute -> WriteBack -> Release` sequence. The
companion receives no personal subscription secret. The selected attached
Codex process still needs its exact owner-local `auth.json`, under the
existing generation/revision-fenced lifecycle and exact-file OCI bridge.
No ambient login, keychain, user HOME, copied provider cache or cloud custody
is inferred from container placement.

The [owner-local subscription state design](owner-local-subscription-state.md)
defines a bounded encrypted local adapter for that existing lifecycle and its
metadata-only native reader. It does not yet provide a serving backend, an
authenticated generation-publication path or permission to use real OAuth.

The first profile has no shell, model-controlled filesystem operations, MCP,
plugins, dynamic dependencies or native search. Input/media and structured
final semantics remain with [the attached media design](attached-harness-media-and-artifacts.md)
and #199–201. Enumerate the complete exact native tool set, including read-only
and implicit tools; a read-only tool that can read the credential is enough to
make the profile ineligible. Native permission prompts and a single shell flag
are not tool exclusion. Prove absence at actual effect sinks, including
image/file helper paths, credential inheritance and unexpected event variants.

If model-controlled descendants are enabled later, the secret must be outside
their enforced privilege/read/inheritance boundary or the profile is no-go.
This design does not create that future sandbox. It does not inject credentials
into end-to-end TLS or silently turn the companion into a model API emulator.
Managed shell/tool-enabled native CLI remains no-go under the existing
[credential exclusion](../serverless-harness.md#egress-model).

## Selected Codex profile integration

The implementation owner is [#133](https://gitcode.com/urandon/sessionless/issues/133),
consuming [#220's released-artifact findings](https://gitcode.com/urandon/sessionless/issues/220#note_195175490).
Use an explicit `NativeProfileV2` successor through the existing prepared
pipeline. Preserve the old `Config`, descriptor, constructors, direct-effect
checks and unqualified `MediaConfigV1` denial. Do not select native semantics
from process exit, attachments, a reachable socket or ambient installation.

### Immutable installation and actual configuration

The first native candidate is stock Codex 0.162.1, associated with official
[source 092d3acd](https://github.com/openai/codex/tree/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d).
The investigated Linux musl executable has SHA-256
`628e35487d888a28f0427fc37501da529dd3d81fcbad7a2c3421c1e8670d92e1`.
These identify the research candidate, not an activated package or a verified
release signature. A different executable requires its own exact-profile proof.

The native descriptor and existing install-policy identity must seal the actual
bounded generated configuration/catalog bytes, argv and environment, selected
model/metadata and context evidence, media policy, conduit/artifact identities,
platform and numeric limits. Derive digests from the actual frozen bytes; a
caller-provided matching digest is not execution authority. Exclude the local
activation toggle from identity so cancellation survives disabling admission.

Stage the selected non-secret config/catalog read-only under the original
attempt's private custody and verify their original file identities before
launch. Use only the pinned executable's supported explicit configuration
surface. The V1 command's `--ignore-user-config` must not silently discard the
selected generated config. Seal the effective argv and paths with those bytes;
no user HOME/config, live model discovery, package download, fallback provider,
or caller-supplied per-run config override. `auth.json` remains the separately
fenced credential file owned by the existing outer lifecycle, not a config copy.

The selected configuration must suppress all native tools and implicit helpers,
including Goals and image-view helpers. The investigated native default allowed
Goals, and `features.unbounded_connection_retries` bypassed ordinary provider
retry zero. Both need explicit disabled values in the effective pinned profile;
provider retry settings alone are insufficient. Include all remaining supported
tool/feature exclusions and static catalog fields in the same identity and
effect-sink conformance. Do not transplant the credential-free fake provider's
`requires_openai_auth=false` into the owner-local subscription profile.

### One original execution and result owner

Extend the existing `PreparedAttachedWorkerRunnerV1`/Stack selection explicitly,
not with another invocation runner or a generic factory callback. It must borrow
the actual connected Session's `NativeAttemptV2` from the original verified
input, use `NewNativePreparedProcessBoundaryV2` and retain `BindPreparedEntryV2`.
The existing consumed latch, cancellation arbitration and sole
Issue -> Materialize -> Execute -> WriteBack -> Release remain authoritative.
An accepted-attempt DTO copy or caller predicate cannot replace that Session.

Native reduction must be a separate branch selected by the immutable successor
binding. Retain genuine private observations from that exact boundary and
native child through stop/finalization; fake public process flags cannot create
a receipt. A successful bounded native turn needs the authentic final result,
clean exit, original authority at the existing completion arbitration, complete
workload/relay/companion stop, cleanup and credential quiescence. The outer
runner's actual WriteBack/Release and original receipt/finalizer still decide
canonical success. Lost current authority or unknown cleanup denies success.

Do not set V1 `ProviderEffects=1` or its direct-effect fence for this branch.
One consumed native invocation is not one model request: H4/H7 demonstrated a
second native sampling request after a rejected tool call. Preserve existing
unknown route/usage evidence and conditional policy verdict. If the governing
policy needs an unavailable model/route/token/price/per-operation fact, native
execution is ineligible; tunnel traffic is not that fact.

Recognize a valid native `turn.failed` without setting successful `Terminal`.
Discard partial final bytes; reject any later frame. An error notification or
lost reply alone is not a definitive failed-turn witness. Keep that distinction
private in the original result owner if needed for a failure receipt; no new
public scheduler/lifecycle authority. No failure, lost reply or failed receipt
can reopen the consumed claim or automatically repeat the native invocation.

### Required media and implementation acceptance

The successor must retain the existing canonical text/JSON and PNG/JPEG cohort,
source order/digest associations, finite original-byte/pixel/context preflight,
private staging, actual `--image` input and trusted structured-result serializer.
It must not relabel a text-only milestone as attached MVP delivery. Reuse the
existing validators and receipt candidate; do not harvest the native directory.

Do not make `MediaConfigV1.eligibility()` unconditional or simulate the absent
SDK preparation/request hook. The explicitly different native successor uses
the [native envelope media contract](attached-harness-media-and-artifacts.md#native-envelope-successor)
below, rather than claiming to implement V1's unproved internal preparation,
allocation and exact context-fit assumptions. Original bytes, canonical
model-visible associations, bounded pre-Issue validation and the required
PNG/JPEG/resize cohort remain mandatory. No silent limit increase, SDK hook,
caller approval boolean or text-only MVP substitution is permitted.

Exact native internal allocations, prepared representation sizes and context
fit remain unknown where the released interface does not prove them. Finite
process/traffic ceilings enforce only their actual external boundary; they do
not retrospectively certify those internal facts. A selected profile is
eligible only when the existing governing resource/policy evidence permits
this explicitly named unknown envelope. If it instead requires exact
context/token/price/per-operation or before-send prepared-byte facts, deny
before Issue. `conditional` in an output DTO is not such policy permission.
The immutable admitted evidence bundle and its exact binding, scope, expiry,
policy identity and unknown-capability conditions must be checked; no new
reservation, approval ledger or mutable worker-local grant is introduced.

H7 proves one tiny exact PNG transport and disabled `view_image`, not maximum
cohorts, JPEG/resize/context behavior or useful vision comprehension. Qualify
those against the actual generated profile, original supervisor and receipt
pipeline before activation. The owner-accepted research VM is useful native
evidence; it does not add a product VM backend or prove the serving OCI tuple.

The #133 execution checklist owns one coherent implementation: frozen effective
profile -> original Session/prepared owner -> honest native reduction and
canonical media -> existing output/receipt/ACK. First run bounded offline
wrong-binding/config mutation/failure/cancel/cleanup/no-replay tests, independent
original-goal exact-source review and required exact-head CI. Then qualify and
activate only the exact admitted resource/platform under its own authority.
Config-only registration, a source factory or green fixtures are not the demo.

## Original input binding with fixed workload paths

`NativeProfileV2` needs an explicit `NativeLaunchV3` input-binding successor;
the already delivered V2 launch/install contract remains unchanged. This is
not a new worker, scheduler, process runner or agent-side model transport.
The same original Session child, prepared consumed entry and supervisor run
retain authority, cancellation, conduit, stop/release and credential ownership.

The concrete incompatibility is visible in current source:
`nativeharnessbootstrap.Arguments` embeds the entire native argv in the
bootstrap, `NativeLaunchV2.InstallPolicyDigestV2` seals it, and the OCI launcher
requires byte-exact equality against the actual command. Appending fresh
host-stage `--image` paths or a random catalog pathname cannot preserve the
already accepted installation policy. Removing argv from the V2 hash,
normalizing arbitrary strings, rehashing a changed bundle against itself, or
sharing predictable host staging directories is not a solution.

### Installation versus execution binding

The immutable V3 installation has fixed **workload-visible** read-only slots:
`/sessionless/profile/models.json`, `/sessionless/profile/output.schema.json`,
and at most two `/sessionless/input/image-01.png`/`.jpg` and `image-02.png`/`.jpg`
slots. Only the selected suffix exists. These are container paths, not stable
or shared host directories. Original source staging remains fresh, private
and random under its existing held-directory ownership.

For the explicit native V2 profile, the generated final schema requires both
`answer` and `artifacts`; an empty array is the answer-only wire representation.
The pinned native [turn builder](https://github.com/openai/codex/blob/092d3acd6bec3e3a14bdc7e7a2810ab628ab759d/codex-rs/core/src/session/turn.rs#L1586)
uses strict output outside Guardian, and OpenAI's [strict-schema contract](https://developers.openai.com/api/docs/guides/structured-outputs)
requires every declared property. Do not generate an optional native property
or accept a different host shape as a compatibility fallback. This preserves
answer-only product behavior without inventing files. Host validation retains
the accepted 128 KiB encoded final, 32 KiB UTF-8 answer, 32000-rune, two 32 KiB
artifacts/64 KiB aggregate and depth32 ceilings, separately from the larger
input and raw-protocol limits. These bounds are part of the frozen profile.
Source/schema checks are not evidence of actual native/provider qualification.

Native execution inherits the admitted job runtime from the original accepted
offer's creation time, not a fresh window at launch. Preparation/Issue consume
that same window; caller, credential, evidence, lease and installed process
deadlines may only tighten it. The lease's separate finalization allowance does
not extend model execution. Original WriteBack/Release keep their existing
independent bounded finalization ownership. After actual stop/release and before
the original native child retires, sample the full evidence/current OAuth
eligibility under the same unextended active deadline. Lost eligibility denies
success without erasing observed acceptance, refunding consumption or replaying.

Seal the exact catalog/schema bytes, generated CLI configuration, executable,
helpers, platform/resource bounds and all seven permitted full native commands:
no image; one PNG; one JPEG; PNG/PNG; PNG/JPEG; JPEG/PNG; JPEG/JPEG. Each command
has the exact fixed paths and image order. This is a finite list of exact argv
vectors, not an arbitrary substitution language. Canonical text/JSON file
contents remain in the already verified prompt; they do not create more mount
slots. Keep existing argument count/length/encoding ceilings, including the
complete generated bootstrap/relay argv, and reject installation overflow.
Fixed TOML inline-table overrides may compact the generated settings; never
drop feature/provider exclusions to make an oversized argv fit.

After the original consumed entry, derive the selected variant only from the
original verified canonical input and `mediaStage.verifyImages()`: exact count,
ordered media types, original digests and held no-follow file/directory
identities. Derive a separate execution binding inside that owner from the
already accepted scope/manifest, installation digest, selected exact command,
ordered original associations, actual frozen catalog/schema bytes and finite
source-to-destination mappings. The execution digest is evidence of that
derivation; it is not caller approval, a replacement policy or a second claim.

The explicit runtime carrier is a strict, canonical JSON
`NativeExecutionDescriptorV3` scalar, at most 4 KiB, supplied to the bootstrap
as one `--execution-descriptor-json` value. The original prepared owner produces
it, not the caller. Its closed fields contain version 3, the accepted
scope/manifest association digest, installation digest, selected variant index,
and each selected image's slot ordinal, byte size and SHA-256 in canonical
order, plus the frozen catalog/schema sizes and digests. Slot paths and suffixes
are derived from the sealed variant; the descriptor accepts no arbitrary host
or workload pathname. Host source/inode associations remain in the original
owner and launcher mapping attestation, not in native argv or the descriptor.
Each mapped file has a positive size of at most 1 MiB: the existing original
image cap, and a finite engineering cap for each frozen catalog/schema. These
are input-file bounds, not native prepared, context, wire or allocation facts.

Installation seals the static bootstrap/relay command, environment and this
exact carrier schema and bounds, together with the seven native vectors. It
does not seal the per-execution descriptor values as installation facts.
V3 execution attestation instead seals the actual complete bootstrap command,
including the canonical descriptor, selected exact native vector and original
source-to-destination mappings. The launcher verifies this command and carrier
against the original association before create/start. This explicit V3
static/runtime separation is not normalization or removal of a V2 hash check.
The descriptor is not an extra mount, generic substitution template, new claim
or authority; the bootstrap consumes it and does not pass it to the native
child. Unknown/duplicate fields, noncanonical encoding, wrong count/order,
missing/extra carrier, size overflow or association mismatch deny execution.

The original connected Session child and its bound prepared entry must retain
this exact execution association through the existing conduit/supervisor call.
Copied DTOs, a caller-provided matching hash or a factory predicate cannot
replace the original child. Attempt mismatch, another original stage, a
different selected vector or resource/profile drift deny before create/start
and retain the consumed claim. Scope, source identity and policy digests are
distinct; none is inferred from a successful socket connection.

### Bounded read-only mappings and repeat verification

V3 adds only the four possible read-only files above: two originals and the
frozen catalog/schema. It does not accept arbitrary mount destinations,
directory trees, writable aliases, credentials, executable replacements or
model-supplied paths. Verify canonical absolute host source locators,
owner/mode/regular-file/no-follow identity, size/hash and allowed destination;
source locators alone are not authority. Forbid overlap with native socket,
credentials, executable/helper, private scratch or another destination.

The launcher rechecks the exact installation/variant and original file
associations before create/start and attests the resulting bounded mappings,
not merely the unmounted argv. The existing bootstrap parses the attested
descriptor and performs a bounded pre-exec size/hash check of every actual
mounted image in selected slot order and of the frozen catalog/schema.
Original held-owner/inode provenance, inspected read-only source/destination
mappings and mounted-byte integrity are three distinct checks; the last
does not establish the first two. Replacement, missing/unreadable/extra files, suffix/order drift or
inspection failure prevent native exec. It never parses a model request,
intercepts the SDK, handles provider credentials or grants a new network path.
Actual source-to-destination file mounts replace the old same-path read-root
assumption only for this explicit V3 constructor; V1/V2 remain unchanged.

Native `auth.json` remains the independently fenced exact writeback file in
the existing credential home, never a copied config or an input mount. All
existing credential issue/materialization/drain/writeback/release checks,
input inode cleanup and original outer receipt ownership still apply. An
unknown stop or mount/file handback prevents success and retains custody;
neither failed verification nor receipt loss permits a new native invocation.

Before publication, controlled fake tests must demonstrate all seven vectors,
unchanged install identity across fresh original stages, different execution
associations, every wrong/foreign/copied/substituted mapping and config mutation,
descriptor omission/substitution/order/digest/encoding/limit violations,
argument limits, actual launcher/inspect/bootstrap handoff and cancel/failure
cleanup under the original owner. A constructor-only positive or source hash
is not acceptance. Exact released native/config/media and actual platform
proof remain separate activation requirements; this input binding does not
prove image context fit or create a governing policy grant.

## Explicit installed native profile and admission composition

This engineering delta completes the proposed carrier for `NativeProfileV2`;
it is not profile qualification or activation. It requires independent review
with the coherent #133 composition before acceptance.

The existing manifest's OCI boundary discriminant gains the separate value
`linux-rootless-native-v3`. Only that value admits exact namespace UID/GID
`0:0`, matching the owner-mapped rootless socket topology. Existing boundary
values continue to require nonzero UID and GID. Mixed pairs and unknown
discriminants deny. Ordinary V1 Stack/launcher constructors reject the native
variant before engine access; only the explicit native composition lowers it
to the underlying Linux-rootless launcher. Never rewrite the installed
manifest or silently replace UID fields. Actual non-root host, same-owner
rootless engine and UID/socket proof remain required.

Derive the static profile identity from actual generated configuration,
catalog, schema, seven commands, fixed replacement-environment ABI, model and
declared context/reserve, original manifest platform discriminant and every
OCI field, numeric media/protocol/conduit bounds and closed policy semantics.
`NativeLaunchV3.StaticProfileDigest` carries this derived identity into its
installation digest. It is not a caller approval hash. Zero identifies only
an unqualified lower-level contract, never an eligible full profile. No
activated V3 tuple is upgraded in place; a changed installation identity needs
its own exact-profile evidence. Activation and injected clock are not identity.
An output reserve is not an unsupported native max-output-token setting or
proof of context fit.

The full existing `ProviderEvidenceBundleV1` is frozen beside the static
profile and matched against the server-sealed job binding before Issue.
Putting that whole bundle inside its own descriptor would create a digest
cycle: every evidence scope already contains the descriptor. Validate exact
scope/model/resource, all six evidence digests and policy references, data
class and minimum expiry; future observations or stale evidence deny. A hash
match or generic conditional verdict alone grants nothing.

The closed policy identity `OAI-SUB-NATIVE-ENVELOPE-V2`, revision 1, names only
the reviewed owner-local, tool-free, one-consumed-invocation envelope. Native
context fit, transformed representation/allocation bounds, internal sampling
count, actual encrypted route, usage and price/cost remain unknown. The
matching policy permits those named unknowns, with unknown route and denied
fallback; it does not erase known-route or exact per-operation requirements.
Privacy and data-class requirements remain unchanged. Existing canonical
credential metadata must still be active, unrevoked, exact owner/tenant/resource
generation, OpenAI/ChatGPT OAuth file delivery before the original lifecycle
issues anything. This reads metadata, not secrets, and adds no permission ledger.

Generated files and original images belong to existing `preparedHistory`.
Keep original held directories, readonly file FDs and inode identities; do
not adopt a copied owner or supplied readset. Only the original consumed entry
derives its one V3 execution readset. After actual supervisor dispatch, unknown
handback retains the original stage and handles; Close or receipt retirement
must not discard that custody. Genuine complete cleanup permits the original
bounded removal. No new reservation, runner or durable entity follows.

The implementation handoff remains #133's checklist: explicit full profile
and original Session/prepared/Stack selection; native result and structured
artifacts through original output/receipt/ACK; then exact native/media/platform/
OAuth proof and separately authorized activation. Profile hashing and staging
unit tests alone do not complete these integration items.

## Packaging and resource bounds

The installation manifest pins official executable provenance/digest, native
protocol/schema, model and capability evidence, generated config/argv digest,
relay/companion digests, OCI image and engine identities, rootless host tuple,
numeric UID mapping and all read/write roots. A source checkout hash is not a
released executable pin. No profile may register until the real packaged tuple
exists and passes conformance. Unsupported flags/config/tool behavior reject
the artifact rather than falling back to weaker settings.

Use fresh per-attempt environment, input staging and scratch. Keep read-only
root/input/runtime mounts, no capability/device/daemon grants, no network
namespace sharing, no inherited config, no global native home/cache/session and
no invocation-time package/image download. Native resume is disabled. Rootless
CPU, memory, PID, scratch/file, stdout/stderr/event and total-byte ceilings must
be inspected and adversarially exercised; a flag accepted by Docker is not
proof of a delegated cgroup controller. Charge the relay to workload limits
and bound the companion separately, including goroutines, sockets and buffers.

The existing OCI launcher has memory/PID/disk controls, not verified CPU quota
or this conduit. Preserve its delivered evidence without claiming it proves
these new axes. Native stdout/event/media bounds and parser ownership remain
with the existing Codex/media profile. Do not allocate an unbounded buffer to
obtain a bounded final answer.

## Lifecycle and failure ordering

```text
current accepted authority + fresh immutable profile
  -> consume existing one-use execution reservation
  -> prepare private companion/socket and contained workload
  -> inspect exact artifacts, mounts, namespaces, quotas and local readiness
  -> recheck original authority; start pinned native process
  -> native CONNECT -> current authority -> fixed bounded peer
  -> bounded native output or cancel/fence/deadline/error
  -> deny new connections; close actual sockets; stop whole workload
  -> verify native/relay/companion quiescence
  -> exact-file handback; independent bounded WriteBack and Release
  -> joined output validation/canonical receipt path + residue verification
  -> success only when all owning finalizers permit it; otherwise taint
```

Readiness uses owned local acknowledgements and inspection, not provider calls
or fixed sleeps. It does not reset the original deadline. Cancellation remains
active during preparation, silent native execution and finalization. Bound
graceful stop, escalation, reader joins and release separately; forced teardown
of an outer Docker client is not workload stop. Credential handback follows
verified workload quiescence; failed writeback must not consume Release's
independent cleanup window.

The coordinator joins actual connection and workload outcomes with original
credential and attached receipt owners. Output validation may retain only the
bounded private candidate; no artifact is uploaded/published until the existing
#199–201 finalization conditions pass. No new directory harvest, native
transcript import or stdout pathname becomes canonical authority.

Failures before any possible outbound bytes can report that limited local
fact. Once bytes may have crossed the tunnel, interruption, lost reply, native
failure or companion death cannot prove provider non-acceptance. Reconcile the
existing physical claim; never reopen a tunnel/resume the native session or
rerun the whole attempt as recovery. Internal native retries must be disabled
or conservatively covered by a proven accepted envelope; otherwise no-go.
Closing a socket does not undo an accepted request or prove remote compute
stopped.

Unknown stop, credential handback, socket drain, residue or removal taints the
allocation and blocks reuse/canonical success. Reconciliation uses exact
installation/profile/engine/attempt ownership, not an unscoped process kill,
volume prune or global container cleanup. No task may stop the user's model
server, Docker daemon, VM, Shadowrocket or unrelated workloads.

## Durable state and platform eligibility

Use existing Attempt/reservation/fence, credential generation/CAS intents,
attached receipt/ACK and canonical output records. Companion handles, socket
identities, byte counters and readiness are process-local. Their loss removes
permission; it does not reconstruct send authority. No new operational table,
secret store, scheduler or invocation state machine is needed. A future change
to existing durable semantics must update the entity registry in its owning
implementation task.

| Platform | Eligibility for this design |
| --- | --- |
| Linux rootless, companion on the engine host | First conditional proof target. Requires actual UID/socket reachability, cgroup controller, no-bypass, native and whole-boundary cleanup evidence. No inherited v1 egress/CPU pass. |
| Darwin with a Linux engine VM | Not eligible for the Linux host-socket artifact. A VM-local companion/explicit carrier needs separate packaging proof; native macOS filesystem sharing is not a proven Unix transport. Do not install/start Colima or make Docker a Dockerless-stand dependency. |
| Yandex managed containers | No-go for this native profile until #90 proves an equivalent externally enforced topology and custody. Request suspension/connection loss and the single-image platform contract are not a sidecar/namespace privilege guarantee. Managed own-loop/direct transport continues independently. |

Yandex documents request-driven instance suspension and loss of connections;
cleanup must complete within the owned runtime lifetime, not rely on a
background helper after returning the request. See
[container lifecycle](https://yandex.cloud/en/docs/serverless-containers/concepts/container).
No new cloud service or cross-cloud migration is implied by this comparison.

## Conformance and implementation handoff

Use deterministic owned fake peers and synthetic marker credentials first.
Pure parsing, ownership, cancellation and supervisor tests need no daemon or
model. An engine/native packaging run needs an explicit finite manifest naming
already available artifacts, engine, roots and cleanup owner. Initial proposed
caps: at most 20 cases/20 minutes; 60 seconds per case plus 10 seconds cleanup;
one workload and at most one optional packaging helper container; at most two
owned peers; workload 1 CPU/512 MiB/64 PIDs/64 MiB scratch, companion 64 MiB;
1 MiB each-direction traffic per case, 8 MiB total logs and 2 GiB incremental
disk under the dedicated workspace root. These are proof caps, not measured
production sizing. An artifact that does not fit fails this manifest; increases
need an explicit replacement manifest, not hidden retries/downloads.

| Test group | Required evidence |
| --- | --- |
| Original authority | Foreign/stale/revoked Session/resource, fence/expiry/cancel and replay deny before launch/open; no caller DTO can mint readiness. |
| Native compatibility | Exact packaged exec/config/schema, authentic bounded text/media/final flow, complete excluded tool set and fixed native retry policy; fake auth does not qualify real OAuth refresh. |
| Conduit | No ambient proxy/direct network/DNS/IP/UDP/IPv6/metadata route; socket substitution/sibling connection denied; filtered/rebound DNS, wrong target/TLS name, malformed CONNECT and exhausted budgets fail closed. |
| Lifecycle | Leader/relay/companion death, detached descendants, partial send/lost reply, fence loss and concurrent cancel; original sockets and actual workload stop; no replay after ambiguity. |
| Limits and residue | CPU/memory/PID/scratch/output exhaustion, failed inspect/handback/removal, next-attempt marker/socket/credential denial and exact owned-object cleanup. |

The owning implementation checklist is deliberately small:

1. [#219 required selected-profile delta](https://gitcode.com/urandon/sessionless/issues/219): versioned launcher/supervisor network
   contract and bounded host companion/byte relay, with original authority,
   resource/cleanup tests and fail-closed registration. Reuse #77/#88/#106;
   do not reopen or relabel their network-none delivery.
2. [#220 required selected-profile proof](https://gitcode.com/urandon/sessionless/issues/220), after #219: pin one real official Codex package and
   Linux-rootless tuple, qualify native/config/tool/retry and conduit behavior
   under the finite credential-free gate. Publish go/no-go to #133. Failure
   remains no-go and must not generate SDK instrumentation or relaxed policy.
3. Existing delivery owners: #133 activates only an eligible attached profile;
   #199–201 own media/output integration. #175/#90/#92 own independent managed
   composition/platform/cloud proof; #176/#191 keep their own loop/exports.

The first two are linked implementation children under #13 with the exact
predecessors and evidence above. No new cross-cutting epic is necessary.
Darwin portability and tool-enabled native
profiles are optional follow-up, not extra MVP requirements. #212's configured
local inference path remains independent: it connects the managed Go client to
an already running owner endpoint and does not use this native tunnel.

## Source backed reuse

The accepted #214 report contains the full pinned competitor comparison and
alternatives. Relevant primary mechanism references are Codex
[proxy routing](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/linux-sandbox/src/proxy_routing.rs)
and its [opaque TLS limitations](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/network-proxy/README.md#security-notes-important),
OpenCode's [explicit no-sandbox boundary](https://github.com/anomalyco/opencode/blob/3a31c4ea801915c0b050df4b3842997ea62b6e93/SECURITY.md#no-sandbox),
and Hermes' [container environment](https://github.com/NousResearch/hermes-agent/blob/c80a0a551c7038517456ee0aeb60203ec92aedb6/tools/environments/docker.py).
Adopt external enforcement and lifecycle separation, not their code, default
permissions, persistent workspaces or client/proxy protocols wholesale.

Authoritative repository contracts remain [OCI isolation](../attached-worker-oci.md),
[Codex exec](../codex-exec-adapter.md), [credential lifecycle](../credential-lifecycle.md),
[managed isolation](../serverless-isolation.md), [direct provider egress](../serverless-egress.md)
and [attached media/output](attached-harness-media-and-artifacts.md). No proposed
native profile overrides their existing guarantees or enables real operations.
