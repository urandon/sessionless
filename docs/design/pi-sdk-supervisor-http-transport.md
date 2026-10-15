# Pi SDK supervisor HTTP transport draft

Version: 0.1.0. Engineering proposal under
[#202](https://gitcode.com/urandon/sessionless/issues/202) and
[#203](https://gitcode.com/urandon/sessionless/issues/203).

The proposed transport keeps Pi's genuine loop, standard Responses provider,
SDK construction and parser, while the original Go supervisor owns the sole
provider connection. The child retains network denial. Secret-free supervisor
IPC and the original file-only credential lifecycle remain requirements.

The accepted 0.1.3 [Pi profile](pi-sdk-attached-media-profile.md) remains the
governing contract. Its 0.1.4 SPU2 section is a separate transport draft; this
proposal must receive independent requirements-led acceptance before replacing
that section or authorizing implementation. It enables no profile, provider
call, credential access, platform change or rollout.

## Concrete readiness findings

| Boundary | Source evidence | Engineering consequence |
| --- | --- | --- |
| Child network | [Supervisor](../../internal/attachedworkerdaemon/supervisor.go) requires `NetworkDenied`; [OCI launcher](../../internal/attachedworkeroci/launcher.go) uses and verifies `--network none`. | The unmodified launcher does not provide provider egress. Do not flip its assertion or select host networking. |
| Actual fetch | [Entry](../../internal/pisdkmedia/native/entry.mjs) captures child `globalThis.fetch`; [bootstrap](../../internal/pisdkmedia/native/bootstrap.mjs) passes the guarded fetch to ModelRuntime; [physical boundary](../../internal/pisdktransport/request.mjs) eventually invokes it. | A successful phase reply cannot repair the network mismatch. A concrete transport adaptation is required. |
| Native tuple | The opt-in [functional fixture](../../internal/pisdkmedia/prepared_execution_functional_test.go) selects a Darwin Node executable; OCI preflight requires a Linux engine and image and mounts the selected executable. | That unchanged local tuple is not a Linux OCI runtime. Qualify an actual Linux Node, SDK and image tuple, not the Darwin test's digest. |
| Credential ownership | [Native reader](../../internal/pisdkmedia/native/credential_file.mjs) requires a regular, single-link, mode0600 file owned by child `getuid()`. OCI uses an explicit nonroot UID. | Prove the selected UID mapping and immutable read-only file mount. A mismatch is conditional, not proof all Linux mappings fail. The subscription `auth.json` writeback bridge is not an API-file substitute. |
| Qualification | [Profile validation](../../internal/domain/attached_pi_sdk_profile.go) checks structural commitments; [original owner](../../internal/attachedworkersealedinput/pi_prepared_owner.go) still returns `ErrMissingProducer`. | Declared digests, context sizes, synthetic budgets and fake transport success do not qualify an installed runtime or full model fit. |

Docker documents that the [none network driver](https://docs.docker.com/engine/network/drivers/none/)
creates only loopback. Its [foreground attachment contract](https://docs.docker.com/engine/containers/run/#foreground-and-background)
provides standard streams; it does not make an outer client's extra descriptors
into inner workload descriptors. The existing duplex supervisor channel is the
portable candidate across local Linux and a Darwin-hosted Linux VM.

## Selected design candidate

Use one fixed byte transport inside the SAME Go supervisor and original
invocation. It is not a model client, listening proxy, sidecar, second runner,
second session or separate credential lifecycle. It neither constructs model
messages nor interprets a successful model answer. The real Pi provider still
constructs the request and the real Pi parser still produces the result.

The transport receives the complete actual serialized SDK body and canonical
nonsecret headers through a new closed protocol. The parent independently
compares them with its pre-Issue projection. An opaque capability belonging to
the SAME immutable file owner verifies the SDK's authorization commitment and
writes the exact Bearer header only into that invocation's fixed TLS sink.
Neither raw Authorization nor key bytes cross the process channel.

Rejected alternatives are removing network denial, injecting an ambient host
fetch/client/proxy, changing file delivery to a direct/environment credential,
and carrying Authorization in child stdout. Discarded stdout alone does not
preserve a secret-free IPC contract. A generic host Unix socket is not selected:
its Darwin-to-VM behavior and ownership would introduce another platform gate.

## Original credential custody

Prepare a bounded, single-use transport plan and a fresh 32-byte invocation
nonce before Issue on the original ledger. The nonce is comparison data, not
permission. Failure to obtain it denies preparation. The existing Issue and
Materialize occur once; the genuine native reader still reads the original
immutable file and the genuine ModelRuntime still resolves its literal key.

At the final SDK fetch hook, validate the complete actual method, endpoint,
headers and body as in the accepted profile. Construct an HMAC-SHA256 commitment
using the exact actual Bearer key and a domain-separated, length-delimited
transcript containing the nonce, original harness-binding digest, backend
profile digest, exact request digest and canonical nonsecret-header digest.
The transcript encoding and cross-language vectors must be fixed before code;
string concatenation or a caller-chosen digest format is not a substitute.

The file owner's proposed private capability must verify that commitment
against its original active Materialize record, exact handle/resource/generation,
immutable descriptor and fingerprint. It must not read a secret backend again,
return plaintext, acquire another handle, reenter the callback's state mutex,
accept an arbitrary writer, or outlive the genuine callback. Existing
`VerifyMaterialized` does not already provide this operation.

Only the fixed transport's concrete sink, bound to the SAME profile and
original request, may consume that capability once. Its TLS trust, endpoint
and request identity must be verified by construction, not by a caller's
Boolean or claimed connection metadata. The capability emits the Bearer header
to that sink without exposing it as return data. This is an explicit proposed
extension of the original file owner's internal consumer contract; it is not
permission to reinterpret `file` as the public `direct` delivery arm.

Before any response bytes enter parent-to-child IPC, quarantine the complete
bounded response, including headers, until valid terminal data and actual HTTP
EOF/join. Check UTF-8/JSON/SSE structure and effects, decoded strings, and the
semantic delta streams that Pi would concatenate, against the same credential
without returning the key. A key split across two valid `response.output_text.delta`
events must reject the entire response before either frame is forwarded. Raw
chunk matching, per-frame matching and a final child-side check are insufficient.

Reserve the entire quarantine, frame offsets, decoded strings, semantic joins,
matching work and subsequent IPC copies on the SAME ledger before Issue.
Neither incremental buffer growth nor a fresh budget after receiving a frame
is allowed. Inherited total-response and per-frame limits remain binding;
overflow, unknown semantic assembly, malformed terminal data or missing EOF
rejects without forwarding. The closing design must specify the exact admitted
semantic streams, escape handling and cumulative bounds and prove they fit;
it must not silently truncate the response or narrow the original input cohorts.

Only after the complete guard succeeds may the parent forward the original
frames unchanged. Pull credit controls this replay, not upstream reading.
This candidate explicitly replaces live one-credit streaming with a bounded
whole-response quarantine; it does not promise time-to-first-token streaming.
The native guard remains in place, and parity and split/escaped cross-event
tests must establish the shared rejection contract. Neither guard replaces
Pi's Responses parser.

## Closed duplex protocol

Use a new canonical private magic `SPU3`, never an optional SPU2 alias. Its
17-byte header retains kind, uint32 metadata length and uint64 body length.
Validate header, order and inherited MIN caps before reading declared regions.
Old profiles and artifacts must not be silently reinterpreted: exact transport,
bootstrap and native artifact commitments select the new grammar. Historical
reconciliation is not permission to execute an old transport.

| Frame | Direction | Exact role |
| --- | --- | --- |
| Initialize | Parent to child | Complete pre-funded Init, original file locator and nonce, once; no key, bearer or new source selector. |
| Fetch request | Child to parent | Exact complete SDK body, backend-profile binding, canonical nonsecret headers and authorization commitment, once. No URL override or raw Authorization. |
| Response start | Parent to child | Only validated fixed status200 and bounded admitted SSE/identity headers after the whole response passes quarantine and EOF/join. No reusable use permit. |
| Response part | Parent to child | One bounded original SSE frame from the fully approved quarantine, preserving its bytes; no synthesized model event. |
| Pull | Child to parent | One credit for replaying one next approved response part; no additional upstream read, queue growth or model-use permission. |
| Response end | Parent to child | Valid terminal SSE plus actual bounded HTTP EOF, once. |
| Structured final | Child to parent | Strict provider-authored final after the genuine parser and loop complete. |
| Retire | Parent to child | Revokes and joins the same request/process; never creates cleanup or model-use authority. |

The implementation design must close every kind value, metadata field/length,
direction, count and transition and derive exact maximum wire/copy/live/node
totals before this protocol is executable. It must retain full original bodies,
history and both image cohorts within the existing 1MiB seal/frame and other
MIN bounds. No old SPU2 wire totals can be borrowed for these extra frames.

## Physical connection and deadlines

After full request and file-capability verification, consume the original
`PreparedAPIPhysicalSendV1` at the actual transport boundary. Retain its exact
monotonic cutoff F. DNS, TCP, TLS, the sole request write, response reads,
frame forwarding and supervised process/control lifetime must all be bounded
by the same earlier original F and cancellation. Preparation creates no
connection and grants no network effect.

Use the exact approved HTTPS host, port443, method and path from the sealed
profile; reject IP literals, private/special-use resolutions, redirects,
ambient proxies, alternate addresses/reconnects, compression, trailers and
automatic retries. Pin one validated public address set and one verified TLS
connection. A partial write, lost response, parser rejection or cancellation
burns the attempt; no other model, host or send follows.

The existing [pinned managed HTTPS transport](../../internal/serverlessegress/pinned_https_proxy.go)
is evidence for a one-connection mechanism, not a drop-in Pi implementation:
it seals a DirectOpenRouter route and direct credential delivery. Do not widen
that route, reuse its attestation as Pi authority or alter stopped #189 work.

F governs the actual parent-side network effect; a fresh child timeout or
cross-process clock conversion cannot extend it. Original Source-RPC elapsed
clock provenance, suspend behavior and conservative slack still require their
own qualification. Relocating the socket does not prove those facts.

## Implementation and acceptance sequence

Use the existing #202 design and #203 implementation owners, not another
activation epic. Keep the following work in the #72 decomposition:

- [ ] Independently accept the transport and opaque file-consumer amendment
  against original #203 requirements; resolve the precise transcript, frames,
  allocator schedule and credential-custody contracts before runtime code.
- [ ] Implement the exact protocol and file capability with zero-effect
  budget/foreign-scope/reentry negatives and cross-language vectors.
- [ ] Implement the fixed parent HTTPS sink and prepaid whole-response
  quarantine with decoded-string and cross-event secret/effect checks;
  prove one actual request, public-address/TLS pinning and cancellation using
  credential-free test-owned TLS peers, without altering production roots.
- [ ] Integrate the real Pi SDK and sole original runner through those bytes;
  no fake source/platform result may satisfy the genuine qualification gate.
- [ ] Qualify the actual Linux Node/SDK/image and immutable-file UID mapping,
  full original model/context/vision/allocation fit and the original clock
  bounds before Prepare can return eligible data.
- [ ] Complete actual-body #200 exporter, canonical receipt/ACK/#201 downloads,
  two-owner lifecycle proof, original-first whole-feature review and exact-head
  CI/merge. Keep existing #133/#129/#35 activation separate.

The parent remains ineligible until its original requirements are all proved.
Acceptance of this design must not be recorded as runtime conformance, a
working WebUI answer, real visual/model evidence or rollout permission.
