# Serverless provider egress and invocation credentials

Issue #89 adds the feature-disabled boundary between one authenticated
`PreparedInvocation`, the #85 provider-credential lifecycle, and one attested
provider proxy. The #189 concrete transport described below remains unregistered;
neither boundary nor transport enables a secret backend or production route.

## Fixed effect order

`internal/serverlessegress.BoundaryV1` has one fail-closed order:

1. validate the process-local prepared invocation and exact allocation;
2. validate fresh route/effective-policy evidence, exact owner/resource/model,
   the sealed proxy and endpoint policy, and request-byte bounds;
3. atomically consume the prepared invocation before DNS, credential, or
   provider effects;
4. ask the trusted proxy for a short-lived attestation of the exact sealed
   policy, proxy artifact and workload identity;
5. revalidate the capability and authority after preflight, then issue an
   invocation-only #85 handle for the exact tenant, owner, run,
   attempt, worker, lease fence, resource revision, credential generation, and
   shortest remaining authority window;
6. materialize the exact sealed `file|environment|direct` delivery once;
7. immediately revalidate the process-local capability, lease, evidence, cost,
   profile, and proxy-attestation windows before invoking the proxy;
8. exact-validate the observed route, acceptance class, byte counts, response
   bound, and observation time;
9. release the credential under a separate bounded cleanup context even when
   the caller is cancelled or the provider path fails.

Preflight and every credential/provider operation receive a derived context
whose deadline is no later than the shortest active execution, prepared-
capability, invocation, lease, evidence, profile, price, or proxy-attestation
window. Proxy and credential implementations must honor cancellation before
returning; successful return is rechecked against the authoritative clock.

Consuming the capability before proxy preflight is intentionally conservative.
A failed DNS/proxy attestation burns the effect reservation and is reconciled;
it cannot be retried inside the same physical claim. Replaying the same
prepared invocation fails before a second proxy preflight or credential issue.

## Endpoint and proxy contract

`PolicyV1` maps one already admitted `ProviderRouteV1` to one exact endpoint
and one exact proxy artifact/identity pair. The policy digest is the
`SubstrateBindingV1.EgressPolicyDigest`; allocation attestation already seals
the same proxy digests.

The first contract accepts only:

- HTTPS, port 443, `POST`, one canonical absolute path, and no query,
  fragment, percent-encoding, backslash, or dot-segment ambiguity;
- a lowercase DNS name with at least two labels, never an IP literal or a
  special-use `local|localhost|internal|lan|home|test|invalid|example|onion|arpa`
  suffix;
- public-unicast resolution, a content-free digest of the resolved address
  set, a connection pinned to that set, and explicit DNS-rebinding denial;
- denied redirects and ambient proxies, mandatory certificate validation, and
  request/response ceilings no larger than admission cost authority;
- a proxy attestation that expires no later than the earliest prepared
  capability, invocation, lease, provider evidence, substrate profile, or
  price-evidence deadline.

The concrete proxy remains responsible for resolving every address, rejecting
private, loopback, link-local, multicast, metadata, control-plane, and
alternative-port destinations, and pinning the connection so a second DNS
answer cannot change the target. `DNSClass=public_unicast_only` is an attested
fact, not permission for the backend to perform its own lookup. The backend
has no ambient network path.

There is no redirect follow, endpoint discovery, base-URL override, provider
fallback, alternate model, or generic internet mode. A different origin,
method, path, route, model, proxy artifact, or identity requires a new sealed
policy and a newly admitted attempt.

## Concrete pinned HTTPS transport

`PinnedHTTPSProxyV1` implements the transport interface for the exact
DirectOpenRouter endpoint, route tuple and headers in
`internal/directopenrouter/contract.go`. Its public constructor accepts a sealed
policy, trusted clock and finite time bounds, not an HTTP client, endpoint
override, resolver, dialer, certificate callback or caller-selected roots.
It accepts only direct materialization with one bounded Bearer credential;
file, environment and credentialless conversion are denied.

Preflight consumes the object's preparation before resolving the fixed host.
It rejects empty or more than eight answers and rejects the entire answer set
if any address belongs to a forbidden class. The canonical sorted address set
determines the attestation digest. One literal numeric address is dialed at
port 443; a verified TLS handshake with the fixed DNS name and HTTP/1.1 ALPN
completes before preflight returns. Preflight sends no HTTP request. Invocation
uses that original connection without another lookup, alternative address,
redirect, discovery, ambient proxy or automatic retry.

Invocation exact-compares the retained policy and attestation, then consumes
the object before writing once, including when a write or reply is partial.
The original caller deadline bounds the entire lifetime, capped at 60 seconds;
each preflight or invocation is capped at 30 seconds and at the remaining
lifetime. Cancellation closes the owned connection. Explicit `Close` also
releases an abandoned preflight and is safe to repeat. It does not drain a
response or wait for a TLS shutdown handshake.

Request and opaque response bodies are capped at one MiB or the smaller policy
limit, credentials at 4096 bytes, response headers at 16 KiB and 100 fields,
chunk framing at a separate 16 KiB, and chunk-size lines at 256 bytes. Ambiguous
framing, trailers, informational responses, redirects, compression, cookies,
authentication challenges and non-200 responses fail closed. Transport-owned
request, payload and credential copies are cleared after synchronous use;
private connection errors are replaced with package sentinels.

HTTP 200 returns bounded opaque bytes with `AcceptanceUnknown`. Fixed route
metadata records the intended transport tuple; it does not validate JSON model
or provider restrictions and is not evidence of actual upstream execution.
The DirectOpenRouter codec and parser must establish those facts before
composition may publish actual-model or provider-acceptance evidence. The
existing one-shot `BoundaryV1` is not wired to this concrete proxy by startup.

This process-local connection attestation is not durable registered proxy
authority. Original-source and allocation joins, authenticated create-once
registration/current readback, operation debits, typed consent, production
custody and the multi-operation session remain separate #189/#190 work. MCP
and general web search require their own reviewed service policies, not a
widening of this fixed provider endpoint.

Synthetic package tests use a private test-only resolver/dialer/root seam to
route the selected public numeric address to an owned loopback TLS peer. They
exercise real certificate and DNS-name validation, fixed Host/SNI and headers,
no HTTP during preflight, exactly one physical request, DNS changes, hostile
address sets, policy/attestation tampering, partial TLS writes, bounded hostile
responses, concurrency, cancellation and abandoned-connection cleanup. They
do not establish cloud routing or live-provider behavior. The public
constructor has no such test seam.

The restrictions follow the DNS/IP allowlisting and redirect-denial guidance in
the [OWASP SSRF prevention cheat sheet](https://cheatsheetseries.owasp.org/cheatsheets/Server_Side_Request_Forgery_Prevention_Cheat_Sheet.html)
and Go's [TLS certificate and name verification contract](https://pkg.go.dev/crypto/tls#Config).

## Owned observation format and cutover

The owned multi-operation path uses `OwnedProxyPublicationV2` and the canonical
`RegisteredProxyAttestationV2`, not an untagged HTTPS observation. The explicit
transport kind is `pinned_https.v1` or `local_loopback_http.v1`; mixed or unknown
variants deny. HTTPS retains its actual DNS, pinned address and TLS facts.
Local-loopback data has no DNS/TLS fields and cannot authorize HTTPS, cloud,
MCP or search. Its actual publication remains disabled until the honest local
allocation successor and retained connector producer are installed.

Construction is acyclic: freeze the complete canonical operation first, observe
the actual original connection for that operation, register the observation,
then reserve the operation with that exact registered digest. Operation fields
and deadlines cannot be reconstructed from a later registration or changed
after observation. Registration, reserve, before-send and custody/history all
correlate the same original sequence, whole operation digest, source and
allocation. The operation itself does not refer back to the observation.

Each publication retry authenticates the complete original producer proof at
the transaction's authoritative time, without network, filesystem or clock I/O.
First publication additionally requires the producer's retained connection and
live original lifetime. Historical signature authentication may survive socket
close for exact replay/custody, but does not imply readiness or mint send
permission. Replay never renews time, reconnects, reconstructs a socket or debits
another operation. The connection owner closes abandoned/failed preflights;
actual invocation still consumes the one-use connection/capability before bytes.

V2 is a serving-format change, not a schema migration or live deployment:

1. Under a separately admitted rollout, stop new owned-agent admissions and
   quiesce every old observation writer. Keep the old exact execution/cleanup
   owners available to finish already admitted work within its original bounds.
2. Inventory the exact approved profile/tenant/Attempt scope, including first
   observations, per-sequence observations, operation reservations, accepted
   unknown outcomes and retained custody. Prove old live work and its cleanup
   drained; a generic harness cutover marker is not this proof. Missing state
   never permits blind resend or fabricated completion.
3. Keep historical V1 rows unchanged. Resolve/reconcile remaining old work under
   its original owner before enabling V2. An offline pinned old artifact may be
   used only under the admitted read/reconciliation scope; it cannot be an old
   live writer, a new-send fallback or an automatic V1-to-V2 converter. V2 serving
   readers reject unsupported V1 state, including historical recovery/custody;
   they do not silently discard it or authorize cleanup from a partial decode.
4. With old writers stopped and the drain verified, install the V2-capable
   producer/writer before enabling the migrated readers/consumers. Keep profiles
   disabled until publication, current readback, reserve, before-send, history
   and residual custody all use the same reviewed V2 artifact. Do not overlap
   active V1 and V2 operation owners or reinterpret an old row by its table name.
5. Only the owning rollout may enable a bounded new profile. Record the actual
   writer/readers, scoped drain evidence and rollback/no-go decision there. If
   older binaries cannot read V2, rollback must drain/quarantine V2 work rather
   than re-enable blind V1 writers against it.

The existing first-observation columns and `attempt_operation_proxies_v1`
canonical String records are sufficient. No applied migration, extra table,
endpoint registry, marker-as-authority or second operation ledger follows.

## Credential boundary

The boundary reuses `ports.ProviderResourceCredentialLifecycleV1`; it does not
read a durable secret reference or implement a second credential store. The
#85 service remains authoritative for active/revoked state, resource revision,
credential generation, secret fingerprint, rotation fencing, one-shot read,
file cleanup, and abandoned-candidate recovery.

The returned handle is treated as untrusted adapter output and exact-compared
with invocation authority before materialization. A mismatched handle is
released but never read. Delivery is exact:

- `file` passes only the validated private direct-child path; credential bytes
  are not passed to the proxy callback;
- `environment` passes the sealed canonical environment name and plaintext
  only inside the synchronous process-local callback;
- `direct` has no locator and passes plaintext only to the trusted transport;
- conversion or fallback between delivery kinds is denied.

The callback is guarded against repeated invocation, so a faulty materializer
cannot produce a second provider call. A callback already running when a
faulty adapter returns is joined before cleanup; a missing or late callback is
closed and denied. The lifecycle port requires synchronous, at-most-once use
without retaining the callback or bytes. Secret, payload, delivery locators,
and response copies are marked non-JSON; owned secret/payload copies are
cleared after the synchronous call. Release always uses a fresh cleanup
context capped at 30 seconds. Provider outcome and credential
finalization remain independent evidence: a successful provider response can
coexist with failed credential release, and the failure still blocks a clean
terminal result and instance reuse.

## Content-free evidence and stable failures

Public evidence contains only the enforced egress/proxy states, exact actual
route identifiers, bounded request/response byte counts, provider acceptance
class, observation time, and credential-finalization state. Requests,
credentials, provider responses, native errors, paths, and resolved addresses
are not representable in the public result or its string form.

Dependencies may return private diagnostic errors, but callers receive only
the stable package sentinels: invalid authority, denied policy, failed proxy
attestation, unavailable credential, delivery mismatch, failed egress,
invalid evidence, or failed credential finalization. A provider/proxy error
does not relax route or byte validation, clears any partial response, and never
triggers another route. Successful evidence must carry a UTC observation
inside the exact invocation call and before the credential/attestation expiry.

## Verification surface

The deterministic package tests cover:

- exact private-data route success only under a fresh explicitly accepting
  policy, plus one-effect replay denial;
- no proxy, secret, or network effects for policy/digest/owner/data-class,
  endpoint, redirect, port, metadata/private-host, or payload drift;
- proxy artifact/identity, public resolution, pinned address set, DNS
  rebinding, redirect, ambient proxy, certificate, and expiry attestation;
- exact tenant/owner/run/attempt/worker/lease/resource revision/generation and
  handle expiry;
- all three delivery kinds, no conversion, and malicious callback replay;
- missing, late, and already-running callback behavior under cancellation;
- actual-route, acceptance, timestamp, request/response-count, and response
  overflow tampering;
- attestation expiry between preflight and secret read;
- authority expiry during proxy preflight, before credential issue;
- caller cancellation during an in-flight proxy call and independent release,
  including release failure;
- JSON and string marker scans for payload, secret, delivery path, and provider
  response.

Boundary tests use fake in-memory endpoints and credentials; concrete transport
tests additionally use owned synthetic loopback TLS peers. No real provider key,
external provider request, cloud credential or production route is required.

## Still disabled

The boundary and concrete transport remain feature-disabled prerequisites.
Enabling the first direct tool-free cloud profile still requires reviewed
proxy registration and encrypted secret composition, infrastructure-enforced proxy-only egress,
cloud-dev DNS/private-address probes, warm-instance residue tests, exact image
promotion, two-owner E2E, and the PR-03d through PR-03f rollout gates in
[serverless-harness.md](serverless-harness.md).
