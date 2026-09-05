# Serverless provider egress and invocation credentials

Issue #89 adds the feature-disabled boundary between one authenticated
`PreparedInvocation`, the #85 provider-credential lifecycle, and one attested
provider proxy. It does not register a concrete proxy, secret backend, provider
endpoint, or production route.

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

Tests use only fake in-memory endpoints and credentials. No provider key,
provider request, cloud credential, or production network route is required.

## Still disabled

This package is a composition contract, not a production transport. Enabling
the first direct tool-free cloud profile still requires a reviewed concrete
proxy and encrypted secret adapter, infrastructure-enforced proxy-only egress,
cloud-dev DNS/private-address probes, warm-instance residue tests, exact image
promotion, two-owner E2E, and the PR-03d through PR-03f rollout gates in
[serverless-harness.md](serverless-harness.md).
