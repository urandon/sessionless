# Sessionless-owned serverless harness runtime

Research date: **2026-08-26**

Tracks: [#86](https://gitcode.com/urandon/sessionless/issues/86), provider epic
[#13](https://gitcode.com/urandon/sessionless/issues/13), and
[AI resources and federation](research/ai-resources-and-federation.md)

Status: design candidate; no provider backend, credential, network route, or
production execution profile is enabled by this document

## Decision

Sessionless owns one versioned outer harness above a closed registry of exact
backend implementations. The outer harness is not Codex, OpenCode, Pi, or an
OpenAI-compatible API client. Those are independently attested backends below
the same Sessionless lifecycle and evidence boundary.

The first production **target substrate** is a Yandex Serverless Containers
HTTP-server revision invoked by a Yandex Message Queue trigger with batch size
one and container concurrency one. This reuses the existing cloud runtime and
keeps queue redelivery outside provider selection. It is a conditional target,
not a claim that an ordinary serverless container is a sufficient sandbox.
PR-03d must prove the gates in this document before the profile can be enabled.

The first enableable cloud profile is deliberately smaller than the backend
portfolio:

- one tool-free Sessionless direct API/router turn;
- immutable public or externally-shareable input only, subject to the exact
  accepted provider policy;
- no shell, MCP server, plugin, user-installed dependency, native session
  resume, or arbitrary model-generated code execution;
- deny-by-default egress through one attested provider proxy;
- fresh invocation state for every attempt, with a fresh child process for
  child-process profiles, bounded output, and mandatory cleanup evidence.

Codex, OpenCode, and Pi remain feature-disabled on this cloud substrate until
the same spike proves descendant cleanup, warm-instance erasure, exact egress,
and credential custody for their native processes. They may not inherit a
weaker profile because they passed provider protocol conformance. AW-05 attached
workers retain their own stricter isolation contract and are not a fallback.

If Yandex cannot prove the per-invocation isolation and cleanup gates, the
decision automatically becomes **no-go**, not a degraded launch. The first
migration candidates are a run-to-completion job substrate or a managed
microVM/session substrate behind the same proposed port. No provider, backend,
or substrate fallback occurs inside an attempt.

## Authority boundary

```text
canonical Sessionless authority
  Session / Run / Attempt / Lease / fence / cancellation / terminal commit
  HarnessBindingV1 / provider resource generation / policy evidence
  ExecutionPlacementV2 + sealed substrate profile digest
                         |
                         v
disposable serverless invocation
  verify -> materialize -> supervise exactly one registered backend
         -> emit bounded events and evidence -> erase -> stop
                         |
                         v
non-authoritative native facts
  cloud invocation ID / PID / provider request ID / native session / raw frames
```

The control plane and existing stores remain authoritative for:

- membership, owner scope, admission, quota, route, and execution placement;
- `Run`, `Attempt`, lease ownership and fence, cancellation, and retry lineage;
- canonical context, checkpoints, tool decisions, artifacts, and terminal
  commit;
- provider resource revision, credential generation, policy freshness, and
  public-safe audit.

The serverless harness owns only the bounded lifetime of one admitted
invocation: local preparation, backend translation, process supervision,
network enforcement, cleanup, and evidence collection. A substrate invocation
ID is a diagnostic correlation value. It can never reclaim a lease, authorize
a retry, resume a conversation, select another backend, or commit a terminal.

## Existing contracts reused unchanged

The design composes existing contracts instead of copying them:

- `domain.HarnessBindingV1` seals tenant, owner, run, attempt, backend
  descriptor, provider resource revision and credential generation, model and
  data class, evidence digests, placement digest, and evidence expiry;
- `sessionlessharness.Registry` is the exact no-default/no-discovery backend
  registry and already separates `Preflight`, `Execute`, and expiry-tolerant
  `Cancel`;
- `ports.HarnessDriver`, `ExecutionRequest`, `ExecutionIdentity`, and
  `ProviderExecutionEvidenceV1` remain the backend-facing boundary;
- the #84 conformance contract proves registry and provider evidence behavior;
  a fixture backend cannot claim that a native adapter protocol passed;
- the #85 provider-credential authority owns owner/resource revision,
  generation, candidate recovery, revoke/rotation fencing, and one-shot
  `file|environment|direct` materialization;
- AW-04/AW-05 keep cancellation request, acknowledgement, process stop,
  cleanup, terminal evidence, and canonical terminal commit separate;
- #82 is the only public UX vocabulary. Substrate-native logs and states do not
  add a second read model.

## Proposed provider-neutral runtime contract

These are design-level V1 shapes for PR-03a. Field names are illustrative; the
implementation must use one canonical encoding and must not add compatibility
aliases or zero-value defaults.

```text
SubstrateBindingV1 {
  version
  kind: yandex_serverless_container | cloud_run_job |
        azure_dynamic_session | managed_sandbox
  profile_id, profile_revision, profile_digest, profile_evidence_expires_at
  region, image_digest, outer_harness_artifact_digest
  workload_mode: child_process | in_process_direct
  isolation_profile_digest, egress_policy_digest, cleanup_policy_digest
  egress_proxy_artifact_digest, egress_proxy_identity_digest
  admission_cost_ceiling_digest
  limits {
    invocation_timeout, execution_timeout, cleanup_timeout
    cpu_millis, memory_bytes, scratch_bytes
    stdout_bytes, stderr_bytes, native_event_count, artifact_bytes
  }
}

ServerlessInvocationAuthorityV1 {
  version
  harness_binding: HarnessBindingV1
  substrate_binding: SubstrateBindingV1
  admission_cost_ceiling: AdmissionCostCeilingV1
  lease: existing domain.Lease
  context_manifest_digest, input_manifest_digest
  invocation_deadline
}

SubstrateExecutionEvidenceV1 {
  version
  invocation_authority_digest, substrate_binding_digest
  admission_cost_ceiling_digest
  prepared_invocation_digest, effect_reservation_digest
  prepared_allocation_digest, physical_invocation_claim_id
  allocation: unknown | started | rejected
  process: not_applicable | not_started | running | stopped | stop_unknown
  credential_finalization: not_required | verified | failed | unknown
  cleanup: not_required | verified | failed | unknown
  egress: not_attempted | policy_enforced | denied | unknown
  image_attestation: verified | mismatch | unknown
  backend_attestation: verified | mismatch | unknown
  proxy_attestation: not_required | verified | mismatch | unknown
  cancellation {
    request: none | observed
    backend_signal: not_required | not_sent | sent | acknowledged | unknown
  }
  provider_evidence?: ProviderExecutionEvidenceV1
  resource_observations[] {
    kind: cpu_time | memory_peak | scratch_peak | ingress_bytes |
          egress_bytes | log_bytes | evidence_bytes
    state: unknown | observed
    quantity?, unit?, provenance, observed_at?
  }
  failure_code: none | authority_denied | profile_disabled |
                profile_expired | attestation_mismatch | lease_lost |
                cancelled | process_failed | output_bound_exceeded |
                egress_denied | provider_failed | accepted_outcome_unknown |
                credential_finalize_failed | cleanup_failed | backend_failed
  evidence_digest
}

PreparedAllocationV1 {
  exact substrate registry descriptor
  observed image and outer-harness digests
  workload_attestation {
    child_process { inner executable digest, exact argv, native protocol,
                    backend profile digest }
    | in_process_direct { linked backend profile digest }
  }
  observed proxy artifact and workload-identity digests
}

AttemptEffectReservationV1 {
  version
  existing attempt_id, lease_id, fence
  physical_invocation_claim_id
  effect_sequence, harness_binding_digest, substrate_binding_digest
  admission_cost_ceiling_digest
  kind: provider_turn
  upstream_idempotency_key_digest?
  reserved_at
}

opaque internal PreparedInvocation capability
  MAC over invocation_authority_digest
           effect_reservation_digest, physical_invocation_claim_id
           prepared_allocation_digest
           admission_cost_ceiling_digest
           issued_at, execute_deadline
```

`ServerlessInvocationAuthorityV1` is constructed only after the canonical
lease is claimed. It references the already sealed `HarnessBindingV1`; it does
not repeat or override model, route, provider, credential, or placement fields.
The existing `Lease` supplies the exact run, attempt, worker, fence, acquisition,
and expiry authority; those fields are exact-compared with the binding rather
than copied into another authority.

The previous `ExecutionPlacementV1` could distinguish only `managed` from one
exact attached worker; its managed variant could not seal a substrate profile.
PR-03a therefore replaces it after an empty-backlog cutover with one canonical
shape for both kinds:

```text
ManagedExecutionPlacementV2 {
  version: 2
  kind: managed
  fallback_policy: deny
  substrate_binding_digest
}
```

The full `SubstrateBindingV1` is server-owned and persisted with the dispatch;
its digest is included in the placement, and `HarnessBindingV1` already seals
the placement digest. The exact `AdmissionCostCeilingV1` is persisted in the
dispatch and copied unchanged into `WorkerJob`; its digest is sealed by the
substrate binding and therefore by placement and harness authority. The
implementation must choose one canonical persisted
shape and update the harness binding/version if the existing digest contract
cannot encode it without ambiguity. It must use an explicit empty-backlog
cutover marker, writer-first rollout, and no zero-value or `managed` default.
There is no V1 alias, reader fallback, or zero-value managed default. A mismatch
between the runtime profile and sealed placement fails before workspace, secret,
process, or network effects.

The current managed worker lease prevents a different worker from claiming an
active attempt, but an exact lease replay by the same warm worker is not by
itself proof that a provider turn was never sent. Before the first provider
request byte, the store must append one `AttemptEffectReservationV1` under the
canonical attempt/lease/fence transaction. Presence of that reservation makes
every duplicate delivery reconcile-only; it can never call the provider again.
The record is a one-way effect fence in the existing attempt authority, not a
second lifecycle or retry state machine. A crash after reservation but before
send may conservatively produce an ambiguous attempt. Safety wins over an
automatic duplicate call. A provider idempotency key may improve reconciliation
only when the exact provider contract proves its semantics.

The prepared invocation is a short-lived, non-durable, non-JSON capability
with an unexported process-local authenticator. It is issued
only after exact substrate and harness preflight, the final authority gate, the
atomic effect-reservation append, and exact `PreparedAllocationV1` attestation.
It binds the winning physical claim to the stored reservation and observed
allocation. It is not a lifecycle record and cannot be reconstructed from
caller input or a queue delivery, and no public decoder or constructor exists.

The proposed substrate port is deliberately smaller than a general remote
shell:

```text
ExecutionSubstrateV1.Preflight(ctx, authority) -> verified profile | stable error
ExecutionSubstrateV1.Execute(ctx, prepared_invocation, SessionlessHarnessV1) -> evidence
ExecutionSubstrateV1.Cancel(ctx, exact attempt/lease/fence) -> observation
ExecutionSubstrateV1.Reconcile(ctx, exact attempt/lease/fence) -> observation
```

`Execute` exact-validates the capability against the current authority,
reservation, physical claim and allocation before any effect; it cannot choose
a backend or provider. The returned evidence exact-compares all three digests
and the claim ID. `Cancel` stays routable after
provider-policy evidence expires, but its exact attempt, lease, fence,
substrate, and artifact bindings must still validate. `Reconcile` observes an
already bound invocation; it never starts work.

`SubstrateRegistryV1` is a second closed exact-match registry beside the
backend registry, not a scheduler. It resolves the complete
kind/profile/revision/image/outer-harness/isolation/egress/proxy/cleanup tuple,
has no default or nearest match, and rejects disabled or expired profiles for
new `Execute`. Immutable tombstoned registrations retain only enough trusted
adapter metadata for exact `Cancel` and `Reconcile` after disablement or
expiry. They cannot start work.

`SubstrateExecutionEvidenceV1.ValidateForAuthority` exact-compares both binding
digests and enforces a closed compatibility matrix:

- rejected/mismatched authority or attestation cannot have a running process,
  network attempt, provider evidence, or verified cleanup of started work;
- `credential_finalization=verified|not_required` is an independent fact;
  provider, process and credential failures may coexist as sorted secondary
  reason codes without overwriting one another;
- `cleanup=verified` requires `process=stopped|not_applicable` and
  `credential_finalization=verified|not_required`; absence of a failure code is
  never evidence that credential finalization succeeded;
- completed provider evidence requires compatible accepted policy, exact route
  evidence when policy requires it, `egress=policy_enforced`, verified proxy,
  and verified image/backend attestation;
- `accepted_outcome_unknown` cannot be represented as pre-acceptance or as a
  completed canonical result;
- observed resource quantities require exact units, provenance and time;
  `unknown` has no numeric value;
- every failure code has one allowed allocation/process/cleanup/provider
  combination; backend errors are sanitized before the result is returned.

Canonical terminal commit is intentionally not a field in substrate evidence.
The #82 read model joins it from the canonical attempt/finalization authority;
the runtime cannot assert it about itself.

## Selected request path

```text
DispatchOutbox -> YMQ dispatch message
  -> YMQ trigger, batch=1
  -> private IAM-authenticated Yandex container HTTP invocation
  -> load WorkerJob and exact bindings from YDB
  -> claim canonical lease/fence
  -> construct ServerlessInvocationAuthorityV1
  -> substrate registry preflight, then harness registry preflight
  -> final authority/freshness/cancel/profile gate and atomically append
     the canonical provider-effect reservation
  -> attest PreparedAllocationV1 and issue the non-durable PreparedInvocationV1
  -> only the exact reservation owner materializes immutable context/workspace
  -> repeat the final authority/freshness/cancel/attestation gate
  -> invocation credential issue/materialize
  -> recheck workload/proxy attestation immediately before effect
  -> one attested backend child process or one sealed in-process direct loop
  -> bounded evidence and output materialization
  -> stop proof, output upload, credential finalization/release
  -> typed workspace erase and no-residue verification
  -> canonical terminal transaction
  -> HTTP 2xx only after local delivery is canonically ack-safe
```

The trigger body is delivery evidence, not execution authority. Every useful
field is reloaded under owner scope from canonical stores. The current
`worker-runtime` already adapts a YMQ trigger body into a bounded queue and
processes one message without long-polling inside a billed request.

The first final gate and effect-reservation append share the canonical attempt
transaction. A fresh physical invocation claim ID identifies the winner; a
lost response can be retried only by that still-running claimant, while a new
container receives a different ID and becomes reconcile-only. The gate repeats
after potentially slow materialization and immediately before secret read,
process spawn, and network send. It re-reads the current attempt, lease/fence,
cancellation, substrate registration/profile freshness, provider resource
revision/generation/revoke state, policy/evidence expiry, image/workload
attestation, trusted proxy artifact/identity, and the exact cost-ceiling digest.
Before the claim it also requires fresh price evidence and the admitted queue
delivery ceiling. YMQ `ApproximateReceiveCount` is diagnostic evidence only and
never enters an authority digest; enforcement requires server-owned durable
delivery accounting or exact queue-policy attestation. Until that proof exists,
an unknown or exceeded ceiling fails closed. The prepared capability and
returned evidence exact-compare that same digest. Any change releases local
materialization and stops before the next effect. A configuration digest alone
is not proxy attestation.

YMQ redelivery is expected. A duplicate delivery must either lose the lease,
observe a committed/terminal attempt, or reconcile the exact current
invocation and its effect reservation. It cannot cause two provider calls.
Container concurrency one is a defence in depth; the lease/fence and durable
effect reservation are the cross-instance authority. The current worker
runtime does not yet persist this provider-effect fence, so no production
provider profile can be enabled by configuration alone.

The existing cloud profile also uses a common `WORKER_ID=serverless-worker`, a
two-minute lease, and a 15-minute YMQ visibility window. Long provider work
therefore requires a PR-03a worker refactor, not a larger documentation timeout:

- before credential issue, the lease must already cover the admitted execution
  plus finalization horizon required by the credential contract;
- an independent watchdog renews the lease and reads cancellation throughout
  silent provider calls, output upload, credential finalization, and cleanup;
- `lease.expires_at >= credential.expires_at >= execution_deadline +
  cleanup_budget`; the serverless lease horizon therefore cannot use the
  current two-minute configuration for a 40-minute execution;
- renewal cadence is at most the lesser of one third of the lease horizon and
  one minute, while cancellation polling has its own shorter measured bound;
- the current 15-minute visibility window is shorter than the maximum
  invocation and may expire more than once; it is deliberately not the runtime
  lock. Bounded redelivery cost and safety come from the delivery limit,
  lease/fence, and effect reservation;
- losing renewal or observing a different fence immediately cancels transport,
  stops the process, blocks terminal commit, and records `lease_lost` with
  process/cleanup facts separately;
- `execution_timeout + cleanup_timeout + control_margin <=
  platform_request_limit - hard_platform_guard`; the initial hard guard is five
  minutes and is never allocated to work, cleanup, or ordinary response time.

PR-03d must redeliver the same message during a silent long-running backend and
prove that the effect reservation admits one provider send, the original lease
remains renewable, and every duplicate is reconcile-only.

The initial production policy reserves the one-hour Yandex request limit as:

- at most 40 minutes for materialization and backend execution;
- at most 5 minutes for cancellation, process-tree stop, credential
  finalization, and cleanup;
- at least 10 minutes for cold-start and control-plane/response safety margin.
- at least 5 additional minutes remain unallocated as a hard platform guard.

PR-03d must measure and then reduce these values if the tail does not fit. The
two-minute lease does not pass this profile. PR-03d now keeps independent
polling active across scratch and credential preparation, materialization,
silent harness execution, credential finalization, output and canonical-event
upload, and scratch cleanup. Production promotion still requires the PR-03b
process supervisor and PR-03c transport to consume that cancellation signal
and emit verified stop/cleanup evidence. A request timeout is never reported as
process stopped or cleanup verified.

Yandex's broker acknowledges the queue delivery because the container returned
HTTP 2xx. The in-memory trigger adapter's `Ack` only marks that local delivery
safe. Therefore the server returns 2xx after canonical commit/reconciliation
makes acknowledgement safe; a lost HTTP response remains a normal redelivery.
It never claims that a remote broker acknowledgement preceded the response.

## Lifecycle and replay matrix

| Observation at interruption | Durable action | Same-attempt automatic execution | Later action |
|---|---|---|---|
| Delivery duplicated before lease claim | One claimant wins; other delivery reconciles/acks | No second execution | Return duplicate evidence. |
| Binding/profile/policy/image/protocol mismatch before effects | Stable terminal configuration failure | No | Correct authority and create a new admitted attempt. |
| Failure before effect reservation commits; no workspace/secret/process/network effect | Release local allocation; preserve exact delivery | Yes, bounded queue redelivery may retry the same attempt | Re-run all freshness checks and race for the reservation. |
| Workspace or credential materialized after reservation but provider request provably not sent | Finalize/release credential and erase workspace | **No** new physical invocation | The live reservation owner may continue only after exact rechecks; otherwise reconcile/new attempt. |
| Provider-effect reservation exists, even if send was not observed | Treat as possible external effect; reconcile only | **No** | Exact provider idempotency/reconcile or a reviewed new attempt. |
| Provider request bytes may have been sent but acceptance is unknown | Persist `accepted_outcome_unknown`; fence late result | **No** | Reconcile provider request if an exact safe API exists, otherwise operator/user decision and a new attempt. |
| Provider explicitly accepted; no terminal observed | Persist accepted-unknown evidence | **No** | Reconcile or cancel; never duplicate the call. |
| Provider terminal observed; process or cleanup failed | Preserve terminal candidate and teardown failure separately | **No** | Finish cleanup/reconcile; canonical success waits for all required gates. |
| Canonical terminal committed; HTTP response lost | Redelivery reads terminal and acks | No | Return duplicate/replayed outcome. |
| Cancellation requested before provider send | Stop preparation, release, erase | No provider call | Commit cancelled only after process/cleanup facts support it. |
| Cancellation races accepted provider work | Request cancel, stop process, fence late output | No | Keep requested/accepted/stopped/cleanup/terminal facts orthogonal. |
| Lease expires while old process is alive | Fence old owner; reject late events/terminal | No automatic provider replay | Reconcile ambiguity before any new attempt. |

`pre_acceptance` is retry-safe only when the trusted transport can prove that
no request bytes or effect crossed the boundary. Missing a native
`turn.started` event is not such proof. A socket write followed by timeout is
accepted-unknown. Provider retry headers, CLI retry loops, trigger retries, and
SDK defaults are disabled or set to zero; Sessionless owns any later attempt.

### Cancellation delivery

The same watchdog that renews the lease reads canonical cancellation at a
bounded interval even when a provider produces no event. A revisioned cancel
observation is bound to the exact attempt/lease/fence and delivered to the
active direct transport or child-process supervisor. It closes request bodies
and sockets, sends the reviewed graceful signal, escalates to process-group and
external-boundary kill, and waits only the cleanup budget. A separate
cancel-trigger/reconciler may accelerate this path, but it addresses only an
already reserved exact invocation and cannot allocate or reroute work.

`cancellation requested`, `backend signal sent/acknowledged`, `process stopped`,
`cleanup verified`, provider terminal, and canonical terminal remain separate
facts. Context cancellation or a successful cancel API response alone proves
none of the later facts. A stale/foreign allocation lookup is denied and the
canonical fence still rejects late events.

## Isolation and warm reuse

Yandex's service-level container boundary is a deployment isolation layer, but
this design does not infer per-invocation erasure, descendant death, syscall
policy, or egress control from the product name. PR-03b/PR-03d must prove all
of the following for the exact image and runtime revision:

- digest-pinned image, outer harness, backend artifact, argv/protocol, and
  supervisor profile;
- one fresh, mode-restricted, no-follow workspace rooted below a dedicated
  scratch directory;
- a child environment built from an allowlist, with no inherited provider,
  queue, YDB, Object Storage, IAM, or operator credentials;
- process-group and external workload stop, bounded reader shutdown, and no
  surviving descendants after normal exit, cancellation, timeout, leader
  loss, and cleanup failure;
- enforced CPU, memory, scratch, output, event-count, file-count, and wall-time
  limits;
- no cross-attempt files, environment, sockets, background processes, caches,
  native sessions, or credential material on a warm reused instance;
- fail-closed instance termination after any unverified cleanup. A tainted
  instance is never returned to the warm pool.

The first profile does not execute untrusted model-generated code. Ordinary
container isolation plus exact trusted code is not promoted to a general tool
sandbox. A future shell/tool profile needs an independently reviewed stronger
launcher, such as a managed per-session microVM or an equivalent measured
boundary. AW-05's `NetworkDenied=true` profile is not weakened or relabelled to
make cloud provider egress appear compatible.

The current `worker.Manager` does not meet the cleanup ordering above: its
invocation directory is removed by a best-effort `defer` after terminal/queue
handling and the removal error is ignored. PR-03a/PR-03b must replace that path
with a typed finalization coordinator. It stops the workload, uploads allowed
outputs, finalizes/releases credentials, removes and scans the workspace, and
only then permits canonical success and trigger acknowledgement. A cleanup
failure is committed as an independent failure/ambiguity fact, taints and
terminates the warm instance, and can never be converted into `verified`.

The feature-disabled PR-03b implementation contract and its inherited AW-05
boundary are documented in
[serverless-isolation.md](serverless-isolation.md).

`PreparedAllocationV1` separates outer substrate allocation from workload
attestation. A child-process profile exact-compares the inner executable,
artifact digest, argv, native protocol and backend profile immediately before
spawn, following the #81/AW-05 boundary. An `in_process_direct` profile has no
inner executable and must attest the outer image/binary plus the linked direct
backend profile; `process=not_applicable` is then the only legal process fact.
These modes are distinct registry entries and cannot substitute for each other.

### Snapshots and warm pools

Only immutable startup accelerators are permitted:

- a reviewed container image or filesystem template keyed by image, outer
  harness, backend artifact, native protocol, and isolation-profile digests;
- no tenant workspace, prompt, result, provider native session, process memory,
  credential, bearer, queue receipt, lease, or fence;
- snapshot creation happens in a credential-free build pipeline;
- resume always performs the full authority and freshness preflight.

Memory snapshots, paused sandboxes, forks, and persistent warm workspaces are
not product memory and are disabled for the first profile. A vendor snapshot ID
cannot be stored as canonical retry authority. This intentionally declines the
persistent-session lifecycle offered by E2B, Daytona, Modal, and GitHub cloud
sandboxes.

## Egress model

The implemented, still feature-disabled PR-03c composition contract and its
effect ordering are specified in [serverless-egress.md](serverless-egress.md).

All profiles start with network denied. An admitted provider route adds one
exact egress policy, not general internet access:

1. the sealed provider route identifies transport kind, transport provider,
   upstream provider and endpoint policy;
2. a trusted proxy permits only the reviewed TLS origin, method, port, request
   size, response size, redirect policy, and DNS/IP class;
3. link-local metadata, YDB, queue, Object Storage, Lockbox/KMS, control-plane,
   localhost escape, private RFC1918 ranges, and arbitrary DNS are denied to
   the backend process;
4. redirects, proxies from ambient environment, alternate ports, WebSockets,
   non-HTTP protocols, and certificate bypass fail closed unless separately
   admitted;
5. the proxy records only content-free route, byte-count, decision, and timing
   evidence; provider bodies and credentials never enter public logs.

For a direct API backend, the preferred boundary keeps the provider credential
in the trusted proxy/transport and never exposes it to the model-facing loop.
A CLI backend that requires a file or environment credential is a distinct
profile. VM/container isolation protects the host and other tenants but does
not protect a secret from model-controlled shell/tool descendants running with
the same UID, environment, or read roots. Therefore `file|environment`
delivery is incompatible with a shell/tool-enabled cloud CLI profile. Such a
profile remains no-go until either the credential is moved to a trusted
proxy/privilege-separated reader or an externally enforced sandbox proves that
every model-controlled descendant cannot read, inherit, or exfiltrate it.
Backend self-report, prompt rules, and ordinary file modes are not proof.

The Yandex target is enabled only if PR-03d proves that the container has no
unmediated internet route and reaches the provider solely through the private
proxy. VPC attachment and a missing public NAT are hypotheses until tested;
configuration screenshots or a successful denied request are insufficient
without positive/negative route probes and infrastructure-state evidence.

OpenRouter use is eligible for synthetic fixtures, public research, and
reviewed public open-source development. It is not limited to synthetic data.
Private input remains no-go unless a fresh accepted policy explicitly permits
the exact data class, router workspace, upstream route, retention/training
posture, and credential owner. Ox Alpha being free changes neither data policy
nor routing authority.

## Credential flow

No provider key is accepted during this design iteration. The production flow
must compose #85 and a reviewed tenant-scoped encrypted secret backend:

1. dispatch contains only the opaque provider resource and sealed generation,
   never a secret reference or value;
2. runtime loads the owner-scoped binding and runs substrate, harness, resource,
   route, data-policy, evidence-expiry, image, protocol, and delivery-plan
   preflight before a secret read;
3. the provider credential service fences the exact resource revision and
   generation, then issues one invocation handle;
4. a Yandex workload identity may access only the credential service/secret
   namespace required by the server, not expose a general Lockbox token to the
   backend;
5. `direct`, `environment`, or `file` delivery follows the exact backend
   descriptor. There is no conversion or fallback;
6. finalization, release, rotation/revoke fencing, abandoned-candidate cleanup,
   pinned-file crash recovery, and zeroization use #85 authority;
7. cleanup failure is a distinct terminal fact and blocks instance reuse.

Lockbox's environment-secret feature is not used for per-attempt provider
credentials: it is process-wide, contributes to Yandex's 4 KB environment
limit, and would bypass the #85 generation and cleanup lifecycle. KMS/Lockbox
stores encrypted authority; the invocation service owns the one-shot plaintext
boundary.

## Evidence and public observability

Private runtime evidence may retain bounded encrypted native frames only for a
declared incident/reconciliation window. Public and ordinary operational DTOs
contain no prompt, result, provider body/error, credential, path, stdout,
stderr, raw frame, queue receipt, provider account locator, or tenant-existence
oracle.

The substrate reports independent facts rather than a combined health state:

- authority/preflight accepted or denied;
- allocation started or unknown;
- provider acceptance class and finish class;
- cancellation requested and backend cancel observation;
- process stopped and cleanup verified;
- image/backend attestation;
- egress policy decision and actual-route evidence state;
- usage provenance and cloud resource observations;
- canonical terminal committed, projected only from canonical stores.

#82 remains the vocabulary and separation rule, but a serverless attempt is not
an attached worker. It must not appear in `/v1/attached-workers`, acquire a
worker identity, or synthesize online/offline, heartbeat, daemon, or
connectivity facts. A future owner-scoped attempt/profile read model projects
only applicable evidence as follows:

| #82 cohort | Serverless projection | Authority and freshness |
|---|---|---|
| identity | Canonical attempt locator plus sealed substrate profile name/revision. | Attempt store and immutable placement/profile binding; owner is taken from authenticated scope, not a request field. |
| readiness | Profile enabled/fresh, image/backend/proxy attestation, and isolation support. | Exact registry/profile evidence and attestation times; expiry remains separate from current state. |
| connectivity | **Omitted as inapplicable.** No fake worker, connection, heartbeat, online, offline, or last-contact row. | None. Omission is not `unknown` or `offline`. |
| eligibility | Canonical admission decision, input-data policy, capacity/quota observation, and admitted cost ceiling. | Scheduler/admission authority with evaluated-at, input digests, price expiry, and quota freshness; never reusable as a grant. |
| execution | Provider acceptance/finish, cancellation request and backend acknowledgement, process stop, credential finalization, cleanup, resource observations, and canonical terminal as independent rows. | Substrate evidence for observations, joined with canonical attempt/cancellation/finalization authority; substrate evidence never asserts terminal commit. |
| governance | Policy verdict, substrate profile revision, region/residency evidence, rollout gate, revoke/disable, and rollback availability. | Versioned policy/profile/deployment authorities with independent freshness and acknowledgement. |

Private `SubstrateExecutionEvidenceV1.failure_code` values are not automatically
public reason codes. A projection may use an existing #82 stable code only when
its documented meaning and authority are exact; otherwise an explicitly
reviewed versioned addition is required before exposure. Physical claim IDs,
digests, queue receipts, billing references, provider account locators, raw
errors and native evidence remain private. Inapplicable facts are omitted;
missing applicable evidence is `unknown`, never inferred.

The only control is the existing canonical attempt cancellation path. The
substrate does not publish a second cancel/retry/reconcile action API. Every
attempt, profile, diagnostics, or operation lookup returns the same public
`not_found` envelope, status and timing class for missing and foreign-owner
objects. No projection may infer `healthy`, `safe`, `erased`, `stopped`, or
`retryable` from a missing heartbeat or cloud invocation status.

## Substrate and competitor comparison

All rows below are **documented vendor behavior**, not Sessionless measurements,
unless explicitly marked. Current Yandex pages were rechecked on 2026-09-05;
other pages were checked on 2026-08-26; preview
surfaces can change. Cold-start values are unknown until PR-03d runs the exact
image and region.

| Surface | Lifecycle and isolation | Persistence/warm state | Network, credentials, evidence | Runtime and cost facts | Sessionless conclusion |
|---|---|---|---|---|---|
| [OpenAI Codex safety model](https://openai.com/index/running-codex-safely/) | Sandboxing and approvals are separate; managed config constrains write/network/protected paths. | Not a general substrate contract in the cited article. | Managed allow/deny/approval network policy, OS keyring credentials, workspace pinning, OTel and compliance logs. | Product deployment details and comparable sandbox price are not published there. | Reuse policy separation and agent-aware evidence; do not adopt Codex logs or approval state as Sessionless authority. |
| [GitHub Copilot cloud sandbox](https://docs.github.com/en/copilot/concepts/about-cloud-and-local-sandboxes) | Public preview; isolated ephemeral Linux on Azure Container Apps Sandboxes. Cloud mode is currently interactive, not programmatic `-p/-i`. | Active/stopped/deleted; stop snapshots files, environment, and in-progress work. | GitHub supplies identity, policy and billing. | $0.000024/compute-second, $0.000003/GiB-second, $0.005/GiB-month snapshot storage in the cited page. | Strong comparator, but its persistent user-session model and interactive-only surface are not our attempt contract. |
| [Yandex Serverless Containers](https://yandex.cloud/en/docs/serverless-containers/concepts/limits) | HTTP or command container; current control plane already uses private IAM invocation and YMQ triggers. Per-invocation isolation/cleanup remains unmeasured. | Provisioned instances reduce cold starts; warm reuse semantics relevant to erasure are unproven. | VPC and Lockbox exist, but exact deny-by-default provider proxy and one-shot credentials require our design. | Max request including init 1h; >10m requires long-lived container; 8 GB RAM, 10 GB ephemeral disk, 512 MB temp files, 3.5 MB HTTP request/response, 4 KB env, 10 GB image. [Pricing](https://yandex.cloud/en/docs/serverless-containers/pricing) is 100 ms metered plus invocations and egress. | **Conditional first target** because it matches the current cloud. No-go unless PR-03d proves all gates. |
| [Cloud Run Jobs](https://cloud.google.com/run/docs/create-jobs) | Run-to-completion job/task; 1-10,000 independent tasks; no listener. | Image startup; no product session persistence implied. | Service identity, secrets, VPC and logs are platform primitives. | Default 10m task timeout, max 168h; task retry default 3, configurable 0-10. Jobs have a one-minute minimum billable instance time in [pricing](https://cloud.google.com/run/pricing). | Best conventional job fallback. Must set retry zero and preserve Sessionless lease/fence authority. Multi-cloud cost is the main objection. |
| [Azure Container Apps Dynamic Sessions](https://learn.microsoft.com/en-us/azure/container-apps/sessions) | Prewarmed session pools with Hyper-V isolation and millisecond allocation; custom containers supported. | Ephemeral session with pool/cooldown lifecycle; all users of one session share its environment. | Optional network controls and platform lifecycle logs; session API owns allocation. | Exact price/limits are region/plan dependent and not measured here. | Strong managed-isolation fallback for untrusted tool profiles; introduces another cloud and a session lifecycle that must remain noncanonical. |
| [E2B](https://docs.e2b.dev/sandbox) | Sandboxes run up to 1h Base or 24h Pro; kill is explicit. | [Pause/resume](https://docs.e2b.dev/sandbox/persistence) preserves filesystem and optionally memory indefinitely; runtime window resets after resume. | Lifecycle events, metrics, secured access, and workload identity are exposed by the product. | [Pricing](https://e2b.dev/pricing) is per-second CPU/RAM/storage plus plan; current page lists 20 Hobby or 100 Pro concurrent sandboxes. | Useful microVM/persistence comparator. Indefinite paused state and memory resume are explicitly outside first-profile semantics. |
| [Modal Sandboxes](https://modal.com/docs/guide/sandboxes) | Explicit create/wait/terminate lifecycle, secure containers, default 5m and max 24h timeout in current docs. | [Snapshots](https://modal.com/docs/guide/sandbox-snapshots) include filesystem/directory and memory forms with different retention and restore constraints. | Resource limits, logs/metrics and region selection are exposed. | [Sandbox pricing](https://modal.com/docs/guide/sandbox-resources) is per second on max(requested, actual); current product page lists $0.00003942/core-second and $0.00000667/GiB-second. | Strong API and Go SDK comparator; Python-centered ecosystem and snapshot semantics are not production authority. |
| [Daytona](https://www.daytona.io/docs/en/sandboxes/) | Started/stopped/paused/archived/deleted container/VM lifecycles. | Persistent filesystem, snapshots, forks, and [warm pools](https://www.daytona.io/docs/en/warm-pools/) tied to exact resources. | Rich sandbox product and management API. | Pricing/cold start were not normalized in this iteration. Warm resources consume quota. | Valuable developer-environment comparator; too much persistent product lifecycle for the minimal attempt boundary. |
| [Cloudflare Sandbox](https://developers.cloudflare.com/sandbox/) | Current stable SDK plus 1.0 preview; sandbox IDs are Durable Objects and each sandbox runs in a VM-backed container. | Current stable container loses state after inactivity/restart unless kept alive; keep-alive requires explicit destruction. | [Outbound handlers](https://developers.cloudflare.com/sandbox/guides/outbound-traffic/) can deny internet, allow hosts and inject credentials; non-HTTP traffic needs separate denial. Preview/tunnel URLs need application auth. | Workers Paid plan; underlying Containers pricing/limits apply. Exact comparable attempt cost is not measured. | Excellent egress-proxy reference, but SDK preview/stability and JS/Worker command surface are additional supply-chain/protocol risks. |

### Agent-harness implementation sources

The following repositories are cloned only into the read-only research library;
they are not vendored dependencies. The exact revisions below make the completed
review repeatable. Their native session, permission, provider, or telemetry
state is competitor evidence, never Sessionless durable authority.

| Repository and reviewed revision | Implementation evidence | Sessionless conclusion |
|---|---|---|
| [OpenAI Codex `f5420174`](https://github.com/openai/codex/tree/f5420174dafba153913a3e697f89002c338dfd7e) | The official [`app-server` protocol](https://github.com/openai/codex/blob/f5420174dafba153913a3e697f89002c338dfd7e/codex-rs/app-server/README.md) separates thread/turn/item, exposes binary-version-specific generated schemas, uses a required initialize handshake, bounded queues, explicit interrupt, and stdio JSONL as the supported local transport. The same broad API also contains unsandboxed shell/process operations, while sandbox and approval settings remain Codex-owned turn configuration. | Keep Codex as one closed, method-allowlisted backend below the Sessionless harness registry. Pin binary artifact, protocol/schema and profile; never promote Codex thread IDs, approvals, persistence, model fallback, native retries, or telemetry into canonical attempt authority. |
| [Earendil Works Pi `6c4f3602`](https://github.com/earendil-works/pi/tree/6c4f360264397c59801f6da2bdac13e3b1fcbe91) | Pi exposes a multi-provider API/runtime and typed telemetry. Its [RPC surface](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/docs/rpc.md) is correlated JSONL with streaming events; prompt acknowledgement is acceptance, while later failure is an event. Its [security contract](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/docs/security.md) explicitly says project trust is not a sandbox. The [custom-provider surface](https://github.com/earendil-works/pi/blob/6c4f360264397c59801f6da2bdac13e3b1fcbe91/packages/coding-agent/docs/custom-provider.md) supports native providers, proxy/base-URL overrides, OpenAI-compatible transports, OAuth, and OpenRouter-specific compatibility. | Pi RPC is the likely closed subprocess boundary, but use one admitted prompt plus abort only; keep its queues/sessions/catalog/fallback outside authority. Provider selection, credentials, isolation, route evidence, and retries stay server-sealed, and Pi remains disabled until the outer isolation profile proves every gate. |
| [codeaashu Claude Code mirror `6a259091`](https://github.com/codeaashu/claude-code/tree/6a2590911df240ff5ea56aa355696cfb94d128cb) | The mirror documents centralized per-tool permission checks, bridge transports, remote-session control, MCP, worktree/same-directory spawn modes, and teardown. However its own `README` and `LICENSE` describe leaked Anthropic proprietary source and mark it unlicensed. It is not an official Anthropic source. | Treat only as low-trust architectural orientation. Do not copy, build, redistribute, or derive implementation from it; verify any product claim against official Anthropic documentation before it can affect a decision or contract. |

Research clone receipt (updated 2026-09-04):
`/Volumes/hubdisk/workspace/research/sessionless-competitors`. Reviewed sources
currently pinned above are `codex`, `pi`, and `claude-code`; additional cloned
candidates are `opencode`, `deepseek-harness`, `hermes-agent`, `zed`, and
`needle`. A clone is inventory, not accepted evidence: every later claim must
still pin an exact revision and cite the inspected source. The library contains
no credentials and is outside every Sessionless worktree.

No vendor marketing claim is treated as a Sessionless pass. The cloud-dev spike
records exact image/profile/region, raw measurement artifacts in private test
storage, sanitized results, and reproducible commands. The executable gate
contract, current no-go baseline, and operator-safe probe sequence are in
[yandex-serverless-substrate.md](yandex-serverless-substrate.md).

### Region and data residency

The initial Yandex candidate is sealed to `ru-central1`; a different region is
a different substrate profile and placement digest. Provider processing region,
router upstream region, cloud runtime region, artifact storage region, logging,
and encrypted evidence retention are separate facts. A matching cloud region
does not prove a provider stayed there.

Cloud Run, Azure, Modal, and other products expose region controls in different
forms; GitHub, E2B, Daytona, and Cloudflare product defaults/availability were
not normalized into a comparable residency guarantee in this iteration. Their
residency state is therefore `unknown`, not global or acceptable for private
data. A future migration requires current contractual region/retention evidence
and an exact deployment measurement, not only an API region parameter.

## Threat model

| Threat | Required control and evidence |
|---|---|
| Foreign owner or resource reaches execution | Owner-scoped WorkerJob/resource reads, full binding digest, two-owner negative before secret/workspace/process/network counters move. |
| Warm instance leaks a previous attempt | Fresh root, no inherited environment/session/cache, before/after marker scan, process/socket scan, credential zeroization, cleanup proof, and tainted-instance termination. |
| Stale credential/resource generation | #85 generation/revision check immediately before secret read and again before backend start; revoke/rotation race test. |
| Route/model/provider bait-switch | Exact #83/#84 model vendor, model, transport kind/provider, upstream, endpoint, policy and evidence digests; actual route either matches or is explicitly unknown/no-go. |
| Image, backend, or protocol substitution | Registry exact match plus image and workload attestation; no `PATH`, tag, latest, environment discovery, wrapper substitution, or default backend. |
| Network exfiltration or metadata theft | No direct internet; one policy proxy; metadata/private/control-plane/alternate-port/redirect/DNS-rebinding negatives; byte and destination evidence. |
| Descendant outlives leader/request | Process group plus external runtime observation; TERM/KILL/timeout/leader-loss tests; cleanup `verified` only after no survivors. |
| Cancellation races provider acceptance | Persist independent request/acceptance/stop/cleanup/terminal facts; fence rejects late output; no automatic resend after possible acceptance. |
| Trigger response is lost | Canonical lease and terminal reconciliation; duplicate delivery cannot call provider. |
| Cleanup fails or instance dies mid-erase | Cleanup remains failed/unknown, instance exits and is not reused, durable candidate/credential recovery runs, new attempt remains policy-gated. |
| Snapshot contains tenant or secret data | Credential-free build-only snapshots, typed allowlist, content/marker scan, digest key excludes runtime state, delete/retention evidence. |
| Output/log exposes private content | Bounded private evidence, public allowlist DTO, marker tests over logs/YDB/queue/artifacts/telemetry, #82 owner-safe projection. |

## Cost and capacity model

Admission keeps provider cost and substrate cost independent. A free provider
model does not make compute, egress, logging, storage, or retries free.

```text
AdmissionCostCeilingV1 {
  currency, price_revision, price_observed_at, price_expires_at
  max_deliveries, max_pre_effect_duration_per_delivery
  max_active_duration, max_cleanup_and_reconcile_duration
  configured_memory, configured_vcpu
  max_ingress_bytes, max_egress_bytes, max_log_bytes, max_evidence_bytes
  substrate_price: unknown | known(amounts...)
  provider_price: unknown | known_free | known(amounts...)
  max_substrate_amount?, max_provider_amount?, max_total_amount?
}

PostRunCostObservationV1 {
  state: unknown | observed | reconciled
  currency?, amount?, provenance?, observed_at?, billing_reference_digest?
}

admitted_substrate_ceiling =
  max_deliveries * (invocation_charge + bounded pre-effect compute)
  + one effect-owner active compute
  + bounded cleanup/reconcile compute
  + bounded egress/log/evidence/startup-artifact charges

admitted_total_ceiling =
  admitted_substrate_ceiling + admitted_provider_ceiling
```

Admission validates canonical currency, fresh price revision, nonzero bounds,
and the configured maximum delivery count (currently five). If a component
required by policy is `unknown`, the corresponding ceiling and total remain
unknown and admission fails closed; unknown is never coerced to zero.
`known_free` is an explicit provider price observation, not the absence of one.
The canonical ceiling digest covers every field, including currency, price
revision and expiry, maximum deliveries, per-delivery pre-effect duration,
active duration, cleanup/reconcile duration, resource sizes and aggregate
amount ceilings; there is no partial or caller-selected projection.
Observed/reconciled post-run cost is recorded separately and never substituted
into the pre-run ceiling.

For illustration only, the current Yandex USD formula gives a 2 GB, 1 vCPU,
55-minute *single active invocation* (the admitted 40m execution, 5m cleanup,
and 10m control margin, leaving the 5m hard platform guard unused) a compute
ceiling of roughly **$0.10**
before the bounded duplicate-delivery/pre-effect allowance, monthly free
amounts, invocation charge, egress, logging, storage, provider charges, taxes,
and regional/currency differences. This is a source-based calculation, not a
benchmark or quote. Production admission uses current price evidence and a
server-owned budget, never this constant.

Initial rollout caps:

- maximum two concurrent serverless harness invocations for the canary;
- one trigger message and one admitted attempt per invocation;
- execution 40m, cleanup 5m, request safety margin 10m, hard platform guard 5m;
- scratch at most 256 MiB within the current 512 MiB temporary-file ceiling;
- no provisioned instances, memory snapshots, paused sandboxes, or warm-pool
  reservations until measured cold-start value exceeds their idle cost;
- provider retries zero, trigger/queue redelivery bounded by the canonical
  delivery policy, and no retry after possible provider acceptance;
- daily and per-tenant currency/compute/egress ceilings with `unknown` distinct
  from zero and exhausted.

PR-03d records cold-start p50/p95/p99, active duration, CPU/RAM/scratch/output,
queue-to-start, cleanup duration, provider-proxy bytes, retry/duplicate counts,
and billed observations. A production gate needs enough samples to expose
tails; a single successful call is not evidence. Cost or quota unavailable is
visible and fail-closed for admission where policy requires it.

## Cloud-dev evidence and rollout gates

No real provider or key is required for PR-03a through PR-03d. The deterministic
plan uses a fake backend and a private in-memory/fake HTTPS provider transport:

1. strict DTO/codec/digest/property tests and #84 conformance fixtures;
2. local process fixture for normal, timeout, cancellation, leader loss,
   descendant resistance, output overflow, protocol drift, and cleanup failure;
3. egress proxy tests for exact host/path/method, redirect, alternate port,
   private/metadata IP, DNS rebinding, oversized request/response, and secret
   injection/redaction;
4. cloud-dev YMQ duplicate, lost response, lease expiry, concurrent delivery,
   cancellation, container termination, and warm cross-owner marker tests;
5. exact image/workload/protocol attestation and negative substitution;
6. public-safe #82 diagnostics and raw-log/YDB/queue/artifact marker scans;
7. measured limits/cost report for the immutable candidate digest.

Production remains disabled until all of these are true:

- PR-03a contract and owner reviews are accepted;
- PR-03b proves process, filesystem, output, timeout, and cleanup boundaries;
- PR-03c proves proxy-only egress and real #85 encrypted-backend lifecycle with
  no key in dispatch/YDB/logs/artifacts;
- PR-03d proves Yandex warm reuse, cancellation, ambiguity, limits, cold start,
  concurrency, and cost for the exact revision;
- every enabled backend passes #84 with native protocol `supported`, not fake
  registry `pass`;
- two-owner/cloud recovery E2E and public-safe observability pass;
- policy evidence for the exact provider/data/resource/placement is fresh;
- an operator promotion record names exact digests and rollback action.

Rollback disables the substrate profile/registry entry and stops new admission.
It never switches an in-flight or queued attempt to another substrate or
provider. Existing attempts are fenced, cancelled, reconciled, or allowed to
finish according to their sealed authority.

## Decision record and reevaluation triggers

Accepted:

- Sessionless outer harness and execution substrate are separate layers;
- Yandex request-driven YMQ-triggered HTTP container is the conditional first
  target;
- direct tool-free API/router is the smallest initial cloud profile;
- cloud CLI backends stay disabled until stronger evidence exists;
- immutable startup images only; no tenant/session snapshot semantics;
- all fallback and retry decisions remain canonical and pre-attempt.

Rejected:

- making Codex, OpenCode, Pi, OpenRouter, or a vendor sandbox the product
  harness or history authority;
- one process-wide installed CLI/env-based backend selection;
- trigger, SDK, CLI, router, or provider automatic retries;
- direct public internet from the harness process;
- persistent user sandboxes, memory snapshots, or warm workspaces as retry or
  conversation state;
- weakening AW-05 `NetworkDenied` or treating ordinary containers as a proven
  arbitrary-code sandbox;
- accepting an OpenRouter key before #85 has a reviewed encrypted production
  adapter and cleanup/reset proof.

The substrate decision is reopened if any of these occur:

- Yandex cannot prove warm erasure, process-tree stop, proxy-only egress, or
  cleanup inside the one-hour request bound;
- required agent tasks exceed 40 minutes or 256 MiB scratch at material tail;
- CLI/tool profiles become MVP-critical and need a microVM boundary;
- measured cold-start or idle/cost tails exceed the accepted budget;
- region/data-residency policy cannot be met;
- a managed sandbox/job alternative materially reduces security or operating
  risk without importing a second state machine;
- Yandex limits, pricing, trigger, isolation, or cancellation behavior changes.

The migration seam is `ExecutionSubstrateV1` plus the sealed substrate binding.
Changing substrate requires a new placement/profile revision and a new admitted
attempt. Existing attempts never migrate in place.

## Post-review implementation decomposition

These issues are created only after this design receives the required owner
reviews. Each must carry `harness`, `provider`, and the relevant additional
labels, and milestone `MVP — Provider & harness (#13)`.

1. **PR-03a — Serverless harness authority and invocation contract**
   - `SubstrateBindingV1`, invocation authority/evidence, strict codec/digests,
     no-fallback registry composition, and conformance/property tests.
2. **PR-03b — Isolation launcher and execution supervisor**
   - image/workload attestation, fresh workspace, process tree,
     timeout/cancel/kill, output bounds, cleanup proof, and network-denied fake.
3. **PR-03c — Provider egress and invocation credentials**
   - exact provider proxy, #85 encrypted adapter/materialization, generation
     fencing, zeroization/recovery, and fake endpoints only; the first
     feature-disabled composition boundary is documented in
     [serverless-egress.md](serverless-egress.md).
4. **PR-03d — Yandex scale-to-zero substrate spike**
   - immutable HTTP-server revision, YMQ trigger, warm reuse, cold-start,
     timeout/concurrency/cost, cancellation, lost response, and ambiguous
     completion measurements.
5. **PR-03e — Backend composition**
   - feature-disabled Codex/OpenCode/Pi/direct registrations below the outer
     harness; every native adapter must pass #84 before enablement.
6. **PR-03f — Cloud-dev two-owner E2E and rollout gates**
   - owner/resource denial before effects, exact routing/no fallback,
     cancel/fence/cleanup, #82 observability, budgets, promotion, and rollback.

Required reviewers are the owners of #83/#84 provider contracts, #85 credential
ingestion, #81 Codex adapter, #72 attached workers, #1/#14 cloud runtime, and
#82 UX observability. Review acceptance authorizes decomposition, not production
provider calls or secret ingestion.
