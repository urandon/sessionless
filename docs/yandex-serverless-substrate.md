# Yandex scale-to-zero substrate evidence

This document is the PR-03d (#90) measurement plan and current promotion
decision for the immutable `ru-central1` Yandex Serverless Containers
candidate. It complements the authority, isolation, and egress contracts in
[serverless-harness.md](serverless-harness.md),
[serverless-isolation.md](serverless-isolation.md), and
[serverless-egress.md](serverless-egress.md).

## Current decision

**No-go for production promotion as of 2026-09-05.** The decision is about the
current deployable composition, not about Yandex Serverless Containers as a
product. The existing deterministic cloud worker is still useful for the
tool-free control-plane smoke path.

The following promotion gates are known to fail in the current composition:

- the managed worker now keeps one serialized renewal/cancellation watchdog
  active across scratch preparation, credential issue/materialization, input
  materialization, `HarnessDriver.Execute`, credential finalization, output and
  canonical-event upload, and scratch cleanup. Deterministic race tests cover
  silent materialization, execution, finalization, and fence loss during
  upload; a failed scratch removal/absence check now blocks success with
  `cleanup_failed`. The cloud composition still has to prove that the PR-03b
  process supervisor and PR-03c egress transport stop under that cancellation
  signal and taint a warm instance on any wider residue ambiguity;
- the cloud profile sets `WORKER_LEASE_TTL=2m`, while a credentialed 40-minute
  execution must acquire authority covering execution and bounded cleanup;
- an exact retry-stable lease ID can be returned to the same worker identity on
  redelivery. `cmd/worker-runtime` now configures the PR-03a grant issuer, and
  `worker.Manager` creates a fresh physical claim and reserves the durable
  provider effect before credential or workspace materialization. A different
  physical delivery is reconcile-only and cannot call `Execute`; the returned
  ownership grant is not yet composed through a PR-03b prepared invocation and
  consumed at the PR-03c egress boundary;
- no immutable candidate has completed cold/warm, 15-minute redelivery, lost
  response, fence-loss, cleanup, or tainted-instance tests in cloud-dev;
- cold-start latency, warm-start latency, billed duration, comparable cost,
  and provider processing residency are unknown. Unknown is not zero, free,
  global, or acceptable evidence.

The public-safe evidence schema and fail-closed evaluator live in
`internal/yandexsubstrate`. A failed mandatory safety gate yields `no_go`;
missing evidence or an undersized cohort yields `conditional`; `go` is possible
only when every mandatory gate passes, all mandatory quantities are known, the
cold and warm cohorts each contain at least 30 samples, and every race cohort
has at least one observation.

## Vendor facts and assumptions

Vendor pages were rechecked on 2026-09-05. They are source facts, not measured
Sessionless guarantees:

- the [container limits](https://yandex.cloud/en/docs/serverless-containers/concepts/limits)
  page documents a maximum one-hour request including initialization, the
  long-lived mode requirement beyond ten minutes, 8 GB maximum memory, 10 GB
  ephemeral disk, 512 MB temporary-file limit, 3.5 MB HTTP request/response
  limits, 4 KB environment-variable limit, and 10 GB image limit;
- [long-lived containers](https://yandex.cloud/en/docs/serverless-containers/concepts/long-lived-containers)
  may still be terminated before their configured timeout; a warning is only a
  best-effort signal when enough time remains;
- the [YMQ trigger contract](https://yandex.cloud/en/docs/serverless-containers/concepts/trigger/ymq-trigger)
  deletes a message after a successful invocation and returns it to the queue
  after an error subject to visibility timeout. Therefore an HTTP response lost
  after durable completion is an ordinary duplicate-delivery case;
- [container lifecycle](https://yandex.cloud/en/docs/serverless-containers/concepts/container)
  allows an instance to remain available for an unspecified period, so warm
  cross-owner reuse must be treated as expected rather than exceptional;
- [pricing](https://yandex.cloud/en/docs/serverless-containers/pricing) uses
  invocation, CPU/RAM duration, and outbound traffic components with 100 ms
  duration granularity and free allowances. Account-specific billed cost must
  still be measured; a free allowance is not a zero unit price.

Provider execution residency is a separate provider/router fact and cannot be
inferred from the container region.

## Immutable candidate

One run fixes all of the following before the first sample:

- image by registry digest, never tag;
- worker, outer harness, backend protocol, egress proxy, Terraform plan, and
  public report schema revisions;
- `ru-central1`, IAM-only invocation, HTTP runtime, batch size one, concurrency
  one, long-lived mode, and a dedicated non-production resource prefix;
- 40 minutes maximum materialization/backend execution, 5 minutes maximum
  cancellation/finalization/cleanup, at least 10 minutes control margin, and 5
  unallocated minutes as a hard platform guard;
- 15-minute YMQ visibility, bounded delivery count, deterministic fake
  provider, synthetic marker data, and no user/provider credential;
- rollback by disabling the exact substrate profile and preventing new
  admission. Rollback never reroutes an in-flight attempt.

Raw queue receipts, cloud IDs, logs, billing records, marker values, and failure
details stay in private evidence storage. The committed report contains only
aggregate distributions, fixed gate codes, immutable digests, official source
URLs, and numeric quantities.

## Cohorts and success observations

Run at least 30 cold and 30 warm samples. Correctness cohorts are independent
of the latency cohorts and use channel/state acknowledgements rather than
fixed sleeps:

| Cohort | Fault injection | Required durable observation |
|---|---|---|
| Cold/warm | invoke the exact image with and without eligible warm reuse | queue-to-start, start, active, cleanup, billed duration, peak memory, scratch, output, and egress distributions |
| Silent long call | fake provider emits no events across at least one lease-renewal boundary | watchdog renews exact lease/fence and polls canonical cancellation |
| Redelivery | make the same message visible during the silent call | one effect owner and one fake-provider send; duplicate is reconcile-only |
| Lost response | suppress the successful trigger response after canonical terminal commit | redelivery observes terminal/effect reservation and performs no send |
| Cancellation | request cancellation before and after fake-provider acceptance | request, signal, provider acknowledgement, process stop, cleanup, and terminal remain separate observations |
| Lease/fence loss | reject renewal or advance canonical fence while work is active | transport/process stop begins, late output is rejected, success cannot commit |
| Warm cross-owner reuse | alternate two synthetic owners and plant bounded markers | no previous workspace, environment, process, socket, credential, output, or marker is observable |
| Cleanup failure | retain a descendant, socket, file, or proxy state | cleanup is failed/unknown, instance is tainted and exits, success/ack is blocked |
| Egress negative | try metadata/private IP, alternate port, redirect, DNS rebinding, and direct internet | only the attested proxy route can carry bounded request bytes; every bypass is denied |

The fake provider records only a random run-local marker digest and the count of
accepted sends. It supports explicit pre-acceptance, accepted-unknown, terminal,
and cancellation states so the test never equates a missing response with no
effect.

## Safe execution boundary

The repository work before cloud execution is non-provisioning: validate the
Terraform plan, build the immutable image, run local/race/conformance tests,
and prepare the private evidence destination. Creating or changing Yandex
resources, pushing the candidate image, invoking the paid candidate, or reading
account billing evidence requires an explicit operator authorization for that
exact probe.

Before authorization, record all cloud-derived gates and quantities as
`unknown`; do not manufacture a passing fixture. After authorization:

1. capture account, folder, quota, budget, and current deployment evidence;
2. choose a unique `pr03d-<timestamp>` prefix and declare the maximum sample
   count, wall time, retries, memory, vCPU, egress, and spend;
3. apply only the reviewed non-production plan and capture its digest;
4. execute each bounded cohort and stop immediately on a safety-gate failure;
5. export private raw evidence, generate the public report, and validate it;
6. disable the candidate profile before teardown, then destroy only resources
   carrying the exact probe prefix/labels;
7. verify queues, images, containers, logs, secrets, and temporary evidence
   have the declared retained-or-removed state.

The run is incomplete if teardown, billing, or residual-state evidence is
unknown. It may still produce a `conditional` or `no_go` report, but never
`go`.

## Work needed before the cloud run

The remaining implementation slice is composition, not a timeout increase:

1. carry the newly reserved PR-03a ownership grant through an attested PR-03b
   prepared invocation and consume it exactly once at the PR-03c provider
   boundary; keep foreign physical claims reconcile-only;
2. compose the full-lifecycle worker watchdog with PR-03b process supervision
   and PR-03c transport cancellation, then retain its deterministic local race
   cohorts as promotion gates;
3. cancel transport and PR-03b process supervision on renewal/fence loss and
   block every event, artifact, terminal commit, and trigger acknowledgement;
4. compose PR-03c egress and invocation credentials only after the fresh
   effect-ownership grant, and keep reconciliation credential-free;
5. expose fixed-code, content-free probe observations and generate the report
   from private evidence;
6. update the cloud profile only after the local race suite proves the above.

Until that composition lands and the cloud cohorts run, production provider
profiles remain disabled.
