# Attached-worker transport experiment preparation

Owner: [#129](https://gitcode.com/urandon/sessionless/issues/129), parent
[#75](https://gitcode.com/urandon/sessionless/issues/75), epic
[#72](https://gitcode.com/urandon/sessionless/issues/72).
Status: proposed Phase-0 preparation, 2026-10-09; no cloud run or rollout decision.
Accepted runtime inspection base: `e1dd2d663b3c9dfbec7922829805b9f5e52bbb21`.
Current launch dependencies are in the [MVP plan](mvp-delivery-plan.md).

## Local preparation and its evidence boundary

```sh
make attached-worker-experiment-plan
make attached-worker-experiment-test
```

The first command reads the [public draft](experiments/attached-worker-transport-draft.json)
and prints JSON. It reuses `PollCountUpperBound` and
`PollCountUpperBoundWithWake` in
[`poller.go`](../internal/attachedworkertransport/poller.go); it has no network,
provider, process-launch or credential operation. It never becomes an execution
tool: `cloud_execution:true`, unexpected fields, incomplete limits, non-24-hour
windows, duplicate/unsupported cohorts and oversized input fail validation.
Errors do not echo input or private file paths. An accepted draft still reports
`draft_not_authorized` and lists missing deployment/telemetry/approval facts.
The CLI is a development tool; it is excluded from shipped runtime components.

The second command uses the repository-shared Go caches and runs race/shuffled,
uncached tests of the planner and existing transport/daemon cadence. It
exercises existing fake-clock cooldown, wake coalescing, bounded retry,
single-flight, cancellation and ambiguous-recovery regressions. It does not
simulate 24 hours of billed cloud traffic, validate actual sleep/wake behavior
on a host, or supply a new cloud recovery result.

| Cohort | Workers | Window | Scheduled exchange upper bound | Upper bound with local wake |
| --- | ---: | --- | ---: | ---: |
| 15-minute idle | 1 | 24 hours | 96 | 96 |
| 30-minute idle | 1 | 24 hours | 48 | 96 |
| 60-minute idle | 1 | 24 hours | 24 | 96 |

These are scheduling bounds, not observed counts or complete HTTP budgets.
The canonical first post-Manifest cooldown applies to initial attach and
reconnect. Count observations in a half-open `[start, end)` window; record
boundary events separately. First-poll timing and runtime overhead can reduce
observed counts. The longer-interval idle cohorts must have local wake disabled;
test queued wake separately. Attach/Manifest, reconnect, exact replay, active
controls, health/telemetry queries, setup and teardown have separate ledgers.

## Proposed limits and stop ownership

The draft proposes aggregate limits across all three simultaneously observed
cohorts and the bounded recovery matrix: 1,000 HTTP requests; 10,000 YDB RU;
600 billed invocation-seconds; 10 MiB egress; 100 RUB; 30 hours elapsed
including setup/teardown; at most five process restart attempts. They are
proposals, not approved spending or enforced runtime caps. The request budget
must retain separately itemized headroom above 288 wake-permitted exchanges.
There is no universal number of HTTP or YDB operations per heartbeat.

The final execution manifest must replace these proposals with reviewed limits
derived from the exact deployed revision, tariffs and telemetry. Reserve setup,
teardown, monitoring and an upper bound for in-flight requests. One named
operator owns cancellation and a watchdog; one request ledger counts attempts
including rejected/lost-response calls. A watchdog must stop before exhausting
the reserved teardown headroom. Billing exports cannot act as an immediate
kill switch; derive a conservative live cost bound from observed usage and
current pricing, then reconcile the actual charge later. The existing
[folder budget](cloud-development.md) is notification-only.

Do not execute unless telemetry supports timely stop decisions. Missing samples,
unattributable RU, a counter reset without continuity, unexpected traffic,
unbounded retries, or an unavailable watchdog stop the run and retain `unknown`
or `no-go`. Any duplicate semantic effect or cross-owner evidence stops it
immediately. The local planner checks presence/shape only; it does not implement
this watchdog or certify the operational limits.

## Exact execution manifest required before any cloud write

Record one reviewed immutable manifest in the private deployment record and
publish a redacted digest/approval receipt in #129. It must include:

1. Git/source and harness SHA; control image/revision digest; binary versions
   and digests; capability/policy profile; schema revision; experiment runner
   revision and the normal default-off gates used by this dedicated fixture.
2. Exact region/folder, isolated gateway/container revision, YDB database and
   synthetic tenant/owner/worker/enrollment scopes. Keep synthetic scope separate
   from both pilot resources and the laptop's #34 deployment. No mutation of a
   shared live resource or unrelated deployment state.
3. Three worker cohorts, UTC start/end, 24-hour duration per cohort, wake policy,
   maximum five restarts at one-minute spacing, recovery-case bounds, all
   proposed/approved ceilings and reserved teardown headroom.
4. Reviewed Terraform/config diff and least-privilege setup/measurement/teardown
   permissions. Zero prepared instances; no production content or provider call.
   Synthetic worker transport authentication is necessary, but is kept in private
   files/secret storage and never in the public manifest, report or logs. No
   provider credential is present. Public observation envelopes contain no
   bearer, challenge, proof, prompt, payload, endpoint or private identifier.
5. Exact monitoring selectors, aggregation/reduction, sampling period,
   counter-reset handling and attribution controls; export destinations/retention
   and redaction; named operator/watchdog, stop mechanism and teardown operations.
6. Independent review, owner approval, expiry of that approval and the exact
   manifest digest. `draft_not_authorized` does not satisfy this gate.

The deployment runner, HTTP observer, cloud watchdog, telemetry export and
resource teardown remain to be prepared/verified against that concrete manifest.
No such runner is delivered by the offline CLI. Do not run deployment commands
copied from #34, use its credentials implicitly, or change its source branch.

## Observation ledger and query plan

For every metric preserve original query, exact private selectors, source
revision, UTC range, units, sample period, missing-sample fraction, raw export
digest and redacted summary. Observe an isolated no-worker control window
before/after; report background activity separately, without silently subtracting
it from a shared pilot database. If attribution cannot separate three cohorts,
use separate reviewed databases/revisions, or sequential 24-hour runs with a
separately reviewed time budget. The draft's 30-hour global wall limit cannot
cover three sequential days.

| Observation | Query or source to freeze | Aggregation and limitation |
| --- | --- | --- |
| Challenge/attach/Manifest/exchange/reject/replay | Experiment-owned client attempt ledger and redacted handler counters by operation/cohort | Count each request attempt including failed/lost responses; separate AW-02 batch contents from HTTP requests |
| Container invocations/processing time | `serverless.containers.execution_time_milliseconds_count` and `_sum`, filtered by exact `container`/`revision` | Counter deltas with continuity; `_sum` is processing milliseconds, not billed duration |
| Container errors/concurrency | `serverless.containers.errors_per_second`, `serverless.containers.inflight` | Time integration of error rate; maximum in-flight; missing samples remain unknown |
| YDB RU | `api.units.consumed_per_second` and `api.units.consumed_by_method_per_second` for the isolated DB | Integrate sampled rate over UTC seconds; verify exported metric selectors first; retain sampling uncertainty |
| YDB writes | Exact experiment transaction/call ledger plus `table.datashard.bulk_upsert.rows`/`erase.rows` where applicable | Bulk upsert is not a count of all transaction writes; do not infer writes from RU or heartbeat count |
| Cold/warm start and memory | Exact revision execution telemetry; `serverless.containers.used_memory_bytes` histogram | Freeze available labels/buckets; missing cold-start markers remain unknown |
| Body bytes/egress | Content-free client byte counters, gateway/service export, billed network SKU | Request/response body size is not TLS overhead or billable egress; report all separately |
| RUB and billed duration | Extended Billing export by experiment resource/service/SKU plus timestamped regional tariffs | Retain credits/free-tier treatment and allocation uncertainty; processing time is not a billing receipt |

Source references checked 2026-10-09:
[Serverless Containers metrics](https://yandex.cloud/en/docs/serverless-containers/metrics),
[YDB metrics](https://yandex.cloud/en/docs/ydb/metrics),
[extended billing details](https://yandex.cloud/en/docs/billing/operations/get-folder-report),
[Serverless Containers pricing](https://yandex.cloud/en/docs/serverless-containers/pricing).
These references define available metric semantics, not measurements of our
resources. Validate queries against the actual isolated resource before approval.

## Bounded recovery matrix

Each case uses a fresh synthetic worker and exact source/config. The idle
baseline carries no active work. Active/cancel/terminal cases use the already
accepted credential-free synthetic denied/test fixture in a separately scoped
ledger; they do not introduce a real Codex invocation or provider credential.

| Case | Injection and bound | Required evidence |
| --- | --- | --- |
| Sleep/wake | One suspension shorter than interval, one longer; no catch-up loop | Actual host suspend/resume timestamps and all exchanges; minimum gap preserved |
| Pre-send outage | One refused connection through fixture-owned proxy; bounded existing retry policy | Attempts/backoff/gap, no accepted remote effect |
| Post-send loss | Drop one response after the server commits; bounded exact in-process replay only | Same logical batch, server effect count one, no replacement invocation |
| Clean restart | One graceful stop/restart at idle | Bootstrap/Manifest counted separately, durable generation fence, first heartbeat cooldown |
| Restart storm | Five attempts maximum, one-minute spacing, no concurrent owner | Local lease/checkpoint and server heads; no duplicate launch/claim/effect |
| Queued local wake | One buffered wake before first post-Manifest interval | First cooldown cannot be shortened; later wake traffic accounted separately |
| Active/draining recovery | One interruption for each accepted test-only head | Safety/reconciliation result and at-most-once claim/process/cancel/terminal evidence |

Do not weaken an existing fence just to obtain a positive recovery observation.
Record expected `reconciliation_required` as a safety result, alongside the
availability limitation. Unit/CI fixtures are supporting evidence; each actual
cloud case still needs its own trace, denominator and source provenance.

## Teardown and decision receipt

Stop the three owned workers/watchdog under a bounded deadline; verify all owned
processes and outstanding requests are quiescent. Revoke experiment enrollment
and transport access; return experiment gates to disabled. Export only redacted
receipts; delete exactly experiment-owned resources/state through the reviewed
teardown plan. Retain immutable exports at their reviewed retention location.
Verify no extra provisioned instance, owned process, experiment routing/pin,
credential or temporary revision remains; compare against the frozen pre-run
inventory. Teardown failure blocks go and requires its exact residual inventory.

The report includes all three complete 24-hour denominators; scheduled vs observed
counts; bootstrap/reconnect/replay/control/teardown totals; measured RU/writes,
invocations/processing and billed duration, egress and RUB; uncertainty and missing
telemetry; every recovery effect count; teardown receipt; and one decision:

- `go`: attributed upper bounds fit the accepted budget, safety and teardown pass;
- `conditional`: identify the exact durable admission/limiter or telemetry change
  and its acceptance evidence in a bounded follow-up;
- `no-go`: identify the failed budget, safety, attribution or teardown gate.

Update #129, #128 and #75 with the exact evidence and decision; #133 consumes that
decision before cloud rollout. Phase-0 preparation does not close #129 or enable
the foreground. The [MVP acceptance](mvp-delivery-plan.md) remains owned by its
existing tasks.
