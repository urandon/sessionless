# Cloud development alerts

Version: 0.1.0, engineering design for
[#19](https://gitcode.com/urandon/sessionless/issues/19), feeding
[#35](https://gitcode.com/urandon/sessionless/issues/35).
Research: [metric and notification contracts](https://gitcode.com/urandon/sessionless/issues/19#note_194853419).

Use Yandex Monitoring for a small set of actionable failure notifications.
Cover Web/control, canonical persistence, queues, storage and both selected
execution routes. A successful container invocation is not a durable answer.
Alerting observes failures; it never authorizes a retry, provider switch or
another model/tool effect.

## Scope

Inventory the resources actually used by the selected deployment. Mark each
component as enabled, disabled or not yet exercised. Attribute managed
execution and attached transport separately; a local demo does not qualify
managed cloud execution. Cloudflare, Telegram and Workflow alerts apply only
when the selected deployment actually uses those components. They are not
additional WebUI MVP prerequisites.

Prefer existing platform metrics and content-free application signals. Add
missing signals at their owning component under #19 rather than introducing
a collector service, polling ledger, database schema or always-on model probe.
Platform metrics cannot conceal the application-signal gaps below.

## Rules and coverage

Each row becomes one rule or a small set separated by component and route.
The deployment operator owns the rules and receives their notifications.
The private inventory binds the exact folder, resource, active revisions and
verified metric labels; the descriptions below are not executable selectors.
Aggregate only matching dimensions, never unrelated tenants or storage totals
and their component series together.

The following values are initial engineering settings, not measured service
levels. Activation must validate metric delivery delay and bounded baseline
behavior before selecting the evaluation delay and confirming thresholds.
Use a 10-minute evaluation window unless a row states otherwise.

| Failure class | Signal and selector | Initial evaluation and recovery | First safe action |
| --- | --- | --- | --- |
| Web/control server errors | `api_gateway.errors_count_per_second`, rate in requests/s, scoped to the selected gateway, release, safe route template and 5xx codes; request rate provides demand context. | Alarm on a nonzero error point. Recover on observed zero errors with positive request telemetry, not disappearance of the series. Expected 4xx are excluded. | Inspect selected ingress/revision and sanitized component errors; distinguish expected access denial from service failure. |
| Web/control latency | `api_gateway.requests_latency_milliseconds`, histogram in ms, with the same route/release scope. | Trial threshold: p95 above 2000 ms with at least 20 requests in the window; recover below the threshold with enough fresh samples. Validate histogram aggregation and request-count derivation before enabling. If unavailable, record an implementation gap rather than inventing a percentile. Include cold starts in the observed distribution. | Compare gateway and component timing; do not restart or add prepared instances automatically. |
| Container or trigger execution failure | `serverless.containers.errors_per_second`, rate; scoped to the active component/revision. Bind documented trigger error/access-error series by actual trigger and error type. | Alarm on a nonzero error point; require fresh non-error activity for recovery. Treat timeout/access failure separately in the runbook. Zero invocations or CPU at idle is normal. | Inspect revision/trigger configuration and sanitized failure category. Container success does not resolve a canonical unknown outcome. |
| Concurrency saturation | `serverless.containers.inflight`, gauge in concurrent invocations, scoped to each exercised component/revision and its verified effective concurrency/capacity limit. | Alarm only when sustained occupancy at the actual limit coincides with waiting demand or measured request delay beyond the route allowance. Bind duration and impact explicitly. Inflight alone is diagnostic; if effective limit/demand/impact cannot be established, saturation remains an instrumentation gap, not a covered rule. | Compare pending work and the actual active revision limit; idle scale-to-zero is normal. Do not automatically increase capacity. |
| Pending queue demand | `queue.messages.stored_count` and `queue.messages.inflight_count`, gauges in messages; `queue.messages.oldest_age_milliseconds`, gauge actually measured in **seconds**. Bind each dispatch/scheduler queue separately. | Alarm when positive backlog has age above the selected route's documented processing, visibility and permitted retry allowance. Record that allowance explicitly in seconds. A vendor threshold clear is not canonical progress; disable automatic OK notifications and confirm route progress separately. | Inspect consumer/trigger and known retry state. A backlog decrease can be retention or purge; never blindly redrive. |
| DLQ occupancy or retry growth | Per-DLQ stored count; original consumer's bounded retry/redelivery outcome signal is a required application gap until demonstrated. | Alarm on observed DLQ occupancy above zero. Disable automatic OK notifications: zero occupancy can clear the vendor condition without resolving work. Operator disposition plus fresh queue observations establishes semantic recovery. | Preserve evidence; reconcile ambiguous effects before any separately authorized replay. |
| YDB failures and pressure | `api.grpc.response.count`, RATE/count by verified service/method/status; `api.units.consumed_per_second`, RU/s; documented storage used/limit, bytes. Bind the actual database. | Alarm on nonzero unavailable/resource-exhausted response rate, excluding expected/recovered conflicts by verified status classification. Trial capacity warning: usage above 85% for 15 minutes; vendor clear at or below that same threshold with fresh samples. No separate 75% hysteresis is assumed. RU is diagnostic unless a real configured limit is bound. | Check throttling, storage and transaction outcomes; distinguish a recovered conflict from an ambiguous commit. Never infer data loss or reset state. |
| Object storage capacity and failures | `space_usage` and, when present, `max_size`, byte gauges for the actual bucket and selected aggregate dimensions. Read/write/capability failures require a content-free application signal. | Trial capacity warning above 85% for 15 minutes; vendor clear at or below the same threshold with fresh samples. No ratio when the limit series is absent. I/O semantic recovery follows the verified operation outcome, not capacity or request volume; automatic OK is disabled for unqualified outcome signals. | Check capacity and capability/service errors without opening user objects or exposing capability URLs. |
| Login/session/upload failure | Required application signals for unexpected callback/session/revocation failures and abandoned upload cleanup; do not assume they already exist. Scope by component and bounded error class, not identity. | Alarm on an unexpected terminal service failure; expected login rejection, expired session or denied membership is not an outage. Cleanup delay uses its original declared deadline. Recovery needs a successful equivalent operation or confirmed cleanup. | Inspect sanitized state/error class; no cookies, account claims, file names or tokens. |
| Canonical progress and result publication | Required application signals for expected work, durable terminal outcome, uncertain completion and missed progress deadline; preserve managed/attached route attribution. | Alarm on an unresolved unknown outcome or demand exceeding its original execution/cleanup deadline. Recovery requires canonical reconciliation/result publication, not HTTP 200 or telemetry silence. | Inspect the existing Run/Attempt/finalization through authorized diagnostics; do not turn an alert into replay permission. |
| Attached transport and managed execution | Existing route-specific dispatch/ACK/lease and substrate outcome signals must be identified or implemented. Attribute cost separately. | Detect lost expected progress against original route deadlines. Idle/disconnected-but-unselected hosts do not page. Recovery requires current transport and canonical result evidence for the same selected route. | Check the selected host/substrate/profile; no silent fallback between execution routes. |

Metric contracts: [API Gateway](https://yandex.cloud/en/docs/api-gateway/metrics),
[Containers and triggers](https://yandex.cloud/en/docs/serverless-containers/metrics),
[Message Queue](https://yandex.cloud/en/docs/message-queue/metrics),
[DLQ behavior](https://yandex.cloud/en/docs/message-queue/concepts/dlq),
[YDB](https://yandex.cloud/en/docs/ydb/metrics),
[Object Storage](https://yandex.cloud/en/docs/storage/metrics).
Rates, counts, gauges and histograms require their own aggregation; summing
rate samples is not an event count without interval conversion.

## Missing data and notification policy

Set both Monitoring policies, no metrics matching a selector and no points in
the window, explicitly to `No data`. Unknown telemetry is not fabricated zero
or recovery. Do not notify on idle `No data`; an independently observed demand
signal with a missed progress deadline is a separate incident. A missing
selector during activation is a configuration failure, not a passing check.
See [alert evaluation semantics](https://yandex.cloud/en/docs/monitoring/concepts/alerting/alert).

Separate the vendor condition from semantic recovery. Monitoring changes status
from its configured threshold; it cannot wait for an operator disposition or
canonical reconciliation unless a verified predicate actually expresses it.
Do not add a new incident ledger or collector to simulate that capability.

For platform/capacity rules, an OK notification means only that fresh qualified
telemetry cleared that metric condition. Include that meaning in the rule's
safe annotation; it does not resolve a Run or prove delivery. The query must
return No data without the required fresh activity/samples. Ordinary threshold
rules do not provide separate recovery hysteresis.

For queue/DLQ, canonical unknown/progress, attached/managed outcome, login/session
failure, upload cleanup and application I/O rules, select Warning/Alarm notifications but disable
automatic OK until their actual positive recovery predicate is implemented and
qualified. A vendor transition to OK can still occur; it is not a recovery
message or permission to resume work. Until then, the operator records the
confirmed disposition and communicates semantic recovery through the existing
operator channel. That confirmation is a manual runbook step, not an assumed
Monitoring automation. Track the predicate gap explicitly in #19.

Disable periodic re-notification for the first sparse-development profile.
Group rules by component/route/incident class; vendor state transitions may
still send another entry notification after an intervening status change.
Do not promise exactly-once notification deduplication or merge independent
route failures into a fictitious shared success.

One operator-confirmed supported channel is sufficient; it must not depend
on enabling the deferred Telegram product path. Before activation verify the
recipient account permissions/settings, selected statuses and actual receipt.
Service delivery status is not operator acknowledgement. Configure per-rule
statuses as above, without No-data idle pages.
See [notification channel semantics](https://yandex.cloud/en/docs/monitoring/concepts/alerting/notification-channel).

## Privacy and operator record

Notifications contain component, selected route, bounded failure category,
rule name and a safe runbook link. Exclude prompts, search queries, model
results, file names, secrets, cookies, identity/session identifiers and capability
URLs. Never include raw gateway paths; use an approved route template/aggregate.
Keep metric dimensions bounded and free of user-controlled high-cardinality data.

The private operator record contains:

- exact deployment commit/image, config/state/schema/profile binding;
- folder/resource/revision selectors and verified metric label names;
- each rule's type/unit/aggregation, threshold, window/delay, both NoData policies,
  recovery condition, owner and safe action;
- channel/rule IDs, private recipient confirmation and finite test manifest;
- redacted failure, receipt, recovery and idle-observation evidence.

Keep contacts and operational bindings out of Git and public issue comments.
The existing billing budget remains notification-only, not a hard spend cutoff.

## Implementation and activation

Implement #19 in this order, reusing existing tasks and component owners:

1. Freeze the exercised inventory and rule bindings; resolve the explicit
   application-signal gaps with bounded component instrumentation and fake tests.
   Link the implementation changes and unresolved gaps in #19 comments.
2. Supply the redacted inventory/runbook and validate selectors, units,
   aggregation, privacy and sparse-traffic policy. No required exercised failure
   class is accepted as covered merely because its metric is planned.
3. Under separate operator authority, configure the selected rules/channel
   through a supported surface and read them back. The documented
   [console creation flow](https://yandex.cloud/en/docs/monitoring/operations/alert/create-alert)
   is sufficient. The public [REST overview](https://yandex.cloud/en/docs/monitoring/api-ref/)
   does not establish an Alert-create endpoint; do not guess REST/CLI operations.
4. Execute one approved finite failure-through-rule-and-channel experiment,
   then join its receipt to #35. Do not require destructive vendor fault injection
   for every class or a new production paging platform.

The experiment manifest names an isolated test resource or approved benign
condition, the actual rule/channel, maximum duration/requests/cost, operator,
stop condition and cleanup. Select a rule whose positive metric-clear or
application recovery predicate is actually qualified; the experiment cannot
qualify the unresolved recovery predicates of other rules. It must not break
shared YDB, delete payloads,
disable networking or cause a real provider/tool send. Prove condition → Alarm
→ operator receipt → condition removal → fresh recovery → recovery receipt.
Observe an agreed idle window and confirm no repeated idle notification.
If any stage is uncertain, retain it as unproven rather than retrying effects.
A channel-test button alone does not exercise the rule chain.

Design acceptance does not configure alerts or complete #19/#35. Activation
requires the private bindings, reviewed implementations and separately approved
finite experiment; no credential, migration or deployment authority follows
from this document.
