# Native observability YAML and operator runbook

Version: 1.0.2, offline configuration tooling for
[#225](https://gitcode.com/urandon/sessionless/issues/225), under
[#19](https://gitcode.com/urandon/sessionless/issues/19). Accepted design:
[Wiki 0.1.1](https://gitcode.com/urandon/sessionless/wiki/Cloud-Dev-Observability-Design.md).
Preserve the [alert policy](design/cloud-dev-alerts.md) and
[testing contract](testing-best-practices.md).

This is an operator overview in Yandex Monitoring, not a new Sessionless admin
UI, collector, exporter daemon, database or incident state machine. It does not
contact Yandex, discover credentials, apply infrastructure, send notifications,
call a model or change execution/retry deadlines. Both real managed and attached
MVP routes remain required. A deterministic fixture qualifies neither route.

## Editable source

### Native log destination ownership

[#243](https://gitcode.com/urandon/sessionless/issues/243) separates native log
delivery from legacy log-group management. Cloud-dev requires exactly one
explicit destination in its private tfvars:

- `runtime_log_group_id`: the exact existing operator-owned log-group ID;
- `runtime_log_folder_id`: the exact deployment folder ID, selecting the
  provider's native folder-default destination.

Set the unused input to `null` or omit it. Both set, both missing, empty, padded,
URL, project-name and placeholder inputs fail validation. Folder routing must
name this deployment's folder; it is not an automatic fallback from a failed
group lookup. The two outputs preserve that distinction: the unselected output
has no value and may be omitted by `terraform output`. No name lookup,
Monium-project substitution, data source, managed group
or implicit destination is introduced.
The operator owns the destination, retention and access verification. Remove the
obsolete `log_retention` input: Terraform no longer promises to enforce it.
The existing INFO stdout integration and write-only IAM remain unchanged for
all containers and both gateways. This is not a new exporter or alert stack.

The foundation's declarative `removed` block has `destroy = false`: an existing
group is retained, not deleted/recreated. Review its exact `forget` plan action
and the explicitly selected destination wiring before apply. Never substitute `state rm`,
`refresh=false`, a guessed ID, a logging-disabled revision or an OTLP ingestion
endpoint as a Terraform management endpoint. Other resources still refresh.
A new environment must first have a verified destination and an exact folder
identity. This configuration does not provision a log group through the failed
management API. Folder routing delegates the native default-group resolution
to the provider service; it does not prove that service is healthy.

The supported [logging destination API](https://yandex.cloud/en/docs/serverless-containers/operations/logs-write)
distinguishes `logGroupId` from `folderId`. Every container and both gateways use
the selected field with INFO still enabled and the same write-only IAM. A
reviewed folder-native plan can therefore avoid an explicit retained-group
reference during revision deployment while preserving the old group and logs.
The Terraform handoff alone does not eliminate a service-side lookup or prove
that a revision using a retained group can be created.

Before the separately authorized handoff, verify a fresh benign native record
is readable in the exact destination, along with its actual retention, ownership
and cost. Old archive visibility is not delivery proof. The
[provider migration guide](https://yandex.cloud/en/docs/monium/logs/logging-migration)
describes the Logging-to-Monium bridge and 31-day Monium retention; it does not
prove that this deployment still has the former seven-day policy. Do not silently
increase raw-log retention or claim the unavailable management API is healthy.
If these checks fail, leave the handoff/deployment unaccepted and resolve the
specific delivery or policy gap; passing offline plans is not rollout evidence.

The [Terraform removed contract](https://developer.hashicorp.com/terraform/language/block/removed)
defines non-destruction; the pinned
[1.15.5 orphan planner](https://github.com/hashicorp/terraform/blob/v1.15.5/internal/terraform/node_resource_plan_orphan.go)
skips the provider refresh for an explicit forget. Regression verification must
exercise that actual engine with a synthetic state, not a cloud credential.

Folder-default routing is not synonymous with direct Monium ingestion and does
not establish a retention period. The accepted DEV policy is 31-day Monium
retention with no secrets or user content; verify actual destination, delivery,
retention and cost separately. Any DEV rollout waiver for an unqualified native
route must remain explicit and must not be described as observability acceptance.

The alternatives are explicit: a compatible documented management endpoint may
retain Terraform ownership if proven. Direct bounded OTLP is deferred unless
native delivery fails and a separate reviewed lifecycle/auth design is accepted.
No authentication migration, application-data change or rollout permission is
implied by this infrastructure handoff.

Human-authorable policy lives in these files:

- [queries.yaml](../observability/queries.yaml): named metric/type/unit/selector
  contracts and qualifications, not private resource values.
- [dashboards.yaml](../observability/dashboards.yaml): the five overview sections,
  panels and explicit gaps, with logical query/runbook references.
- [alerts.yaml](../observability/alerts.yaml): conditions, windows, delay, demand,
  freshness, both NoData policies, recovery and notification references.
- [cloud-dev.yaml](../observability/profiles/cloud-dev.yaml): selected queries,
  selected rules and finite read/cardinality limits.

Do not maintain a second handwritten Terraform or JSON policy. Generated schema,
normalization, dashboard input and alert console sheet come from the same typed
YAML contract. `schema_version: 1` is explicit. Unknown/duplicate fields or IDs,
aliases/merge expansion, tags, multi-document/trailing input, ambiguous scalar
coercion, excessive nesting/size, unsafe strings and missing references fail
validation. Durations carry units; rate samples are not event counts. Fields
cannot silently default NoData into zero, idle pages or automatic semantic OK.

`series_aggregation` is separate from grid/time `aggregation`. The two gateway
activity queries preserve #223's `series_sum` across matching path/code/release
series at each instant: one aggregate rate, with no raw path labels exposed.
It does not sum rate samples over time into an event count. Other queries use
explicit `none`; this adapter accepts no arbitrary query-language expressions.
Queue and semantic notification safety follows the underlying query family,
not a caller-chosen display class that could bypass the recovery restriction.

The parser's structural schema is machine-readable; semantic validation also
checks references, unit/aggregation compatibility, policy and budget invariants.
Passing structural validation alone does not qualify a selector or deployment.

## Offline commands

Use documented Make entry points, which share the repository Go caches:

```sh
make observability-config-check
make observability-config-render
make observability-config-schema
make observability-config-test
make observability-config-fuzz
```

The default render destination is `.build/observability-preview`. It must be a
new directory: the tool never overwrites an earlier reviewed export. For another
render, select a new path with `OBSERVABILITY_OUTPUT`; schema export similarly
uses a new `.build/observability-schema` or `OBSERVABILITY_SCHEMA_OUTPUT` path.
It contains
private-permission preview/normalization/operator-sheet files, no executable
Terraform resource without explicit bindings and ownership evidence. `schema`
exports the structural schemas; generated schema files are not a parallel
hand-maintained policy source. The helper is development-only and excluded from
the deployed component/image list. The ordinary Go suites cover its packages;
`ci-core` also validates the checked-in configuration. No cloud credential is a
test dependency. All real rules in the initial `cloud-dev` profile are disabled.

### Isolated synthetic canary

[#228](https://gitcode.com/urandon/sessionless/issues/228) also needs a finite
platform alert-to-operator proof. Its [accepted engineering design](design/observability-canary.md)
uses a separate closed [canary manifest](../observability/canary.yaml), not a
custom-service exception in production `Config.Validate`. It has one synthetic
dimensionless DGAUGE, fixed metric namespace and one private experiment nonce;
there are no tenant/session/run identifiers or arbitrary labels. Both missing
series and missing points are NoData; no idle notification or periodic repeat.
Normal → breach → fresh clear → idle describes metric-condition recovery only,
never canonical/application recovery or coverage of both real execution paths.

```sh
make observability-config-check
make observability-canary-render
make observability-canary-schema
```

`observability-config-check` validates both the normal catalog and canary YAML;
the focused parser/export tests cover both. Canary render defaults to the new
private directory `.build/observability-canary-plan`, schema export to
`.build/observability-canary-schema`. Override `OBSERVABILITY_CANARY_INPUT`,
`OBSERVABILITY_CANARY_OUTPUT` or `OBSERVABILITY_CANARY_SCHEMA_OUTPUT` explicitly
for another source/export; no existing reviewed output is overwritten.

The dedicated CLI operations are:

```sh
go run ./cmd/observability-config canary-validate --input observability/canary.yaml
go run ./cmd/observability-config canary-render --input observability/canary.yaml --out .build/observability-canary-plan
go run ./cmd/observability-config canary-schema --out .build/observability-canary-schema
```

An explicit bound planning render additionally accepts `--bindings` with an
owner-only private YAML and `--now` with the exact RFC3339 evaluation instant.
The [example binding](../observability/canary-bindings.example.yaml) is
deliberately incomplete/expired, not an admission input. Fresh operator
scope/collision/channel assertions remain assertions, not a tool-acquired live
receipt. Missing nonce/destination cannot become an ambient or wildcard target.
The already-confirmed logical channel is `operator_primary`; contacts and device
tokens do not belong in these artifacts. No second channel is created.
Actual sent states and repeat are configured on the test alert's notification
attachment, not the existing channel form. They are deliberately absent from
private preflight and exported as `PENDING_RULE_READBACK`; otherwise a not-yet
created rule would require a fictional completed readback before plan rendering.
Desired Alarm/OK-only, repeat-off and `noise_suppression: false` policy remains
strictly validated. These are desired settings, not existing-channel facts.
In the unsaved alert form inspect the Notifications table's method/status/repeat
and the default-methods banner; explicitly set both NoData cases, Maximum and
no gap filling rather than retaining Default/Manual, Auto or All-values defaults.
Channel IDs are bounded opaque private data: the tool does not infer the
provider's ID grammar or ownership from a prefix. The operator's explicit
`channel_scope_confirmed` assertion records the exact-project readback; it is
not independent scope verification by the renderer.

Phase durations are upper budgets, not scheduled waits. Advance immediately
on the required observation, except the full twenty-minute idle. Fresh zero and
native normal must be rechecked immediately before arming and again before
breach. Use the supported unsaved **Check result** before separately authorized
**Create**; actual persisted rule/attachment readback is mandatory before the
first breach. A saved-unarmed workflow is not assumed. Unsupported controls,
uncertain save or drift stop incomplete without a blind retry. If configuration
consumes baseline freshness, stop or revise within the
original write/time limits; do not silently add refresh samples. Generated
cumulative duration budgets provide latest aggregate accounting bounds, not
scheduled start offsets.

Outputs `canary-normalized.json`, `canary-console.json` and `canary-plan.json`
come mechanically from the same YAML and explicit bindings. They identify a
separate generator version, schema, input digest, private binding revision and
PLAN_ONLY authority. Both bound and unbound exports are inert: no Terraform,
writer payload, credential acquisition, API call, notification or apply.
`canary-schema` exports the closed policy and private-binding structural
schemas. Production schema version/export/`compare` behavior is unchanged;
ordinary `compare` rejects canary envelopes instead of inventing live MATCH.

The plan accounts for the aggregate twelve one-point writes, forty read
requests, ten thousand returned values, four planned notifications including
initial OK, ninety minutes including twenty-minute idle/cleanup, and at most
five RUB estimated incremental spend. These are planned ceilings, not a runtime
enforcer, guaranteed provider notification cap or verified bill. MAX recovery
requires the breach to leave its delayed window plus fresh clear observations.
UI-internal polling, delivery retries/duplicates and missing receipts stay
explicit execution-gate limitations. A later experiment needs exact supported
surface/readback, current tariff/push cost/IAM qualification, ownership checks,
operator-ready window and separate finite authority. No dummy VERIFIED source
or fabricated dashboard ownership is required merely to export this plan.
Published Russia tariff components give approximately 0.0791 RUB incremental
for twelve custom values, ten thousand API-read values and one leaf evaluated
for ninety minutes. Native push's zero incremental delivery estimate is an
inference from [Monium's listed chargeable components](https://yandex.cloud/ru/docs/monium/pricing),
not a verified invoice or another notification service's tariff; no advance
account-charge proof is required to prepare this bounded estimate. Actual live
scope and material tariff changes still require budget admission. If cleanup
only detaches notifications or mutes the test, evaluation billing can continue
(about 1.08 RUB per thirty days for one leaf). Record that retained lifetime
until actual removal or a supported evaluation-disable control is proven;
neither deletion nor a fictional disable API is authorized here.

The CLI supports explicit offline inputs:

```sh
go run ./cmd/observability-config validate --root observability --profile cloud-dev
go run ./cmd/observability-config render --root observability --profile cloud-dev --out .build/observability-preview
go run ./cmd/observability-config schema --out .build/observability-schema
go run ./cmd/observability-config compare --expected .build/expected.json --actual .build/actual.json --out .build/observability-preview/drift.json
```

Use the Make entry points normally so cache paths stay shared. The explicit
commands describe the helper interface, not permission to perform a cloud
operation. `compare` compares complete normalized offline snapshots, not a
native Monitoring API response or live discovery. Missing/extra/changed settings
are drift, never silently reconciled. The operator must obtain and normalize an
actual supported readback under [#228](https://gitcode.com/urandon/sessionless/issues/228).
Comparing two copies of generated desired configuration proves no deployment.

## Binding and ownership gate

Exact folder/resource/revision/project values and contacts stay outside public
Git. Do not copy the private #223 receipts, object URLs, raw paths, identifiers,
session data, secrets or recipients into public configuration or CI fixtures.
The four public documents use logical references. A private bindings document
contains an explicit revision, destination folder and per-query dated type/unit,
exact label values and qualifications. It cannot change reviewed public metric,
service, label key set, fixed labels, aggregation or rule policy. No wildcard,
ambient folder/environment fallback or credential-store search is allowed.

Bound rendering additionally requires an explicit evaluation timestamp and a
fresh complete private ownership receipt for the same folder/stable dashboard
name. New-resource creation requires no colliding dashboard. Import/adoption
requires the exact known Sessionless-owned ID and full snapshot digest; unknown
ownership, incomplete listing, concurrent editor or stale evidence aborts.
Private query evidence must be fresh, match the exact reviewed type/unit and
include every required qualification. This validates a supplied receipt, not
the trustworthiness of its author, live IAM or permission to apply.

The [binding example](../observability/bindings.example.yaml) and
[ownership example](../observability/ownership.example.yaml) are deliberately
incomplete/expired and not valid deployment inputs. They explain the shape
without exposing real values. Filled copies belong outside Git in owner-only
regular files; the tool rejects permissive or symlinked private input files.
Use explicit `--bindings`, `--ownership` and `--now` only for a separately
authorized private export. No example supplies a recipient or credential.

The pinned Yandex provider 0.220.0 can automatically adopt a unique same-name
dashboard; update replaces complete widget/parameter/label sets. Name/folder
changes replace the resource. Keep stable identity, `prevent_destroy`, a full
prior snapshot and sole provisioning ownership. Inspect the full saved plan and
existing approved Terraform state mapping before any separately authorized
apply. Do not create an untracked parallel state/workspace or run a blind import.
No targeted-deploy exception or cloud-write authority follows from this tool.

Outputs containing resolved bindings are operator-private, including the
Terraform JSON resource input and normalized snapshot. They must not enter
public artifacts, logs, MR attachments or generated snapshots. Output is
written to explicit private-permission paths, not printed with private values.
Keep input/schema/generator/binding digests for reproducibility; a hash identifies
evidence and never grants rollout authority. Rendering itself applies nothing.

## Native support and explicit gaps

Native request, invocation, queue and size families reuse the dated
[#223 inventory](https://gitcode.com/urandon/sessionless/wiki/Cloud-Dev-Observability-Deployed-Inventory-2026-10-21.md).
`VERIFIED` describes that bounded query receipt, not perpetual metric presence,
freshness, a healthy component or a configured alert. Fresh private bindings
are still required. An empty control request window and absent worker series
must remain NoData, not zero/success. Sparse request charts show points, not
connected/forward-filled success. Disabled/unexercised routes remain explicit.

- Bucket size selects `storage_type: All` alone. Component storage series and
  All overlap; never sum them. YDB logical size is not physical quota usage.
- p95 needs histogram semantics plus qualified count/integration. Inflight alone
  is not saturation. Capacity ratios require compatible fresh used/limit values.
  This first adapter cannot activate a made-up ratio from a raw size series.
- Queue `oldest_age_milliseconds` is measured in **seconds**. Queue visibility
  and redrive attributes are not a trigger override or timer DLQ setting.
  Qualify actual attributes and original route allowances before an age rule.
- Monium log-derived application signals remain conditional on actual project,
  routing, query/alert support, IAM, retention and tariff qualification. Classic
  dashboard support proves none of those. No runtime Monitoring editor grant,
  silent OTLP fallback or new collector is introduced.
- Dispatch/ACK, worker terminal, result publication, cleanup and durable
  canonical completion are distinct. Existing authorized Run/Attempt diagnostics
  remain the drill-down; missing progress observations are not green counters.
- Issuing a download capability does not observe direct bucket GET success.
  Upload URL expiry and multipart cleanup do not erase a fully PUT object.
  [#57](https://gitcode.com/urandon/sessionless/issues/57)/
  [#163](https://gitcode.com/urandon/sessionless/issues/163) owns that existing
  lifecycle gap; no new deleter or broad deletion-MVP gate is added here.

Dashboard export uses only the supported classic resource schema. Alerts and
channel settings produce an operator console sheet, not invented native CRUD or
Terraform resources. Configure/read back through the actually supported surface
only after separate authority. Never claim all settings are automated by YAML.

## Operator runbooks

The YAML `runbook_ref` names below are stable logical anchors. Private deep links
may be bound by the operator; public notifications use only safe generic links.

### deployment

Check declared source/image/config/profile timestamp against actual admitted
deployment. Paused triggers and disabled profiles are not healthy from silence.
Do not turn observability configuration into runtime activation permission.

### web_ingress

Compare the selected gateway and active container revision. Check actual error
classification and request activity before claiming recovery. An HTTP200 or303
does not prove login, persistence, upload or canonical result success. Never log
raw paths/query parameters, cookies, account claims or capability URLs.

### web_semantics

Expected expiry, denied membership and CSRF/provider rejection are not outages.
Unexpected service failure requires owning component classification. Confirm a
successful equivalent operation privately; a missing event is not recovery.
No automatic semantic OK notification until a positive predicate is qualified.

### canonical_progress

Inspect existing authorized custody/finalization diagnostics and the original
Job/lease/cleanup deadline. Confirm canonical outcome positively. No alert or
unknown result authorizes another model/tool send, replay or longer deadline.

### managed_route

Check the explicitly selected managed profile, substrate and canonical terminal
evidence. A deterministic/local demo is not managed cloud proof. No attached
fallback or new provider effect is implied by an alarm.

### attached_route

Check selected worker transport freshness and the existing terminal receipt
chain. Do not page on an idle unselected host or treat ACK as canonical commit.
Do not change enrollment/credentials or contact another machine from this tool.

### queue_backlog

Inspect source queue and consumer/trigger separately. Bind exact visibility,
permitted retries and original processing allowance in seconds. Never blindly
redrive, purge or turn backlog decrease into evidence of canonical progress.

### queue_dlq

Preserve evidence and reconcile ambiguous effects through existing authority.
Fresh empty occupancy may clear the vendor condition but is not semantic
recovery. Automatic OK is disabled; operator disposition stays separate.

### ydb_capacity

Inspect fresh compatible limits, throttling and typed transaction outcome.
Logical database size alone is diagnostic. A cleared metric cannot reconcile an
ambiguous commit. Never reset tables or expand RU limits from an alert.

### bucket_capacity

Use All only and a compatible positive limit before computing utilization.
Do not open user objects/capabilities for diagnosis. Capacity clear does not
prove application I/O or cleanup completion.

### telemetry_gap

Both no matching series and no points are NoData. Do not fill with zero or last
success. Idle NoData does not notify. Record the missing binding/qualification,
owner and next bounded action; no automatic replay, new observer or fallback.

## Separately authorized activation and proof

[#228](https://gitcode.com/urandon/sessionless/issues/228) owns actual supported
surface/readback, IAM/project/billing qualification, recipient confirmation,
full reviewed plan and the finite isolated rule-to-operator experiment. Preserve
the design's limits and stop/rollback procedure. A channel-test button is not
rule-to-receipt proof. Dashboard-only success does not complete #19/#35 or prove
semantic recovery for both real MVP execution routes.
