# Offline synthetic observability canary

Version 0.1.0, engineering extension for #228 under #19. Consumes the accepted
[native observability design v0.1.1](https://gitcode.com/urandon/sessionless/wiki/Cloud-Dev-Observability-Design.md)
sections 3.1 and 10 and [YAML tooling](../cloud-dev-observability.md).
This document and its implementation authorize no live operation.

## Problem and smallest supported change

#228 requires one finite synthetic metric-condition → alert → human receipt →
fresh clear → recovery receipt → idle proof. Production `Config.Validate`
intentionally rejects arbitrary custom services and requires a five-section
dashboard. Bound `Render` requires dashboard ownership and emits Terraform.
Reusing that path for a synthetic gauge would require widening production
guards, fake VERIFIED evidence or an irrelevant dashboard. Reject those options.

Add one separate closed `CanaryDocument` to the existing offline
`observabilityconfig` package and CLI. Reuse hardened typed YAML decoding,
bounded private file handling, deterministic hashing and exclusive private
output. Leave production configuration, dashboard export, semantic/queue
recovery restrictions and the default zero-enabled-alert profile unchanged.
No writer, API client, network, credential discovery, scheduler, collector,
Terraform resource, apply operation or normal-config custom-service exception.

The checked-in `observability/canary.yaml` is the single desired-policy source
for this experiment. It is not a parallel handwritten console policy. Separate
CLI operations validate/render it and export its structural schema. The output
contains a generated console sheet and finite experiment plan, never a live
receipt or permission. A human operator must use the actually supported UI;
there is no documented native alert/channel CRUD adapter in this implementation.

## Closed experiment contract

- Exactly one explicitly synthetic, dimensionless DGAUGE, service `custom`,
  fixed name `sessionless_obs228_canary`; exactly one `instance` label besides
  service/name. No map of arbitrary labels or expressions.
- The instance is a private 32-character lowercase hexadecimal experiment
  nonce, not a tenant/user/Session/Run/Attempt identifier or a hash of one.
  It is generated independently, never inferred from user material.
- Private bindings resolve the exact folder/project and already-confirmed
  native push channel through logical `operator_primary`, not a second channel.
  Rule name and query identity derive mechanically from the same nonce.
  No wildcard, ambient environment substitution, escaping injection or scope
  broadening. Catalog/collision/ownership preflight precedes any later write.
- Evidence is always `planned_not_observed`; no `VERIFIED` switch. A dated
  operator preflight assertion is not an independently verified observation.
  Bound export still says PLAN_ONLY and supplies no activation authority.
- The fixed test values are normal `0`, breach `1`, clear `0`. Rule uses
  Maximum, `> 0.5`, a two-minute window, one-minute evaluation delay and
  three-minute freshness bound; one series, no decomposition or gap filling.
  These are test parameters, not measured platform latency or changed real-rule
  defaults. Missing series and missing points both become explicit NoData.
- Native metric clear is only synthetic metric-condition recovery, never
  canonical/application/queue recovery. NoData/stale data never establish OK.
  MAX cannot clear until the breach leaves the delayed evaluation window;
  require fresh zero samples, native clear state and operator recovery receipt.
- Alarm and OK are the only planned notification states; no periodic repeats,
  default-severity subscription, escalation or idle NoData notification. Initial
  OK is included in the notification budget, not silently assumed suppressed.
  Desired `noise_suppression: false` prevents transition coalescing for this
  bounded test; an unchecked UI default is not a persisted settings receipt.
  Inspect/read back the existing channel's actual policies before arming;
  any correction needs its own exact shared-channel write authority.
  Sent statuses and repeat belong to the test alert's notification attachment,
  not the existing channel's edit form. Actual values remain
  `PENDING_RULE_READBACK` in every offline export; they are not preconditions
  to rendering a plan for a rule that does not yet exist.

## Private planned binding and output

Explicit owner-only binding input supplies revision, exact folder and matching
folder-scoped Monium project, channel identity, synthetic nonce and dated
operator assertions: native push/one confirmed recipient/no default severity
subscription/sole experiment owner/complete collision preflight with no conflicting
rule or series. Do not include credentials, contact values or device identifiers.
Channel IDs are bounded opaque private values, not an inferred provider grammar
based on one observed prefix. An explicit `channel_scope_confirmed` operator
assertion joins the actual existing channel readback to this exact project;
the parser does not discover or independently verify that scope.
Missing, stale, future, mismatched, permissive or symlinked private input fails.
Example binding is deliberately incomplete/expired and not live admission.

Unbound export is useful for review, but no exact selector/target is invented.
Bound export includes the exact same mechanically derived selector for rule
and read/write identity instructions. No executable writer payload is emitted.
No artificial dashboard receipt or Terraform output. Metadata identifies
schema/generator version, normalized input digest, private binding revision/
digest and explicit validation time, with PLAN_ONLY authority.

Normal `compare` does not accept canary artifacts. No comparison of copied
desired values is called actual readback. Existing production schemas/version
must remain backward-compatible; give canary outputs their own generator
version rather than silently invalidating previous dashboard envelopes.

## Finite plan, not an executor

The generated plan describes actors, concrete observations and stop conditions:

1. Fresh scoped ownership/collision/channel/configuration and cost checks;
   exact later authority required before any live action.
2. Up to four fresh baseline zero samples, exactly a minute apart; observe
   the expected native series and a qualified normal predicate, then advance
   immediately. The phase duration is an upper budget, not a required wait.
3. Prepare the unsaved exact test-alert form with both NoData policies and the
   intended existing channel's attachment: Alarm/OK only, repeat off. Use the
   documented **Check result** before **Create** to inspect fresh zero usable
   in the delayed evaluation window and calculated native normal. Separately
   authorized Create/arming may emit the budgeted initial OK. Read back the
   persisted exact rule, channel and notification settings after saving; an
   unsupported control, uncertain save or drift stops incomplete with no blind
   retry and only separately authorized owned cleanup. A saved-unarmed creation
   workflow is not assumed or required.
   The unsaved form exposes method/status/repeat in its Notifications table and
   noise suppression separately. Inspect its default-methods banner as well as
   explicit rows: no extra effective destination may be armed. Set both absence
   policies explicitly to NoData (not Default/Manual), Maximum condition and
   downsampling, no gap filling, and noise suppression off; do not accept the
   form's Auto/All-values/fill defaults as this reviewed policy.
4. Before the first breach require that saved attachment readback, then
   immediately recheck fresh zero and persisted native normal. Up to four
   breach samples, exactly a minute apart; bounded query confirms
   condition, native Alarm, then actual operator push receipt.
5. Up to four fresh clear samples, exactly a minute apart; wait for the breach
   to leave the delayed MAX window, query/native clear and recovery receipt.
6. Stop writer, observe twenty minutes of idle without an idle page, then
   disable owned test notifications/rule and read back bounded cleanup.

Aggregate ceilings: twelve one-point metric writes, forty bounded observed
query/config-read requests, ten thousand returned values, four planned
notifications including any initial OK, ninety minutes including twenty-minute
idle and cleanup, estimated incremental spend at most five RUB. Sum per-phase
allocations and waits; do not multiply these ceilings by number of phases.
Reserve idle/cleanup before starting; missing evidence never extends the clock
or triggers a retry. All phase durations are upper budgets; advance immediately
when observations allow, except the genuine twenty-minute idle observation.
Generated cumulative duration budgets are latest aggregate accounting bounds,
not scheduled phase starts. A point older than three minutes or absent from
the delayed evaluation window is not a fresh baseline. If configuration consumes
that freshness before arming or breach, stop incomplete or revise within the
original write/time caps; never silently add refresh samples or restart budgets.
Missing/ambiguous write acknowledgement, unexpected extra
series/recipient, stale clear, budget/tariff uncertainty, wrong scope, no operator
receipt or unavailable controls stops the experiment as incomplete.

These are offline plan ceilings, not enforcement of cloud billing/delivery or
UI-internal requests. Platform notification retries, duplicates or delayed
messages may exceed intended transitions; stopping/disabling cannot retract
queued messages. Do not promise exactly-once or a hard recipient-delivery cap.
Unknown hidden console polling must be reconciled in the later execution
manifest, not counted as one HTTP request per displayed page.

The later live gate must freeze absolute phase deadlines and actual admitted
cost/IAM/query/write/notification surfaces. This tool never independently checks
those. Any cleanup that deletes/changes shared resources needs explicit exact
authority; metric retention is not immediate erasure or an invented delete API.
No faults against YDB/network/objects, no application/user request, model/tool
call, redrive, runtime rollout or broader IAM/key acquisition.

## Evidence and verification

Strict decoder and validation rejection tests cover malformed/missing/unknown/
duplicate/type/alias/tag/size cases, closed synthetic identity, changed values/
threshold/NoData/repeat, aggregate budgets and phase duration sums, private
scope/nonce/timestamp/preflight collisions, deterministic digests, private
stdout/error/output safety and CLI flag isolation. Add seeds to bounded fuzz
coverage. Existing production tests must still reject the synthetic service and
preserve `cloud-dev` disabled alerts. No network or wall-clock sleeps in tests.

Use existing Make-focused race, uncached, repeat/shuffle and fuzz entry points;
no full local runtime build. Exact-head normal CI and independent stabilized
artifact review remain merge gates. A passing fixture removes only the offline
tooling blocker: #228/#19/#35 stay open for actual activation, operator receipt,
conditional semantic ingestion and exercised real-route coverage. Both MVP
execution tracks, WebUI-first scope and owner priorities remain unchanged.

Primary design acceptance is recorded with independent original-goal-first
review; it is not owner review or authorization to send a notification.

## Provider references and unresolved live admission

- [Classic metric write contract](https://yandex.cloud/ru/docs/monitoring/api-ref/MetricsData/write)
  supports a typed DGAUGE with a fresh timestamp; an HTTP response alone does
  not prove the point was accepted or visible to an alert.
- [Metric read contract](https://yandex.cloud/ru/docs/monitoring/api-ref/MetricsData/read)
  supports MAX downsampling and gap filling NONE; proposed reads use sixty
  points, not a zero fill or stale-value carry.
- [Alert semantics](https://yandex.cloud/ru/docs/monium/concepts/alerting/alert)
  define evaluation windows and both NoData cases.
- [Notification methods](https://yandex.cloud/ru/docs/monium/concepts/alerting/notification-channel)
  include native mobile push, default-level subscriptions and periodic repeats.

These vendor contracts are not deployed capability/IAM receipts. The known
Monium Russia tariff gives an incremental estimate of approximately 0.0791 RUB:
12 custom values at 0.32 RUB/million, 10,000 API-read values at 7.686 RUB/million,
and one evaluated leaf for ninety minutes at 1.5 RUB/1000 leaf-hours. Estimated
native Cloud push delivery is zero incremental RUB, an explicit inference from
the published chargeable-component list (SMS and calls, not native push); this
is not an invoice, contractual guarantee or another notification service's
tariff. A bounded estimate does not require impossible advance proof of actual
account charges. The five-RUB ceiling still requires exact later live authority
and stops on material tariff/scope changes; it is not enforced billing.

The [channel creation instructions](https://yandex.cloud/ru/docs/monium/operations/alert/create-channel)
place statuses/repeat in the alert after the channel exists. The
[alert creation instructions](https://yandex.cloud/ru/docs/monium/operations/alert/create-alert)
document unsaved **Check result** before **Create**. These are the adopted
supported pre-arm checks, not a request to find missing channel-form fields.

Notification cleanup does not imply stopping evaluation. A detached route or
[exact-rule mute](https://yandex.cloud/ru/docs/monium/alerts/mutes) may suppress
delivery while the leaf keeps evaluating and billing; a finite mute expires.
Under the published rate one retained evaluated leaf costs approximately
1.08 RUB per thirty days. Record that separate lifetime/disposition until the
leaf is actually removed or an evaluation-disable operation is proven through
the supported surface. No delete API, permanent mute or evaluation-disable
control is invented or silently authorized by this plan.
