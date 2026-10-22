# Execution outcome observations at existing owners

Owner: [#227](https://gitcode.com/urandon/sessionless/issues/227), under
[#19](https://gitcode.com/urandon/sessionless/issues/19). Contract:
[operational signals](operational-signals.md) and accepted
[observability design v0.1.1](https://gitcode.com/urandon/sessionless/wiki/Cloud-Dev-Observability-Design.md).
This is source integration, not a cloud qualification or runtime activation.
The YAML remains the operator-facing policy; no second policy, exporter, state
store, scheduler, table or deployment is introduced.

## Qualified hook and evidence matrix

| Route / observation | Existing owner and evidence | What it does not prove |
| --- | --- | --- |
| Managed execution failure | Worker Manager's actual execution result; local-owned V2's attempted model operation. Expected cancellation and admission refusal are not provider outages. | Preflight, effect reservation and dispatch do not prove execution occurred. |
| Managed publication failure | Existing output/canonical-result publication or local `PublishOutput` returns an unexpected failure. | Prepared-output admission refusal is not a backend outage. A published object or retained custody does not prove canonical completion. |
| Managed cleanup failure | Existing invocation scratch cleanup or concrete local terminal cleanup/close reports failure. | A diagnostic record is not a cleanup receipt and does not authorize retry. |
| Managed cleanup success | Concrete local V2 terminal cleanup and original session close both succeed. | This is original operation-namespace cleanup, not upload retention, canonical completion or scratch-only V1 cleanup. |
| Managed canonical unknown | Existing completion operation is ambiguous or its existing scoped readback is unavailable, mismatched or nonterminal. | No new read, recovery action or replay authority follows. A later wake/ACK failure cannot downgrade already observed success. |
| Managed canonical success | Existing local V2 post-completion `LoadWorkerJob` reads the same tenant/Run/Attempt and actual `RunSucceeded`; an already-terminal canonical queue delivery can also observe its existing scoped successful read. | `CompleteWorkerJob == nil`, `OutcomeCompleted`, cancellation, queue ACK and model return are not success observations. Generic V1 has no immediate completion readback; it remains unobserved until an existing qualifying read occurs. |
| Managed deadline exceeded | An active owning operation observes the original execution or fixed terminal-cleanup deadline expiring. | Not a disappeared-process detector. No replacement timeout or telemetry grace period exists. |
| Attached transport failure | Existing exchange connection read or attempt-broker exchange returns an unexpected backend failure. | This is not exhaustive transport coverage. Acceptance, heartbeat, empty polling and expected denied/stale/expired requests do not imply execution success or infrastructure failure. |
| Attached publication failure | Server-owned output-receipt creation returns an unexpected backend error. | Receipt creation is not canonical publication. |
| Attached canonical success / execution failure | The real terminal finalizer returns after its canonical transaction; its typed current attempt must exactly match the authenticated binding, committed state and terminal evidence. Its authoritative succeeded/failed outcome selects the observation. | The inbound worker Terminal or protocol acceptance/ACK alone is insufficient; cancellation is not successful answer publication or an outage. |
| Attached canonical unknown | A failed terminal-commit call leaves canonical outcome uncertain. | Rejection is not success; no log record can permit resending an external effect. |

Emission is outside retryable database transaction callbacks. These hooks do
not add reads, advance protocol state, refresh leases, change custody or alter
queue ACK/retry decisions. Historical retained-ledger replay can intentionally
return no current singleton Attempt; missing current evidence must not be
invented from the inbound frame or labelled a fresh unknown incident. This
narrow hook may omit such a replay observation without affecting its response.

One process-owned stdout sink is shared by worker Manager and its existing
queue/wake boundaries, and by control API's attached transport/receipt and
optional Telegram boundaries. Nil sinks keep isolated callers disabled.
Idle/disabled routes emit no synthetic successful observations. Duplicated or
reordered delivery may duplicate observations; there is no finish-minus-start
counter, exact-once billing, recovery state or causal ordering promise.

## Original deadline and missing-owner disposition

Use the context of the existing operation. Where an inner execution deadline
has expired but its original outer invocation is still live, a deadline
observation can use that original outer context; it never replaces execution
authority. `deadline_state` describes the emitting context, not a reconstructed
Run clock. No new detached context, deadline, wait or after-response flush is
created. Expired/canceled observations can be dropped. The stdout limitation in
the shared contract still applies: a sink may delay the caller and downstream
authority checks retain their original deadlines.

The following are **unavailable**, not zero stuck work or semantic OK:

- Managed disappeared-worker detection: the existing scheduler expires quota
  reservations but skips running Runs. Its quota expiry is neither a route
  identifier nor an execution deadline. A live invocation's lease watchdog
  cannot report its own disappeared process. The execution/reconciliation owner
  must qualify any future bounded active-run observer; #227 adds no scanner.
- Attached disappeared-worker/deadline detection: bounded due-deadline/fence
  ports exist, but accepted source has no production scheduled caller. No
  `attached.deadline_exceeded` hook is activated from heartbeat or quota expiry.
  Existing execution owners under [#77](https://gitcode.com/urandon/sessionless/issues/77)
  retain this qualification; neither #227 nor a dashboard adds a poller.
- Attached cloud cleanup failure: host-local cleanup exists, but no qualified
  server-negative cleanup result exists. `attached.cleanup_failure` remains
  inactive; worker-supplied positive receipt evidence is not a missing-cleanup
  observer. No new channel or release authority is implied.
- Real managed source composition: accepted `cmd/worker-runtime` selects a
  deterministic V1 fixture; local V2 is explicitly composed by connected tests.
  Source hooks do not qualify a real model or no-user-host execution. Normal
  [#190](https://gitcode.com/urandon/sessionless/issues/190)/
  [#193](https://gitcode.com/urandon/sessionless/issues/193)/
  [#218](https://gitcode.com/urandon/sessionless/issues/218) handoffs own their
  integration; unpublished work is neither consumed nor marked delivered here.
- Expired-upload residual cleanup: the accepted
  [retention/replay contract](design/session-retention-replay.md) is delivered
  through closed [#159](https://gitcode.com/urandon/sessionless/issues/159),
  despite historical draft wording (see the
  [acceptance receipt](https://gitcode.com/urandon/sessionless/issues/159#note_193643620)). Existing
  [#57](https://gitcode.com/urandon/sessionless/issues/57)/
  [#163](https://gitcode.com/urandon/sessionless/issues/163) own implementation.
  URL expiry or multipart cleanup does not erase a fully PUT object. No new
  deleter, reopening of #159 or broad deletion-MVP gate is introduced.

## Verification and rollout handoff

`make ops-signal-test` covers deterministic isolated service/Manager fixtures,
closed-schema/privacy checks, race detection and repeated shuffled signal
tests. It opens no database/container/cloud client and calls no model.
Assertions must compare returned outcomes and existing backend/queue calls as
well as emitted records: ACK without materialization, failed/ambiguous commit,
foreign/stale evidence, cancellation, cleanup before canonical failure,
duplicate/lost observations and original-expiry behavior cannot become false
success. Connected local-V2/YDB runtime proof remains with its existing tests
and exact-head CI, not a fake service result or helper-only unit test.

All ordinary CI packages and nine required exact-head jobs remain unchanged.
No schema/entity change requires migration or rollout. Source checks do not
prove deployed stdout ingestion, native Monium support, alert transitions,
notification delivery, direct bucket GET coverage or a real execution route.
Those stay separately authorized under
[#228](https://gitcode.com/urandon/sessionless/issues/228); #19/#35 remain open.
Both MVP execution routes remain required; Telegram rollout stays deferred.
