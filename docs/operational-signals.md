# Content-free operational signals

[#226](https://gitcode.com/urandon/sessionless/issues/226) implements the Web,
queue and wake portion of the accepted
[cloud-dev observability design v0.1.1](https://gitcode.com/urandon/sessionless/wiki/Cloud-Dev-Observability-Design.md).
It does not provision metrics, alerts, notifications or Cloud Logging resources.

## Record and carrier

`internal/opssignal` owns `ops_signal_v1`: fixed `schema_version`, `signal_id`,
`component`, `route`, `stage`, `outcome`, `error_class` and `deadline_state`.
The entire combination is validated against compiled constants before a sink
receives it, and again before the standard writer encodes it. Unknown values,
incompatible combinations and inconsistent context/deadline metadata are
dropped. There are 22 named signal slots and two reserved, inactive slots;
only `signal_id` may become a derived metric dimension. The schema intentionally
has no arbitrary attributes, IDs (including hashes), raw errors, paths, URLs,
filenames, prompts, model output, cookies or credentials.

The standard sink writes one newline-framed JSON record, at most 768 bytes,
through the process's existing stdout/Cloud Logging carrier. Composition shares
one writer per process. A short write or writer error is not retried. A nil sink
disables observations in existing callers and isolated fixtures. Custom injected
sinks are trusted in-process implementations: their returned errors are ignored;
an arbitrary sink panic is not recovered or treated as a supported failure mode.

The original operation context is passed unchanged. Canceled observations may
be dropped; no detached context, new deadline, exporter goroutine, spool,
shutdown flush or after-response delivery promise exists. A context cannot
cancel an in-progress OS stdout write or the mutex wait for that writer. Record
size and boundary emission counts are bounded, **not** write completion latency
or aggregate traffic/cost. A blocked writer can delay the operation; all existing
downstream authority/expiry checks still apply using their original contexts.
No observation grants an effect, extends a Job/cleanup deadline, retries a queue
operation or reconstructs canonical state. Duplicate operations may yield duplicate
records; these records are not exactly-once counters, billing or recovery proof.

## Boundary ownership

| Owner | Signals and actual boundary |
| --- | --- |
| Web BFF | `web.auth_failure` for unexpected login/auth-store/provider failures; `web.auth_success` only after authenticated session persistence; `web.session_failure` for canonical session service operations. |
| Web application service | `web.upload_failure` distinguishes intent persistence, presign, stat verification and commit; `web.session_failure` distinguishes message/canonical work; `web.artifact_access_failure` covers attachment and run-artifact capability issuance. |
| Outbox wake publisher | `control.wake_deferred` after a failed wake publish; callers retain committed success and existing recovery policy, including canonical ingress replay. |
| Existing scheduler | `control.dispatch_failure` on unexpected failed dispatch/pass invocation, never a normal policy/quota/non-admission/parked decision. |
| Queue boundary decorator | `control.queue_failure` on failed publish, receive, ACK, retry or dead-letter operation, with a finite stage, not envelope/receipt/reason data. |
| Serverless HTTP ingress | `control.queue_failure` at a failed invocation; response/retry ownership remains unchanged. |

One record is attempted per observed failed boundary (auth success has its own
single persisted boundary). Separate genuine boundaries may each report the same
propagated infrastructure incident: e.g. queue publish, scheduler invocation,
and trigger invocation. Do not sum them as independent failures or jobs. No record
is emitted per token, chunk, successful empty receive, expected quota decision or
ordinary polling iteration. Existing bounded batch/candidate loops are unchanged.

Expected validation, authorization, missing membership/resource, login denial,
expiry and policy decisions do not become outage signals. Internal typed provider
classification separates transport/service failures from rejected credentials
without weakening public fail-closed authentication. Shared typed queue control-flow
sentinels preserve adapter identities: empty receive, requested trigger retry and
pending redrive are not failed queue infrastructure. Legacy correlation/raw logs
are unchanged and are **not** inputs to this signal schema or derived metrics.

## Deliberate gaps and handoff

Presign issuance does not prove a browser uploaded bytes. An upload commit is not
canonical attachment publication. Issuing a download capability does not observe
the subsequent direct browser-to-bucket GET; GET failures remain explicitly
uncovered until [#228](https://gitcode.com/urandon/sessionless/issues/228) qualifies
native coverage under [#19](https://gitcode.com/urandon/sessionless/issues/19).

The seven managed and seven attached slots are a shared immutable vocabulary for
[#227](https://gitcode.com/urandon/sessionless/issues/227), not implemented route
observations here. Their future owner must prove actual canonical/publication,
cleanup, deadline and unknown-state predicates without a second poller or replay
authority. A successful ACK, offer or dispatch is not canonical execution success.
Both execution routes remain required for MVP; Telegram rollout remains deferred.

`make ops-signal-test` verifies the local contract and deterministic injected
failures. Exact-head CI and independent review are implementation evidence, not
proof of stdout ingestion, log-derived metric availability, notification delivery
or deployed alert recovery. Those need separately authorized #228 qualification.
