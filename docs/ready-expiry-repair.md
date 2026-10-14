# Ready and expiry index repair

`ready-expiry-repair` reconstructs one ready or expiry v2 index tuple from its
retained authoritative records. It is an operator command, not a serving API,
batch backfill, migration or external-effect retry. It does not renew leases,
send messages, dispatch workers or change source records, TTLs or cutover markers.

## Operator access

Select an explicitly authorized database through `YDB_CONNECTION_STRING` and
the existing [YDB environment credential chain](../migrations/ydb/README.md).
The database principal must already have the required access. `--tenant`,
`--run` and `--id` select records; they do not authenticate an application user
or grant tenant access. Never put credentials in flags, a DSN or the repository.
Running either mode against an environment requires separate authority for that
environment; a source check does not grant it.

## Commands

Supply all seven flags exactly once. The command accepts one locator and only
the kinds `lease`, `dispatch`, `quota` and `telegram`; there are no table overrides
or batch selectors.

```sh
make ready-expiry-repair ARGS="plan --tenant TENANT --kind lease --run RUN --id LEASE --timeout 30s --max-source-bytes 131072 --max-output-bytes 4096"
```

Inspect the bounded metadata JSON before replacing `plan` with `apply` for the
same authorized locator. `plan` issues no DML and does not lock in a later
decision. `apply` rereads and validates the sources in its own serializable
transaction, then UPSERTs only the selected v2 tuple. Concurrent source changes
can therefore make a previously successful plan ineligible.

| Kind | Authoritative records | Reconstructed tuple |
| --- | --- | --- |
| `lease` | Run, current lease head, matching Lease and Attempt | `lease_expiry_v2`, preserving original expiry and fence |
| `dispatch` | Run, pending dispatch outbox and matching Attempt | `dispatch_ready_v2`, preserving original creation time |
| `quota` | Run and held quota reservation with matching connection | `quota_expiry_v2`, preserving original expiry |
| `telegram` | Run and pending, retry-wait or sending delivery outbox | `telegram_delivery_ready_v2`, preserving its original availability time |

Every kind refuses a Session deletion fence, missing source, malformed payload,
or inconsistent physical key, scalar column or decoded owner. A past-due current
lease or held reservation remains past due; repair does not extend it. A
completed Run alone does not forbid legitimate unfinished Telegram delivery.
Other family terminal states are not resurrected. Repeating repair of an
unchanged eligible source produces the same tuple, rather than a new execution.

## Work and output limits

The timeout must be greater than zero and no longer than ten minutes. One
original deadline covers opening the dependency, transaction retries and closing
the connection. At most three source snapshots are admitted, with fifteen total
point reads and no more than five reads per admitted snapshot. Additional SDK
callbacks are refused before source SQL; counters do not reset on retry.

`--max-source-bytes` must be between 1 and 131072 bytes per JSON source cell.
SQL returns a byte-capped prefix with an overflow sentinel before decoding;
malformed UTF-8, overflow and JSON nesting deeper than 64 are refused. ID and
status cells have separate bounded prefixes. The cumulative reserved string
budget is `3 * (3 * (B + 1) + 19 * 161 + 2 * 33)` bytes for source cap `B`,
including failed reads and retries. These are refusal and returned-data limits,
not measurements of server I/O, RU, SDK buffers or total process memory.
[YQL string functions](https://ydb.tech/docs/en/yql/reference/builtins/basic)
use byte offsets; timestamp comparisons use YDB's
[microsecond precision](https://ydb.tech/docs/en/yql/reference/types/primitive).

`--max-output-bytes` must be between 1 and 4096 bytes, including the JSON newline.
Output contains only kind, tenant, Run, selected ID, bucket, due time, optional
fence and outcome. Both `applied` and the longer
`possibly_applied_metadata` variant must fit before DML. Diagnostics use a
separate constant stderr message bounded to 128 bytes; no driver errors, source
payloads, DSNs or credentials are echoed. Arguments are limited to fifteen,
256 bytes each and 4096 bytes in total.

## Ambiguous completion

An error after an attempted UPSERT can mean metadata was committed. The command
returns a nonzero status and, when output is available, the outcome
`possibly_applied_metadata`. A later retry refusal does not erase an earlier
write attempt. Likewise, an output or connection-close failure after apply is
not proof of rollback. Reinspect the exact locator and sources before another
metadata repair; never use this result to authorize model calls, uploads,
dispatch, message delivery or other irreversible replay.

## Upgrade and schema boundary

Before deploying v2-only code over retained work, use the compatible
dual-writing predecessor to finish the four-family backfill, verify exact
tuples and drain old readers and writers over the configured lease and retry
window. The lifecycle cutover marker does not prove that handoff. The owning
[#206 execution comments](https://gitcode.com/urandon/sessionless/issues/206)
pin the predecessor and record environment-specific verification and rollback.
This one-locator repair is not a replacement for the upgrade inventory.

The four legacy CREATE records and reset ownership remain. The entity registry
still contains 105 CREATE tables; physical retirement belongs to
[#207](https://gitcode.com/urandon/sessionless/issues/207), with its own rollout
and recovery controls.

## Verification

```sh
make ready-expiry-repair-test
make entity-registry-check
make entity-registry-test
```

The operator test target runs offline CLI and store race tests, without opening
a database. It removes the inherited DSN, forces native database opt-ins off and
disables module and toolchain downloads. The local Go toolchain and modules must
already be available in the shared caches; missing prerequisites fail offline.
It also overrides private-module proxy bypasses and disables direct VCS access,
following the [Go module download controls](https://go.dev/ref/mod#privacy).
The existing gated YDB CI suite exercises actual SQL using unique
test-owned rows and bounded cleanup in a disposable database. Fake transaction
tests or compiling that suite alone do not establish a real commit, deployed
backfill or successful environment repair.
