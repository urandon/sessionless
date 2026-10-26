# Go testing best practices

This document is the repository contract for writing and reviewing Go tests.
The mandatory rules below combine official Go guidance, the Google Go style
guide, and Sessionless-specific requirements for YDB, workers, queues, and
external processes. A test is not acceptable merely because it passes once.

## Core contract

Every test must be:

- deterministic for the same code and explicit inputs;
- independently runnable with `-run`, without relying on another test's order
  or leftovers;
- isolated from unrelated tests and tenant data;
- bounded in time, data, goroutines, subprocesses, retries, and output;
- diagnostic enough that the first failure identifies the operation, inputs,
  observed value, expected invariant, and underlying error;
- cleaned up on success, failure, skip, and timeout.

These requirements follow Go's model of individually addressable tests and
subtests, explicit cleanup, and bounded test execution. See the official
[`testing` package](https://pkg.go.dev/testing), Go's
[`go test` command](https://go.dev/cmd/go/#hdr-Test_packages), and the Go team's
[subtest guidance](https://go.dev/blog/subtests).

## Time and asynchronous work

Use one time authority per scenario.

- Pure domain and in-process concurrency tests must use an injected clock or
  [`testing/synctest`](https://pkg.go.dev/testing/synctest) where its isolation
  constraints fit. Advance logical time explicitly and wait for quiescence.
- Define subtests outside `synctest.Test`: its inner `*testing.T` forbids
  `Run`, `Parallel`, and `Deadline`. Its `Cleanup` callbacks run inside the
  bubble before `synctest.Test` returns. Keep bubble channels and timers
  inside that bubble; external process tests remain outside it. These limits
  are part of the official [`synctest.Test` contract](https://pkg.go.dev/testing/synctest#Test).
- Integration tests against an external system whose clock is authoritative
  must sample that clock and derive all related start, expiry, renewal, and
  completion timestamps from the sampled value. Do not combine `time.Now()`
  with YDB `CurrentUtcTimestamp()` for one lease or freshness assertion.
- A test that is not testing expiry must give the resource a lifetime longer
  than the enclosing test or CI timeout. Boundary tests for expiry must control
  or explicitly sample the authoritative clock.
- Never use a fixed sleep to prove readiness or completion. Wait for an
  observable state, acknowledgement, process exit, channel event, or
  quiescence condition with a deadline. Polling must have a bounded interval,
  a bounded total deadline, and a useful timeout diagnostic.
- A deadline is a failure bound, not synchronization. Synchronize goroutines
  with channels, mutexes, wait groups, contexts, or `synctest.Wait`.
- Do not assume `select` prioritizes completion over a ready deadline: the
  [Go specification](https://go.dev/ref/spec#Select_statements) permits either
  ready case. For readiness or cleanup failure bounds, recheck completion
  before reporting it still pending. Expiry tests must instead establish the
  expiry decision from their authoritative clock.

The Go team documents why real-time sleeps make concurrent tests slow and
flaky, and recommends fake time plus a way to wait for quiescence in
[Testing Time](https://go.dev/blog/testing-time). The stable `synctest` package
also states that its bubbles should avoid external networks and processes, so
it is appropriate for in-process tests, not as a fake clock around YDB.

## Isolation, setup, and cleanup

- Create unique tenant, run, attempt, lease, object, queue, and container names
  per test. Never depend on a global counter alone when separate test processes
  can collide.
- Establish every schema marker, membership, binding, and fixture required by
  the test itself. A test must not depend on a previous test creating it.
- Use `t.TempDir`, `t.Setenv`, and `t.Cleanup` for temporary files, environment,
  and owned resources. Remember that `t.Setenv` changes process-global state
  and is forbidden in parallel tests or tests with parallel ancestors.
- Register cleanup immediately after acquiring a resource. Cleanup must be
  idempotent and bounded; it must not hide the primary failure.
- External containers and processes must be ephemeral, explicitly named,
  stopped on every exit path, and checked for descendants or residual state
  when that is part of the contract.
- Tests sharing a mutable database, schema, environment, port, filesystem
  location, or process-global singleton must not call `t.Parallel`. Parallelize
  only after proving resource independence.

The cleanup and environment rules are defined by the
[`testing` package](https://pkg.go.dev/testing). The Go team's
[subtest documentation](https://go.dev/blog/subtests) defines parallel-test
ordering and grouped cleanup semantics. Google Go's
[test-helper guidance](https://google.github.io/styleguide/go/best-practices.html#test-helpers)
reinforces immediate, test-owned cleanup and `t.Helper` attribution.

## Test structure and assertions

- Prefer table-driven tests when cases share setup and assertion logic. Use
  named cases or named subtests, explicit struct field names, and failure
  messages that include the relevant input and `got`/`want` values.
- Split materially different success and failure flows instead of building a
  table whose rows select complex conditional logic.
- Subtests must pass when run individually. Names must be stable and useful in
  `go test -run` filters.
- Helpers that receive `*testing.T` must call `t.Helper()`. Helpers may use
  `t.Fatal` for failed setup that makes the current test impossible; ordinary
  independent checks should use `t.Error`/`t.Errorf` so one failure does not
  erase additional evidence.
- Assert externally meaningful invariants rather than incidental ordering,
  exact wall-clock duration, scheduler interleaving, map iteration order, or
  implementation-private formatting.
- Preserve the causal error in failure output. If production intentionally
  collapses internal authorization failures, tests should validate the
  constituent authority or expose a test-only diagnostic path; do not weaken
  fail-closed production behavior for test observability.

This is consistent with the Go wiki's
[table-driven test guidance](https://go.dev/wiki/TableDrivenTests), the Go
team's [subtest guidance](https://go.dev/blog/subtests), and Google Go's
[test-structure decisions](https://google.github.io/styleguide/go/decisions.html#test-structure).

## Concurrency, retries, and repeated execution

- All concurrency-sensitive packages and integration paths must pass the Go
  [race detector](https://go.dev/doc/articles/race_detector). A green race run
  does not replace assertions about ordering, idempotency, or fencing.
- Retry only operations whose contract permits retry. Make attempts and the
  final error visible; never convert an unknown or ambiguous result into
  success.
- Re-run newly added or repaired tests once with caching disabled, then repeat
  the focused test enough times to exercise its concurrency and timing paths.
  Use shuffled execution to expose order coupling and retain the reported seed
  for reproduction.
- A failed CI test is not labelled flaky without evidence. Reproduce the exact
  revision, isolate the first failing test, and distinguish product failure,
  test-model failure, infrastructure failure, and timeout.

The Go command documents `-count=1` as the idiomatic way to disable successful
test-result caching and provides `-shuffle` and `-timeout` in its
[testing flags](https://go.dev/cmd/go/#hdr-Testing_flags). The Go project also
documents an evidence-first workflow for
[investigating test failures](https://go.dev/wiki/TestFailures).

## Repository verification

Use repository-owned entry points so local and CI behavior stay aligned:

```text
make test
make integration
make ydb-integration
make local-integration
make ci
```

For a changed unit or integration test, the minimum evidence is:

1. the exact focused test passes with `-race`;
2. its package or integration suite passes;
3. `git diff --check` and the repository format/lint gates pass;
4. a fresh uncached run uses `-count=1`; a focused stress run uses `-count=N`
   with `N > 1`, and order-sensitive suites use `-shuffle=on`;
5. the mirrored GitHub CI is green for the exact commit before merge.

Fuzz tests are appropriate for parsers, canonical encoders, protocol frames,
IDs, cursors, and validation boundaries. Seed every known regression and keep
fuzz execution bounded in CI. See Go's official
[fuzzing documentation](https://go.dev/doc/security/fuzz/) and
[tutorial](https://go.dev/doc/tutorial/fuzz/).

### Cumulative package runtime

Go's `-timeout` bounds the complete test binary, not each individual test. An
expanded connected suite can exhaust that cumulative budget while the last
reported test has only just started. Do not call that test hung or flaky from
its name in the timeout report; inspect elapsed execution and actual evidence.

`make test` calls `test-ordinary` then `test-connected` using sequential recursive
Make recipe lines, including under `make -j`. The ordinary target now uses four
sequential uncached executions over the complete package patterns: first it
skips `TestLocal`, `TestOwned`, and `TestPrivate` prefixes; next it selects the
complete `TestOwned` and `TestPrivate` prefixes; next it selects the complete
`TestLocalCompaction` prefix; finally it selects the remaining `TestLocal` prefix
while skipping `TestLocal(Session|Compaction)`. The two local selections are
disjoint and together cover the original complete non-session local prefix,
including future families through the remainder. This keeps the ordinary
test names, examples, and fuzz seed corpora in the first execution, and gives
each prefixed family exactly one execution. Connected work uses two sequential
uncached executions: `-run='^TestLocalSessionTerminal'`, then
`-run='^TestLocalSession' -skip='^TestLocalSessionTerminal'`. The connected
selections are disjoint and together cover the original complete
`^TestLocalSession` prefix. All six executions retain the race detector,
`-count=1`, and the existing 15-minute per-binary bound. They do not drop tests,
subtests, examples or fuzz seed corpora or shorten scenarios. Every execution
covers the same complete package patterns: matching names exist outside the
state-store package too.
After the two normal connected partitions, `make test-connected` runs
`make test-mcp-offline`: a separate uncached race/shuffle execution with a
five-minute bound for the explicitly tagged synthetic MCP transport fixture.
Its fixed additive overlay introduces only absent test-support files; it must
never replace production source or authority checks. Normal keeper/session
packages are separately built without the overlay. This instrumentation is
offline evidence only, not live DNS/TLS endpoint qualification.
Keep all ordinary and connected commands as sequential recipe lines: a failure
stops the target before its next execution, including under `make -j`; do not
background any partition.
Formatting, vet and package-layout checks remain prerequisites of the ordinary part.

Preserve this complement if changing the selector. Do not introduce a second
unrelated filter, ignored failure, `-short`, wider timeout or hidden test skip
to obtain a green result. Package initialization runs separately; cross-test
process-state dependencies are not supported. Record fresh execution of all
selections; both connected executions explicitly disable result caching. The CI job's overall
30-minute budget is unchanged and still needs actual verification. See the Go
command's [`-run`, `-skip` and `-timeout` contracts](https://pkg.go.dev/cmd/go#hdr-Testing_flags).

GitHub CI has ten required jobs. Its 30-minute Go verification job runs
`make ci-core-base`: the same canonical ordered prerequisite list as `ci-core`,
with only `test-ordinary` replaced by `test-ordinary-ci-base` in its original
position before build and integration. The base runs the
first three original ordinary commands sequentially. A parallel 30-minute
`go-local` job runs `make test-ordinary-ci-tail` over the same complete package
patterns in two sequential executions: `^TestLocalTerminal`, then `^TestLocal`
excluding `^TestLocal(Session|Compaction|Terminal)`. These disjoint selections
together cover exactly the original fourth ordinary command, including future
families through the remainder. CI therefore has five ordinary executions;
local/release retains four. No test is omitted or duplicated. Race detection,
`-count=1`, 15-minute per-binary bounds and the 30-minute job ceiling are unchanged.
This addresses an observed cumulative binary timeout, not an inference that
the test named in its final stack was hung. Actual timings still qualify the split.
Local/release `make test-ordinary` and `ci-core` retain the complete four-command
ordinary contract. A parallel 30-minute `go-connected` job runs the two
sequential normal executions over the same full package patterns, followed by
the bounded additive MCP offline gate in `make test-connected`. It needs no Web build:
the committed embed placeholder suffices for compilation. No Go job requires
a cache hit. The existing YDB, isolated-security, local-stand and Terraform jobs
join all three Go jobs, preserving their original complete-unit-test predecessor.
The unchanged offline
`attached-worker-security-gate`, including both crash/restart prerequisites,
and the complete unchanged `serverless-egress-conformance` target run in the
existing isolated YDB security job before that job starts YDB.
That offline step removes the job's YDB connection and credential environment
variables. Local and release `make ci` still require core, connected tests and
both offline gates. Images retain all seven original predecessors and also
require `go-connected` and `go-local`. `make ci-partition-test` checks exact local
and CI complements, original core prerequisite order and its sole substitution,
and the complete dependency graph.
The 25-minute isolated-job bound is unchanged: actual exact-head CI must prove
all three parallel 30-minute Go jobs and the checks/native gates fit on a cold
cache miss; the partition alone is not proof. No previous-head CI qualifies a
changed graph.

A reviewed push whose HEAD commit message starts with `ci(cold-go): ` requests
explicit cold Go build/module-cache qualification. GitHub's `startsWith` is
case-insensitive; a quoted prefix later in a PR/merge-message body does not
trigger it. All six host-Go jobs (`verify`, `go-local`, `go-connected`, `ydb`,
`ydb-security-isolated`, `local-stand`) disable setup-go cache restore and
post-save for that intent. Immediately after checkout, before setup-go or any
other Go/repository command, each job proves its configured repository
`GOCACHE` and `GOMODCACHE` paths are
absent or empty; nonempty, redirected or invalid paths fail without creating,
clearing or relocating caches. setup-go itself runs full `go env` even with
`cache: false`, which legitimately initializes `GOCACHE/README` on a fresh cache;
the freshness guard must therefore run before the action, not reject its later
bootstrap metadata or permit/delete pre-existing entries. Normal pushes,
generated merge subjects without that prefix and manual dispatch without a HEAD
message retain default caching.
This controls only these host Go caches, not toolchain, npm, Docker or runner
coldness, test selection, permissions or provider-publication authority. A new
intent-bearing source HEAD still requires independent review and a complete
successful ten-job run with actual timings; static policy tests and prior warm
runs are not cold-cache evidence.
The complete `cache` and `if` expressions are quoted YAML scalars because the
intent prefix contains colon-space. Exact source-policy tests reject unquoted
forms; those tests alone do not prove YAML validity. Check the edited workflow
with an available actual YAML parser before accepting it.

The two standalone YDB jobs wait on the pinned container's SQL/storage-backed
healthcheck before either migration invocation. HTTP monitoring alone is not
database readiness. The read-only `scripts/wait-ci-ydb.sh` observer accepts only
the two exact CI-owned container names and the expected healthcheck command;
unknown/unhealthy/exited/inspect failure is fatal. Readiness has a 90-second and
90-poll ceiling, each inspect at most 3 seconds within that original budget;
failure diagnostics get independent bounded 5-second inspect/log calls. It does
not retry migrations or widen the 25-minute jobs. Deterministic fake-command
policy tests run through `make ci-partition-test`; actual exact-head CI still
must prove the real image becomes ready and both native gates pass.

The pinned [YDB 25.3.1.25 healthcheck](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/.github/docker/files/health_check)
checks SQL, scheme access and a create/drop pair under its own `.sys_health`
path; its [Dockerfile](https://github.com/ydb-platform/ydb/blob/8187ce049bed9e63a5ec04769da2d83dd237c08a/.github/docker/Dockerfile)
defines the expected command. The observer invokes neither healthcheck nor DDL:
it only observes the image's existing Docker health state.

## Review checklist

Before accepting a test change, verify:

- the test fails for the intended defect and passes for the fix;
- no fixed sleep or narrow real-time window decides correctness;
- one authoritative clock governs each temporal invariant;
- all waits, retries, payloads, and resource lifetimes are bounded;
- the test can run alone and in shuffled suite order;
- parallel tests share no mutable global or external state;
- cleanup is immediate, idempotent, and complete;
- failure output contains actionable causal evidence;
- race, focused, suite, and exact-commit CI evidence are recorded.
