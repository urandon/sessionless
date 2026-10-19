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
Make recipe lines, including under `make -j`. These targets use two complementary selections over the same complete package
patterns: `-skip='^TestLocalSession'`, then `-run='^TestLocalSession'`. Both retain
the race detector and the existing 15-minute per-binary bound. This separates
connected-session work without dropping tests, their subtests, examples or fuzz
seed corpora, or shortening scenarios. Both commands must cover all packages:
matching test names exist outside the state-store package as well. Keep ordinary
sequential Make recipe lines so failure of either command fails the target.
Formatting, vet and package-layout checks remain prerequisites of the ordinary part.

Preserve this complement if changing the selector. Do not introduce a second
unrelated filter, ignored failure, `-short`, wider timeout or hidden test skip
to obtain a green result. Package initialization runs separately; cross-test
process-state dependencies are not supported. Record fresh execution of both
halves with caching disabled or in clean exact-head CI. The CI job's overall
30-minute budget is unchanged and still needs actual verification. See the Go
command's [`-run`, `-skip` and `-timeout` contracts](https://pkg.go.dev/cmd/go#hdr-Testing_flags).

GitHub CI has nine required jobs. Its Go verification job runs `make ci-core`,
including `test-ordinary`; a parallel 30-minute `go-connected` job runs
`make test-connected` over the same full package patterns. It needs no Web build:
the committed embed placeholder suffices for compilation. Neither job requires
a cache hit. The existing YDB, isolated-security, local-stand and Terraform jobs
join both Go jobs, preserving their original complete-unit-test predecessor.
The unchanged offline
`attached-worker-security-gate`, including both crash/restart prerequisites,
and the complete unchanged `serverless-egress-conformance` target run in the
existing isolated YDB security job before that job starts YDB.
That offline step removes the job's YDB connection and credential environment
variables. Local and release `make ci` still require core, connected tests and
both offline gates. Images retain all seven original predecessors and also
require `go-connected`. `make ci-partition-test` checks the partition's static coverage.
The 25-minute isolated-job bound is unchanged: actual exact-head CI must prove
both parallel 30-minute jobs and the relocated checks/native gates fit on a cold
cache miss; the partition alone is not proof. No previous-head CI qualifies a
changed graph.

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
