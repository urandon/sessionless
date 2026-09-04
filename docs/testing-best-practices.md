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
