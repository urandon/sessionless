# Native provider composition

`internal/providercomposition` is the closed composition boundary for the five
reviewed native provider adapters below the Sessionless-owned harness registry:

- Codex exec with an owner-local subscription credential;
- Codex over OpenRouter;
- OpenCode over OpenRouter;
- Pi over OpenRouter;
- the native direct OpenRouter reference backend.

The V1 registry factory accepts only five explicitly constructed, already
pinned driver
instances and an injected clock. It does not inspect the environment, discover
installed binaries, choose profiles, construct process or HTTP boundaries, or
select a default backend. Missing dependencies and enabled profiles fail the
composition before a registry is returned. The Codex subscription driver has a
separate exact constructor that composes the immutable accepted-attempt snapshot
and prepared Go supervisor; it still performs no discovery or activation.

Every registration is built through its adapter's `DisabledRegistrationV1`
contract and passed to the existing exact-match `sessionlessharness.Registry`.
Registration order has no routing meaning. Unknown and near-match descriptors
fail closed; there is no priority, chaining, provider fallback, or cross-backend
retry. Disabled preflight cannot reach credential, process, or network effects.

Cancellation remains routable through an exact binding while a profile is
disabled. This is a teardown path, not execution authorization: a mismatched
descriptor, model, owner/run/attempt scope, worker, connection generation,
lease, capability, or policy authority fails before the boundary. The
subscription registration uses its prepared-process `HarnessDriver`, so an
exact cancellation reaches only the matching attached-worker boundary; it
cannot reach the older credential-issuing invocation runner.

The composition is deliberately not wired into `worker-runtime`. Production
enablement remains blocked on the accepted isolation, egress, credential,
provider-evidence, two-owner security, cost, and recovery gates. The factory
does not satisfy or bypass any of those gates.
