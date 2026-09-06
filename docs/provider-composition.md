# Native provider composition

`internal/providercomposition` is the closed composition boundary for the four
reviewed native provider adapters below the Sessionless-owned harness registry:

- Codex over OpenRouter;
- OpenCode over OpenRouter;
- Pi over OpenRouter;
- the native direct OpenRouter reference backend.

The V1 factory accepts only four explicitly constructed, already pinned driver
instances and an injected clock. It does not inspect the environment, discover
installed binaries, choose profiles, construct process or HTTP boundaries, or
select a default backend. Missing dependencies and enabled profiles fail the
composition before a registry is returned.

Every registration is built through its adapter's `DisabledRegistrationV1`
contract and passed to the existing exact-match `sessionlessharness.Registry`.
Registration order has no routing meaning. Unknown and near-match descriptors
fail closed; there is no priority, chaining, provider fallback, or cross-backend
retry. Disabled preflight cannot reach credential, process, or network effects.

Cancellation remains routable through an exact binding while a profile is
disabled. This is a teardown path, not execution authorization: a mismatched
descriptor, model, or owner/run/attempt scope fails before the boundary.

The composition is deliberately not wired into `worker-runtime`. Production
enablement remains blocked on the accepted isolation, egress, credential,
provider-evidence, two-owner security, cost, and recovery gates. The factory
does not satisfy or bypass any of those gates.
