# Provider conformance fixtures V1

These manifests exercise the production `sessionlessharness.Registry` through
the bounded `harnessconformance` runner. They are credential-free, contain no
prompt, provider body, native error, raw frame, stdout/stderr, or local path,
and never perform a provider network call.

`deterministic-execute.json` covers the real embedded deterministic contract.
`openrouter-ox-alpha-public.json` covers the provider-neutral registry and
policy tuple for public or externally-shareable work. Because the native
OpenRouter adapter is not implemented, its `backend_protocol` is deliberately
`skipped`; the fixture must not be cited as native API conformance evidence.

Every manifest is decoded with the strict V1 codec (64 KiB total limit,
canonical unsigned numbers, bounded nesting/members/arrays/strings, and
duplicate/case-colliding/unknown/null/BOM/trailing-input rejection). No
production routing or feature enablement is implied by a passing fixture.
