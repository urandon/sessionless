# Test fixtures

Fixtures contain synthetic Telegram updates and fake storage payloads only.
Real chat contents, user identifiers, credentials, and subscription tokens must
never be copied into the repository.

`queue/*.json` contains versioned, payload-free envelopes used to verify JSON
compatibility between the control plane, serverless transports, and isolated
workers.

`make dev-seed` loads `telegram/text-message.json` into the local Telegram fake.
Loading is idempotent by positive `update_id`; it never contacts Telegram.

`make e2e-local` creates additional synthetic updates at runtime so IDs remain
unique against a persistent local YDB volume. The E2E inputs contain no real
Telegram or provider data and are correlated in test output by update, tenant,
run, and synthetic chat identifiers.
