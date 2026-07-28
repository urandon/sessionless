# Test fixtures

Fixtures contain synthetic Telegram updates and fake storage payloads only.
Real chat contents, user identifiers, credentials, and subscription tokens must
never be copied into the repository.

`queue/*.json` contains versioned, payload-free envelopes used to verify JSON
compatibility between the control plane, serverless transports, and isolated
workers.
