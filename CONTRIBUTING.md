# Contributing

Start with [docs/development.md](docs/development.md), then run the same gate as
CI before requesting review:

```sh
make ci
make images
```

Keep packages behind the existing boundaries:

- executable wiring belongs in `cmd/<component>`;
- reusable application code belongs in `internal`;
- YDB schema changes belong in `migrations/ydb`;
- Yandex Cloud definitions belong in `infra`;
- synthetic payloads belong in `test/fixtures`.

Do not commit `.env`, cloud profiles, Telegram tokens, AI subscription
credentials, service-account keys, Terraform state, or real chat data. A change
that needs a secret must accept it through the process environment or the
deployment platform's secret mechanism.

Placeholders must say that they are placeholders. Do not expose a route,
command, or document that implies Telegram delivery, quota enforcement, or an
AI harness works before its implementation issue is complete.
