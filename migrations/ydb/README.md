# YDB migrations

YDB schema migrations will live here as ordered SQL files. The repository pins
Goose because MVP-01 requires a common migration authoring tool, but Goose does
not currently provide a YDB database driver. `make migrate-local` therefore
refuses to apply non-empty migrations until the YDB implementation issue adds a
durable migration ledger and a YDB-native runner.

This is intentional: replaying raw DDL without recording ownership, checksum,
and applied version would make serverless deployments unsafe.
