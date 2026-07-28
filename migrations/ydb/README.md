# YDB migrations

YDB schema migrations live here as ordered Goose SQL files. Goose has official
YDB support through the YDB `database/sql` driver; the required scripting,
transaction-emulation, and binding flags are part of the local connection
string in `.env.example`.

At foundation stage this directory contains no SQL migrations, so
`make migrate-local` is a successful no-op. Once migration files exist, the
command requires both the pinned Goose binary and `YDB_CONNECTION_STRING`.

YDB does not support schema transactions. A multi-statement DDL migration can
therefore be left partially applied even though Goose exposes a transaction-like
interface. MVP-03 must add and test the production rules:

- one idempotent schema operation per recoverable step;
- expand/migrate/contract compatibility across mixed application versions;
- single-flight migration execution with a YDB-backed lock;
- immutable applied files plus checksum/drift validation;
- no automatic destructive down migration in production.

Reference:
https://ydb.tech/docs/en/integrations/migration/goose
