// Package ydbmigrations exposes the versioned YDB migration set to the
// repository-owned schema migrator.
package ydbmigrations

import "embed"

// Files contains ordered Goose SQL migrations and their checksummed source.
//
//go:embed *.sql
var Files embed.FS

// RetirementDefinitions is source ownership/order metadata, not a database
// ledger or permission to apply destructive DDL. The source checker and runner
// consume the same exact four-table contract.
//
//go:embed retirements.json
var RetirementDefinitions []byte
