// Package ydbmigrations exposes the immutable YDB migration set to the
// repository-owned schema migrator.
package ydbmigrations

import "embed"

// Files contains ordered Goose SQL migrations and their checksummed source.
//
//go:embed *.sql
var Files embed.FS
