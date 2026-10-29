package ydbmigrate

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"strings"
	"testing"

	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

// These are source-policy checks only: no DB is opened and no DDL is applied.
func TestLocalCapacitySchemaExtensionPreservesExistingTables(t *testing.T) {
	migrations, err := LoadMigrations(ydbmigrations.Files)
	if err != nil {
		t.Fatal("load complete source migration catalog", err)
	}
	byVersion := make(map[int64]Migration, len(migrations))
	for _, migration := range migrations {
		byVersion[migration.Version] = migration
	}
	for _, tc := range []struct {
		name, table, create, createSHA string
		version                        int64
	}{
		{name: "00133_add_scheduler_slot_local_capacity.sql", table: "subscription_scheduler_slots", version: 133,
			create: "00039_create_subscription_scheduler_slots.sql", createSHA: "ae3031fc38af010b4f0f91de8464dbbc060e84bbf489248d11edac24cfb128b8"},
		{name: "00134_add_quota_expiry_local_capacity.sql", table: "quota_expiry_v2", version: 134,
			create: "00026_create_quota_expiry_v2.sql", createSHA: "d648de634ae0432b109e50ee328b1050d53eea47cbf5ba17f888fba16542ed72"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			migration, exists := byVersion[tc.version]
			if !exists || migration.Filename != tc.name {
				t.Fatalf("owned forward migration: version=%d got=%q exists=%t want=%q", tc.version, migration.Filename, exists, tc.name)
			}
			body, err := fs.ReadFile(ydbmigrations.Files, tc.name)
			if err != nil {
				t.Fatal("read owned additive source", err)
			}
			up, down, found := strings.Cut(string(body), "-- +goose Down")
			if !found {
				t.Fatal("owned additive source missing Down boundary")
			}
			// One exact ALTER adds two nullable columns. No defaults, key/TTL
			// changes, DML, index or table creation can hide in this statement.
			want := "ALTER TABLE `" + tc.table + "` ADD COLUMN `local_capacity_association` String, ADD COLUMN `local_capacity_digest` Utf8;"
			if got := localCapacityExecutableSQL(up); got != want {
				t.Errorf("owned additive shape got=%q want=%q", got, want)
			}
			if got := localCapacityExecutableSQL(down); got != "" {
				t.Errorf("Down must remain comment-only: got=%q", got)
			}
			original, err := fs.ReadFile(ydbmigrations.Files, tc.create)
			if err != nil {
				t.Fatal("read original CREATE lineage", err)
			}
			digest := sha256.Sum256(original)
			if got := hex.EncodeToString(digest[:]); got != tc.createSHA {
				t.Errorf("historical CREATE bytes/key/retention changed: migration=%s gotSHA=%s wantSHA=%s", tc.create, got, tc.createSHA)
			}
		})
	}
}

func localCapacityExecutableSQL(source string) string {
	var lines []string
	for _, line := range strings.Split(source, "\n") {
		if before, _, found := strings.Cut(line, "--"); found {
			line = before
		}
		lines = append(lines, line)
	}
	return strings.Join(strings.Fields(strings.Join(lines, " ")), " ")
}
