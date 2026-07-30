package ydbmigrate

import (
	"errors"
	"testing"
	"testing/fstest"

	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

func TestLoadMigrationsOrdersAndChecksums(t *testing.T) {
	files := fstest.MapFS{
		"00002_b.sql": {Data: []byte("-- +goose Up\nCREATE TABLE b (id Uint64, PRIMARY KEY (id));\n-- +goose Down\n")},
		"00001_a.sql": {Data: []byte("-- +goose Up\nCREATE TABLE a (id Uint64, PRIMARY KEY (id));\n-- +goose Down\n")},
	}
	migrations, err := LoadMigrations(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 2 || migrations[0].Version != 1 || migrations[1].Version != 2 {
		t.Fatalf("unexpected migration order: %#v", migrations)
	}
	if migrations[0].SHA256 == "" || migrations[0].SHA256 == migrations[1].SHA256 {
		t.Fatalf("unexpected checksums: %#v", migrations)
	}
}

func TestLoadMigrationsRejectsMultipleOperations(t *testing.T) {
	files := fstest.MapFS{
		"00001_bad.sql": {Data: []byte("-- +goose Up\nCREATE TABLE a (id Uint64, PRIMARY KEY (id));\nCREATE TABLE b (id Uint64, PRIMARY KEY (id));\n-- +goose Down\n")},
	}
	_, err := LoadMigrations(files)
	if err == nil {
		t.Fatal("expected multiple-operation migration to fail")
	}
}

func TestErrorsAreStableSentinels(t *testing.T) {
	if !errors.Is(fmtError(ErrChecksumDrift), ErrChecksumDrift) {
		t.Fatal("checksum drift sentinel should survive wrapping")
	}
}

func TestEmbeddedMigrationsAreSingleOperationAndOrdered(t *testing.T) {
	migrations, err := LoadMigrations(ydbmigrations.Files)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 41 {
		t.Fatalf("embedded migration count = %d, want 41", len(migrations))
	}
	for index, migration := range migrations {
		want := int64(index + 1)
		if migration.Version != want {
			t.Fatalf("migration[%d].Version = %d, want %d", index, migration.Version, want)
		}
	}
}

func fmtError(err error) error {
	return errors.Join(errors.New("context"), err)
}
