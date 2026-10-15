package ydbmigrate

import (
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

func retirementFixture(t *testing.T) fstest.MapFS {
	t.Helper()
	files := fstest.MapFS{}
	entries, err := fs.ReadDir(ydbmigrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		body, err := fs.ReadFile(ydbmigrations.Files, entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = &fstest.MapFile{Data: body}
	}
	return files
}

// Derive a genuinely later fixture version from the unmodified complete source
// set. Fixed130 stopped exercising retirement guards once real130 was added.
func nextRetirementFixtureVersion(t *testing.T) int64 {
	t.Helper()
	migrations, err := LoadMigrations(retirementFixture(t))
	if err != nil || len(migrations) == 0 {
		t.Fatalf("load original migration fixture: count=%d err=%v", len(migrations), err)
	}
	return migrations[len(migrations)-1].Version + 1
}

func TestRetirementMigrationSetPreservesHistoryAndOwnsFourDrops(t *testing.T) {
	files := retirementFixture(t)
	migrations, err := LoadMigrations(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != len(files) {
		t.Fatalf("loaded migration count=%d want complete source count=%d", len(migrations), len(files))
	}
	for _, m := range migrations {
		if _, found := files[m.Filename]; !found {
			t.Fatalf("loader invented migration %q", m.Filename)
		}
	}
	definitions, err := retirementDefinitions()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"lease_expiry": "lease_expiry_v2", "dispatch_ready": "dispatch_ready_v2", "quota_expiry": "quota_expiry_v2", "telegram_delivery_ready": "telegram_delivery_ready_v2"}
	for _, r := range definitions {
		if want[r.Table] != r.Successor {
			t.Fatalf("unowned retirement %#v", r)
		}
		delete(want, r.Table)
	}
	if len(want) != 0 {
		t.Fatalf("missing retirements=%v", want)
	}
}

func TestRetirementMigrationSetRejectsUnsafeDefinitionsBeforeOpen(t *testing.T) {
	const first = "00126_retire_lease_expiry.sql"
	next := nextRetirementFixtureVersion(t)
	for _, tc := range []struct {
		name   string
		mutate func(fstest.MapFS)
		want   string
	}{
		{name: "missing one", mutate: func(f fstest.MapFS) { delete(f, first) }, want: "missing declared retirement"},
		{name: "missing all", mutate: func(f fstest.MapFS) {
			for name := range f {
				if strings.Contains(name, "_retire_") {
					delete(f, name)
				}
			}
		}, want: "missing declared retirement"},
		{name: "foreign target", mutate: func(f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE IF EXISTS `lease_expiry_v2`;\n-- +goose Down\n")
		}, want: "owned idempotent DROP"},
		{name: "without IF EXISTS", mutate: func(f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE `lease_expiry`;\n-- +goose Down\n")
		}, want: "owned idempotent DROP"},
		{name: "extra SQL", mutate: func(f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE IF EXISTS `lease_expiry`; DELETE FROM runs;\n-- +goose Down\n")
		}, want: "exactly one schema statement"},
		{name: "destructive Down", mutate: func(f fstest.MapFS) {
			f[first].Data = append(f[first].Data, []byte("DROP TABLE IF EXISTS `runs`;\n")...)
		}, want: "Down must contain comments only"},
		{name: "missing successor", mutate: func(f fstest.MapFS) { delete(f, "00023_create_lease_expiry_v2.sql") }, want: "CREATE lineage"},
		{name: "missing CREATE", mutate: func(f fstest.MapFS) { delete(f, "00019_create_lease_expiry.sql") }, want: "CREATE lineage"},
		{name: "missing baseline", mutate: func(f fstest.MapFS) { delete(f, "00125_add_owned_operation_residual.sql") }, want: "missing predecessor"},
		{name: "early wrong filename", mutate: func(f fstest.MapFS) { f["00018_retire_lease_expiry.sql"] = f[first]; delete(f, first) }, want: "duplicate migration version"},
		{name: "duplicate", mutate: func(f fstest.MapFS) { f[fmt.Sprintf("%05d_duplicate_drop.sql", next)] = f[first] }, want: "unowned schema operation"},
		{name: "duplicate version", mutate: func(f fstest.MapFS) { f["00126_collision.sql"] = f[first] }, want: "duplicate migration version"},
		{name: "reactivation", mutate: func(f fstest.MapFS) {
			f[fmt.Sprintf("%05d_recreate.sql", next)] = &fstest.MapFile{Data: []byte("-- +goose Up\nCREATE TABLE IF NOT EXISTS `lease_expiry` (id Utf8, PRIMARY KEY(id));\n-- +goose Down\n")}
		}, want: "cannot recreate"},
		{name: "later ALTER", mutate: func(f fstest.MapFS) {
			f[fmt.Sprintf("%05d_alter.sql", next)] = &fstest.MapFile{Data: []byte("-- +goose Up\nALTER TABLE `lease_expiry` ADD COLUMN extra Utf8;\n-- +goose Down\n")}
		}, want: "cannot recreate or ALTER"},
		{name: "bare later ALTER", mutate: func(f fstest.MapFS) {
			f[fmt.Sprintf("%05d_alter.sql", next)] = &fstest.MapFile{Data: []byte("-- +goose Up\nALTER TABLE lease_expiry ADD COLUMN extra Utf8;\n-- +goose Down\n")}
		}, want: "cannot recreate or ALTER"},
		{name: "unparsed ALTER target", mutate: func(f fstest.MapFS) {
			f[fmt.Sprintf("%05d_alter.sql", next)] = &fstest.MapFile{Data: []byte("-- +goose Up\nALTER TABLE namespace.lease_expiry ADD COLUMN extra Utf8;\n-- +goose Down\n")}
		}, want: "unsupported CREATE/ALTER target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := retirementFixture(t)
			tc.mutate(files)
			_, err := LoadMigrations(files)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadMigrations error=%v want fragment%q", err, tc.want)
			}
		})
	}
}

func TestRetirementMigrationSetAllowsUnrelatedALTER(t *testing.T) {
	next := nextRetirementFixtureVersion(t)
	for _, target := range []string{"runs", "`runs`"} {
		t.Run(target, func(t *testing.T) {
			files := retirementFixture(t)
			files[fmt.Sprintf("%05d_alter_unrelated.sql", next)] = &fstest.MapFile{Data: []byte("-- +goose Up\nALTER TABLE " + target + " ADD COLUMN later_note Utf8;\n-- +goose Down\n")}
			if _, err := LoadMigrations(files); err != nil {
				t.Fatalf("legitimate unrelated ALTER %s: %v", target, err)
			}
		})
	}
}
