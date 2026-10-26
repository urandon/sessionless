package ydbmigrate

import (
	"bytes"
	"fmt"
	"io/fs"
	"math"
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

// Append after the actual embedded history, rather than reserving a version
// which a later legitimate migration can occupy. Validate the fixture before
// mutation so each negative case still reaches its intended retirement check.
func appendRetirementMigration(t *testing.T, files fstest.MapFS, suffix, body string) string {
	t.Helper()
	migrations, err := LoadMigrations(files)
	if err != nil {
		t.Fatalf("load unmodified retirement fixture: %v", err)
	}
	if len(migrations) == 0 || migrations[len(migrations)-1].Version == math.MaxInt64 {
		t.Fatal("retirement fixture has no available successor version")
	}
	name := fmt.Sprintf("%05d_%s.sql", migrations[len(migrations)-1].Version+1, suffix)
	if _, exists := files[name]; exists {
		t.Fatalf("synthetic migration would replace %s", name)
	}
	files[name] = &fstest.MapFile{Data: []byte(body)}
	return name
}

func TestRetirementFixtureAppendPreservesExistingMigrations(t *testing.T) {
	files := retirementFixture(t)
	original := retirementFixture(t)
	for range 2 {
		appendRetirementMigration(t, files, "unrelated", "-- +goose Up\nALTER TABLE runs ADD COLUMN later_note Utf8;\n-- +goose Down\n")
	}
	if len(files) != len(original)+2 {
		t.Fatalf("fixture files=%d want=%d", len(files), len(original)+2)
	}
	for name, before := range original {
		after, exists := files[name]
		if !exists || !bytes.Equal(before.Data, after.Data) {
			t.Fatalf("existing migration %s was removed or changed", name)
		}
	}
	if _, err := LoadMigrations(files); err != nil {
		t.Fatalf("valid later migrations rejected: %v", err)
	}
}

func TestRetirementMigrationSetPreservesHistoryAndOwnsFourDrops(t *testing.T) {
	files := retirementFixture(t)
	migrations, err := LoadMigrations(files)
	if err != nil {
		t.Fatal(err)
	}
	// Retirement ends at 129; unrelated later migrations must remain possible.
	// Keep the complete original version prefix, not a frozen total file count.
	const retirementHistory = 129
	if len(migrations) < retirementHistory {
		t.Fatalf("migration count=%d want at least %d", len(migrations), retirementHistory)
	}
	for i, migration := range migrations[:retirementHistory] {
		if migration.Version != int64(i+1) {
			t.Fatalf("historical migration[%d]=%d want=%d", i, migration.Version, i+1)
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
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, fstest.MapFS)
		want   string
	}{
		{name: "missing one", mutate: func(_ *testing.T, f fstest.MapFS) { delete(f, first) }, want: "missing declared retirement"},
		{name: "missing all", mutate: func(_ *testing.T, f fstest.MapFS) {
			for name := range f {
				if strings.Contains(name, "_retire_") {
					delete(f, name)
				}
			}
		}, want: "missing declared retirement"},
		{name: "foreign target", mutate: func(_ *testing.T, f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE IF EXISTS `lease_expiry_v2`;\n-- +goose Down\n")
		}, want: "owned idempotent DROP"},
		{name: "without IF EXISTS", mutate: func(_ *testing.T, f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE `lease_expiry`;\n-- +goose Down\n")
		}, want: "owned idempotent DROP"},
		{name: "extra SQL", mutate: func(_ *testing.T, f fstest.MapFS) {
			f[first].Data = []byte("-- +goose Up\nDROP TABLE IF EXISTS `lease_expiry`; DELETE FROM runs;\n-- +goose Down\n")
		}, want: "exactly one schema statement"},
		{name: "destructive Down", mutate: func(_ *testing.T, f fstest.MapFS) {
			f[first].Data = append(f[first].Data, []byte("DROP TABLE IF EXISTS `runs`;\n")...)
		}, want: "Down must contain comments only"},
		{name: "missing successor", mutate: func(_ *testing.T, f fstest.MapFS) { delete(f, "00023_create_lease_expiry_v2.sql") }, want: "CREATE lineage"},
		{name: "missing CREATE", mutate: func(_ *testing.T, f fstest.MapFS) { delete(f, "00019_create_lease_expiry.sql") }, want: "CREATE lineage"},
		{name: "missing baseline", mutate: func(_ *testing.T, f fstest.MapFS) { delete(f, "00125_add_owned_operation_residual.sql") }, want: "missing predecessor"},
		{name: "early wrong filename", mutate: func(_ *testing.T, f fstest.MapFS) { f["00018_retire_lease_expiry.sql"] = f[first]; delete(f, first) }, want: "duplicate migration version"},
		{name: "duplicate", mutate: func(t *testing.T, f fstest.MapFS) {
			appendRetirementMigration(t, f, "duplicate_drop", string(f[first].Data))
		}, want: "unowned schema operation"},
		{name: "duplicate version", mutate: func(_ *testing.T, f fstest.MapFS) { f["00126_collision.sql"] = f[first] }, want: "duplicate migration version"},
		{name: "reactivation", mutate: func(t *testing.T, f fstest.MapFS) {
			appendRetirementMigration(t, f, "recreate", "-- +goose Up\nCREATE TABLE IF NOT EXISTS `lease_expiry` (id Utf8, PRIMARY KEY(id));\n-- +goose Down\n")
		}, want: "cannot recreate"},
		{name: "later ALTER", mutate: func(t *testing.T, f fstest.MapFS) {
			appendRetirementMigration(t, f, "alter", "-- +goose Up\nALTER TABLE `lease_expiry` ADD COLUMN extra Utf8;\n-- +goose Down\n")
		}, want: "cannot recreate or ALTER"},
		{name: "bare later ALTER", mutate: func(t *testing.T, f fstest.MapFS) {
			appendRetirementMigration(t, f, "alter", "-- +goose Up\nALTER TABLE lease_expiry ADD COLUMN extra Utf8;\n-- +goose Down\n")
		}, want: "cannot recreate or ALTER"},
		{name: "unparsed ALTER target", mutate: func(t *testing.T, f fstest.MapFS) {
			appendRetirementMigration(t, f, "alter", "-- +goose Up\nALTER TABLE namespace.lease_expiry ADD COLUMN extra Utf8;\n-- +goose Down\n")
		}, want: "unsupported CREATE/ALTER target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := retirementFixture(t)
			tc.mutate(t, files)
			_, err := LoadMigrations(files)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadMigrations error=%v want fragment%q", err, tc.want)
			}
		})
	}
}

func TestRetirementMigrationSetAllowsUnrelatedALTER(t *testing.T) {
	for _, target := range []string{"runs", "`runs`"} {
		t.Run(target, func(t *testing.T) {
			files := retirementFixture(t)
			appendRetirementMigration(t, files, "alter_unrelated", "-- +goose Up\nALTER TABLE "+target+" ADD COLUMN later_note Utf8;\n-- +goose Down\n")
			if _, err := LoadMigrations(files); err != nil {
				t.Fatalf("legitimate unrelated ALTER %s: %v", target, err)
			}
		})
	}
}
