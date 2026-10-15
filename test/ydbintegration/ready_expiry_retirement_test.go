//go:build ydbintegration

package ydbintegration

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

var retiredReadyExpiry = []string{"lease_expiry", "dispatch_ready", "quota_expiry", "telegram_delivery_ready"}

func readyExpiryInventory(t *testing.T, client *ydbclient.Client) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	directory, err := client.Scheme().ListDirectory(ctx, client.DatabasePath())
	if err != nil {
		t.Fatalf("native complete table inventory: %v", err)
	}
	tables := map[string]bool{}
	for _, child := range directory.Children {
		if child.Type == scheme.EntryTable {
			if tables[child.Name] {
				t.Fatalf("duplicate native table %q", child.Name)
			}
			tables[child.Name] = true
		}
	}
	return tables
}

// Successful SQL errors are never interpreted as absence. Both native scheme
// absence and the retained physical v2 successors are required.
func assertRetiredReadyExpiryAbsent(t *testing.T, client *ydbclient.Client) {
	t.Helper()
	tables := readyExpiryInventory(t, client)
	for _, name := range retiredReadyExpiry {
		if tables[name] {
			t.Errorf("retired table %s remains present", name)
		}
		if !tables[name+"_v2"] {
			t.Errorf("retained successor %s_v2 missing", name)
		}
	}
}

// Deliberately separate from the general suite: the owner authorized exactly
// these four existing/missing DROP controls in disposable GitHub CI only.
// No local execution, reset, credentials, provider calls or deployed drain.
func TestReadyExpiryRetirementYDBExistingAndMissing(t *testing.T) {
	if os.Getenv("SESSIONLESS_READY_EXPIRY_RETIREMENT_CI") != "1" {
		t.Skip("requires the explicit disposable GitHub CI retirement gate")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		t.Fatal("retirement fixture is restricted to GitHub CI")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	_, client := openStore(t)
	assertRetiredReadyExpiryAbsent(t, client)
	beforeTables := readyExpiryInventory(t, client)
	var manifest struct {
		Retirements []struct{ Table, CreateMigration, Migration string }
	}
	if err := json.Unmarshal(ydbmigrations.RetirementDefinitions, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Retirements) != 4 {
		t.Fatalf("owned retirement count=%d want4", len(manifest.Retirements))
	}
	schemaClient, err := ydbclient.OpenScripting(ctx, requireConnectionString(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := schemaClient.Close(closeCtx); err != nil {
			t.Errorf("close schema client: %v", err)
		}
	})
	up := func(filename string) string {
		t.Helper()
		body, err := fs.ReadFile(ydbmigrations.Files, filename)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Split(string(body), "-- +goose Down")[0]
	}
	// Register exact owned cleanup before creating the first historical table.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 45*time.Second)
		defer stop()
		for _, r := range manifest.Retirements {
			if _, err := schemaClient.DB.ExecContext(cleanup, up(r.Migration)); err != nil {
				t.Errorf("cleanup exact legacy table %s: %v", r.Table, err)
			}
		}
	})
	fixtures := make([]*expiryRepairCI, 0, 4)
	for _, kind := range []string{"lease", "dispatch", "quota", "telegram"} {
		f := newExpiryRepairCI(t, kind)
		// All later readbacks share the enclosing scenario bound; the earliest
		// fixture must not expire its observation context while others are built.
		f.ctx = ctx
		f.request.Mode = "apply"
		if _, err := f.store.RepairReadyExpiryV2(f.ctx, f.request); err != nil {
			t.Fatalf("retained %s fixture repair: %v", kind, err)
		}
		f.requireIndex(t, 1)
		fixtures = append(fixtures, f)
	}
	snapshots := make([][]string, len(fixtures))
	for i, f := range fixtures {
		snapshots[i] = f.sourceSnapshot(t)
	}
	for i, r := range manifest.Retirements {
		if _, err := schemaClient.DB.ExecContext(ctx, up(r.CreateMigration)); err != nil {
			t.Fatalf("create owned legacy %s: %v", r.Table, err)
		}
		f := fixtures[i]
		var q string
		var args []any
		switch r.Table {
		case "lease_expiry":
			q = "UPSERT INTO lease_expiry (tenant_id,expires_at,run_id,lease_id,fence_token) VALUES($1,$2,$3,$4,$5)"
			args = []any{f.run.TenantID, f.due, f.run.ID, f.request.ID, f.fence}
		case "dispatch_ready":
			q = "UPSERT INTO dispatch_ready (tenant_id,available_at,dispatch_outbox_id,run_id,attempt_id) VALUES($1,$2,$3,$4,$5)"
			args = []any{f.run.TenantID, f.due, f.request.ID, f.run.ID, f.attempt.ID}
		case "quota_expiry":
			q = "UPSERT INTO quota_expiry (tenant_id,expires_at,quota_reservation_id,run_id,subscription_connection_id) VALUES($1,$2,$3,$4,$5)"
			args = []any{f.run.TenantID, f.due, f.request.ID, f.run.ID, f.run.SubscriptionConnectionID}
		case "telegram_delivery_ready":
			q = "UPSERT INTO telegram_delivery_ready (tenant_id,available_at,telegram_delivery_id,run_id) VALUES($1,$2,$3,$4)"
			args = []any{f.run.TenantID, f.due, f.request.ID, f.run.ID}
		default:
			t.Fatalf("unowned retirement %q", r.Table)
		}
		if _, err := client.DB.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("seed exact legacy %s: %v", r.Table, err)
		}
		assertCount(t, client, r.Table, f.run.TenantID, 1)
	}
	for _, phase := range []string{"existing", "already_missing"} {
		for _, r := range manifest.Retirements {
			if _, err := schemaClient.DB.ExecContext(ctx, up(r.Migration)); err != nil {
				t.Fatalf("%s DROP %s: %v", phase, r.Table, err)
			}
		}
		assertRetiredReadyExpiryAbsent(t, client)
		if got := readyExpiryInventory(t, client); !reflect.DeepEqual(got, beforeTables) {
			t.Fatalf("%s changed retained table inventory: before%v after%v", phase, beforeTables, got)
		}
		for i, f := range fixtures {
			f.requireIndex(t, 1)
			if got := f.sourceSnapshot(t); !reflect.DeepEqual(got, snapshots[i]) {
				t.Fatalf("%s changed %s source/canonical/TTL snapshot", phase, f.request.Kind)
			}
		}
		t.Logf("%s: exact four legacy tables absent; retained v2/source inventory unchanged", phase)
	}
	// Record the application count against complete source ownership, excluding
	// the runner's three bookkeeping tables. This is this disposable DB only.
	var registry struct {
		Records []struct {
			Table      string
			Retirement *json.RawMessage
		}
	}
	registryBytes, err := os.ReadFile("../../docs/architecture/entity-registry.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(registryBytes, &registry); err != nil {
		t.Fatal(err)
	}
	active := 0
	ownedApplication := map[string]bool{}
	for _, r := range registry.Records {
		ownedApplication[r.Table] = true
		if r.Retirement == nil {
			if !beforeTables[r.Table] {
				t.Errorf("active application table %s missing", r.Table)
			} else {
				active++
			}
		} else if beforeTables[r.Table] {
			t.Errorf("retired application table %s present", r.Table)
		}
	}
	bookkeeping := map[string]bool{"sessionless_goose_versions": true, "schema_migration_lock": true, "schema_migration_checksums": true}
	for table := range beforeTables {
		if !ownedApplication[table] && !bookkeeping[table] {
			t.Errorf("unexpected table outside complete source ownership: %s", table)
		}
	}
	if active != 101 {
		t.Fatalf("observed application layout count=%d want101", active)
	}
	t.Log(fmt.Sprintf("disposable CI application tables=%d; historical CREATE entries=%d", active, len(registry.Records)))
}
