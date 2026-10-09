package preprodreset

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
)

func validTarget() Target {
	return Target{
		Environment:    "cloud-dev",
		FolderID:       "b1g-sessionless-dev-folder",
		YDBConnection:  "grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g-dev/etn-dev",
		ArtifactBucket: "sessionless-dev-artifacts-example",
		ObjectPrefix:   RequiredObjectPrefix,
	}
}

func TestBuildPlanRequiresResolvedNonProductionTarget(t *testing.T) {
	target := validTarget()
	plan, err := BuildPlan(target, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Tables) == 0 || plan.Target.Confirmation != "" {
		t.Fatalf("unexpected reset plan: %#v", plan)
	}
	connection, err := url.Parse(plan.Target.YDBConnection)
	if err != nil || connection.RawQuery != "" || connection.User != nil {
		t.Fatalf("reset plan leaked YDB connection credentials: %#v", plan.Target)
	}
	for name, mutate := range map[string]func(*Target){
		"wrong environment":   func(target *Target) { target.Environment = "production" },
		"production database": func(target *Target) { target.YDBConnection = "grpcs://example/production/db" },
		"DSN credentials":     func(target *Target) { target.YDBConnection = "grpcs://token@example/dev/db" },
		"fragment":            func(target *Target) { target.YDBConnection += "#unexpected" },
		"shared bucket":       func(target *Target) { target.ArtifactBucket = "shared-dev-artifacts" },
		"broad prefix":        func(target *Target) { target.ObjectPrefix = "" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := validTarget()
			mutate(&candidate)
			if _, err := BuildPlan(candidate, false); err == nil {
				t.Fatalf("unsafe target accepted: %#v", candidate)
			}
		})
	}
}

func TestBuildPlanYDBDatabaseSelectors(t *testing.T) {
	for _, test := range []struct {
		name       string
		connection string
		redacted   string
	}{
		{
			name:       "database in endpoint path",
			connection: "grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g-dev/etn-dev",
			redacted:   "grpcs://ydb.serverless.yandexcloud.net:2135/ru-central1/b1g-dev/etn-dev",
		},
		{
			name:       "Yandex query database with root endpoint",
			connection: "grpcs://ydb.serverless.yandexcloud.net:2135/?database=/ru-central1/b1g-dev/etn-dev",
			redacted:   "grpcs://ydb.serverless.yandexcloud.net:2135/?database=%2Fru-central1%2Fb1g-dev%2Fetn-dev",
		},
		{
			name:       "Yandex query database without endpoint path",
			connection: "grpcs://ydb.serverless.yandexcloud.net:2135?database=/ru-central1/b1g-dev/etn-dev",
			redacted:   "grpcs://ydb.serverless.yandexcloud.net:2135?database=%2Fru-central1%2Fb1g-dev%2Fetn-dev",
		},
		{
			name:       "encoded query database",
			connection: "grpcs://ydb.serverless.yandexcloud.net:2135/?database=%2Fru-central1%2Fb1g-dev%2Fetn-dev",
			redacted:   "grpcs://ydb.serverless.yandexcloud.net:2135/?database=%2Fru-central1%2Fb1g-dev%2Fetn-dev",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := validTarget()
			target.YDBConnection = test.connection
			plan, err := BuildPlan(target, false)
			if err != nil {
				t.Fatalf("BuildPlan(%q): %v", test.connection, err)
			}
			if plan.Target.YDBConnection != test.redacted {
				t.Errorf("redacted database identity for %q = %q, want %q", test.connection, plan.Target.YDBConnection, test.redacted)
			}
			if plan.Target.FolderID != target.FolderID || plan.Target.ArtifactBucket != target.ArtifactBucket || plan.Target.ObjectPrefix != RequiredObjectPrefix {
				t.Errorf("BuildPlan(%q) changed resolved reset scope: %#v", test.connection, plan.Target)
			}
			if _, err := BuildPlan(target, true); err == nil {
				t.Errorf("BuildPlan(%q) accepted missing typed confirmation", test.connection)
			}
			target.Confirmation = ExpectedConfirmation(target)
			if _, err := BuildPlan(target, true); err != nil {
				t.Errorf("BuildPlan(%q) rejected matching typed confirmation: %v", test.connection, err)
			}
		})
	}
}

func TestBuildPlanRejectsAmbiguousYDBDatabaseSelectors(t *testing.T) {
	for _, test := range []struct {
		name       string
		connection string
	}{
		{name: "missing database", connection: "grpcs://example/"},
		{name: "empty query database", connection: "grpcs://example/?database="},
		{name: "root query database", connection: "grpcs://example/?database=/"},
		{name: "relative query database", connection: "grpcs://example/?database=dev/db"},
		{name: "whitespace query database", connection: "grpcs://example/?database=%20"},
		{name: "interior whitespace", connection: "grpcs://example/?database=/dev/my%20db"},
		{name: "duplicate query database", connection: "grpcs://example/?database=/dev/db&database=/dev/db"},
		{name: "conflicting query database", connection: "grpcs://example/?database=/dev/db&database=/dev/other"},
		{name: "mixed path and query", connection: "grpcs://example/dev/db?database=/dev/db"},
		{name: "unknown query option", connection: "grpcs://example/?database=/dev/db&unknown=1"},
		{name: "query credential", connection: "grpcs://example/?database=/dev/db&token=synthetic"},
		{name: "path credential", connection: "grpcs://example/dev/db?token=synthetic"},
		{name: "malformed query encoding", connection: "grpcs://example/?database=%zz"},
		{name: "query semicolon", connection: "grpcs://example/?database=/dev/db;other=1"},
		{name: "empty query", connection: "grpcs://example/dev/db?"},
		{name: "userinfo", connection: "grpcs://synthetic@example/?database=/dev/db"},
		{name: "fragment", connection: "grpcs://example/?database=/dev/db#unexpected"},
		{name: "empty fragment", connection: "grpcs://example/?database=/dev/db#"},
		{name: "missing hostname", connection: "grpcs://:2135/?database=/dev/db"},
		{name: "wrong scheme", connection: "grpc://example/?database=/dev/db"},
		{name: "production database", connection: "grpcs://example/?database=/production/db"},
		{name: "encoded production database", connection: "grpcs://example/?database=/%70roduction/db"},
		{name: "encoded production path", connection: "grpcs://example/%70roduction/db"},
		{name: "production endpoint", connection: "grpcs://prod-example/?database=/dev/db"},
		{name: "dot traversal", connection: "grpcs://example/?database=/dev/../db"},
		{name: "empty database segment", connection: "grpcs://example/?database=/dev//db"},
		{name: "backslash database", connection: "grpcs://example/?database=/dev%5Cdb"},
		{name: "control character database", connection: "grpcs://example/?database=/dev/db%00"},
		{name: "Unicode whitespace database", connection: "grpcs://example/?database=/dev/my%C2%A0db"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := validTarget()
			target.YDBConnection = test.connection
			if plan, err := BuildPlan(target, false); err == nil {
				t.Fatalf("BuildPlan(%q) accepted ambiguous/unsafe database: %#v", test.connection, plan)
			}
		})
	}
}

func TestExecuteRequiresTypedConfirmationAndDropsOnlyAllowlist(t *testing.T) {
	target := validTarget()
	target.Confirmation = "wrong"
	if _, err := Execute(context.Background(), target, &recordingSchema{}, prefixDeleter{}, emptyCredentialGuard{}); err == nil {
		t.Fatal("reset accepted wrong confirmation")
	}
	target.Confirmation = ExpectedConfirmation(target)
	schema := &recordingSchema{}
	result, err := Execute(context.Background(), target, schema, prefixDeleter{count: 7}, emptyCredentialGuard{})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedObjects != 7 || len(result.DroppedTables) != len(applicationTables) {
		t.Fatalf("reset result = %#v", result)
	}
	if len(schema.statements) != len(applicationTables) {
		t.Fatalf("schema statements = %d", len(schema.statements))
	}
	for index, statement := range schema.statements {
		if statement != "DROP TABLE IF EXISTS `"+applicationTables[index]+"`" || strings.Contains(statement, "/") {
			t.Fatalf("statement[%d] = %q", index, statement)
		}
	}
}

func TestExecuteFailsBeforeDeletionWhenProviderCredentialsAreNotDrained(t *testing.T) {
	target := validTarget()
	target.Confirmation = ExpectedConfirmation(target)
	objects := &recordingPrefixDeleter{}
	if _, err := Execute(context.Background(), target, &recordingSchema{}, objects, failingCredentialGuard{}); err == nil {
		t.Fatal("reset accepted undrained provider credential authority")
	}
	if objects.called {
		t.Fatal("object deletion began before provider credential drain proof")
	}
}

func TestSQLProviderCredentialResetGuardInventory(t *testing.T) {
	credentialTables := []string{
		"provider_credential_cleanup_ready_v1",
		"provider_credential_cleanups",
		"provider_credential_candidate_fences",
		"provider_credential_bindings",
		"provider_credential_audit_events",
	}
	queryFailure := errors.New("synthetic query permission failure")
	inventoryFailure := errors.New("synthetic scheme permission failure")
	for _, test := range []struct {
		name             string
		tables           []string
		inventoryError   error
		withoutInventory bool
		count            uint64
		queryError       error
		wantError        bool
		wantQueries      int
	}{
		{name: "authoritative legacy baseline", tables: []string{"tenants", "worker_jobs"}, queryError: queryFailure},
		{name: "authoritative empty database", tables: []string{}, queryError: queryFailure},
		{name: "complete empty schema", tables: credentialTables, wantQueries: 4},
		{name: "strict default retains SQL checks", withoutInventory: true, wantQueries: 4},
		{name: "partial schema", tables: credentialTables[:4], wantError: true},
		{name: "audit only is not a legacy baseline", tables: credentialTables[4:], wantError: true},
		{name: "duplicate entries do not complete schema", tables: []string{credentialTables[0], credentialTables[0], credentialTables[0], credentialTables[0], credentialTables[0]}, wantError: true},
		{name: "inventory failure never means absence", inventoryError: inventoryFailure, wantError: true},
		{name: "partial inventory with failure", tables: []string{"tenants"}, inventoryError: inventoryFailure, wantError: true},
		{name: "nonempty credential authority", tables: credentialTables, count: 1, wantError: true, wantQueries: 1},
		{name: "SQL error never means absence", tables: credentialTables, queryError: queryFailure, wantError: true, wantQueries: 1},
		{name: "strict SQL error without inventory", withoutInventory: true, queryError: queryFailure, wantError: true, wantQueries: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &credentialCountConnector{count: test.count, queryError: test.queryError}
			db := sql.OpenDB(fixture)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close credential-count fixture: %v", err)
				}
			})
			guard := SQLProviderCredentialResetGuard{DB: db}
			inventoryCalls := 0
			if !test.withoutInventory {
				guard.TableInventory = func(context.Context) ([]string, error) {
					inventoryCalls++
					return append([]string(nil), test.tables...), test.inventoryError
				}
			}
			target := validTarget()
			target.Confirmation = ExpectedConfirmation(target)
			objects := &recordingPrefixDeleter{}
			schema := &recordingSchema{}
			_, err := Execute(context.Background(), target, schema, objects, guard)
			if (err != nil) != test.wantError {
				t.Fatalf("Execute inventory=%v, count=%d: error=%v, wantError=%v", test.tables, test.count, err, test.wantError)
			}
			if objects.called == test.wantError || (len(schema.statements) != 0 && test.wantError) {
				t.Errorf("guard error=%v, object deletion=%v, schema deletions=%d", err, objects.called, len(schema.statements))
			}
			if len(fixture.queries) != test.wantQueries {
				t.Errorf("COUNT queries=%v, want %d queries", fixture.queries, test.wantQueries)
			}
			for index, query := range fixture.queries {
				if index >= 4 || query != "SELECT COUNT(*) FROM `"+credentialTables[index]+"`" {
					t.Errorf("credential COUNT query[%d]=%q does not preserve the existing four authority checks", index, query)
				}
			}
			wantInventoryCalls := 1
			if test.withoutInventory {
				wantInventoryCalls = 0
			}
			if inventoryCalls != wantInventoryCalls {
				t.Errorf("inventory calls=%d, want %d", inventoryCalls, wantInventoryCalls)
			}
			if test.inventoryError != nil && !errors.Is(err, test.inventoryError) {
				t.Errorf("inventory causal error lost: %v", err)
			}
			if test.queryError != nil && test.wantQueries > 0 && !errors.Is(err, test.queryError) {
				t.Errorf("query causal error lost: %v", err)
			}
		})
	}
}

func TestSQLProviderCredentialResetGuardRequiresOpenDatabase(t *testing.T) {
	called := false
	guard := SQLProviderCredentialResetGuard{TableInventory: func(context.Context) ([]string, error) {
		called = true
		return nil, nil
	}}
	if err := guard.AssertProviderCredentialsDrained(context.Background()); err == nil {
		t.Fatal("legacy inventory admitted reset without an open YDB database")
	}
	if called {
		t.Fatal("inventory consulted before confirming an open database")
	}
}

// Each test owns its connector and DB; no process-global driver registration.
type credentialCountConnector struct {
	count      uint64
	queryError error
	queries    []string
}

func (fixture *credentialCountConnector) Connect(context.Context) (driver.Conn, error) {
	return credentialCountConn{fixture: fixture}, nil
}
func (fixture *credentialCountConnector) Driver() driver.Driver {
	return credentialCountDriver{fixture: fixture}
}

type credentialCountDriver struct{ fixture *credentialCountConnector }

func (driver credentialCountDriver) Open(string) (driver.Conn, error) {
	return credentialCountConn{fixture: driver.fixture}, nil
}

type credentialCountConn struct{ fixture *credentialCountConnector }

func (conn credentialCountConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (conn credentialCountConn) Close() error { return nil }
func (conn credentialCountConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (conn credentialCountConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	conn.fixture.queries = append(conn.fixture.queries, query)
	if conn.fixture.queryError != nil {
		return nil, conn.fixture.queryError
	}
	return &credentialCountRows{count: conn.fixture.count}, nil
}

type credentialCountRows struct {
	count uint64
	read  bool
}

func (rows *credentialCountRows) Columns() []string { return []string{"count"} }
func (rows *credentialCountRows) Close() error      { return nil }
func (rows *credentialCountRows) Next(values []driver.Value) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	values[0] = int64(rows.count)
	return nil
}

func TestAttachedWorkerTablesAreExplicitlyResettable(t *testing.T) {
	want := map[string]bool{
		"attached_worker_attempt_deadlines_v1": false,
		"attached_worker_attempt_messages":     false,
		"attached_worker_output_receipts":      false,
		"attached_worker_attempt_heads":        false,
		"attached_worker_audit_events":         false,
		"attached_worker_enrollments":          false,
		"attached_workers":                     false,
	}
	for _, table := range applicationTables {
		if _, exists := want[table]; exists {
			want[table] = true
		}
	}
	for table, present := range want {
		if !present {
			t.Errorf("attached-worker table %s is absent from the guarded reset allowlist", table)
		}
	}
}

func TestProviderCredentialTablesAreExplicitlyResettable(t *testing.T) {
	want := map[string]bool{
		"provider_credential_cleanup_ready_v1": false,
		"provider_credential_cleanups":         false,
		"provider_credential_audit_events":     false,
		"provider_credential_candidate_fences": false,
		"provider_credential_bindings":         false,
	}
	for _, table := range applicationTables {
		if _, exists := want[table]; exists {
			want[table] = true
		}
	}
	for table, present := range want {
		if !present {
			t.Errorf("provider credential table %s is absent from the guarded reset allowlist", table)
		}
	}
}

type recordingSchema struct{ statements []string }

func (schema *recordingSchema) ExecContext(_ context.Context, statement string, _ ...any) (sql.Result, error) {
	schema.statements = append(schema.statements, statement)
	return driver.RowsAffected(0), nil
}

type prefixDeleter struct{ count uint64 }

func (deleter prefixDeleter) DeletePrefix(_ context.Context, prefix string) (uint64, error) {
	if prefix != RequiredObjectPrefix {
		panic("unexpected prefix")
	}
	return deleter.count, nil
}

type emptyCredentialGuard struct{}

func (emptyCredentialGuard) AssertProviderCredentialsDrained(context.Context) error { return nil }

type failingCredentialGuard struct{}

func (failingCredentialGuard) AssertProviderCredentialsDrained(context.Context) error {
	return errors.New("provider secret namespace is not drained")
}

type recordingPrefixDeleter struct{ called bool }

func (deleter *recordingPrefixDeleter) DeletePrefix(context.Context, string) (uint64, error) {
	deleter.called = true
	return 0, nil
}
