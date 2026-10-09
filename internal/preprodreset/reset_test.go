package preprodreset

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
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
