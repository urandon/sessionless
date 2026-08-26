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
