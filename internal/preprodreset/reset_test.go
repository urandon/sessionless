package preprodreset

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	if _, err := Execute(context.Background(), target, &recordingSchema{}, prefixDeleter{}); err == nil {
		t.Fatal("reset accepted wrong confirmation")
	}
	target.Confirmation = ExpectedConfirmation(target)
	schema := &recordingSchema{}
	result, err := Execute(context.Background(), target, schema, prefixDeleter{count: 7})
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
