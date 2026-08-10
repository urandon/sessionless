// Package preprodreset implements the fail-closed application-data reset used
// only before the first production migration baseline is frozen.
package preprodreset

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
)

const RequiredObjectPrefix = "tenants/"

type Target struct {
	Environment    string `json:"environment"`
	FolderID       string `json:"folder_id"`
	YDBConnection  string `json:"ydb_connection"`
	ArtifactBucket string `json:"artifact_bucket"`
	ObjectPrefix   string `json:"object_prefix"`
	Confirmation   string `json:"-"`
}

type Plan struct {
	Target Target   `json:"target"`
	Tables []string `json:"tables"`
}

type SchemaExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type PrefixDeleter interface {
	DeletePrefix(context.Context, string) (uint64, error)
}

type Result struct {
	DeletedObjects uint64   `json:"deleted_objects"`
	DroppedTables  []string `json:"dropped_tables"`
}

func ExpectedConfirmation(target Target) string {
	return fmt.Sprintf("reset-sessionless-cloud-dev:%s:%s", target.FolderID, target.ArtifactBucket)
}

func BuildPlan(target Target, requireConfirmation bool) (Plan, error) {
	if target.Environment != "cloud-dev" {
		return Plan{}, fmt.Errorf("pre-production reset requires APP_ENV=cloud-dev")
	}
	if strings.TrimSpace(target.FolderID) == "" || containsProduction(target.FolderID) {
		return Plan{}, fmt.Errorf("cloud-dev folder identity is missing or production-like")
	}
	connection, err := url.Parse(target.YDBConnection)
	if err != nil || connection.Scheme != "grpcs" || connection.Host == "" ||
		strings.Trim(connection.Path, "/") == "" || connection.User != nil || connection.Fragment != "" ||
		containsProduction(target.YDBConnection) {
		return Plan{}, fmt.Errorf("cloud-dev YDB connection must be an explicit non-production grpcs endpoint")
	}
	bucket := strings.ToLower(strings.TrimSpace(target.ArtifactBucket))
	if bucket == "" || !strings.Contains(bucket, "sessionless") || !strings.Contains(bucket, "dev") || containsProduction(bucket) {
		return Plan{}, fmt.Errorf("artifact bucket must be an explicit Sessionless development bucket")
	}
	if target.ObjectPrefix != RequiredObjectPrefix {
		return Plan{}, fmt.Errorf("object prefix must be exactly %q", RequiredObjectPrefix)
	}
	if requireConfirmation && target.Confirmation != ExpectedConfirmation(target) {
		return Plan{}, fmt.Errorf("typed reset confirmation does not match the resolved cloud-dev target")
	}
	redacted := target
	redacted.YDBConnection = connection.Scheme + "://" + connection.Host + connection.Path
	redacted.Confirmation = ""
	return Plan{Target: redacted, Tables: append([]string(nil), applicationTables...)}, nil
}

func Execute(
	ctx context.Context,
	target Target,
	schema SchemaExecutor,
	objects PrefixDeleter,
) (Result, error) {
	plan, err := BuildPlan(target, true)
	if err != nil {
		return Result{}, err
	}
	if schema == nil || objects == nil {
		return Result{}, fmt.Errorf("reset executors must not be nil")
	}
	deleted, err := objects.DeletePrefix(ctx, target.ObjectPrefix)
	if err != nil {
		return Result{}, fmt.Errorf("delete Sessionless-owned object prefix: %w", err)
	}
	result := Result{DeletedObjects: deleted}
	for _, table := range plan.Tables {
		statement := fmt.Sprintf("DROP TABLE IF EXISTS `%s`", table)
		if _, err := schema.ExecContext(ctx, statement); err != nil {
			return result, fmt.Errorf("drop application table %s: %w", table, err)
		}
		result.DroppedTables = append(result.DroppedTables, table)
	}
	return result, nil
}

func containsProduction(value string) bool {
	value = strings.ToLower(value)
	return strings.Contains(value, "production") || strings.Contains(value, "-prod") || strings.Contains(value, "prod-")
}

var applicationTables = []string{
	"web_security_audit_events",
	"web_sessions",
	"oidc_login_challenges",
	"development_bootstrap_grants",
	"tenant_invitations",
	"tenant_memberships",
	"external_identities_by_user",
	"external_identities",
	"telegram_delivery_ready_v2",
	"telegram_delivery_ready",
	"telegram_delivery_outbox",
	"dispatch_ready_v2",
	"dispatch_ready",
	"dispatch_outbox",
	"quota_expiry_v2",
	"quota_expiry",
	"quota_reservations",
	"lease_expiry_v2",
	"lease_expiry",
	"lease_heads",
	"leases",
	"checkpoints",
	"usage_observations",
	"worker_jobs",
	"attempts",
	"runs_by_session",
	"run_idempotency",
	"runs",
	"artifact_manifests",
	"telegram_updates",
	"tenant_scheduler_counters",
	"subscription_scheduler_slots",
	"subscription_connections",
	"session_activity",
	"session_snapshots",
	"session_participants",
	"frontend_binding_keys",
	"frontend_bindings",
	"frontend_ingress_idempotency",
	"session_event_idempotency",
	"session_events",
	"sessions",
	"actors",
	"audit_events",
	"tenants",
	"sessionless_goose_versions",
	"schema_migration_checksums",
	"schema_migration_lock",
}
