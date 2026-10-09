// Package preprodreset implements the fail-closed application-data reset used
// only before the first production migration baseline is frozen.
package preprodreset

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"unicode"
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

type ProviderCredentialResetGuard interface {
	AssertProviderCredentialsDrained(context.Context) error
}

type SQLProviderCredentialResetGuard struct {
	DB *sql.DB
	// TableInventory, when supplied, must return a successful, complete native
	// scheme inventory of the same open YDB connection's exact database root.
	// Only authoritative absence of all five tables admits a legacy baseline.
	// SQL or inventory errors are never interpreted as absence.
	TableInventory func(context.Context) ([]string, error)
}

func (guard SQLProviderCredentialResetGuard) AssertProviderCredentialsDrained(ctx context.Context) error {
	if guard.DB == nil {
		return fmt.Errorf("provider credential reset guard requires YDB")
	}
	if guard.TableInventory != nil {
		tables, err := guard.TableInventory(ctx)
		if err != nil {
			return fmt.Errorf("verify provider credential table inventory: %w", err)
		}
		present := make(map[string]bool, len(tables))
		for _, table := range tables {
			present[table] = true
		}
		credentialTables := []string{
			"provider_credential_cleanup_ready_v1",
			"provider_credential_cleanups",
			"provider_credential_candidate_fences",
			"provider_credential_bindings",
			"provider_credential_audit_events",
		}
		found := 0
		for _, table := range credentialTables {
			if present[table] {
				found++
			}
		}
		if found == 0 {
			return nil
		}
		if found != len(credentialTables) {
			return fmt.Errorf("provider credential reset requires a complete credential schema or an authoritative pre-credential baseline")
		}
	}
	for _, table := range []string{"provider_credential_cleanup_ready_v1", "provider_credential_cleanups", "provider_credential_candidate_fences", "provider_credential_bindings"} {
		var count uint64
		if err := guard.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&count); err != nil {
			return fmt.Errorf("verify provider credential reset guard: %w", err)
		}
		if count != 0 {
			return fmt.Errorf("provider credential reset requires an explicitly drained secret namespace and empty metadata tables")
		}
	}
	return nil
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
	connection, err := cloudDevYDBConnection(target.YDBConnection)
	if err != nil {
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
	redacted.YDBConnection = connection.String()
	redacted.Confirmation = ""
	return Plan{Target: redacted, Tables: append([]string(nil), applicationTables...)}, nil
}

// Yandex's public DSN selects the database through a query parameter, while
// older callers use the endpoint path. Admit exactly one unambiguous selector;
// dropping the query would lose the identity of the database being planned.
func cloudDevYDBConnection(value string) (*url.URL, error) {
	connection, err := url.Parse(value)
	if err != nil || connection.Scheme != "grpcs" || connection.Hostname() == "" ||
		connection.User != nil || strings.Contains(value, "#") || connection.Opaque != "" ||
		connection.ForceQuery || containsProduction(value) {
		return nil, fmt.Errorf("invalid cloud-dev YDB endpoint")
	}
	query, err := url.ParseQuery(connection.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("invalid cloud-dev YDB query")
	}
	database := connection.Path
	if connection.RawQuery != "" {
		selectors, exists := query["database"]
		if !exists || len(query) != 1 || len(selectors) != 1 ||
			(connection.Path != "" && connection.Path != "/") {
			return nil, fmt.Errorf("ambiguous cloud-dev YDB database selector")
		}
		database = selectors[0]
		connection.RawQuery = url.Values{"database": {database}}.Encode()
	}
	if !explicitDatabasePath(database) || containsProduction(database) {
		return nil, fmt.Errorf("invalid cloud-dev YDB database")
	}
	return connection, nil
}

func explicitDatabasePath(database string) bool {
	if !strings.HasPrefix(database, "/") || strings.Contains(database, "\\") ||
		strings.IndexFunc(database, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return false
	}
	for _, segment := range strings.Split(strings.TrimPrefix(database, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func Execute(
	ctx context.Context,
	target Target,
	schema SchemaExecutor,
	objects PrefixDeleter,
	credentialGuard ProviderCredentialResetGuard,
) (Result, error) {
	plan, err := BuildPlan(target, true)
	if err != nil {
		return Result{}, err
	}
	if schema == nil || objects == nil || credentialGuard == nil {
		return Result{}, fmt.Errorf("reset executors must not be nil")
	}
	if err := credentialGuard.AssertProviderCredentialsDrained(ctx); err != nil {
		return Result{}, err
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
	"provider_credential_cleanup_ready_v1",
	"provider_credential_cleanups",
	"provider_credential_candidate_fences",
	"provider_credential_audit_events",
	"provider_credential_bindings",
	"attached_worker_attempt_deadlines_v1",
	"attached_worker_attempt_messages",
	"attached_worker_output_receipts",
	"attached_worker_attempt_heads",
	"attached_worker_presence_expiry_v1",
	"attached_worker_connections",
	"attached_worker_capability_manifests",
	"attached_worker_attach_challenges",
	"attached_worker_audit_events",
	"attached_worker_enrollments",
	"attached_workers",
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
	"telegram_deliveries_by_run",
	"checkpoint_objects_by_run",
	"session_lifecycle_backfill_state",
	"execution_placement_cutover_state",
	"harness_binding_cutover_state",
	"managed_execution_authority_v2_cutover_state",
	"attempt_effect_reservations",
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
	"artifact_manifests_by_run",
	"attempts",
	"runs_by_session",
	"run_idempotency",
	"run_finalizations",
	"runs",
	"artifact_manifests",
	"telegram_updates",
	"tenant_scheduler_counters",
	"subscription_scheduler_slots",
	"subscription_connections_by_user",
	"subscription_connections",
	"session_activity",
	"session_api_idempotency",
	"session_api_mutations",
	"session_displays",
	"session_deletions",
	"session_legal_holds",
	"session_snapshots",
	"session_participants",
	"frontend_binding_keys",
	"frontend_bindings_by_session",
	"frontend_bindings",
	"frontend_ingress_idempotency",
	"frontend_projection_outbox",
	"frontend_projections_by_session",
	"frontend_projections_by_run",
	"frontend_projection_ready_v1",
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
