//go:build ydbintegration

package ydbintegration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestCanonicalSessionQueriesUseBoundedPlans(t *testing.T) {
	_, client := openStore(t)
	tenantID := domain.TenantID("tenant-query-plan")
	userID := domain.UserID("user-query-plan")
	sessionID := domain.SessionID("session-query-plan")
	runID := domain.RunID("run-query-plan")
	workerID := domain.AttachedWorkerID("worker-query-plan")
	enrollmentID := domain.AttachedWorkerEnrollmentID("worker-enrollment-query-plan")
	limit := uint64(100)

	tests := []struct {
		name     string
		query    string
		args     []any
		contract queryPlanContract
	}{
		{
			name:  "session point lookup",
			query: `SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
			args:  []any{tenantID, sessionID},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "sessions",
			},
		},
		{
			name: "session participant authorization point lookup",
			query: `SELECT record FROM session_participants
				WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
			args: []any{tenantID, sessionID, userID},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "session_participants",
			},
		},
		{
			name:  "session display point lookup",
			query: `SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2`,
			args:  []any{tenantID, sessionID},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "session_displays",
			},
		},
		{
			name: "frontend binding identity point lookup",
			query: `SELECT binding_id FROM frontend_binding_keys
				WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
			args: []any{tenantID, domain.FrontendTelegram, "conversation-query-plan"},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "frontend_binding_keys",
			},
		},
		{
			name: "attached worker enrollment point lookup",
			query: `SELECT record FROM attached_worker_enrollments
				WHERE tenant_id = $1 AND owner_user_id = $2 AND enrollment_id = $3`,
			args: []any{tenantID, userID, enrollmentID},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "attached_worker_enrollments",
			},
		},
		{
			name: "attached worker point lookup",
			query: `SELECT record FROM attached_workers
				WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`,
			args: []any{tenantID, userID, workerID},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "attached_workers",
			},
		},
		{
			name: "attached worker audit point lookup",
			query: `SELECT version, enrollment_id, action, enrollment_generation,
					connection_generation, occurred_at
				FROM attached_worker_audit_events
				WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3
					AND worker_revision = $4`,
			args: []any{tenantID, userID, workerID, uint64(1)},
			contract: queryPlanContract{
				operator: "TablePointLookup",
				table:    "attached_worker_audit_events",
			},
		},
		{
			name: "event history range",
			query: `SELECT record FROM session_events
				WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
				ORDER BY sequence ASC LIMIT $4`,
			args: []any{tenantID, sessionID, uint64(0), limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "session_events",
			},
		},
		{
			name: "session activity bucket range",
			query: `SELECT session_id, updated_at FROM session_activity
				WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4
				ORDER BY updated_at DESC, session_id DESC LIMIT $5`,
			args: []any{tenantID, userID, domain.SessionActive, uint32(0), limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "session_activity",
			},
		},
		{
			name: "subscription connections by user range",
			query: `SELECT subscription_connection_id FROM subscription_connections_by_user
				WHERE tenant_id = $1 AND user_id = $2
				ORDER BY subscription_connection_id ASC LIMIT $3`,
			args: []any{tenantID, userID, uint64(2)},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "subscription_connections_by_user",
			},
		},
		{
			name: "frontend projection ready time range",
			query: `SELECT tenant_id, frontend_projection_id, run_id
				FROM frontend_projection_ready_v1
				WHERE frontend = $1 AND shard_bucket = $2 AND created_at <= $3
				ORDER BY created_at, tenant_id, frontend_projection_id
				LIMIT $4`,
			args: []any{
				domain.FrontendTelegram,
				uint32(0),
				time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC),
				limit,
			},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "frontend_projection_ready_v1",
			},
		},
		{
			name: "runs by session range",
			query: `SELECT run_id FROM runs_by_session
				WHERE tenant_id = $1 AND session_id = $2
				ORDER BY created_at DESC, run_id DESC LIMIT $3`,
			args: []any{tenantID, sessionID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "runs_by_session",
			},
		},
		{
			name: "frontend bindings by session range",
			query: `SELECT binding_id FROM frontend_bindings_by_session
				WHERE tenant_id = $1 AND session_id = $2 ORDER BY binding_id ASC LIMIT $3`,
			args: []any{tenantID, sessionID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "frontend_bindings_by_session",
			},
		},
		{
			name: "frontend projections by session range",
			query: `SELECT frontend_projection_id FROM frontend_projections_by_session
				WHERE tenant_id = $1 AND session_id = $2 ORDER BY frontend_projection_id ASC LIMIT $3`,
			args: []any{tenantID, sessionID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "frontend_projections_by_session",
			},
		},
		{
			name: "artifact manifests by run range",
			query: `SELECT artifact_manifest_id FROM artifact_manifests_by_run
				WHERE tenant_id = $1 AND run_id = $2 ORDER BY artifact_manifest_id ASC LIMIT $3`,
			args: []any{tenantID, runID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "artifact_manifests_by_run",
			},
		},
		{
			name: "telegram deliveries by run range",
			query: `SELECT telegram_delivery_id FROM telegram_deliveries_by_run
				WHERE tenant_id = $1 AND run_id = $2 ORDER BY telegram_delivery_id ASC LIMIT $3`,
			args: []any{tenantID, runID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "telegram_deliveries_by_run",
			},
		},
		{
			name: "checkpoint objects by run range",
			query: `SELECT checkpoint_id FROM checkpoint_objects_by_run
				WHERE tenant_id = $1 AND run_id = $2 ORDER BY checkpoint_id ASC LIMIT $3`,
			args: []any{tenantID, runID, limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "checkpoint_objects_by_run",
			},
		},
		{
			name: "attached workers by owner range",
			query: `SELECT record FROM attached_workers
				WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id > $3
				ORDER BY worker_id ASC LIMIT $4`,
			args: []any{tenantID, userID, domain.AttachedWorkerID(""), limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "attached_workers",
			},
		},
		{
			name: "attached worker audit by owner and worker range",
			query: `SELECT version, enrollment_id, action, worker_revision,
					enrollment_generation, connection_generation, occurred_at
				FROM attached_worker_audit_events
				WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3
					AND worker_revision >= $4
				ORDER BY worker_revision ASC LIMIT $5`,
			args: []any{tenantID, userID, workerID, uint64(0), limit},
			contract: queryPlanContract{
				operator: "TableRangeScan",
				table:    "attached_worker_audit_events",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var ast, plan string
			err := client.DB.QueryRowContext(
				ydb.WithQueryMode(context.Background(), ydb.ExplainQueryMode),
				test.query,
				test.args...,
			).Scan(&ast, &plan)
			if err != nil {
				t.Fatalf("explain query: %v", err)
			}
			if strings.TrimSpace(ast) == "" {
				t.Fatal("explain query returned an empty AST")
			}
			if err := validateBoundedQueryPlan(plan, test.contract); err != nil {
				t.Fatalf("query plan contract: %v\nplan: %s", err, plan)
			}
		})
	}
}

type queryPlanContract struct {
	operator string
	table    string
}

type queryPlanFacts struct {
	allStrings  []string
	tableValues []string
}

func validateBoundedQueryPlan(plan string, contract queryPlanContract) error {
	if strings.TrimSpace(plan) == "" {
		return errors.New("plan is empty")
	}
	if strings.TrimSpace(contract.operator) == "" || strings.TrimSpace(contract.table) == "" {
		return errors.New("operator and table expectations are required")
	}

	decoder := json.NewDecoder(strings.NewReader(plan))
	decoder.UseNumber()
	var root any
	if err := decoder.Decode(&root); err != nil {
		return fmt.Errorf("decode plan JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("plan JSON contains more than one value")
		}
		return fmt.Errorf("decode trailing plan JSON: %w", err)
	}
	if _, object := root.(map[string]any); !object {
		if _, array := root.([]any); !array {
			return fmt.Errorf("plan JSON root has unsupported type %T", root)
		}
	}

	facts := queryPlanFacts{}
	collectQueryPlanFacts(root, false, &facts)
	for _, value := range facts.allStrings {
		if strings.Contains(compactPlanToken(value), "tablefullscan") {
			return fmt.Errorf("plan contains forbidden TableFullScan operator in %q", value)
		}
	}
	if !containsPlanToken(facts.allStrings, contract.operator) {
		return fmt.Errorf("plan does not contain expected operator %q", contract.operator)
	}
	if !containsPlanTable(facts.tableValues, contract.table) {
		return fmt.Errorf("plan does not identify expected table %q in a table field", contract.table)
	}
	return nil
}

func collectQueryPlanFacts(value any, tableContext bool, facts *queryPlanFacts) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			facts.allStrings = append(facts.allStrings, key)
			collectQueryPlanFacts(child, tableContext || isPlanTableField(key), facts)
		}
	case []any:
		for _, child := range value {
			collectQueryPlanFacts(child, tableContext, facts)
		}
	case string:
		facts.allStrings = append(facts.allStrings, value)
		if tableContext {
			facts.tableValues = append(facts.tableValues, value)
		}
	}
}

func isPlanTableField(key string) bool {
	normalized := compactPlanToken(key)
	return normalized == "table" || normalized == "tables" || normalized == "tablename" || normalized == "tablepath"
}

func containsPlanToken(values []string, expected string) bool {
	expected = compactPlanToken(expected)
	for _, value := range values {
		if strings.Contains(compactPlanToken(value), expected) {
			return true
		}
	}
	return false
}

func containsPlanTable(values []string, expected string) bool {
	expected = strings.ToLower(strings.Trim(strings.TrimSpace(expected), "`\""))
	for _, value := range values {
		value = strings.Trim(strings.TrimSpace(value), "`\"")
		parts := strings.FieldsFunc(value, func(r rune) bool {
			return r == '/' || r == '\\'
		})
		for _, part := range parts {
			if strings.ToLower(strings.Trim(part, "`\"")) == expected {
				return true
			}
		}
	}
	return false
}

func compactPlanToken(value string) string {
	var compact strings.Builder
	compact.Grow(len(value))
	for _, r := range strings.ToLower(value) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			compact.WriteRune(r)
		}
	}
	return compact.String()
}

func TestValidateBoundedQueryPlan(t *testing.T) {
	t.Run("accepts bounded range", func(t *testing.T) {
		plan := `{"Plan":{"Plans":[{"Node Type":"TableLookup","Operators":[{"Name":"TableRangeScan","Table":"/local/session_events"}]}]}}`
		err := validateBoundedQueryPlan(plan, queryPlanContract{
			operator: "TableRangeScan",
			table:    "session_events",
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	for _, test := range []struct {
		name string
		plan string
	}{
		{name: "malformed JSON", plan: `{"Plan":`},
		{name: "full scan", plan: `{"Plan":{"Operators":[{"Name":"TableFullScan","Table":"/local/session_events"}]}}`},
		{name: "wrong table", plan: `{"Plan":{"Operators":[{"Name":"TableRangeScan","Table":"/local/sessions"}]}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateBoundedQueryPlan(test.plan, queryPlanContract{
				operator: "TableRangeScan",
				table:    "session_events",
			})
			if err == nil {
				t.Fatal("plan unexpectedly satisfied the bounded query contract")
			}
		})
	}
}
