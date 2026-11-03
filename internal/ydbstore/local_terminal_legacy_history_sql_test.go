package ydbstore

import (
	"fmt"
	"strings"
	"testing"
)

// A source contract, not a YQL compiler. The existing disposable native gate
// executes the actual constant; scripted history readers cannot prove grammar.
func validateLocalTerminalLegacyJoinShape(statement string) error {
	s := strings.Join(strings.Fields(statement), " ")
	projection, from, ok := strings.Cut(s, " FROM ")
	if !ok || strings.Contains(s, ";") || from != "(SELECT $1 AS tenant_id,$2 AS attempt_id,$3 AS kind) AS k INNER JOIN attempt_effect_reservations AS p ON p.tenant_id=k.tenant_id AND p.attempt_id=k.attempt_id AND p.kind=k.kind INNER JOIN substrate_execution_evidence AS e ON e.tenant_id=k.tenant_id AND e.attempt_id=k.attempt_id" {
		return fmt.Errorf("legacy history requires one three-key row and full-key cross-input joins")
	}
	want := `SELECT
 SUBSTRING(CAST(e.run_id AS String),0,161),SUBSTRING(CAST(e.attempt_id AS String),0,161),
 SUBSTRING(CAST(e.physical_invocation_claim_id AS String),0,161),
 SUBSTRING(CAST(e.evidence_digest AS String),0,65),SUBSTRING(CAST(e.record AS String),0,32769),
 SUBSTRING(CAST(p.record AS String),0,32769),
 CASE WHEN p.owned_record IS NOT NULL OR p.owned_digest IS NOT NULL
 OR p.preparation_started_at IS NOT NULL OR p.preparation_parent_digest IS NOT NULL
 OR p.allocation_original_record IS NOT NULL OR p.allocation_original_digest IS NOT NULL
 OR p.allocation_current_record IS NOT NULL OR p.allocation_current_digest IS NOT NULL
 OR p.allocation_publication_digest IS NOT NULL OR p.allocation_transition_parent_digest IS NOT NULL
 OR p.proxy_original_record IS NOT NULL OR p.proxy_original_digest IS NOT NULL
 OR p.proxy_current_record IS NOT NULL OR p.proxy_current_digest IS NOT NULL
 OR p.proxy_policy_body IS NOT NULL OR p.proxy_attestation_body IS NOT NULL
 OR p.operation_accounting_record IS NOT NULL OR p.operation_accounting_digest IS NOT NULL
 OR p.web_search_consent_record IS NOT NULL OR p.web_search_consent_digest IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END`
	if projection != strings.Join(strings.Fields(want), " ") {
		return fmt.Errorf("legacy history lost its seven bounded cells or original twenty owned-contamination guards")
	}
	return nil
}

func TestLocalTerminalLegacyHistoryJoinInputs(t *testing.T) {
	if err := validateLocalTerminalLegacyJoinShape(localTerminalLegacyEvidenceSQL); err != nil {
		t.Fatal(err)
	}
}

func TestLocalTerminalLegacyHistoryJoinShapeRejectsWeakening(t *testing.T) {
	mutations := []struct{ name, old, replacement string }{
		{"scalar-kind", "p.kind=k.kind", "p.kind=$3"},
		{"missing-kind", " AND p.kind=k.kind", ""},
		{"missing-attempt", " AND p.attempt_id=k.attempt_id", ""},
		{"foreign-tenant", "e.tenant_id=k.tenant_id", "e.tenant_id=p.attempt_id"},
		{"foreign-attempt", "e.attempt_id=k.attempt_id", "e.attempt_id=p.tenant_id"},
		{"left-fallback", "INNER JOIN substrate_execution_evidence", "LEFT JOIN substrate_execution_evidence"},
		{"limit-masks-fanout", "e.attempt_id=k.attempt_id", "e.attempt_id=k.attempt_id LIMIT 1"},
		{"unbounded-effect", "SUBSTRING(CAST(p.record AS String),0,32769)", "p.record"},
		{"shortened-evidence", "e.record AS String),0,32769)", "e.record AS String),0,32768)"},
		{"second-query", "SELECT\n", "SELECT 1; SELECT\n"},
	}
	for _, field := range []string{
		"owned_record", "owned_digest", "preparation_started_at", "preparation_parent_digest",
		"allocation_original_record", "allocation_original_digest", "allocation_current_record", "allocation_current_digest",
		"allocation_publication_digest", "allocation_transition_parent_digest", "proxy_original_record", "proxy_original_digest",
		"proxy_current_record", "proxy_current_digest", "proxy_policy_body", "proxy_attestation_body",
		"operation_accounting_record", "operation_accounting_digest", "web_search_consent_record", "web_search_consent_digest",
	} {
		mutations = append(mutations, struct{ name, old, replacement string }{"missing-" + field, "p." + field + " IS NOT NULL", "FALSE"})
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			changed := strings.Replace(localTerminalLegacyEvidenceSQL, tc.old, tc.replacement, 1)
			if changed == localTerminalLegacyEvidenceSQL {
				t.Fatal("ineffective legacy-history query mutation")
			}
			if err := validateLocalTerminalLegacyJoinShape(changed); err == nil {
				t.Fatal("weakened legacy-history query accepted")
			}
		})
	}
}
