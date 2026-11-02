package ydbstore

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// Structural source contract, not a YQL compiler. The pinned native gate must
// still compile and execute this query. Scripted drivers only recognize its
// text and cannot prove that ON expressions depend on both JOIN inputs.
func validateLocalTerminalHistoryJoinShape(statement string) error {
	s := strings.Join(strings.Fields(statement), " ")
	_, from, ok := strings.Cut(s, " FROM ")
	if !ok {
		return fmt.Errorf("missing history source")
	}
	parts := strings.Split(from, " INNER JOIN ")
	fields := strings.Fields(parts[0])
	if len(fields) == 0 {
		return fmt.Errorf("missing left input")
	}
	left := map[string]bool{fields[len(fields)-1]: true}
	column := regexp.MustCompile(`^([a-z]+)\.([a-z_]+)$`)
	for _, part := range parts[1:] {
		header, on, ok := strings.Cut(part, " ON ")
		words := strings.Fields(header)
		if !ok || len(words) != 3 || words[1] != "AS" || left[words[2]] {
			return fmt.Errorf("invalid history JOIN input")
		}
		right := words[2]
		on, _, _ = strings.Cut(on, " WHERE ")
		for _, equality := range strings.Split(on, " AND ") {
			a, b, ok := strings.Cut(equality, "=")
			ac, bc := column.FindStringSubmatch(a), column.FindStringSubmatch(b)
			if !ok || len(ac) != 3 || len(bc) != 3 ||
				!((ac[1] == right && left[bc[1]]) || (bc[1] == right && left[ac[1]])) {
				return fmt.Errorf("history ON equality requires one left and one right input: %s", equality)
			}
		}
		left[right] = true
	}
	if parts[0] != "(SELECT $1 AS tenant_id,$2 AS attempt_id,$3 AS kind,$4 AS artifact_manifest_id) AS k" || len(parts) != 6 {
		return fmt.Errorf("history must be driven by one complete four-key row")
	}
	want := []string{
		"attempt_effect_reservations AS p ON p.tenant_id=k.tenant_id AND p.attempt_id=k.attempt_id AND p.kind=k.kind",
		"attempt_execution_admissions_v1 AS a ON a.tenant_id=p.tenant_id AND a.attempt_id=p.attempt_id",
		"substrate_execution_evidence AS e ON e.tenant_id=p.tenant_id AND e.attempt_id=p.attempt_id",
		"run_finalizations AS f ON f.tenant_id=p.tenant_id AND f.run_id=p.run_id",
		"artifact_manifests AS m ON m.tenant_id=k.tenant_id AND m.artifact_manifest_id=k.artifact_manifest_id",
	}
	for i, join := range want {
		if parts[i+1] != join {
			return fmt.Errorf("history JOIN %d lost its exact full primary-key correlation", i+1)
		}
	}
	projection, _, _ := strings.Cut(s, " FROM ")
	wantProjection := "SELECT SUBSTRING(p.owned_record,0,32769),SUBSTRING(CAST(p.owned_digest AS String),0,65), " +
		"SUBSTRING(a.record,0,32769),SUBSTRING(CAST(a.digest AS String),0,65), " +
		"SUBSTRING(CAST(e.record AS String),0,32769),SUBSTRING(CAST(e.evidence_digest AS String),0,65), " +
		"SUBSTRING(CAST(f.terminal_status AS String),0,33),SUBSTRING(CAST(f.content_digest AS String),0,74), " +
		"SUBSTRING(CAST(m.payload AS String),0,32769), " +
		"SUBSTRING(p.operation_accounting_record,0,32769),SUBSTRING(CAST(p.operation_accounting_digest AS String),0,65)"
	if projection != wantProjection || strings.Contains(s, ";") {
		return fmt.Errorf("history lost its original eleven bounded projections or one-query shape")
	}
	return nil
}

func TestLocalTerminalHistoryJoinInputs(t *testing.T) {
	if err := validateLocalTerminalHistoryJoinShape(localTerminalHistorySQL); err != nil {
		t.Fatal(err)
	}
}

func TestLocalTerminalHistoryJoinShapeRejectsWeakening(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"scalar-manifest", "m.artifact_manifest_id=k.artifact_manifest_id", "m.artifact_manifest_id=$4"},
		{"missing-kind", " AND p.kind=k.kind", ""},
		{"missing-attempt", " AND p.attempt_id=k.attempt_id", ""},
		{"missing-manifest-key", " AND m.artifact_manifest_id=k.artifact_manifest_id", ""},
		{"foreign-manifest-key", "m.artifact_manifest_id=k.artifact_manifest_id", "m.artifact_manifest_id=k.attempt_id"},
		{"foreign-tenant-key", "m.tenant_id=k.tenant_id", "m.tenant_id=k.attempt_id"},
		{"missing-admission", "a.attempt_id=p.attempt_id", "a.attempt_id=p.run_id"},
		{"left-fallback", "INNER JOIN artifact_manifests", "LEFT JOIN artifact_manifests"},
		{"first-row-hides-fanout", "m.artifact_manifest_id=k.artifact_manifest_id", "m.artifact_manifest_id=k.artifact_manifest_id LIMIT 1"},
		{"unbounded-manifest", "SUBSTRING(CAST(m.payload AS String),0,32769)", "m.payload"},
		{"lost-marker-overflow", "content_digest AS String),0,74)", "content_digest AS String),0,73)"},
		{"second-query", "SELECT\n", "SELECT 1; SELECT\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := strings.Replace(localTerminalHistorySQL, tc.old, tc.replacement, 1)
			if changed == localTerminalHistorySQL {
				t.Fatal("ineffective history-query mutation")
			}
			if err := validateLocalTerminalHistoryJoinShape(changed); err == nil {
				t.Fatal("weakened history-query shape accepted")
			}
		})
	}
}
