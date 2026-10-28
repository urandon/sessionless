package ydbstore_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Failure-only scripted SQL DATA, not a replay, new observation, issuer MAC
// verification or permission to continue. Callers log the original failure
// first because existing fixture accessors can themselves fail the test.
func localTerminalFailureDiagnostic(t *testing.T, result providerloopsession.ModelStepResultV1, seed *ydbstore.LocalSessionConnectedSeed) string {
	t.Helper()
	if result.Compaction == nil {
		return formatLocalTerminalFailureDiagnostic(nil, false, nil, nil)
	}
	returned := result.Compaction.Observation
	run := seed.WorkerPreflightState(t).Run
	return formatLocalTerminalFailureDiagnostic(&returned.Observation, returned.Validate() == nil, seed.PersistedCompactionObservations(t), run.DerivedContext)
}

// Only closed labels, booleans and capped counts leave this projection. In
// particular, an older persisted compaction cannot be identified as the failed
// step without an exact valid returned-observation correspondence. Even that
// correspondence does not prove publication or a successful connection Close.
func formatLocalTerminalFailureDiagnostic(returned *domain.AttemptOperationObservationV1, envelopeValid bool, persisted []domain.AttemptOperationObservationV1, derived *domain.RunDerivedContextV1) string {
	const capCount = 12
	bounded := func(n int) int { return min(max(n, 0), capCount) }
	phase, outcome := "no_compaction", "absent"
	match, completed, unknown, invalid := false, 0, 0, 0
	if returned != nil {
		phase, outcome = "compaction_returned", "invalid"
		if returned.Validate() == nil {
			switch returned.Outcome {
			case domain.AttemptOperationCompletedV1:
				outcome = "completed"
			case domain.AttemptOperationAcceptedOutcomeUnknownV1:
				outcome = "accepted_unknown"
			case domain.AttemptOperationRejectedBeforeSendV1:
				outcome = "rejected"
			}
		}
	} else {
		envelopeValid = false
	}
	for _, row := range persisted {
		if row.Validate() != nil {
			invalid = bounded(invalid + 1)
			continue
		}
		switch row.Outcome {
		case domain.AttemptOperationCompletedV1:
			completed = bounded(completed + 1)
		case domain.AttemptOperationAcceptedOutcomeUnknownV1:
			unknown = bounded(unknown + 1)
		}
		if returned != nil && envelopeValid {
			rd, re := returned.Digest()
			pd, pe := row.Digest()
			match = match || (re == nil && pe == nil && rd == pd)
		}
	}
	if returned != nil && returned.Validate() != nil {
		envelopeValid = false
	}
	inventory, head := 0, false
	if derived != nil {
		inventory, head = bounded(len(derived.Inventory)), derived.Head != nil
	}
	return fmt.Sprintf("phase=%s returned_outcome=%s envelope_shape_valid=%t persisted_capped=%d completed_capped=%d unknown_capped=%d invalid_capped=%d returned_row_match=%t derived_inventory_capped=%d derived_head=%t", phase, outcome, envelopeValid, bounded(len(persisted)), completed, unknown, invalid, match, inventory, head)
}

func TestLocalTerminalFailureDiagnostic(t *testing.T) {
	secret := "DO_NOT_PRINT_body_auth_ref_id_url"
	completed := domain.AttemptOperationObservationV1{Version: 1,
		OperationDigest: domain.AttemptOperationDigestV1(strings.Repeat("a", 64)), ReservationDigest: domain.AttemptOperationReservationDigestV1(strings.Repeat("b", 64)),
		Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1,
		ResponseRef: secret, ResponseDigest: strings.Repeat("c", 64), ResponseBytes: 1,
		ObservedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	if err := completed.Validate(); err != nil {
		t.Fatal("diagnostic DATA fixture", err)
	}
	unknown := completed.Clone()
	unknown.Outcome, unknown.Reason = domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
	unknown.ResponseRef, unknown.ResponseDigest, unknown.ResponseBytes = "", "", 0
	invalid := completed.Clone()
	invalid.Outcome = domain.AttemptOperationOutcomeV1(secret)
	cleared := domain.AttemptOperationObservationV1{}
	other := completed.Clone()
	other.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("d", 64))
	for _, tc := range []struct {
		name     string
		returned *domain.AttemptOperationObservationV1
		shape    bool
		rows     []domain.AttemptOperationObservationV1
		want     []string
	}{
		{name: "absent", shape: true, want: []string{"phase=no_compaction", "returned_outcome=absent", "envelope_shape_valid=false", "returned_row_match=false"}},
		{name: "cleared_after_persist", returned: &cleared, rows: []domain.AttemptOperationObservationV1{completed}, want: []string{"returned_outcome=invalid", "completed_capped=1", "returned_row_match=false"}},
		{name: "completed", returned: &completed, shape: true, rows: []domain.AttemptOperationObservationV1{completed}, want: []string{"returned_outcome=completed", "envelope_shape_valid=true", "returned_row_match=true"}},
		{name: "invalid_envelope", returned: &completed, rows: []domain.AttemptOperationObservationV1{completed}, want: []string{"envelope_shape_valid=false", "returned_row_match=false"}},
		{name: "older_row", returned: &completed, shape: true, rows: []domain.AttemptOperationObservationV1{other}, want: []string{"completed_capped=1", "returned_row_match=false"}},
		{name: "unknown", returned: &unknown, shape: true, rows: []domain.AttemptOperationObservationV1{unknown}, want: []string{"returned_outcome=accepted_unknown", "unknown_capped=1", "returned_row_match=true"}},
		{name: "hostile", returned: &invalid, shape: true, rows: []domain.AttemptOperationObservationV1{invalid}, want: []string{"returned_outcome=invalid", "envelope_shape_valid=false", "invalid_capped=1", "returned_row_match=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := formatLocalTerminalFailureDiagnostic(tc.returned, tc.shape, tc.rows, nil)
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Fatalf("missing %s in %s", want, got)
				}
			}
			if len(got) > 320 || strings.Contains(got, secret) || strings.Contains(got, strings.Repeat("a", 64)) || strings.Contains(got, strings.Repeat("c", 64)) {
				t.Fatal("diagnostic is not bounded non-content DATA")
			}
			if again := formatLocalTerminalFailureDiagnostic(tc.returned, tc.shape, tc.rows, nil); again != got {
				t.Fatal("nondeterministic diagnostic")
			}
		})
	}
	rows := make([]domain.AttemptOperationObservationV1, 100)
	for i := range rows {
		rows[i] = completed
	}
	derived := &domain.RunDerivedContextV1{Inventory: make([]domain.DerivedContextPlanV1, 100), Head: &domain.DerivedContextHeadV1{PlanDigest: secret}}
	got := formatLocalTerminalFailureDiagnostic(&completed, true, rows, derived)
	for _, want := range []string{"persisted_capped=12", "completed_capped=12", "derived_inventory_capped=12", "derived_head=true"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing cap %s in %s", want, got)
		}
	}
	if len(got) > 320 || strings.Contains(got, secret) {
		t.Fatal("capped diagnostic leaked input")
	}
}
