package ports_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func searchWirePortRequest(t *testing.T) ports.ReserveAttemptOperationRequestV1 {
	t.Helper()
	_, r := consentPortFixture()
	s := r.Scope
	rd, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	d := strings.Repeat("a", 64)
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID, EffectReservationDigest: domain.AttemptEffectReservationDigestV1(s.EffectReservationDigest), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(s.InvocationAuthorityDigest), HarnessBindingDigest: domain.HarnessBindingDigestV2(s.HarnessBindingDigest), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(s.PolicyDigest)},
		Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: r.CallID, RequestRef: "search-wire", RequestDigest: d, PredecessorObservationDigest: domain.AttemptOperationObservationDigestV1(d), ModelResponseDigest: r.ModelResponseDigest, ArgumentsDigest: r.ArgumentsDigest, QueryDigest: r.QueryDigest, ToolGrantDigest: r.GrantDigest, ConsentRequestDigest: string(rd), ConsentDecisionDigest: d, ConsentDecisionRef: string(r.ID), Deadline: r.CreatedAt.Add(time.Second), CleanupDuration: time.Second,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: 1024, ResponseBytes: 512, AmountMicrounits: 1, ActiveDuration: time.Second}, RequestBytes: 128, ResultBytes: 512, Currency: "USD", PriceRevision: "outer-price", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(d), ExternalChargeEvidenceDigest: d,
		SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(r.Arguments.Size), CompilerArtifactDigest: d}}
	return ports.ReserveAttemptOperationRequestV1{Operation: op, ConsentReceipt: &domain.WebSearchConsentReceiptBindingV1{Request: r, RequestDigest: rd, DecisionDigest: domain.WebSearchConsentDecisionDigestV1(d)}}
}

func TestAttemptOperationSearchWireReceiptArgumentSize(t *testing.T) {
	request := searchWirePortRequest(t)
	if err := request.Validate(); err != nil {
		t.Fatalf("exact receipt/wire association: %v", err)
	}
	request.Operation.SearchWire.ArgumentsBytes++
	if request.Validate() == nil {
		t.Fatal("different physical-wire argument count accepted against authentic consent blob size")
	}
}

func TestAttemptOperationSearchWireCapabilityAndRecoveryCloneIsolation(t *testing.T) {
	for _, kind := range []string{"capability", "observation-grant", "authenticated-observation"} {
		t.Run(kind, func(t *testing.T) {
			result := operationPortFixture(t)
			result.Reservation.Operation.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: 23, CompilerArtifactDigest: strings.Repeat("a", 64)}
			// This tests deep-copy ownership only, not validity of an arm on MODEL.
			var copied domain.AttemptOperationV1
			switch kind {
			case "capability":
				c := *result.Capability
				c.Reservation = result.Reservation
				copied = c.Clone().Reservation.Operation
			case "observation-grant":
				g := result.ObservationGrant
				g.Reservation = result.Reservation
				copied = g.Clone().Reservation.Operation
			default:
				a := ports.AuthenticatedAttemptOperationObservationV1{Reservation: result.Reservation}
				copied = a.Clone().Reservation.Operation
			}
			copied.SearchWire.ArgumentsBytes++
			if result.Reservation.Operation.SearchWire.ArgumentsBytes != 23 {
				t.Fatal("cloned process-local grant aliases original arm")
			}
		})
	}
	// Preserve the original observation-only recovery contract independently of
	// whether the new producer exists. A capability is never a recovered result.
	result := operationPortFixture(t)
	result.Status = ports.AttemptOperationReconcileOnlyV1
	if result.Validate() == nil {
		t.Fatal("recovery accepted a send capability")
	}
	result.Capability = nil
	if err := result.Validate(); err != nil {
		t.Fatalf("legacy observation-only recovery: %v", err)
	}
}
