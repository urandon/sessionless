package ports_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func operationPortFixture(t *testing.T) ports.ReserveAttemptOperationResultV1 {
	t.Helper()
	hash := strings.Repeat("a", 64)
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	op := domain.AttemptOperationV1{Version: 1, Scope: domain.AttemptOperationScopeV1{TenantID: "tenant-ports", SessionID: "session-ports", OwnerUserID: "owner-ports", RunID: "run-ports", AttemptID: "attempt-ports", LeaseID: "lease-ports", FenceToken: 1, PhysicalInvocationClaimID: "claim-ports", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(hash), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(hash), HarnessBindingDigest: domain.HarnessBindingDigestV2(hash), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(hash)}, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-ports", RequestRef: "request-ports", RequestDigest: hash, Deadline: at.Add(10 * time.Second), CleanupDuration: 30 * time.Second, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: 1000, ResponseBytes: 1000, AmountMicrounits: 1, ActiveDuration: 10 * time.Second}, RequestBytes: 100, ResultBytes: 900, ContextBytes: 100, Currency: "USD", PriceRevision: "price-ports", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV1(hash)}
	digest, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: digest, ReservedAt: at, AggregateDebit: op.Debit}
	c := ports.AttemptOperationCapabilityV1{Version: 1, Reservation: r, IssuedAt: at, ExpiresAt: op.Deadline, Authenticator: []byte(strings.Repeat("c", 32))}
	g := ports.AttemptOperationObservationGrantV1{Version: 1, Reservation: r, IssuedAt: at, ExpiresAt: at.Add(30 * time.Second), Authenticator: []byte(strings.Repeat("g", 32))}
	return ports.ReserveAttemptOperationResultV1{Status: ports.AttemptOperationReservedV1, Reservation: r, Capability: &c, ObservationGrant: g}
}

func TestAttemptOperationPortRecoveryCannotContainSendAuthority(t *testing.T) {
	t.Parallel()
	fresh := operationPortFixture(t)
	if err := fresh.Validate(); err != nil {
		t.Fatalf("fresh structural result: %v", err)
	}
	recovered := fresh
	recovered.Status = ports.AttemptOperationReconcileOnlyV1
	if err := recovered.Validate(); err == nil {
		t.Fatal("recovered reservation reminted send authority")
	}
	recovered.Capability = nil
	if err := recovered.Validate(); err != nil {
		t.Fatalf("observation-only recovery: %v", err)
	}
	for name, mutate := range map[string]func(*ports.ReserveAttemptOperationResultV1){"unknown_status": func(v *ports.ReserveAttemptOperationResultV1) { v.Status = "replayed" }, "missing_fresh_capability": func(v *ports.ReserveAttemptOperationResultV1) { v.Capability = nil }, "missing_observation": func(v *ports.ReserveAttemptOperationResultV1) {
		v.ObservationGrant = ports.AttemptOperationObservationGrantV1{}
	}, "foreign_claim": func(v *ports.ReserveAttemptOperationResultV1) {
		v.ObservationGrant.Reservation.Operation.Scope.PhysicalInvocationClaimID = "foreign-claim"
	}, "wrong_mac_length": func(v *ports.ReserveAttemptOperationResultV1) { v.ObservationGrant.Authenticator = []byte("x") }} {
		t.Run(name, func(t *testing.T) {
			v := operationPortFixture(t)
			mutate(&v)
			if err := v.Validate(); err == nil {
				t.Fatalf("invalid result %s accepted", name)
			}
		})
	}
	request := ports.RecoverAttemptOperationRequestV1{Operation: fresh.Reservation.Operation}
	if err := request.Validate(); err != nil {
		t.Fatalf("lost-response request: %v", err)
	}
	request.ExpectedReservationDigest = domain.AttemptOperationReservationDigestV1("not-sha256")
	if err := request.Validate(); err == nil {
		t.Fatal("invalid exact recovery digest accepted")
	}
}

func TestAttemptOperationProcessLocalProofsRejectJSONAndCloneMutation(t *testing.T) {
	t.Parallel()
	result := operationPortFixture(t)
	reservationDigest, err := result.Reservation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: result.Reservation.OperationDigest, ReservationDigest: reservationDigest, Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonWorkerLostV1, ObservedAt: result.Reservation.ReservedAt.Add(time.Second), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	a := ports.AuthenticatedAttemptOperationObservationV1{Version: 1, Reservation: result.Reservation, Observation: o, Authenticator: []byte(strings.Repeat("a", 32))}
	if err := a.Validate(); err != nil {
		t.Fatalf("authentic unknown shaped proof: %v", err)
	}
	for name, value := range map[string]any{"capability": result.Capability, "observation_grant": result.ObservationGrant, "observation": a} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(value); !errors.Is(err, ports.ErrAttemptOperationCapabilityNotSerializable) {
				t.Fatalf("serialized %s permission err=%v", name, err)
			}
		})
	}
	for name, target := range map[string]any{"capability": new(ports.AttemptOperationCapabilityV1), "observation_grant": new(ports.AttemptOperationObservationGrantV1), "observation": new(ports.AuthenticatedAttemptOperationObservationV1)} {
		t.Run("decode_"+name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(`{}`), target); !errors.Is(err, ports.ErrAttemptOperationCapabilityNotSerializable) {
				t.Fatalf("decoded %s permission err=%v", name, err)
			}
		})
	}
	cloned := result.Capability.Clone()
	cloned.Authenticator[0] = 'x'
	if result.Capability.Authenticator[0] == 'x' {
		t.Fatal("capability clone shares MAC")
	}
	grantClone := result.ObservationGrant.Clone()
	grantClone.Authenticator[0] = 'x'
	if result.ObservationGrant.Authenticator[0] == 'x' {
		t.Fatal("observation grant clone shares MAC")
	}
	proofClone := a.Clone()
	proofClone.Authenticator[0] = 'x'
	if a.Authenticator[0] == 'x' {
		t.Fatal("authenticated observation clone shares MAC")
	}
	record := ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: result.ObservationGrant, Observation: a}
	if err := record.Validate(); err != nil {
		t.Fatalf("record exact proof: %v", err)
	}
	record.Observation.Reservation.Operation.Scope.FenceToken++
	if err := record.Validate(); err == nil {
		t.Fatal("foreign-fence observation record accepted")
	}
	// A structurally shaped 32-byte MAC is deliberately not verified here. A
	// production issuer must reject fabricated authenticators before store/use.
}

func TestAttemptOperationObservationWindowIndependentOfExecution(t *testing.T) {
	t.Parallel()
	r := operationPortFixture(t)
	g := r.ObservationGrant.Clone()
	g.IssuedAt = r.Reservation.Operation.Deadline.Add(time.Second)
	g.ExpiresAt = r.Reservation.Operation.Deadline.Add(30 * time.Second)
	if err := g.Validate(); err != nil {
		t.Fatalf("bounded historical reconciliation denied: %v", err)
	}
	g.ExpiresAt = g.ExpiresAt.Add(time.Nanosecond)
	if err := g.Validate(); err == nil {
		t.Fatal("unbounded reconciliation window accepted")
	}
	g = r.ObservationGrant.Clone()
	g.IssuedAt = r.Reservation.Operation.Deadline.Add(time.Hour)
	g.ExpiresAt = g.IssuedAt.Add(30 * time.Second)
	if err := g.Validate(); err == nil {
		t.Fatal("late reconciliation grant restarted the original cleanup window")
	}
	c := r.Capability.Clone()
	c.ExpiresAt = c.ExpiresAt.Add(time.Nanosecond)
	if err := c.Validate(); err == nil {
		t.Fatal("send capability extended operation deadline")
	}
	g = r.ObservationGrant.Clone()
	g.Reservation.Operation.CleanupDuration = 5 * time.Second
	g.Reservation.OperationDigest, _ = g.Reservation.Operation.Digest()
	if err := g.Validate(); err == nil {
		t.Fatal("30-second reconciliation exceeded admitted 5-second cleanup window")
	}
	g.ExpiresAt = g.IssuedAt.Add(5 * time.Second)
	if err := g.Validate(); err != nil {
		t.Fatalf("exact smaller cleanup window: %v", err)
	}
}

func TestAttemptOperationReserveRequiresTypedReceipt(t *testing.T) {
	t.Parallel()
	base := operationPortFixture(t).Reservation.Operation
	if err := (ports.ReserveAttemptOperationRequestV1{Operation: base}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ports.ReserveAttemptOperationRequestV1{Operation: base, ConsentReceipt: &domain.WebSearchConsentReceiptBindingV1{}}).Validate(); err == nil {
		t.Fatal("model operation accepted search receipt")
	}
	base.Kind = domain.AttemptOperationWebSearchV1
	base.Sequence = 2
	base.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("b", 64))
	base.Debit.ModelCalls = 0
	base.Debit.InputTokens = 0
	base.Debit.OutputTokens = 0
	base.Debit.ToolDispatches = 1
	base.Debit.WebSearches = 1
	base.ContextBytes = 0
	base.ModelResponseDigest = strings.Repeat("b", 64)
	base.ArgumentsDigest = strings.Repeat("c", 64)
	base.QueryDigest = strings.Repeat("d", 64)
	base.ToolGrantDigest = strings.Repeat("e", 64)
	base.ExternalChargeEvidenceDigest = strings.Repeat("f", 64)
	base.ConsentRequestDigest = strings.Repeat("a", 64)
	base.ConsentDecisionDigest = strings.Repeat("a", 64)
	base.ConsentDecisionRef = "decision-ports"
	if err := base.Validate(); err != nil {
		t.Fatalf("structural search request: %v", err)
	}
	if err := (ports.ReserveAttemptOperationRequestV1{Operation: base}).Validate(); err == nil {
		t.Fatal("search reserved without typed receipt")
	}
	s := base.Scope
	consent := domain.WebSearchConsentRequestV1{Version: 1, ID: "consent-ports", Scope: domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID, EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)}, InitiatingUserID: "initiator-ports", ModelResponseDigest: base.ModelResponseDigest, CallID: base.CallID, ArgumentsDigest: base.ArgumentsDigest, QueryDigest: base.QueryDigest, GrantDigest: base.ToolGrantDigest, ConsentRevision: 1, RevocationGeneration: 1, Arguments: domain.BlobRef{TenantID: s.TenantID, Key: domain.SessionRunObjectPrefix(s.TenantID, s.SessionID, s.RunID) + "attempts/" + string(s.AttemptID) + "/arguments.json", Size: 100, SHA256: base.ArgumentsDigest}, CreatedAt: base.Deadline.Add(-10 * time.Second), ExpiresAt: base.Deadline}
	requestDigest, err := consent.Digest()
	if err != nil {
		t.Fatalf("consent receipt fixture: %v", err)
	}
	base.ConsentRequestDigest = string(requestDigest)
	receipt := domain.WebSearchConsentReceiptBindingV1{Request: consent, RequestDigest: requestDigest, DecisionDigest: domain.WebSearchConsentDecisionDigestV1(base.ConsentDecisionDigest)}
	request := ports.ReserveAttemptOperationRequestV1{Operation: base, ConsentReceipt: &receipt}
	if err := request.Validate(); err != nil {
		t.Fatalf("exact typed receipt request: %v", err)
	}
	for name, change := range map[string]func(*domain.AttemptOperationV1){"foreign_session": func(v *domain.AttemptOperationV1) { v.Scope.SessionID = "other-session" }, "claim": func(v *domain.AttemptOperationV1) { v.Scope.PhysicalInvocationClaimID = "another-claim" }, "query": func(v *domain.AttemptOperationV1) { v.QueryDigest = strings.Repeat("9", 64) }, "arguments": func(v *domain.AttemptOperationV1) { v.ArgumentsDigest = strings.Repeat("9", 64) }, "model_response": func(v *domain.AttemptOperationV1) { v.ModelResponseDigest = strings.Repeat("9", 64) }, "call": func(v *domain.AttemptOperationV1) { v.CallID = "another-call" }, "grant": func(v *domain.AttemptOperationV1) { v.ToolGrantDigest = strings.Repeat("9", 64) }, "decision": func(v *domain.AttemptOperationV1) { v.ConsentDecisionDigest = strings.Repeat("9", 64) }, "expiry": func(v *domain.AttemptOperationV1) { v.Deadline = v.Deadline.Add(time.Nanosecond) }} {
		t.Run(name, func(t *testing.T) {
			v := request
			v.Operation = request.Operation.Clone()
			change(&v.Operation)
			if err := v.Validate(); err == nil {
				t.Fatalf("receipt mismatched %s accepted", name)
			}
		})
	}
	// Matching a typed binding is not an assertion that its decision was
	// approved: the resource transaction must reread and validate stored state.
}
