package ydbstore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Structural pre-disposal controls only. The existing local scripted parent
// and authentic in-process preparation are reused; these do not assert native
// YDB inventory, TLS custody, physical deletion or canonical cleanup success.
func ownedResidualInventoryFixture(t *testing.T) ports.OwnedOperationResidualInventoryV1 {
	t.Helper()
	f := newOwnedAllocationFixture(t)
	registered, err := f.parent.store.publishBoundedOwnedAllocation(t.Context(), f.publication)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.parent.sql.parentRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	a := registered.Original
	retired := a.Clone()
	retired.State, retired.RevocationGeneration, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, a.RevocationGeneration+1, a.UpdatedAt.Add(time.Microsecond)
	hash := strings.Repeat("a", 64)
	proxy, err := domain.BuildRegisteredProxyAttestationV1(a, f.parent.source, domain.RegisteredProxyProviderV1, hash, a.Substrate.EgressPolicyDigest, hash,
		domain.OwnedAgentAuthorityBodyRefV1{Ref: "structural-policy", Digest: hash, Bytes: 1}, domain.OwnedAgentAuthorityBodyRefV1{Ref: "structural-attestation", Digest: hash, Bytes: 1}, a.Window)
	if err != nil {
		t.Fatal(err)
	}
	request, response := []byte(`{"kind":"model","input":"fixture"}`), []byte(`{"version":1,"kind":"model_response","text":"done","calls":[]}`)
	cost := parent.Authority.AdmissionCostCeiling
	cd, _ := cost.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(parent), Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "inventory-model", RequestRef: "inventory-request", RequestDigest: ports.OwnedFixtureBytesDigestV1(request), Deadline: parent.Reservation.ReservedAt.Add(5 * time.Second), CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration, RequestBytes: uint64(len(request)), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(request)) + 512, ResponseBytes: 512, AmountMicrounits: *f.parent.source.Spending[0].MaxAmountMicrounits, ActiveDuration: 5 * time.Second}}
	od, _ := op.Digest()
	ad, _ := a.Digest()
	pd, _ := proxy.Digest()
	aggregate := op.Debit
	aggregate.BodyBytes += 8192
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: parent.Reservation.ReservedAt, AggregateDebit: aggregate, RegisteredAllocationDigest: ad, RegisteredProxyDigest: pd}
	r.ComponentAmounts[0], r.ComponentAmounts[1] = op.Debit.AmountMicrounits, *f.parent.source.Spending[1].MaxAmountMicrounits
	rd, _ := r.Digest()
	one := uint64(1)
	amount := op.Debit.AmountMicrounits
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: od, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "inventory-response", ResponseDigest: ports.OwnedFixtureBytesDigestV1(response), ResponseBytes: uint64(len(response)), ObservedAt: parent.Reservation.ReservedAt.Add(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &one, OutputTokens: &one, AmountMicrounits: &amount}}
	v := ports.OwnedOperationResidualInventoryV1{Version: 1, Parent: parent, Source: f.parent.source, OriginalAllocation: a, RetiredAllocation: retired, AccountingDigest: hash, AggregateDebit: aggregate, ComponentAmounts: r.ComponentAmounts, Operations: []ports.OwnedOperationResidualRowV1{{Reservation: r, Observation: o}}, Proxies: []ports.OwnedOperationResidualProxyV1{{Sequence: 1, Original: proxy, Current: proxy}}, Bodies: []ports.OwnedOperationResidualBodyV1{{Ref: op.RequestRef, Digest: op.RequestDigest, Bytes: uint64(len(request))}, {Ref: o.ResponseRef, Digest: o.ResponseDigest, Bytes: uint64(len(response))}}}
	if _, err := v.IntentBytes(); err != nil {
		t.Fatalf("valid structural inventory fixture: %v; op=%v reservation=%v observation=%v", err, op.ValidateForParent(parent.Authority, parent.Reservation), r.ValidateForParent(parent.Authority, parent.Reservation), o.ValidateForReservation(r))
	}
	return v
}

func TestOwnedOperationResidualInventoryRejectsUnknownBeforeDisposal(t *testing.T) {
	fixture := ownedResidualInventoryFixture(t)
	for _, tc := range []struct {
		name   string
		change func(*ports.OwnedOperationResidualInventoryV1)
	}{
		{"active-allocation", func(v *ports.OwnedOperationResidualInventoryV1) { v.RetiredAllocation = v.OriginalAllocation }},
		{"unknown-effect", func(v *ports.OwnedOperationResidualInventoryV1) {
			v.Operations[0].Observation.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			v.Operations[0].Observation.Reason = domain.AttemptOperationReasonWorkerLostV1
			v.Operations[0].Observation.ResponseRef = ""
			v.Operations[0].Observation.ResponseDigest = ""
			v.Operations[0].Observation.ResponseBytes = 0
			v.Operations[0].Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
		}},
		{"unknown-usage", func(v *ports.OwnedOperationResidualInventoryV1) {
			v.Operations[0].Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
		}},
		{"usage-overrun", func(v *ports.OwnedOperationResidualInventoryV1) {
			n := v.Operations[0].Reservation.Operation.Debit.AmountMicrounits + 1
			v.Operations[0].Observation.Usage.AmountMicrounits = &n
		}},
		{"missing-response-object", func(v *ports.OwnedOperationResidualInventoryV1) { v.Bodies = v.Bodies[:1] }},
		{"missing-request-object", func(v *ports.OwnedOperationResidualInventoryV1) { v.Bodies = v.Bodies[1:] }},
		{"changed-body-digest", func(v *ports.OwnedOperationResidualInventoryV1) { v.Bodies[0].Digest = strings.Repeat("b", 64) }},
		{"missing-transport", func(v *ports.OwnedOperationResidualInventoryV1) { v.Proxies = nil }},
		{"missing-operation", func(v *ports.OwnedOperationResidualInventoryV1) { v.Operations = nil }},
		{"wrong-sequence", func(v *ports.OwnedOperationResidualInventoryV1) { v.Proxies[0].Sequence = 2 }},
		{"changed-accounting", func(v *ports.OwnedOperationResidualInventoryV1) { v.AggregateDebit.BodyBytes++ }},
		{"changed-component", func(v *ports.OwnedOperationResidualInventoryV1) { v.ComponentAmounts[0]++ }},
		{"duplicate-object", func(v *ports.OwnedOperationResidualInventoryV1) { v.Bodies = append(v.Bodies, v.Bodies[0]) }},
		{"too-many-objects", func(v *ports.OwnedOperationResidualInventoryV1) {
			for len(v.Bodies) < 29 {
				v.Bodies = append(v.Bodies, v.Bodies[0])
			}
		}},
		{"foreign-source", func(v *ports.OwnedOperationResidualInventoryV1) { v.Source.Scope.SessionID = "foreign-session" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(fixture)
			var v ports.OwnedOperationResidualInventoryV1
			if json.Unmarshal(raw, &v) != nil {
				t.Fatal("fixture clone")
			}
			tc.change(&v)
			if _, err := v.IntentBytes(); err == nil {
				t.Fatal("unsafe structural inventory permitted original disposal")
			}
		})
	}
	_, pending, _ := ownedConsentCodecFixture(t)
	state := pending.States[0]
	state.Request.Scope = ownedConsentScope(fixture.Parent)
	state.Request.CreatedAt = fixture.Parent.Reservation.ReservedAt
	state.Request.ExpiresAt = state.Request.CreatedAt.Add(time.Second)
	state.UpdatedAt = state.Request.CreatedAt
	if state.Validate() != nil {
		t.Fatal("pending control must remain structurally valid")
	}
	fixture.Consents = []domain.WebSearchConsentStateV1{state}
	fixture.Bodies = append(fixture.Bodies, ports.OwnedOperationResidualBodyV1{Ref: state.Request.Arguments.Key, Digest: state.Request.ArgumentsDigest, Bytes: uint64(state.Request.Arguments.Size)})
	if _, err := fixture.IntentBytes(); err == nil {
		t.Fatal("actual structurally valid pending consent permitted disposal")
	}
}
