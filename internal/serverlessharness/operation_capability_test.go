package serverlessharness

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type operationTestClock struct {
	mu sync.Mutex
	at time.Time
}

func (clock *operationTestClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.at
}

func (clock *operationTestClock) set(at time.Time) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.at = at
}

func operationIssuerFixture(t *testing.T, capacity int) (*OperationCapabilityIssuer, *operationTestClock, domain.AttemptOperationReservationV1) {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	clock := &operationTestClock{at: at}
	issuer, err := NewOperationCapabilityIssuer(clock.now, bytes.NewReader(bytes.Repeat([]byte{1}, 32)), capacity)
	if err != nil {
		t.Fatalf("construct issuer capacity=%d: %v", capacity, err)
	}
	hash := strings.Repeat("a", 64)
	op := domain.AttemptOperationV1{
		Version: 1,
		Scope: domain.AttemptOperationScopeV1{
			TenantID: "tenant-issuer", SessionID: "session-issuer", OwnerUserID: "owner-issuer", RunID: "run-issuer", AttemptID: "attempt-issuer", LeaseID: "lease-issuer", FenceToken: 1, PhysicalInvocationClaimID: "claim-issuer",
			EffectReservationDigest: domain.AttemptEffectReservationDigestV1(hash), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(hash), HarnessBindingDigest: domain.HarnessBindingDigestV2(hash), AgentLoopPolicyDigest: domain.AgentLoopPolicyDigestV1(hash),
		},
		Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-issuer", RequestRef: "request-issuer", RequestDigest: hash,
		Deadline: at.Add(10 * time.Second), CleanupDuration: 5 * time.Second,
		Debit:        domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: 1000, ResponseBytes: 1000, AmountMicrounits: 1, ActiveDuration: 10 * time.Second},
		RequestBytes: 100, ResultBytes: 900, ContextBytes: 100, Currency: "USD", PriceRevision: "price-issuer", AdmissionCostCeilingDigest: domain.AdmissionCostCeilingDigestV2(hash),
	}
	digest, err := op.Digest()
	if err != nil {
		t.Fatalf("operation fixture: %v", err)
	}
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: digest, ReservedAt: at, AggregateDebit: op.Debit}
	r.RegisteredAllocationDigest = domain.RegisteredAllocationDigestV1(strings.Repeat("1", 64))
	r.RegisteredProxyDigest = domain.RegisteredProxyAttestationDigestV2(strings.Repeat("2", 64))
	r.ComponentAmounts[0] = op.Debit.AmountMicrounits
	if err := r.Validate(); err != nil {
		t.Fatalf("reservation fixture: %v", err)
	}
	return issuer, clock, r
}

func mintOperationTestCapability(t *testing.T, issuer *OperationCapabilityIssuer, r domain.AttemptOperationReservationV1) ports.AttemptOperationCapabilityV1 {
	t.Helper()
	v, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt, r.Operation.Deadline)
	if err != nil {
		t.Fatalf("mint send for %s: %v", r.Operation.CallID, err)
	}
	return v
}

func mintOperationTestObservationGrant(t *testing.T, issuer *OperationCapabilityIssuer, r domain.AttemptOperationReservationV1, at time.Time) ports.AttemptOperationObservationGrantV1 {
	t.Helper()
	v, err := issuer.MintAttemptOperationObservationGrant(r, at, at.Add(r.Operation.CleanupDuration))
	if err != nil {
		t.Fatalf("mint observation grant for %s at %s: %v", r.Operation.CallID, at, err)
	}
	return v
}

func operationTestObservation(t *testing.T, r domain.AttemptOperationReservationV1, at time.Time) domain.AttemptOperationObservationV1 {
	t.Helper()
	digest, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Known spend survives an unknown accepted outcome; missing metrics are not
	// invented as zero, and even an overrun is preserved as authenticated evidence.
	input := r.Operation.Debit.InputTokens + 1
	return domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.OperationDigest, ReservationDigest: digest, Outcome: domain.AttemptOperationAcceptedOutcomeUnknownV1, Reason: domain.AttemptOperationReasonTransportUnknownV1, ObservedAt: at, Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &input}}
}

func TestOperationCapabilityIssuerRejectsInvalidConstructionAndZeroValues(t *testing.T) {
	t.Parallel()
	clock := func() time.Time { return time.Unix(1, 0).UTC() }
	for _, test := range []struct {
		name     string
		clock    Clock
		entropy  io.Reader
		capacity int
	}{
		{name: "nil_clock", entropy: bytes.NewReader(bytes.Repeat([]byte{1}, 32)), capacity: 1},
		{name: "zero_key", clock: clock, entropy: bytes.NewReader(make([]byte, 32)), capacity: 1},
		{name: "short_key", clock: clock, entropy: bytes.NewReader([]byte{1}), capacity: 1},
		{name: "zero_capacity", clock: clock, capacity: 0},
		{name: "negative_capacity", clock: clock, capacity: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewOperationCapabilityIssuer(test.clock, test.entropy, test.capacity); !errors.Is(err, ErrInvalidAttemptOperationCapability) {
				t.Fatalf("constructor %s error=%v, want fail closed", test.name, err)
			}
		})
	}
	for _, issuer := range []*OperationCapabilityIssuer{nil, {}} {
		if _, err := issuer.MintAttemptOperationCapability(domain.AttemptOperationReservationV1{}, time.Time{}, time.Time{}); err == nil {
			t.Error("uninitialized issuer minted send")
		}
		if err := issuer.ConsumeAttemptOperationCapability(ports.AttemptOperationCapabilityV1{}); err == nil {
			t.Error("uninitialized issuer consumed send")
		}
		if _, err := issuer.MintAttemptOperationObservationGrant(domain.AttemptOperationReservationV1{}, time.Time{}, time.Time{}); err == nil {
			t.Error("uninitialized issuer minted observation grant")
		}
		if err := issuer.VerifyAttemptOperationObservationGrant(ports.AttemptOperationObservationGrantV1{}); err == nil {
			t.Error("uninitialized issuer verified grant")
		}
		if _, err := issuer.AuthenticateAttemptOperationObservation(ports.AttemptOperationObservationGrantV1{}, domain.AttemptOperationObservationV1{}); err == nil {
			t.Error("uninitialized issuer authenticated observation")
		}
		if err := issuer.VerifyAttemptOperationObservation(ports.AuthenticatedAttemptOperationObservationV1{}); err == nil {
			t.Error("uninitialized issuer verified observation")
		}
	}
}

func TestOperationCapabilityConcurrentMintAndConsumeExactlyOnce(t *testing.T) {
	t.Parallel()
	issuer, _, r := operationIssuerFixture(t, 1)
	const contenders = 16
	start := make(chan struct{})
	results := make(chan ports.AttemptOperationCapabilityV1, contenders)
	var wait sync.WaitGroup
	for range contenders {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			v, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt, r.Operation.Deadline)
			if err == nil {
				results <- v
			}
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	if len(results) != 1 {
		t.Fatalf("concurrent successful mints=%d, want 1", len(results))
	}
	v := <-results
	consumeStart := make(chan struct{})
	consumed := make(chan error, 2)
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-consumeStart
			consumed <- issuer.ConsumeAttemptOperationCapability(v.Clone())
		}()
	}
	close(consumeStart)
	wait.Wait()
	close(consumed)
	successes := 0
	for err := range consumed {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent successful consumes=%d, want 1", successes)
	}
	if _, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt.Add(time.Nanosecond), r.Operation.Deadline); err == nil {
		t.Fatal("consumed reservation reminted with changed timestamp")
	}
	if err := issuer.ConsumeAttemptOperationCapability(v); err == nil {
		t.Fatal("consumed capability reused")
	}
}

func TestOperationCapabilityMACBindsFullReservationAndWindow(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*ports.AttemptOperationCapabilityV1){
		"version":             func(v *ports.AttemptOperationCapabilityV1) { v.Version++ },
		"authenticator":       func(v *ports.AttemptOperationCapabilityV1) { v.Authenticator[0] ^= 1 },
		"short_authenticator": func(v *ports.AttemptOperationCapabilityV1) { v.Authenticator = v.Authenticator[:31] },
		"issued_at":           func(v *ports.AttemptOperationCapabilityV1) { v.IssuedAt = v.IssuedAt.Add(time.Nanosecond) },
		"expires_at":          func(v *ports.AttemptOperationCapabilityV1) { v.ExpiresAt = v.ExpiresAt.Add(-time.Nanosecond) },
		"reserved_at": func(v *ports.AttemptOperationCapabilityV1) {
			v.Reservation.ReservedAt = v.Reservation.ReservedAt.Add(-time.Nanosecond)
		},
		"aggregate_debit": func(v *ports.AttemptOperationCapabilityV1) { v.Reservation.AggregateDebit.AmountMicrounits++ },
		"scope":           func(v *ports.AttemptOperationCapabilityV1) { v.Reservation.Operation.Scope.OwnerUserID = "other-owner" },
		"request": func(v *ports.AttemptOperationCapabilityV1) {
			v.Reservation.Operation.RequestDigest = strings.Repeat("b", 64)
		},
		"debit":   func(v *ports.AttemptOperationCapabilityV1) { v.Reservation.Operation.Debit.AmountMicrounits++ },
		"cleanup": func(v *ports.AttemptOperationCapabilityV1) { v.Reservation.Operation.CleanupDuration += time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			issuer, clock, r := operationIssuerFixture(t, 1)
			original := mintOperationTestCapability(t, issuer, r)
			v := original.Clone()
			mutate(&v)
			// Recompute untrusted public digests to ensure they cannot bypass MAC.
			v.Reservation.OperationDigest, _ = v.Reservation.Operation.Digest()
			clock.set(r.ReservedAt.Add(time.Second))
			if err := issuer.ConsumeAttemptOperationCapability(v); err == nil {
				t.Fatalf("mutated %s consumed send", name)
			}
			if err := issuer.ConsumeAttemptOperationCapability(original); err != nil {
				t.Fatalf("mutation burned original permission: %v", err)
			}
		})
	}
	issuer, clock, r := operationIssuerFixture(t, 1)
	v := mintOperationTestCapability(t, issuer, r)
	other, err := NewOperationCapabilityIssuer(clock.now, bytes.NewReader(bytes.Repeat([]byte{2}, 32)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.ConsumeAttemptOperationCapability(v); err == nil {
		t.Fatal("foreign issuer consumed send")
	}
	g := mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)
	fabricated := v.Clone()
	fabricated.Authenticator = append([]byte(nil), g.Authenticator...)
	if err := issuer.ConsumeAttemptOperationCapability(fabricated); err == nil {
		t.Fatal("observation grant authenticated a send")
	}
	if err := issuer.ConsumeAttemptOperationCapability(ports.AttemptOperationCapabilityV1{}); err == nil {
		t.Fatal("malformed send consumed")
	}
}

func TestOperationCapabilityBoundedCapacityPruningAndClockRewind(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		name := "outstanding"
		if consumed {
			name = "consumed"
		}
		t.Run(name, func(t *testing.T) {
			issuer, clock, r := operationIssuerFixture(t, 1)
			v := mintOperationTestCapability(t, issuer, r)
			if consumed {
				if err := issuer.ConsumeAttemptOperationCapability(v); err != nil {
					t.Fatal(err)
				}
			}
			fresh := r.Clone()
			fresh.Operation.CallID = "another-model"
			fresh.OperationDigest, _ = fresh.Operation.Digest()
			if _, err := issuer.MintAttemptOperationCapability(fresh, fresh.ReservedAt, fresh.Operation.Deadline); err == nil {
				t.Fatal("capacity evicted live/tombstone entry")
			}
			clock.set(r.Operation.Deadline)
			if err := issuer.ConsumeAttemptOperationCapability(v); err == nil {
				t.Fatal("send accepted at exclusive deadline")
			}
			clock.set(r.Operation.Deadline.Add(r.Operation.CleanupDuration).Add(-time.Nanosecond))
			fresh.ReservedAt = clock.now()
			fresh.Operation.Deadline = fresh.ReservedAt.Add(10 * time.Second)
			fresh.OperationDigest, _ = fresh.Operation.Digest()
			if _, err := issuer.MintAttemptOperationCapability(fresh, fresh.ReservedAt, fresh.Operation.Deadline); err == nil {
				t.Fatal("capacity reclaimed before original absolute cleanup end")
			}
			clock.set(r.Operation.Deadline.Add(r.Operation.CleanupDuration))
			fresh.ReservedAt = clock.now()
			fresh.Operation.Deadline = fresh.ReservedAt.Add(10 * time.Second)
			fresh.OperationDigest, _ = fresh.Operation.Digest()
			if _, err := issuer.MintAttemptOperationCapability(fresh, fresh.ReservedAt, fresh.Operation.Deadline); err != nil {
				t.Fatalf("safely expired capacity not reclaimed: %v", err)
			}
			if len(issuer.entries) != 1 {
				t.Fatalf("state size=%d, want capacity 1", len(issuer.entries))
			}
			clock.set(r.ReservedAt)
			if _, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt, r.Operation.Deadline); err == nil {
				t.Fatal("clock rewind reminted pruned original reservation")
			}
			if len(issuer.entries) != 1 {
				t.Fatal("clock rewind pruned live state")
			}
			clock.set(fresh.ReservedAt)
			if _, err := issuer.MintAttemptOperationCapability(r, r.ReservedAt, r.Operation.Deadline); err == nil {
				t.Fatal("expired reservation reminted after clock catches up")
			}
		})
	}
}

func TestOperationObservationIndependentAbsoluteCleanupWindow(t *testing.T) {
	t.Parallel()
	issuer, clock, r := operationIssuerFixture(t, 1)
	v := mintOperationTestCapability(t, issuer, r)
	clock.set(r.Operation.Deadline.Add(time.Second))
	at := clock.now()
	end := r.Operation.Deadline.Add(r.Operation.CleanupDuration)
	g, err := issuer.MintAttemptOperationObservationGrant(r, at, end)
	if err != nil {
		t.Fatalf("late bounded observation grant: %v", err)
	}
	if err := issuer.VerifyAttemptOperationObservationGrant(g); err != nil {
		t.Fatalf("late grant verification: %v", err)
	}
	if err := issuer.ConsumeAttemptOperationCapability(v); err == nil {
		t.Fatal("late observation window enabled send")
	}
	if _, err := issuer.MintAttemptOperationObservationGrant(r, at, at.Add(r.Operation.CleanupDuration)); err == nil {
		t.Fatal("reissue extended original absolute cleanup bound")
	}
	o := operationTestObservation(t, r, at)
	proof, err := issuer.AuthenticateAttemptOperationObservation(g, o)
	if err != nil {
		t.Fatalf("late unknown outcome authentication: %v", err)
	}
	clock.set(end)
	if err := issuer.VerifyAttemptOperationObservationGrant(g); err == nil {
		t.Fatal("observation grant accepted at exclusive cleanup end")
	}
	if _, err := issuer.AuthenticateAttemptOperationObservation(g, o); err == nil {
		t.Fatal("expired observation grant authenticated new evidence")
	}
	if _, err := issuer.MintAttemptOperationObservationGrant(r, end, end.Add(time.Second)); err == nil {
		t.Fatal("mint after absolute cleanup end granted authority")
	}
	if err := issuer.VerifyAttemptOperationObservation(proof); err != nil {
		t.Fatalf("historical immutable proof lost at grant expiry: %v", err)
	}
	clock.set(at)
	if err := issuer.VerifyAttemptOperationObservation(proof); err == nil {
		t.Fatal("clock rewind accepted historical verification")
	}
}

func TestOperationObservationAuthenticatesUnknownPartialUsageAndClones(t *testing.T) {
	t.Parallel()
	issuer, clock, r := operationIssuerFixture(t, 1)
	g := mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)
	o := operationTestObservation(t, r, r.ReservedAt)
	proof, err := issuer.AuthenticateAttemptOperationObservation(g, o)
	if err != nil {
		t.Fatal(err)
	}
	*o.Usage.InputTokens = 0
	g.Authenticator[0] ^= 1
	g.Reservation.AggregateDebit.AmountMicrounits++
	if err := issuer.VerifyAttemptOperationObservation(proof); err != nil {
		t.Fatalf("input mutations changed proof: %v", err)
	}
	if got := *proof.Observation.Usage.InputTokens; got != r.Operation.Debit.InputTokens+1 {
		t.Fatalf("partial measured overrun=%d, want %d", got, r.Operation.Debit.InputTokens+1)
	}
	if proof.Observation.Usage.OutputTokens != nil || proof.Observation.Usage.AmountMicrounits != nil {
		t.Fatal("absent measured metrics invented")
	}
	for name, mutate := range map[string]func(*ports.AuthenticatedAttemptOperationObservationV1){
		"version": func(v *ports.AuthenticatedAttemptOperationObservationV1) { v.Version++ },
		"mac":     func(v *ports.AuthenticatedAttemptOperationObservationV1) { v.Authenticator[0] ^= 1 },
		"reservation": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			v.Reservation.AggregateDebit.AmountMicrounits++
		},
		"outcome": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			v.Observation.Outcome = domain.AttemptOperationRejectedBeforeSendV1
		},
		"reason": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			v.Observation.Reason = domain.AttemptOperationReasonWorkerLostV1
		},
		"time": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			v.Observation.ObservedAt = v.Observation.ObservedAt.Add(time.Nanosecond)
		},
		"measured_usage": func(v *ports.AuthenticatedAttemptOperationObservationV1) { *v.Observation.Usage.InputTokens++ },
		"missing_usage": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			v.Observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
		},
		"added_usage": func(v *ports.AuthenticatedAttemptOperationObservationV1) {
			zero := uint64(0)
			v.Observation.Usage.AmountMicrounits = &zero
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proof.Clone()
			mutate(&changed)
			if err := issuer.VerifyAttemptOperationObservation(changed); err == nil {
				t.Fatalf("mutated observation %s verified", name)
			}
		})
	}
	other, err := NewOperationCapabilityIssuer(clock.now, bytes.NewReader(bytes.Repeat([]byte{2}, 32)), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.VerifyAttemptOperationObservation(proof); err == nil {
		t.Fatal("foreign issuer verified observation")
	}
	if err := other.VerifyAttemptOperationObservationGrant(mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)); err == nil {
		t.Fatal("foreign issuer verified observation grant")
	}
	validGrant := mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)
	future := operationTestObservation(t, r, r.ReservedAt.Add(time.Second))
	if _, err := issuer.AuthenticateAttemptOperationObservation(validGrant, future); err == nil {
		t.Fatal("future observation authenticated")
	}
	if _, err := issuer.AuthenticateAttemptOperationObservation(g, proof.Observation); err == nil {
		t.Fatal("mutated grant authenticated observation")
	}
	if _, err := issuer.AuthenticateAttemptOperationObservation(ports.AttemptOperationObservationGrantV1{}, proof.Observation); err == nil {
		t.Fatal("malformed grant authenticated observation")
	}
	if err := issuer.VerifyAttemptOperationObservation(ports.AuthenticatedAttemptOperationObservationV1{}); err == nil {
		t.Fatal("malformed proof verified")
	}
}

func TestOperationProofsHaveNoJSONAuthoritySurface(t *testing.T) {
	t.Parallel()
	issuer, _, r := operationIssuerFixture(t, 1)
	v := mintOperationTestCapability(t, issuer, r)
	g := mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)
	o, err := issuer.AuthenticateAttemptOperationObservation(g, operationTestObservation(t, r, r.ReservedAt))
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"send": v, "grant": g, "observation": o} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(value); !errors.Is(err, ports.ErrAttemptOperationCapabilityNotSerializable) {
				t.Fatalf("marshal %s error=%v, want nonserializable", name, err)
			}
		})
	}
	for name, value := range map[string]any{"send": new(ports.AttemptOperationCapabilityV1), "grant": new(ports.AttemptOperationObservationGrantV1), "observation": new(ports.AuthenticatedAttemptOperationObservationV1)} {
		t.Run("decode_"+name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(`{}`), value); !errors.Is(err, ports.ErrAttemptOperationCapabilityNotSerializable) {
				t.Fatalf("unmarshal %s error=%v, want nonserializable", name, err)
			}
		})
	}
}

func TestOperationCapabilityMintRejectsMalformedAndNonLiveWindows(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*domain.AttemptOperationReservationV1, *time.Time, *time.Time, *operationTestClock)
	}{
		{name: "malformed_reservation", change: func(r *domain.AttemptOperationReservationV1, _, _ *time.Time, _ *operationTestClock) { r.Version = 0 }},
		{name: "before_reservation", change: func(r *domain.AttemptOperationReservationV1, issue, _ *time.Time, _ *operationTestClock) {
			*issue = r.ReservedAt.Add(-time.Nanosecond)
		}},
		{name: "future_issue", change: func(_ *domain.AttemptOperationReservationV1, issue, _ *time.Time, _ *operationTestClock) {
			*issue = issue.Add(time.Second)
		}},
		{name: "deadline_extension", change: func(_ *domain.AttemptOperationReservationV1, _, end *time.Time, _ *operationTestClock) {
			*end = end.Add(time.Nanosecond)
		}},
		{name: "expired", change: func(_ *domain.AttemptOperationReservationV1, _, end *time.Time, clock *operationTestClock) {
			clock.set(*end)
		}},
		{name: "zero_clock", change: func(_ *domain.AttemptOperationReservationV1, _, _ *time.Time, clock *operationTestClock) {
			clock.set(time.Time{})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			issuer, clock, r := operationIssuerFixture(t, 1)
			issue, end := r.ReservedAt, r.Operation.Deadline
			test.change(&r, &issue, &end, clock)
			if _, err := issuer.MintAttemptOperationCapability(r, issue, end); err == nil {
				t.Fatalf("mint %s accepted", test.name)
			}
			if len(issuer.entries) != 0 {
				t.Fatalf("failed mint %s occupied capacity", test.name)
			}
		})
	}
}

func TestOperationObservationGrantMACAndCompletedResponseFields(t *testing.T) {
	t.Parallel()
	issuer, clock, r := operationIssuerFixture(t, 1)
	g := mintOperationTestObservationGrant(t, issuer, r, r.ReservedAt)
	clock.set(r.ReservedAt.Add(time.Second))
	for name, mutate := range map[string]func(*ports.AttemptOperationObservationGrantV1){
		"version":     func(v *ports.AttemptOperationObservationGrantV1) { v.Version++ },
		"mac":         func(v *ports.AttemptOperationObservationGrantV1) { v.Authenticator[0] ^= 1 },
		"short_mac":   func(v *ports.AttemptOperationObservationGrantV1) { v.Authenticator = nil },
		"issued_at":   func(v *ports.AttemptOperationObservationGrantV1) { v.IssuedAt = v.IssuedAt.Add(time.Nanosecond) },
		"expires_at":  func(v *ports.AttemptOperationObservationGrantV1) { v.ExpiresAt = v.ExpiresAt.Add(-time.Nanosecond) },
		"reservation": func(v *ports.AttemptOperationObservationGrantV1) { v.Reservation.AggregateDebit.InputTokens++ },
	} {
		t.Run("grant_"+name, func(t *testing.T) {
			changed := g.Clone()
			mutate(&changed)
			if err := issuer.VerifyAttemptOperationObservationGrant(changed); err == nil {
				t.Fatalf("mutated grant %s verified", name)
			}
		})
	}
	o := operationTestObservation(t, r, clock.now())
	o.Outcome, o.Reason = domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
	o.ResponseRef, o.ResponseDigest, o.ResponseBytes = "response-issuer", strings.Repeat("b", 64), 100
	proof, err := issuer.AuthenticateAttemptOperationObservation(g, o)
	if err != nil {
		t.Fatalf("completed response authentication: %v", err)
	}
	for name, mutate := range map[string]func(*domain.AttemptOperationObservationV1){
		"version": func(v *domain.AttemptOperationObservationV1) { v.Version++ },
		"operation_digest": func(v *domain.AttemptOperationObservationV1) {
			v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("c", 64))
		},
		"reservation_digest": func(v *domain.AttemptOperationObservationV1) {
			v.ReservationDigest = domain.AttemptOperationReservationDigestV1(strings.Repeat("c", 64))
		},
		"response_ref":    func(v *domain.AttemptOperationObservationV1) { v.ResponseRef = "other-response" },
		"response_digest": func(v *domain.AttemptOperationObservationV1) { v.ResponseDigest = strings.Repeat("c", 64) },
		"response_bytes":  func(v *domain.AttemptOperationObservationV1) { v.ResponseBytes++ },
		"observed_at":     func(v *domain.AttemptOperationObservationV1) { v.ObservedAt = r.ReservedAt },
	} {
		t.Run("response_"+name, func(t *testing.T) {
			changed := proof.Clone()
			mutate(&changed.Observation)
			if err := issuer.VerifyAttemptOperationObservation(changed); err == nil {
				t.Fatalf("mutated completed response %s verified", name)
			}
		})
	}
	// Send and observation grants share neither their MAC namespace nor authority.
	capability := mintOperationTestCapability(t, issuer, r)
	changed := g.Clone()
	changed.Authenticator = append([]byte(nil), capability.Authenticator...)
	if err := issuer.VerifyAttemptOperationObservationGrant(changed); err == nil {
		t.Fatal("send MAC authenticated observation grant")
	}
	if err := issuer.VerifyAttemptOperationObservation(proof); err != nil {
		t.Fatalf("mutation damaged original completed proof: %v", err)
	}
}
