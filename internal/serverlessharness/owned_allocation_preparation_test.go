package serverlessharness

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
)

func newOwnedAllocationPreparationFixture(t testing.TB) (*ownedPreparationFixture, *BoundedOwnedAllocationPreparationV1) {
	t.Helper()
	f := newOwnedPreparationFixture(t)
	driver, err := NewInProcessExecutionSubstrateV1(func() time.Time { return f.now }, f.issuer, f.grant.Authority.HarnessBinding.Backend)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := NewBoundedOwnedAllocationPreparationV1(f.issuer, f.grant.Authority.HarnessBinding.Backend, f.grant.Authority.SubstrateBinding, driver, f.burn)
	if err != nil {
		t.Fatal(err)
	}
	return f, bridge
}

func TestOwnedAllocationPreparationActualProducerObservationOnly(t *testing.T) {
	f, bridge := newOwnedAllocationPreparationFixture(t)
	p, err := bridge.PrepareAllocationPublication(t.Context(), f.grant)
	if err != nil || bridge.VerifyOwnedAllocationPublication(p, f.now) != nil {
		t.Fatalf("actual private burn/preflight/publication: %v", err)
	}
	if !reflect.DeepEqual(p.Reservation, f.grant.Reservation) || p.AllocationID != string(f.grant.Authority.Lease.ID)+"-in-process" ||
		p.AllocationGeneration != 1 || p.Allocation.ValidateForBinding(f.grant.Authority.SubstrateBinding, f.grant.Authority.HarnessBinding.Backend) != nil {
		t.Fatal("producer failed to preserve the actual linked in-process identity/original prepared observation")
	}
	burned, calls := f.burn.snapshot()
	if !burned || calls != 1 {
		t.Fatalf("confirmed durable burn: burned=%t calls=%d", burned, calls)
	}
	// Publication is not the executable PreparedInvocation and cannot cross a
	// serialized request boundary. Historical authenticity remains read-only.
	if _, err := json.Marshal(p); err == nil {
		t.Fatal("publication serialized across process boundary")
	}
	if err := bridge.VerifyOwnedAllocationPublication(p, p.PreparedInvocationExpiresAt.Add(time.Hour)); err != nil {
		t.Fatalf("exact historical observation lost authenticity: %v", err)
	}
	if bridge.VerifyOwnedAllocationPublication(p, p.ObservedAt.Add(-time.Microsecond)) == nil {
		t.Fatal("future observation accepted")
	}
	second, err := bridge.PrepareAllocationPublication(t.Context(), f.grant)
	if err == nil || !reflect.DeepEqual(second, ports.OwnedAllocationPublicationV1{}) {
		t.Fatal("replay minted a replacement observation")
	}
	copy := p.Clone()
	copy.Allocation.InProcess.LinkedBackendProfileDigest = strings.Repeat("f", 64)
	copy.Authenticator[0] ^= 1
	if bridge.VerifyOwnedAllocationPublication(p, f.now) != nil {
		t.Fatal("clone mutation aliased authentic observation")
	}
}

func TestOwnedAllocationPreparationRejectsForgedOrChangedIntent(t *testing.T) {
	f, bridge := newOwnedAllocationPreparationFixture(t)
	p, err := bridge.PrepareAllocationPublication(t.Context(), f.grant)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		name   string
		mutate func(*ports.OwnedAllocationPublicationV1)
	}{
		{"authenticator", func(p *ports.OwnedAllocationPublicationV1) { p.Authenticator[0] ^= 1 }},
		{"prepared_digest", func(p *ports.OwnedAllocationPublicationV1) { p.PreparedInvocationDigest = strings.Repeat("a", 64) }},
		{"supervisor_identity", func(p *ports.OwnedAllocationPublicationV1) { p.AllocationID = "caller-selected-allocation" }},
		{"generation", func(p *ports.OwnedAllocationPublicationV1) { p.AllocationGeneration++ }},
		{"original_time", func(p *ports.OwnedAllocationPublicationV1) { p.ObservedAt = p.ObservedAt.Add(time.Microsecond) }},
		{"expiry", func(p *ports.OwnedAllocationPublicationV1) {
			p.PreparedInvocationExpiresAt = p.PreparedInvocationExpiresAt.Add(-time.Microsecond)
		}},
		{"parent_claim", func(p *ports.OwnedAllocationPublicationV1) {
			p.Reservation.PhysicalInvocationClaimID = "replacement-claim"
		}},
		{"prepared_observation", func(p *ports.OwnedAllocationPublicationV1) {
			p.Allocation.ObservedProxyIdentityDigest = strings.Repeat("b", 64)
		}},
		{"complete_authority", func(p *ports.OwnedAllocationPublicationV1) { p.Authority.Lease.WorkerID = "other-worker" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			changed := p.Clone()
			change.mutate(&changed)
			if bridge.VerifyOwnedAllocationPublication(changed, f.now.Add(time.Second)) == nil {
				t.Fatal("changed original producer intent accepted")
			}
		})
	}
	// A genuine arbitrary prepared-token MAC is NOT evidence that this producer
	// confirmed durable burn and actually performed its preflight.
	prepared, err := f.issuer.Issue(f.grant, p.Allocation)
	if err != nil {
		t.Fatal(err)
	}
	forged := p.Clone()
	forged.PreparedInvocationDigest = string(prepared.Digest())
	forged.Authenticator = make([]byte, 32)
	if bridge.VerifyOwnedAllocationPublication(forged, f.now) == nil {
		t.Fatal("generic issuer token became actual producer evidence")
	}
}

func TestOwnedAllocationPreparationRestartAndUnknownBurnRemainClosed(t *testing.T) {
	for _, outcome := range []string{"confirmed", "lost_burn_ack", "denied"} {
		t.Run(outcome, func(t *testing.T) {
			f, bridge := newOwnedAllocationPreparationFixture(t)
			f.burn.unknownCommit = outcome == "lost_burn_ack"
			f.burn.deny = outcome == "denied"
			p, err := bridge.PrepareAllocationPublication(t.Context(), f.grant)
			if outcome == "confirmed" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !reflect.DeepEqual(p, ports.OwnedAllocationPublicationV1{}) {
				t.Fatal("unknown/denied burn leaked publication")
			}
			issuer, err := NewCapabilityIssuer(func() time.Time { return f.now }, bytes.NewReader(bytes.Repeat([]byte{29}, 32)))
			if err != nil {
				t.Fatal(err)
			}
			driver, err := NewInProcessExecutionSubstrateV1(func() time.Time { return f.now }, issuer, f.grant.Authority.HarnessBinding.Backend)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := NewBoundedOwnedAllocationPreparationV1(issuer, f.grant.Authority.HarnessBinding.Backend, f.grant.Authority.SubstrateBinding, driver, f.burn)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := issuer.MintAttemptEffectOwnershipGrant(f.grant.Authority, f.grant.Reservation, f.grant.GrantExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			second, err := restarted.PrepareAllocationPublication(t.Context(), grant)
			if err == nil || !reflect.DeepEqual(second, ports.OwnedAllocationPublicationV1{}) {
				t.Fatal("new key/process minted replacement allocation after burn")
			}
			if outcome == "confirmed" && restarted.VerifyOwnedAllocationPublication(p, f.now) == nil {
				t.Fatal("foreign producer key authenticated original receipt")
			}
		})
	}
}

func TestOwnedAllocationPreparationConcurrentProducesOneObservation(t *testing.T) {
	f, bridge := newOwnedAllocationPreparationFixture(t)
	start := make(chan struct{})
	type result struct {
		p   ports.OwnedAllocationPublicationV1
		err error
	}
	results := make(chan result, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			p, err := bridge.PrepareAllocationPublication(t.Context(), f.grant)
			results <- result{p, err}
		})
	}
	close(start)
	wg.Wait()
	success := 0
	for range 8 {
		r := <-results
		if r.err == nil {
			success++
			if bridge.VerifyOwnedAllocationPublication(r.p, f.now) != nil {
				t.Fatal("concurrent success unauthentic")
			}
		} else if !reflect.DeepEqual(r.p, ports.OwnedAllocationPublicationV1{}) {
			t.Fatal("loser leaked observation")
		}
	}
	if success != 1 {
		t.Fatalf("producer successes=%d want=1", success)
	}
	_, calls := f.burn.snapshot()
	if calls != 1 {
		t.Fatalf("durable burn calls=%d want=1", calls)
	}
}

func TestOwnedAllocationPreparationConstructorAndNilClosed(t *testing.T) {
	f, _ := newOwnedAllocationPreparationFixture(t)
	if _, err := NewBoundedOwnedAllocationPreparationV1(f.issuer, f.grant.Authority.HarnessBinding.Backend, f.grant.Authority.SubstrateBinding, nil, f.burn); err == nil {
		t.Fatal("missing actual driver accepted")
	}
	var absent *BoundedOwnedAllocationPreparationV1
	if p, err := absent.PrepareAllocationPublication(t.Context(), f.grant); err == nil || !reflect.DeepEqual(p, ports.OwnedAllocationPublicationV1{}) {
		t.Fatal("nil producer accepted")
	}
	if absent.VerifyOwnedAllocationPublication(ports.OwnedAllocationPublicationV1{}, f.now) == nil {
		t.Fatal("nil verifier accepted")
	}
}
