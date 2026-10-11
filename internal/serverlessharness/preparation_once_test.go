package serverlessharness

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type preparationOnceDriver struct {
	*substrateRegistryRecordingDriver
	preparations atomic.Uint64
	failure      error
}

func (d *preparationOnceDriver) Preflight(ctx context.Context, _ domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error) {
	if err := ctx.Err(); err != nil {
		return domain.PreparedAllocationV1{}, err
	}
	d.preparations.Add(1)
	if d.failure != nil {
		return domain.PreparedAllocationV1{}, d.failure
	}
	return d.allocation.Clone(), nil
}

func TestSubstratePreparationConsumesParentBeforePreflight(t *testing.T) {
	for _, failed := range []bool{false, true} {
		name := "successful_preparation"
		if failed {
			name = "partial_preparation_unknown"
		}
		t.Run(name, func(t *testing.T) {
			authority, parent, allocation, at := capabilityFixture(t)
			issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			driver := &preparationOnceDriver{substrateRegistryRecordingDriver: &substrateRegistryRecordingDriver{allocation: allocation}}
			if failed {
				driver.failure = errors.New("test-owned partial preparation outcome unknown")
			}
			preparer, err := NewExactExecutionPreparerV1(func() time.Time { return at }, issuer,
				func(domain.ServerlessInvocationAuthorityV1) (SubstrateRegistrationV1, error) {
					return SubstrateRegistrationV1{Binding: authority.SubstrateBinding, Enabled: true, Driver: driver}, nil
				})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			t.Cleanup(cancel)
			result := ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: parent, Grant: &grant}
			_, err = preparer.PrepareExecution(ctx, result)
			if (err != nil) != failed || driver.preparations.Load() != 1 {
				t.Fatalf("first preparation failed=%t err=%v calls=%d, want one actual preflight", failed, err, driver.preparations.Load())
			}
			// Each call constructs a NEW registry; a registry-local map would not
			// prevent the second physical preparation. Also remint the authentic
			// grant as committed reservation readback can do: the parent is fixed.
			reminted, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(30*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			result.Status, result.Grant = ports.AttemptEffectReplayedV1, &reminted
			if _, err := preparer.PrepareExecution(ctx, result); err == nil {
				t.Fatal("same parent prepared again through newly constructed registry")
			}
			if driver.preparations.Load() != 1 {
				t.Fatalf("replayed preparation calls=%d, want 1 even after unknown first outcome", driver.preparations.Load())
			}
		})
	}
}

func TestSubstratePreparationConcurrentRegistriesShareOneParentFence(t *testing.T) {
	authority, parent, allocation, at := capabilityFixture(t)
	issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	driver := &preparationOnceDriver{substrateRegistryRecordingDriver: &substrateRegistryRecordingDriver{allocation: allocation}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	for range 2 {
		registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer,
			SubstrateRegistrationV1{Binding: authority.SubstrateBinding, Enabled: true, Driver: driver})
		if err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- ctx.Err()
				return
			}
			_, err := registry.Prepare(ctx, ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: parent, Grant: &grant})
			results <- err
		})
	}
	close(start)
	var admitted, denied int
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				admitted++
			} else {
				denied++
			}
		case <-ctx.Done():
			t.Fatalf("preparation workers did not finish: %v", ctx.Err())
		}
	}
	workers.Wait()
	if admitted != 1 || denied != 1 || driver.preparations.Load() != 1 {
		t.Fatalf("concurrent preparation admitted=%d denied=%d calls=%d, want 1/1/1", admitted, denied, driver.preparations.Load())
	}
}

func TestSubstratePreparationForgedGrantDoesNotBurnLegitimateParent(t *testing.T) {
	authority, parent, allocation, at := capabilityFixture(t)
	issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	driver := &preparationOnceDriver{substrateRegistryRecordingDriver: &substrateRegistryRecordingDriver{allocation: allocation}}
	registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer,
		SubstrateRegistrationV1{Binding: authority.SubstrateBinding, Enabled: true, Driver: driver})
	if err != nil {
		t.Fatal(err)
	}
	bad := grant.Clone()
	bad.Authenticator[0] ^= 1
	if issuer.ConsumePreparationGrant(bad) == nil {
		t.Fatal("forged grant burned preparation right")
	}
	prepared, err := registry.Prepare(t.Context(), ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: parent, Grant: &grant})
	if err != nil || driver.preparations.Load() != 1 {
		t.Fatalf("legitimate parent denied after forgery: calls=%d err=%v", driver.preparations.Load(), err)
	}
	// The preparation fence is separate from the actual one-use effect gate.
	if err := issuer.Consume(prepared); err != nil {
		t.Fatalf("first effect denied after legitimate preparation: %v", err)
	}
	if issuer.Consume(prepared) == nil {
		t.Fatal("preparation fence weakened the original one-use execution gate")
	}
}

func TestSubstratePreparationBoundedTombstonesAndClockRewind(t *testing.T) {
	authority, parent, _, at := capabilityFixture(t)
	clock := at
	issuer, err := NewCapabilityIssuer(func() time.Time { return clock }, nil)
	if err != nil {
		t.Fatal(err)
	}
	var original ports.AttemptEffectOwnershipGrantV1
	// This tests the process-local issuer's capacity only. A real canonical
	// store decides one sole claim; these synthetic grants prove no YDB claim.
	for i := range preparationClaimCapacityV1 {
		r, err := domain.BuildAttemptEffectReservationV1(authority, fmt.Sprintf("bounded-parent-%d", i), nil, at)
		if err != nil {
			t.Fatal(err)
		}
		g, err := issuer.MintAttemptEffectOwnershipGrant(authority, r, at.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			original = g.Clone()
		}
		if err := issuer.ConsumePreparationGrant(g); err != nil {
			t.Fatalf("valid capacity entry=%d denied: %v", i, err)
		}
	}
	spare, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if issuer.ConsumePreparationGrant(spare) == nil || len(issuer.prepared) != preparationClaimCapacityV1 {
		t.Fatal("preparation permission map exceeded its finite capacity")
	}
	// A later canonical authority is independently sealed, not a renewal of
	// the original parent. Only original absolute expiry permits pruning.
	clock = authority.InvocationDeadline.Add(31 * time.Second)
	next := authority.Clone()
	next.Lease.ID = "later-lease"
	next.Lease.FenceToken++
	next.Lease.AcquiredAt, next.Lease.ExpiresAt = clock, clock.Add(time.Hour)
	next.InvocationDeadline = clock.Add(50 * time.Minute)
	r, err := domain.BuildAttemptEffectReservationV1(next, "later-parent", nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := issuer.MintAttemptEffectOwnershipGrant(next, r, clock.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := issuer.ConsumePreparationGrant(fresh); err != nil || len(issuer.prepared) != 1 {
		t.Fatalf("expired tombstones not boundedly reclaimed: count=%d err=%v", len(issuer.prepared), err)
	}
	clock = at
	if issuer.ConsumePreparationGrant(original) == nil || len(issuer.prepared) != 1 {
		t.Fatal("clock rewind revived a pruned parent or changed retained state")
	}
}

func TestSubstratePreparationInvalidContextNeverAllocates(t *testing.T) {
	for _, name := range []string{"nil", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			authority, parent, allocation, at := capabilityFixture(t)
			issuer, err := NewCapabilityIssuer(func() time.Time { return at }, nil)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			driver := &preparationOnceDriver{substrateRegistryRecordingDriver: &substrateRegistryRecordingDriver{allocation: allocation}}
			registry, err := NewSubstrateRegistryV1(func() time.Time { return at }, issuer,
				SubstrateRegistrationV1{Binding: authority.SubstrateBinding, Enabled: true, Driver: driver})
			if err != nil {
				t.Fatal(err)
			}
			var ctx context.Context
			if name == "cancelled" {
				cancelled, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				cancel()
				ctx = cancelled
			}
			if _, err := registry.Prepare(ctx, ports.ReserveAttemptEffectResultV1{Status: ports.AttemptEffectOwnedV1, Reservation: parent, Grant: &grant}); err == nil {
				t.Fatal("invalid context accepted for physical preparation")
			}
			if calls := driver.preparations.Load(); calls != 0 {
				t.Fatalf("context=%s caused preparations=%d, want 0", name, calls)
			}
		})
	}
}

func TestSubstratePreparationConcurrentClockSamplesDoNotDenyDistinctParents(t *testing.T) {
	authority, _, _, at := capabilityFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	resumeEarlier := make(chan struct{})
	var resume sync.Once
	releaseEarlier := func() { resume.Do(func() { close(resumeEarlier) }) }
	sampleOrder := make(chan bool, 1)
	var controlled bool
	var clockCalls atomic.Uint64
	var issuer *CapabilityIssuer
	var err error
	issuer, err = NewCapabilityIssuer(func() time.Time {
		if !controlled {
			return at
		}
		if clockCalls.Add(1) == 1 {
			// Test-only scheduling hook: if sampling is outside the issuer's
			// critical section, suspend the earlier sampled parent and let the
			// later one commit first. No real-time sleep decides correctness.
			outside := issuer.mu.TryLock()
			if outside {
				issuer.mu.Unlock()
			}
			sampleOrder <- outside
			if outside {
				select {
				case <-resumeEarlier:
				case <-ctx.Done():
				}
			}
			return at
		}
		return at.Add(time.Microsecond)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var grants [2]ports.AttemptEffectOwnershipGrantV1
	// Unit-only authenticated parents exercise issuer scheduling, not a claim
	// that a canonical Attempt may have two physical owners.
	for i := range grants {
		parent, err := domain.BuildAttemptEffectReservationV1(authority, fmt.Sprintf("clock-parent-%d", i), nil, at)
		if err != nil {
			t.Fatal(err)
		}
		grants[i], err = issuer.MintAttemptEffectOwnershipGrant(authority, parent, at.Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
	}
	controlled = true // Written before either goroutine; immutable thereafter.
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); releaseEarlier(); workers.Wait() })
	results := [2]chan error{make(chan error, 1), make(chan error, 1)}
	workers.Go(func() { results[0] <- issuer.ConsumePreparationGrant(grants[0]) })
	var outside bool
	select {
	case outside = <-sampleOrder:
	case <-ctx.Done():
		t.Fatalf("earlier parent did not sample the controlled clock: %v", ctx.Err())
	}
	workers.Go(func() { results[1] <- issuer.ConsumePreparationGrant(grants[1]) })
	for _, i := range []int{1, 0} {
		select {
		case err := <-results[i]:
			if err != nil {
				t.Errorf("distinct parent=%d denied on a monotonic clock (earlier sample outside lock=%t): %v", i, outside, err)
			}
		case <-ctx.Done():
			t.Fatalf("distinct parent=%d did not complete: %v", i, ctx.Err())
		}
		if i == 1 {
			releaseEarlier()
		}
	}
	workers.Wait()
	if len(issuer.prepared) != 2 {
		t.Errorf("accepted distinct parent tombstones=%d, want 2", len(issuer.prepared))
	}
}
