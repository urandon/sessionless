package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

type nativeOwnedPreparationBurnBridge struct{ store *Store }

func (b nativeOwnedPreparationBurnBridge) BeginOwnedAttemptPreparation(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	return b.store.beginBoundedOwnedAttemptPreparation(ctx, grant)
}

// This invokes the existing trusted in-process fixture substrate, not a caller
// supplied allocation. Its native point read would fail if preflight ran before
// the burn committed. It is not a cloud allocation or a production source keeper.
type nativeOwnedPreparationDriverBridge struct {
	store        *Store
	parentDigest string
	driver       *serverlessharness.InProcessExecutionSubstrateV1
	preflights   atomic.Uint64
	partial      bool
}

func (d *nativeOwnedPreparationDriverBridge) Preflight(ctx context.Context, a domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV2, error) {
	d.preflights.Add(1)
	var at time.Time
	var digest string
	if err := d.store.db.QueryRowContext(ctx, `SELECT preparation_started_at,preparation_parent_digest FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		a.Lease.TenantID, a.Lease.AttemptID, domain.ProviderEffectTurnV1).Scan(&at, &digest); err != nil || at.IsZero() || digest != d.parentDigest {
		return domain.PreparedAllocationV2{}, errors.New("native preflight preceded confirmed original durable burn")
	}
	observed, err := d.driver.Preflight(ctx, a)
	if err != nil {
		return domain.PreparedAllocationV2{}, err
	}
	if d.partial {
		return domain.PreparedAllocationV2{}, errors.New("test-owned prepared allocation outcome unknown")
	}
	return observed, nil
}

func nativeOwnedPreparationIssuer(t *testing.T, g nativeOwnedParentGraph, key string) (*serverlessharness.CapabilityIssuer, func() time.Time) {
	t.Helper()
	now := func() time.Time {
		var at time.Time
		if g.f.store.db.QueryRowContext(g.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at) != nil {
			return time.Time{}
		}
		return at.UTC()
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(now, strings.NewReader(strings.Repeat(key, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return issuer, now
}

func nativeOwnedPreparationBridge(t *testing.T, g nativeOwnedParentGraph, issuer *serverlessharness.CapabilityIssuer, now func() time.Time, grant ports.AttemptEffectOwnershipGrantV1, driver *nativeOwnedPreparationDriverBridge) *serverlessharness.BoundedOwnedAttemptPreparationV1 {
	t.Helper()
	// Each process has its own verifier; they share ONLY the canonical database.
	store := *g.f.store
	store.ownedPreparationGrantVerifier = issuer
	if driver.driver == nil {
		var err error
		driver.driver, err = serverlessharness.NewInProcessExecutionSubstrateV1(now, issuer, grant.Authority.HarnessBinding.Backend)
		if err != nil {
			t.Fatal(err)
		}
	}
	bridge, err := serverlessharness.NewBoundedOwnedAttemptPreparationV1(issuer, grant.Authority.HarnessBinding.Backend, grant.Authority.SubstrateBinding, driver, nativeOwnedPreparationBurnBridge{store: &store})
	if err != nil {
		t.Fatalf("native actual preparation composition: %v", err)
	}
	return bridge
}

func TestOwnedAttemptPreparationYDBPhysicalBridgeNoRestartRetry(t *testing.T) {
	for _, outcome := range []string{"success", "partial_unknown", "crash_before_preflight"} {
		t.Run(outcome, func(t *testing.T) {
			g := newNativeOwnedParentGraph(t)
			result, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
			if err != nil || result.Grant == nil {
				t.Fatalf("original native parent grant: %v", err)
			}
			parent, originalBytes := g.persistedParent(t)
			digest, _ := parent.Digest()
			issuer, now := nativeOwnedPreparationIssuer(t, g, "physical-original-key")
			grant, err := issuer.MintAttemptEffectOwnershipGrant(result.Grant.Authority, result.Reservation, result.Grant.GrantExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			driver := &nativeOwnedPreparationDriverBridge{store: g.f.store, parentDigest: digest, partial: outcome == "partial_unknown"}
			bridge := nativeOwnedPreparationBridge(t, g, issuer, now, grant, driver)
			if outcome == "crash_before_preflight" {
				store := *g.f.store
				store.ownedPreparationGrantVerifier = issuer
				fresh, err := store.beginBoundedOwnedAttemptPreparation(g.ctx, grant)
				if err != nil || !fresh {
					t.Fatalf("confirmed native burn before simulated process loss: fresh=%t err=%v", fresh, err)
				}
			}
			prepared, err := bridge.Prepare(g.ctx, grant)
			if outcome == "success" {
				if err != nil || issuer.Validate(prepared) != nil || prepared.Allocation().ValidateForBinding(grant.Authority.SubstrateBinding, grant.Authority.HarnessBinding.Backend) != nil {
					t.Fatalf("actual native burn -> trusted in-process preflight -> authentic capability: %v", err)
				}
			} else if err == nil || !reflect.DeepEqual(prepared, serverlessharness.PreparedInvocation{}) {
				t.Fatalf("unknown/crash leaked preparation result: err=%v zero=%t", err, reflect.DeepEqual(prepared, serverlessharness.PreparedInvocation{}))
			}
			wantCalls := uint64(1)
			if outcome == "crash_before_preflight" {
				wantCalls = 0
			}
			// A NEW key and empty issuer map simulate restart. Even an authentic
			// remint of the exact original claim cannot undo the actual DB burn.
			restarted, restartNow := nativeOwnedPreparationIssuer(t, g, "physical-restarted-key")
			reminted, err := restarted.MintAttemptEffectOwnershipGrant(grant.Authority, grant.Reservation, grant.GrantExpiresAt)
			if err != nil {
				t.Fatal(err)
			}
			second := nativeOwnedPreparationBridge(t, g, restarted, restartNow, reminted, driver)
			retry, err := second.Prepare(g.ctx, reminted)
			if err == nil || !reflect.DeepEqual(retry, serverlessharness.PreparedInvocation{}) || driver.preflights.Load() != wantCalls {
				t.Fatalf("restart repeated physical preparation: err=%v zero=%t calls=%d want=%d", err, reflect.DeepEqual(retry, serverlessharness.PreparedInvocation{}), driver.preflights.Load(), wantCalls)
			}
			_, after := g.persistedParent(t)
			if after != originalBytes {
				t.Fatal("preparation changed the original native parent")
			}
			observed, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
			if err != nil || observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil || observed.ObservationGrant == nil {
				t.Fatalf("burned native readback regained preparation ownership: err=%v result=%+v", err, observed)
			}
		})
	}
}

func TestOwnedAttemptPreparationYDBConcurrentPhysicalBridges(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	result, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || result.Grant == nil {
		t.Fatalf("native original parent: %v", err)
	}
	parent, _ := g.persistedParent(t)
	digest, _ := parent.Digest()
	driver := &nativeOwnedPreparationDriverBridge{store: g.f.store, parentDigest: digest}
	type candidate struct {
		bridge *serverlessharness.BoundedOwnedAttemptPreparationV1
		grant  ports.AttemptEffectOwnershipGrantV1
		issuer *serverlessharness.CapabilityIssuer
	}
	candidates := make([]candidate, 0, 2)
	for _, key := range []string{"physical-concurrent-first", "physical-concurrent-second"} {
		issuer, now := nativeOwnedPreparationIssuer(t, g, key)
		grant, err := issuer.MintAttemptEffectOwnershipGrant(result.Grant.Authority, result.Reservation, result.Grant.GrantExpiresAt)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate{nativeOwnedPreparationBridge(t, g, issuer, now, grant, driver), grant, issuer})
	}
	start := make(chan struct{})
	type completed struct {
		prepared serverlessharness.PreparedInvocation
		err      error
		issuer   *serverlessharness.CapabilityIssuer
	}
	results := make(chan completed, 2)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(g.ctx, 30*time.Second)
	t.Cleanup(func() { cancel(); wg.Wait() })
	for _, c := range candidates {
		wg.Go(func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- completed{err: ctx.Err()}
				return
			}
			prepared, err := c.bridge.Prepare(ctx, c.grant)
			results <- completed{prepared, err, c.issuer}
		})
	}
	close(start)
	winners := 0
	for range 2 {
		select {
		case r := <-results:
			if r.err == nil {
				winners++
				if r.issuer.Validate(r.prepared) != nil {
					t.Error("native winner returned unauthenticated capability")
				}
			} else if !reflect.DeepEqual(r.prepared, serverlessharness.PreparedInvocation{}) {
				t.Error("native losing process retained a preparation capability")
			}
		case <-ctx.Done():
			t.Fatalf("native physical preparation race did not finish: %v", ctx.Err())
		}
	}
	wg.Wait()
	if winners != 1 || driver.preflights.Load() != 1 {
		t.Fatalf("native fresh commit -> physical boundary: winners=%d actualPreflights=%d want1/1", winners, driver.preflights.Load())
	}
}
