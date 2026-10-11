package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func nativeOwnedAllocationPublication(t *testing.T, g nativeOwnedParentGraph) ports.OwnedAllocationPublicationV1 {
	t.Helper()
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("native sole-parent preparation prerequisite: %v", err)
	}
	a := created.Grant.Authority
	driver, err := serverlessharness.NewInProcessExecutionSubstrateV1(func() time.Time { return nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph) }, g.issuer, a.HarnessBinding.Backend)
	if err != nil {
		t.Fatal(err)
	}
	bridge, err := serverlessharness.NewBoundedOwnedAllocationPreparationV1(g.issuer, a.HarnessBinding.Backend, a.SubstrateBinding, driver, allocationBurnAdapter{g.f.store})
	if err != nil {
		t.Fatal(err)
	}
	g.f.store.ownedAllocationPublicationVerifier = bridge
	p, err := bridge.PrepareAllocationPublication(g.ctx, *created.Grant)
	if err != nil {
		t.Fatalf("actual linked preparation outside native Tx: %v", err)
	}
	return p
}

func nativeOwnedAllocationBytes(t *testing.T, g nativeOwnedParentGraph) (string, string, string, string) {
	t.Helper()
	var original, originalDigest, current, currentDigest string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT allocation_original_record,allocation_original_digest,allocation_current_record,allocation_current_digest
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&original, &originalDigest, &current, &currentDigest); err != nil {
		t.Fatalf("read actual persisted allocation snapshots: %v", err)
	}
	return original, originalDigest, current, currentDigest
}

func TestOwnedAllocationRegistrationYDBConcurrentActualProducerPublicationAndFreshStoreRead(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	p := nativeOwnedAllocationPublication(t, g)
	_, parentBytes := g.persistedParent(t)
	storeA, storeB := *g.f.store, *g.f.store
	start := make(chan struct{})
	type outcome struct {
		receipt ports.OwnedAllocationRegistrationReceiptV1
		err     error
	}
	results := make(chan outcome, 2)
	ctx, cancel := context.WithTimeout(g.ctx, 25*time.Second)
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	for _, store := range []*Store{&storeA, &storeB} {
		wg.Go(func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- outcome{err: ctx.Err()}
				return
			}
			receipt, err := store.publishBoundedOwnedAllocation(ctx, p.Clone())
			results <- outcome{receipt, err}
		})
	}
	close(start)
	var outcomes []outcome
	for range 2 {
		select {
		case got := <-results:
			outcomes = append(outcomes, got)
		case <-ctx.Done():
			t.Fatalf("native concurrent publication deadline: %v", ctx.Err())
		}
	}
	wg.Wait()
	first, second := outcomes[0], outcomes[1]
	if first.err != nil || second.err != nil || first.receipt.Replayed == second.receipt.Replayed || first.receipt.Digest != second.receipt.Digest || !reflect.DeepEqual(first.receipt.Original, second.receipt.Original) {
		t.Fatalf("native create-once actual observer: first err=%v replay=%t digest=%s; second err=%v replay=%t digest=%s", first.err, first.receipt.Replayed, first.receipt.Digest, second.err, second.receipt.Replayed, second.receipt.Digest)
	}
	original, originalDigest, current, currentDigest := nativeOwnedAllocationBytes(t, g)
	if original != current || originalDigest != currentDigest || currentDigest != first.receipt.Digest {
		t.Fatal("actual initial original/current columns are not the exact committed snapshot")
	}
	resumed := *g.f.store
	resumed.ownedAllocationPublicationVerifier = nil // restarted issuer/proof is not rebuilt
	history, err := resumed.observeBoundedOwnedAllocation(g.ctx, g.request, false)
	if err != nil || history.Digest != currentDigest || !reflect.DeepEqual(history.Original, first.receipt.Original) {
		t.Fatalf("fresh Store historical original read without rebuilding MAC err=%v digest=%s", err, history.Digest)
	}
	currentReceipt, err := resumed.observeBoundedOwnedAllocation(g.ctx, g.request, true)
	if err != nil || currentReceipt.Digest != currentDigest {
		t.Fatalf("fresh Store complete-current observation err=%v digest=%s", err, currentReceipt.Digest)
	}
	reservation, err := resumed.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || reservation.Grant != nil || reservation.Status != ports.AttemptEffectReconcileOnlyV1 {
		t.Fatalf("registered parent cannot grant another preparation: err=%v status=%s", err, reservation.Status)
	}
	_, afterParent := g.persistedParent(t)
	if afterParent != parentBytes {
		t.Fatal("actual registration replaced original parent bytes")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestOwnedAllocationRegistrationYDBRetirementRevokeAndHistoricalCurrentSeparation(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	p := nativeOwnedAllocationPublication(t, g)
	first, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, p)
	if err != nil {
		t.Fatalf("native registered actual producer prerequisite: %v", err)
	}
	original, originalDigest, _, _ := nativeOwnedAllocationBytes(t, g)
	g.revokeParentProfile(t)
	if got, err := g.f.store.observeBoundedOwnedAllocation(g.ctx, g.request, true); !errors.Is(err, domain.ErrMembershipDenied) || got.Digest != "" {
		t.Fatalf("revoked canonical permission must deny current observation err=%v digest=%s", err, got.Digest)
	}
	if got, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, p.Clone()); err != nil || !got.Replayed || got.Digest != first.Digest {
		t.Fatalf("exact expired/revoked publication reconciliation must preserve original bytes err=%v replay=%t", err, got.Replayed)
	}
	retired, err := g.f.store.retireBoundedOwnedAllocation(g.ctx, g.request, first.Digest, domain.RegisteredPreparationRetiredV1)
	if err != nil || retired.Current.State != domain.RegisteredPreparationRetiredV1 {
		t.Fatalf("native historical retirement after source revoke: %v", err)
	}
	resumed := *g.f.store
	ack, err := resumed.retireBoundedOwnedAllocation(g.ctx, g.request, first.Digest, domain.RegisteredPreparationRetiredV1)
	if err != nil || !ack.Replayed || ack.Digest != retired.Digest {
		t.Fatalf("fresh caller same exact retirement intent observation: %v", err)
	}
	revoked, err := resumed.retireBoundedOwnedAllocation(g.ctx, g.request, retired.Digest, domain.RegisteredPreparationRevokedV1)
	if err != nil || revoked.Current.RevocationGeneration != 3 {
		t.Fatalf("native monotonic retired→revoked transition: %v", err)
	}
	ack, err = resumed.retireBoundedOwnedAllocation(g.ctx, g.request, retired.Digest, domain.RegisteredPreparationRevokedV1)
	if err != nil || !ack.Replayed || ack.Digest != revoked.Digest {
		t.Fatalf("exact latest revoke result recovery: %v", err)
	}
	if got, err := resumed.retireBoundedOwnedAllocation(g.ctx, g.request, first.Digest, domain.RegisteredPreparationRetiredV1); err == nil || got.Digest != "" {
		t.Fatalf("superseded CAS cannot overwrite revoked head err=%v", err)
	}
	after, afterDigest, current, currentDigest := nativeOwnedAllocationBytes(t, g)
	if original != after || originalDigest != afterDigest || current == original || currentDigest == originalDigest || currentDigest != revoked.Digest {
		t.Fatal("native tombstone corrupted original active snapshot or current digest")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestOwnedAllocationRegistrationYDBMissingBurnForeignClaimAndPartialChildDeny(t *testing.T) {
	for _, name := range []string{"missing_burn", "foreign_claim", "partial_child"} {
		t.Run(name, func(t *testing.T) {
			g := newNativeOwnedParentGraph(t)
			p := nativeOwnedAllocationPublication(t, g)
			_, parentBytes := g.persistedParent(t)
			switch name {
			case "missing_burn":
				if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET preparation_started_at=NULL,preparation_parent_digest=NULL WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1); err != nil {
					t.Fatal(err)
				}
			case "foreign_claim":
				p.Reservation.PhysicalInvocationClaimID += "-foreign"
			case "partial_child":
				if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET allocation_original_digest=$4 WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1, p.PreparedInvocationDigest); err != nil {
					t.Fatal(err)
				}
			}
			got, err := g.f.store.publishBoundedOwnedAllocation(g.ctx, p)
			if !errors.Is(err, domain.ErrMembershipDenied) || got.Digest != "" {
				t.Fatalf("native %s must deny without preparing/replacing child err=%v", name, err)
			}
			_, after := g.persistedParent(t)
			if after != parentBytes {
				t.Fatal("denied child publication changed original parent bytes")
			}
			var currentPresent uint64
			if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CASE WHEN allocation_current_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&currentPresent); err != nil {
				t.Fatal(err)
			}
			if currentPresent != 0 {
				t.Fatal("failed native publication manufactured an active child")
			}
			g.requireCount(t, "attempt_effect_reservations", 1)
		})
	}
}

func TestOwnedAllocationRegistrationYDBOrphanChildCannotAuthorizeFreshPreparation(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("native original unburned parent prerequisite: %v", err)
	}
	_, originalParent := g.persistedParent(t)
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET allocation_original_digest=$4
 WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1, created.Grant.Reservation.InvocationAuthorityDigest); err != nil {
		t.Fatalf("create actual orphan child without preparation burn: %v", err)
	}
	fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if !errors.Is(err, domain.ErrMembershipDenied) || fresh {
		t.Fatalf("orphan allocation must deny first preparation before physical I/O: err=%v fresh=%t", err, fresh)
	}
	reservation, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if !errors.Is(err, domain.ErrMembershipDenied) || reservation.Grant != nil {
		t.Fatalf("orphan allocation cannot mint another preparation grant: err=%v status=%s", err, reservation.Status)
	}
	var timePresent, digestPresent uint64
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CASE WHEN preparation_started_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&timePresent, &digestPresent); err != nil {
		t.Fatal(err)
	}
	if timePresent != 0 || digestPresent != 0 {
		t.Fatalf("orphan child was repaired by burning or replacing its parent: time=%d digest=%d", timePresent, digestPresent)
	}
	_, after := g.persistedParent(t)
	if after != originalParent {
		t.Fatal("denied orphan preparation changed immutable original parent")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}
