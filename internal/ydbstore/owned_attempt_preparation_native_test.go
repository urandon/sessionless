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
)

// This uses the repository's gated disposable-YDB graph; it does not create
// or start a database. Separate Store values model independent store instances,
// not separate OS processes. A fresh instance after the confirmed durable burn
// models resume by a caller that did not observe the first result.
func TestOwnedAttemptPreparationYDBConcurrentFreshStoreResumeBurnsOnlyOnce(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil {
		t.Fatalf("native owned-parent prerequisite: err=%v result=%+v", err, created)
	}
	if err := g.issuer.VerifyGrant(*created.Grant); err != nil {
		t.Fatalf("authentic test grant before burn: %v", err)
	}
	parent, originalParentRaw := g.persistedParent(t)
	parentDigest, err := parent.Digest()
	if err != nil {
		t.Fatal(err)
	}

	storeA, storeB := *g.f.store, *g.f.store
	start := make(chan struct{})
	type burnResult struct {
		fresh bool
		err   error
	}
	results := make(chan burnResult, 2)
	ctx, cancel := context.WithTimeout(g.ctx, 25*time.Second)
	var wg sync.WaitGroup
	t.Cleanup(func() { cancel(); wg.Wait() })
	for _, store := range []*Store{&storeA, &storeB} {
		wg.Go(func() {
			select {
			case <-start:
			case <-ctx.Done():
				results <- burnResult{err: ctx.Err()}
				return
			}
			fresh, err := store.beginBoundedOwnedAttemptPreparation(ctx, *created.Grant)
			results <- burnResult{fresh: fresh, err: err}
		})
	}
	close(start)
	completed := make([]burnResult, 0, 2)
	for range 2 {
		select {
		case result := <-results:
			completed = append(completed, result)
		case <-ctx.Done():
			t.Fatalf("native concurrent burn did not finish: %v", ctx.Err())
		}
	}
	wg.Wait()
	first, second := completed[0], completed[1]
	if first.err != nil || second.err != nil || first.fresh == second.fresh {
		t.Fatalf("two independent store instances must confirm exactly one fresh burn: first=%+v second=%+v", first, second)
	}
	if g.issuer.VerifyGrant(*created.Grant) != nil {
		t.Fatal("authentic original grant stopped verifying after burn")
	}

	var startedAt time.Time
	var storedDigest string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT preparation_started_at,preparation_parent_digest
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&startedAt, &storedDigest); err != nil {
		t.Fatalf("read actual durable preparation burn: %v", err)
	}
	if startedAt.IsZero() || storedDigest != parentDigest {
		t.Fatalf("actual burn must bind the exact persisted parent: at=%s digest=%q want=%q", startedAt, storedDigest, parentDigest)
	}

	storeAfterResume := *g.f.store
	fresh, err := storeAfterResume.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if err != nil || fresh {
		t.Fatalf("fresh Store instance after an unobserved successful burn must not reprepare: fresh=%t err=%v", fresh, err)
	}
	_, parentAfterResume := g.persistedParent(t)
	if parentAfterResume != originalParentRaw {
		t.Fatal("burn/resume changed the immutable original parent bytes")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestOwnedAttemptPreparationYDBBurnedParentReservationIsObservationOnly(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("native owned-parent prerequisite: err=%v result=%+v", err, created)
	}
	parent, originalRaw := g.persistedParent(t)
	fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if err != nil || !fresh {
		t.Fatalf("first native preparation burn: fresh=%t err=%v", fresh, err)
	}
	result, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil || result.ObservationGrant == nil || !reflect.DeepEqual(result.Reservation, parent.Reservation) {
		t.Fatalf("burned owner reservation must become observation-only: err=%v result=%+v", err, result)
	}
	_, afterRaw := g.persistedParent(t)
	if afterRaw != originalRaw {
		t.Fatal("post-burn reservation flow changed original parent bytes")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestOwnedAttemptPreparationYDBCurrentRevocationDeniesBeforeBurn(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("native owned-parent prerequisite: err=%v result=%+v", err, created)
	}
	_, originalRaw := g.persistedParent(t)
	g.revokeParentProfile(t)
	fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if !errors.Is(err, domain.ErrMembershipDenied) || fresh {
		t.Fatalf("current source revocation must deny before preparation burn: fresh=%t err=%v", fresh, err)
	}
	var timePresent, digestPresent uint64
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CASE WHEN preparation_started_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&timePresent, &digestPresent); err != nil {
		t.Fatalf("read revoked parent's burn markers: %v", err)
	}
	if timePresent != 0 || digestPresent != 0 {
		t.Fatalf("revoked parent must remain unburned: time=%d digest=%d", timePresent, digestPresent)
	}
	_, afterRaw := g.persistedParent(t)
	if afterRaw != originalRaw {
		t.Fatal("current source revocation changed the immutable owned parent")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestOwnedAttemptPreparationYDBRejectsMalformedBurnWithoutRepair(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	g.f.store.ownedPreparationGrantVerifier = g.issuer
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Grant == nil {
		t.Fatalf("native owned-parent prerequisite: err=%v result=%+v", err, created)
	}
	parent, originalRaw := g.persistedParent(t)
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET preparation_started_at=$4
 WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1, at); err != nil {
		t.Fatalf("create actual one-sided preparation marker: %v", err)
	}
	fresh, err := g.f.store.beginBoundedOwnedAttemptPreparation(g.ctx, *created.Grant)
	if !errors.Is(err, domain.ErrMembershipDenied) || fresh {
		t.Fatalf("malformed existing burn must deny rather than repair: fresh=%t err=%v", fresh, err)
	}
	var timePresent, digestPresent uint64
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT CASE WHEN preparation_started_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_parent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&timePresent, &digestPresent); err != nil {
		t.Fatalf("read malformed burn markers: %v", err)
	}
	if timePresent != 1 || digestPresent != 0 {
		t.Fatalf("malformed burn was unexpectedly repaired/cleared: time=%d digest=%d", timePresent, digestPresent)
	}
	_, afterRaw := g.persistedParent(t)
	if afterRaw != originalRaw || parent.Reservation.PhysicalInvocationClaimID != g.request.PhysicalInvocationClaimID {
		t.Fatal("malformed burn changed original parent or claim")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}

func TestReserveAttemptEffectYDBLegacyParentRejectsPreparationMarkers(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	legacy := g.validOpposingLegacyRecord(t)
	legacyRaw, err := marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	r := legacy.Reservation
	if _, err := g.f.store.db.ExecContext(g.ctx, `INSERT INTO attempt_effect_reservations
 (tenant_id,attempt_id,kind,run_id,lease_id,fence_token,physical_invocation_claim_id,effect_sequence,invocation_authority_digest,reserved_at,record)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`, r.TenantID, r.AttemptID, r.Kind, r.RunID, r.LeaseID, r.FenceToken, r.PhysicalInvocationClaimID, r.EffectSequence, r.InvocationAuthorityDigest, r.ReservedAt, legacyRaw); err != nil {
		t.Fatalf("insert actual pure legacy control: %v", err)
	}
	positive, err := g.f.store.ReserveAttemptEffect(g.ctx, g.request)
	if err != nil || positive.Status != ports.AttemptEffectReconcileOnlyV1 || positive.Grant != nil || positive.ObservationGrant == nil || g.issuer.VerifyObservationGrant(*positive.ObservationGrant) != nil || !reflect.DeepEqual(positive.Reservation, legacy.Reservation) {
		t.Fatalf("pure legacy control should remain readable: err=%v result=%+v", err, positive)
	}
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET preparation_started_at=$4
 WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1, at); err != nil {
		t.Fatalf("attach orphan preparation marker to actual legacy row: %v", err)
	}
	denied, err := g.f.store.ReserveAttemptEffect(g.ctx, g.request)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(denied, ports.ReserveAttemptEffectResultV1{}) {
		t.Fatalf("legacy row with an owned-only marker must deny with zero result: err=%v result=%+v", err, denied)
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}
