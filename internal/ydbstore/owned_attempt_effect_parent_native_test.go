package ydbstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// Immutable synthetic provenance, derived from ACTUAL fixture publication
// receipts and the original ingress identity. No MAC, caller pointer, generic
// approval boolean or current permission is inferred. This is not installation
// or payer approval for production, and performs no I/O on Store retries.
type nativeOwnedParentSourceVerifier struct {
	sourceBytes []byte
	profile     domain.OwnedAgentProfileCurrentV1
	spending    [4]domain.AttemptSpendingAuthorizationCurrentV1
}

func (v nativeOwnedParentSourceVerifier) VerifyOwnedAttemptEffectSource(source domain.AttemptExecutionSourceV1) error {
	raw, err := domain.EncodeAttemptExecutionSourceV1(source)
	if err != nil || !bytes.Equal(raw, v.sourceBytes) || source.ValidateAgainstCurrentSources(v.profile, v.spending, source.IssuedAt) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

type nativeOwnedParentGraph struct {
	nativeOwnedFinalGraph
	final   domain.AttemptExecutionAdmissionV1
	lease   domain.Lease
	request ports.ReserveAttemptEffectRequestV1
	issuer  *serverlessharness.CapabilityIssuer
}

func newNativeOwnedParentGraph(t *testing.T) nativeOwnedParentGraph {
	t.Helper()
	g := newNativeOwnedFinalGraph(t)
	// Keep the complete parent scenario shorter than its original immutable
	// preparation/runtime windows; the ordinary graph's one-minute product
	// ceiling is not an accidental timing gate for this native authority test.
	parentCtx, parentCancel := context.WithTimeout(g.ctx, time.Minute)
	t.Cleanup(parentCancel)
	g.ctx = parentCtx
	g.request.Limits.MaxRuntime = 2 * time.Minute
	store := g.f.store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, table := range []string{"attempt_effect_reservations", "lease_expiry_v2", "lease_expiry", "lease_heads", "leases"} {
			if _, err := store.db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", g.run.TenantID); err != nil {
				t.Errorf("cleanup unique parent fixture table=%s: %v", table, err)
			}
		}
	})
	admitted, err := store.admitBoundedOwnedDispatch(g.ctx, g.request)
	if err != nil {
		t.Fatalf("owned Final prerequisite: %v", err)
	}
	g.requireCommittedGraph(t, admitted)
	final, _ := g.finalAdmission(t)
	loaded, found, err := store.LoadWorkerJob(g.ctx, g.run.TenantID, g.run.ID)
	if err != nil || !found {
		t.Fatalf("canonical worker job prerequisite found=%v err=%v", found, err)
	}
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	lease, err := store.ClaimWorkerLease(g.ctx, ports.WorkerLeaseRequest{TenantID: g.run.TenantID, RunID: g.run.ID, AttemptID: g.attempt.ID,
		LeaseID: domain.LeaseID("parent-lease-" + string(g.run.ID)), WorkerID: "parent-worker-" + string(g.run.ID), Now: at, ExpiresAt: at.Add(time.Hour)})
	if err != nil {
		t.Fatalf("canonical physical lease prerequisite: %v", err)
	}
	at = nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	if err := store.StartWorkerJob(g.ctx, loaded, lease, at); err != nil {
		t.Fatalf("canonical queued-to-running prerequisite: %v", err)
	}
	verifier := nativeOwnedParentSourceVerifier{sourceBytes: []byte(g.outbox.OwnedExecutionSourceRecord), profile: g.profileReceipt.Profile.Clone()}
	for i, receipt := range g.spendReceipts {
		verifier.spending[i] = receipt.Authorization.Clone()
	}
	if err := verifier.VerifyOwnedAttemptEffectSource(final.Source); err != nil {
		t.Fatalf("original receipt-derived synthetic source provenance: %v", err)
	}
	store.ownedAttemptEffectSourceVerifier = verifier
	// The issuer's post-commit clock reads the SAME native DB time authority.
	// Queries are bounded by the test context and happen outside the Store's
	// authority transaction. No time.Now or guessed 1-ms wall-clock allowance.
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time {
		var sampled time.Time
		if store.db.QueryRowContext(g.ctx, `SELECT CurrentUtcTimestamp()`).Scan(&sampled) != nil {
			return time.Time{}
		}
		return sampled.UTC()
	}, strings.NewReader(strings.Repeat("parent-fixture-key", 4)))
	if err != nil {
		t.Fatal(err)
	}
	store.attemptEffectGrantIssuer = issuer
	upstream := strings.Repeat("7", 64)
	request := ports.ReserveAttemptEffectRequestV1{TenantID: g.run.TenantID, RunID: g.run.ID, AttemptID: g.attempt.ID,
		LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "parent-claim-" + string(g.run.ID), UpstreamIdempotencyKeyDigest: &upstream}
	result := nativeOwnedParentGraph{nativeOwnedFinalGraph: g, final: final, lease: lease, request: request, issuer: issuer}
	// Diagnostic use of the EXACT production current-authority join before the
	// physical INSERT, so SQL/type failures aren't mislabeled permission failures.
	err = store.ownedAuthorityTx(g.ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		_, actualFinal, err := store.currentOwnedAttemptEffectAuthority(ctx, q, request)
		if err != nil {
			return fmt.Errorf("complete current owned parent join: %w", err)
		}
		if !reflect.DeepEqual(actualFinal, final) {
			return errors.New("current join changed original Final")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("owned parent current whole-graph prerequisite: %v", err)
	}
	result.requireCount(t, "attempt_effect_reservations", 0)
	return result
}

func (g nativeOwnedParentGraph) persistedParent(t *testing.T) (domain.OwnedAttemptEffectParentV1, string) {
	t.Helper()
	var raw, digest string
	if err := g.f.store.db.QueryRowContext(g.ctx, `SELECT owned_record,owned_digest FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`,
		g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1).Scan(&raw, &digest); err != nil {
		t.Fatalf("actual persisted owned parent: %v", err)
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(raw))
	actual, digestErr := parent.Digest()
	if err != nil || digestErr != nil || actual != digest || parent.ValidateForAdmission(g.final) != nil {
		t.Fatalf("actual original parent/Final bytes: decode=%v digest=%v match=%v", err, digestErr, actual == digest)
	}
	return parent, raw
}

func (g nativeOwnedParentGraph) requireParentDenied(t *testing.T, request ports.ReserveAttemptEffectRequestV1, wantCount uint64) {
	t.Helper()
	result, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, request)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, ports.ReserveAttemptEffectResultV1{}) {
		t.Errorf("denial leaked reservation or grant: err=%v result=%+v", err, result)
	}
	g.requireCount(t, "attempt_effect_reservations", wantCount)
}

func TestOwnedAttemptEffectParentYDBCreateExactReplayAndCompetingObservation(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	created, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || created.Status != ports.AttemptEffectOwnedV1 || created.Grant == nil || created.ObservationGrant != nil {
		var persisted uint64
		countErr := g.f.store.db.QueryRowContext(g.ctx, `SELECT COUNT(*) FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID).Scan(&persisted)
		t.Logf("failed create diagnostic: committed_parent_rows=%d read_error=%v (whole current-authority prerequisite passed)", persisted, countErr)
		t.Fatalf("confirmed actual parent create: err=%v result=%+v", err, created)
	}
	if err := g.issuer.VerifyGrant(*created.Grant); err != nil {
		t.Fatalf("confirmed create ownership MAC: %v", err)
	}
	parent, original := g.persistedParent(t)
	if !reflect.DeepEqual(parent.Reservation, created.Reservation) || !created.Grant.GrantExpiresAt.Equal(parent.PreparationExpiresAt) || !parent.Authority.Lease.AcquiredAt.Equal(g.lease.AcquiredAt) {
		t.Fatal("create changed original lease/reservation/preparation window")
	}
	replayed, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request)
	if err != nil || replayed.Status != ports.AttemptEffectReplayedV1 || replayed.Grant == nil || replayed.ObservationGrant != nil || !reflect.DeepEqual(replayed.Reservation, created.Reservation) || !replayed.Grant.GrantExpiresAt.Equal(created.Grant.GrantExpiresAt) {
		t.Fatalf("exact original claim replay: err=%v result=%+v", err, replayed)
	}
	other := g.request
	other.PhysicalInvocationClaimID = "another-parent-claim-" + string(g.run.ID)
	observed, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, other)
	if err != nil || observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil || observed.ObservationGrant == nil || !reflect.DeepEqual(observed.Reservation, created.Reservation) {
		t.Fatalf("competing physical claim must be original observation only: err=%v result=%+v", err, observed)
	}
	if err := g.issuer.VerifyObservationGrant(*observed.ObservationGrant); err != nil {
		t.Fatalf("confirmed original observation MAC: %v", err)
	}
	_, after := g.persistedParent(t)
	if after != original {
		t.Error("replay/losing claim rewrote original parent bytes")
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
	// An owned private fixture claim must never be reinterpreted by the public
	// single-turn reader as a new serving permission.
	public, err := g.f.store.ReserveAttemptEffect(g.ctx, g.request)
	if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(public, ports.ReserveAttemptEffectResultV1{}) {
		t.Errorf("public reader accepted owned representation: err=%v result=%+v", err, public)
	}
}

func TestOwnedAttemptEffectParentYDBCurrentDenialsBeforeInsert(t *testing.T) {
	for _, name := range []string{"missing_verifier", "wrong_lease", "wrong_fence", "wrong_worker", "released_quota", "changed_Final", "revoked_source"} {
		t.Run(name, func(t *testing.T) {
			g := newNativeOwnedParentGraph(t)
			request := g.request
			switch name {
			case "missing_verifier":
				g.f.store.ownedAttemptEffectSourceVerifier = nil
			case "wrong_lease":
				request.LeaseID = "different-parent-lease"
			case "wrong_fence":
				request.FenceToken++
			case "wrong_worker":
				if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE leases SET worker_id=$3 WHERE tenant_id=$1 AND lease_id=$2`, g.run.TenantID, g.lease.ID, "other-worker"); err != nil {
					t.Fatal(err)
				}
			case "released_quota":
				quota, found, err := readJSON[domain.QuotaReservation](g.ctx, g.f.store.db, `SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, g.run.TenantID, g.nativeOwnedFinalGraph.request.ReservationID)
				if err != nil || !found {
					t.Fatalf("held quota fixture found=%v err=%v", found, err)
				}
				at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
				quota.Status, quota.UpdatedAt = domain.ReservationReleased, at
				if err := g.f.store.Transact(g.ctx, g.run.TenantID, func(state ports.StateTx) error { return state.PutQuotaReservation(g.ctx, quota) }); err != nil {
					t.Fatal(err)
				}
			case "changed_Final":
				if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_execution_admissions_v1 SET reservation_id=$3 WHERE tenant_id=$1 AND attempt_id=$2`, g.run.TenantID, g.attempt.ID, "different-held-capacity"); err != nil {
					t.Fatal(err)
				}
			case "revoked_source":
				g.revokeParentProfile(t)
			}
			g.requireParentDenied(t, request, 0)
		})
	}
}

func (g nativeOwnedParentGraph) revokeParentProfile(t *testing.T) {
	t.Helper()
	value := g.profileReceipt.Profile.Clone()
	value.State, value.RevocationGeneration, value.MutationID = domain.OwnedAgentCurrentRevokedV1, value.RevocationGeneration+1, "parent-revoke-"+string(g.run.ID)
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	grant := g.profileVerifier.install(t, ports.OwnedProfilePublicationGrantV1{Profile: value, ExpectedGeneration: g.profileReceipt.Profile.RevocationGeneration,
		ExpectedDigest: g.profileReceipt.Digest, IssuedAt: at, ExpiresAt: at.Add(10 * time.Second)})
	if _, err := g.f.store.PublishOwnedProfile(g.ctx, grant); err != nil {
		t.Fatalf("actual public profile revocation: %v", err)
	}
}

func TestOwnedAttemptEffectParentYDBReplayCannotBypassCurrentRevocation(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	if _, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request); err != nil {
		t.Fatal(err)
	}
	_, original := g.persistedParent(t)
	g.revokeParentProfile(t)
	g.requireParentDenied(t, g.request, 1)
	other := g.request
	other.PhysicalInvocationClaimID = "revoked-losing-claim-" + string(g.run.ID)
	observed, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, other)
	if err != nil || observed.Status != ports.AttemptEffectReconcileOnlyV1 || observed.Grant != nil || observed.ObservationGrant == nil {
		t.Fatalf("preserved historical source after revoke must be observation only: err=%v result=%+v", err, observed)
	}
	_, after := g.persistedParent(t)
	if original != after {
		t.Error("source revocation resurrected/replaced original parent")
	}
}

func TestOwnedAttemptEffectParentYDBSinglePrimaryKeyAndDualRepresentationDenial(t *testing.T) {
	for _, direction := range []string{"legacy_then_owned", "owned_then_legacy", "dual_reader_denial"} {
		t.Run(direction, func(t *testing.T) {
			g := newNativeOwnedParentGraph(t)
			legacy := g.validOpposingLegacyRecord(t)
			legacyRaw, err := marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if direction == "legacy_then_owned" {
				// The valid opposing representation is independently accepted by
				// its actual public historical reader BEFORE testing collision.
				r := legacy.Reservation
				if _, err := g.f.store.db.ExecContext(g.ctx, `INSERT INTO attempt_effect_reservations (tenant_id,attempt_id,kind,run_id,lease_id,fence_token,physical_invocation_claim_id,effect_sequence,invocation_authority_digest,reserved_at,record) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CAST($11 AS JsonDocument))`, r.TenantID, r.AttemptID, r.Kind, r.RunID, r.LeaseID, r.FenceToken, r.PhysicalInvocationClaimID, r.EffectSequence, r.InvocationAuthorityDigest, r.ReservedAt, legacyRaw); err != nil {
					t.Fatal(err)
				}
				public, err := g.f.store.ReserveAttemptEffect(g.ctx, g.request)
				if err != nil || public.Status != ports.AttemptEffectReconcileOnlyV1 || public.Grant != nil || public.ObservationGrant == nil || g.issuer.VerifyObservationGrant(*public.ObservationGrant) != nil || !reflect.DeepEqual(public.Reservation, legacy.Reservation) {
					t.Fatalf("valid native legacy reader control: err=%v result=%+v", err, public)
				}
				g.requireParentDenied(t, g.request, 1)
				return
			}
			if _, err := g.f.store.reserveBoundedOwnedAttemptEffect(g.ctx, g.request); err != nil {
				t.Fatal(err)
			}
			_, original := g.persistedParent(t)
			if direction == "owned_then_legacy" {
				if _, err := g.f.store.db.ExecContext(g.ctx, `INSERT INTO attempt_effect_reservations (tenant_id,attempt_id,kind,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, g.run.TenantID, g.attempt.ID, domain.ProviderEffectTurnV1, legacyRaw); err == nil {
					t.Error("same primary key legacy INSERT replaced an owned physical claim")
				}
				_, after := g.persistedParent(t)
				if original != after {
					t.Error("legacy collision overwrote original owned claim")
				}
			} else {
				if _, err := g.f.store.db.ExecContext(g.ctx, `UPDATE attempt_effect_reservations SET record=CAST($3 AS JsonDocument) WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$4`, g.run.TenantID, g.attempt.ID, legacyRaw, domain.ProviderEffectTurnV1); err != nil {
					t.Fatal(err)
				}
				g.requireParentDenied(t, g.request, 1)
			}
			public, err := g.f.store.ReserveAttemptEffect(g.ctx, g.request)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(public, ports.ReserveAttemptEffectResultV1{}) {
				t.Errorf("legacy public reader accepted owned/dual record: err=%v result=%+v", err, public)
			}
			g.requireCount(t, "attempt_effect_reservations", 1)
		})
	}
}

func (g nativeOwnedParentGraph) validOpposingLegacyRecord(t *testing.T) attemptEffectRecordV1 {
	t.Helper()
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(g.run.TenantID, g.f.scope.owner, g.run.ID, g.attempt.ID, g.run.SubscriptionConnectionID, g.lease.AcquiredAt)
	if err != nil {
		t.Fatal(err)
	}
	authority := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
		SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
		Lease: g.lease, ContextManifestDigest: strings.Repeat("8", 64), InputManifestDigest: strings.Repeat("9", 64), InvocationDeadline: g.lease.AcquiredAt.Add(10 * time.Minute)}
	at := nativeOwnedFinalDBTime(t, g.nativeManagedAdmissionGraph)
	reservation, err := domain.BuildAttemptEffectReservationV1(authority, "valid-legacy-original-"+string(g.run.ID), g.request.UpstreamIdempotencyKeyDigest, at)
	if err != nil {
		t.Fatalf("valid opposing legacy reservation: %v", err)
	}
	legacy := attemptEffectRecordV1{Authority: authority, Reservation: reservation}
	if err := legacy.validate(); err != nil || legacy.Authority.HarnessBinding.Profile != domain.HarnessExecutionSingleTurnV2 || !attemptEffectRequestMatchesRecord(g.request, legacy) {
		t.Fatalf("opposing native legacy control must be fully valid/matching public scope: %v", err)
	}
	return legacy
}

func TestOwnedAttemptEffectParentYDBConcurrentSolePhysicalClaim(t *testing.T) {
	g := newNativeOwnedParentGraph(t)
	ctx, cancel := context.WithCancel(g.ctx)
	ready, start := make(chan struct{}, 2), make(chan struct{})
	type outcome struct {
		index  int
		result ports.ReserveAttemptEffectResultV1
		err    error
	}
	finished := make(chan outcome, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	for i := range 2 {
		request := g.request
		request.PhysicalInvocationClaimID = fmt.Sprintf("parent-racer-%d-%s", i, g.run.ID)
		go func(index int, request ports.ReserveAttemptEffectRequestV1) {
			defer workers.Done()
			ready <- struct{}{}
			select {
			case <-start:
				result, err := g.f.store.reserveBoundedOwnedAttemptEffect(ctx, request)
				finished <- outcome{index, result, err}
			case <-ctx.Done():
				finished <- outcome{index: index, err: ctx.Err()}
			}
		}(i, request)
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-cleanup.Done():
			t.Error("parent claim workers exceeded bounded cleanup")
		}
	})
	released := false
	defer func() {
		if !released {
			close(start)
		}
	}()
	for range 2 {
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("parent claim barrier readiness deadline", ctx.Err())
		}
	}
	close(start)
	released = true
	owned, observed := 0, 0
	var winner domain.AttemptEffectReservationV1
	var reservations []domain.AttemptEffectReservationV1
	for range 2 {
		select {
		case out := <-finished:
			if out.err != nil {
				t.Errorf("actual serializable parent racer=%d err=%v", out.index, out.err)
				continue
			}
			reservations = append(reservations, out.result.Reservation.Clone())
			switch out.result.Status {
			case ports.AttemptEffectOwnedV1:
				owned++
				winner = out.result.Reservation
				if out.result.Grant == nil || out.result.ObservationGrant != nil {
					t.Errorf("winner lacked sole ownership grant")
				}
			case ports.AttemptEffectReconcileOnlyV1:
				observed++
				if out.result.Grant != nil || out.result.ObservationGrant == nil {
					t.Errorf("losing claimant received fresh permission")
				}
			default:
				t.Errorf("unexpected concurrent parent result=%s", out.result.Status)
			}
		case <-ctx.Done():
			t.Fatal("parent claim completion deadline", ctx.Err())
		}
	}
	select {
	case <-joined:
	case <-ctx.Done():
		t.Fatal("parent claim worker join deadline", ctx.Err())
	}
	if owned != 1 || observed != 1 {
		t.Fatalf("actual physical claims: owned=%d observed=%d want=1/1", owned, observed)
	}
	parent, _ := g.persistedParent(t)
	if !reflect.DeepEqual(parent.Reservation, winner) {
		t.Error("persisted physical claim differs from actual winning transaction")
	}
	for _, reservation := range reservations {
		if !reflect.DeepEqual(reservation, winner) {
			t.Error("a concurrent result substituted another physical claim for the confirmed original reservation")
		}
	}
	g.requireCount(t, "attempt_effect_reservations", 1)
}
