package attachedworkeronboardingserver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var onboardingTime = time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

type fixtureClock struct{}

func (fixtureClock) Now() time.Time { return onboardingTime.Add(time.Second) }

type fixtureIDs struct{}

func (fixtureIDs) NewID(_ context.Context, kind ports.IDKind) (string, error) {
	return "test-" + string(kind), nil
}

type onboardingStore struct {
	ports.AttachedWorkerStore
	enrollment           domain.AttachedWorkerEnrollment
	worker               domain.AttachedWorker
	registrationError    error
	claimCalls           int
	casCalls             int
	lostRotationResponse bool
	rotationAudit        domain.AttachedWorkerAuditEvent
	entitlement          domain.EntitlementState
}

func (s *onboardingStore) CreateAttachedWorkerEnrollment(_ context.Context, e domain.AttachedWorkerEnrollment, _ domain.AttachedWorkerAuditEvent) error {
	s.enrollment = e
	return nil
}
func (s *onboardingStore) LoadAttachedWorkerEnrollment(_ context.Context, t domain.TenantID, o domain.UserID, id domain.AttachedWorkerEnrollmentID) (domain.AttachedWorkerEnrollment, bool, error) {
	return s.enrollment, s.enrollment.TenantID == t && s.enrollment.OwnerUserID == o && s.enrollment.ID == id, nil
}
func (s *onboardingStore) ClaimAttachedWorkerEnrollment(_ context.Context, m ports.AttachedWorkerClaimMutation) (ports.AttachedWorkerClaimResult, error) {
	s.claimCalls++
	if s.worker.ID == "" {
		s.worker = domain.AttachedWorker{TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, ID: s.enrollment.WorkerID, DisplayName: s.enrollment.DisplayName, IdentityPublicKey: append([]byte(nil), m.IdentityPublicKey...), EnrollmentGeneration: 1, ConnectionGeneration: 0, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOffline, Revision: 1, CreatedAt: onboardingTime, UpdatedAt: onboardingTime}
		s.enrollment.ConsumedAt = onboardingTime
		s.enrollment.Revision++
	}
	if s.worker.Revision != 1 || !bytes.Equal(s.worker.IdentityPublicKey, m.IdentityPublicKey) {
		return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerConsumed}, nil
	}
	return ports.AttachedWorkerClaimResult{Status: ports.AttachedWorkerClaimed, Worker: s.worker}, nil
}
func (s *onboardingStore) LoadAttachedWorker(_ context.Context, t domain.TenantID, o domain.UserID, id domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	return s.worker, s.worker.TenantID == t && s.worker.OwnerUserID == o && s.worker.ID == id, nil
}
func (s *onboardingStore) RegisterAttachedComputeResource(_ context.Context, r ports.AttachedComputeRegistration) (ports.ComputeConnectionState, error) {
	if s.registrationError != nil {
		return ports.ComputeConnectionState{}, s.registrationError
	}
	if r.Worker.Revision != s.worker.Revision || r.Worker.OwnerUserID != s.worker.OwnerUserID || !bytes.Equal(r.Worker.IdentityPublicKey, s.worker.IdentityPublicKey) {
		return ports.ComputeConnectionState{}, errors.New("stale registration")
	}
	entitlement := s.entitlement
	if entitlement == "" {
		entitlement = domain.EntitlementUnknown
	}
	return ports.ComputeConnectionState{ID: r.ResourceID, Provider: "codex", Entitlement: entitlement, Quota: domain.ProviderQuotaUnknown, ObservedAt: onboardingTime}, nil
}
func (s *onboardingStore) CompareAndSwapAttachedWorker(_ context.Context, m ports.AttachedWorkerCASMutation) (bool, error) {
	s.casCalls++
	if s.worker.Revision != m.ExpectedRevision {
		return false, nil
	}
	s.worker = m.Next
	s.rotationAudit = m.Audit
	if s.lostRotationResponse {
		return false, errors.New("lost committed CAS response")
	}
	return true, nil
}
func (s *onboardingStore) ReconcileAttachedWorkerRotation(_ context.Context, prior domain.AttachedWorker, key []byte) (domain.AttachedWorker, bool, error) {
	ok := s.worker.Revision == prior.Revision+1 && s.worker.EnrollmentGeneration == prior.EnrollmentGeneration+1 && s.worker.ConnectionGeneration == prior.ConnectionGeneration && bytes.Equal(s.worker.IdentityPublicKey, key) && s.rotationAudit.Action == domain.AttachedWorkerAuditIdentityRotated && s.worker.DesiredState == domain.AttachedWorkerDesiredActive
	return s.worker, ok, nil
}

func newOnboardingFixture(t *testing.T) (*Service, *onboardingStore, attachedworkeronboarding.ClaimV1, ed25519.PrivateKey) {
	t.Helper()
	store := &onboardingStore{}
	service, err := New(attachedworker.Config{Clock: fixtureClock{}, IDs: fixtureIDs{}, Random: bytes.NewReader(bytes.Repeat([]byte{7}, 32)), MaxEnrollmentTTL: 10 * time.Minute, EnrollmentRetention: time.Hour}, store)
	if err != nil {
		t.Fatal(err)
	}
	secret := bytes.Repeat([]byte{7}, 32)
	enrollment := domain.AttachedWorkerEnrollment{TenantID: "tenant", OwnerUserID: "owner", ID: "enrollment", WorkerID: "worker", DisplayName: "laptop", Audience: "worker-v1", BootstrapDigest: domain.DigestWorkerBootstrap(secret), CreatedAt: onboardingTime, ExpiresAt: onboardingTime.Add(5 * time.Minute), RetainUntil: onboardingTime.Add(65 * time.Minute), Revision: 1}
	store.enrollment = enrollment
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, 32))
	pub := key.Public().(ed25519.PublicKey)
	transcript, err := attachedworker.ClaimProofTranscript(enrollment, 1, pub)
	if err != nil {
		t.Fatal(err)
	}
	return service, store, attachedworkeronboarding.ClaimV1{Version: 1, Enrollment: enrollment, BootstrapSecret: secret, IdentityPublicKey: pub, Proof: ed25519.Sign(key, transcript)}, key
}

func TestOnboardingClaimRequiresRegistrationAndReusesPristineReplay(t *testing.T) {
	service, store, claim, _ := newOnboardingFixture(t)
	store.registrationError = errors.New("registration transaction unavailable")
	if receipt, err := service.Claim(context.Background(), claim); err == nil || receipt.Version != 0 {
		t.Fatalf("failed registration produced receipt: %#v, %v", receipt, err)
	}
	store.registrationError = nil
	receipt, err := service.Claim(context.Background(), claim)
	if err != nil || receipt.Validate() != nil || receipt.Worker.Revision != 1 || receipt.Entitlement != domain.EntitlementUnknown || receipt.Quota != domain.ProviderQuotaUnknown {
		t.Fatalf("exact pristine retry: %#v, %v", receipt, err)
	}
	if store.claimCalls != 2 {
		t.Fatalf("claim calls=%d, want 2 delegated attempts", store.claimCalls)
	}
}

func TestOnboardingClaimRejectsChangedPersistedGrantBeforeMutation(t *testing.T) {
	service, store, claim, _ := newOnboardingFixture(t)
	store.enrollment.DisplayName = "different immutable grant"
	if _, err := service.Claim(context.Background(), claim); !errors.Is(err, attachedworker.ErrEnrollmentDenied) {
		t.Fatalf("changed persisted grant: %v", err)
	}
	if store.claimCalls != 0 {
		t.Fatal("changed grant reached claim mutation")
	}
}

func rotationFixture(t *testing.T) (*Service, *onboardingStore, attachedworkeronboarding.RotationV1) {
	t.Helper()
	service, store, claim, currentKey := newOnboardingFixture(t)
	receipt, err := service.Claim(context.Background(), claim)
	if err != nil {
		t.Fatal(err)
	}
	nextKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{11}, 32))
	pub := nextKey.Public().(ed25519.PublicKey)
	transcript, err := attachedworker.RotationProofTranscript(receipt.Worker, receipt.Worker.Revision, pub)
	if err != nil {
		t.Fatal(err)
	}
	request := attachedworkeronboarding.RotationV1{Version: 1, Worker: receipt.Worker, NewPublicKey: pub, CurrentProof: ed25519.Sign(currentKey, transcript), NewProof: ed25519.Sign(nextKey, transcript)}
	return service, store, request
}

func TestOnboardingRotationReconcilesLostResponseOnlyAtExactNextHead(t *testing.T) {
	service, store, request := rotationFixture(t)
	store.lostRotationResponse = true
	receipt, err := service.Rotate(context.Background(), request)
	if err != nil || receipt.Validate() != nil || receipt.Worker.Revision != 2 || receipt.Worker.EnrollmentGeneration != 2 {
		t.Fatalf("lost committed response reconciliation: %#v %v", receipt, err)
	}
	replay, err := service.Rotate(context.Background(), request)
	if err != nil || replay.Worker.Revision != 2 || store.casCalls != 1 {
		t.Fatalf("rotation replay: %#v %v CAS=%d, want one", replay, err, store.casCalls)
	}
	store.worker.Revision++
	if _, err := service.Rotate(context.Background(), request); err == nil {
		t.Fatal("later worker head accepted as same rotation")
	}
}

func TestOnboardingRotationDoesNotAlterActivatedProviderResource(t *testing.T) {
	service, store, request := rotationFixture(t)
	store.entitlement = domain.EntitlementActive
	if _, err := service.Rotate(context.Background(), request); err == nil {
		t.Fatal("activated provider resource rotated through disabled onboarding")
	}
	if store.casCalls != 0 || store.worker.Revision != 1 {
		t.Fatal("provider-state denial occurred after identity mutation")
	}
}

type frozenOnboardingClock struct{ at time.Time }

func (c frozenOnboardingClock) Now() time.Time { return c.at }

func TestOnboardingRotationDeniesEqualOrRegressedTimeBeforeCAS(t *testing.T) {
	for _, test := range []struct {
		name  string
		delta time.Duration
	}{{name: "equal"}, {name: "regressed", delta: -time.Microsecond}} {
		t.Run(test.name, func(t *testing.T) {
			service, store, first := rotationFixture(t)
			receipt, err := service.Rotate(context.Background(), first)
			if err != nil {
				t.Fatal(err)
			}
			priorKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{11}, 32))
			nextKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{13}, 32))
			pub := nextKey.Public().(ed25519.PublicKey)
			transcript, err := attachedworker.RotationProofTranscript(receipt.Worker, receipt.Worker.Revision, pub)
			if err != nil {
				t.Fatal(err)
			}
			second := attachedworkeronboarding.RotationV1{Version: 1, Worker: receipt.Worker, NewPublicKey: pub, CurrentProof: ed25519.Sign(priorKey, transcript), NewProof: ed25519.Sign(nextKey, transcript)}
			service, err = New(attachedworker.Config{Clock: frozenOnboardingClock{at: receipt.Worker.UpdatedAt.Add(test.delta)}, IDs: fixtureIDs{}, MaxEnrollmentTTL: time.Hour, EnrollmentRetention: time.Hour}, store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Rotate(context.Background(), second); !errors.Is(err, attachedworker.ErrWorkerConflict) {
				t.Fatalf("%s clock: error=%v, want conflict", test.name, err)
			}
			if store.casCalls != 1 || store.worker.Revision != 2 || !bytes.Equal(store.worker.IdentityPublicKey, receipt.Worker.IdentityPublicKey) {
				t.Fatalf("non-advancing time mutated key: CAS=%d revision=%d", store.casCalls, store.worker.Revision)
			}
			if replay, err := service.Rotate(context.Background(), first); err != nil || replay.Worker.Revision != 2 {
				t.Fatalf("prior committed request no longer reconciles with non-advancing clock: rev=%d err=%v", replay.Worker.Revision, err)
			}
		})
	}
}

func TestPrepareAndPersistOnboardingGrantDelegatesExactSecretDigest(t *testing.T) {
	service, store, _, _ := newOnboardingFixture(t)
	store.enrollment = domain.AttachedWorkerEnrollment{}
	grant, err := service.PrepareGrant(context.Background(), "tenant", "owner", attachedworker.CreateEnrollmentRequest{DisplayName: "laptop", Audience: "worker-v1", ExpiresAt: fixtureClock{}.Now().Add(5 * time.Minute)}, "https://control.example")
	if err != nil || grant.Validate() != nil {
		t.Fatalf("prepare: %#v %v", grant, err)
	}
	if store.enrollment.ID != "" {
		t.Fatal("prepare mutated server enrollment")
	}
	if err := service.PersistGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	if store.enrollment.ID != grant.Enrollment.ID || store.enrollment.BootstrapDigest != domain.DigestWorkerBootstrap(grant.BootstrapSecret) {
		t.Fatal("persist replaced grant identity/digest")
	}
}
