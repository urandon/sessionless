// Package attachedworker implements the owner-scoped attached-worker
// enrollment and identity lifecycle. It deliberately contains no transport,
// capability, provider, credential, or dispatch concerns.
package attachedworker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	bootstrapSecretBytes = 32
	maxWorkerListLimit   = 100
	claimProofDomain     = "sessionless.attached-worker.enrollment-proof.v1"
	rotationProofDomain  = "sessionless.attached-worker.identity-rotation-proof.v1"
)

var (
	ErrEnrollmentDenied   = errors.New("attached worker enrollment is not authorized")
	ErrEnrollmentExpired  = errors.New("attached worker enrollment has expired")
	ErrEnrollmentConsumed = errors.New("attached worker enrollment has already been consumed")
	ErrInvalidProof       = errors.New("attached worker identity proof is invalid")
	ErrWorkerNotFound     = errors.New("attached worker was not found")
	ErrWorkerConflict     = errors.New("attached worker revision is stale")
	ErrWorkerRevoked      = errors.New("attached worker is revoked")
	ErrBackend            = errors.New("attached worker backend operation failed")
)

type Config struct {
	Clock               ports.Clock
	IDs                 ports.IDGenerator
	Random              io.Reader
	MaxEnrollmentTTL    time.Duration
	EnrollmentRetention time.Duration
}

type Service struct {
	clock     ports.Clock
	ids       ports.IDGenerator
	random    io.Reader
	maxTTL    time.Duration
	retention time.Duration
	store     ports.AttachedWorkerStore
}

func New(config Config, store ports.AttachedWorkerStore) (*Service, error) {
	if config.Clock == nil || config.IDs == nil || store == nil {
		return nil, errors.New("attached worker dependencies must not be nil")
	}
	if config.MaxEnrollmentTTL <= 0 || config.EnrollmentRetention <= 0 {
		return nil, errors.New("attached worker enrollment TTL and retention must be positive")
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	return &Service{
		clock: config.Clock, ids: config.IDs, random: config.Random,
		maxTTL: config.MaxEnrollmentTTL, retention: config.EnrollmentRetention, store: store,
	}, nil
}

// BootstrapSecret is transient bootstrap material. Its formatting and JSON
// representations are always redacted; Bytes is an explicit delivery boundary.
// Persistence ports receive only Digest.
type BootstrapSecret struct {
	value [bootstrapSecretBytes]byte
}

func ParseBootstrapSecret(value []byte) (BootstrapSecret, error) {
	if len(value) != bootstrapSecretBytes {
		return BootstrapSecret{}, ErrEnrollmentDenied
	}
	var secret BootstrapSecret
	copy(secret.value[:], value)
	return secret, nil
}

func (secret BootstrapSecret) Bytes() []byte {
	return append([]byte(nil), secret.value[:]...)
}

func (secret BootstrapSecret) Digest() domain.WorkerBootstrapDigest {
	return domain.DigestWorkerBootstrap(secret.value[:])
}

func (BootstrapSecret) String() string   { return "[REDACTED]" }
func (BootstrapSecret) GoString() string { return "[REDACTED]" }
func (BootstrapSecret) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

type CreateEnrollmentRequest struct {
	DisplayName string
	Audience    string
	ExpiresAt   time.Time
}

type EnrollmentGrant struct {
	Enrollment domain.AttachedWorkerEnrollment
	Secret     BootstrapSecret
}

func (service *Service) CreateEnrollment(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request CreateEnrollmentRequest,
) (EnrollmentGrant, error) {
	now := canonicalPersistenceTime(service.clock.Now())
	if err := validateOwnerScope(tenantID, ownerUserID); err != nil {
		return EnrollmentGrant{}, err
	}
	request.ExpiresAt = canonicalPersistenceTime(request.ExpiresAt)
	if !request.ExpiresAt.After(now) || request.ExpiresAt.After(now.Add(service.maxTTL)) {
		return EnrollmentGrant{}, domain.ValidationError{Field: "attached_worker_enrollment.expires_at", Reason: "must be future and within the configured short TTL"}
	}
	workerValue, err := service.ids.NewID(ctx, ports.IDAttachedWorker)
	if err != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	enrollmentValue, err := service.ids.NewID(ctx, ports.IDAttachedWorkerEnrollment)
	if err != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	workerID := domain.AttachedWorkerID(workerValue)
	enrollmentID := domain.AttachedWorkerEnrollmentID(enrollmentValue)
	if workerID.Validate() != nil || enrollmentID.Validate() != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	var secret BootstrapSecret
	if _, err := io.ReadFull(service.random, secret.value[:]); err != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	enrollment := domain.AttachedWorkerEnrollment{
		TenantID: tenantID, OwnerUserID: ownerUserID, ID: enrollmentID, WorkerID: workerID,
		DisplayName: request.DisplayName, Audience: request.Audience, BootstrapDigest: secret.Digest(),
		ExpiresAt: request.ExpiresAt, RetainUntil: canonicalPersistenceTime(request.ExpiresAt.Add(service.retention)),
		CreatedAt: now, Revision: 1,
	}
	if err := enrollment.Validate(); err != nil {
		return EnrollmentGrant{}, err
	}
	audit := domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: workerID, EnrollmentID: enrollmentID,
		Action: domain.AttachedWorkerAuditEnrollmentCreated, OccurredAt: now,
	}
	if err := audit.Validate(); err != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	if err := service.store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
		return EnrollmentGrant{}, ErrBackend
	}
	return EnrollmentGrant{Enrollment: enrollment, Secret: secret}, nil
}

type ClaimRequest struct {
	EnrollmentID               domain.AttachedWorkerEnrollmentID
	ExpectedEnrollmentRevision uint64
	Audience                   string
	BootstrapSecret            BootstrapSecret
	IdentityPublicKey          []byte
	Proof                      []byte
}

func (service *Service) Claim(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request ClaimRequest,
) (domain.AttachedWorker, error) {
	if err := validateOwnerScope(tenantID, ownerUserID); err != nil {
		return domain.AttachedWorker{}, err
	}
	if err := request.EnrollmentID.Validate(); err != nil {
		return domain.AttachedWorker{}, err
	}
	enrollment, found, err := service.store.LoadAttachedWorkerEnrollment(ctx, tenantID, ownerUserID, request.EnrollmentID)
	if err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	if !found {
		return domain.AttachedWorker{}, ErrEnrollmentDenied
	}
	if enrollment.Validate() != nil || enrollment.TenantID != tenantID || enrollment.OwnerUserID != ownerUserID || enrollment.ID != request.EnrollmentID {
		return domain.AttachedWorker{}, ErrBackend
	}
	presentedDigest := request.BootstrapSecret.Digest()
	if request.Audience != enrollment.Audience || !equalDigest(presentedDigest, enrollment.BootstrapDigest) {
		return domain.AttachedWorker{}, ErrEnrollmentDenied
	}
	if request.ExpectedEnrollmentRevision == 0 || request.ExpectedEnrollmentRevision == math.MaxUint64 ||
		(enrollment.ConsumedAt.IsZero() && enrollment.Revision != request.ExpectedEnrollmentRevision) ||
		(!enrollment.ConsumedAt.IsZero() && enrollment.Revision != request.ExpectedEnrollmentRevision+1) {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	transcript, err := ClaimProofTranscript(enrollment, request.ExpectedEnrollmentRevision, request.IdentityPublicKey)
	if err != nil || len(request.Proof) != ed25519.SignatureSize ||
		!ed25519.Verify(ed25519.PublicKey(request.IdentityPublicKey), transcript, request.Proof) {
		return domain.AttachedWorker{}, ErrInvalidProof
	}
	mutation := ports.AttachedWorkerClaimMutation{
		TenantID: tenantID, OwnerUserID: ownerUserID, EnrollmentID: enrollment.ID,
		ExpectedEnrollmentRevision: request.ExpectedEnrollmentRevision, PresentedAudience: request.Audience,
		PresentedDigest: presentedDigest, IdentityPublicKey: cloneBytes(request.IdentityPublicKey),
	}
	result, err := service.store.ClaimAttachedWorkerEnrollment(ctx, mutation)
	if err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	switch result.Status {
	case ports.AttachedWorkerClaimed:
		if result.Worker.Validate() != nil || result.Worker.TenantID != tenantID || result.Worker.OwnerUserID != ownerUserID ||
			result.Worker.ID != enrollment.WorkerID || result.Worker.DisplayName != enrollment.DisplayName ||
			subtle.ConstantTimeCompare(result.Worker.IdentityPublicKey, request.IdentityPublicKey) != 1 ||
			result.Worker.Revision != 1 || result.Worker.EnrollmentGeneration != 1 || result.Worker.ConnectionGeneration != 0 ||
			result.Worker.DesiredState != domain.AttachedWorkerDesiredActive ||
			result.Worker.ObservedState != domain.AttachedWorkerObservedOffline ||
			!result.Worker.CreatedAt.Equal(result.Worker.UpdatedAt) || !result.Worker.RevokedAt.IsZero() {
			return domain.AttachedWorker{}, ErrBackend
		}
		return cloneWorker(result.Worker), nil
	case ports.AttachedWorkerExpired:
		return domain.AttachedWorker{}, ErrEnrollmentExpired
	case ports.AttachedWorkerConsumed:
		return domain.AttachedWorker{}, ErrEnrollmentConsumed
	case ports.AttachedWorkerConflict:
		return domain.AttachedWorker{}, ErrWorkerConflict
	case ports.AttachedWorkerDenied:
		return domain.AttachedWorker{}, ErrEnrollmentDenied
	default:
		return domain.AttachedWorker{}, ErrBackend
	}
}

func ClaimProofTranscript(
	enrollment domain.AttachedWorkerEnrollment,
	expectedEnrollmentRevision uint64,
	publicKey []byte,
) ([]byte, error) {
	if err := enrollment.Validate(); err != nil {
		return nil, err
	}
	if expectedEnrollmentRevision == 0 || expectedEnrollmentRevision == math.MaxUint64 {
		return nil, domain.ValidationError{Field: "attached_worker_claim.expected_enrollment_revision", Reason: "must be bounded and positive"}
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, domain.ValidationError{Field: "attached_worker.identity_public_key", Reason: "must be an Ed25519 public key"}
	}
	return lengthPrefixedTranscript(
		[]byte(claimProofDomain), []byte(enrollment.TenantID), []byte(enrollment.OwnerUserID),
		[]byte(enrollment.ID), []byte(enrollment.WorkerID), []byte(enrollment.Audience),
		[]byte(enrollment.BootstrapDigest), publicKey, uint64Bytes(expectedEnrollmentRevision),
		int64Bytes(enrollment.ExpiresAt.UnixNano()),
	), nil
}

type RenameRequest struct {
	WorkerID         domain.AttachedWorkerID
	ExpectedRevision uint64
	DisplayName      string
}

func (service *Service) Rename(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request RenameRequest,
) (domain.AttachedWorker, error) {
	return service.mutateWorker(ctx, tenantID, ownerUserID, request.WorkerID, request.ExpectedRevision,
		domain.AttachedWorkerAuditWorkerRenamed, func(worker *domain.AttachedWorker, _ time.Time) error {
			worker.DisplayName = request.DisplayName
			return nil
		})
}

type RotateIdentityRequest struct {
	WorkerID         domain.AttachedWorkerID
	ExpectedRevision uint64
	NewPublicKey     []byte
	CurrentProof     []byte
	NewProof         []byte
}

func (service *Service) RotateIdentity(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request RotateIdentityRequest,
) (domain.AttachedWorker, error) {
	return service.mutateWorker(ctx, tenantID, ownerUserID, request.WorkerID, request.ExpectedRevision,
		domain.AttachedWorkerAuditIdentityRotated, func(worker *domain.AttachedWorker, _ time.Time) error {
			if len(request.NewPublicKey) != ed25519.PublicKeySize ||
				subtle.ConstantTimeCompare(worker.IdentityPublicKey, request.NewPublicKey) == 1 {
				return ErrInvalidProof
			}
			transcript, err := RotationProofTranscript(*worker, request.ExpectedRevision, request.NewPublicKey)
			if err != nil || len(request.CurrentProof) != ed25519.SignatureSize || len(request.NewProof) != ed25519.SignatureSize ||
				!ed25519.Verify(ed25519.PublicKey(worker.IdentityPublicKey), transcript, request.CurrentProof) ||
				!ed25519.Verify(ed25519.PublicKey(request.NewPublicKey), transcript, request.NewProof) {
				return ErrInvalidProof
			}
			worker.IdentityPublicKey = cloneBytes(request.NewPublicKey)
			worker.EnrollmentGeneration++
			return nil
		})
}

func RotationProofTranscript(worker domain.AttachedWorker, expectedRevision uint64, newPublicKey []byte) ([]byte, error) {
	if err := worker.Validate(); err != nil {
		return nil, err
	}
	if expectedRevision == 0 || len(newPublicKey) != ed25519.PublicKeySize {
		return nil, domain.ValidationError{Field: "attached_worker.rotation", Reason: "revision and new Ed25519 key are required"}
	}
	return lengthPrefixedTranscript(
		[]byte(rotationProofDomain), []byte(worker.TenantID), []byte(worker.OwnerUserID), []byte(worker.ID),
		uint64Bytes(expectedRevision), uint64Bytes(worker.EnrollmentGeneration),
		uint64Bytes(worker.ConnectionGeneration), newPublicKey,
	), nil
}

type WorkerRevisionRequest struct {
	WorkerID         domain.AttachedWorkerID
	ExpectedRevision uint64
}

func (service *Service) AdvanceConnectionGeneration(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request WorkerRevisionRequest,
) (domain.AttachedWorker, error) {
	return service.mutateWorker(ctx, tenantID, ownerUserID, request.WorkerID, request.ExpectedRevision,
		domain.AttachedWorkerAuditConnectionGenerationAdvanced, func(worker *domain.AttachedWorker, _ time.Time) error {
			worker.ConnectionGeneration++
			return nil
		})
}

func (service *Service) Revoke(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	request WorkerRevisionRequest,
) (domain.AttachedWorker, error) {
	worker, err := service.loadWorker(ctx, tenantID, ownerUserID, request.WorkerID)
	if err != nil {
		return domain.AttachedWorker{}, err
	}
	if request.ExpectedRevision == 0 || worker.Revision != request.ExpectedRevision {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
		return worker, nil
	}
	if worker.Revision == math.MaxUint64 || worker.EnrollmentGeneration == math.MaxUint64 || worker.ConnectionGeneration == math.MaxUint64 {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	now := canonicalPersistenceTime(service.clock.Now())
	next := cloneWorker(worker)
	next.DesiredState = domain.AttachedWorkerDesiredRevoked
	next.EnrollmentGeneration++
	next.ConnectionGeneration++
	next.Revision++
	next.UpdatedAt = now
	next.RevokedAt = now
	if err := next.Validate(); err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	audit := auditForWorker(next, domain.AttachedWorkerAuditWorkerRevoked, "", now)
	if err := audit.Validate(); err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	swapped, err := service.store.RevokeAttachedWorker(ctx, ports.AttachedWorkerRevokeMutation{
		TenantID: tenantID, OwnerUserID: ownerUserID, WorkerID: worker.ID,
		ExpectedRevision: worker.Revision, Next: next, Audit: audit, At: now,
	})
	if err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	if !swapped {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	return next, nil
}

func (service *Service) Get(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (domain.AttachedWorker, error) {
	return service.loadWorker(ctx, tenantID, ownerUserID, workerID)
}

func (service *Service) List(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	after domain.AttachedWorkerID,
	limit uint64,
) ([]domain.AttachedWorker, error) {
	if err := validateOwnerScope(tenantID, ownerUserID); err != nil {
		return nil, err
	}
	if after != "" {
		if err := after.Validate(); err != nil {
			return nil, err
		}
	}
	if limit == 0 || limit > maxWorkerListLimit {
		return nil, domain.ValidationError{Field: "attached_worker.list.limit", Reason: "must be between 1 and 100"}
	}
	workers, err := service.store.ListAttachedWorkers(ctx, tenantID, ownerUserID, after, limit)
	if err != nil {
		return nil, ErrBackend
	}
	if uint64(len(workers)) > limit {
		return nil, ErrBackend
	}
	result := make([]domain.AttachedWorker, len(workers))
	for index, worker := range workers {
		if worker.Validate() != nil || worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID ||
			worker.ID <= after || (index > 0 && worker.ID <= workers[index-1].ID) {
			return nil, ErrBackend
		}
		result[index] = cloneWorker(worker)
	}
	return result, nil
}

func (service *Service) mutateWorker(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
	expectedRevision uint64,
	action domain.AttachedWorkerAuditAction,
	mutate func(*domain.AttachedWorker, time.Time) error,
) (domain.AttachedWorker, error) {
	worker, err := service.loadWorker(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return domain.AttachedWorker{}, err
	}
	if worker.DesiredState == domain.AttachedWorkerDesiredRevoked {
		return domain.AttachedWorker{}, ErrWorkerRevoked
	}
	if expectedRevision == 0 || worker.Revision != expectedRevision {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	if worker.Revision == math.MaxUint64 ||
		(action == domain.AttachedWorkerAuditIdentityRotated && worker.EnrollmentGeneration == math.MaxUint64) ||
		(action == domain.AttachedWorkerAuditConnectionGenerationAdvanced && worker.ConnectionGeneration == math.MaxUint64) {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	now := canonicalPersistenceTime(service.clock.Now())
	next := cloneWorker(worker)
	if err := mutate(&next, now); err != nil {
		return domain.AttachedWorker{}, err
	}
	next.Revision++
	next.UpdatedAt = now
	if err := next.Validate(); err != nil {
		return domain.AttachedWorker{}, err
	}
	audit := auditForWorker(next, action, "", now)
	if err := audit.Validate(); err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	swapped, err := service.store.CompareAndSwapAttachedWorker(ctx, ports.AttachedWorkerCASMutation{
		ExpectedRevision: worker.Revision, Next: next, Audit: audit, At: now,
	})
	if err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	if !swapped {
		return domain.AttachedWorker{}, ErrWorkerConflict
	}
	return next, nil
}

func (service *Service) loadWorker(
	ctx context.Context,
	tenantID domain.TenantID,
	ownerUserID domain.UserID,
	workerID domain.AttachedWorkerID,
) (domain.AttachedWorker, error) {
	if err := validateOwnerScope(tenantID, ownerUserID); err != nil {
		return domain.AttachedWorker{}, err
	}
	if err := workerID.Validate(); err != nil {
		return domain.AttachedWorker{}, err
	}
	worker, found, err := service.store.LoadAttachedWorker(ctx, tenantID, ownerUserID, workerID)
	if err != nil {
		return domain.AttachedWorker{}, ErrBackend
	}
	if !found {
		return domain.AttachedWorker{}, ErrWorkerNotFound
	}
	if worker.Validate() != nil || worker.TenantID != tenantID || worker.OwnerUserID != ownerUserID || worker.ID != workerID {
		return domain.AttachedWorker{}, ErrBackend
	}
	return cloneWorker(worker), nil
}

func auditForWorker(worker domain.AttachedWorker, action domain.AttachedWorkerAuditAction, enrollmentID domain.AttachedWorkerEnrollmentID, at time.Time) domain.AttachedWorkerAuditEvent {
	return domain.AttachedWorkerAuditEvent{
		Version:  domain.AttachedWorkerAuditEventVersionV1,
		TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID,
		EnrollmentID: enrollmentID, Action: action, WorkerRevision: worker.Revision,
		EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration,
		OccurredAt: at,
	}
}

func validateOwnerScope(tenantID domain.TenantID, ownerUserID domain.UserID) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	return ownerUserID.Validate()
}

func sameAttachedWorker(left, right domain.AttachedWorker) bool {
	return left.TenantID == right.TenantID && left.OwnerUserID == right.OwnerUserID && left.ID == right.ID &&
		left.DisplayName == right.DisplayName &&
		subtle.ConstantTimeCompare(left.IdentityPublicKey, right.IdentityPublicKey) == 1 &&
		left.EnrollmentGeneration == right.EnrollmentGeneration && left.ConnectionGeneration == right.ConnectionGeneration &&
		left.DesiredState == right.DesiredState && left.ObservedState == right.ObservedState && left.Revision == right.Revision &&
		left.CreatedAt.Equal(right.CreatedAt) && left.UpdatedAt.Equal(right.UpdatedAt) && left.RevokedAt.Equal(right.RevokedAt)
}

func equalDigest(left, right domain.WorkerBootstrapDigest) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func cloneWorker(worker domain.AttachedWorker) domain.AttachedWorker {
	worker.IdentityPublicKey = cloneBytes(worker.IdentityPublicKey)
	return worker
}

func cloneBytes(value []byte) []byte { return append([]byte(nil), value...) }

func canonicalPersistenceTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func lengthPrefixedTranscript(fields ...[]byte) []byte {
	size := 0
	for _, field := range fields {
		size += 4 + len(field)
	}
	transcript := make([]byte, 0, size)
	var length [4]byte
	for _, field := range fields {
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		transcript = append(transcript, length[:]...)
		transcript = append(transcript, field...)
	}
	return transcript
}

func uint64Bytes(value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded[:]
}

func int64Bytes(value int64) []byte { return uint64Bytes(uint64(value)) }
