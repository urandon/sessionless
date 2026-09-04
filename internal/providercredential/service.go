package providercredential

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrInvalid  = &Failure{Code: FailureInvalidRequestV1}
	ErrNotFound = &Failure{Code: FailureNotFoundV1}
	ErrConflict = &Failure{Code: FailureConflictV1}
	ErrBackend  = &Failure{Code: FailureBackendUnavailableV1}
)

type FailureCodeV1 string

const (
	FailureInvalidRequestV1     FailureCodeV1 = "invalid_request"
	FailureNotFoundV1           FailureCodeV1 = "not_found"
	FailureConflictV1           FailureCodeV1 = "conflict"
	FailureBackendUnavailableV1 FailureCodeV1 = "backend_unavailable"
	FailureExpiredV1            FailureCodeV1 = "expired"
	FailureConsumedV1           FailureCodeV1 = "consumed"
	FailureDeliveryMismatchV1   FailureCodeV1 = "delivery_mismatch"
)

type Failure struct{ Code FailureCodeV1 }

func (failure *Failure) Error() string { return string(failure.Code) }
func (failure *Failure) Is(target error) bool {
	other, ok := target.(*Failure)
	return ok && failure != nil && other != nil && failure.Code == other.Code
}

type Config struct {
	Clock          ports.Clock
	IDs            ports.IDGenerator
	MaxSecretBytes int64
	CleanupBatch   uint64
}

type Service struct {
	clock        ports.Clock
	ids          ports.IDGenerator
	maxBytes     int64
	cleanupBatch uint64
	bindings     ports.ProviderCredentialBindingStore
	secrets      ports.ProviderCredentialSecretStore
	authorizer   ports.ProviderCredentialMutationAuthorizer
	revoker      ports.ProviderCredentialInvocationRevokerV1
}

func New(config Config, authorizer ports.ProviderCredentialMutationAuthorizer, revoker ports.ProviderCredentialInvocationRevokerV1, bindings ports.ProviderCredentialBindingStore, secrets ports.ProviderCredentialSecretStore) (*Service, error) {
	if config.Clock == nil || config.IDs == nil || authorizer == nil || revoker == nil || bindings == nil || secrets == nil {
		return nil, errors.New("provider credential dependencies must not be nil")
	}
	if config.MaxSecretBytes == 0 {
		config.MaxSecretBytes = 8 << 10
	}
	if config.CleanupBatch == 0 {
		config.CleanupBatch = 16
	}
	if config.MaxSecretBytes < 1 || config.MaxSecretBytes > 64<<10 || config.CleanupBatch > 64 {
		return nil, errors.New("provider credential bounds are invalid")
	}
	return &Service{clock: config.Clock, ids: config.IDs, maxBytes: config.MaxSecretBytes, cleanupBatch: config.CleanupBatch, bindings: bindings, secrets: secrets, authorizer: authorizer, revoker: revoker}, nil
}

func (service *Service) Ingest(ctx context.Context, request ports.ProviderCredentialIngestRequestV1, secret []byte) (ports.ProviderCredentialReceiptV1, error) {
	if ctx == nil || request.Validate() != nil || !validSecret(secret, service.maxBytes) {
		return ports.ProviderCredentialReceiptV1{}, ErrInvalid
	}
	authorized, err := service.authorizer.AuthorizeProviderCredentialMutation(ctx, request.Principal, ports.ProviderCredentialOperationIngestV1)
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if !authorized {
		return ports.ProviderCredentialReceiptV1{}, ErrNotFound
	}
	locator := request.Locator()
	owned := append([]byte(nil), secret...)
	defer zero(owned)
	fingerprint := domain.FingerprintCredential(owned)
	current, found, err := service.bindings.LoadProviderCredential(ctx, locator)
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if found {
		if err := validateBindingForLocator(current, locator); err != nil {
			return ports.ProviderCredentialReceiptV1{}, ErrBackend
		}
	}
	if err := service.reconcileCandidates(ctx, locator, current, found); err != nil {
		return ports.ProviderCredentialReceiptV1{}, err
	}
	if found {
		if current.State == domain.ProviderCredentialActiveV1 && current.SecretFingerprint == fingerprint {
			if current.ResourceRevision == 0 {
				return ports.ProviderCredentialReceiptV1{}, ErrBackend
			}
			reconciled, err := service.bindings.CompareAndSwapProviderCredential(ctx, current.ResourceRevision-1, current)
			if err != nil || !reconciled.Applied || !reconciled.Found || validateExactBinding(reconciled.Binding, current) != nil || reconciled.AuditReceiptID == "" {
				return ports.ProviderCredentialReceiptV1{}, ErrBackend
			}
			if err := service.revoker.FenceProviderCredentialInvocations(ctx, locator, current.CredentialGeneration); err != nil {
				return ports.ProviderCredentialReceiptV1{}, ErrBackend
			}
			if err := service.drainCleanups(ctx, locator); err != nil {
				return ports.ProviderCredentialReceiptV1{}, err
			}
			return receipt(current, ports.ProviderCredentialReplayedV1, reconciled.AuditReceiptID)
		}
		if current.ResourceRevision == math.MaxUint64 || current.CredentialGeneration == math.MaxUint64 {
			return ports.ProviderCredentialReceiptV1{}, ErrConflict
		}
	}
	revision, generation := uint64(1), uint64(1)
	expectedRevision := uint64(0)
	if found {
		expectedRevision = current.ResourceRevision
		revision, generation = current.ResourceRevision+1, current.CredentialGeneration+1
	}
	mutationID, err := service.ids.NewID(ctx, ports.IDProviderCredentialMutation)
	if err != nil || domain.ValidateOpaqueID("provider_credential.mutation_id", mutationID) != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	scope := ports.ProviderCredentialCandidateScopeV1{Locator: locator, ResourceRevision: revision, CredentialGeneration: generation, MutationID: mutationID}
	candidate, err := service.secrets.PutProviderCredentialCandidate(ctx, scope, owned)
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if candidate.Scope != scope || candidate.Reference.Validate() != nil || candidate.Fingerprint != fingerprint || candidate.CreatedAt.IsZero() {
		_ = service.secrets.DeleteProviderCredentialCandidate(ctx, candidate)
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	next := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: locator.TenantID,
		OwnerUserID: locator.OwnerUserID, ResourceKind: locator.ResourceKind, ResourceID: locator.ResourceID,
		ResourceRevision: revision, CredentialGeneration: generation, State: domain.ProviderCredentialActiveV1,
		CandidateMutationID: mutationID,
		SecretRef:           candidate.Reference, SecretFingerprint: candidate.Fingerprint, UpdatedAt: canonicalTime(service.clock.Now()),
	}
	if err := next.Validate(); err != nil {
		_ = service.secrets.DeleteProviderCredentialCandidate(ctx, candidate)
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	swap, err := service.bindings.CompareAndSwapProviderCredential(ctx, expectedRevision, next)
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if !swap.Applied {
		_ = service.secrets.DeleteProviderCredentialCandidate(ctx, candidate)
		return ports.ProviderCredentialReceiptV1{}, ErrConflict
	}
	if err := validateExactBinding(swap.Binding, next); err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if err := service.revoker.FenceProviderCredentialInvocations(ctx, locator, next.CredentialGeneration); err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if err := service.secrets.CommitProviderCredentialCandidate(ctx, candidate); err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if err := service.drainCleanups(ctx, locator); err != nil {
		return ports.ProviderCredentialReceiptV1{}, err
	}
	return receipt(next, ports.ProviderCredentialAppliedV1, swap.AuditReceiptID)
}

func (service *Service) Revoke(ctx context.Context, request ports.ProviderCredentialRevokeRequestV1) (ports.ProviderCredentialReceiptV1, error) {
	if ctx == nil || request.Validate() != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrInvalid
	}
	authorized, err := service.authorizer.AuthorizeProviderCredentialMutation(ctx, request.Principal, ports.ProviderCredentialOperationRevokeV1)
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if !authorized {
		return ports.ProviderCredentialReceiptV1{}, ErrNotFound
	}
	locator := request.Locator()
	swap, err := service.bindings.RevokeProviderCredential(ctx, locator, canonicalTime(service.clock.Now()))
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if !swap.Found {
		return ports.ProviderCredentialReceiptV1{}, ErrNotFound
	}
	if err := service.revoker.FenceProviderCredentialInvocations(ctx, locator, swap.Binding.CredentialGeneration); err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if err := validateBindingForLocator(swap.Binding, locator); err != nil || swap.Binding.State != domain.ProviderCredentialRevokedV1 {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if err := service.drainCleanups(ctx, locator); err != nil {
		return ports.ProviderCredentialReceiptV1{}, err
	}
	status := ports.ProviderCredentialReplayedV1
	if swap.Applied {
		status = ports.ProviderCredentialAppliedV1
	}
	return receipt(swap.Binding, status, swap.AuditReceiptID)
}

func (service *Service) reconcileCandidates(ctx context.Context, locator ports.ProviderCredentialLocatorV1, current domain.ProviderCredentialBindingV1, found bool) error {
	// Re-list after each bounded batch so a burst of crash-orphaned candidates
	// cannot hide behind the first page and become a second secret authority.
	// Four batches keep one mutation bounded; a later retry can continue cleanup.
	for batch := 0; batch < 4; batch++ {
		candidates, err := service.secrets.ListUncommittedProviderCredentialCandidates(ctx, locator, service.cleanupBatch)
		if err != nil {
			return ErrBackend
		}
		if len(candidates) == 0 {
			return nil
		}
		for _, candidate := range candidates {
			if candidate.Scope.Locator != locator || candidate.Scope.ResourceRevision == 0 || candidate.Scope.CredentialGeneration == 0 || domain.ValidateOpaqueID("provider_credential.mutation_id", candidate.Scope.MutationID) != nil || candidate.Reference.Validate() != nil || candidate.Fingerprint.Validate() != nil || candidate.CreatedAt.IsZero() {
				return ErrBackend
			}
			if found && current.State == domain.ProviderCredentialActiveV1 && candidate.Scope.ResourceRevision == current.ResourceRevision && candidate.Scope.CredentialGeneration == current.CredentialGeneration && candidate.Scope.MutationID == current.CandidateMutationID && candidate.Reference == current.SecretRef && candidate.Fingerprint == current.SecretFingerprint {
				recovered, err := service.secrets.RecoverProviderCredentialCandidate(ctx, current)
				if err != nil || !recovered {
					return ErrBackend
				}
				continue
			}
			fence, err := service.bindings.FenceProviderCredentialCandidate(ctx, candidate, canonicalTime(service.clock.Now()))
			if err != nil {
				return ErrBackend
			}
			if fence.Authoritative {
				recovered, err := service.secrets.RecoverProviderCredentialCandidate(ctx, fence.Binding)
				if err != nil || !recovered {
					return ErrBackend
				}
				continue
			}
			if err := service.secrets.DeleteProviderCredentialCandidate(ctx, candidate); err != nil {
				return ErrBackend
			}
		}
	}
	return ErrBackend
}

// DrainAbandonedCandidates is the bounded autonomous recovery surface for a
// secret backend candidate written before a process stopped or a binding CAS
// rolled back. The age cutoff is supplied by a trusted cleanup worker so an
// in-flight ingestion cannot be mistaken for abandoned plaintext.
func (service *Service) DrainAbandonedCandidates(ctx context.Context, before time.Time, cursor ports.ProviderCredentialCandidateCursorV1) (ports.ProviderCredentialCandidatePageV1, error) {
	if ctx == nil || before.IsZero() {
		return ports.ProviderCredentialCandidatePageV1{}, ErrInvalid
	}
	page, err := service.secrets.ListAbandonedProviderCredentialCandidates(ctx, canonicalTime(before), cursor, service.cleanupBatch)
	if err != nil {
		return ports.ProviderCredentialCandidatePageV1{}, ErrBackend
	}
	for _, candidate := range page.Items {
		locator := candidate.Scope.Locator
		if locator.Validate() != nil || candidate.Scope.ResourceRevision == 0 || candidate.Scope.CredentialGeneration == 0 || domain.ValidateOpaqueID("provider_credential.mutation_id", candidate.Scope.MutationID) != nil || candidate.Reference.Validate() != nil || candidate.Fingerprint.Validate() != nil || candidate.CreatedAt.IsZero() || !candidate.CreatedAt.Before(before) {
			return ports.ProviderCredentialCandidatePageV1{}, ErrBackend
		}
		fence, err := service.bindings.FenceProviderCredentialCandidate(ctx, candidate, canonicalTime(service.clock.Now()))
		if err != nil {
			return ports.ProviderCredentialCandidatePageV1{}, ErrBackend
		}
		if fence.Authoritative {
			recovered, err := service.secrets.RecoverProviderCredentialCandidate(ctx, fence.Binding)
			if err != nil || !recovered {
				return ports.ProviderCredentialCandidatePageV1{}, ErrBackend
			}
			continue
		}
		if err := service.secrets.DeleteProviderCredentialCandidate(ctx, candidate); err != nil {
			return ports.ProviderCredentialCandidatePageV1{}, ErrBackend
		}
	}
	return page, nil
}

func (service *Service) drainCleanups(ctx context.Context, locator ports.ProviderCredentialLocatorV1) error {
	cleanups, err := service.bindings.ListProviderCredentialCleanups(ctx, locator, service.cleanupBatch)
	if err != nil {
		return ErrBackend
	}
	for _, cleanup := range cleanups {
		if cleanup.Locator != locator || cleanup.CredentialGeneration == 0 || cleanup.Reference.Validate() != nil {
			return ErrBackend
		}
		if err := service.secrets.DeleteProviderCredentialSecret(ctx, cleanup); err != nil {
			return ErrBackend
		}
		if err := service.bindings.AcknowledgeProviderCredentialCleanup(ctx, cleanup); err != nil {
			return ErrBackend
		}
	}
	return nil
}

// DrainDueCleanups is the bounded internal recovery surface used by an
// operator or cleanup worker. It has no public owner-facing response and never
// reads credential plaintext; deletion by opaque reference must be idempotent.
func (service *Service) DrainDueCleanups(ctx context.Context, bucket uint32, before time.Time, cursor ports.ProviderCredentialCleanupCursorV1) (ports.ProviderCredentialCleanupPageV1, error) {
	if ctx == nil {
		return ports.ProviderCredentialCleanupPageV1{}, ErrInvalid
	}
	page, err := service.bindings.ListDueProviderCredentialCleanups(ctx, bucket, before, cursor, service.cleanupBatch)
	if err != nil {
		return ports.ProviderCredentialCleanupPageV1{}, ErrBackend
	}
	for _, item := range page.Items {
		if err := service.secrets.DeleteProviderCredentialSecret(ctx, item.Cleanup); err != nil {
			return ports.ProviderCredentialCleanupPageV1{}, ErrBackend
		}
		if err := service.bindings.AcknowledgeProviderCredentialCleanup(ctx, item.Cleanup); err != nil {
			return ports.ProviderCredentialCleanupPageV1{}, ErrBackend
		}
	}
	return page, nil
}

func receipt(binding domain.ProviderCredentialBindingV1, status ports.ProviderCredentialMutationStatusV1, auditReceiptID domain.ProviderCredentialAuditReceiptIDV1) (ports.ProviderCredentialReceiptV1, error) {
	resource, err := binding.AuthorityResourceBinding()
	if err != nil {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	if auditReceiptID == "" {
		return ports.ProviderCredentialReceiptV1{}, ErrBackend
	}
	return ports.ProviderCredentialReceiptV1{Status: status, Resource: resource, Fingerprint: binding.SecretFingerprint, Revoked: binding.State == domain.ProviderCredentialRevokedV1, UpdatedAt: binding.UpdatedAt, AuditReceiptID: auditReceiptID}, nil
}

func validateBindingForLocator(binding domain.ProviderCredentialBindingV1, locator ports.ProviderCredentialLocatorV1) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	if binding.TenantID != locator.TenantID || binding.OwnerUserID != locator.OwnerUserID || binding.ResourceKind != locator.ResourceKind || binding.ResourceID != locator.ResourceID {
		return ErrBackend
	}
	return nil
}

func validateExactBinding(actual, expected domain.ProviderCredentialBindingV1) error {
	if err := actual.Validate(); err != nil || actual != expected {
		return ErrBackend
	}
	return nil
}

func validSecret(secret []byte, max int64) bool {
	if len(secret) == 0 || int64(len(secret)) > max || !bytes.Equal(secret, bytes.TrimSpace(secret)) {
		return false
	}
	return !bytes.ContainsAny(secret, "\x00\r\n")
}

func zero(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func canonicalTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}
