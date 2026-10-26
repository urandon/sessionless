package sessionlessharness

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxAttachedResourcePins = 64

var ErrAttachedResourceUnavailable = errors.New("exact owned attached resource is unavailable")

// AttachedResourcePin is an operator-reviewed immutable resource tuple, not a
// browser selector or a credential. Binding is a scope-free template: tenant,
// owner and resource are exact; run/attempt/placement digest must be absent.
// Enrollment rotation requires replacing the pin. Connection generations may
// advance under the existing authenticated reconnect contract.
type AttachedResourcePin struct {
	TenantID                 domain.TenantID                       `json:"tenant_id"`
	OwnerUserID              domain.UserID                         `json:"owner_user_id"`
	SubscriptionConnectionID domain.SubscriptionConnectionID       `json:"subscription_connection_id"`
	WorkerID                 domain.AttachedWorkerID               `json:"worker_id"`
	EnrollmentGeneration     uint64                                `json:"enrollment_generation"`
	CapabilityDigest         domain.AttachedWorkerCapabilityDigest `json:"capability_digest"`
	PolicyDigest             domain.AttachedWorkerPolicyDigest     `json:"policy_digest"`
	Binding                  domain.HarnessBindingV2               `json:"binding"`
}

type AttachedResourceBinderConfig struct {
	Enabled   bool                  `json:"enabled"`
	Resources []AttachedResourcePin `json:"resources"`
	// Generations is the SAME immutable operator registry installed in Store.
	// It never comes from browser input or a worker's reported evidence.
	Generations *attachedresourcegeneration.Registry `json:"-"`
}

// AttachedResourceAuthorityStore performs only exact owner-scoped point reads.
// Preflight never reserves work. AdmitDispatch rechecks live worker/connection
// authority and creates the offer with job/quota/lease in its one transaction.
type AttachedResourceAuthorityStore interface {
	LoadAttachedWorker(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, bool, error)
	LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error)
}

type attachedResourceKey struct {
	tenant domain.TenantID
	owner  domain.UserID
	id     domain.SubscriptionConnectionID
}

type AttachedResourceBinder struct {
	enabled         bool
	store           AttachedResourceAuthorityStore
	pins            map[attachedResourceKey]AttachedResourcePin
	generations     *attachedresourcegeneration.Registry
	generationHeads ports.AttachedResourceGenerationHeadStoreV1
}

func NewAttachedResourceBinder(config AttachedResourceBinderConfig, store AttachedResourceAuthorityStore) (*AttachedResourceBinder, error) {
	if store == nil || len(config.Resources) > maxAttachedResourcePins || config.Enabled && len(config.Resources) == 0 && config.Generations.Len() == 0 {
		return nil, ErrAttachedResourceUnavailable
	}
	binder := &AttachedResourceBinder{enabled: config.Enabled, store: store, pins: make(map[attachedResourceKey]AttachedResourcePin, len(config.Resources))}
	if config.Generations != nil {
		heads, ok := store.(ports.AttachedResourceGenerationHeadStoreV1)
		if !config.Enabled || !ok || config.Generations.Len() == 0 {
			return nil, ErrAttachedResourceUnavailable
		}
		binder.generations, binder.generationHeads = config.Generations, heads
	}
	workerKeys := make(map[struct {
		tenant domain.TenantID
		owner  domain.UserID
		worker domain.AttachedWorkerID
	}]bool)
	for _, configured := range config.Resources {
		pin := configured
		pin.Binding = configured.Binding.Clone()
		if err := validateAttachedResourcePin(pin); err != nil {
			return nil, err
		}
		key := attachedResourceKey{tenant: pin.TenantID, owner: pin.OwnerUserID, id: pin.SubscriptionConnectionID}
		workerKey := struct {
			tenant domain.TenantID
			owner  domain.UserID
			worker domain.AttachedWorkerID
		}{tenant: pin.TenantID, owner: pin.OwnerUserID, worker: pin.WorkerID}
		if _, collision := config.Generations.Lookup(pin.TenantID, pin.OwnerUserID, pin.SubscriptionConnectionID); collision ||
			config.Generations.ContainsWorker(pin.TenantID, pin.OwnerUserID, pin.WorkerID) {
			return nil, ErrAttachedResourceUnavailable
		}
		if _, duplicate := binder.pins[key]; duplicate || workerKeys[workerKey] {
			return nil, domain.ValidationError{Field: "attached_resource_pin", Reason: "resource and worker IDs must be unique within the exact owner scope"}
		}
		binder.pins[key], workerKeys[workerKey] = pin, true
	}
	return binder, nil
}

func (binder *AttachedResourceBinder) BindHarness(ctx context.Context, request ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	if binder == nil || !binder.enabled || ctx == nil || ctx.Err() != nil {
		return ports.ExecutionAuthorityV2{}, ErrAttachedResourceUnavailable
	}
	pin, found := binder.pins[attachedResourceKey{tenant: request.TenantID, owner: request.OwnerUserID, id: request.SubscriptionConnectionID}]
	if baseline, configured := binder.generations.Lookup(request.TenantID, request.OwnerUserID, request.SubscriptionConnectionID); configured {
		var err error
		pin, err = binder.generationPin(ctx, request, baseline)
		if err != nil {
			return ports.ExecutionAuthorityV2{}, ErrAttachedResourceUnavailable
		}
		found = true
	}
	if !found {
		return ports.ExecutionAuthorityV2{}, ErrAttachedResourceUnavailable
	}
	authority, err := pin.authority(request)
	if err != nil {
		return ports.ExecutionAuthorityV2{}, err
	}
	worker, found, err := binder.store.LoadAttachedWorker(ctx, request.TenantID, request.OwnerUserID, pin.WorkerID)
	if err != nil || !found {
		return ports.ExecutionAuthorityV2{}, ErrAttachedResourceUnavailable
	}
	connection, found, err := binder.store.LoadAttachedWorkerConnection(ctx, request.TenantID, request.OwnerUserID, pin.WorkerID)
	if err != nil || !found || worker.Validate() != nil || connection.Validate() != nil ||
		worker.TenantID != pin.TenantID || worker.OwnerUserID != pin.OwnerUserID || worker.ID != pin.WorkerID ||
		connection.TenantID != pin.TenantID || connection.OwnerUserID != pin.OwnerUserID || connection.WorkerID != pin.WorkerID ||
		worker.EnrollmentGeneration != pin.EnrollmentGeneration || connection.EnrollmentGeneration != pin.EnrollmentGeneration ||
		worker.ConnectionGeneration != connection.ConnectionGeneration || connection.CapabilityDigest != pin.CapabilityDigest ||
		worker.DesiredState != domain.AttachedWorkerDesiredActive || worker.ObservedState != domain.AttachedWorkerObservedOnline ||
		connection.State != domain.AttachedWorkerConnectionOnline || !request.At.UTC().Before(connection.PresenceExpiresAt) ||
		!request.At.UTC().Before(connection.AuthExpiresAt) {
		return ports.ExecutionAuthorityV2{}, ErrAttachedResourceUnavailable
	}
	return authority, nil
}

// This is only owner-scoped preflight. Fresh scheduler/direct-offer admission
// must re-read the exact head and baseline in their allocating transaction.
// A missing/revoked head never falls back to the original fixed generation.
func (binder *AttachedResourceBinder) generationPin(ctx context.Context, request ports.HarnessBindingRequest,
	baseline domain.AttachedResourceGenerationBaselineV1,
) (AttachedResourcePin, error) {
	head, found, err := binder.generationHeads.LoadAttachedResourceGenerationHeadV1(ctx, request.TenantID, request.OwnerUserID, request.SubscriptionConnectionID)
	if err != nil || !found || head.Validate() != nil || head.Revoked || head.OwnerUserID != baseline.OwnerUserID ||
		head.WorkerID != baseline.WorkerID || head.EnrollmentGeneration != baseline.EnrollmentGeneration || head.BaselineRevision != baseline.BaselineRevision {
		return AttachedResourcePin{}, ErrAttachedResourceUnavailable
	}
	binding, evidence, err := domain.ProjectAttachedResourceGenerationV1(baseline, head.CredentialGeneration)
	if err != nil {
		return AttachedResourcePin{}, ErrAttachedResourceUnavailable
	}
	for _, observed := range []time.Time{evidence.Catalog.ObservedAt, evidence.Route.ObservedAt, evidence.Capability.ObservedAt,
		evidence.Privacy.ObservedAt, evidence.Price.ObservedAt, evidence.Policy.ObservedAt} {
		if observed.After(request.At) {
			return AttachedResourcePin{}, ErrAttachedResourceUnavailable
		}
	}
	return AttachedResourcePin{TenantID: baseline.TenantID, OwnerUserID: baseline.OwnerUserID,
		SubscriptionConnectionID: baseline.SubscriptionConnectionID, WorkerID: baseline.WorkerID,
		EnrollmentGeneration: baseline.EnrollmentGeneration, CapabilityDigest: baseline.CapabilityDigest,
		PolicyDigest: domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest), Binding: binding}, nil
}

func validateAttachedResourcePin(pin AttachedResourcePin) error {
	if pin.EnrollmentGeneration == 0 || pin.Binding.TenantID != pin.TenantID || pin.Binding.OwnerUserID != pin.OwnerUserID ||
		pin.Binding.RunID != "" || pin.Binding.AttemptID != "" || pin.Binding.ExecutionPlacementDigest != "" {
		return domain.ValidationError{Field: "attached_resource_pin", Reason: "must bind one exact owner and enrollment with a scope-free harness template"}
	}
	if pin.Binding.EvidenceExpiresAt == nil {
		return domain.ValidationError{Field: "attached_resource_pin.evidence_expires_at", Reason: "must be explicit"}
	}
	// Reuse serving validation with a constructor-only scope. This performs no
	// resource reads or effects and rejects invalid mixed resource/policy tuples.
	_, err := pin.authority(ports.HarnessBindingRequest{
		TenantID: pin.TenantID, OwnerUserID: pin.OwnerUserID,
		RunID: "pin-validation-run", AttemptID: "pin-validation-attempt",
		SubscriptionConnectionID: pin.SubscriptionConnectionID, At: pin.Binding.EvidenceExpiresAt.Add(-1),
	})
	return err
}

func (pin AttachedResourcePin) authority(request ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	placement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker,
		FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: pin.OwnerUserID, WorkerID: pin.WorkerID,
		CapabilityDigest: pin.CapabilityDigest, PolicyDigest: pin.PolicyDigest}
	digest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		return ports.ExecutionAuthorityV2{}, err
	}
	binding := pin.Binding.Clone()
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = request.RunID, request.AttemptID, string(digest)
	authority := ports.ExecutionAuthorityV2{ExecutionPlacementV2: placement, HarnessBinding: binding}
	if err := authority.ValidateForScope(request); err != nil {
		return ports.ExecutionAuthorityV2{}, err
	}
	return authority, nil
}

var _ ports.HarnessBinder = (*AttachedResourceBinder)(nil)
