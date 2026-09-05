package serverlessegress

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrConfig             = errors.New("serverless egress configuration is invalid")
	ErrAuthority          = errors.New("serverless egress authority is invalid")
	ErrPolicy             = errors.New("serverless egress policy denied")
	ErrProxyAttestation   = errors.New("serverless egress proxy attestation failed")
	ErrCredential         = errors.New("serverless egress credential unavailable")
	ErrDelivery           = errors.New("serverless egress credential delivery mismatch")
	ErrEgress             = errors.New("serverless provider egress failed")
	ErrEvidence           = errors.New("serverless provider egress evidence is invalid")
	ErrCredentialFinalize = errors.New("serverless egress credential finalization failed")
)

type ConfigV1 struct {
	Clock       ports.Clock
	Gate        PreparedInvocationGate
	Credentials ports.ProviderResourceCredentialLifecycleV1
	Proxy       Proxy
}

type BoundaryV1 struct {
	config ConfigV1
}

func NewBoundaryV1(config ConfigV1) (*BoundaryV1, error) {
	if config.Clock == nil || config.Gate == nil || config.Credentials == nil || config.Proxy == nil {
		return nil, ErrConfig
	}
	return &BoundaryV1{config: config}, nil
}

func (boundary *BoundaryV1) Execute(ctx context.Context, request RequestV1) (result ResultV1, returnedErr error) {
	result.Evidence = EvidenceV1{
		Egress:                 domain.SubstrateEgressNotAttemptedV1,
		ProxyAttestation:       domain.SubstrateProxyAttestationUnknownV1,
		RouteState:             domain.ProviderEvidenceUnknownV1,
		Acceptance:             domain.ProviderAcceptanceUnknownV1,
		CredentialFinalization: domain.CredentialFinalizationUnknownV1,
	}
	if boundary == nil || ctx == nil || ctx.Err() != nil || boundary.config.Gate.Validate(request.Prepared) != nil {
		return result, ErrAuthority
	}
	authority := request.Prepared.Authority()
	allocation := request.Prepared.Allocation()
	now := boundary.config.Clock.Now().UTC()
	if err := request.Policy.ValidateForAuthority(authority, allocation, request.RoutePolicy, request.EffectivePolicy, now); err != nil {
		result.Evidence.Egress = domain.SubstrateEgressDeniedV1
		return result, ErrPolicy
	}
	if uint64(len(request.Payload)) > request.Policy.Endpoint.MaxRequestBytes {
		result.Evidence.Egress = domain.SubstrateEgressDeniedV1
		return result, ErrPolicy
	}
	attestationExpiry := earliestExpiry(
		now.Add(authority.SubstrateBinding.Limits.ExecutionTimeout),
		now.Add(authority.AdmissionCostCeiling.MaxActiveDuration),
		authority.InvocationDeadline,
		request.Prepared.ExecuteDeadline(),
		authority.Lease.ExpiresAt,
		authority.SubstrateBinding.ProfileEvidenceExpiresAt,
		authority.AdmissionCostCeiling.PriceExpiresAt,
		request.RoutePolicy.ExpiresAt,
		request.EffectivePolicy.ExpiresAt,
	)
	if authority.HarnessBinding.EvidenceExpiresAt != nil {
		attestationExpiry = earliestExpiry(attestationExpiry, *authority.HarnessBinding.EvidenceExpiresAt)
	}
	if boundary.config.Gate.Consume(request.Prepared) != nil {
		return result, ErrAuthority
	}
	effectCtx, cancelEffect := context.WithDeadline(ctx, attestationExpiry)
	defer cancelEffect()
	attestation, err := boundary.config.Proxy.Preflight(effectCtx, request.Policy)
	if err != nil {
		result.Evidence.Egress = domain.SubstrateEgressDeniedV1
		result.Evidence.ProxyAttestation = domain.SubstrateProxyAttestationMismatchV1
		return result, ErrProxyAttestation
	}
	now = boundary.config.Clock.Now().UTC()
	if effectCtx.Err() != nil || boundary.config.Gate.Validate(request.Prepared) != nil || authority.ValidateAt(now) != nil {
		result.Evidence.Egress = domain.SubstrateEgressDeniedV1
		return result, ErrAuthority
	}
	if attestation.ValidateForPolicy(request.Policy, now, attestationExpiry) != nil {
		result.Evidence.Egress = domain.SubstrateEgressDeniedV1
		result.Evidence.ProxyAttestation = domain.SubstrateProxyAttestationMismatchV1
		return result, ErrProxyAttestation
	}
	result.Evidence.ProxyAttestation = domain.SubstrateProxyAttestationVerifiedV1
	credentialExpiry := earliestExpiry(attestationExpiry, attestation.ExpiresAt)
	credentialCtx, cancelCredential := context.WithDeadline(effectCtx, credentialExpiry)
	defer cancelCredential()

	handle, err := boundary.config.Credentials.IssueProviderCredential(credentialCtx, ports.ProviderCredentialIssueRequestV1{
		HarnessBinding: authority.HarnessBinding.Clone(),
		WorkerID:       authority.Lease.WorkerID,
		LeaseID:        authority.Lease.ID,
		LeaseFence:     authority.Lease.FenceToken,
		ExpiresAt:      credentialExpiry,
	})
	if err != nil {
		return result, ErrCredential
	}
	cleanupTimeout := authority.SubstrateBinding.Limits.CleanupTimeout
	if cleanupTimeout > 30*time.Second {
		cleanupTimeout = 30 * time.Second
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		releaseErr := boundary.config.Credentials.ReleaseProviderCredential(cleanupCtx, handle)
		cancel()
		if releaseErr != nil {
			result.Evidence.CredentialFinalization = domain.CredentialFinalizationFailedV1
			returnedErr = errors.Join(returnedErr, ErrCredentialFinalize)
		} else {
			result.Evidence.CredentialFinalization = domain.CredentialFinalizationVerifiedV1
		}
	}()
	if validateCredentialHandle(handle, authority, credentialExpiry) != nil {
		return result, ErrCredential
	}

	sealedPayload := append([]byte(nil), request.Payload...)
	defer clear(sealedPayload)
	var callbackErr error
	const (
		callbackOpen uint32 = iota
		callbackRunning
		callbackComplete
		callbackClosed
	)
	var callbackState atomic.Uint32
	callbackDone := make(chan struct{})
	materializeErr := boundary.config.Credentials.MaterializeProviderCredential(credentialCtx, handle, func(materialization ports.ProviderCredentialMaterializationV1, secret []byte) error {
		if !callbackState.CompareAndSwap(callbackOpen, callbackRunning) {
			return ErrCredential
		}
		defer func() {
			callbackState.Store(callbackComplete)
			close(callbackDone)
		}()
		if materialization.Validate() != nil || materialization.Kind != authority.HarnessBinding.Backend.CredentialDeliveryKind ||
			!validSecretShape(materialization.Kind, secret) {
			callbackErr = ErrDelivery
			return callbackErr
		}
		now = boundary.config.Clock.Now().UTC()
		if credentialCtx.Err() != nil || boundary.config.Gate.Validate(request.Prepared) != nil || authority.ValidateAt(now) != nil ||
			attestation.ValidateForPolicy(request.Policy, now, attestationExpiry) != nil {
			callbackErr = ErrAuthority
			return callbackErr
		}
		invokeStartedAt := now
		proxyResult, invokeErr := boundary.config.Proxy.Invoke(credentialCtx, ProxyInvocationV1{
			Policy: request.Policy, Attestation: attestation, Materialization: materialization,
			Secret: secret, Payload: sealedPayload,
		})
		completedAt := boundary.config.Clock.Now().UTC()
		validationErr := validateProxyResult(request.Policy, proxyResult, sealedPayload, invokeStartedAt, completedAt, credentialExpiry)
		if invokeErr != nil {
			clear(proxyResult.Response)
			if validationErr == nil {
				result.Evidence = evidenceFromProxy(result.Evidence, proxyResult)
			}
			callbackErr = ErrEgress
			return callbackErr
		}
		if credentialCtx.Err() != nil || authority.ValidateAt(completedAt) != nil ||
			attestation.ValidateForPolicy(request.Policy, completedAt, attestationExpiry) != nil {
			clear(proxyResult.Response)
			callbackErr = ErrAuthority
			return callbackErr
		}
		if validationErr != nil {
			clear(proxyResult.Response)
			callbackErr = ErrEvidence
			return callbackErr
		}
		result.Response = proxyResult.Response
		result.Evidence = evidenceFromProxy(result.Evidence, proxyResult)
		return nil
	})
	if callbackState.CompareAndSwap(callbackOpen, callbackClosed) {
		// A successful lifecycle call without its synchronous callback is invalid.
	} else if callbackState.Load() == callbackRunning {
		<-callbackDone
	}
	if callbackErr != nil {
		return result, callbackErr
	}
	if materializeErr != nil || callbackState.Load() != callbackComplete {
		clear(result.Response)
		result.Response = nil
		return result, ErrCredential
	}
	return result, nil
}

func validateCredentialHandle(handle ports.ProviderInvocationCredentialV1, authority domain.ServerlessInvocationAuthorityV1, expiresAt time.Time) error {
	if handle.Validate() != nil || handle.TenantID != authority.HarnessBinding.TenantID ||
		handle.OwnerUserID != authority.HarnessBinding.OwnerUserID || handle.RunID != authority.HarnessBinding.RunID ||
		handle.AttemptID != authority.HarnessBinding.AttemptID || handle.WorkerID != authority.Lease.WorkerID ||
		handle.LeaseID != authority.Lease.ID || handle.LeaseFence != authority.Lease.FenceToken ||
		handle.ProviderResource != authority.HarnessBinding.Resource || !handle.ExpiresAt.UTC().Equal(expiresAt.UTC()) {
		return ErrCredential
	}
	return nil
}

func earliestExpiry(values ...time.Time) time.Time {
	var earliest time.Time
	for _, value := range values {
		value = value.UTC()
		if value.IsZero() {
			return time.Time{}
		}
		if earliest.IsZero() || value.Before(earliest) {
			earliest = value
		}
	}
	return earliest
}

func validSecretShape(kind domain.ProviderCredentialDeliveryKindV1, secret []byte) bool {
	switch kind {
	case domain.ProviderCredentialDeliveryFileV1:
		return len(secret) == 0
	case domain.ProviderCredentialDeliveryEnvironmentV1, domain.ProviderCredentialDeliveryDirectV1:
		return len(secret) > 0
	default:
		return false
	}
}

func validateProxyResult(policy PolicyV1, result ProxyResultV1, payload []byte, startedAt, completedAt, expiresAt time.Time) error {
	if result.Route != policy.Route || result.ObservedAt.IsZero() || result.ObservedAt.Location() != time.UTC ||
		startedAt.IsZero() || completedAt.IsZero() || expiresAt.IsZero() || completedAt.Before(startedAt) ||
		result.ObservedAt.Before(startedAt) || result.ObservedAt.After(completedAt) || !result.ObservedAt.Before(expiresAt) ||
		result.RequestBytes != uint64(len(payload)) ||
		result.ResponseBytes != uint64(len(result.Response)) || result.RequestBytes > policy.Endpoint.MaxRequestBytes ||
		result.ResponseBytes > policy.Endpoint.MaxResponseBytes {
		return ErrEvidence
	}
	switch result.Acceptance {
	case domain.ProviderAcceptanceUnknownV1, domain.ProviderAcceptancePreAcceptanceV1, domain.ProviderAcceptanceAcceptedV1:
		return nil
	default:
		return ErrEvidence
	}
}

func evidenceFromProxy(base EvidenceV1, result ProxyResultV1) EvidenceV1 {
	base.Egress = domain.SubstrateEgressPolicyEnforcedV1
	base.RouteState = domain.ProviderEvidenceSupportedV1
	base.ActualModelVendorID = result.Route.ModelVendorID
	base.ActualModelID = result.Route.ModelID
	base.TransportKind = result.Route.TransportKind
	base.TransportProvider = result.Route.TransportProvider
	base.UpstreamProviderID = result.Route.UpstreamProviderID
	base.EndpointID = result.Route.EndpointID
	base.RequestBytes = result.RequestBytes
	base.ResponseBytes = result.ResponseBytes
	base.Acceptance = result.Acceptance
	base.ObservedAt = result.ObservedAt.UTC()
	return base
}
