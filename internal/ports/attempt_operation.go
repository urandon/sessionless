package ports

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var ErrAttemptOperationCapabilityNotSerializable = errors.New("attempt operation capabilities are process-local")

// Actual authorized immutable bytes, never an approval boolean or claimed
// digest. Implementations must enforce the original scoped object identity and
// hard cap before materialization. Reads occur outside retryable SQL callbacks.
type AttemptOperationBodyStoreV1 interface {
	ReadAttemptOperationBody(context.Context, domain.AttemptOperationScopeV1, string, uint64) ([]byte, error)
}

type AttemptOperationReservationStatusV1 string

const (
	AttemptOperationReservedV1      AttemptOperationReservationStatusV1 = "reserved"
	AttemptOperationReconcileOnlyV1 AttemptOperationReservationStatusV1 = "reconcile_only"
)

type ReserveAttemptOperationRequestV1 struct {
	Operation      domain.AttemptOperationV1
	ConsentReceipt *domain.WebSearchConsentReceiptBindingV1
}

func (v ReserveAttemptOperationRequestV1) Validate() error {
	if err := v.Operation.Validate(); err != nil {
		return err
	}
	if v.Operation.Kind != domain.AttemptOperationWebSearchV1 {
		if v.ConsentReceipt != nil {
			return operationPortError("reserve.consent_receipt", "is exclusive to web_search")
		}
		return nil
	}
	if v.ConsentReceipt == nil {
		return operationPortError("reserve.consent_receipt", "requires the exact typed durable receipt binding")
	}
	if err := v.ConsentReceipt.Validate(); err != nil {
		return err
	}
	r := v.ConsentReceipt.Request
	s := v.Operation.Scope
	expected := domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID, EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)}
	if r.Scope != expected || r.ModelResponseDigest != v.Operation.ModelResponseDigest || r.CallID != v.Operation.CallID || r.ArgumentsDigest != v.Operation.ArgumentsDigest || r.QueryDigest != v.Operation.QueryDigest || r.GrantDigest != v.Operation.ToolGrantDigest || string(v.ConsentReceipt.RequestDigest) != v.Operation.ConsentRequestDigest || string(v.ConsentReceipt.DecisionDigest) != v.Operation.ConsentDecisionDigest || v.Operation.Deadline.After(r.ExpiresAt) {
		return operationPortError("reserve.consent_receipt", "must exact-match scope, model, call, arguments, query, grant, receipt and deadline")
	}
	return nil
}

// Recovery uses the original immutable request, never replacement arguments or
// a new physical claim. ExpectedReservationDigest is optional only when the
// append response was lost before the caller learned it. An absent row remains
// absent: recovery must never create a row, debit or send capability.
type RecoverAttemptOperationRequestV1 struct {
	Operation                 domain.AttemptOperationV1
	ExpectedReservationDigest domain.AttemptOperationReservationDigestV1
}

func (v RecoverAttemptOperationRequestV1) Validate() error {
	if err := v.Operation.Validate(); err != nil {
		return err
	}
	if v.ExpectedReservationDigest != "" {
		return v.ExpectedReservationDigest.Validate()
	}
	return nil
}

// AttemptOperationCapabilityV1 follows the outer process-local grant pattern.
// Validate checks shape, not the MAC, freshness or one-use state. Only the exact
// trusted issuer may verify/consume it. No decoded DTO can authorize a send.
type AttemptOperationCapabilityV1 struct {
	Version       uint32
	Reservation   domain.AttemptOperationReservationV1
	IssuedAt      time.Time
	ExpiresAt     time.Time
	Authenticator []byte
}

func (v AttemptOperationCapabilityV1) Clone() AttemptOperationCapabilityV1 {
	v.Reservation = v.Reservation.Clone()
	v.Authenticator = append([]byte(nil), v.Authenticator...)
	return v
}
func (v AttemptOperationCapabilityV1) Validate() error {
	if v.Version != 1 {
		return operationPortError("capability.version", "must equal 1")
	}
	if err := v.Reservation.Validate(); err != nil {
		return err
	}
	if v.IssuedAt.IsZero() || v.IssuedAt.Before(v.Reservation.ReservedAt) || !v.ExpiresAt.After(v.IssuedAt) || v.ExpiresAt.After(v.Reservation.Operation.Deadline) || len(v.Authenticator) != 32 {
		return operationPortError("capability", "requires a bounded process-local MAC grant")
	}
	return nil
}
func (AttemptOperationCapabilityV1) MarshalJSON() ([]byte, error) {
	return nil, ErrAttemptOperationCapabilityNotSerializable
}
func (*AttemptOperationCapabilityV1) UnmarshalJSON([]byte) error {
	return ErrAttemptOperationCapabilityNotSerializable
}

// Observation grants are domain-separated from send permission. They remain
// usable within their own bounded reconciliation window after execution expires.
// Reissuing one never extends the original operation deadline plus cleanup.
type AttemptOperationObservationGrantV1 struct {
	Version       uint32
	Reservation   domain.AttemptOperationReservationV1
	IssuedAt      time.Time
	ExpiresAt     time.Time
	Authenticator []byte
}

func (v AttemptOperationObservationGrantV1) Clone() AttemptOperationObservationGrantV1 {
	v.Reservation = v.Reservation.Clone()
	v.Authenticator = append([]byte(nil), v.Authenticator...)
	return v
}
func (v AttemptOperationObservationGrantV1) Validate() error {
	if v.Version != 1 {
		return operationPortError("observation_grant.version", "must equal 1")
	}
	if err := v.Reservation.Validate(); err != nil {
		return err
	}
	if v.IssuedAt.IsZero() || v.IssuedAt.Before(v.Reservation.ReservedAt) || !v.ExpiresAt.After(v.IssuedAt) || v.ExpiresAt.After(v.IssuedAt.Add(v.Reservation.Operation.CleanupDuration)) || v.ExpiresAt.After(v.Reservation.Operation.Deadline.Add(v.Reservation.Operation.CleanupDuration)) || len(v.Authenticator) != 32 {
		return operationPortError("observation_grant", "requires a bounded process-local MAC grant")
	}
	return nil
}
func (AttemptOperationObservationGrantV1) MarshalJSON() ([]byte, error) {
	return nil, ErrAttemptOperationCapabilityNotSerializable
}
func (*AttemptOperationObservationGrantV1) UnmarshalJSON([]byte) error {
	return ErrAttemptOperationCapabilityNotSerializable
}

// Implementations mint only AFTER one confirmed fresh reservation transaction
// commits, never inside a retried/rolled-back transaction callback. The issuer
// atomically consumes once per exact reservation, authenticates every sealed
// field, and uses its injected authoritative clock. The before-send store gate
// must additionally recheck cancellation/expiry/revocation and the exact search
// receipt. Consumption does not itself attest those durable facts.
type AttemptOperationCapabilityIssuerV1 interface {
	MintAttemptOperationCapability(domain.AttemptOperationReservationV1, time.Time, time.Time) (AttemptOperationCapabilityV1, error)
	ConsumeAttemptOperationCapability(AttemptOperationCapabilityV1) error
	MintAttemptOperationObservationGrant(domain.AttemptOperationReservationV1, time.Time, time.Time) (AttemptOperationObservationGrantV1, error)
	VerifyAttemptOperationObservationGrant(AttemptOperationObservationGrantV1) error
	AuthenticateAttemptOperationObservation(AttemptOperationObservationGrantV1, domain.AttemptOperationObservationV1) (AuthenticatedAttemptOperationObservationV1, error)
	VerifyAttemptOperationObservation(AuthenticatedAttemptOperationObservationV1) error
}

type ReserveAttemptOperationResultV1 struct {
	Status           AttemptOperationReservationStatusV1
	Reservation      domain.AttemptOperationReservationV1
	Capability       *AttemptOperationCapabilityV1
	ObservationGrant AttemptOperationObservationGrantV1
}

func (v ReserveAttemptOperationResultV1) Validate() error {
	if err := v.Reservation.Validate(); err != nil {
		return err
	}
	if err := v.ObservationGrant.Validate(); err != nil {
		return err
	}
	digest, _ := v.Reservation.Digest()
	observed, _ := v.ObservationGrant.Reservation.Digest()
	if digest != observed {
		return operationPortError("result.observation_grant", "must exact-match the returned reservation")
	}
	switch v.Status {
	case AttemptOperationReservedV1:
		if v.Capability == nil {
			return operationPortError("result.capability", "fresh committed reservation requires its one-use capability")
		}
		if err := v.Capability.Validate(); err != nil {
			return err
		}
		capable, _ := v.Capability.Reservation.Digest()
		if capable != digest {
			return operationPortError("result.capability", "must exact-match returned reservation")
		}
	case AttemptOperationReconcileOnlyV1:
		if v.Capability != nil {
			return operationPortError("result.capability", "duplicate/recovered reservation must not contain send authority")
		}
	default:
		return operationPortError("result.status", "is unsupported")
	}
	return nil
}

// An authenticated observation, including unknown/error outcomes, is trusted
// only after exact issuer verification. A raw backend export is not this proof.
type AuthenticatedAttemptOperationObservationV1 struct {
	Version       uint32
	Reservation   domain.AttemptOperationReservationV1
	Observation   domain.AttemptOperationObservationV1
	Authenticator []byte
}

func (v AuthenticatedAttemptOperationObservationV1) Clone() AuthenticatedAttemptOperationObservationV1 {
	v.Reservation = v.Reservation.Clone()
	v.Observation = v.Observation.Clone()
	v.Authenticator = append([]byte(nil), v.Authenticator...)
	return v
}
func (v AuthenticatedAttemptOperationObservationV1) Validate() error {
	if v.Version != 1 || len(v.Authenticator) != 32 {
		return operationPortError("authenticated_observation", "requires version 1 and issuer MAC")
	}
	return v.Observation.ValidateForReservation(v.Reservation)
}
func (AuthenticatedAttemptOperationObservationV1) MarshalJSON() ([]byte, error) {
	return nil, ErrAttemptOperationCapabilityNotSerializable
}
func (*AuthenticatedAttemptOperationObservationV1) UnmarshalJSON([]byte) error {
	return ErrAttemptOperationCapabilityNotSerializable
}

type RecordAttemptOperationObservationRequestV1 struct {
	ObservationGrant AttemptOperationObservationGrantV1
	Observation      AuthenticatedAttemptOperationObservationV1
}

func (v RecordAttemptOperationObservationRequestV1) Validate() error {
	if err := v.ObservationGrant.Validate(); err != nil {
		return err
	}
	if err := v.Observation.Validate(); err != nil {
		return err
	}
	grant, _ := v.ObservationGrant.Reservation.Digest()
	observed, _ := v.Observation.Reservation.Digest()
	if grant != observed {
		return operationPortError("record.correlation", "must exact-match the observation grant")
	}
	return nil
}

// AttemptOperationStoreV1 extends the existing Attempt effect store/retention
// authority; it creates no scheduler, retry, native-resume, terminal or quota
// authority. Its production resolver MUST reread Run/Attempt/lease/fence, parent
// sole claim, cancellation, owner/Session participation, resource generations/
// revocation, profile/policy, price/billing allocation and proxy/allocation
// freshness with resource time inside ONE atomic transaction. Reconstruct and
// exact-match completed digests, immutable blobs, call ID/argument replay index,
// sole durable known predecessor and every aggregate debit including consent
// rows/bodies. No caller-supplied valid/approved/current booleans are accepted.
//
// Web search additionally requires the exact approved durable consent receipt
// for this query/arguments/model/call/original claim/revisions/deadline. Opaque
// evidence digests do not create price/grant authority: unrepresentable or
// unknown external pricing/billing denies reserve. No implicit funding or zero
// MCP/search price is permitted. Deadline is the minimum of ALL fresh windows.
//
// Duplicate reserve and lost-response recovery return reconcile-only, never a
// new send capability, including absent-row/crash/persistence-failure paths.
// Only the first confirmed fresh commit may mint a capability. Record is an
// immutable compare-and-set: identical writes recover, conflicting ones deny.
// Reads return authenticated evidence only; no undurable/unknown continuation.
type AttemptOperationStoreV1 interface {
	ReserveAttemptOperation(context.Context, ReserveAttemptOperationRequestV1) (ReserveAttemptOperationResultV1, error)
	RecoverAttemptOperation(context.Context, RecoverAttemptOperationRequestV1) (ReserveAttemptOperationResultV1, error)
	RecordAttemptOperationObservation(context.Context, RecordAttemptOperationObservationRequestV1) error
	ReadAttemptOperationObservation(context.Context, AttemptOperationObservationGrantV1) (AuthenticatedAttemptOperationObservationV1, error)
	AuthorizeAttemptOperationBeforeSend(context.Context, AttemptOperationCapabilityV1) error
}

func operationPortError(field, reason string) error {
	return domain.ValidationError{Field: "attempt_operation_port." + field, Reason: reason}
}
