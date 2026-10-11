package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const WebSearchConsentVersionV1 uint32 = 1

var (
	ErrWebSearchConsentConflict  = errors.New("web search consent compare-and-set conflict")
	ErrWebSearchConsentAuthority = errors.New("web search consent authority denied")
	ErrWebSearchConsentStopped   = errors.New("web search consent is stopped")
)

type WebSearchConsentRequestIDV1 string
type WebSearchConsentRequestDigestV1 string
type WebSearchConsentDecisionDigestV1 string
type WebSearchConsentChoiceV1 string
type WebSearchConsentStatusV1 string
type WebSearchConsentClaimStateV1 string

const (
	WebSearchConsentApproveV1       WebSearchConsentChoiceV1     = "approve"
	WebSearchConsentDenyV1          WebSearchConsentChoiceV1     = "deny"
	WebSearchConsentPendingV1       WebSearchConsentStatusV1     = "pending"
	WebSearchConsentApprovedV1      WebSearchConsentStatusV1     = "approved"
	WebSearchConsentDeniedV1        WebSearchConsentStatusV1     = "denied"
	WebSearchConsentExpiredV1       WebSearchConsentStatusV1     = "expired"
	WebSearchConsentCancelledV1     WebSearchConsentStatusV1     = "cancelled"
	WebSearchConsentRevokedV1       WebSearchConsentStatusV1     = "revoked"
	WebSearchConsentReconcileOnlyV1 WebSearchConsentStatusV1     = "reconcile_only"
	WebSearchConsentClaimActiveV1   WebSearchConsentClaimStateV1 = "active"
	WebSearchConsentClaimLostV1     WebSearchConsentClaimStateV1 = "reconcile_only"
)

// WebSearchConsentScopeV1 binds completed authorities in acyclic digest order.
// Neither this serializable value nor a receipt is an execution capability.
type WebSearchConsentScopeV1 struct {
	TenantID                  TenantID  `json:"tenant_id"`
	SessionID                 SessionID `json:"session_id"`
	OwnerUserID               UserID    `json:"owner_user_id"`
	RunID                     RunID     `json:"run_id"`
	AttemptID                 AttemptID `json:"attempt_id"`
	LeaseID                   LeaseID   `json:"lease_id"`
	FenceToken                uint64    `json:"fence_token"`
	PhysicalInvocationClaimID string    `json:"physical_invocation_claim_id"`
	EffectReservationDigest   string    `json:"effect_reservation_digest"`
	InvocationAuthorityDigest string    `json:"invocation_authority_digest"`
	HarnessBindingDigest      string    `json:"harness_binding_digest"`
	PolicyDigest              string    `json:"policy_digest"`
}

func (value WebSearchConsentScopeV1) Validate() error {
	for _, validate := range []func() error{value.TenantID.Validate, value.SessionID.Validate, value.OwnerUserID.Validate, value.RunID.Validate, value.AttemptID.Validate, value.LeaseID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if value.FenceToken == 0 {
		return ValidationError{Field: "web_search_consent.fence_token", Reason: "must be positive"}
	}
	if err := ValidateOpaqueID("web_search_consent.physical_invocation_claim_id", value.PhysicalInvocationClaimID); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"effect_reservation_digest", value.EffectReservationDigest}, {"invocation_authority_digest", value.InvocationAuthorityDigest},
		{"harness_binding_digest", value.HarnessBindingDigest}, {"policy_digest", value.PolicyDigest},
	} {
		if err := validateSHA256("web_search_consent."+field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

func (value WebSearchConsentScopeV1) appendDigest(digest *serverlessDigest) {
	for _, field := range []string{string(value.TenantID), string(value.SessionID), string(value.OwnerUserID), string(value.RunID), string(value.AttemptID), string(value.LeaseID), value.PhysicalInvocationClaimID, value.EffectReservationDigest, value.InvocationAuthorityDigest, value.HarnessBindingDigest, value.PolicyDigest} {
		digest.str(field)
	}
	digest.uint(value.FenceToken)
}

// WebSearchConsentRequestV1 is immutable and contains no plaintext query. The
// initiating user comes from the canonical task, not the resource's owner or
// a caller-selected principal. Arguments must be verified against the durable
// complete model response before insertion; blob metadata alone is not proof.
type WebSearchConsentRequestV1 struct {
	Version              uint32                      `json:"version"`
	ID                   WebSearchConsentRequestIDV1 `json:"id"`
	Scope                WebSearchConsentScopeV1     `json:"scope"`
	InitiatingUserID     UserID                      `json:"initiating_user_id"`
	ModelResponseDigest  string                      `json:"model_response_digest"`
	CallID               string                      `json:"call_id"`
	ArgumentsDigest      string                      `json:"arguments_digest"`
	QueryDigest          string                      `json:"query_digest"`
	GrantDigest          string                      `json:"grant_digest"`
	ConsentRevision      uint64                      `json:"consent_revision"`
	RevocationGeneration uint64                      `json:"revocation_generation"`
	Arguments            BlobRef                     `json:"arguments"`
	CreatedAt            time.Time                   `json:"created_at"`
	ExpiresAt            time.Time                   `json:"expires_at"`
}

func (id WebSearchConsentRequestIDV1) Validate() error {
	return ValidateOpaqueID("web_search_consent.id", string(id))
}
func (digest WebSearchConsentRequestDigestV1) Validate() error {
	return validateSHA256("web_search_consent.request_digest", string(digest))
}
func (digest WebSearchConsentDecisionDigestV1) Validate() error {
	return validateSHA256("web_search_consent.decision_digest", string(digest))
}
func (choice WebSearchConsentChoiceV1) Validate() error {
	if choice != WebSearchConsentApproveV1 && choice != WebSearchConsentDenyV1 {
		return ValidationError{Field: "web_search_consent.choice", Reason: "must be approve or deny"}
	}
	return nil
}

func (value WebSearchConsentRequestV1) Validate() error {
	if value.Version != WebSearchConsentVersionV1 {
		return ValidationError{Field: "web_search_consent.version", Reason: "must equal 1"}
	}
	for _, validate := range []func() error{value.ID.Validate, value.Scope.Validate, value.InitiatingUserID.Validate, value.Arguments.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if err := ValidateOpaqueID("web_search_consent.call_id", value.CallID); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{{"model_response_digest", value.ModelResponseDigest}, {"arguments_digest", value.ArgumentsDigest}, {"query_digest", value.QueryDigest}, {"grant_digest", value.GrantDigest}} {
		if err := validateSHA256("web_search_consent."+field.name, field.value); err != nil {
			return err
		}
	}
	if value.ConsentRevision == 0 || value.RevocationGeneration == 0 {
		return ValidationError{Field: "web_search_consent.revisions", Reason: "must be positive"}
	}
	prefix := SessionRunObjectPrefix(value.Scope.TenantID, value.Scope.SessionID, value.Scope.RunID) + "attempts/" + string(value.Scope.AttemptID) + "/"
	if value.Arguments.TenantID != value.Scope.TenantID || !strings.HasPrefix(value.Arguments.Key, prefix) || len(value.Arguments.Key) > 2048 || value.Arguments.Size <= 0 || value.Arguments.Size > 4<<10 || value.Arguments.SHA256 != value.ArgumentsDigest {
		return ValidationError{Field: "web_search_consent.arguments", Reason: "requires the exact bounded argument digest in the owning Attempt's immutable namespace"}
	}
	if value.CreatedAt.IsZero() || value.CreatedAt.UTC().Year() < 1 || value.ExpiresAt.UTC().Year() > 9999 || !value.ExpiresAt.After(value.CreatedAt) || value.ExpiresAt.Sub(value.CreatedAt) > 120*time.Second {
		return ValidationError{Field: "web_search_consent.expires_at", Reason: "requires an absolute window within the original active-time ceiling"}
	}
	return nil
}

func (value WebSearchConsentRequestV1) Digest() (WebSearchConsentRequestDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.web-search-consent-request.v1")
	digest.uint(uint64(value.Version))
	digest.str(string(value.ID))
	value.Scope.appendDigest(digest)
	for _, field := range []string{string(value.InitiatingUserID), value.ModelResponseDigest, value.CallID, value.ArgumentsDigest, value.QueryDigest, value.GrantDigest} {
		digest.str(field)
	}
	digest.uint(value.ConsentRevision)
	digest.uint(value.RevocationGeneration)
	appendServerlessBlobDigest(digest, value.Arguments)
	digest.instant(value.CreatedAt)
	digest.instant(value.ExpiresAt)
	return WebSearchConsentRequestDigestV1(digest.sum()), nil
}

// ValidateArguments verifies the exact immutable bytes without normalizing or
// replacing the approved query. Duplicate/extra keys, partial JSON and invalid
// UTF-8 are denied. It does not classify private data or authorize disclosure.
func (value WebSearchConsentRequestV1) ValidateArguments(body []byte) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if int64(len(body)) != value.Arguments.Size || !utf8.Valid(body) || consentBytesDigest(body) != value.ArgumentsDigest {
		return ValidationError{Field: "web_search_consent.arguments", Reason: "bytes do not match the exact immutable reference"}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return ValidationError{Field: "web_search_consent.arguments", Reason: "must be one complete JSON object"}
	}
	seen := map[string]bool{}
	query := ""
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok || seen[key] || (key != "query" && key != "max_results") {
			return ValidationError{Field: "web_search_consent.arguments", Reason: "duplicate or unsupported argument"}
		}
		seen[key] = true
		if key == "query" {
			if err := decoder.Decode(&query); err != nil {
				return err
			}
		} else {
			var maximum *int
			if err := decoder.Decode(&maximum); err != nil {
				return err
			}
			if maximum == nil || *maximum < 1 || *maximum > 5 {
				return ValidationError{Field: "web_search_consent.max_results", Reason: "must be 1 through 5"}
			}
		}
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return ValidationError{Field: "web_search_consent.arguments", Reason: "must be complete"}
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		return ValidationError{Field: "web_search_consent.arguments", Reason: "trailing JSON is denied"}
	}
	if !seen["query"] || strings.TrimSpace(query) == "" || len(query) > 1024 || consentBytesDigest([]byte(query)) != value.QueryDigest {
		return ValidationError{Field: "web_search_consent.query", Reason: "must match the exact nonempty bounded query"}
	}
	return nil
}

func consentBytesDigest(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

// WebSearchConsentDecisionV1 retains all request bindings through its sealed
// request. Actor/session/version are derived from authenticated transaction
// rows. No exported flag can make this receipt authoritative or transferable.
type WebSearchConsentDecisionV1 struct {
	Version                   uint32                          `json:"version"`
	Request                   WebSearchConsentRequestV1       `json:"request"`
	RequestDigest             WebSearchConsentRequestDigestV1 `json:"request_digest"`
	RequestRevision           uint64                          `json:"request_revision"`
	ActorUserID               UserID                          `json:"actor_user_id"`
	WebSessionDigest          SecretDigest                    `json:"web_session_digest"`
	MembershipSecurityVersion uint64                          `json:"membership_security_version"`
	Choice                    WebSearchConsentChoiceV1        `json:"choice"`
	ObservedAt                time.Time                       `json:"observed_at"`
}

func (value WebSearchConsentDecisionV1) Validate() error {
	digest, err := value.Request.Digest()
	if err != nil {
		return err
	}
	if value.Version != WebSearchConsentVersionV1 || value.RequestRevision != 1 || digest != value.RequestDigest || value.ActorUserID != value.Request.InitiatingUserID || value.MembershipSecurityVersion == 0 {
		return ValidationError{Field: "web_search_consent.decision", Reason: "must seal the exact initial request and authenticated initiating user"}
	}
	if err := value.WebSessionDigest.Validate("web_search_consent.web_session_digest"); err != nil {
		return err
	}
	if err := value.Choice.Validate(); err != nil {
		return err
	}
	if value.ObservedAt.Before(value.Request.CreatedAt) || !value.ObservedAt.Before(value.Request.ExpiresAt) {
		return ValidationError{Field: "web_search_consent.observed_at", Reason: "must be inside the unchanged request window"}
	}
	return nil
}

func (value WebSearchConsentDecisionV1) Digest() (WebSearchConsentDecisionDigestV1, error) {
	if err := value.Validate(); err != nil {
		return "", err
	}
	digest := newServerlessDigest("sessionless.web-search-consent-decision.v1")
	digest.uint(uint64(value.Version))
	digest.str(string(value.RequestDigest))
	digest.uint(value.RequestRevision)
	digest.str(string(value.ActorUserID))
	digest.str(string(value.WebSessionDigest))
	digest.uint(value.MembershipSecurityVersion)
	digest.str(string(value.Choice))
	digest.instant(value.ObservedAt)
	return WebSearchConsentDecisionDigestV1(digest.sum()), nil
}

// WebSearchConsentStateV1 is the bounded CAS projection, not a new ledger.
// Stop states never forge a human approve/deny receipt. An approved receipt
// survives reconciliation, but a stopped projection cannot authorize sends.
type WebSearchConsentStateV1 struct {
	Request   WebSearchConsentRequestV1   `json:"request"`
	Revision  uint64                      `json:"revision"`
	Status    WebSearchConsentStatusV1    `json:"status"`
	Decision  *WebSearchConsentDecisionV1 `json:"decision,omitempty"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

func (value WebSearchConsentStateV1) Clone() WebSearchConsentStateV1 {
	clone := value
	if value.Decision != nil {
		decision := *value.Decision
		clone.Decision = &decision
	}
	return clone
}

func (value WebSearchConsentStateV1) Validate() error {
	digest, err := value.Request.Digest()
	if err != nil {
		return err
	}
	if value.UpdatedAt.Before(value.Request.CreatedAt) {
		return ValidationError{Field: "web_search_consent.updated_at", Reason: "must not move backwards"}
	}
	if value.Decision != nil {
		if err := value.Decision.Validate(); err != nil {
			return err
		}
		if value.Decision.RequestDigest != digest || value.UpdatedAt.Before(value.Decision.ObservedAt) {
			return ErrWebSearchConsentConflict
		}
	}
	switch value.Status {
	case WebSearchConsentPendingV1:
		if value.Revision != 1 || value.Decision != nil || !value.UpdatedAt.Equal(value.Request.CreatedAt) {
			return ErrWebSearchConsentConflict
		}
	case WebSearchConsentApprovedV1, WebSearchConsentDeniedV1:
		choice := WebSearchConsentApproveV1
		if value.Status == WebSearchConsentDeniedV1 {
			choice = WebSearchConsentDenyV1
		}
		if value.Revision != 2 || value.Decision == nil || value.Decision.Choice != choice || !value.UpdatedAt.Equal(value.Decision.ObservedAt) {
			return ErrWebSearchConsentConflict
		}
	case WebSearchConsentExpiredV1, WebSearchConsentCancelledV1, WebSearchConsentRevokedV1, WebSearchConsentReconcileOnlyV1:
		if (value.Decision == nil && value.Revision != 2) || (value.Decision != nil && (value.Revision != 3 || value.Decision.Choice != WebSearchConsentApproveV1)) {
			return ErrWebSearchConsentConflict
		}
	default:
		return ValidationError{Field: "web_search_consent.status", Reason: "unknown status"}
	}
	return nil
}

// ValidateWebSearchConsentAppendV1 checks remaining shared Attempt storage
// capacity, supplied by the same transaction's existing aggregate reducer.
// Reserve two row slots (request plus eventual decision/stop) and blob bytes,
// not a search dispatch or monetary debit. No separate consent ledger exists.
func ValidateWebSearchConsentAppendV1(existing []WebSearchConsentStateV1, request WebSearchConsentRequestV1, remainingRows, remainingBodyBytes uint64) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if len(existing) >= 2 || remainingRows < 2 || uint64(request.Arguments.Size) > remainingBodyBytes {
		return ValidationError{Field: "web_search_consent.storage", Reason: "requires bounded shared rows/blobs and at most two requests per Attempt"}
	}
	for _, state := range existing {
		if err := state.Validate(); err != nil {
			return err
		}
		if state.Request.Scope != request.Scope || state.Request.InitiatingUserID != request.InitiatingUserID || state.Request.ID == request.ID || state.Status == WebSearchConsentPendingV1 || state.Request.CallID == request.CallID || state.Request.ArgumentsDigest == request.ArgumentsDigest || state.Request.QueryDigest == request.QueryDigest {
			return ErrWebSearchConsentConflict
		}
	}
	return nil
}

// WebSearchConsentAuthorityV1 is only a pure projection of transaction-loaded
// canonical rows. It is NOT self-authenticating and MUST NOT be accepted as
// caller-supplied authority by any store/egress adapter. The same resource
// transaction reloads task initiator, grant consent authority, Web session,
// membership, participation, Run/Attempt/lease/claim and all shorter windows.
type WebSearchConsentAuthorityV1 struct {
	Scope                WebSearchConsentScopeV1
	Run                  Run
	Attempt              Attempt
	Lease                Lease
	WebSession           WebSession
	Membership           TenantMembership
	Participant          SessionParticipant
	InitiatingUserID     UserID
	GrantDigest          string
	ConsentRevision      uint64
	RevocationGeneration uint64
	DataClass            ProviderDataClassV1
	ClaimState           WebSearchConsentClaimStateV1
	InvocationDeadline   time.Time
	ActiveDeadline       time.Time
	GrantExpiresAt       time.Time
	PolicyExpiresAt      time.Time
	PriceExpiresAt       time.Time
	AllocationExpiresAt  time.Time
}

func (value WebSearchConsentAuthorityV1) ValidateCurrent(request WebSearchConsentRequestV1, at time.Time) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if err := value.Scope.Validate(); err != nil {
		return err
	}
	if err := value.Lease.ValidateForAttempt(value.Run, value.Attempt); err != nil {
		return err
	}
	if value.Scope != request.Scope || value.Scope.TenantID != value.Run.TenantID || value.Scope.SessionID != value.Run.SessionID || value.Scope.RunID != value.Run.ID || value.Scope.AttemptID != value.Attempt.ID || value.Scope.LeaseID != value.Lease.ID || value.Scope.FenceToken != value.Lease.FenceToken || value.Attempt.WorkerID != value.Lease.WorkerID || value.InitiatingUserID != request.InitiatingUserID || value.GrantDigest != request.GrantDigest || value.ConsentRevision != request.ConsentRevision || value.RevocationGeneration != request.RevocationGeneration || value.DataClass != ProviderDataPublicV1 {
		return ErrWebSearchConsentAuthority
	}
	if value.ClaimState != WebSearchConsentClaimActiveV1 || value.Run.Status != RunRunning || value.Attempt.Status != AttemptRunning || value.Run.CancellationRequestedAt != nil {
		return ErrWebSearchConsentStopped
	}
	if at.IsZero() || at.Before(request.CreatedAt) || at.Before(value.Run.UpdatedAt) || at.Before(value.Attempt.UpdatedAt) || request.CreatedAt.Before(value.Lease.AcquiredAt) || !value.Lease.ActiveAt(at) || !at.Before(request.ExpiresAt) {
		return ErrWebSearchConsentStopped
	}
	for _, expiry := range []time.Time{value.Lease.ExpiresAt, value.InvocationDeadline, value.ActiveDeadline, value.GrantExpiresAt, value.PolicyExpiresAt, value.PriceExpiresAt, value.AllocationExpiresAt} {
		if expiry.IsZero() || request.ExpiresAt.After(expiry) || !at.Before(expiry) {
			return ErrWebSearchConsentStopped
		}
	}
	return nil
}

func (value WebSearchConsentAuthorityV1) ValidateActor(request WebSearchConsentRequestV1, at time.Time) error {
	if value.WebSession.UserID != request.InitiatingUserID || value.WebSession.ActiveTenantID != request.Scope.TenantID || value.InitiatingUserID != request.InitiatingUserID || at.Before(value.WebSession.LastSeenAt) || at.Before(value.Membership.UpdatedAt) || at.Before(value.Participant.UpdatedAt) {
		return ErrWebSearchConsentAuthority
	}
	if err := value.WebSession.Authorize(value.Membership, TenantPermissionWrite, at); err != nil {
		return err
	}
	return value.Participant.Authorize(request.Scope.TenantID, request.Scope.SessionID, request.InitiatingUserID, true)
}

// DecideWebSearchConsentV1 is a pure CAS reducer. Persist the returned state
// and receipt atomically under the SAME reloaded authority. A recovered receipt
// is append evidence only; it never recovers or mints an egress capability.
func DecideWebSearchConsentV1(state WebSearchConsentStateV1, expectedRevision uint64, choice WebSearchConsentChoiceV1, sessionDigest, csrfDigest SecretDigest, authority WebSearchConsentAuthorityV1, at time.Time) (WebSearchConsentStateV1, error) {
	if err := state.Validate(); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	if err := choice.Validate(); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	if err := authority.ValidateActor(state.Request, at); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	if sessionDigest != authority.WebSession.SessionDigest || csrfDigest != authority.WebSession.CSRFTokenDigest {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentAuthority
	}
	if expectedRevision != 1 {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentConflict
	}
	if state.Status != WebSearchConsentPendingV1 && state.Status != WebSearchConsentApprovedV1 && state.Status != WebSearchConsentDeniedV1 {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentStopped
	}
	if at.Before(state.UpdatedAt) {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentConflict
	}
	if err := authority.ValidateCurrent(state.Request, at); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	if state.Decision != nil && (state.Status == WebSearchConsentApprovedV1 || state.Status == WebSearchConsentDeniedV1) {
		if state.Decision.Choice != choice || state.Decision.ActorUserID != authority.WebSession.UserID || state.Decision.WebSessionDigest != sessionDigest || state.Decision.MembershipSecurityVersion != authority.Membership.SecurityVersion {
			return WebSearchConsentStateV1{}, ErrWebSearchConsentConflict
		}
		return state.Clone(), nil
	}
	digest, _ := state.Request.Digest()
	decision := WebSearchConsentDecisionV1{Version: WebSearchConsentVersionV1, Request: state.Request, RequestDigest: digest, RequestRevision: expectedRevision, ActorUserID: authority.WebSession.UserID, WebSessionDigest: authority.WebSession.SessionDigest, MembershipSecurityVersion: authority.Membership.SecurityVersion, Choice: choice, ObservedAt: at.UTC()}
	next := state.Clone()
	next.Decision = &decision
	next.Revision = 2
	next.UpdatedAt = at.UTC()
	next.Status = WebSearchConsentApprovedV1
	if choice == WebSearchConsentDenyV1 {
		next.Status = WebSearchConsentDeniedV1
	}
	return next, next.Validate()
}

// StopWebSearchConsentV1 records only an observed local stop. It cannot reopen
// a request, extend a deadline, create a human decision, or authorize replay.
// The resource transaction must supply the original Attempt's current rows.
func StopWebSearchConsentV1(state WebSearchConsentStateV1, expectedRevision uint64, authority WebSearchConsentAuthorityV1, at time.Time) (WebSearchConsentStateV1, error) {
	if err := state.Validate(); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	if expectedRevision != state.Revision {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentConflict
	}
	if state.Status != WebSearchConsentPendingV1 && state.Status != WebSearchConsentApprovedV1 {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentStopped
	}
	if at.IsZero() || at.Before(state.UpdatedAt) || at.Before(authority.Run.UpdatedAt) || at.Before(authority.Attempt.UpdatedAt) || authority.Run.TenantID != state.Request.Scope.TenantID || authority.Run.ID != state.Request.Scope.RunID || authority.Run.SessionID != state.Request.Scope.SessionID || authority.Attempt.ID != state.Request.Scope.AttemptID {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentAuthority
	}
	if err := authority.Lease.ValidateForAttempt(authority.Run, authority.Attempt); err != nil {
		return WebSearchConsentStateV1{}, err
	}
	// Pending requests have no selected browser session. Reconciliation must
	// not fabricate one or infer revocation from an empty session projection.
	// Membership/participation still come from actual current transaction rows.
	// An approved receipt instead retains its ORIGINAL authenticated session.
	actorErr := authority.ValidateActor(state.Request, at)
	if state.Decision == nil {
		actorErr = authority.Membership.Authorize(state.Request.InitiatingUserID, state.Request.Scope.TenantID, TenantPermissionWrite)
		if actorErr == nil {
			actorErr = authority.Participant.Authorize(state.Request.Scope.TenantID, state.Request.Scope.SessionID, state.Request.InitiatingUserID, true)
		}
		if at.Before(authority.Membership.UpdatedAt) || at.Before(authority.Participant.UpdatedAt) {
			return WebSearchConsentStateV1{}, ErrWebSearchConsentAuthority
		}
	}
	status := WebSearchConsentStatusV1("")
	switch {
	case authority.Run.CancellationRequestedAt != nil || authority.Run.Status == RunCancelled || authority.Attempt.Status == AttemptCancelled:
		status = WebSearchConsentCancelledV1
	case authority.ClaimState == WebSearchConsentClaimLostV1 || authority.Scope.PhysicalInvocationClaimID != state.Request.Scope.PhysicalInvocationClaimID || authority.Run.Status != RunRunning || authority.Attempt.Status != AttemptRunning:
		status = WebSearchConsentReconcileOnlyV1
	case authority.Scope != state.Request.Scope || authority.InitiatingUserID != state.Request.InitiatingUserID || authority.GrantDigest != state.Request.GrantDigest || authority.ConsentRevision != state.Request.ConsentRevision || authority.RevocationGeneration != state.Request.RevocationGeneration || authority.DataClass != ProviderDataPublicV1:
		status = WebSearchConsentRevokedV1
	case actorErr != nil || (state.Decision != nil && (state.Decision.WebSessionDigest != authority.WebSession.SessionDigest || state.Decision.MembershipSecurityVersion != authority.Membership.SecurityVersion)):
		status = WebSearchConsentRevokedV1
	default:
		for _, expiry := range []time.Time{state.Request.ExpiresAt, authority.Lease.ExpiresAt, authority.InvocationDeadline, authority.ActiveDeadline, authority.GrantExpiresAt, authority.PolicyExpiresAt, authority.PriceExpiresAt, authority.AllocationExpiresAt} {
			if !expiry.IsZero() && !at.Before(expiry) {
				status = WebSearchConsentExpiredV1
				break
			}
		}
	}
	if status == "" {
		return WebSearchConsentStateV1{}, ErrWebSearchConsentAuthority
	}
	next := state.Clone()
	next.Status = status
	next.Revision++
	next.UpdatedAt = at.UTC()
	return next, next.Validate()
}

// WebSearchConsentReceiptBindingV1 is the exact binding expected by operation
// reserve and before-send gates. It is a selector, NEVER a transferable grant.
// Deadline is the original absolute consent expiry, not a replacement window.
type WebSearchConsentReceiptBindingV1 struct {
	Request        WebSearchConsentRequestV1
	RequestDigest  WebSearchConsentRequestDigestV1
	DecisionDigest WebSearchConsentDecisionDigestV1
}

func (value WebSearchConsentReceiptBindingV1) Validate() error {
	digest, err := value.Request.Digest()
	if err != nil {
		return err
	}
	if value.RequestDigest != digest {
		return ErrWebSearchConsentConflict
	}
	return value.DecisionDigest.Validate()
}

// ValidateWebSearchConsentReceiptV1 must run inside the atomic reservation or
// existing one-use before-send gate against authoritative stored state and a
// freshly reloaded transaction projection. Success is not a public capability.
func ValidateWebSearchConsentReceiptV1(state WebSearchConsentStateV1, binding WebSearchConsentReceiptBindingV1, authority WebSearchConsentAuthorityV1, at time.Time) error {
	if err := state.Validate(); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	if state.Status != WebSearchConsentApprovedV1 || state.Decision == nil {
		return ErrWebSearchConsentStopped
	}
	if at.Before(state.UpdatedAt) {
		return ErrWebSearchConsentAuthority
	}
	decisionDigest, _ := state.Decision.Digest()
	if state.Decision.RequestDigest != binding.RequestDigest || decisionDigest != binding.DecisionDigest {
		return ErrWebSearchConsentConflict
	}
	if err := authority.ValidateActor(state.Request, at); err != nil {
		return err
	}
	if authority.WebSession.SessionDigest != state.Decision.WebSessionDigest || authority.Membership.SecurityVersion != state.Decision.MembershipSecurityVersion {
		return ErrWebSearchConsentAuthority
	}
	return authority.ValidateCurrent(state.Request, at)
}
