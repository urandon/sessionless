package ports

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// IdentityAccountServiceV2 is the trusted joined service, not a Store installer
// or a browser proof-recording API. Construction requires an already configured
// V2 Store and the identical immutable provider manifest. Nil at the BFF keeps
// ordinary production login unchanged. No method accepts identity ownership,
// verified provider observations, clocks, endpoints or configuration from HTTP.
//
// Methods, Outcome and CallbackPurpose are read-only: no session activity touch,
// pruning or replay. Mutations and Callback inherit one original 15s request
// context; their existing <=5s SQL and <=10s provider children cannot renew it.
type IdentityAccountServiceV2 interface {
	Methods(context.Context, IdentityAccountMethodsRequestV2) (IdentityAccountMethodsViewV2, error)
	Start(context.Context, IdentityAccountStartRequestV2) (IdentityAccountStartResultV2, error)
	Continue(context.Context, IdentityAccountContinueRequestV2) (IdentityAccountContinueResultV2, error)
	CallbackPurpose(context.Context, IdentityAccountCallbackPurposeRequestV2) (IdentityAccountCallbackPurposeV2, error)
	Callback(context.Context, IdentityAccountCallbackRequestV2) (IdentityAccountOperationViewV2, error)
	Confirm(context.Context, IdentityAccountConfirmRequestV2) (IdentityAccountConfirmResultV2, error)
	Outcome(context.Context, IdentityAccountOutcomeRequestV2) (IdentityAccountOperationViewV2, error)
	Cancel(context.Context, IdentityAccountCancelRequestV2) (IdentityAccountOperationViewV2, error)
}

// Raw cookie/protocol values are server-private, ephemeral and never JSON DTOs.
// Exact Origin and cookie/header equality are checked by the BFF; the service
// still authenticates their digests against CURRENT canonical rows in the
// owning transaction. A preflight Web authorization is not mutation authority.
type IdentityAccountReadAuthV2 struct {
	SessionCookie string
}

type IdentityAccountMutationAuthV2 struct {
	SessionCookie string
	CSRFCookie    string
	PresentedCSRF string
}

type IdentityAccountMethodsRequestV2 struct {
	Auth IdentityAccountReadAuthV2
}

type IdentityAccountStartRequestV2 struct {
	Auth                    IdentityAccountMutationAuthV2
	Kind                    domain.IdentityOperationKindV2
	TargetProvider          domain.IdentityProvider // Link only.
	MethodHandle            string                  // Unlink only; own opaque handle.
	ExpectedRevision        domain.AuthRevisionV2
	PreviousOperationCookie string // Optional exact previous operation, not a global browser index.
}

type IdentityAccountContinueRequestV2 struct {
	Auth             IdentityAccountMutationAuthV2
	OperationID      string
	OperationCookie  string
	ExpectedRevision domain.AuthRevisionV2
}

// This lookup selects ONLY the canonical stored purpose and configured callback
// binding. It never consumes, exchanges, infers purpose from cookie presence or
// falls back from a failed login decoder to proof. LOGIN remains the ordinary
// BFF handler's branch; Callback below accepts only PROVIDER_PROOF.
type IdentityAccountCallbackPurposeRequestV2 struct {
	CallbackPath string
	State        string
}

type IdentityAccountCallbackPurposeV2 uint8

const (
	IdentityAccountCallbackLoginV2 IdentityAccountCallbackPurposeV2 = iota + 1
	IdentityAccountCallbackProofV2
)

type IdentityAccountCallbackAuthV2 struct {
	SessionCookie      string
	OperationCookie    string
}

// Code and ProviderDenied are exclusive. ProviderDenied is only the closed
// callback denial signal, not an arbitrary upstream error/description. A proof
// callback requires the original session and operation browser binding, but NOT
// a SameSite=Strict CSRF cookie/header after cross-provider navigation. The
// owning consume transaction derives its original CSRF digest from that exact
// current session; other mutation paths retain presented-CSRF requirements.
type IdentityAccountCallbackRequestV2 struct {
	Auth           IdentityAccountCallbackAuthV2
	CallbackPath   string
	State          string
	Code           string
	ProviderDenied bool
}

type IdentityAccountConfirmRequestV2 struct {
	Auth             IdentityAccountMutationAuthV2
	OperationID      string
	OperationCookie  string
	ExpectedRevision domain.AuthRevisionV2
}

type IdentityAccountOutcomeRequestV2 struct {
	Auth        IdentityAccountReadAuthV2
	OperationID string
	// Optional action hints ONLY. Missing or wrong cookie must not deny a
	// current same-user GET/receipt or cause a session activity refresh.
	OperationCookie string
}

type IdentityAccountCancelRequestV2 struct {
	Auth             IdentityAccountMutationAuthV2
	OperationID      string
	OperationCookie  string
	ExpectedRevision domain.AuthRevisionV2
}

type IdentityAccountActionV2 string

const (
	IdentityAccountActionLinkV2     IdentityAccountActionV2 = "link"
	IdentityAccountActionUnlinkV2   IdentityAccountActionV2 = "unlink"
	IdentityAccountActionContinueV2 IdentityAccountActionV2 = "continue"
	IdentityAccountActionConfirmV2  IdentityAccountActionV2 = "confirm"
	IdentityAccountActionCancelV2   IdentityAccountActionV2 = "cancel"
)

// Exactly the two known provider rows. A handle is present only for an own
// linked method; it binds owner/provider/subject/revision without disclosing
// those private identifiers. AllowedActions contains only link/unlink here and
// is a projection, never reusable permission or provider availability proof.
type IdentityAccountMethodViewV2 struct {
	Provider       domain.IdentityProvider
	MethodHandle   string
	Linked         bool
	Current        bool
	Enabled        bool
	// Set only when the server allows unlink; no browser eligibility guess.
	RemainingProvider domain.IdentityProvider
	AllowedActions []IdentityAccountActionV2
}

type IdentityAccountMethodsViewV2 struct {
	Revision domain.AuthRevisionV2
	Methods  []IdentityAccountMethodViewV2
}

// Stored facts only. Missing retained outcomes expose outcome_unavailable, not
// a fabricated terminal receipt. Pending expiry is not a completed transaction.
// AllowedActions is restricted to continue/confirm/cancel and contributes no
// mutation authority. Pending hints are empty unless the current ORIGINAL
// session/browser/revision/manifest and sensitivity match in this SAME read
// transaction. Hint failure never denies an otherwise authorized same-user
// view. TargetProvider may be omitted when no owned row exists.
type IdentityAccountOperationViewV2 struct {
	OperationID      string
	Kind             domain.IdentityOperationKindV2
	Phase            domain.IdentityOperationPhaseV2
	TargetProvider   domain.IdentityProvider
	Outcome          domain.IdentityOperationOutcomeV2
	ExpiresAt        time.Time
	BeforeRevision   domain.AuthRevisionV2
	AfterRevision    *domain.AuthRevisionV2
	CompletedAt      *time.Time
	AllowedActions   []IdentityAccountActionV2
}

// OperationCookie encodes opaque operation ID + independent 256-bit browser
// secret. Start reserves both phase-state digests in the original parent before
// Continue. A previous-operation cancellation ambiguity aborts the new start:
// never publish a replacement cookie or claim a cancellation that is unknown.
type IdentityAccountStartResultV2 struct {
	View            IdentityAccountOperationViewV2
	OperationCookie string
}

// This is the ONLY authorization-URL return path: generated by the configured
// prover after authenticating the exact stored phase/unconsumed child. The BFF
// deliberately projects it to the authorized continue response, not logs or a
// general raw provider-response DTO. Reuse never resets consumed secrets.
type IdentityAccountContinueResultV2 struct {
	AuthorizationURL string
}

type IdentityAccountSessionCookiesV2 struct {
	SessionCookie string
	CSRFCookie    string
}

// Cookies is nonnil ONLY for an actually newly committed replacement session.
// Receipt-only replay returns View with nil Cookies. Lost replacements require
// relogin; neither this result nor Outcome can recreate invalidated cookies.
type IdentityAccountConfirmResultV2 struct {
	View    IdentityAccountOperationViewV2
	Cookies *IdentityAccountSessionCookiesV2
}

// Closed, content-free service failures. Errors are not durable receipts and
// must never terminalize an operation merely because a request failed. Unknown
// storage/transport outcomes map to Unavailable (temporarily_unavailable/503),
// not the legitimate retained-view outcome_unavailable/410. Unauthenticated
// maps to 401. No raw provider error escapes.
type IdentityAccountErrorV2 uint8

const (
	IdentityAccountAccessDeniedV2 IdentityAccountErrorV2 = iota + 1
	IdentityAccountUnavailableV2
	IdentityAccountUnauthenticatedV2
	IdentityAccountConflictV2
	IdentityAccountStaleRevisionV2
	IdentityAccountProofRequiredV2
	IdentityAccountProofUnavailableV2
	IdentityAccountLastMethodV2
	IdentityAccountProviderUnavailableV2
	IdentityAccountExpiredV2
)

func (failure IdentityAccountErrorV2) Error() string {
	switch failure {
	case IdentityAccountAccessDeniedV2: return "access_denied"
	case IdentityAccountUnauthenticatedV2: return "unauthenticated"
	case IdentityAccountUnavailableV2: return "temporarily_unavailable"
	case IdentityAccountConflictV2: return "conflict"
	case IdentityAccountStaleRevisionV2: return "stale_revision"
	case IdentityAccountProofRequiredV2: return "proof_required"
	case IdentityAccountProofUnavailableV2: return "proof_unavailable"
	case IdentityAccountLastMethodV2: return "last_method"
	case IdentityAccountProviderUnavailableV2: return "provider_unavailable"
	case IdentityAccountExpiredV2: return "expired"
	default: return "temporarily_unavailable"
	}
}

func (IdentityAccountReadAuthV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account authentication is ephemeral") }
func (IdentityAccountMutationAuthV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account authentication is ephemeral") }
func (IdentityAccountCallbackAuthV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account authentication is ephemeral") }
func (IdentityAccountCallbackPurposeRequestV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity callback protocol is ephemeral") }
func (IdentityAccountStartResultV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account cookie result is ephemeral") }
func (IdentityAccountContinueResultV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account authorization is ephemeral") }
func (IdentityAccountSessionCookiesV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account replacement cookies are ephemeral") }
func (IdentityAccountConfirmResultV2) MarshalJSON() ([]byte, error) { return nil, errors.New("identity account confirmation result is ephemeral") }
