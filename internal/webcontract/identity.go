package webcontract

import (
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	RouteAccountLoginMethods       = "/api/web/v1/account/login-methods"
	RouteIdentityOperations        = "/api/web/v1/account/identity-operations"
	RouteIdentityOperation         = "/api/web/v1/account/identity-operations/{operation_id}"
	RouteIdentityOperationContinue = RouteIdentityOperation + "/continue"
	RouteIdentityOperationConfirm  = RouteIdentityOperation + "/confirm"
	RouteIdentityOperationCancel   = RouteIdentityOperation + "/cancel"
	RouteAccountLoginMethodsPage   = "/account/login-methods"
)

// These are presentation data, not identity ownership or authorization claims.
// Only the server resolves a method handle against the current owner's set.
type AccountLoginMethod struct {
	Provider          domain.IdentityProvider `json:"provider"`
	MethodHandle      string                  `json:"method_handle,omitempty"`
	Linked            bool                    `json:"linked"`
	Current           bool                    `json:"current"`
	Enabled           bool                    `json:"enabled"`
	AllowedActions    []string                `json:"allowed_actions"`
	RemainingProvider domain.IdentityProvider `json:"remaining_provider,omitempty"`
}

type AccountLoginMethods struct {
	Revision domain.AuthRevisionV2 `json:"revision"`
	Methods  []AccountLoginMethod  `json:"methods"`
}

type IdentityOperationStartRequest struct {
	Kind             domain.IdentityOperationKindV2 `json:"kind"`
	TargetProvider   domain.IdentityProvider        `json:"target_provider,omitempty"`
	MethodHandle     string                         `json:"method_handle,omitempty"`
	ExpectedRevision domain.AuthRevisionV2          `json:"expected_revision"`
}

func (request IdentityOperationStartRequest) Validate() error {
	if err := request.ExpectedRevision.Validate(); err != nil {
		return err
	}
	switch request.Kind {
	case domain.IdentityOperationLinkV2:
		if (request.TargetProvider != domain.IdentityProviderYandex && request.TargetProvider != domain.IdentityProviderTelegram) || request.MethodHandle != "" {
			return domain.ValidationError{Field: "identity_operation.start", Reason: "link requires only a configured provider selector"}
		}
	case domain.IdentityOperationUnlinkV2:
		if request.TargetProvider != "" || domain.ValidateOpaqueID("identity_operation.method_handle", request.MethodHandle) != nil {
			return domain.ValidationError{Field: "identity_operation.start", Reason: "unlink requires only an own method handle"}
		}
	default:
		return domain.ValidationError{Field: "identity_operation.kind", Reason: "must be link or unlink"}
	}
	return nil
}

// Revision is an opaque decimal string on the wire, never a JavaScript number.
// The operation ID is an independent selector; cookies and current authority
// remain required even when a caller knows that ID.
type IdentityOperationRevisionRequest struct {
	ExpectedRevision domain.AuthRevisionV2 `json:"expected_revision"`
}

func (request IdentityOperationRevisionRequest) Validate() error {
	return request.ExpectedRevision.Validate()
}

// Missing/retention-expired outcomes omit unknown phase/revision facts rather
// than manufacturing a terminal receipt. No external subject or proof is public.
type IdentityOperationView struct {
	OperationID    string                            `json:"operation_id"`
	Kind           domain.IdentityOperationKindV2    `json:"kind,omitempty"`
	Phase          domain.IdentityOperationPhaseV2   `json:"phase,omitempty"`
	TargetProvider domain.IdentityProvider           `json:"target_provider,omitempty"`
	Outcome        domain.IdentityOperationOutcomeV2 `json:"outcome,omitempty"`
	ExpiresAt      string                            `json:"expires_at,omitempty"`
	BeforeRevision *domain.AuthRevisionV2            `json:"before_revision,omitempty"`
	AfterRevision  *domain.AuthRevisionV2            `json:"after_revision,omitempty"`
	CompletedAt    string                            `json:"completed_at,omitempty"`
	AllowedActions []string                          `json:"allowed_actions"`
}

type IdentityOperationContinuation struct {
	AuthorizationURL string `json:"authorization_url"`
}
