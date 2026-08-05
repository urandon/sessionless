package ports

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OIDCAuthorizationRequest contains browser-facing protocol values only. The
// client secret remains inside the provider adapter and Lockbox.
type OIDCAuthorizationRequest struct {
	Provider      domain.IdentityProvider
	RedirectURI   string
	State         string
	Nonce         string
	CodeChallenge string
	Scopes        []string
}

type OIDCTokenRequest struct {
	Provider      domain.IdentityProvider
	Code          string
	RedirectURI   string
	PKCEVerifier  string
	ExpectedNonce string
	Policy        domain.OIDCVerificationPolicy
	Now           time.Time
}

// OIDCProvider exchanges and verifies the authorization code server-side. It
// returns identity claims only; access and ID tokens must not cross this port.
type OIDCProvider interface {
	AuthorizationURL(ctx context.Context, request OIDCAuthorizationRequest) (string, error)
	ExchangeAndVerify(ctx context.Context, request OIDCTokenRequest) (domain.OIDCIdentityClaims, error)
}

type EnrollmentRequest struct {
	Identity          domain.ExternalIdentity
	Source            domain.EnrollmentSource
	TenantID          domain.TenantID
	FrontendBindingID *domain.FrontendBindingID
	InvitationID      *domain.TenantInvitationID
	InvitationDigest  *domain.SecretDigest
	Bootstrap         *domain.DevelopmentBootstrapGrant
	At                time.Time
}

type WebAuthorization struct {
	Session    domain.WebSession
	Membership domain.TenantMembership
}

// WebAuthStore is the WEB-02 persistence contract. Challenge consumption,
// invitation consumption, membership creation, and session rotation are each
// single serializable operations. Selectors supplied by a browser never bypass
// the membership lookup performed by AuthorizeWebSession or SwitchTenant.
type WebAuthStore interface {
	CreateLoginChallenge(ctx context.Context, challenge domain.OIDCLoginChallenge) error
	ConsumeLoginChallenge(
		ctx context.Context,
		stateDigest domain.SecretDigest,
		browserBindingSecret string,
		at time.Time,
	) (domain.OIDCLoginChallenge, error)
	ResolveOrCreateExternalIdentity(
		ctx context.Context,
		subject domain.ExternalSubject,
		candidateUserID domain.UserID,
		at time.Time,
	) (identity domain.ExternalIdentity, created bool, err error)
	ListTenantMemberships(ctx context.Context, userID domain.UserID, limit uint64) ([]domain.TenantMembership, error)
	Enroll(ctx context.Context, request EnrollmentRequest) (domain.TenantMembership, error)
	BootstrapDevelopmentMembership(
		ctx context.Context,
		grant domain.DevelopmentBootstrapGrant,
	) (domain.TenantMembership, error)
	CreateWebSession(ctx context.Context, session domain.WebSession) error
	AuthorizeWebSession(
		ctx context.Context,
		sessionDigest domain.SecretDigest,
		permission domain.TenantPermission,
		at time.Time,
	) (WebAuthorization, error)
	SwitchTenant(
		ctx context.Context,
		currentDigest domain.SecretDigest,
		next domain.WebSession,
		selectedTenantID domain.TenantID,
		at time.Time,
	) (WebAuthorization, error)
	RevokeWebSession(ctx context.Context, sessionDigest domain.SecretDigest, at time.Time) error
}

// UploadIntentStore authorizes the target session on creation and again on
// commit. Commit obtains the object metadata from storage; browser-supplied
// size, digest, tenant, and object key are never authoritative.
type UploadIntentStore interface {
	CreateUploadIntent(ctx context.Context, intent domain.UploadIntent) error
	CommitUploadIntent(
		ctx context.Context,
		tenantID domain.TenantID,
		userID domain.UserID,
		intentID domain.UploadIntentID,
		observed domain.BlobRef,
		at time.Time,
	) (domain.UploadIntent, error)
}
