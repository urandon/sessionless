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
	RecordWebSecurityEvent(ctx context.Context, event domain.WebSecurityAuditEvent) error
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

// ObjectCapability is a short-lived, exact-object browser capability. Headers
// are authenticated by signed headers or provider-generated query fields and
// therefore must be sent verbatim by the browser. Capability URLs are secrets
// and must never be logged or persisted.
type ObjectCapability struct {
	Method    string
	URL       string
	Headers   map[string]string
	ExpiresAt time.Time
}

// UploadCapabilityRequest binds a direct browser PUT to immutable intent
// metadata. SHA256 is the lowercase hexadecimal digest used by the domain;
// adapters translate it to the wire representation required by the provider.
type UploadCapabilityRequest struct {
	TenantID  domain.TenantID
	ObjectKey string
	MediaType string
	Size      int64
	SHA256    string
	ExpiresIn time.Duration
}

// ObjectMetadata is authoritative storage metadata obtained with an exact-key
// HEAD. ETag is used as the source precondition during promotion.
type ObjectMetadata struct {
	Blob      domain.BlobRef
	MediaType string
	ETag      string
}

// PromoteObjectRequest copies a verified staging object to an immutable final
// key. SourceETag prevents a browser overwrite between HEAD and COPY, while the
// destination is created only if it does not already exist.
type PromoteObjectRequest struct {
	TenantID   domain.TenantID
	Source     domain.BlobRef
	SourceETag string
	FinalKey   string
	MediaType  string
}

// WebObjectStore is deliberately exact-object only: regular web request paths
// cannot list a bucket or a tenant prefix.
type WebObjectStore interface {
	PresignUpload(ctx context.Context, request UploadCapabilityRequest) (ObjectCapability, error)
	StatObject(ctx context.Context, tenantID domain.TenantID, key string) (ObjectMetadata, error)
	PromoteObject(ctx context.Context, request PromoteObjectRequest) (domain.BlobRef, error)
	PresignDownload(
		ctx context.Context,
		tenantID domain.TenantID,
		ref domain.BlobRef,
		expiresIn time.Duration,
	) (ObjectCapability, error)
}
