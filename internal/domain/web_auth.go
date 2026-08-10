package domain

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const DevelopmentEnvironment = "cloud-dev"

var (
	ErrLoginChallengeConsumed    = errors.New("login challenge has already been consumed")
	ErrLoginChallengeExpired     = errors.New("login challenge has expired")
	ErrEnrollmentGrantRequired   = errors.New("an explicit enrollment grant is required")
	ErrInvitationConsumed        = errors.New("tenant invitation has already been consumed")
	ErrInvitationExpired         = errors.New("tenant invitation has expired")
	ErrInvitationSubjectMismatch = errors.New("tenant invitation is bound to another external subject")
	ErrMembershipDenied          = errors.New("tenant membership does not authorize this operation")
	ErrWebSessionExpired         = errors.New("web session has expired")
	ErrWebSessionRevoked         = errors.New("web session has been revoked")
	ErrMembershipVersionChanged  = errors.New("tenant membership security version has changed")
	ErrExternalIdentityConflict  = errors.New("external subject is already mapped to another user")
	ErrWebSessionRotation        = errors.New("web session rotation invariants were violated")
	ErrUploadIntentExpired       = errors.New("upload intent has expired")
	ErrUploadIntentCommitted     = errors.New("upload intent has already been committed")
)

// SecretDigest is the lowercase SHA-256 digest stored for browser-facing
// bearer material. Raw state, invitation, session, and CSRF secrets must never
// be persisted in application tables.
type SecretDigest string

func DigestSecret(secret string) SecretDigest {
	digest := sha256.Sum256([]byte(secret))
	return SecretDigest(hex.EncodeToString(digest[:]))
}

func (digest SecretDigest) Validate(field string) error {
	decoded, err := hex.DecodeString(string(digest))
	if err != nil || len(decoded) != sha256.Size || string(digest) != strings.ToLower(string(digest)) {
		return ValidationError{Field: field, Reason: "must be a lowercase SHA-256 digest"}
	}
	return nil
}

func (digest SecretDigest) Matches(secret string) bool {
	actual := DigestSecret(secret)
	return subtle.ConstantTimeCompare([]byte(digest), []byte(actual)) == 1
}

type IdentityProvider string

const IdentityProviderTelegram IdentityProvider = "telegram"

func (provider IdentityProvider) Validate() error {
	return ValidateOpaqueID("identity_provider", string(provider))
}

type ExternalSubject struct {
	Provider IdentityProvider `json:"provider"`
	Subject  string           `json:"subject"`
}

func (subject ExternalSubject) Validate() error {
	if err := subject.Provider.Validate(); err != nil {
		return err
	}
	return ValidateOpaqueID("external_subject", subject.Subject)
}

type ExternalIdentity struct {
	Subject   ExternalSubject `json:"subject"`
	UserID    UserID          `json:"user_id"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func (identity ExternalIdentity) Validate() error {
	if err := identity.Subject.Validate(); err != nil {
		return err
	}
	if err := identity.UserID.Validate(); err != nil {
		return err
	}
	if identity.CreatedAt.IsZero() || identity.UpdatedAt.Before(identity.CreatedAt) {
		return ValidationError{Field: "external_identity.updated_at", Reason: "must not be before a non-zero created_at"}
	}
	return nil
}

func ValidateExternalIdentityMapping(existing, candidate ExternalIdentity) error {
	if err := existing.Validate(); err != nil {
		return err
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if existing.Subject != candidate.Subject || existing.UserID != candidate.UserID ||
		!existing.CreatedAt.Equal(candidate.CreatedAt) {
		return ErrExternalIdentityConflict
	}
	if candidate.UpdatedAt.Before(existing.UpdatedAt) {
		return ValidationError{Field: "external_identity.updated_at", Reason: "must not move backwards"}
	}
	return nil
}

type TenantMembershipRole string
type TenantMembershipStatus string
type TenantPermission string

const (
	TenantMembershipOwner     TenantMembershipRole   = "owner"
	TenantMembershipMember    TenantMembershipRole   = "member"
	TenantMembershipViewer    TenantMembershipRole   = "viewer"
	TenantMembershipActive    TenantMembershipStatus = "active"
	TenantMembershipSuspended TenantMembershipStatus = "suspended"
	TenantMembershipRevoked   TenantMembershipStatus = "revoked"
	TenantPermissionRead      TenantPermission       = "read"
	TenantPermissionWrite     TenantPermission       = "write"
	TenantPermissionAdmin     TenantPermission       = "admin"
)

type TenantMembership struct {
	TenantID        TenantID               `json:"tenant_id"`
	UserID          UserID                 `json:"user_id"`
	Role            TenantMembershipRole   `json:"role"`
	Status          TenantMembershipStatus `json:"status"`
	SecurityVersion uint64                 `json:"security_version"`
	CreatedAt       time.Time              `json:"created_at"`
	UpdatedAt       time.Time              `json:"updated_at"`
}

func (membership TenantMembership) Validate() error {
	if err := membership.TenantID.Validate(); err != nil {
		return err
	}
	if err := membership.UserID.Validate(); err != nil {
		return err
	}
	switch membership.Role {
	case TenantMembershipOwner, TenantMembershipMember, TenantMembershipViewer:
	default:
		return ValidationError{Field: "tenant_membership.role", Reason: "is unknown"}
	}
	switch membership.Status {
	case TenantMembershipActive, TenantMembershipSuspended, TenantMembershipRevoked:
	default:
		return ValidationError{Field: "tenant_membership.status", Reason: "is unknown"}
	}
	if membership.SecurityVersion == 0 {
		return ValidationError{Field: "tenant_membership.security_version", Reason: "must be positive"}
	}
	if membership.CreatedAt.IsZero() || membership.UpdatedAt.Before(membership.CreatedAt) {
		return ValidationError{Field: "tenant_membership.updated_at", Reason: "must not be before a non-zero created_at"}
	}
	return nil
}

func (membership TenantMembership) Authorize(userID UserID, tenantID TenantID, permission TenantPermission) error {
	if err := membership.Validate(); err != nil {
		return err
	}
	if membership.UserID != userID || membership.TenantID != tenantID || membership.Status != TenantMembershipActive {
		return ErrMembershipDenied
	}
	switch permission {
	case TenantPermissionRead:
		return nil
	case TenantPermissionWrite:
		if membership.Role != TenantMembershipViewer {
			return nil
		}
	case TenantPermissionAdmin:
		if membership.Role == TenantMembershipOwner {
			return nil
		}
	default:
		return ValidationError{Field: "tenant_permission", Reason: "is unknown"}
	}
	return ErrMembershipDenied
}

type EnrollmentSource string

const (
	EnrollmentExistingFrontend     EnrollmentSource = "existing_frontend"
	EnrollmentTenantInvitation     EnrollmentSource = "tenant_invitation"
	EnrollmentDevelopmentBootstrap EnrollmentSource = "development_bootstrap"
)

type TenantInvitation struct {
	ID            TenantInvitationID   `json:"id"`
	TenantID      TenantID             `json:"tenant_id"`
	SecretDigest  SecretDigest         `json:"secret_digest"`
	Role          TenantMembershipRole `json:"role"`
	TargetSubject *ExternalSubject     `json:"target_subject,omitempty"`
	ExpiresAt     time.Time            `json:"expires_at"`
	ConsumedAt    *time.Time           `json:"consumed_at,omitempty"`
	ConsumedBy    *UserID              `json:"consumed_by,omitempty"`
	CreatedAt     time.Time            `json:"created_at"`
}

func (invitation TenantInvitation) Validate() error {
	if err := invitation.ID.Validate(); err != nil {
		return err
	}
	if err := invitation.TenantID.Validate(); err != nil {
		return err
	}
	if err := invitation.SecretDigest.Validate("tenant_invitation.secret_digest"); err != nil {
		return err
	}
	if invitation.Role != TenantMembershipOwner && invitation.Role != TenantMembershipMember && invitation.Role != TenantMembershipViewer {
		return ValidationError{Field: "tenant_invitation.role", Reason: "is unknown"}
	}
	if invitation.TargetSubject != nil {
		if err := invitation.TargetSubject.Validate(); err != nil {
			return err
		}
	}
	if invitation.CreatedAt.IsZero() || !invitation.ExpiresAt.After(invitation.CreatedAt) {
		return ValidationError{Field: "tenant_invitation.expires_at", Reason: "must be after a non-zero created_at"}
	}
	if (invitation.ConsumedAt == nil) != (invitation.ConsumedBy == nil) {
		return ValidationError{Field: "tenant_invitation.consumption", Reason: "consumed_at and consumed_by must be set together"}
	}
	if invitation.ConsumedBy != nil {
		if err := invitation.ConsumedBy.Validate(); err != nil {
			return err
		}
		if invitation.ConsumedAt.Before(invitation.CreatedAt) || !invitation.ConsumedAt.Before(invitation.ExpiresAt) {
			return ValidationError{Field: "tenant_invitation.consumed_at", Reason: "must be within the invitation lifetime"}
		}
	}
	return nil
}

func (invitation *TenantInvitation) Consume(subject ExternalSubject, userID UserID, at time.Time) error {
	if invitation == nil {
		return ValidationError{Field: "tenant_invitation", Reason: "must not be nil"}
	}
	if err := invitation.Validate(); err != nil {
		return err
	}
	if err := subject.Validate(); err != nil {
		return err
	}
	if err := userID.Validate(); err != nil {
		return err
	}
	if invitation.ConsumedAt != nil {
		return ErrInvitationConsumed
	}
	if at.Before(invitation.CreatedAt) {
		return ValidationError{Field: "tenant_invitation.consumed_at", Reason: "must not be before created_at"}
	}
	if !at.Before(invitation.ExpiresAt) {
		return ErrInvitationExpired
	}
	if invitation.TargetSubject != nil && *invitation.TargetSubject != subject {
		return ErrInvitationSubjectMismatch
	}
	invitation.ConsumedAt, invitation.ConsumedBy = &at, &userID
	return nil
}

type DevelopmentBootstrapGrant struct {
	TenantID    TenantID             `json:"tenant_id"`
	UserID      UserID               `json:"user_id"`
	Role        TenantMembershipRole `json:"role"`
	Environment string               `json:"environment"`
	Operator    string               `json:"operator"`
	Reason      string               `json:"reason"`
	GrantedAt   time.Time            `json:"granted_at"`
}

func (grant DevelopmentBootstrapGrant) Validate() error {
	if err := grant.TenantID.Validate(); err != nil {
		return err
	}
	if err := grant.UserID.Validate(); err != nil {
		return err
	}
	if grant.Role != TenantMembershipOwner && grant.Role != TenantMembershipMember && grant.Role != TenantMembershipViewer {
		return ValidationError{Field: "development_bootstrap.role", Reason: "is unknown"}
	}
	if grant.Environment != DevelopmentEnvironment {
		return ValidationError{Field: "development_bootstrap.environment", Reason: "must be cloud-dev"}
	}
	if strings.TrimSpace(grant.Operator) == "" || strings.TrimSpace(grant.Reason) == "" {
		return ValidationError{Field: "development_bootstrap.audit", Reason: "operator and reason are required"}
	}
	if grant.GrantedAt.IsZero() {
		return ValidationError{Field: "development_bootstrap.granted_at", Reason: "must not be zero"}
	}
	return nil
}

type EnrollmentCandidates struct {
	ExistingFrontendBindingID *FrontendBindingID
	Invitation                *TenantInvitation
	Bootstrap                 *DevelopmentBootstrapGrant
}

// SelectEnrollmentSource freezes the precedence used by the persistence
// transaction. The selected grant still has to be atomically consumed there.
func SelectEnrollmentSource(
	candidates EnrollmentCandidates,
	subject ExternalSubject,
	userID UserID,
	at time.Time,
) (EnrollmentSource, error) {
	if err := subject.Validate(); err != nil {
		return "", err
	}
	if err := userID.Validate(); err != nil {
		return "", err
	}
	if candidates.ExistingFrontendBindingID != nil {
		if err := candidates.ExistingFrontendBindingID.Validate(); err != nil {
			return "", err
		}
		return EnrollmentExistingFrontend, nil
	}
	if candidates.Invitation != nil {
		if err := candidates.Invitation.Validate(); err != nil {
			return "", err
		}
		if candidates.Invitation.ConsumedAt != nil {
			return "", ErrInvitationConsumed
		}
		if !at.Before(candidates.Invitation.ExpiresAt) {
			return "", ErrInvitationExpired
		}
		if candidates.Invitation.TargetSubject != nil && *candidates.Invitation.TargetSubject != subject {
			return "", ErrInvitationSubjectMismatch
		}
		return EnrollmentTenantInvitation, nil
	}
	if candidates.Bootstrap != nil {
		if err := candidates.Bootstrap.Validate(); err != nil {
			return "", err
		}
		if candidates.Bootstrap.UserID != userID {
			return "", ErrMembershipDenied
		}
		return EnrollmentDevelopmentBootstrap, nil
	}
	return "", ErrEnrollmentGrantRequired
}

type OIDCLoginChallenge struct {
	StateDigest          SecretDigest `json:"state_digest"`
	BrowserBindingDigest SecretDigest `json:"browser_binding_digest"`
	PKCEVerifier         string       `json:"pkce_verifier"`
	Nonce                string       `json:"nonce"`
	RedirectPath         string       `json:"redirect_path"`
	CreatedAt            time.Time    `json:"created_at"`
	ExpiresAt            time.Time    `json:"expires_at"`
	ConsumedAt           *time.Time   `json:"consumed_at,omitempty"`
}

func (challenge OIDCLoginChallenge) Validate() error {
	if err := challenge.StateDigest.Validate("oidc_challenge.state_digest"); err != nil {
		return err
	}
	if err := challenge.BrowserBindingDigest.Validate("oidc_challenge.browser_binding_digest"); err != nil {
		return err
	}
	if err := ValidatePKCEVerifier(challenge.PKCEVerifier); err != nil {
		return err
	}
	if err := ValidateOpaqueID("oidc_challenge.nonce", challenge.Nonce); err != nil {
		return err
	}
	if !safeLocalRedirect(challenge.RedirectPath) {
		return ValidationError{Field: "oidc_challenge.redirect_path", Reason: "must be a local absolute path"}
	}
	if challenge.CreatedAt.IsZero() || !challenge.ExpiresAt.After(challenge.CreatedAt) {
		return ValidationError{Field: "oidc_challenge.expires_at", Reason: "must be after a non-zero created_at"}
	}
	if challenge.ConsumedAt != nil && challenge.ConsumedAt.Before(challenge.CreatedAt) {
		return ValidationError{Field: "oidc_challenge.consumed_at", Reason: "must not be before created_at"}
	}
	return nil
}

func (challenge *OIDCLoginChallenge) Consume(browserBindingSecret string, at time.Time) error {
	if challenge == nil {
		return ValidationError{Field: "oidc_challenge", Reason: "must not be nil"}
	}
	if err := challenge.Validate(); err != nil {
		return err
	}
	if challenge.ConsumedAt != nil {
		return ErrLoginChallengeConsumed
	}
	if at.Before(challenge.CreatedAt) {
		return ValidationError{Field: "oidc_challenge.consumed_at", Reason: "must not be before created_at"}
	}
	if !at.Before(challenge.ExpiresAt) {
		return ErrLoginChallengeExpired
	}
	if !challenge.BrowserBindingDigest.Matches(browserBindingSecret) {
		return ErrMembershipDenied
	}
	challenge.ConsumedAt = &at
	return nil
}

type OIDCVerificationPolicy struct {
	Issuer            string
	Audience          string
	AllowedAlgorithms []string
	MaxClockSkew      time.Duration
}

func (policy OIDCVerificationPolicy) Validate() error {
	if strings.TrimSpace(policy.Issuer) == "" || strings.TrimSpace(policy.Audience) == "" {
		return ValidationError{Field: "oidc.policy", Reason: "issuer and audience are required"}
	}
	issuer, err := url.Parse(policy.Issuer)
	if err != nil || issuer.Scheme != "https" || issuer.Host == "" || issuer.User != nil ||
		issuer.Path != "" || issuer.RawQuery != "" || issuer.Fragment != "" {
		return ValidationError{Field: "oidc.policy.issuer", Reason: "must be an HTTPS origin"}
	}
	if policy.MaxClockSkew < 0 {
		return ValidationError{Field: "oidc.policy.max_clock_skew", Reason: "must not be negative"}
	}
	if len(policy.AllowedAlgorithms) == 0 {
		return ValidationError{Field: "oidc.policy.allowed_algorithms", Reason: "must not be empty"}
	}
	seen := make(map[string]struct{}, len(policy.AllowedAlgorithms))
	for _, algorithm := range policy.AllowedAlgorithms {
		if err := ValidateOpaqueID("oidc.policy.allowed_algorithm", algorithm); err != nil {
			return err
		}
		if algorithm == "none" {
			return ValidationError{Field: "oidc.policy.allowed_algorithm", Reason: "none is forbidden"}
		}
		if _, exists := seen[algorithm]; exists {
			return ValidationError{Field: "oidc.policy.allowed_algorithms", Reason: "must not contain duplicates"}
		}
		seen[algorithm] = struct{}{}
	}
	return nil
}

type OIDCIdentityClaims struct {
	Issuer    string
	Audience  []string
	Subject   string
	Nonce     string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

type WebSecurityAuditAction string

const (
	WebSecurityLoginFailed  WebSecurityAuditAction = "web.login.failed"
	WebSecurityCSRFRejected WebSecurityAuditAction = "web.csrf.rejected"
)

// WebSecurityAuditEvent is the redacted, durable record for authentication
// failures that do not necessarily have a tenant-scoped audit destination.
// SubjectFingerprint is a one-way digest of the verified provider subject;
// raw claims and browser credentials never cross this boundary.
type WebSecurityAuditEvent struct {
	RequestID                 string                 `json:"request_id"`
	Action                    WebSecurityAuditAction `json:"action"`
	Provider                  IdentityProvider       `json:"provider"`
	SubjectFingerprint        SecretDigest           `json:"subject_fingerprint,omitempty"`
	TenantID                  TenantID               `json:"tenant_id,omitempty"`
	UserID                    UserID                 `json:"user_id,omitempty"`
	MembershipSecurityVersion uint64                 `json:"membership_security_version,omitempty"`
	ReasonCode                string                 `json:"reason_code"`
	OccurredAt                time.Time              `json:"occurred_at"`
}

func (event WebSecurityAuditEvent) Validate() error {
	if err := ValidateOpaqueID("web_security_audit.request_id", event.RequestID); err != nil {
		return err
	}
	switch event.Action {
	case WebSecurityLoginFailed, WebSecurityCSRFRejected:
	default:
		return ValidationError{Field: "web_security_audit.action", Reason: "is unknown"}
	}
	if err := event.Provider.Validate(); err != nil {
		return err
	}
	if event.SubjectFingerprint != "" {
		if err := event.SubjectFingerprint.Validate("web_security_audit.subject_fingerprint"); err != nil {
			return err
		}
	}
	if event.TenantID != "" {
		if err := event.TenantID.Validate(); err != nil {
			return err
		}
	}
	if event.UserID != "" {
		if err := event.UserID.Validate(); err != nil {
			return err
		}
	}
	if event.MembershipSecurityVersion > 0 && (event.TenantID == "" || event.UserID == "") {
		return ValidationError{Field: "web_security_audit.membership_security_version", Reason: "requires tenant and user"}
	}
	if err := ValidateOpaqueID("web_security_audit.reason_code", event.ReasonCode); err != nil {
		return err
	}
	if event.OccurredAt.IsZero() {
		return ValidationError{Field: "web_security_audit.occurred_at", Reason: "must not be zero"}
	}
	if event.Action == WebSecurityCSRFRejected &&
		(event.TenantID == "" || event.UserID == "" || event.MembershipSecurityVersion == 0) {
		return ValidationError{Field: "web_security_audit.csrf", Reason: "requires authorized tenant, user, and membership version"}
	}
	return nil
}

// Verify checks claims only after an OIDC adapter has verified the JWT
// signature against a bounded JWKS cache and an allowed algorithm.
func (claims OIDCIdentityClaims) Verify(policy OIDCVerificationPolicy, expectedNonce string, now time.Time) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if now.IsZero() {
		return ValidationError{Field: "oidc.now", Reason: "must not be zero"}
	}
	if claims.Issuer != policy.Issuer {
		return ValidationError{Field: "oidc.issuer", Reason: "does not match the configured issuer"}
	}
	audienceMatch := false
	for _, audience := range claims.Audience {
		audienceMatch = audienceMatch || audience == policy.Audience
	}
	if !audienceMatch {
		return ValidationError{Field: "oidc.audience", Reason: "does not contain the configured client"}
	}
	if err := ValidateOpaqueID("oidc.subject", claims.Subject); err != nil {
		return err
	}
	if expectedNonce == "" || subtle.ConstantTimeCompare([]byte(claims.Nonce), []byte(expectedNonce)) != 1 {
		return ValidationError{Field: "oidc.nonce", Reason: "does not match the login challenge"}
	}
	if claims.ExpiresAt.IsZero() || !now.Before(claims.ExpiresAt.Add(policy.MaxClockSkew)) {
		return ValidationError{Field: "oidc.expires_at", Reason: "token has expired"}
	}
	if claims.IssuedAt.After(now.Add(policy.MaxClockSkew)) {
		return ValidationError{Field: "oidc.issued_at", Reason: "is in the future"}
	}
	if claims.IssuedAt.IsZero() || !claims.ExpiresAt.After(claims.IssuedAt) {
		return ValidationError{Field: "oidc.token_lifetime", Reason: "exp must be after a non-zero iat"}
	}
	return nil
}

type WebSession struct {
	SessionDigest             SecretDigest    `json:"session_digest"`
	CSRFTokenDigest           SecretDigest    `json:"csrf_token_digest"`
	UserID                    UserID          `json:"user_id"`
	ActiveTenantID            TenantID        `json:"active_tenant_id"`
	AuthenticatedSubject      ExternalSubject `json:"authenticated_subject"`
	MembershipSecurityVersion uint64          `json:"membership_security_version"`
	IssuedAt                  time.Time       `json:"issued_at"`
	LastSeenAt                time.Time       `json:"last_seen_at"`
	IdleExpiresAt             time.Time       `json:"idle_expires_at"`
	AbsoluteExpiresAt         time.Time       `json:"absolute_expires_at"`
	RevokedAt                 *time.Time      `json:"revoked_at,omitempty"`
}

func (session WebSession) Validate() error {
	if err := session.SessionDigest.Validate("web_session.session_digest"); err != nil {
		return err
	}
	if err := session.CSRFTokenDigest.Validate("web_session.csrf_token_digest"); err != nil {
		return err
	}
	if err := session.UserID.Validate(); err != nil {
		return err
	}
	if err := session.ActiveTenantID.Validate(); err != nil {
		return err
	}
	if err := session.AuthenticatedSubject.Validate(); err != nil {
		return err
	}
	if session.MembershipSecurityVersion == 0 {
		return ValidationError{Field: "web_session.membership_security_version", Reason: "must be positive"}
	}
	if session.IssuedAt.IsZero() || session.LastSeenAt.Before(session.IssuedAt) ||
		!session.IdleExpiresAt.After(session.LastSeenAt) || !session.AbsoluteExpiresAt.After(session.IssuedAt) ||
		session.IdleExpiresAt.After(session.AbsoluteExpiresAt) {
		return ValidationError{Field: "web_session.expiry", Reason: "timestamps are inconsistent"}
	}
	if session.RevokedAt != nil && session.RevokedAt.Before(session.IssuedAt) {
		return ValidationError{Field: "web_session.revoked_at", Reason: "must not be before issued_at"}
	}
	return nil
}

func (session WebSession) Authorize(membership TenantMembership, permission TenantPermission, now time.Time) error {
	if err := session.Validate(); err != nil {
		return err
	}
	if session.RevokedAt != nil {
		return ErrWebSessionRevoked
	}
	if !now.Before(session.IdleExpiresAt) || !now.Before(session.AbsoluteExpiresAt) {
		return ErrWebSessionExpired
	}
	if err := membership.Authorize(session.UserID, session.ActiveTenantID, permission); err != nil {
		return err
	}
	if session.MembershipSecurityVersion != membership.SecurityVersion {
		return ErrMembershipVersionChanged
	}
	return nil
}

func (session *WebSession) Revoke(at time.Time) error {
	if session == nil {
		return ValidationError{Field: "web_session", Reason: "must not be nil"}
	}
	if err := session.Validate(); err != nil {
		return err
	}
	if at.Before(session.IssuedAt) {
		return ValidationError{Field: "web_session.revoked_at", Reason: "must not be before issued_at"}
	}
	if session.RevokedAt == nil {
		session.RevokedAt = &at
	}
	return nil
}

// ValidateWebSessionRotation is the invariant checked inside login and tenant
// switch transactions before the previous digest is revoked and the next one
// is inserted.
func ValidateWebSessionRotation(previous, next WebSession, selected TenantMembership, at time.Time) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if previous.RevokedAt != nil || !at.Before(previous.IdleExpiresAt) || !at.Before(previous.AbsoluteExpiresAt) {
		return ErrWebSessionRotation
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if previous.UserID != next.UserID || previous.AuthenticatedSubject != next.AuthenticatedSubject ||
		previous.SessionDigest == next.SessionDigest || previous.CSRFTokenDigest == next.CSRFTokenDigest ||
		!next.IssuedAt.Equal(at) {
		return ErrWebSessionRotation
	}
	if err := selected.Authorize(next.UserID, next.ActiveTenantID, TenantPermissionRead); err != nil {
		return err
	}
	if next.MembershipSecurityVersion != selected.SecurityVersion {
		return ErrMembershipVersionChanged
	}
	return nil
}

func ValidatePKCEVerifier(verifier string) error {
	if len(verifier) < 43 || len(verifier) > 128 {
		return ValidationError{Field: "oidc_challenge.pkce_verifier", Reason: "must contain 43 to 128 characters"}
	}
	for _, character := range verifier {
		if !((character >= 'A' && character <= 'Z') || (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("-._~", character)) {
			return ValidationError{Field: "oidc_challenge.pkce_verifier", Reason: "contains a character outside the RFC 7636 unreserved set"}
		}
	}
	return nil
}

func safeLocalRedirect(target string) bool {
	parsed, err := url.Parse(target)
	return err == nil && strings.HasPrefix(target, "/") && !strings.HasPrefix(target, "//") &&
		parsed.IsAbs() == false && parsed.Host == "" && !strings.Contains(target, "\\") &&
		!strings.Contains(parsed.Path, "\\") && !containsControl(parsed.Path)
}

func containsControl(value string) bool {
	return strings.IndexFunc(value, func(character rune) bool { return character < 0x20 || character == 0x7f }) >= 0
}

func (source EnrollmentSource) String() string { return string(source) }

func (subject ExternalSubject) String() string {
	return fmt.Sprintf("%s:%s", subject.Provider, subject.Subject)
}
