package localoperator

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// A declaration is optional for retained installations. Presence requires the
// original immutable V2 grant's exact credential identity, not the MODEL
// resource or grant resource revision/generation reused as credential metadata.
func validateSearchCredentialManifest(m ManifestV1) error {
	if m.SearchCredential == nil {
		return nil
	}
	d := *m.SearchCredential
	if d.Validate() != nil || m.Profile.WebSearchResourceGrant == nil || m.SearchExpenditure == nil ||
		d.TenantID != m.Configuration.TenantID || d.Resource.OwnerUserID != m.Configuration.OwnerUserID ||
		d.TenantID != m.Profile.Locator.TenantID || d.Resource.OwnerUserID != m.Profile.Locator.OwnerUserID {
		return ErrDenied
	}
	body, err := exactSearchEvidence(m.Evidence, m.Profile.WebSearchResourceGrant.Body, domain.WebSearchResourceGrantMaxBytesV2)
	if err != nil {
		return ErrDenied
	}
	g, err := domain.DecodeWebSearchResourceGrantV2(body)
	digest, de := g.Digest()
	if err != nil || de != nil || digest != m.Profile.WebSearchGrantDigest || d.TenantID != g.TenantID || d.Resource.OwnerUserID != g.OwnerUserID || d.Resource.ResourceID != g.CredentialRef || d.Audience != g.CredentialAudience {
		return ErrDenied
	}
	return nil
}

// SearchCredentialConfiguration returns only nonsecret constructor DATA. Nil
// means authenticated omission, never a closed/invalid Source or a serving
// declaration lookup failure. Serving must still use the exact-profile reader
// below and independently check current binding, grant, consent and send rights.
func (s *Source) SearchCredentialConfiguration() (*ports.SearchCredentialDeclarationV1, error) {
	if s == nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipt == nil || s.verifyOriginalInstallationLocked(*s.receipt) != nil {
		return nil, ErrDenied
	}
	if s.searchCredential == nil {
		return nil, nil
	}
	declaration := *s.searchCredential
	return &declaration, nil
}

// ReadSearchCredentialDeclaration returns only original nonsecret DATA for the
// exact installed profile. Omission, closure and expiry deny; no current/latest
// generation or environment lookup exists. The lifecycle independently checks
// the current credential binding and the original Store owns consent/send.
func (s *Source) ReadSearchCredentialDeclaration(ctx context.Context, ref domain.OwnedAgentProfileReferenceV1) (ports.SearchCredentialDeclarationV1, error) {
	if s == nil || ctx == nil || ctx.Err() != nil {
		return ports.SearchCredentialDeclarationV1{}, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.searchCredential == nil || s.receipt == nil || s.verifyOriginalInstallationLocked(*s.receipt) != nil ||
		ref != (domain.OwnedAgentProfileReferenceV1{Locator: s.receipt.Profile.Locator, RevocationGeneration: s.receipt.Profile.RevocationGeneration, Digest: s.receipt.Digest}) {
		return ports.SearchCredentialDeclarationV1{}, ErrDenied
	}
	if _, err := s.searchWindowLocked(s.receipt.Profile, time.Now().UTC().Truncate(time.Microsecond)); err != nil || ctx.Err() != nil {
		return ports.SearchCredentialDeclarationV1{}, ErrDenied
	}
	return *s.searchCredential, nil
}
