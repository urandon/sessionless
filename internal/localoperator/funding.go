package localoperator

import (
	"context"
	"crypto/hmac"
	"encoding/hex"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// SpendingPublication derives one original local invoice authorization. It
// deliberately cannot issue MCP/search funding: those require their own real
// scoped source and price, even though the canonical package still requires all
// four independently current components. There is no balance or per-run map.
func (s *Source) SpendingPublication(ctx context.Context, scope domain.AttemptExecutionSourceScopeV1, component domain.AttemptSpendingComponentKindV1) (ports.AttemptSpendingPublicationGrantV1, error) {
	zero := ports.AttemptSpendingPublicationGrantV1{}
	if s == nil || ctx == nil || ctx.Err() != nil || scope.Validate() != nil {
		return zero, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || (component != domain.AttemptSpendingProviderV1 && component != domain.AttemptSpendingSubstrateV1) {
		return zero, ErrDenied
	}
	p := s.receipt.Profile
	now := time.Now().UTC().Truncate(time.Microsecond)
	if p.ValidateAt(now) != nil || s.config.ValidateAt(now) != nil || scope.TenantID != p.Locator.TenantID || scope.OwnerUserID != p.Locator.OwnerUserID {
		return zero, ErrDenied
	}
	mutation, err := idgen.New().NewID(ctx, ports.IDOwnedPublication)
	if err != nil {
		return zero, ErrDenied
	}
	v := domain.AttemptSpendingAuthorizationCurrentV1{
		Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: scope.TenantID, AttemptID: scope.AttemptID, ComponentKind: component, BillingOwnerUserID: p.BillingOwnerUserID},
		RunID: scope.RunID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, SourceKind: domain.AttemptSpendingOwnerLocalSourceV1,
		SourceInstallationID: p.InstallationID, SourceInstallationRevision: p.InstallationRevision,
		SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: s.receipt.Digest},
		ResourceID:    p.Resource.ResourceID, ResourceRevision: p.Resource.Revision, ResourceGeneration: p.Resource.CredentialGeneration,
		ProfileID: string(p.Locator.Profile), ProfileRevision: p.ProfileRevision, ScopedGrantDigest: p.EffectivePolicyDigest,
		Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest,
		PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1,
		Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: now, PublishedAt: now, ExpiresAt: p.Window.ExpiresAt}, UpdatedAt: now, MutationID: mutation,
	}
	if component == domain.AttemptSpendingSubstrateV1 {
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
		v.ProfileID, v.ProfileRevision = p.Substrate.ProfileID, p.Substrate.ProfileRevision
		v.ScopedGrantDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest = p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
	}
	// Bound the original Attempt envelope; a later call cannot top up an existing
	// row, and the Store independently CAS-rejects a second initial publication.
	end := now.Add(p.Limits.MaxActiveDuration + p.Limits.MaxCleanupDuration)
	if end.Before(v.Window.ExpiresAt) {
		v.Window.ExpiresAt = end
	}
	raw, err := v.SourceCanonicalBytes()
	if err != nil {
		return zero, ErrDenied
	}
	v.SourceAuthorization = domain.OwnedAgentAuthorityBodyRefV1{Ref: "local-invoice-" + hex.EncodeToString(s.mac("invoice-source", raw)), Digest: hash(raw), Bytes: uint64(len(raw))}
	if v.ValidateAgainstProfile(p, now) != nil {
		return zero, ErrDenied
	}
	expires := now.Add(10 * time.Second)
	if v.Window.ExpiresAt.Before(expires) {
		expires = v.Window.ExpiresAt
	}
	g := ports.AttemptSpendingPublicationGrantV1{Authorization: v, IssuedAt: now, ExpiresAt: expires, Authenticator: make([]byte, 32)}
	intent, err := g.IntentBytes()
	if err != nil {
		return zero, ErrDenied
	}
	g.Authenticator = s.mac("spending-publication", intent)
	return g, nil
}

// Verification is I/O-free and authenticates both the actual retained local
// source and exact original source-envelope bytes. It grants no current DB,
// Session/member, lease/fence, reservation, credential or model-call authority.
func (s *Source) VerifyAttemptSpendingPublication(g ports.AttemptSpendingPublicationGrantV1, at time.Time) error {
	if s == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || g.Validate() != nil || at.Before(g.IssuedAt) || !at.Before(g.ExpiresAt) || g.ExpectedGeneration != 0 {
		return ErrDenied
	}
	v := g.Authorization
	if v.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 || (v.Locator.ComponentKind != domain.AttemptSpendingProviderV1 && v.Locator.ComponentKind != domain.AttemptSpendingSubstrateV1) || v.ValidateAgainstProfile(s.receipt.Profile, g.IssuedAt) != nil || s.config.ValidateAt(g.IssuedAt) != nil {
		return ErrDenied
	}
	raw, err := v.SourceCanonicalBytes()
	if err != nil || v.SourceAuthorization.Ref != "local-invoice-"+hex.EncodeToString(s.mac("invoice-source", raw)) {
		return ErrDenied
	}
	intent, err := g.IntentBytes()
	if err != nil || !hmac.Equal(g.Authenticator, s.mac("spending-publication", intent)) {
		return ErrDenied
	}
	return nil
}
