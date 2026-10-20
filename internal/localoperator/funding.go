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

// SpendingPublication derives one original local invoice authorization or an
// explicitly accepted search expenditure envelope. Provider/substrate retain
// their original no-invoice source; MCP uses its independent retained service
// grant, reports and documents. Search is never
// inferred from those zero terms. There is no balance or per-run map.
func (s *Source) SpendingPublication(ctx context.Context, scope domain.AttemptExecutionSourceScopeV1, component domain.AttemptSpendingComponentKindV1) (ports.AttemptSpendingPublicationGrantV1, error) {
	return s.spendingPublication(ctx, scope, component, time.Time{})
}

// SpendingPublicationBefore uses the SAME trusted original issuer, allowing
// only an earlier exclusive envelope end. It cannot renew an existing row;
// the Store must first prove this component absent and retain the original
// earliest window. The cap is applied BEFORE either original MAC is produced.
func (s *Source) SpendingPublicationBefore(ctx context.Context, scope domain.AttemptExecutionSourceScopeV1, component domain.AttemptSpendingComponentKindV1, end time.Time) (ports.AttemptSpendingPublicationGrantV1, error) {
	if end.IsZero() || end.Year() < 1 || end.Year() > 9999 || end.Nanosecond()%1000 != 0 {
		return ports.AttemptSpendingPublicationGrantV1{}, ErrDenied
	}
	return s.spendingPublication(ctx, scope, component, end.UTC())
}

func (s *Source) spendingPublication(ctx context.Context, scope domain.AttemptExecutionSourceScopeV1, component domain.AttemptSpendingComponentKindV1, originalEnd time.Time) (ports.AttemptSpendingPublicationGrantV1, error) {
	zero := ports.AttemptSpendingPublicationGrantV1{}
	if s == nil || ctx == nil || ctx.Err() != nil || scope.Validate() != nil {
		return zero, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || (component != domain.AttemptSpendingProviderV1 && component != domain.AttemptSpendingSubstrateV1 && component != domain.AttemptSpendingMCPToolV1 && component != domain.AttemptSpendingWebSearchV1) {
		return zero, ErrDenied
	}
	p := s.receipt.Profile
	now := time.Now().UTC().Truncate(time.Microsecond)
	if p.ValidateAt(now) != nil || s.config.ValidateAt(now) != nil || scope.TenantID != p.Locator.TenantID || scope.OwnerUserID != p.Locator.OwnerUserID {
		return zero, ErrDenied
	}
	if !originalEnd.IsZero() {
		// This recovery issuer serves a complete four-component package. Bound
		// its first missing grant by BOTH retained service windows before MAC
		// production, so a later component cannot reveal an earlier ceiling.
		_, mcpEnd, err := s.mcpFundingLocked(now)
		if err != nil {
			return zero, ErrDenied
		}
		searchEnd, err := s.searchWindowLocked(p, now)
		if err != nil {
			return zero, ErrDenied
		}
		for _, limit := range []time.Time{mcpEnd, searchEnd} {
			if limit.Before(originalEnd) {
				originalEnd = limit
			}
		}
	}
	var searchEnd time.Time
	if component == domain.AttemptSpendingWebSearchV1 {
		var err error
		searchEnd, err = s.searchWindowLocked(p, now)
		if err != nil {
			return zero, ErrDenied
		}
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
	if component == domain.AttemptSpendingWebSearchV1 {
		c := p.CostCeiling.WebSearchCost // authenticated retained acceptance above
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = c.ResourceID, c.ResourceRevision, c.RevocationGeneration
		v.ProfileID, v.ProfileRevision = c.ProfileID, c.ProfileRevision
		v.ScopedGrantDigest, v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest = c.ScopedGrantDigest, c.PriceRevision, c.PriceEvidenceDigest, c.TermsEvidenceDigest
		v.PriceState = domain.ExternalServicePriceKnownPaidV1
		amount := s.search.expenditure.AggregateReservationMicrounits
		v.MaxAmountMicrounits = &amount
		v.Window.ExpiresAt = searchEnd
	}
	sourceKind, sourcePrefix := "invoice-source", "local-invoice-"
	if component == domain.AttemptSpendingMCPToolV1 {
		cost, end, err := s.mcpFundingLocked(now)
		if err != nil {
			return zero, ErrDenied
		}
		v.ResourceID, v.ResourceRevision, v.ResourceGeneration = cost.ResourceID, cost.ResourceRevision, cost.RevocationGeneration
		v.ProfileID, v.ProfileRevision, v.ScopedGrantDigest = cost.ProfileID, cost.ProfileRevision, cost.ScopedGrantDigest
		v.Currency, v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest = cost.Currency, cost.PriceRevision, cost.PriceEvidenceDigest, cost.TermsEvidenceDigest
		v.PriceState, v.MaxAmountMicrounits = cost.PriceState, cost.MaxAmountMicrounits
		if end.Before(v.Window.ExpiresAt) {
			v.Window.ExpiresAt = end
		}
		sourceKind, sourcePrefix = "mcp-source", "local-mcp-"
	}
	// Bound the original Attempt envelope; a later call cannot top up an existing
	// row, and the Store independently CAS-rejects a second initial publication.
	end := now.Add(p.Limits.MaxActiveDuration + p.Limits.MaxCleanupDuration)
	if end.Before(v.Window.ExpiresAt) {
		v.Window.ExpiresAt = end
	}
	if !originalEnd.IsZero() {
		if !originalEnd.After(now) {
			return zero, ErrDenied
		}
		if originalEnd.Before(v.Window.ExpiresAt) {
			v.Window.ExpiresAt = originalEnd
		}
	}
	raw, err := v.SourceCanonicalBytes()
	if err != nil {
		return zero, ErrDenied
	}
	v.SourceAuthorization = domain.OwnedAgentAuthorityBodyRefV1{Ref: sourcePrefix + hex.EncodeToString(s.mac(sourceKind, raw)), Digest: hash(raw), Bytes: uint64(len(raw))}
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
	if ctx.Err() != nil || !s.aliveLocked() {
		return zero, ErrDenied
	}
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
	if v.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 || (v.Locator.ComponentKind != domain.AttemptSpendingProviderV1 && v.Locator.ComponentKind != domain.AttemptSpendingSubstrateV1 && v.Locator.ComponentKind != domain.AttemptSpendingMCPToolV1 && v.Locator.ComponentKind != domain.AttemptSpendingWebSearchV1) || v.ValidateAgainstProfile(s.receipt.Profile, g.IssuedAt) != nil || s.config.ValidateAt(g.IssuedAt) != nil {
		return ErrDenied
	}
	if v.Locator.ComponentKind == domain.AttemptSpendingWebSearchV1 {
		end, err := s.searchWindowLocked(s.receipt.Profile, g.IssuedAt)
		if err != nil || !at.Before(end) || v.Window.ExpiresAt.After(end) || g.ExpiresAt.After(end) {
			return ErrDenied
		}
	}
	sourceKind, sourcePrefix := "invoice-source", "local-invoice-"
	if v.Locator.ComponentKind == domain.AttemptSpendingMCPToolV1 {
		_, end, err := s.mcpFundingLocked(g.IssuedAt)
		if err != nil || !at.Before(end) || v.Window.ExpiresAt.After(end) || g.ExpiresAt.After(end) {
			return ErrDenied
		}
		sourceKind, sourcePrefix = "mcp-source", "local-mcp-"
	}
	raw, err := v.SourceCanonicalBytes()
	if err != nil || v.SourceAuthorization.Ref != sourcePrefix+hex.EncodeToString(s.mac(sourceKind, raw)) {
		return ErrDenied
	}
	intent, err := g.IntentBytes()
	if err != nil || !hmac.Equal(g.Authenticator, s.mac("spending-publication", intent)) {
		return ErrDenied
	}
	if !s.aliveLocked() {
		return ErrDenied
	}
	return nil
}
