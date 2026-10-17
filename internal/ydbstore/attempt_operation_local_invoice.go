package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

// Private, captured ORIGINAL custody. It is not caller evidence or a provider
// tariff; the Source/lifetime association cannot be reconstructed from terms.
type ownedLocalInvoiceEvidence struct {
	source        *localoperator.Source
	lifetime      context.Context
	configuration domain.ConfiguredInferenceProfileV1
	terms         localoperator.InvoiceTermsV1
	digest        string
}

func (store *Store) localInvoiceLifetimeCurrent(e *ownedLocalInvoiceEvidence) bool {
	return store != nil && store.localCompositionReady() && store.localBootstrap != nil &&
		e != nil && e.source != nil && store.localBootstrap.source == e.source && e.lifetime != nil && e.lifetime.Err() == nil
}

// Capture bytes outside retryable SQL using only the original completed local
// bootstrap. A missing local producer cannot fall back to standard-price data.
func (store *Store) readOwnedLocalInvoiceEvidence(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1) (*ownedProviderPriceEvidence, error) {
	if store == nil || !store.localCompositionReady() || store.localBootstrap == nil || store.localBootstrap.source == nil || ctx == nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	s := store.localBootstrap.source
	c, err := s.Configuration()
	if err != nil || profile.Resource.Kind != domain.ProviderResourceLocalEndpointV1 || c.ValidateForOwnedProfile(profile) != nil {
		return nil, domain.ErrMembershipDenied
	}
	digest, err := profile.Digest()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	ref := domain.OwnedAgentProfileReferenceV1{Locator: profile.Locator, RevocationGeneration: profile.RevocationGeneration, Digest: digest}
	life, err := s.ExecutionLifetime()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	body, err := s.ReadInvoiceTerms(ctx, ref, ownedProviderPriceBodyBytes)
	if err != nil || len(body) == 0 || len(body) > ownedProviderPriceBodyBytes || chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return nil, domain.ErrMembershipDenied
	}
	defer clear(body)
	var terms localoperator.InvoiceTermsV1
	if json.Unmarshal(body, &terms) != nil {
		return nil, domain.ErrMembershipDenied
	}
	canonical, err := json.Marshal(terms)
	if err != nil || !bytes.Equal(body, canonical) {
		return nil, domain.ErrMembershipDenied
	}
	sum := sha256.Sum256(body)
	e := &ownedLocalInvoiceEvidence{source: s, lifetime: life, configuration: c.Clone(), terms: terms, digest: hex.EncodeToString(sum[:])}
	if ctx.Err() != nil || !store.localInvoiceLifetimeCurrent(e) {
		return nil, domain.ErrMembershipDenied
	}
	return &ownedProviderPriceEvidence{profile: profile.Clone(), local: e}, nil
}

// This pure current check uses original DB time, never filesystem/network I/O
// or a supplied approval callback. Zero means invoice only, not economic cost.
func (store *Store) validateOwnedLocalInvoiceEvidence(e *ownedLocalInvoiceEvidence, p domain.OwnedAgentProfileCurrentV1, source domain.AttemptExecutionSourceV1, op domain.AttemptOperationV1, at time.Time) error {
	if !store.localInvoiceLifetimeCurrent(e) || p.ValidateAt(at) != nil || source.Validate() != nil || e.configuration.ValidateAt(at) != nil || e.configuration.ValidateForOwnedProfile(p) != nil {
		return domain.ErrMembershipDenied
	}
	t, c, spending := e.terms, p.CostCeiling, source.Spending[0]
	pd, err := p.Digest()
	if err != nil || source.Profile.Digest != pd || spending.SourceProfile != source.Profile || spending.ValidateAgainstProfile(p, at) != nil ||
		spending.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 || spending.Locator.ComponentKind != domain.AttemptSpendingProviderV1 ||
		p.Resource.Kind != domain.ProviderResourceLocalEndpointV1 || p.Backend.BackendKind != domain.HarnessBackendDirectOpenAICompatibleV1 || p.Backend.ProviderContractKind != domain.ProviderContractConfiguredEndpointV1 || p.Substrate.Kind != domain.SubstrateLocalDevelopmentV1 ||
		p.BillingOwnerUserID != p.Locator.OwnerUserID || spending.Locator.BillingOwnerUserID != p.BillingOwnerUserID ||
		t.Version != 1 || t.TenantID != p.Locator.TenantID || t.OwnerUserID != p.Locator.OwnerUserID || t.BaseURL != e.configuration.BaseURL || t.ModelVendorID != p.Locator.ModelVendorID || t.ModelID != p.Locator.ModelID ||
		t.Currency != c.Currency || t.Currency != spending.Currency || op.Currency != t.Currency || t.Revision != c.PriceRevision || t.Revision != spending.PriceRevision || op.PriceRevision != t.Revision ||
		!t.OwnerSuppliesHost || !t.OwnerSuppliesModel || t.ProviderInvoiceAmount != 0 || t.SubstrateInvoiceAmount != 0 ||
		!t.IssuedAt.Equal(e.configuration.IssuedAt) || !t.ExpiresAt.Equal(e.configuration.ExpiresAt) ||
		e.digest != e.configuration.OperatorTerms.Digest || e.digest != p.ProviderPriceDigest || e.digest != p.ProviderTermsDigest || e.digest != spending.PriceEvidenceDigest || e.digest != spending.TermsEvidenceDigest ||
		c.ProviderPriceState != domain.ProviderPriceKnownFreeV1 || spending.PriceState != domain.ExternalServicePriceKnownFreeV1 || c.MaxProviderAmountMicrounits == nil || *c.MaxProviderAmountMicrounits != 0 || spending.MaxAmountMicrounits == nil || *spending.MaxAmountMicrounits != 0 || op.Debit.AmountMicrounits != 0 ||
		(op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) || op.Debit.InputTokens == 0 || op.Debit.OutputTokens == 0 || op.Debit.InputTokens > e.configuration.Limits.MaxInputTokens || op.Debit.OutputTokens > e.configuration.Limits.MaxOutputTokens ||
		at.Before(t.IssuedAt) || !at.Before(t.ExpiresAt) || at.Before(c.PriceObservedAt) || !at.Before(c.PriceExpiresAt) || op.Deadline.After(t.ExpiresAt) || op.Deadline.After(c.PriceExpiresAt) || t.IssuedAt.After(p.Window.PublishedAt) || c.PriceObservedAt.After(p.Window.PublishedAt) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Recheck after the final current transaction before minting/consuming an
// operation capability. Historical recovery has no new send capability and
// does not reconstruct local custody after Source closure.
func (store *Store) operationEvidenceLifetimeCurrent(e *ownedOperationEvidence) bool {
	return e == nil || e.price == nil || e.price.local == nil || store.localInvoiceLifetimeCurrent(e.price.local)
}
