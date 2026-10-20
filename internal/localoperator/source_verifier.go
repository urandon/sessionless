package localoperator

import (
	"bytes"
	"crypto/hmac"
	"encoding/hex"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.OwnedAttemptEffectSourceVerifierV1 = (*Source)(nil)

// VerifyOwnedAttemptEffectSource authenticates original installation and funding
// DATA, including an expired Attempt while this original installation is alive.
// It performs no I/O and issues nothing. The caller must independently join
// current SQL authority, original physical claimant, lifetime and all send gates.
// Closed/expired installations retain their existing key destruction semantics.
func (s *Source) VerifyOwnedAttemptEffectSource(v domain.AttemptExecutionSourceV1) error {
	if s == nil || v.Validate() != nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipt == nil || s.verifyOriginalInstallationLocked(*s.receipt) != nil {
		return ErrDenied
	}
	p := s.receipt.Profile
	if s.config.ValidateAt(v.IssuedAt) != nil ||
		v.Profile != (domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: s.receipt.Digest}) ||
		!hasBody(s.bodies, s.config.CapabilityEvidence) || !hasBody(s.bodies, p.ReviewReceipt) {
		return ErrDenied
	}
	for _, component := range v.Spending {
		if s.verifyOriginalSpendingLocked(component, v.IssuedAt) != nil {
			return ErrDenied
		}
	}
	derived, err := domain.BuildOwnedAgentExecutionPackageV1(v.Scope, p, v.Spending, v.IssuedAt)
	if err != nil {
		return ErrDenied
	}
	want, err := domain.EncodeAttemptExecutionSourceV1(derived.Source)
	if err != nil {
		return ErrDenied
	}
	defer clear(want)
	got, err := domain.EncodeAttemptExecutionSourceV1(v)
	if err != nil {
		return ErrDenied
	}
	defer clear(got)
	if !bytes.Equal(want, got) || !s.aliveLocked() {
		return ErrDenied
	}
	return nil
}

// VerifyOriginalSpendingSources authenticates only original installation and
// preserved component DATA. Empty components authenticate the SAME privately
// retained receipt, not absence, funding or permission to issue. The trusted
// Store separately proves current heads/absence/audits in its transaction.
// No grant is reconstructed, signed, installed or accepted by this read.
func (s *Source) VerifyOriginalSpendingSources(receipt ports.OwnedProfilePublicationReceiptV1, components []domain.AttemptSpendingAuthorizationCurrentV1) error {
	if s == nil || len(components) > 4 {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.verifyOriginalInstallationLocked(receipt) != nil {
		return ErrDenied
	}
	var seen uint8
	for _, component := range components {
		var bit uint8
		switch component.Locator.ComponentKind {
		case domain.AttemptSpendingProviderV1:
			bit = 1
		case domain.AttemptSpendingSubstrateV1:
			bit = 2
		case domain.AttemptSpendingMCPToolV1:
			bit = 4
		case domain.AttemptSpendingWebSearchV1:
			bit = 8
		default:
			return ErrDenied
		}
		if seen&bit != 0 || s.config.ValidateAt(component.Window.ObservedAt) != nil || s.verifyOriginalSpendingLocked(component, component.Window.PublishedAt) != nil {
			return ErrDenied
		}
		seen |= bit
	}
	if !s.aliveLocked() {
		return ErrDenied
	}
	return nil
}

func (s *Source) verifyOriginalInstallationLocked(receipt ports.OwnedProfilePublicationReceiptV1) error {
	if !s.aliveLocked() || s.receipt == nil || !s.receiptMatchesLocked(*s.receipt) || receipt.Digest != s.receipt.Digest ||
		len(s.manifest) == 0 || len(s.manifest) > maxOperatorManifestBytes || uint64(len(s.manifest)) != s.config.OriginalSource.Bytes ||
		hash(s.manifest) != s.config.OriginalSource.Digest || s.receipt.Profile.InstalledManifest != s.config.OriginalSource {
		return ErrDenied
	}
	digest, err := receipt.Profile.Digest()
	p := s.receipt.Profile
	if err != nil || digest != s.receipt.Digest || s.config.ValidateForOwnedProfile(p) != nil ||
		!hasBody(s.bodies, s.config.CapabilityEvidence) || !hasBody(s.bodies, p.ReviewReceipt) {
		return ErrDenied
	}
	invoice, err := exactSearchEvidence(s.bodies, s.config.OperatorTerms, 4096)
	var terms InvoiceTermsV1
	if err != nil || strictJSON(invoice, &terms) != nil || !validTerms(terms, s.config, p) {
		return ErrDenied
	}
	return nil
}

func (s *Source) verifyOriginalSpendingLocked(component domain.AttemptSpendingAuthorizationCurrentV1, at time.Time) error {
	p := s.receipt.Profile
	if component.SourceKind != domain.AttemptSpendingOwnerLocalSourceV1 || component.ValidateAgainstProfile(p, at) != nil {
		return ErrDenied
	}
	observed := component.Window.ObservedAt
	end := observed.Add(p.Limits.MaxActiveDuration + p.Limits.MaxCleanupDuration)
	if component.Window.ExpiresAt.After(end) {
		return ErrDenied
	}
	kind, prefix := "invoice-source", "local-invoice-"
	switch component.Locator.ComponentKind {
	case domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1:
	case domain.AttemptSpendingMCPToolV1:
		_, bound, err := s.mcpFundingLocked(observed)
		if err != nil || component.Window.ExpiresAt.After(bound) {
			return ErrDenied
		}
		kind, prefix = "mcp-source", "local-mcp-"
	case domain.AttemptSpendingWebSearchV1:
		bound, err := s.searchWindowLocked(p, observed)
		if err != nil || component.Window.ExpiresAt.After(bound) {
			return ErrDenied
		}
	default:
		return ErrDenied
	}
	raw, err := component.SourceCanonicalBytes()
	if err != nil {
		return ErrDenied
	}
	valid := uint64(len(raw)) == component.SourceAuthorization.Bytes && hash(raw) == component.SourceAuthorization.Digest &&
		hmac.Equal([]byte(component.SourceAuthorization.Ref), []byte(prefix+hex.EncodeToString(s.mac(kind, raw))))
	clear(raw)
	if !valid {
		return ErrDenied
	}
	return nil
}
