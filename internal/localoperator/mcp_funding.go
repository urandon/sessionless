package localoperator

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
)

// retainedMCPBodyLocked reads only original private-file bytes, never a URL or
// caller reference. Intake already bounded the entire list/manifest; every use
// independently checks the requested body's exact size/hash and role limit.
func (s *Source) retainedMCPBodyLocked(digest string, cap uint64) ([]byte, bool) {
	for _, b := range s.bodies {
		if b.Reference.Digest == digest && b.Reference.Validate() == nil && b.Reference.Bytes > 0 && b.Reference.Bytes <= cap &&
			b.Reference.Bytes == uint64(len(b.Body)) && hash(b.Body) == digest {
			return b.Body, true
		}
	}
	return nil, false
}

func (s *Source) retainedMCPDocumentLocked(ref domain.OwnedAgentAuthorityBodyRefV1) bool {
	// Local invoice bytes have a different meaning. An operator must retain
	// independent external-service documentation, not relabel a zero invoice.
	if ref.Validate() != nil || ref.Bytes == 0 || ref.Bytes > mcpadmission.GrantBodyBytesV1 || ref.Digest == s.config.OperatorTerms.Digest {
		return false
	}
	if s.receipt == nil || s.receipt.Profile.CostCeiling.MCPToolCost == nil {
		return false
	}
	p := s.receipt.Profile
	cost := p.CostCeiling.MCPToolCost
	// A renamed grant/report remains a declaration, not its independent source
	// document. Compare digests so alternate retained reference names cannot
	// conceal this role substitution. One real document may support both reports.
	if ref.Digest == p.MCPToolGrantDigest || ref.Digest == cost.PriceEvidenceDigest || ref.Digest == cost.TermsEvidenceDigest {
		return false
	}
	for _, b := range s.bodies {
		if b.Reference == ref && b.Reference.Bytes == uint64(len(b.Body)) && hash(b.Body) == ref.Digest {
			return true
		}
	}
	return false
}

// mcpFundingLocked interprets the operator's two independently retained reports
// against the actual installed grant and receipt, not only copied cost digests.
// It is I/O-free, including inside publisher SQL retries. Private file custody
// authenticates the declaration, not official tariffs or live qualification.
func (s *Source) mcpFundingLocked(at time.Time) (domain.ExternalServiceCostCeilingV1, time.Time, error) {
	deny := func() (domain.ExternalServiceCostCeilingV1, time.Time, error) {
		return domain.ExternalServiceCostCeilingV1{}, time.Time{}, ErrDenied
	}
	if !s.aliveLocked() || s.receipt == nil || s.config.ValidateForOwnedProfile(s.receipt.Profile) != nil {
		return deny()
	}
	p := s.receipt.Profile
	if p.Locator.InputDataClass != domain.ProviderDataPublicV1 || p.CostCeiling.MCPToolCost == nil {
		return deny()
	}
	cost := p.CostCeiling.MCPToolCost.Clone()
	rawGrant, ok := s.retainedMCPBodyLocked(p.MCPToolGrantDigest, mcpadmission.GrantBodyBytesV1)
	if !ok {
		return deny()
	}
	g, err := mcpadmission.ParseMCPToolGrantV1(rawGrant)
	if err != nil || g.ValidateForCost(cost, at) != nil || g.DataPolicyDigest != p.PrivacyPolicyDigest {
		return deny()
	}
	rawPrice, priceOK := s.retainedMCPBodyLocked(cost.PriceEvidenceDigest, mcpadmission.ServiceEvidenceBytesV1)
	rawTerms, termsOK := s.retainedMCPBodyLocked(cost.TermsEvidenceDigest, mcpadmission.ServiceEvidenceBytesV1)
	if !priceOK || !termsOK {
		return deny()
	}
	price, err := mcpadmission.ParseMCPPriceEvidenceV1(rawPrice)
	if err != nil || price.ValidateForCost(g, cost, at) != nil || !s.retainedMCPDocumentLocked(price.SourceBody) {
		return deny()
	}
	terms, err := mcpadmission.ParseMCPTermsEvidenceV1(rawTerms)
	if err != nil || terms.ValidateForCost(g, cost, at) != nil || !s.retainedMCPDocumentLocked(terms.SourceBody) {
		return deny()
	}
	end := cost.PriceExpiresAt
	for _, expiry := range []time.Time{g.ExpiresAt, terms.ExpiresAt} {
		if expiry.Before(end) {
			end = expiry
		}
	}
	// The original caller can shorten the Source lifetime below the installed
	// configuration. Seal that actual deadline, not just the current Alive bit;
	// publisher time may be ahead of the local process clock.
	deadline, ok := s.ctx.Deadline()
	if !ok {
		return deny()
	}
	deadline = deadline.UTC().Truncate(time.Microsecond)
	if deadline.Before(end) {
		end = deadline
	}
	if !at.Before(end) {
		return deny()
	}
	return cost, end, nil
}
