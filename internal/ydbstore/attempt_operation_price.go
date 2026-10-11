package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"math/bits"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedProviderPriceBodyBytes = 4 << 10

// Produced from an actual current-profile point read and the original private
// keeper's bytes outside SQL. It is never accepted as caller operation input.
type ownedProviderPriceEvidence struct {
	profile domain.OwnedAgentProfileCurrentV1
	price   domain.ProviderPriceObservationV1
}

func decodeOwnedProviderPrice(body []byte) (domain.ProviderPriceObservationV1, error) {
	var price domain.ProviderPriceObservationV1
	if len(body) == 0 || len(body) > ownedProviderPriceBodyBytes || json.Unmarshal(body, &price) != nil || price.Validate() != nil {
		return price, domain.ErrMembershipDenied
	}
	canonical, err := json.Marshal(price)
	if err != nil || !bytes.Equal(canonical, body) {
		return domain.ProviderPriceObservationV1{}, domain.ErrMembershipDenied
	}
	return price, nil
}

// Integer microunits, always rounded UP after the combined exact rational
// input/output charge. No floats, envelope division, caller rates or refunds.
// Mul64/Add64 preserve the full product before checked 128/64 division.
func quoteOwnedProviderTokens(price domain.ProviderPriceObservationV1, input, output uint64) (uint64, error) {
	if price.Validate() != nil || price.State != domain.ProviderEvidenceSupportedV1 || input == 0 || input > 32768 || output == 0 || output > 2048 {
		return 0, domain.ErrMembershipDenied
	}
	hi, lo := bits.Mul64(input, price.InputMicrounitsPerMTokens)
	ohi, olo := bits.Mul64(output, price.OutputMicrounitsPerMTokens)
	lo, carry := bits.Add64(lo, olo, 0)
	hi, carry = bits.Add64(hi, ohi, carry)
	if carry != 0 || hi >= 1_000_000 {
		return 0, domain.ErrMembershipDenied
	}
	amount, remainder := bits.Div64(hi, lo, 1_000_000)
	if remainder != 0 {
		if amount == math.MaxUint64 {
			return 0, domain.ErrMembershipDenied
		}
		amount++
	}
	return amount, nil
}

func validateOwnedProviderPrice(price domain.ProviderPriceObservationV1, profile domain.OwnedAgentProfileCurrentV1, spending domain.AttemptSpendingAuthorizationCurrentV1, op domain.AttemptOperationV1, at time.Time) (uint64, error) {
	denied := func() (uint64, error) { return 0, domain.ErrMembershipDenied }
	if profile.ValidateAt(at) != nil || spending.ValidateAgainstProfile(profile, at) != nil || spending.Locator.ComponentKind != domain.AttemptSpendingProviderV1 || price.Validate() != nil || price.State != domain.ProviderEvidenceSupportedV1 || (op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) {
		return denied()
	}
	digest, err := price.Digest()
	cost := profile.CostCeiling
	// PriceObservationV1 has no revision field: the exact body digest is sealed
	// by the original profile, and its admitted revision is the SAME current
	// profile/spending revision. Do not invent a revision in the rate body.
	if err != nil || string(digest) != profile.ProviderPriceDigest || string(digest) != spending.PriceEvidenceDigest || price.Scope.TenantID != profile.Locator.TenantID || price.Scope.OwnerUserID != profile.Locator.OwnerUserID || price.Scope.Resource != profile.Resource || price.Scope.Backend != profile.Backend || price.ModelVendorID != profile.Locator.ModelVendorID || price.ModelID != profile.Locator.ModelID || price.Currency != cost.Currency || price.Currency != spending.Currency || op.Currency != price.Currency || op.PriceRevision != cost.PriceRevision || spending.PriceRevision != cost.PriceRevision || !price.ObservedAt.Equal(cost.PriceObservedAt) || !price.ExpiresAt.Equal(cost.PriceExpiresAt) || at.Before(price.ObservedAt) || !at.Before(price.ExpiresAt) || op.Deadline.After(price.ExpiresAt) || price.ObservedAt.After(profile.Window.PublishedAt) {
		return denied()
	}
	free := price.InputMicrounitsPerMTokens == 0 && price.OutputMicrounitsPerMTokens == 0
	if (free && (cost.ProviderPriceState != domain.ProviderPriceKnownFreeV1 || spending.PriceState != domain.ExternalServicePriceKnownFreeV1)) || (!free && (cost.ProviderPriceState != domain.ProviderPriceKnownV1 || spending.PriceState != domain.ExternalServicePriceKnownPaidV1)) {
		return denied()
	}
	amount, err := quoteOwnedProviderTokens(price, op.Debit.InputTokens, op.Debit.OutputTokens)
	if err != nil || spending.MaxAmountMicrounits == nil || amount > *spending.MaxAmountMicrounits || op.Debit.AmountMicrounits != amount || (!free && amount == 0) {
		return denied()
	}
	return amount, nil
}

func (store *Store) readOwnedProviderPriceEvidence(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1) (*ownedProviderPriceEvidence, error) {
	if store.ownedProviderPriceCustody == nil {
		return nil, domain.ErrMembershipDenied
	}
	digest, err := profile.Digest()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	ref := domain.OwnedAgentProfileReferenceV1{Locator: profile.Locator, RevocationGeneration: profile.RevocationGeneration, Digest: digest}
	body, err := store.ownedProviderPriceCustody.ReadOwnedProviderPrice(ctx, ref, ownedProviderPriceBodyBytes)
	if err != nil || ctx.Err() != nil || len(body) == 0 || len(body) > ownedProviderPriceBodyBytes || chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return nil, domain.ErrMembershipDenied
	}
	clone := bytes.Clone(body)
	defer clear(clone)
	price, err := decodeOwnedProviderPrice(clone)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	return &ownedProviderPriceEvidence{profile: profile.Clone(), price: price}, nil
}

func (store *Store) validateOwnedOperationProviderPrice(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1, evidence *ownedProviderPriceEvidence, source domain.AttemptExecutionSourceV1) error {
	if evidence == nil {
		return q.budget.deny()
	}
	profile, err := readOwnedProfileCurrent(ctx, q, source.Profile)
	digest, _ := evidence.profile.Digest()
	if err != nil || digest != source.Profile.Digest {
		return q.budget.deny()
	}
	if _, err := validateOwnedProviderPrice(evidence.price, profile, source.Spending[0], op, q.at); err != nil {
		return q.budget.deny()
	}
	return nil
}
