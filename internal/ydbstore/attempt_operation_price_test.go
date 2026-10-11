package ydbstore

import (
	"encoding/json"
	"math"
	"math/big"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Structural pricing controls; genuine publication, SQL reservation, TLS send
// and original keeper custody require the joined native fixture separately.
func ownedProviderPriceFixture(t *testing.T, paid bool) (domain.ProviderPriceObservationV1, domain.OwnedAgentProfileCurrentV1, domain.AttemptSpendingAuthorizationCurrentV1, domain.AttemptOperationV1, time.Time) {
	t.Helper()
	f := newOwnedReaderFixture(t)
	p := f.profile.Clone()
	price := domain.ProviderPriceObservationV1{Version: 1, Scope: domain.ProviderEvidenceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, Resource: p.Resource, Backend: p.Backend}, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: p.Locator.ModelVendorID, ModelID: p.Locator.ModelID, Currency: p.CostCeiling.Currency, ObservedAt: p.CostCeiling.PriceObservedAt, ExpiresAt: p.CostCeiling.PriceExpiresAt}
	spending := f.spending[0].Clone()
	if paid {
		price.InputMicrounitsPerMTokens, price.OutputMicrounitsPerMTokens = 1000, 3000
		p.CostCeiling.ProviderPriceState = domain.ProviderPriceKnownV1
		*p.CostCeiling.MaxProviderAmountMicrounits = 100
		*p.CostCeiling.MaxTotalAmountMicrounits += 100
		spending.PriceState, *spending.MaxAmountMicrounits = domain.ExternalServicePriceKnownPaidV1, 100
	}
	priceDigest, _ := price.Digest()
	p.ProviderPriceDigest = string(priceDigest)
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	spending.PriceEvidenceDigest = p.ProviderPriceDigest
	spending.SourceProfile.Digest, _ = p.Digest()
	bindReaderSpendingSource(t, &spending)
	at := f.driver.at
	if p.ValidateAt(at) != nil || spending.ValidateAgainstProfile(p, at) != nil || price.Validate() != nil {
		t.Fatal("valid structural rate/profile/spending fixture")
	}
	op := domain.AttemptOperationV1{Kind: domain.AttemptOperationModelV1, Deadline: at.Add(time.Second), Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, Debit: domain.AttemptOperationDebitV1{InputTokens: 1000, OutputTokens: 100}}
	op.Debit.AmountMicrounits, _ = quoteOwnedProviderTokens(price, op.Debit.InputTokens, op.Debit.OutputTokens)
	return price, p, spending, op, at
}

func TestOwnedProviderPriceQuoteRoundsUpWithoutOverflow(t *testing.T) {
	price, _, _, _, _ := ownedProviderPriceFixture(t, true)
	for _, tc := range []struct{ input, output, inRate, outRate uint64 }{
		{1, 1, 1, 1}, {1000, 100, 1000, 3000}, {32768, 2048, math.MaxUint64, math.MaxUint64}, {32768, 2048, math.MaxUint64, 0}, {1, 1, 0, 0},
	} {
		price.InputMicrounitsPerMTokens, price.OutputMicrounitsPerMTokens = tc.inRate, tc.outRate
		amount, err := quoteOwnedProviderTokens(price, tc.input, tc.output)
		n := new(big.Int).Mul(new(big.Int).SetUint64(tc.input), new(big.Int).SetUint64(tc.inRate))
		n.Add(n, new(big.Int).Mul(new(big.Int).SetUint64(tc.output), new(big.Int).SetUint64(tc.outRate)))
		n.Add(n, big.NewInt(999999)).Div(n, big.NewInt(1000000))
		if err != nil || !n.IsUint64() || amount != n.Uint64() {
			t.Fatalf("upward exact rational quote got=%d err=%v expected=%s", amount, err, n)
		}
	}
	for _, bounds := range [][2]uint64{{0, 1}, {1, 0}, {32769, 1}, {1, 2049}, {math.MaxUint64, math.MaxUint64}} {
		if _, err := quoteOwnedProviderTokens(price, bounds[0], bounds[1]); err == nil {
			t.Fatal("out-of-contract token bounds quoted")
		}
	}
}

func TestOwnedProviderPriceRequiresExactOriginalAuthority(t *testing.T) {
	price, profile, spending, op, at := ownedProviderPriceFixture(t, true)
	if amount, err := validateOwnedProviderPrice(price, profile, spending, op, at); err != nil || amount != 2 {
		t.Fatalf("actual structural rates must yield 2 not full component100: amount=%d err=%v", amount, err)
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.ProviderPriceObservationV1, *domain.OwnedAgentProfileCurrentV1, *domain.AttemptSpendingAuthorizationCurrentV1, *domain.AttemptOperationV1, *time.Time)
	}{
		{"unknown-is-not-zero", func(p *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, _ *domain.AttemptOperationV1, _ *time.Time) {
			p.State, p.Currency, p.InputMicrounitsPerMTokens, p.OutputMicrounitsPerMTokens = domain.ProviderEvidenceUnknownV1, "", 0, 0
		}},
		{"changed-rates", func(p *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, _ *domain.AttemptOperationV1, _ *time.Time) {
			p.InputMicrounitsPerMTokens++
		}},
		{"foreign-model", func(p *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, _ *domain.AttemptOperationV1, _ *time.Time) {
			p.ModelID = "other"
		}},
		{"expired-original", func(_ *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, _ *domain.AttemptOperationV1, at *time.Time) {
			*at = price.ExpiresAt
		}},
		{"underquote", func(_ *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, op *domain.AttemptOperationV1, _ *time.Time) {
			op.Debit.AmountMicrounits--
		}},
		{"full-envelope-not-rate", func(_ *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, op *domain.AttemptOperationV1, _ *time.Time) {
			op.Debit.AmountMicrounits = 100
		}},
		{"wrong-revision", func(_ *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, op *domain.AttemptOperationV1, _ *time.Time) {
			op.PriceRevision = "replacement"
		}},
		{"free-rates-paid-head", func(p *domain.ProviderPriceObservationV1, _ *domain.OwnedAgentProfileCurrentV1, _ *domain.AttemptSpendingAuthorizationCurrentV1, _ *domain.AttemptOperationV1, _ *time.Time) {
			p.InputMicrounitsPerMTokens, p.OutputMicrounitsPerMTokens = 0, 0
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, profile, spending, op, at := price.Clone(), profile.Clone(), spending.Clone(), op.Clone(), at
			tc.change(&p, &profile, &spending, &op, &at)
			if _, err := validateOwnedProviderPrice(p, profile, spending, op, at); err == nil {
				t.Fatal("changed price authority admitted")
			}
		})
	}
	free, p, s, op, now := ownedProviderPriceFixture(t, false)
	if amount, err := validateOwnedProviderPrice(free, p, s, op, now); err != nil || amount != 0 {
		t.Fatal("explicit original zero rates/free head denied", err)
	}
}

func TestOwnedProviderPriceBodyRequiresBoundedCanonicalBytes(t *testing.T) {
	price, _, _, _, _ := ownedProviderPriceFixture(t, true)
	body, _ := json.Marshal(price)
	if _, err := decodeOwnedProviderPrice(body); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]byte{nil, append([]byte(" "), body...), append(body[:len(body)-1:len(body)-1], []byte(`,"caller_approved":true}`)...), make([]byte, ownedProviderPriceBodyBytes+1)} {
		if _, err := decodeOwnedProviderPrice(changed); err == nil {
			t.Fatal("noncanonical/unbounded body admitted")
		}
	}
}
