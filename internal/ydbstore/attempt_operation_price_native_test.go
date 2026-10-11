package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Only this credential-free native fixture producer creates synthetic price
// observations. The canonical body is frozen BEFORE real private HMAC profile
// publication/admission, then bound to the actual returned publication receipt.
// It is not a production rate issuer, public signer, caller quote or Session
// object. Exactly one bounded immutable metadata body, no unbounded map.
type nativeOwnedProviderPriceCustody struct {
	mu   sync.Mutex
	body []byte
	ref  domain.OwnedAgentProfileReferenceV1
}

func installNativeOwnedProviderPrice(t *testing.T, f *ownedReaderFixture, paid bool) {
	t.Helper()
	p := &f.profile
	if p.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || p.Resource.CredentialMode != domain.ProviderCredentialNoneV1 || f.store.ownedProviderPriceCustody != nil {
		t.Fatal("price fixture requires its original credential-free producer exactly once")
	}
	price := domain.ProviderPriceObservationV1{Version: 1, Scope: domain.ProviderEvidenceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, Resource: p.Resource, Backend: p.Backend}, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: p.Locator.ModelVendorID, ModelID: p.Locator.ModelID, Currency: p.CostCeiling.Currency, ObservedAt: p.CostCeiling.PriceObservedAt, ExpiresAt: p.CostCeiling.PriceExpiresAt}
	if paid {
		// Actual synthetic canonical rates yield 2 microunits per fixture model
		// call at one input+output token; the ORIGINAL shared ceiling is 20.
		price.InputMicrounitsPerMTokens, price.OutputMicrounitsPerMTokens = 1_000_000, 1_000_000
		old := *p.CostCeiling.MaxProviderAmountMicrounits
		p.CostCeiling.ProviderPriceState = domain.ProviderPriceKnownV1
		*p.CostCeiling.MaxProviderAmountMicrounits = 20
		*p.CostCeiling.MaxTotalAmountMicrounits += 20 - old
		f.spending[0].PriceState = domain.ExternalServicePriceKnownPaidV1
		*f.spending[0].MaxAmountMicrounits = 20
	} else if p.CostCeiling.ProviderPriceState != domain.ProviderPriceKnownFreeV1 || f.spending[0].PriceState != domain.ExternalServicePriceKnownFreeV1 || *p.CostCeiling.MaxProviderAmountMicrounits != 0 {
		t.Fatal("fixture zero rates require an explicitly original known-free profile/head")
	}
	digest, err := price.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p.ProviderPriceDigest = string(digest)
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.spending[0].PriceEvidenceDigest = string(digest)
	bindReaderSpendingSource(t, &f.spending[0])
	body, err := json.Marshal(price)
	if err != nil || len(body) > ownedProviderPriceBodyBytes {
		t.Fatal("bounded original price body", err)
	}
	keeper := &nativeOwnedProviderPriceCustody{body: bytes.Clone(body)}
	f.store.ownedProviderPriceCustody = keeper
	t.Cleanup(func() { keeper.mu.Lock(); defer keeper.mu.Unlock(); clear(keeper.body); keeper.body = nil })
}

func (k *nativeOwnedProviderPriceCustody) bindActualPublication(t *testing.T, receipt ports.OwnedProfilePublicationReceiptV1) {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	price, err := decodeOwnedProviderPrice(k.body)
	digest, digestErr := price.Digest()
	actual, actualErr := receipt.Profile.Digest()
	if err != nil || digestErr != nil || actualErr != nil || string(digest) != receipt.Profile.ProviderPriceDigest || actual != receipt.Digest || receipt.Replayed || k.ref.Digest != "" || price.Scope.Backend != receipt.Profile.Backend || price.Scope.Resource != receipt.Profile.Resource {
		t.Fatal("original frozen rate body must bind the actual first publication receipt")
	}
	k.ref = domain.OwnedAgentProfileReferenceV1{Locator: receipt.Profile.Locator, RevocationGeneration: receipt.Profile.RevocationGeneration, Digest: receipt.Digest}
}

func (k *nativeOwnedProviderPriceCustody) ReadOwnedProviderPrice(ctx context.Context, ref domain.OwnedAgentProfileReferenceV1, cap uint64) ([]byte, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if ctx.Err() != nil || cap != ownedProviderPriceBodyBytes || ref.Validate() != nil || ref != k.ref || len(k.body) == 0 || uint64(len(k.body)) > cap {
		return nil, domain.ErrMembershipDenied
	}
	return bytes.Clone(k.body), nil
}
