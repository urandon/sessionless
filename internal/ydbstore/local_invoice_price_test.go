package ydbstore

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// A panic here proves that local custody never falls back to the unrelated
// structured external-provider price port. It cannot authorize a local invoice.
type localInvoiceRejectStandardCustody struct{}

func (localInvoiceRejectStandardCustody) ReadOwnedProviderPrice(context.Context, domain.OwnedAgentProfileReferenceV1, uint64) ([]byte, error) {
	panic("local invoice entered external tariff custody")
}

func localInvoicePriceFixture(t *testing.T) (localBootstrapFixture, *ownedProviderPriceEvidence, domain.AttemptOperationV1, context.Context) {
	t.Helper()
	f := newLocalBootstrapFixtureWithPrebuild(t, localRuntimeActualPrebuild{laterPriceObservation: true})
	if err := f.bootstrap.Complete(f.bridge); err != nil {
		t.Fatal(err)
	}
	f.store.ownedProviderPriceCustody = localInvoiceRejectStandardCustody{}
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	t.Cleanup(cancel)
	e, err := f.store.readOwnedProviderPriceEvidence(ctx, f.actual.profile)
	if err != nil || e == nil || e.local == nil || e.price != nil {
		t.Fatalf("genuine local invoice evidence: %v", err)
	}
	op := domain.AttemptOperationV1{Kind: domain.AttemptOperationModelV1, Deadline: f.actual.at.Add(time.Second), Currency: f.actual.profile.CostCeiling.Currency, PriceRevision: f.actual.profile.CostCeiling.PriceRevision,
		Debit: domain.AttemptOperationDebitV1{InputTokens: 1, OutputTokens: 1}}
	return f, e, op, ctx
}

func TestLocalInvoiceOperationPriceRequiresOriginalCustodyAndSeparateWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, e, op, ctx := localInvoicePriceFixture(t)
		p := f.actual.profile
		if err := f.store.validateOwnedLocalInvoiceEvidence(e.local, p, f.actual.execution, op, f.actual.at); err != nil {
			t.Fatalf("actual local terms quote: %v", err)
		}
		if e.local.digest != p.ProviderPriceDigest || !f.store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: e}) {
			t.Fatal("original invoice/lifetime lost")
		}
		// Read again through the same authoritative transaction path. Metadata
		// and DB isolation remain scripted; Source/private custody is genuine.
		if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			return f.store.validateOwnedOperationProviderPrice(ctx, q, op, e, f.actual.execution)
		}); err != nil {
			t.Fatalf("current original profile/invoice join: %v", err)
		}
		// Different valid original starts are not normalized into equality.
		if !e.local.terms.IssuedAt.Before(p.CostCeiling.PriceObservedAt) {
			t.Fatal("fixture did not preserve distinct original invoice/price starts before sealing")
		}
		if err := f.store.validateOwnedLocalInvoiceEvidence(e.local, p, f.actual.execution, op, f.actual.at); err != nil {
			t.Fatal("different valid original timestamps denied", err)
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		if f.store.operationEvidenceLifetimeCurrent(&ownedOperationEvidence{price: e}) {
			t.Fatal("captured invoice survived original Source closure")
		}
		if err := f.store.validateOwnedLocalInvoiceEvidence(e.local, p, f.actual.execution, op, f.actual.at); err == nil {
			t.Fatal("captured invoice authorized after closure")
		}
		if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
			return f.store.validateOwnedOperationProviderPrice(ctx, q, op, e, f.actual.execution)
		}); err == nil {
			t.Fatal("unchanged canonical rows hid original Source closure")
		}
	})
}

func TestLocalInvoiceOperationPriceRejectsChangedTermsAndAuthority(t *testing.T) {
	for _, name := range []string{"terms-currency", "terms-revision", "terms-model", "terms-endpoint", "supplies-host", "provider-invoice", "substrate-invoice", "digest", "issued", "expiry", "nonzero-debit", "unknown-price", "paid-source", "billing-owner", "foreign-profile", "foreign-source", "before-terms", "price-expiry", "deadline", "missing-bootstrap", "mixed-evidence"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, e, op, ctx := localInvoicePriceFixture(t)
				p, src, at := f.actual.profile.Clone(), f.actual.execution.Clone(), f.actual.at
				invoice := *e.local
				switch name {
				case "terms-currency":
					invoice.terms.Currency = "OTHER"
				case "terms-revision":
					invoice.terms.Revision = "other-revision"
				case "terms-model":
					invoice.terms.ModelID = "other-model"
				case "terms-endpoint":
					invoice.terms.BaseURL = "http://127.0.0.1:1/v1"
				case "supplies-host":
					invoice.terms.OwnerSuppliesHost = false
				case "provider-invoice":
					invoice.terms.ProviderInvoiceAmount = 1
				case "substrate-invoice":
					invoice.terms.SubstrateInvoiceAmount = 1
				case "digest":
					invoice.digest = strings.Repeat("b", 64)
				case "issued":
					invoice.terms.IssuedAt = invoice.terms.IssuedAt.Add(-time.Microsecond)
				case "expiry":
					invoice.terms.ExpiresAt = invoice.terms.ExpiresAt.Add(time.Microsecond)
				case "nonzero-debit":
					op.Debit.AmountMicrounits = 1
				case "unknown-price":
					p.CostCeiling.ProviderPriceState = domain.ProviderPriceUnknownV1
				case "paid-source":
					src.Spending[0].PriceState = domain.ExternalServicePriceKnownPaidV1
				case "billing-owner":
					src.Spending[0].Locator.BillingOwnerUserID = "foreign-owner"
				case "foreign-profile":
					p.Resource.Revision++
				case "foreign-source":
					invoice.source = nil
				case "before-terms":
					at = invoice.terms.IssuedAt.Add(-time.Microsecond)
				case "price-expiry":
					at = p.CostCeiling.PriceExpiresAt
				case "deadline":
					op.Deadline = p.CostCeiling.PriceExpiresAt.Add(time.Microsecond)
				case "missing-bootstrap":
					f.store.localBootstrap = nil
				case "mixed-evidence":
					e.price = new(domain.ProviderPriceObservationV1)
					if err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
						return f.store.validateOwnedOperationProviderPrice(ctx, q, op, e, src)
					}); err == nil {
						t.Fatal("mixed price evidence authorized")
					}
					return
				}
				if err := f.store.validateOwnedLocalInvoiceEvidence(&invoice, p, src, op, at); err == nil {
					t.Fatalf("%s local invoice substitution authorized", name)
				}
			})
		})
	}
}
