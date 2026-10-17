package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

func TestLocalInvoiceCustodyOriginalReceiptBodyAndLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeActualFixture(t, true, true)
		digest, err := f.profile.Digest()
		if err != nil {
			t.Fatal(err)
		}
		ref := domain.OwnedAgentProfileReferenceV1{Locator: f.profile.Locator, RevocationGeneration: f.profile.RevocationGeneration, Digest: digest}
		life, err := f.source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		body, err := f.source.ReadInvoiceTerms(t.Context(), ref, 4096)
		sum := sha256.Sum256(body)
		if err != nil || uint64(len(body)) != f.config.OperatorTerms.Bytes || hex.EncodeToString(sum[:]) != f.config.OperatorTerms.Digest {
			t.Fatalf("actual original local invoice custody: %v", err)
		}
		var terms localoperator.InvoiceTermsV1
		if json.Unmarshal(body, &terms) != nil || terms.BaseURL != f.config.BaseURL || terms.ModelID != f.config.ModelID || !terms.OwnerSuppliesHost || !terms.OwnerSuppliesModel || terms.ProviderInvoiceAmount != 0 || terms.SubstrateInvoiceAmount != 0 {
			t.Fatal("original custody returned substituted invoice semantics")
		}
		original := bytes.Clone(body)
		body[0] = 'x'
		again, err := f.source.ReadInvoiceTerms(t.Context(), ref, uint64(len(original)))
		if err != nil || !bytes.Equal(again, original) {
			t.Fatal("caller mutation changed retained original terms", err)
		}
		if _, err := f.source.ReadInvoiceTerms(t.Context(), ref, uint64(len(original)-1)); err == nil {
			t.Fatal("first-over invoice body was cloned")
		}
		for _, cap := range []uint64{0, 4097, ^uint64(0)} {
			if body, err := f.source.ReadInvoiceTerms(t.Context(), ref, cap); err == nil || body != nil {
				t.Fatal("invalid invoice ceiling returned custody")
			}
		}
		foreign := ref
		foreign.RevocationGeneration++
		if body, err := f.source.ReadInvoiceTerms(t.Context(), foreign, 4096); err == nil || body != nil {
			t.Fatal("foreign generation acquired original custody")
		}
		foreign = ref
		foreign.Locator.ResourceID = "foreign-resource"
		if body, err := f.source.ReadInvoiceTerms(t.Context(), foreign, 4096); err == nil || body != nil {
			t.Fatal("foreign locator acquired original custody")
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if body, err := f.source.ReadInvoiceTerms(ctx, ref, 4096); err == nil || body != nil {
			t.Fatal("cancelled custody read returned terms")
		}
		if err := f.source.Close(); err != nil {
			t.Fatal(err)
		}
		if life.Err() == nil {
			t.Fatal("original lifetime survived Source closure")
		}
		if body, err := f.source.ReadInvoiceTerms(t.Context(), ref, 4096); err == nil || body != nil {
			t.Fatal("closed Source returned fresh invoice custody")
		}
	})
}

func TestLocalInvoiceCustodyRequiresAcceptedOriginalReceipt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalRuntimeActualFixture(t, true, false)
		d, err := f.profile.Digest()
		if err != nil {
			t.Fatal(err)
		}
		ref := domain.OwnedAgentProfileReferenceV1{Locator: f.profile.Locator, RevocationGeneration: f.profile.RevocationGeneration, Digest: d}
		if body, err := f.source.ReadInvoiceTerms(t.Context(), ref, 4096); err == nil || body != nil {
			t.Fatal("unaccepted receipt acquired custody")
		}
		var missing *localoperator.Source
		if body, err := missing.ReadInvoiceTerms(t.Context(), ref, 4096); err == nil || body != nil {
			t.Fatal("missing original Source returned custody")
		}
	})
}
