package ports_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Structural fixture only: no issuer, provider measurement or cleanup proof.
func TestOwnedOperationAccountingV2PreservesFullReservation(t *testing.T) {
	r := operationPortFixture(t).Reservation
	rd, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	input, output, amount := uint64(3), uint64(2), uint64(0)
	for _, tc := range []struct {
		name  string
		usage domain.AttemptOperationUsageV1
		kind  ports.OwnedOperationAccountingKindV2
	}{
		{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, ports.OwnedOperationUnknownReservedV2},
		{"reported", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &input, OutputTokens: &output}, ports.OwnedOperationProviderReportedReservedV2},
		{"measured_DATA", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: &input, OutputTokens: &output, AmountMicrounits: &amount}, ports.OwnedOperationMeasuredV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ObservedAt: r.ReservedAt.Add(time.Second), Usage: tc.usage, ResponseRef: "normalized-result", ResponseDigest: strings.Repeat("b", 64), ResponseBytes: 50}
			row, err := ports.BuildOwnedOperationAccountingRowV2(r, o)
			if err != nil {
				t.Fatal("valid DATA row", err)
			}
			if row.Disposition != tc.kind || row.Reserved != r.Operation.Debit || !reflect.DeepEqual(row.Usage, o.Usage) {
				t.Fatal("reservation refunded or usage fabricated")
			}
			if row.Usage.InputTokens != nil {
				*row.Usage.InputTokens = 99
				if *o.Usage.InputTokens == 99 {
					t.Fatal("usage aliases caller")
				}
			}
			o.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
			o.Reason = domain.AttemptOperationReasonWorkerLostV1
			o.ResponseRef = ""
			o.ResponseDigest = ""
			o.ResponseBytes = 0
			if o.ValidateForReservation(r) != nil {
				t.Fatal("causal unknown-effect fixture invalid")
			}
			if _, err := ports.BuildOwnedOperationAccountingRowV2(r, o); err == nil {
				t.Fatal("unknown effect accepted as completed accounting")
			}
		})
	}
}

func TestOwnedOperationAccountingV2BoundedDigestAndClone(t *testing.T) {
	r := operationPortFixture(t).Reservation
	rd, err := r.Digest()
	if err != nil {
		t.Fatal(err)
	}
	input := uint64(2)
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ObservedAt: r.ReservedAt.Add(time.Second), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &input}, ResponseRef: "normalized-result", ResponseDigest: strings.Repeat("b", 64), ResponseBytes: 50}
	row, err := ports.BuildOwnedOperationAccountingRowV2(r, o)
	if err != nil {
		t.Fatal(err)
	}
	v := ports.OwnedOperationAccountingDispositionV2{Version: 2, Scope: r.Operation.Scope, AccountingDigest: strings.Repeat("c", 64), AggregateDebit: r.AggregateDebit, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: strings.Repeat("d", 64), TermsDigest: strings.Repeat("e", 64), Revision: "original-owner-terms", Currency: "USD", IssuedAt: r.ReservedAt, ExpiresAt: r.ReservedAt.Add(time.Minute)}, Operations: []ports.OwnedOperationAccountingRowV2{row}}
	before, err := v.Digest()
	if err != nil {
		t.Fatal("valid bounded DATA digest", err)
	}
	cloned := v.Clone()
	*cloned.Operations[0].Usage.InputTokens = 3
	after, err := cloned.Digest()
	if err != nil || after == before || *v.Operations[0].Usage.InputTokens != 2 {
		t.Fatal("digest/clone omitted actual usage", err)
	}
	for _, name := range []string{"version", "kind", "enum", "rows", "scope", "terms", "revision", "time"} {
		t.Run(name, func(t *testing.T) {
			bad := v.Clone()
			switch name {
			case "version":
				bad.Version = 1
			case "kind":
				bad.Operations[0].Kind = "future"
			case "enum":
				bad.Operations[0].Disposition = "refunded"
			case "rows":
				bad.Operations = make([]ports.OwnedOperationAccountingRowV2, 13)
			case "scope":
				bad.Scope.RunID = ""
			case "terms":
				bad.ProviderInvoice.TermsDigest = "not-a-digest"
			case "revision":
				bad.ProviderInvoice.Revision = strings.Repeat("x", 161)
			case "time":
				bad.ProviderInvoice.ExpiresAt = bad.ProviderInvoice.IssuedAt
			}
			if _, err := bad.Digest(); err == nil {
				t.Fatal("malformed DATA accepted")
			}
		})
	}
}
