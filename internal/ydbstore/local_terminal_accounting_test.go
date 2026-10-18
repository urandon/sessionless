package ydbstore

import (
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func captureTerminalAccountingFixture(t *testing.T, f *compactionSourceFixture, h *LocalTerminalV1) (*localTerminalAccountingV2, error) {
	t.Helper()
	call, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	f.entry.preparationMu.Lock()
	defer f.entry.preparationMu.Unlock()
	h.source.state.mu.Lock()
	defer h.source.state.mu.Unlock()
	return prepareLocalTerminalAccounting(call, h, f.parent.graph.at, domain.OwnedAgentProfileCurrentV1{})
}

// Actual original Source/entry/issuer/Store capture, with scripted SQL funding
// and normalized MODEL DATA. No live provider, erasure or terminal commit.
func TestLocalTerminalAccountingOriginalCaptureHistoricalLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _, h := terminalOutputFixture(t)
		c, err := captureTerminalAccountingFixture(t, f, h)
		if err != nil || c == nil {
			t.Fatal("original accounting capture", err)
		}
		digest, err := c.Digest()
		if err != nil || domain.ValidateSHA256Digest("accounting", digest) != nil {
			t.Fatal("bounded accounting digest", err)
		}
		if c.disposition.ValidateFor(f.entry.parent, f.entry.source) != nil || len(c.proxies) != 1 || len(c.operations) != 1 {
			t.Fatal("incomplete original frontier")
		}
		row := c.disposition.Operations[0]
		if row.Disposition != ports.OwnedOperationUnknownReservedV2 || row.Reserved != c.operations[0].Reservation.Operation.Debit || row.Usage.InputTokens != nil || row.Usage.OutputTokens != nil || row.Usage.AmountMicrounits != nil {
			t.Fatal("unknown usage synthesized measurement or refunded reserve")
		}
		if !reflect.DeepEqual(c.disposition.AggregateDebit, h.source.state.snapshot.Accounting.Aggregate) || c.disposition.ComponentAmounts != h.source.state.snapshot.Accounting.Amounts {
			t.Fatal("original aggregate/components changed")
		}
		if _, err := json.Marshal(c); err == nil {
			t.Fatal("private original accounting serialized")
		}
		copyCapture := *c
		if _, err := copyCapture.Digest(); err == nil {
			t.Fatal("copied capture acquired ownership")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		m := f.entry.preparedModel.Load()
		if f.store.validateOwnedLocalInvoiceEvidence(m.price.local, m.price.profile, f.entry.source, m.operation, f.parent.graph.at) == nil {
			t.Fatal("active invoice check weakened after Source close")
		}
		after, err := c.Digest()
		if err != nil || after != digest {
			t.Fatal("captured historical content requires live Source", err)
		}
		if _, err := captureTerminalAccountingFixture(t, f, h); err == nil {
			t.Fatal("new capture accepted closed Source")
		}
		if _, err := c.Residual(domain.RegisteredAllocationV2{}, nil); err == nil {
			t.Fatal("DATA capture substituted retirement/physical erasure")
		}
	})
}

func TestLocalTerminalAccountingRejectsCorruptedCapturedData(t *testing.T) {
	for _, name := range []string{"invoice", "reservation", "unknown_enum", "scope", "row_count", "component"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, _, h := terminalOutputFixture(t)
				c, err := captureTerminalAccountingFixture(t, f, h)
				if err != nil {
					t.Fatal(err)
				}
				switch name {
				case "invoice":
					c.invoice.terms.ProviderInvoiceAmount = 1
				case "reservation":
					c.disposition.Operations[0].Reserved.OutputTokens = 1
				case "unknown_enum":
					c.disposition.Operations[0].Disposition = "free"
				case "scope":
					c.disposition.Scope.RunID = "foreign"
				case "row_count":
					c.disposition.Operations = nil
				case "component":
					c.disposition.ComponentAmounts[0] = 1
				}
				if _, err := c.Digest(); err == nil {
					t.Fatal("corrupted historical accounting accepted")
				}
			})
		})
	}
}
