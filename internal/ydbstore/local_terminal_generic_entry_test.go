package ydbstore

import (
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// The lower-level original bootstrap entry is not LocalV1.InvokeFirstModel's
// terminal-ineligible wrapper. Its real Store Reserve/BeforeSend/Record path
// can complete seq1 without creating a private preparedModel. SQL/model bytes
// are scripted; no private prepared owner is synthesized by this fixture.
func TestLocalTerminalGenericFirstEntryCustody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, _ := localPreparedModelFixture(t, 0)
		op := f.operation(t, 1)
		op.Kind, op.SummaryBytes, op.Debit.CompactionCalls = domain.AttemptOperationModelV1, 0, 0
		op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
		if op.ValidateForParent(f.entry.parent.Authority, f.entry.parent.Reservation) != nil {
			t.Fatal("original generic first MODEL must be valid")
		}
		recordPreparedLocalResponse(t, f, op, f.frozen, "generic original entry answer")
		if f.entry.preparedModel.Load() != nil {
			t.Fatal("generic first path unexpectedly fabricated a prepared model")
		}
		h, err := f.entry.ClaimTerminal(t.Context())
		if err != nil {
			t.Fatal("original entry terminal claim", err)
		}
		source, err := h.Capture(t.Context())
		if err != nil {
			t.Fatal("original complete known frontier", err)
		}
		t.Cleanup(source.Clear)
		p, err := h.PrepareOutput(t.Context())
		if err != nil || p == nil {
			t.Fatal("original generic first entry lost output custody", err)
		}
		t.Cleanup(p.Clear)
		if p.accounting == nil || len(p.accounting.operations) != 1 || p.accounting.operations[0].Reservation.Operation.Sequence != 1 || f.entry.preparedModel.Load() != nil {
			t.Fatal("generic custody fabricated prepared ownership or lost original accounting")
		}
	})
}
