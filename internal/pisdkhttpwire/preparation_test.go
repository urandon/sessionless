package pisdkhttpwire

import (
	"reflect"
	"testing"
)

func TestSequencePreparationAccountsPrivateHashBeforeConstruction(t *testing.T) {
	b := MeasureSequencePreparation()
	want := uint64(reflect.TypeOf(Sequence{}).Size()) + uint64(reflect.TypeOf(sequenceState{}).Size()) + 512
	if b.LiveBytes != want || b.Nodes != 3 {
		t.Fatalf("preparation=%+v want live=%d nodes=3", b, want)
	}
	if got := testing.AllocsPerRun(20, func() { _ = MeasureSequencePreparation() }); got != 0 {
		t.Fatalf("pre-reservation accounting allocated %g objects", got)
	}
	if !SequencePreparationSupported() {
		return // alternate backends must deny the new coordinator, not old DATA APIs
	}
	s := newSequence(t, testLimits())
	if !s.PreparedLayoutFits() {
		t.Fatal("audited fixed Go SHA layout drift")
	}
	if got := testing.AllocsPerRun(20, func() { _ = s.PreparedLayoutFits() }); got != 0 {
		t.Fatalf("fixed hash sanity check allocated %g objects", got)
	}
	if (*Sequence)(nil).PreparedLayoutFits() || (&Sequence{}).PreparedLayoutFits() {
		t.Fatal("nil/zero sequence accepted as funded fixed state")
	}
}
