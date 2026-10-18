package ydbstore

import (
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// Actual original Source intake/Store/fresh reserve/bridge, with SCRIPTED SQL,
// external funding and storage. The denying keeper supplies no network proof.
func TestLocalTerminalBudgetOriginalEntryDeniesBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name            string
		body, artifacts uint64
	}{
		{name: "body_no_room", body: operationbody.LocalTerminalCapsuleBytesV1},
		{name: "body_no_room_after_original_setup", body: operationbody.LocalTerminalCapsuleBytesV1 + (8 << 10)},
		{name: "artifact_first_over", artifacts: operationbody.LocalTerminalCapsuleBytesV1 - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalBootstrapFixtureWithPrebuild(t, localRuntimeActualPrebuild{bodyBytes: tc.body, artifactBytes: tc.artifacts})
				completeLocalEntryFixture(t, f)
				blobs := new(localEntryBlobs)
				entry, err := f.bootstrap.StartLocalInvocation(t.Context(), f.parent.request, blobs)
				assertLocalEntryDenied(t, entry, err)
				allocation := f.store.db.Driver().(allocationDriver).f
				if f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 0 || allocation.writes != 0 || blobs.calls != 0 || f.bootstrap.entry.Load() != nil || f.bootstrap.bodySlot.bodies.Load() != nil {
					t.Fatalf("capacity denial escaped pre-effect admission: parent=%d burn=%d allocation=%d blob=%d", f.parent.sql.inserts, f.parent.sql.burnUpdates, allocation.writes, blobs.calls)
				}
				entry, err = f.bootstrap.StartLocalInvocation(t.Context(), f.parent.request, blobs)
				assertLocalEntryDenied(t, entry, err)
				if f.parent.sql.burnUpdates != 0 || allocation.writes != 0 || blobs.calls != 0 {
					t.Fatal("repeated start restored denied original physical capacity")
				}
			})
		})
	}
}

func TestLocalTerminalBudgetOriginalEntryExactBodyRemainder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newLocalBootstrapFixtureWithPrebuild(t, localRuntimeActualPrebuild{bodyBytes: operationbody.LocalTerminalCapsuleBytesV1 + (8 << 10) + 1})
		completeLocalEntryFixture(t, f)
		entry, err := f.bootstrap.StartLocalInvocation(t.Context(), f.parent.request, new(localEntryBlobs))
		if err != nil || entry == nil || entry.terminalOutputs == nil {
			t.Fatal("exact original admitted remainder", err)
		}
		setup := ownedSetupBodyBytes(entry.parent)
		if setup != 8<<10 || entry.localOperationBodyLimit() != setup+1 || entry.terminalOutputs.CapsuleLimit() != 256<<10 || entry.terminalOutputs.FailureNoticeLimit() != 1024 {
			t.Fatal("entry lost the original immutable reservation")
		}
		scope := ownedOperationScope(entry.parent)
		if !f.store.localOperationBudgetAllows(scope, setup+1) || f.store.localOperationBudgetAllows(scope, setup+2) {
			t.Fatal("actual same-Store exact/first-over body ceiling differs from admission")
		}
		foreign := scope
		foreign.AttemptID = "foreign"
		if f.store.localOperationBudgetAllows(foreign, 1) {
			t.Fatal("foreign scope acquired original terminal reservation")
		}
		before := entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxAggregateBodyBytes
		if before != operationbody.LocalTerminalCapsuleBytesV1+setup+1 {
			t.Fatal("effective headroom mutated the sealed policy")
		}
	})
}
