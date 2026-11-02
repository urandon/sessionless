package ydbstore

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Actual Source/private entry/prepared MODEL+two COMPACTION owners and original
// issuer/Store writes/readbacks. Responses/proxy observations/SQL are controlled
// DATA: no socket, real inference, semantic compaction or native SQL is claimed.
// This fixture's verifier is NOT a terminal Keeper; only the private historical
// capture and pure plan language are exercised. It proves no cleanup receipt,
// plan registration, publication or productive Manager outcome.
func knownFailurePrefixFixture(t *testing.T) (*compactionSourceFixture, *LocalFailureV1) {
	t.Helper()
	f, _ := localPreparedModelFixtureWithInitialRequest(t, localRuntimeActualPrebuild{compiledCompaction: true, networkOperation: 5 * time.Second}, false)
	first, err := f.entry.PrepareModel(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	recordPreparedLocalResponse(t, f, first.operation, first.frozen, "original ordinary MODEL prefix")
	for range 2 {
		g, err := f.entry.PrepareCompaction(t.Context())
		if err != nil {
			t.Fatal("actual prefix compaction preparation", err)
		}
		recordPreparedLocalResponse(t, f, g.operation, g.frozen, actualPreparedSummary(t, g))
		if _, err := g.Publish(t.Context()); err != nil {
			t.Fatal("actual retained prefix publication", err)
		}
		m, err := g.PrepareNextModel(t.Context())
		if err != nil {
			t.Fatal("actual next MODEL", err)
		}
		if g.program.Number() == 1 {
			recordPreparedLocalResponse(t, f, m.operation, m.frozen, "original MODEL between compactions")
		} else {
			completeOriginalMCPAfterPreparedModel(t, f, m, true)
		}
	}
	h, err := f.entry.ClaimKnownMCPFailure()
	if err != nil {
		t.Fatal("actual known error claim", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithDeadline(context.Background(), h.deadline)
		defer cancel()
		if err := h.StopAndJoin(ctx); err != nil {
			t.Error("original failure join", err)
		}
	})
	return f, h
}

func TestLocalFailureOriginalCompleteModelCompactionPrefix(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, h := knownFailurePrefixFixture(t)
		for _, row := range h.mcp.source.state.snapshot.Operations {
			if !h.authenticatedRow(row, 6) {
				t.Fatal("original prefix MAC missing", row.reservation.Operation.Sequence)
			}
		}
		ctx, cancel := context.WithDeadline(t.Context(), h.deadline)
		defer cancel()
		ctx, finish := ownedAuthorityCallContext(ctx)
		defer finish()
		if err := h.capture(ctx); err != nil {
			budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			t.Fatal("authentic full prefix failure capture", err, "callbacks", budget.callbacks, "statements", budget.statements)
		}
		if len(h.ops) != 6 || len(h.accounting.Operations) != 6 || h.accounting.AggregateDebit.Operations != 6 || h.accounting.AggregateDebit.CompactionCalls != 2 {
			t.Fatal("complete MODEL/compaction/MCP frontier was dropped")
		}
		for _, row := range h.snapshot.Operations {
			if !h.authenticatedRow(row, 6) {
				t.Fatal("original issuer lineage lost a prefix observation")
			}
		}
		bodies, err := f.entry.terminalOutputs.SnapshotRetainedBodies(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pd, err := f.actual.config.Digest()
		if err != nil {
			t.Fatal(err)
		}
		c := localFailureCapsuleV1{Schema: "sessionless.terminal-failure-custody.v1", Version: 1, Parent: f.entry.parent.Clone(), Source: f.entry.source.Clone(), ProfileDigest: pd, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained", CapturedAt: h.capturedAt, Accounting: h.accounting.Clone(), Bodies: bodies, Derived: h.snapshot.Job.Run.Clone().DerivedContext}
		for _, op := range h.ops {
			c.Operations = append(c.Operations, localFailureOperationV1{Reservation: op.Reservation.Clone(), Observation: op.Observation.Clone(), Normalized: json.RawMessage(op.Response)})
		}
		c.FrontierDigest, err = localFailureFrontierDigest(c.Parent, c.Accounting, c.Operations)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := knownMCPFailurePlan(h.snapshot.Job.Run, h.snapshot.Job.Job.Limits, c)
		if err != nil {
			t.Fatal("complete authenticated prefix pure plan", err)
		}
		defer plan.Release()
		if len(plan.Events()) != 3 {
			t.Fatal("last call/result and one notice lost")
		}
		if h.prepared || h.published || h.registered || h.snapshot.Job.Run.TerminalCustody != nil {
			t.Fatal("planning fabricated registered custody or erasure")
		}
		duplicate := *h
		if duplicate.Prepare(t.Context()) == nil {
			t.Fatal("copied private failure owner admitted")
		}
		if _, err := f.entry.PrepareModel(t.Context()); err == nil {
			t.Fatal("failure latch reopened MODEL")
		}
		if _, err := f.entry.PrepareCompaction(t.Context()); err == nil {
			t.Fatal("failure latch reopened compaction")
		}
	})
}

func TestLocalFailureMissingTailAndExpiredOriginalDeadline(t *testing.T) {
	for _, mode := range []string{"missing_tail", "changed_prefix", "deletion_registered", "expired"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, h := knownFailurePrefixFixture(t)
				original := h.deadline
				switch mode {
				case "missing_tail":
					row := f.queries.operations[len(f.queries.operations)-1]
					row[5], row[6], row[7], row[8] = uint64(0), uint64(0), "", ""
				case "changed_prefix":
					row := f.queries.operations[0]
					row[8] = "0000000000000000000000000000000000000000000000000000000000000000"
				case "deletion_registered":
					state := "requested"
					f.queries.deletionState = &state
				case "expired":
					time.Sleep(time.Until(original))
					synctest.Wait()
				}
				ctx, cancel := context.WithDeadline(t.Context(), h.deadline)
				defer cancel()
				ctx, finish := ownedAuthorityCallContext(ctx)
				defer finish()
				if h.capture(ctx) == nil {
					t.Fatal("incomplete/conflicting/expired frontier published a plan")
				}
				if !reflect.DeepEqual(h.deadline, original) || h.prepared || h.published || h.registered {
					t.Fatal("failure denial renewed deadline or became custody")
				}
				if run := publicationRun(t, f); run.TerminalCustody != nil || run.TerminalFailure != nil {
					t.Fatal("negative capture installed terminal metadata")
				}
			})
		})
	}
}
