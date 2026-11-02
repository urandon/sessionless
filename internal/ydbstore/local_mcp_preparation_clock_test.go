package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These fixtures retain actual private Source intake, the original entry,
// MODEL preparation, Store reserve/before-send, normalized body custody and
// authenticated durable observation. Only SQL and the MODEL response are DATA;
// no server, model, MCP transport or native database is run here. The existing
// graph.at supplies BOTH scripted SQL and original issuers. Advancing it below
// models elapsed work without sleeping or changing any issued authority.
func localMCPClockFixture(t *testing.T) (*compactionSourceFixture, *compactionPublicationSQL, domain.AttemptOperationReservationV1) {
	t.Helper()
	f, physical := localPreparedModelFixture(t, 0)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatal("original MODEL preparation", err)
	}
	recordPreparedLocalToolCall(t, f, model.operation, model.frozen)
	if model.reservation.Validate() != nil || model.observation.Validate() != nil || len(physical.operations) != 1 {
		t.Fatal("original MODEL did not leave exactly one authenticated durable reservation/observation")
	}
	if f.actual.config.Limits.OperationTimeout != 5*time.Second || f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxNetworkOperationDuration != 5*time.Second {
		t.Fatal("fixture no longer has the unchanged five-second per-operation limit")
	}
	return f, physical, model.reservation.Clone()
}

func assertMCPClockBudget(t *testing.T, ctx context.Context, f *compactionSourceFixture) {
	t.Helper()
	b, ok := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	if !ok {
		t.Fatal("actual PrepareMCP lost its original logical-call budget")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.active || b.callbacks == 0 || b.callbacks > 3 || b.statements > 96 || b.callbackStatements > 32 || b.bytes > 4<<20 || max(f.queries.statements, f.queries.maxStatements) > 32 {
		t.Fatalf("PrepareMCP logical budget failed=%t active=%t callbacks=%d statements=%d last_callback=%d bytes=%d observed_max_callback=%d", b.failed, b.active, b.callbacks, b.statements, b.callbackStatements, b.bytes, max(f.queries.statements, f.queries.maxStatements))
	}
}

func TestLocalPreparedMCPClockUsesCurrentPreparationNotModelReservation(t *testing.T) {
	synctest.Test(t, testLocalPreparedMCPClockUsesCurrentPreparationNotModelReservation)
}

func testLocalPreparedMCPClockUsesCurrentPreparationNotModelReservation(t *testing.T) {
	f, physical, model := localMCPClockFixture(t)
	beforeAccounting := append([]driver.Value(nil), f.parent.sql.accountingRow...)
	var accounting ownedAttemptAccounting
	if len(beforeAccounting) != 4 || json.Unmarshal([]byte(beforeAccounting[2].(string)), &accounting) != nil || !accounting.UpdatedAt.Equal(model.ReservedAt) {
		t.Fatal("causal control: accounting timestamp is not the preceding MODEL reservation")
	}
	current := model.ReservedAt.Add(time.Second)
	wantDeadline := current.Add(5 * time.Second)
	if !wantDeadline.Before(f.entry.parent.Authority.InvocationDeadline) || !wantDeadline.Before(f.entry.allocation.Window.ExpiresAt) {
		t.Fatal("causal control: original invocation/allocation, not stale MODEL time, would explain the deadline")
	}
	f.parent.graph.at = current
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	prepared, err := f.entry.PrepareMCP(ctx)
	if err != nil || prepared == nil {
		t.Fatal("actual PrepareMCP denied current original funding and durable MODEL", err)
	}
	op, err := prepared.Operation()
	if err != nil {
		t.Fatal("actual original MCP holder", err)
	}
	if !op.Deadline.Equal(wantDeadline) {
		t.Errorf("MCP deadline anchored to predecessor: after_current=%s want=5s after_model_reservation=%s", op.Deadline.Sub(current), op.Deadline.Sub(model.ReservedAt))
	}
	prior, _ := prepared.source.state.operations[0].Observation.Digest()
	spending := f.entry.source.Spending[2]
	if op.Sequence != 2 || op.PredecessorObservationDigest != prior || op.ToolGrantDigest != spending.ScopedGrantDigest ||
		op.Debit.ActiveDuration != 5*time.Second || op.Debit.AmountMicrounits != *spending.MaxAmountMicrounits ||
		op.Debit.ToolDispatches != 1 || op.Debit.MCPOutboundMessages != 4 || op.Debit.MCPCatalogPages != 1 {
		t.Error("new operation time changed original predecessor, grant or full conservative debit")
	}
	if !reflect.DeepEqual(beforeAccounting, f.parent.sql.accountingRow) || len(physical.operations) != 1 || physical.pending != nil {
		t.Error("preparation rewrote accounting time/history or reserved an MCP effect")
	}
	assertMCPClockBudget(t, ctx, f)
}

func TestLocalPreparedMCPClockRetainsShorterOriginalCallerBound(t *testing.T) {
	synctest.Test(t, testLocalPreparedMCPClockRetainsShorterOriginalCallerBound)
}

func testLocalPreparedMCPClockRetainsShorterOriginalCallerBound(t *testing.T) {
	f, physical, model := localMCPClockFixture(t)
	f.parent.graph.at = model.ReservedAt.Add(time.Second)
	before := append([]driver.Value(nil), f.parent.sql.accountingRow...)
	// Original behavior denies an operation that exceeds its caller's shorter
	// absolute bound; it must not use stale MODEL time to make that bound fit.
	caller, finish := context.WithDeadline(t.Context(), f.parent.graph.at.Add(4*time.Second))
	defer finish()
	ctx, cancel := ownedAuthorityCallContext(caller)
	defer cancel()
	prepared, err := f.entry.PrepareMCP(ctx)
	if !errors.Is(err, domain.ErrMembershipDenied) || prepared != nil || f.entry.preparedMCP.Load() != nil {
		t.Fatal("shorter original caller permitted a fresh five-second MCP operation", err)
	}
	if caller.Err() != nil || ctx.Err() != nil {
		t.Fatal("caller timer expiry, not operation/declaration comparison, explained denial")
	}
	if !reflect.DeepEqual(before, f.parent.sql.accountingRow) || len(physical.operations) != 1 || physical.pending != nil {
		t.Fatal("shorter-caller denial changed original accounting/effects")
	}
	assertMCPClockBudget(t, ctx, f)
}

func TestLocalPreparedMCPClockExpiredOriginalInvocationDenies(t *testing.T) {
	synctest.Test(t, testLocalPreparedMCPClockExpiredOriginalInvocationDenies)
}

func testLocalPreparedMCPClockExpiredOriginalInvocationDenies(t *testing.T) {
	f, physical, model := localMCPClockFixture(t)
	before := append([]driver.Value(nil), f.parent.sql.accountingRow...)
	if !model.ReservedAt.Before(f.entry.parent.Authority.InvocationDeadline) {
		t.Fatal("causal control: no original live MODEL interval")
	}
	f.parent.graph.at = f.entry.parent.Authority.InvocationDeadline
	prepared, err := f.entry.PrepareMCP(t.Context())
	if err == nil || prepared != nil || f.entry.preparedMCP.Load() != nil {
		t.Fatal("fresh preparation time revived an expired original invocation")
	}
	if !reflect.DeepEqual(before, f.parent.sql.accountingRow) || len(physical.operations) != 1 || physical.pending != nil {
		t.Fatal("expired-current denial changed original accounting/effects")
	}
}
