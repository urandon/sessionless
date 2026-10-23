package ydbstore

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalMCPConservativeSecondPaidToolDenied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, mcp := completedOriginalMCPFixture(t)
		if mcp.reservation.Operation.Debit.AmountMicrounits != 30 {
			t.Fatal("original positive-price fixture declaration missing")
		}
		next, err := f.entry.PrepareModel(t.Context())
		if err != nil {
			t.Fatal("actual original MODEL3", err)
		}
		// Distinct correlation and semantic arguments avoid a duplicate-call
		// false positive; the original single-catalog/component cap must deny.
		recordPreparedLocalToolCall(t, f, next.operation, next.frozen, [2]string{"mcp-call-2", "different public docs"})
		rows := cloneGenerationRows(f.queries.operations)
		accounting := append([]driver.Value(nil), f.parent.sql.accountingRow...)
		objects := make(map[string][]byte, len(f.blobs.objects))
		for key, body := range f.blobs.objects {
			objects[key] = append([]byte(nil), body...)
		}
		if again, err := f.entry.PrepareMCP(t.Context()); err == nil || again != nil || f.entry.preparedMCP.Load() != mcp {
			t.Fatal("second positive-priced tools/call obtained a new prepared owner", err)
		}
		if !reflect.DeepEqual(rows, f.queries.operations) || !reflect.DeepEqual(accounting, f.parent.sql.accountingRow) || !reflect.DeepEqual(objects, f.blobs.objects) || mcp.observation.Observation.Usage.AmountMicrounits != nil {
			t.Fatal("second tool denial changed debit, catalog allowance or durable UNKNOWN result")
		}
	})
}

// Independent finite zero/positive synthetic price documents enter the actual
// Source BEFORE publication. No published authority, grant or cap is modified;
// no external tariff or measured bill is inferred from this offline fixture.
func TestLocalMCPSuccessorOriginalFinitePriceRetainsWholeDebit(t *testing.T) {
	for _, price := range []uint64{0, 37} {
		name := "zero"
		if price != 0 {
			name = "positive"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, mcp := completedOriginalMCPFixture(t, localRuntimeActualPrebuild{compiledCompaction: true, networkOperation: 5 * time.Second, mcpSyntheticPriceMicrounits: &price})
				before := f.entry.source.Clone()
				spending := before.Spending[2]
				state := domain.ExternalServicePriceKnownFreeV1
				if price != 0 {
					state = domain.ExternalServicePriceKnownPaidV1
				}
				if spending.PriceState != state || spending.MaxAmountMicrounits == nil || *spending.MaxAmountMicrounits != price || mcp.reservation.Operation.Debit.AmountMicrounits != price || mcp.observation.Observation.Usage.AmountMicrounits != nil || mcp.observation.Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 {
					t.Fatal("actual Source reservation/unknown observation lost the independent finite price")
				}
				if ownedObservationAllowsSuccessor(mcp.reservation, mcp.observation.Observation) {
					t.Fatal("generic measured-money gate admitted UNKNOWN actual money")
				}
				next, err := f.entry.PrepareModel(t.Context())
				if err != nil || next == nil {
					t.Fatal("actual original successor after finite MCP reservation", err)
				}
				assertOriginalMCPNextRequestPair(t, next.frozen.Body(), string(next.operation.Scope.RunID), mcp.call.ID())
				recordPreparedLocalResponse(t, f, next.operation, next.frozen, "final answer after independently finite priced MCP")
				snapshot, err := f.entry.CaptureCompactionSource(t.Context())
				if err != nil {
					t.Fatal("original three-operation retained frontier", err)
				}
				defer snapshot.Clear()
				if len(snapshot.state.operations) != 3 || snapshot.state.snapshot.Accounting.Amounts[2] != price || !reflect.DeepEqual(before, f.entry.source) || !reflect.DeepEqual(snapshot.state.operations[1].Observation, mcp.observation.Observation) {
					t.Fatal("full independent MCP debit was refunded, repriced, rewritten or Source changed")
				}
				var total uint64
				for _, op := range snapshot.state.operations {
					total += op.Reservation.Operation.Debit.AmountMicrounits
				}
				if snapshot.state.snapshot.Accounting.Aggregate.AmountMicrounits != total {
					t.Fatal("exact original aggregate debit changed during continuation")
				}
			})
		})
	}
}
