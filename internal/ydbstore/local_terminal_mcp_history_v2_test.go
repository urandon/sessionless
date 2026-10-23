package ydbstore_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type mcpV2HistoricalBlobs struct {
	ports.BlobStore
	mu                             sync.Mutex
	oldPrefix                      string
	faultRef                       domain.BlobRef
	faultBody                      []byte
	oldBodyReads, faultReads, puts int
}

func (b *mcpV2HistoricalBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	b.mu.Lock()
	b.puts++
	b.mu.Unlock()
	return b.BlobStore.Put(ctx, tenant, key, r)
}
func (b *mcpV2HistoricalBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.mu.Lock()
	if strings.HasPrefix(ref.Key, b.oldPrefix) && strings.Contains(ref.Key, "/attempt-operations-v1/") {
		b.oldBodyReads++
		b.mu.Unlock()
		return nil, errors.New("old erased MCP operation bodies are not historical input")
	}
	if b.faultRef.Key != "" && ref == b.faultRef {
		b.faultReads++
		body := bytes.Clone(b.faultBody)
		b.mu.Unlock()
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	b.mu.Unlock()
	return b.BlobStore.Open(ctx, tenant, ref)
}
func (b *mcpV2HistoricalBlobs) counts() (old, fault, puts int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.oldBodyReads, b.faultReads, b.puts
}

// Old effects/cleanup/seal come only from the genuine joined producer above.
// New owners reopen the original installation and receive freshly published
// funding. SQL/storage are controlled DATA, not native YDB or live MCP proof.
func TestLocalTerminalMCPV2FreshOwnerHistoryAndDeletion(t *testing.T) {
	old := completedOriginalMCPTerminalV2Fixture(t)
	ydbstore.InspectMCPTerminalCapsuleVersionDenialsV2(t, old.capsule)
	closeOriginalMCPTerminalV2Fixture(t, old)
	for _, name := range []string{"positive", "financial_parent", "accounting", "canonical_call", "canonical_result", "capsule_accounting_integrity", "capsule_cross_version_integrity"} {
		t.Run("fresh_"+name, func(t *testing.T) {
			reader := historicalMCPBlobsV2(t, old, name)
			fresh, history := ydbstore.NewFreshRunAfterMCPTerminalV2Seed(t, old.seed, old.completion, reader, "mcp-history-"+name)
			if fresh.Source == old.seed.Source || fresh.Request.RunID == old.seed.Request.RunID || fresh.Request.AttemptID == old.seed.Request.AttemptID || fresh.Request.LeaseID == old.seed.Request.LeaseID {
				t.Fatal("old process/source/execution owner reused")
			}
			if name == "financial_parent" || name == "accounting" {
				history.CorruptMCPTerminalV2(t, name)
			}
			owners := newOriginalMCPTerminalV2Owners(t, fresh, reader)
			before := fresh.Stats()
			_, _, puts := reader.counts()
			model, err := owners.entry.PrepareModel(t.Context())
			oldReads, faultReads, afterPuts := reader.counts()
			if history.TTLReads != 0 || oldReads != 0 {
				t.Fatal("fresh historical reader revived erased old execution material", history.TTLReads, oldReads)
			}
			if name != "positive" {
				if err == nil || model != nil || fresh.Stats() != before || afterPuts != puts {
					t.Fatalf("tampered historical %s reached successor materialization: model=%t err=%v puts=%d/%d", name, model != nil, err, afterPuts, puts)
				}
				if reader.faultRef.Key != "" && faultReads == 0 {
					t.Fatal("controlled actual retained/canonical body fault not reached")
				}
				if (name == "financial_parent" || name == "accounting") && history.DetailReads[old.completed.Run.ID] == 0 {
					t.Fatal("controlled actual historical financial join fault not reached")
				}
				return
			}
			if err != nil || model == nil || history.HistoryReads == 0 || history.DetailReads[old.completed.Run.ID] == 0 {
				t.Fatal("fresh original typed historical reader rejected authentic capsuleV2/V3", err)
			}
			op, err := model.Operation()
			if err != nil || op.Sequence != 1 || op.Scope.RunID != fresh.Request.RunID || fresh.Stats() != before {
				t.Fatal("historical DATA restored old dispatch or consumed fresh operation budget", err)
			}
			frozen, err := model.FrozenRequest()
			if err != nil {
				t.Fatal(err)
			}
			body := frozen.Body()
			defer clear(body)
			assertFreshMCPPairOnceV2(t, old, body)
		})
	}
	for _, mode := range []string{"capture_positive", "start_positive", "service_positive", "service_corrupt_accounting", "service_corrupt_evidence"} {
		t.Run("deletion_"+mode, func(t *testing.T) {
			reader := historicalMCPBlobsV2(t, old, "positive")
			refs, err := ydbstore.InspectRetainedDeletionBodiesV1(t, old.seed, old.completion, reader, mode)
			if strings.Contains(mode, "corrupt_") {
				if err == nil || len(refs) != 0 {
					t.Fatal("tampered original deletion financial/evidence DATA accepted", mode, err)
				}
				return
			}
			if err != nil || len(refs) != 2 || refs[0] != old.completed.Run.TerminalCustody.Plan.Capsule {
				t.Fatal("actual retained capsuleV2/V3 deletion verification failed", mode, err)
			}
			if oldReads, _, _ := reader.counts(); oldReads != 0 {
				t.Fatal("Session deletion attempted erased ordinary MCP bodies", oldReads)
			}
		})
	}
	for _, name := range []string{"canonical_call", "canonical_result", "capsule_accounting_integrity", "capsule_cross_version_integrity"} {
		t.Run("deletion_"+name, func(t *testing.T) {
			reader := historicalMCPBlobsV2(t, old, name)
			refs, err := ydbstore.InspectRetainedDeletionBodiesV1(t, old.seed, old.completion, reader, "start_positive")
			if err == nil || len(refs) != 0 {
				t.Fatal("tampered physical retained/canonical DATA granted deletion eligibility", name, err)
			}
			if oldReads, faultReads, _ := reader.counts(); oldReads != 0 || faultReads == 0 {
				t.Fatal("actual deletion body fault not reached or old bodies revived", oldReads, faultReads)
			}
		})
	}
}

func closeOriginalMCPTerminalV2Fixture(t *testing.T, old *originalMCPTerminalV2Fixture) {
	t.Helper()
	old.output.Clear()
	old.captured.Clear()
	if err := old.owners.bridge.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.owners.keeper.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.seed.Source.Close(); err != nil {
		t.Fatal(err)
	}
}

// A known finite zero reservation is published BEFORE original Source intake.
// It is not measured zero MCP usage: the actual normalized observation and
// terminal accounting retain UNKNOWN/nil money throughout all genuine phases.
func TestLocalTerminalMCPV2OriginalZeroReservationFreshHistory(t *testing.T) {
	zero := uint64(0)
	old := completedOriginalMCPTerminalV2Fixture(t, ydbstore.LocalCompactionConnectedLimits{MCPSyntheticPriceMicrounits: &zero})
	source, err := old.owners.entry.Source()
	if err != nil || source.Spending[2].MaxAmountMicrounits == nil || *source.Spending[2].MaxAmountMicrounits != 0 || source.Spending[2].PriceState != domain.ExternalServicePriceKnownFreeV1 {
		t.Fatal("genuine pre-intake zero MCP funding not retained", err)
	}
	var capsule struct {
		Accounting ports.OwnedOperationAccountingDispositionV3 `json:"accounting_disposition"`
	}
	if err := json.Unmarshal(old.capsule, &capsule); err != nil || len(capsule.Accounting.Operations) != 3 {
		t.Fatal("actual zero-price terminal capsule", err)
	}
	row := capsule.Accounting.Operations[1]
	if row.Disposition != ports.OwnedOperationUnknownFullReservedV3 || row.Reserved.AmountMicrounits != 0 || row.Usage.State != domain.AttemptOperationUsageUnknownV1 || row.Usage.AmountMicrounits != nil || capsule.Accounting.ComponentAmounts[2] != 0 {
		t.Fatal("known zero ceiling was manufactured into measured zero usage")
	}
	closeOriginalMCPTerminalV2Fixture(t, old)
	reader := historicalMCPBlobsV2(t, old, "positive")
	fresh, history := ydbstore.NewFreshRunAfterMCPTerminalV2Seed(t, old.seed, old.completion, reader, "mcp-zero-history")
	if fresh.Source == old.seed.Source || fresh.Request.RunID == old.seed.Request.RunID || fresh.Request.AttemptID == old.seed.Request.AttemptID {
		t.Fatal("zero-price history reused old authority")
	}
	owners := newOriginalMCPTerminalV2Owners(t, fresh, reader)
	before := fresh.Stats()
	model, err := owners.entry.PrepareModel(t.Context())
	if err != nil || model == nil || history.HistoryReads == 0 || history.DetailReads[old.completed.Run.ID] == 0 || history.TTLReads != 0 || fresh.Stats() != before {
		t.Fatal("fresh original zero-price historical preparation", err)
	}
	frozen, err := model.FrozenRequest()
	if err != nil {
		t.Fatal(err)
	}
	body := frozen.Body()
	defer clear(body)
	assertFreshMCPPairOnceV2(t, old, body)
	if oldReads, _, _ := reader.counts(); oldReads != 0 {
		t.Fatal("zero-price history resurrected erased original operation bodies")
	}
	refs, err := ydbstore.InspectRetainedDeletionBodiesV1(t, old.seed, old.completion, reader, "start_positive")
	if err != nil || len(refs) != 2 || refs[0] != old.completed.Run.TerminalCustody.Plan.Capsule {
		t.Fatal("genuine zero-price retained deletion verification", err)
	}
}

func historicalMCPBlobsV2(t *testing.T, old *originalMCPTerminalV2Fixture, kind string) *mcpV2HistoricalBlobs {
	t.Helper()
	b := &mcpV2HistoricalBlobs{BlobStore: old.blobs.sessionCompactionBlobs, oldPrefix: domain.SessionRunObjectPrefix(old.completed.Run.TenantID, old.completed.Run.SessionID, old.completed.Run.ID)}
	switch kind {
	case "positive", "financial_parent", "accounting":
	case "canonical_call", "canonical_result":
		i := 0
		if kind == "canonical_result" {
			i = 1
		}
		b.faultRef = old.completed.Events[i].Payload
		old.blobs.mu.Lock()
		b.faultBody = bytes.Clone(old.blobs.objects[string(b.faultRef.TenantID)+"\x00"+b.faultRef.Key])
		old.blobs.mu.Unlock()
		b.faultBody = bytes.ReplaceAll(b.faultBody, []byte("terminal-mcp-call"), []byte("changed-mcp-call!"))
	case "capsule_accounting_integrity", "capsule_cross_version_integrity":
		b.faultRef = old.completed.Run.TerminalCustody.Plan.Capsule
		var capsule map[string]json.RawMessage
		if err := json.Unmarshal(old.capsule, &capsule); err != nil {
			t.Fatal(err)
		}
		if kind == "capsule_cross_version_integrity" {
			capsule["schema"] = json.RawMessage(`"sessionless.terminal-custody.v1"`)
		} else {
			var accounting ports.OwnedOperationAccountingDispositionV3
			if err := json.Unmarshal(capsule["accounting_disposition"], &accounting); err != nil {
				t.Fatal(err)
			}
			accounting.Operations[1].Disposition = ports.OwnedOperationAccountingKindV3("observed")
			amount := uint64(0)
			accounting.Operations[1].Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: &amount}
			capsule["accounting_disposition"], _ = json.Marshal(accounting)
		}
		b.faultBody, _ = json.Marshal(capsule)
	default:
		t.Fatal("unknown retained/canonical body mutation", kind)
	}
	if b.faultRef.Key != "" && len(b.faultBody) == 0 {
		t.Fatal("controlled actual body fault missing")
	}
	t.Cleanup(func() { clear(b.faultBody) })
	return b
}

func assertFreshMCPPairOnceV2(t *testing.T, old *originalMCPTerminalV2Fixture, body []byte) {
	t.Helper()
	var wire struct {
		Messages []struct{ Role, Content string }
	}
	if err := json.Unmarshal(body, &wire); err != nil || len(wire.Messages) != 2 {
		t.Fatal("new original frozen request", err)
	}
	var history struct {
		Protected []struct {
			EventID string `json:"event_id"`
			RunID   string `json:"run_id"`
			Role    string `json:"original_role"`
			Payload string `json:"original_payload"`
		} `json:"protected_originals"`
		Units []struct {
			EventID string `json:"event_id"`
			RunID   string `json:"run_id"`
			Role    string `json:"original_role"`
			Payload string `json:"original_payload"`
		} `json:"original_units"`
	}
	if err := json.Unmarshal([]byte(wire.Messages[1].Content), &history); err != nil {
		t.Fatal(err)
	}
	frames := append(history.Protected, history.Units...)
	counts := make(map[domain.SessionEventID]int)
	for _, frame := range frames {
		if frame.RunID != string(old.completed.Run.ID) {
			continue
		}
		var expected *domain.SessionEvent
		for i := range old.completed.Events {
			if frame.EventID == string(old.completed.Events[i].ID) {
				expected = &old.completed.Events[i]
			}
		}
		if expected == nil {
			t.Fatal("retained operation rendered again beside canonical projection", frame.EventID, frame.Role)
		}
		old.blobs.mu.Lock()
		want := bytes.Clone(old.blobs.objects[string(expected.TenantID)+"\x00"+expected.Payload.Key])
		old.blobs.mu.Unlock()
		matches := frame.Role == string(expected.Kind) && bytes.Equal([]byte(frame.Payload), want)
		clear(want)
		if !matches {
			t.Fatal("fresh original request changed canonical tool pair/answer", expected.ID)
		}
		counts[expected.ID]++
	}
	for _, event := range old.completed.Events {
		if counts[event.ID] != 1 {
			t.Fatal("canonical/retained dedup is not exactly once", event.ID, counts[event.ID])
		}
	}
	if !strings.Contains(wire.Messages[1].Content, "continue the original task; preserve all constraints") {
		t.Fatal("new original turn lost its protected trigger")
	}
}
