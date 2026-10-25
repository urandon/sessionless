package ydbstore_test

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Original retired terminal capsule -> fresh Source/Store -> public generation,
// actual issuer/Store observation -> retained publication -> prepared MODEL.
// The public compaction choice is explicit: this does not claim automatic
// runtime no-fit selection, native YDB, physical transport or model fidelity.
func TestLocalTerminalMCPV2FreshOwnerCompactionAndNextModel(t *testing.T) {
	old := completedOriginalMCPTerminalV2Fixture(t)
	closeOriginalMCPTerminalV2Fixture(t, old)
	reader := historicalMCPBlobsV2(t, old, "positive")
	fresh, history := ydbstore.NewFreshRunAfterMCPTerminalV2Seed(t, old.seed, old.completion, reader, "mcp-history-compaction")
	owners := newOriginalMCPTerminalV2Owners(t, fresh, reader)
	source, err := owners.entry.Source()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := owners.entry.Parent()
	if err != nil {
		t.Fatal(err)
	}
	canonical := fresh.CanonicalSourceEvents(t)
	before := fresh.Stats()
	baseline, amounts := fresh.MCPHistoryCompactionAccounting(t)
	g, err := owners.entry.PrepareCompaction(t.Context())
	if err != nil || g == nil {
		t.Fatal("fresh authentic capsuleV2 history cannot prepare original compaction", err)
	}
	op, err := g.Operation()
	if err != nil || op.Kind != domain.AttemptOperationCompactionV1 || op.Sequence != 1 || op.Scope.RunID != fresh.Request.RunID || fresh.Stats() != before {
		t.Fatal("original generation preparation changed effect budget or reused retired scope", err)
	}
	frozen, err := g.FrozenRequest()
	if err != nil {
		t.Fatal(err)
	}
	body := frozen.Body()
	defer clear(body)
	assertFreshMCPPairOnceV2(t, old, body)
	input := decodeOriginalMCPCompactionWireV2(t, body)
	if input.Schema != "sessionless.compaction-input.v1" || len(input.Protected) == 0 {
		t.Fatal("actual original generation wire/reference table missing")
	}
	var pairIDs []string
	for _, frame := range input.Units {
		if frame.RunID == string(old.completed.Run.ID) && (frame.Role == "tool_call" || frame.Role == "tool_result") {
			pairIDs = append(pairIDs, frame.SourceID)
		}
	}
	if len(pairIDs) != 2 || pairIDs[0] == pairIDs[1] {
		t.Fatal("generation did not expose one correlated canonical pair with distinct operation citations")
	}
	summary, err := json.Marshal(map[string]any{
		"schema":      "sessionless.compaction-summary.v1",
		"task":        []any{map[string]any{"text": "Preserve the original task", "source_ids": []string{input.Protected[0].SourceID}}},
		"facts":       []any{map[string]any{"text": "The original correlated MCP call and result completed", "source_ids": pairIDs}},
		"constraints": []any{}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(summary)
	wire, err := json.Marshal(map[string]any{"model": fresh.Profile.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": string(summary)}, "finish_reason": "stop"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer clear(wire)
	normalized, err := frozen.NormalizeResponseBodyV1(wire, op.Kind, op.ResultBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(normalized)
	fresh.InstallTerminalMCPV2ProxyData(t, owners.entry, op)
	r, err := owners.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || r.Capability == nil || r.Status != ports.AttemptOperationReservedV1 {
		t.Fatal("actual original historical compaction reserve", err)
	}
	expected, err := baseline.Add(op.Debit)
	if err != nil || r.Reservation.AggregateDebit != expected || r.Reservation.ComponentAmounts != amounts || amounts[2] != 0 {
		t.Fatal("historical MCP DATA was charged as a fresh effect or the actual compaction debit was discounted")
	}
	if err := owners.store.AuthorizeAttemptOperationBeforeSend(t.Context(), r.Capability.Clone()); err != nil {
		t.Fatal("actual original historical compaction before-send", err)
	}
	bodies, err := owners.entry.Bodies()
	if err != nil {
		t.Fatal(err)
	}
	ref, err := bodies.Write(t.Context(), normalized)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := r.Reservation.Digest()
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: r.Reservation.OperationDigest, ReservationDigest: rd, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: time.Now().UTC().Truncate(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
	auth, err := fresh.Operate.AuthenticateAttemptOperationObservation(r.ObservationGrant, o)
	if err != nil {
		t.Fatal(err)
	}
	if err := owners.store.RecordAttemptOperationObservation(t.Context(), ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: r.ObservationGrant, Observation: auth}); err != nil {
		t.Fatal("actual original historical compaction record", err)
	}
	if read, err := owners.store.ReadAttemptOperationObservation(t.Context(), r.ObservationGrant); err != nil || !reflect.DeepEqual(read, auth) {
		t.Fatal("actual authenticated historical compaction readback", err)
	}
	next, err := g.Publish(t.Context())
	if err != nil || next == nil {
		t.Fatal("original V2 generation publisher/retained readback/head", err)
	}
	run := fresh.WorkerPreflightState(t).Run
	if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 1 || run.DerivedContext.Head == nil {
		t.Fatal("original retained head/inventory not committed")
	}
	plan := run.DerivedContext.Inventory[0]
	pd, err := plan.Digest()
	if err != nil || pd != run.DerivedContext.Head.PlanDigest {
		t.Fatal("original committed retained head digest", err)
	}
	readBlob := func(ref domain.BlobRef) []byte {
		t.Helper()
		r, err := reader.Open(t.Context(), ref.TenantID, ref)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(io.LimitReader(r, ref.Size+1))
		ce := r.Close()
		if err != nil || ce != nil || int64(len(b)) != ref.Size {
			t.Fatal("bounded actual retained body read", err, ce)
		}
		t.Cleanup(func() { clear(b) })
		return b
	}
	retainedSummary, manifest := readBlob(plan.Summary), readBlob(plan.Manifest)
	retained, err := compactioncontext.DecodeRetainedV2(fresh.Profile, *parent.Authority.HarnessBinding.AgentLoopPolicy, plan, retainedSummary, manifest, nil)
	if err != nil || retained.CodecVersion() != 2 || !bytes.Equal(retained.SummaryText(), summary) {
		t.Fatal("actual published retainedV2 exact summary and installed decoder", err)
	}
	assertOriginalMCPCompactionLineageV2(t, old, manifest, pairIDs)
	model, err := g.PrepareNextModel(t.Context())
	if err != nil || model == nil {
		t.Fatal("original committed retainedV2 successor MODEL preparation", err)
	}
	modelOp, err := model.Operation()
	if err != nil || modelOp.Sequence != 2 || modelOp.Kind != domain.AttemptOperationModelV1 || modelOp.ValidatePredecessor(r.Reservation, o) != nil {
		t.Fatal("original next MODEL lost exact compaction predecessor", err)
	}
	following, err := model.FrozenRequest()
	if err != nil || following.Digest() != next.Digest() || !bytes.Equal(following.Body(), next.Body()) {
		t.Fatal("next MODEL replaced publisher's exact rejoined frozen request", err)
	}
	nextBody := following.Body()
	defer clear(nextBody)
	nextInput := decodeOriginalMCPCompactionWireV2(t, nextBody)
	if nextInput.Summary != string(summary) {
		t.Fatal("next MODEL lost exact original retained summary")
	}
	for _, frame := range append(nextInput.Protected, nextInput.Units...) {
		if frame.RunID == string(old.completed.Run.ID) && (frame.Role == "tool_call" || frame.Role == "tool_result") {
			t.Error("summary-covered canonical pair rendered again in successor")
		}
	}
	if current, err := owners.entry.Source(); err != nil || !reflect.DeepEqual(current, source) || !reflect.DeepEqual(fresh.CanonicalSourceEvents(t), canonical) || !reflect.DeepEqual(old.completion.Snapshot(t), old.completed) {
		t.Fatal("compaction changed original funding, canonical bytes, or retired completion", err)
	}
	stats := fresh.Stats()
	if stats.Operation != before.Operation+1 || stats.Observation != before.Observation+1 || history.TTLReads != 0 || history.DetailReads[old.completed.Run.ID] == 0 {
		t.Fatalf("historical compaction resurrected effects or omitted retained join: before=%+v after=%+v TTL=%d", before, stats, history.TTLReads)
	}
	if oldReads, _, _ := reader.counts(); oldReads != 0 {
		t.Fatal("historical compaction read erased original operation bodies", oldReads)
	}
}

type originalMCPCompactionFrameV2 struct {
	SourceID string `json:"source_id"`
	EventID  string `json:"event_id"`
	RunID    string `json:"run_id"`
	Role     string `json:"original_role"`
}
type originalMCPCompactionInputV2 struct {
	Schema    string                         `json:"schema"`
	Summary   string                         `json:"derived_summary"`
	Protected []originalMCPCompactionFrameV2 `json:"protected_originals"`
	Units     []originalMCPCompactionFrameV2 `json:"original_units"`
}

func decodeOriginalMCPCompactionWireV2(t *testing.T, body []byte) originalMCPCompactionInputV2 {
	t.Helper()
	var wire struct{ Messages []struct{ Content string } }
	if err := json.Unmarshal(body, &wire); err != nil || len(wire.Messages) != 2 {
		t.Fatal("actual original compaction/MODEL wire", err)
	}
	var input originalMCPCompactionInputV2
	if err := json.Unmarshal([]byte(wire.Messages[1].Content), &input); err != nil {
		t.Fatal("actual original compaction/MODEL history", err)
	}
	return input
}

func assertOriginalMCPCompactionLineageV2(t *testing.T, old *originalMCPTerminalV2Fixture, manifest []byte, pairIDs []string) {
	t.Helper()
	var capsule struct {
		Operations []struct {
			Reservation domain.AttemptOperationReservationV1 `json:"reservation"`
			Observation domain.AttemptOperationObservationV1 `json:"observation"`
		} `json:"operations"`
	}
	if err := json.Unmarshal(old.capsule, &capsule); err != nil || len(capsule.Operations) != 3 {
		t.Fatal("original actual capsule operation lineage", err)
	}
	var retained struct {
		Schema string `json:"schema"`
		Cut    struct {
			Covered []uint32 `json:"covered"`
			Suffix  []uint32 `json:"suffix"`
			Sources []struct {
				SourceID   string               `json:"source_id"`
				Canonical  *domain.SessionEvent `json:"canonical"`
				Projection *struct {
					Reservation  domain.AttemptOperationReservationV1     `json:"reservation"`
					Observation  domain.AttemptOperationObservationV1     `json:"observation"`
					Conservative *compactioncontext.MCPConservativeDataV2 `json:"conservative"`
				} `json:"projection"`
			} `json:"sources"`
		} `json:"cut"`
	}
	if err := json.Unmarshal(manifest, &retained); err != nil || retained.Schema != "sessionless.compaction-manifest.v2" {
		t.Fatal("actual retainedV2 manifest", err)
	}
	positions := [2]int{-1, -1}
	for i, row := range retained.Cut.Sources {
		for j, event := range old.completed.Events[:2] {
			if row.Canonical == nil || row.Canonical.ID != event.ID {
				continue
			}
			want := capsule.Operations[j]
			key, err := compactioncontext.OperationSourceKeyV1(want.Reservation, want.Observation)
			if err != nil || row.SourceID != key || pairIDs[j] != key || !reflect.DeepEqual(*row.Canonical, event) || row.Projection == nil || !reflect.DeepEqual(row.Projection.Reservation, want.Reservation) || !reflect.DeepEqual(row.Projection.Observation, want.Observation) || positions[j] != -1 {
				t.Fatal("canonical pair lost unique exact original operation citation/projection", j, err)
			}
			positions[j] = i
			if j == 1 && (row.Projection.Conservative == nil || row.Projection.Conservative.Reserved != want.Reservation.Operation.Debit || row.Projection.Observation.Usage.State != domain.AttemptOperationUsageUnknownV1 || row.Projection.Observation.Usage.AmountMicrounits != nil) {
				t.Fatal("retained original MCP full debit/unknown usage lost")
			}
		}
	}
	if positions[0] < 0 || positions[1] != positions[0]+1 {
		t.Fatal("retained canonical pair incomplete/reordered", positions)
	}
	covered := func(position int) bool {
		for _, p := range retained.Cut.Covered {
			if int(p) == position {
				return true
			}
		}
		return false
	}
	if !covered(positions[0]) || !covered(positions[1]) {
		t.Fatal("actual next-turn compaction must cover the pair atomically", positions, retained.Cut.Covered, retained.Cut.Suffix)
	}
}
