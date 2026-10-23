package ydbstore

import (
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Public preparation and the earlier original Store effects/readback are real.
// q.at and the publication tuple BELOW are DATA ONLY: no authentic transport
// proof, publisher transaction, send capability or proxy publication is made.
func localMCPProxyContinuationDataFixture(t *testing.T) (*compactionSourceFixture, *ownedOperationRow, ports.OwnedProxyPublicationV2) {
	t.Helper()
	f, mcp := completedOriginalMCPFixture(t)
	model, err := f.entry.PrepareModel(t.Context())
	if err != nil || model == nil {
		t.Fatal("genuine original MODEL3 preparation", err)
	}
	op, err := model.Operation()
	if err != nil || op.Sequence != 3 {
		t.Fatal("actual prepared successor operation", err)
	}
	digest, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	observation := mcp.observation.Observation.Clone()
	previous := &ownedOperationRow{reservation: mcp.reservation.Clone(), observation: &observation}
	if ownedObservationAllowsSuccessor(previous.reservation, *previous.observation) {
		t.Fatal("generic measured-money gate unexpectedly permits UNKNOWN MCP")
	}
	data := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, ServiceKind: domain.RegisteredProxyProviderV1, OperationSequence: op.Sequence, OperationDigest: digest, OperationDeadline: op.Deadline}
	return f, previous, data
}

func callLocalMCPProxyContinuationData(t *testing.T, f *compactionSourceFixture, store *Store, source domain.AttemptExecutionSourceV1, previous *ownedOperationRow, proof ports.OwnedProxyPublicationV2, at time.Time) bool {
	t.Helper()
	ctx, finish := ownedAuthorityCallContext(t.Context())
	defer finish()
	unlock, err := f.entry.beginLocalMutation(ctx)
	if err != nil {
		t.Fatal("original entry mutation barrier for helper DATA check", err)
	}
	defer unlock()
	return store.localMCPProxyContinuation(ctx, &ownedAuthorityQuery{at: at}, f.entry.parent, source, proof, previous)
}

func TestLocalMCPProxyContinuationOriginalPreparedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, previous, proof := localMCPProxyContinuationDataFixture(t)
		originalSource := f.entry.source.Clone()
		originalModel := f.entry.preparedModel.Load()
		originalRows := cloneGenerationRows(f.queries.operations)
		checkPositive := func() {
			t.Helper()
			if !callLocalMCPProxyContinuationData(t, f, f.store, f.entry.source, previous, proof, f.parent.graph.at) {
				t.Fatal("exact genuine prepared MODEL3 tuple not recognized as continuation DATA")
			}
			if ownedObservationAllowsSuccessor(previous.reservation, *previous.observation) {
				t.Fatal("local continuation changed generic measured-money gate")
			}
		}
		checkPositive()
		for _, name := range []string{"digest", "deadline_earlier", "deadline_later", "service", "transport", "sequence_previous", "sequence_future", "absent_original_slot", "copied_original_slot", "foreign_store", "foreign_source", "foreign_parent", "missing_previous", "missing_observation", "changed_observation", "expired_funding", "at_model_deadline"} {
			candidate := proof.Clone()
			source := f.entry.source.Clone()
			parent := f.entry.parent.Clone()
			row := &ownedOperationRow{reservation: previous.reservation.Clone()}
			observation := previous.observation.Clone()
			row.observation = &observation
			at, store := f.parent.graph.at, f.store
			switch name {
			case "digest":
				candidate.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("f", 64))
			case "deadline_earlier":
				candidate.OperationDeadline = candidate.OperationDeadline.Add(-time.Microsecond)
			case "deadline_later":
				candidate.OperationDeadline = candidate.OperationDeadline.Add(time.Microsecond)
			case "service":
				candidate.ServiceKind = domain.RegisteredProxyMCPToolV1
			case "transport":
				candidate.TransportKind = domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1
			case "sequence_previous":
				candidate.OperationSequence--
			case "sequence_future":
				candidate.OperationSequence++
			case "absent_original_slot":
				f.entry.preparedModel.Store(nil)
			case "copied_original_slot":
				copy := *originalModel
				f.entry.preparedModel.Store(&copy)
			case "foreign_store":
				copy := *f.store
				store = &copy // different object is not the registered original Store
			case "foreign_source":
				source.Scope.AttemptID = "foreign-attempt"
			case "foreign_parent":
				parent.Scope.RunID = "foreign-run"
			case "missing_previous":
				row = nil
			case "missing_observation":
				row.observation = nil
			case "changed_observation":
				row.observation.ResponseDigest = strings.Repeat("e", 64)
			case "expired_funding":
				at = source.Spending[2].Window.ExpiresAt
			case "at_model_deadline":
				at = candidate.OperationDeadline
			}
			ctx, finish := ownedAuthorityCallContext(t.Context())
			unlock, err := f.entry.beginLocalMutation(ctx)
			if err != nil {
				finish()
				t.Fatal("original barrier", name, err)
			}
			allowed := store.localMCPProxyContinuation(ctx, &ownedAuthorityQuery{at: at}, parent, source, candidate, row)
			unlock()
			finish()
			f.entry.preparedModel.Store(originalModel)
			if allowed {
				t.Error("changed/foreign/unfunded continuation DATA accepted", name)
			}
			checkPositive() // no negative case may poison or establish another owner
		}
		if !reflect.DeepEqual(originalSource, f.entry.source) || !reflect.DeepEqual(originalRows, f.queries.operations) || f.entry.preparedModel.Load() != originalModel {
			t.Fatal("DATA helper checks changed original funding, durable effects or prepared ownership")
		}
	})
}

func TestLocalMCPProxyContinuationClosedOriginalSourceDeniesData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, previous, proof := localMCPProxyContinuationDataFixture(t)
		if !callLocalMCPProxyContinuationData(t, f, f.store, f.entry.source, previous, proof, f.parent.graph.at) {
			t.Fatal("live same-original positive control")
		}
		if err := f.actual.source.Close(); err != nil {
			t.Fatal(err)
		}
		// A bounded DATA-only caller cannot replace the closed original Source
		// lifetime. Historical classification remains available for cleanup, not
		// a current publisher transaction or another reserve/send permission.
		ctx, finish := ownedAuthorityCallContext(t.Context())
		defer finish()
		f.entry.preparationMu.Lock()
		allowed := f.store.localMCPProxyContinuation(ctx, &ownedAuthorityQuery{at: f.parent.graph.at}, f.entry.parent, f.entry.source, proof, previous)
		f.entry.preparationMu.Unlock()
		if allowed || !f.entry.originalObservationAllowsSuccessor(previous.reservation, *previous.observation) || ownedObservationAllowsSuccessor(previous.reservation, *previous.observation) {
			t.Fatal("closed Source restored current permission or destroyed historical/generic distinction")
		}
	})
}
