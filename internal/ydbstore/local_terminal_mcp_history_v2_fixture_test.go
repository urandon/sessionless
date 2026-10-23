package ydbstore

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// V2 test bridge: only actual committed STORAGE DATA and installation files
// enter the existing OpenExisting/new-funding/new-Store fixture path. No old
// live entry, issuer, witness, source, reservation or cleanup owner is reused.
func NewFreshRunAfterMCPTerminalV2Seed(t *testing.T, old *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, blobs ports.BlobStore, name string) (*LocalTerminalPublicationSessionConnectedSeed, *LocalFreshRunHistorySQLFixture) {
	t.Helper()
	state := completed.Snapshot(t)
	if completed.cleanup.seed != old || state.Finalizations != 1 || state.Run.Status != domain.RunSucceeded || len(state.Events) != 3 || state.Run.TerminalCustody == nil || state.Run.TerminalCustody.LocalEvidence == nil {
		t.Fatal("genuine three-event completed original MCP terminal required")
	}
	for i, kind := range []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage} {
		if state.Events[i].Kind != kind || state.Events[i].RunID == nil || *state.Events[i].RunID != state.Run.ID {
			t.Fatal("actual canonical MCP pair+answer required", i)
		}
	}
	history := localFreshCommittedHistory(t, old, state, nil)
	events := append(old.CanonicalSourceEvents(t), state.Events...)
	fresh, history, _ := newFreshRunFromCommittedHistory(t, old.LocalSessionConnectedSeed, state.Session, events, history, blobs, name)
	return fresh, history
}

// Negative DATA faults keep the authentic old marker/seal unchanged. These
// mutations cannot manufacture a valid financial owner or billing fact.
func (f *LocalFreshRunHistorySQLFixture) CorruptMCPTerminalV2(t *testing.T, kind string) {
	t.Helper()
	row := append([]driver.Value(nil), f.details[f.oldAttempt]...)
	if len(row) != 11 {
		t.Fatal("original bounded historical detail columns")
	}
	switch kind {
	case "financial_parent":
		var parent domain.OwnedAttemptEffectParentV1
		if err := json.Unmarshal([]byte(row[0].(string)), &parent); err != nil || parent.Authority.AdmissionCostCeiling.MCPToolCost == nil || parent.Authority.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits == nil {
			t.Fatal("actual original financial parent required", err)
		}
		amount := *parent.Authority.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits + 1
		parent.Authority.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits = &amount
		raw, err := json.Marshal(parent)
		if err != nil {
			t.Fatal(err)
		}
		row[0] = string(raw)
	case "accounting":
		var accounting ownedAttemptAccounting
		if err := json.Unmarshal([]byte(row[9].(string)), &accounting); err != nil {
			t.Fatal(err)
		}
		accounting.Aggregate.AmountMicrounits++
		raw, err := json.Marshal(accounting)
		if err != nil {
			t.Fatal(err)
		}
		row[9] = string(raw)
	default:
		t.Fatal("unknown original financial DATA mutation", kind)
	}
	f.details[f.oldAttempt] = row
}

// Closed grammar evidence on an authentic producer's retained bytes. Mutated
// variants below are negative DATA only; no reference, Run seal or accounting
// authority is reminted to make a changed capsule appear authentic.
func InspectMCPTerminalCapsuleVersionDenialsV2(t *testing.T, body []byte) {
	t.Helper()
	v, err := decodeLocalTerminalCapsuleV2(body)
	if err != nil {
		t.Fatal("authentic joined V2 capsule must decode before version controls", err)
	}
	var old localRetainedTerminalCapsuleV1
	if decodeOwnedEffectCanonicalJSON(string(body), &old) == nil {
		t.Fatal("V2 capsule was accepted as old V1 grammar")
	}
	v.Schema = "sessionless.terminal-custody.v1"
	mixed, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLocalTerminalCapsuleV2(mixed); err == nil {
		t.Fatal("V1 schema with V2 projections/V3 accounting accepted")
	}
	clear(mixed)
	v.Schema = "sessionless.terminal-custody.v2"
	v.Accounting.Version = 2
	mixed, err = json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLocalTerminalCapsuleV2(mixed); err == nil {
		t.Fatal("V2 capsule with old accounting version accepted")
	}
	clear(mixed)
}
