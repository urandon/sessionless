//go:build sessionless_mcp_offline

package ydbstore

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// MCPOfflineTerminalSeed installs the existing scripted SQL adapters before
// construction. It does not replace the original Source, issuers or scope.
func MCPOfflineTerminalSeed(t *testing.T, s *LocalSessionConnectedSeed) *LocalTerminalPublicationSessionConnectedSeed {
	t.Helper()
	if s == nil || s.compaction == nil || s.Stats().Parent != 0 {
		t.Fatal("terminal adapters must precede original invocation entry")
	}
	return terminalPublicationSessionSeed(t, s)
}

// MCPOfflineOperationReadback is read-only scripted canonical SQL DATA. No
// capability, issuer MAC, Source or prepared owner is manufactured here.
func (s *LocalSessionConnectedSeed) MCPOfflineOperationReadback(t *testing.T) ([]domain.AttemptOperationReservationV1, []domain.AttemptOperationObservationV1) {
	t.Helper()
	if s == nil || s.compaction == nil {
		t.Fatal("original MCP connected SQL seed required")
	}
	observations := s.PersistedCompactionObservations(t)
	reservations := make([]domain.AttemptOperationReservationV1, len(observations))
	for i, row := range s.compaction.operations {
		var r domain.AttemptOperationReservationV1
		raw, ok := row[1].(string)
		if !ok || json.Unmarshal([]byte(raw), &r) != nil || r.Validate() != nil || observations[i].ValidateForReservation(r) != nil {
			t.Fatalf("durable reservation/observation row%d mismatch", i+1)
		}
		rd, err := r.Digest()
		if err != nil || row[2] != string(rd) {
			t.Fatalf("durable reservation digest row%d mismatch: %v", i+1, err)
		}
		reservations[i] = r.Clone()
	}
	return reservations, observations
}
