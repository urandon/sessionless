//go:build sessionless_mcp_offline

package ydbstore

import (
	"encoding/json"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
)

func (s *LocalSessionConnectedSeed) MCPOfflineInstalledGrant(t *testing.T) mcpadmission.MCPToolGrantV1 {
	t.Helper()
	g, e := s.Source.ReadMCPToolGrant(t.Context(), domain.OwnedAgentProfileReferenceV1{Locator: s.receipt.Profile.Locator, RevocationGeneration: s.receipt.Profile.RevocationGeneration, Digest: s.receipt.Digest}, time.Now().UTC().Truncate(time.Microsecond))
	if e != nil {
		t.Fatal("original installed MCP grant DATA", e)
	}
	return g
}

// MCPAfterCatalogFault changes only canonical scripted SQL DATA while holding
// its original database connection. The real same Store must reread it. There
// is no approval callback, replacement Store or fabricated permission.
func (s *LocalSessionConnectedSeed) MCPAfterCatalogFault(t *testing.T, kind string) {
	t.Helper()
	if s == nil || s.compaction == nil || (kind != "revoke_mcp" && kind != "missing_call_index") {
		t.Fatal("closed original MCP fault")
	}
	tx, e := s.DB.BeginTx(t.Context(), nil)
	if e != nil {
		t.Fatal("serialize exact SQL fault", e)
	}
	defer tx.Rollback()
	rows := s.compaction.compactionEffectSQL.operations
	if len(rows) != 2 || len(rows[1]) != 11 {
		t.Fatal("fault must follow two genuine reservations")
	}
	var reservation domain.AttemptOperationReservationV1
	if json.Unmarshal([]byte(rows[1][1].(string)), &reservation) != nil || reservation.Operation.Kind != domain.AttemptOperationMCPToolV1 || reservation.Operation.Sequence != 2 || rows[1][5] != uint64(0) || rows[1][6] != uint64(0) {
		t.Fatal("fault must precede exact original MCP BeforeSend")
	}
	if kind == "missing_call_index" {
		rows[1][9], rows[1][10] = uint64(0), ""
	} else {
		r := s.parent.base.reader
		r.spending[2] = r.spending[2].Clone()
		r.spending[2].State = domain.OwnedAgentCurrentRevokedV1
		r.spending[2].RevocationGeneration++
		r.spending[2].UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
		r.sync(t)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal("commit canonical scripted fault", e)
	}
}
