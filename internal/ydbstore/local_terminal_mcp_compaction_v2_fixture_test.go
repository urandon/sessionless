package ydbstore

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Test-only inspection of the SAME original accounting row already written by
// setup/reserve. It creates no ledger, owner, approval or permission. In
// particular the preexisting setup body-custody debit is not refunded/omitted.
func (s *LocalSessionConnectedSeed) MCPHistoryCompactionAccounting(t *testing.T) (domain.AttemptOperationDebitV1, [4]uint64) {
	t.Helper()
	if s == nil || s.parent == nil || s.parent.sql == nil || len(s.parent.sql.accountingRow) != 4 {
		t.Fatal("original accounting SQL DATA required")
	}
	var value ownedAttemptAccounting
	if err := json.Unmarshal([]byte(s.parent.sql.accountingRow[2].(string)), &value); err != nil {
		t.Fatal("original accounting row encoding", err)
	}
	return value.Aggregate, value.Amounts
}
