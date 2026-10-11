package ydbstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are metadata envelopes, not conversation or blob bodies. An outer Job
// or outbox may quote an already bounded 32-KiB source and carry manifest refs;
// its bound is separate from the immutable authority-head codec bound.
const ownedCanonicalEnvelopeBytes = 128 << 10

// Only complete canonical point reads are registered. Never turn an arbitrary
// SELECT, range scan, alternate key or future helper into an unbounded fallback.
var ownedCanonicalJSONReads = map[string]int{
	"SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2":                                     ownedAuthorityHeadBytes,
	"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2":                             ownedAuthorityHeadBytes,
	"SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2":         ownedAuthorityHeadBytes,
	"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2":                                  ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM dispatch_outbox WHERE tenant_id = $1 AND dispatch_outbox_id = $2":              ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2":         ownedCanonicalEnvelopeBytes,
	"SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":                              ownedAuthorityHeadBytes,
	"SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":                     ownedAuthorityHeadBytes,
	"SELECT record FROM session_legal_holds WHERE tenant_id = $1 AND session_id = $2":                   ownedAuthorityHeadBytes,
	"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3": ownedAuthorityHeadBytes,
	"SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3":  ownedAuthorityHeadBytes,
}

func ownedCanonicalJSONSelect(statement string) (string, int, bool) {
	statement = strings.Join(strings.Fields(statement), " ")
	limit, ok := ownedCanonicalJSONReads[statement]
	if !ok {
		return "", 0, false
	}
	fields := strings.SplitN(statement, " ", 3)
	return fmt.Sprintf("SELECT SUBSTRING(CAST(%s AS String),0,%d) %s", fields[1], limit+1, fields[2]), limit, true
}

func (b *ownedAuthorityBudget) deny() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failed = true
	return domain.ErrMembershipDenied
}

func (q *ownedAuthorityQuery) readCanonicalJSON(ctx context.Context, statement string, args ...any) (string, error) {
	bounded, limit, ok := ownedCanonicalJSONSelect(statement)
	if !ok {
		return "", q.budget.deny()
	}
	var payload string
	returned, err := scanOwnedCurrentRow(q.QueryRowContext(ctx, bounded, args...), &payload)
	if q.budget.materialized(q.callback, returned) != nil {
		return "", domain.ErrMembershipDenied
	}
	if err == sql.ErrNoRows {
		return "", err
	}
	if err != nil || len(payload) > limit {
		return "", q.budget.deny()
	}
	return payload, nil
}

var _ stateSQLExecutor = (*ownedAuthorityQuery)(nil)
var _ stateSQLExecutor = (*sql.Tx)(nil)
