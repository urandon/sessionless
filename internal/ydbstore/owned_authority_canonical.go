package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
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
	"SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2":             ownedAuthorityHeadBytes,
	"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2":                                  ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM dispatch_outbox WHERE tenant_id = $1 AND dispatch_outbox_id = $2":              ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2":                  ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2":         ownedCanonicalEnvelopeBytes,
	"SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2":             ownedCanonicalEnvelopeBytes,
	"SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":                              ownedAuthorityHeadBytes,
	"SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3":      ownedAuthorityHeadBytes,
	"SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2":                     ownedAuthorityHeadBytes,
	"SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2":                      ownedAuthorityHeadBytes,
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
	return q.readMetadata(ctx, bounded, limit, args...)
}

type ownedMetadataCacheEntry struct {
	table string
	raw   string
	err   error
}

// A bounded snapshot-local cache, never permission or cross-callback state.
// Every SQL write invalidates its table BEFORE execution; unknown write forms
// clear everything. Missing rows are invalidated too, so read-your-writes holds.
func (q *ownedAuthorityQuery) invalidateMetadata(statement string) {
	fields := strings.Fields(statement)
	table := ""
	if len(fields) >= 3 && ((fields[0] == "UPSERT" || fields[0] == "INSERT") && fields[1] == "INTO" || fields[0] == "DELETE" && fields[1] == "FROM") {
		table = fields[2]
	} else if len(fields) >= 2 && fields[0] == "UPDATE" {
		table = fields[1]
	}
	table = strings.Trim(table, "`")
	for _, c := range table {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			table = "" // qualified/unknown syntax cannot leave stale cached rows
			break
		}
	}
	for key, entry := range q.metadataCache {
		if table == "" || entry.table == table {
			delete(q.metadataCache, key)
		}
	}
}

func (q *ownedAuthorityQuery) readMetadata(ctx context.Context, statement string, limit int, args ...any) (string, error) {
	q.budget.mu.Lock()
	valid := ctx.Err() == nil && !q.budget.failed && q.budget.active && q.callback == q.budget.callbacks
	q.budget.mu.Unlock()
	if !valid {
		return "", q.budget.deny()
	}
	encoded, err := json.Marshal(args)
	if err != nil {
		return "", q.budget.deny()
	}
	key := statement + "\n" + string(encoded)
	if cached, ok := q.metadataCache[key]; ok {
		return cached.raw, cached.err
	}
	var payload string
	returned, err := scanOwnedCurrentRow(q.QueryRowContext(ctx, statement, args...), &payload)
	if q.budget.materialized(q.callback, returned) != nil {
		return "", domain.ErrMembershipDenied
	}
	if err == sql.ErrNoRows {
		q.cacheMetadata(key, statement, "", err)
		return "", err
	}
	if err != nil || len(payload) > limit {
		return "", q.budget.deny()
	}
	q.cacheMetadata(key, statement, payload, nil)
	return payload, nil
}

func (q *ownedAuthorityQuery) cacheMetadata(key, statement, raw string, err error) {
	if q.metadataCache == nil {
		return
	}
	fields := strings.Fields(statement)
	for i, field := range fields {
		if field == "FROM" && i+1 < len(fields) {
			q.metadataCache[key] = ownedMetadataCacheEntry{table: fields[i+1], raw: raw, err: err}
			return
		}
	}
}

func (q *ownedAuthorityQuery) scanMetadata(ctx context.Context, statement string, args []any, destinations ...any) error {
	returned, err := scanOwnedCurrentRow(q.QueryRowContext(ctx, statement, args...), destinations...)
	if q.budget.materialized(q.callback, returned) != nil {
		return domain.ErrMembershipDenied
	}
	if err != nil && err != sql.ErrNoRows {
		return q.budget.deny()
	}
	return err
}

var _ stateSQLExecutor = (*ownedAuthorityQuery)(nil)
var _ stateSQLExecutor = (*sql.Tx)(nil)
