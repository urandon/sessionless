package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

var errSessionMaterialSource = errors.New("canonical material source unavailable")

const sessionMaterialCurrentSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 version, through_sequence, SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(session_id AS String),0,161),
 SUBSTRING(CAST(last_event_id AS String),0,161), SUBSTRING(CAST(prefix_digest AS String),0,65)
 FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`

const sessionMaterialBoundarySQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 version, through_sequence, SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(session_id AS String),0,161),
 SUBSTRING(CAST(last_event_id AS String),0,161), SUBSTRING(CAST(prefix_digest AS String),0,65)
 FROM session_material_boundaries_v1 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3`

// Two independent exact primary keys in ONE statement. UNION ALL retains an
// orphan boundary (unlike a current-rooted LEFT JOIN). Matching aliases and
// the outer projection preserve scan order under YQL's by-name UNION:
// https://ydb.tech/docs/en/yql/reference/syntax/select/union
// Tags express independent absence, never permission or a skipped check.
const ownedSessionMaterialPredecessorSQL = `SELECT prefix_kind, prefix_record, prefix_version,
 prefix_through, prefix_tenant, prefix_session, prefix_last, prefix_digest FROM (
 SELECT CAST(0 AS Uint64) AS prefix_kind,
 SUBSTRING(CAST(record AS String),0,8193) AS prefix_record, version AS prefix_version,
 through_sequence AS prefix_through, SUBSTRING(CAST(tenant_id AS String),0,161) AS prefix_tenant,
 SUBSTRING(CAST(session_id AS String),0,161) AS prefix_session,
 SUBSTRING(CAST(last_event_id AS String),0,161) AS prefix_last,
 SUBSTRING(CAST(prefix_digest AS String),0,65) AS prefix_digest
 FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2
 UNION ALL
 SELECT CAST(1 AS Uint64) AS prefix_kind,
 SUBSTRING(CAST(record AS String),0,8193) AS prefix_record, version AS prefix_version,
 through_sequence AS prefix_through, SUBSTRING(CAST(tenant_id AS String),0,161) AS prefix_tenant,
 SUBSTRING(CAST(session_id AS String),0,161) AS prefix_session,
 SUBSTRING(CAST(last_event_id AS String),0,161) AS prefix_last,
 SUBSTRING(CAST(prefix_digest AS String),0,65) AS prefix_digest
 FROM session_material_boundaries_v1 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3
 )`

type sessionMaterialPrefixRow struct {
	record, tenant, session, last, digest string
	version, through                      uint64
}

func (r *sessionMaterialPrefixRow) destinations() []any {
	return []any{&r.record, &r.version, &r.through, &r.tenant, &r.session, &r.last, &r.digest}
}

// Shared physical/canonical decoder; batching cannot create a weaker parser.
func (r sessionMaterialPrefixRow) decode(tenant domain.TenantID, session domain.SessionID, sequence uint64) (sessioncontext.SessionMaterialPrefixV1, error) {
	if len(r.record) > sessioncontext.MaxSessionMaterialPrefixBytesV1 || len(r.tenant) > 160 || len(r.session) > 160 || len(r.last) > 160 || len(r.digest) != 64 ||
		r.version != 1 || r.through != sequence || r.tenant != string(tenant) || r.session != string(session) {
		return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
	}
	prefix, err := sessioncontext.DecodeSessionMaterialPrefixV1([]byte(r.record))
	if err != nil || prefix.Version != uint32(r.version) || prefix.ThroughSequence != r.through ||
		prefix.TenantID != tenant || prefix.SessionID != session || string(prefix.LastEventID) != r.last || prefix.PrefixDigest != r.digest {
		return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
	}
	return prefix, nil
}

func readOwnedSessionMaterialPredecessor(ctx context.Context, q *ownedAuthorityQuery, tenant domain.TenantID, session domain.SessionID, sequence uint64) (sessioncontext.SessionMaterialPrefixV1, error) {
	deny := func() (sessioncontext.SessionMaterialPrefixV1, error) {
		_ = q.budget.deny()
		return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
	}
	if sequence == 0 {
		return deny()
	}
	rows, err := q.QueryContext(ctx, ownedSessionMaterialPredecessorSQL, tenant, session, sequence)
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	defer rows.Close()
	var values [2]sessionMaterialPrefixRow
	var seen [2]bool
	for rows.Next() {
		var kind uint64
		var row sessionMaterialPrefixRow
		dest := append([]any{&kind}, row.destinations()...)
		n, err := scanOwnedCurrentRow(rows, dest...)
		// Charge tags/scalars/detection bytes before any canonical JSON decode.
		// Exact PKs permit at most two rows; duplicate/unknown tags fail closed.
		if q.budget.materialized(q.callback, n) != nil || err != nil || kind > 1 || seen[kind] {
			return deny()
		}
		seen[kind], values[kind] = true, row
	}
	if err := rows.Err(); err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	if err := rows.Close(); err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	if seen == [2]bool{} {
		return sessioncontext.NewUnknownSessionMaterialPrefixV1(tenant, session, sequence)
	}
	if seen != [2]bool{true, true} {
		return deny()
	}
	current, err := values[0].decode(tenant, session, sequence)
	if err != nil {
		return deny()
	}
	boundary, err := values[1].decode(tenant, session, sequence)
	if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(current, boundary) {
		return deny()
	}
	return current, nil
}

// The byte charge is made BEFORE parsing, including oversize detection bytes
// and all projected scalars. Writers do not inherit the readonly choice budget;
// An owned canonical callback supplies its ORIGINAL budget even when a generic
// writer passes nil. Other writers/selection callers keep their existing charge.
func readSessionMaterialPrefixV1(ctx context.Context, query rowQuery, statement string,
	tenant domain.TenantID, session domain.SessionID, sequence uint64, charge func(int64) error,
) (sessioncontext.SessionMaterialPrefixV1, bool, error) {
	badSource := func() (sessioncontext.SessionMaterialPrefixV1, bool, error) {
		if q, ok := query.(*ownedAuthorityQuery); ok && charge == nil {
			_ = q.budget.deny()
		}
		return sessioncontext.SessionMaterialPrefixV1{}, false, errSessionMaterialSource
	}
	var row sessionMaterialPrefixRow
	args := []any{tenant, session}
	switch statement {
	case sessionMaterialCurrentSQL:
	case sessionMaterialBoundarySQL:
		args = append(args, sequence)
	default:
		return badSource()
	}
	var err error
	if q, ok := query.(*ownedAuthorityQuery); ok && charge == nil {
		err = q.scanMetadata(ctx, statement, args, row.destinations()...)
	} else {
		err = query.QueryRowContext(ctx, statement, args...).Scan(row.destinations()...)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return sessioncontext.SessionMaterialPrefixV1{}, false, nil
	}
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, false, err
	}
	if charge != nil {
		if err := charge(int64(len(row.record) + len(row.tenant) + len(row.session) + len(row.last) + len(row.digest) + 16)); err != nil {
			return sessioncontext.SessionMaterialPrefixV1{}, false, err
		}
	}
	prefix, err := row.decode(tenant, session, sequence)
	if err != nil {
		return badSource()
	}
	return prefix, true, nil
}

// The cache belongs ONLY to this SDK callback's sql.Tx. It avoids repeating
// the independent current/boundary reads for every event in a terminal batch;
// later folds consume the exact facts written earlier in that same transaction.
// A retry receives a fresh stateTx and cannot retain speculative fold output.
func loadSessionMaterialPredecessorTx(ctx context.Context, tx *stateTx, session domain.SessionID, sequence uint64) (sessioncontext.SessionMaterialPrefixV1, error) {
	if prefix, ok := tx.materialPrefixes[session]; ok {
		if prefix.TenantID != tx.tenantID || prefix.SessionID != session || prefix.ThroughSequence != sequence {
			return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
		}
		return prefix.Clone(), nil
	}
	if q, ok := tx.sqlTx.(*ownedAuthorityQuery); ok && sequence > 0 {
		return readOwnedSessionMaterialPredecessor(ctx, q, tx.tenantID, session, sequence)
	}
	current, found, err := readSessionMaterialPrefixV1(ctx, tx.sqlTx, sessionMaterialCurrentSQL, tx.tenantID, session, sequence, nil)
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	if sequence == 0 {
		if !found {
			return sessioncontext.NewUnknownSessionMaterialPrefixV1(tx.tenantID, session, 0)
		}
		genesis, err := sessioncontext.NewSessionMaterialGenesisV1(tx.tenantID, session)
		if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(current, genesis) {
			return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
		}
		return current, nil
	}
	boundary, anchored, err := readSessionMaterialPrefixV1(ctx, tx.sqlTx, sessionMaterialBoundarySQL, tx.tenantID, session, sequence, nil)
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	if !found && !anchored {
		return sessioncontext.NewUnknownSessionMaterialPrefixV1(tx.tenantID, session, sequence)
	}
	if !found || !anchored || !sessioncontext.EqualSessionMaterialPrefixV1(current, boundary) {
		return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
	}
	return current, nil
}

func prepareSessionMaterialFoldTx(ctx context.Context, tx *stateTx, event domain.SessionEvent, prepared *sessionmaterial.PreparedV1) (sessioncontext.SessionMaterialPrefixV1, error) {
	if event.TenantID != tx.tenantID || event.Validate() != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, errSessionMaterialSource
	}
	previous, err := loadSessionMaterialPredecessorTx(ctx, tx, event.SessionID, event.Sequence-1)
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, err
	}
	var measure *sessioncontext.FinalMaterialMeasureV1
	if prepared != nil {
		final, err := sessioncontext.MeasurePreparedRecordV1(event, prepared)
		if err == nil {
			measure = &final
		}
	}
	// Missing, released, oversize or unsupported preparation records UNKNOWN,
	// not a new rejection of otherwise valid canonical history.
	return sessioncontext.FoldSessionMaterialV1(previous, event, measure)
}

func writeSessionMaterialFoldTx(ctx context.Context, tx *stateTx, prefix sessioncontext.SessionMaterialPrefixV1) error {
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(prefix)
	if err != nil || prefix.TenantID != tx.tenantID || prefix.ThroughSequence == 0 {
		return errSessionMaterialSource
	}
	if _, err := tx.sqlTx.ExecContext(ctx, `INSERT INTO session_material_boundaries_v1
 (tenant_id,session_id,sequence,version,through_sequence,last_event_id,prefix_digest,record)
 VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS String))`,
		prefix.TenantID, prefix.SessionID, prefix.ThroughSequence, uint64(prefix.Version), prefix.ThroughSequence, prefix.LastEventID, prefix.PrefixDigest, string(encoded)); err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx, `UPSERT INTO session_material_current_v1
 (tenant_id,session_id,version,through_sequence,last_event_id,prefix_digest,record)
 VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS String))`,
		prefix.TenantID, prefix.SessionID, uint64(prefix.Version), prefix.ThroughSequence, prefix.LastEventID, prefix.PrefixDigest, string(encoded)); err != nil {
		return err
	}
	if tx.materialPrefixes == nil {
		tx.materialPrefixes = make(map[domain.SessionID]sessioncontext.SessionMaterialPrefixV1)
	}
	tx.materialPrefixes[prefix.SessionID] = prefix.Clone()
	return nil
}

func createSessionMaterialGenesisTx(ctx context.Context, tx *stateTx, session domain.Session) error {
	// General Session metadata may describe legacy nonempty history. Only an
	// actually new EMPTY Session gets complete genesis. Existing retries return
	// before this helper and neither overwrite nor repair missing facts.
	if session.LastEventSequence != 0 {
		return nil
	}
	prefix, err := sessioncontext.NewSessionMaterialGenesisV1(session.TenantID, session.ID)
	if err != nil || session.TenantID != tx.tenantID {
		return errSessionMaterialSource
	}
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(prefix)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx, `INSERT INTO session_material_current_v1
 (tenant_id,session_id,version,through_sequence,last_event_id,prefix_digest,record)
 VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS String))`,
		prefix.TenantID, prefix.SessionID, uint64(prefix.Version), uint64(0), "", prefix.PrefixDigest, string(encoded))
	return err
}

// Exact Session partition, bounded key-only rows; malformed/orphan records are
// still retained in the deletion count, never silently omitted by a decoder.
func countSessionMaterialRows(ctx context.Context, db *sql.DB, statement string, tenant domain.TenantID, session domain.SessionID, remaining uint64) (uint64, error) {
	rows, err := db.QueryContext(ctx, statement, tenant, session, remaining+1)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count uint64
	for rows.Next() {
		count++
		if count > remaining {
			return 0, domain.ValidationError{Field: "session_deletion.material_rows", Reason: "exceeds the configured total inventory bound"}
		}
		var key any
		if err := rows.Scan(&key); err != nil {
			return 0, err
		}
	}
	return count, rows.Err()
}
