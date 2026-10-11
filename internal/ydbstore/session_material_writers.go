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

// The byte charge is made BEFORE parsing, including oversize detection bytes
// and all projected scalars. Writers do not inherit the readonly choice budget;
// passing nil here is not a claim that their total transaction work is bounded.
// Selection integration must supply its operation-wide materialization charge.
func readSessionMaterialPrefixV1(ctx context.Context, query rowQuery, statement string,
	tenant domain.TenantID, session domain.SessionID, sequence uint64, charge func(int64) error,
) (sessioncontext.SessionMaterialPrefixV1, bool, error) {
	var record, rowTenant, rowSession, lastID, digest string
	var version, through uint64
	args := []any{tenant, session}
	switch statement {
	case sessionMaterialCurrentSQL:
	case sessionMaterialBoundarySQL:
		args = append(args, sequence)
	default:
		return sessioncontext.SessionMaterialPrefixV1{}, false, errSessionMaterialSource
	}
	err := query.QueryRowContext(ctx, statement, args...).Scan(&record, &version, &through, &rowTenant, &rowSession, &lastID, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return sessioncontext.SessionMaterialPrefixV1{}, false, nil
	}
	if err != nil {
		return sessioncontext.SessionMaterialPrefixV1{}, false, err
	}
	if charge != nil {
		if err := charge(int64(len(record) + len(rowTenant) + len(rowSession) + len(lastID) + len(digest) + 16)); err != nil {
			return sessioncontext.SessionMaterialPrefixV1{}, false, err
		}
	}
	if len(record) > sessioncontext.MaxSessionMaterialPrefixBytesV1 || len(rowTenant) > 160 || len(rowSession) > 160 || len(lastID) > 160 || len(digest) != 64 ||
		version != 1 || through != sequence || rowTenant != string(tenant) || rowSession != string(session) {
		return sessioncontext.SessionMaterialPrefixV1{}, false, errSessionMaterialSource
	}
	prefix, err := sessioncontext.DecodeSessionMaterialPrefixV1([]byte(record))
	if err != nil || prefix.Version != uint32(version) || prefix.ThroughSequence != through ||
		prefix.TenantID != tenant || prefix.SessionID != session || string(prefix.LastEventID) != lastID || prefix.PrefixDigest != digest {
		return sessioncontext.SessionMaterialPrefixV1{}, false, errSessionMaterialSource
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
