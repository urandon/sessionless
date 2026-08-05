package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const maxSessionPageSize = uint64(200)

var (
	ErrSessionConflict  = errors.New("canonical session already exists with different content")
	ErrBindingConflict  = errors.New("frontend binding already exists with different content")
	ErrSnapshotConflict = errors.New("session snapshot already exists with different content")
)

func (store *Store) CreateSession(
	ctx context.Context,
	session domain.Session,
	owner domain.SessionParticipant,
) error {
	if err := validateSessionOwner(session, owner); err != nil {
		return err
	}
	return store.Transact(ctx, session.TenantID, func(state ports.StateTx) error {
		return createSessionTx(ctx, state.(*stateTx), session, owner)
	})
}

func (store *Store) CreateAndSwitchSession(
	ctx context.Context,
	session domain.Session,
	owner domain.SessionParticipant,
	bindingID domain.FrontendBindingID,
	expectedRevision uint64,
	at time.Time,
) (result domain.FrontendBinding, err error) {
	if err := validateSessionOwner(session, owner); err != nil {
		return result, err
	}
	if session.Status != domain.SessionActive {
		return result, domain.ValidationError{
			Field:  "frontend_binding.session_id",
			Reason: "must reference an active session",
		}
	}
	if err := bindingID.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, session.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		binding, found, err := readBindingTx(ctx, tx, bindingID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("frontend binding %q not found", bindingID)
		}
		if binding.Revision == expectedRevision+1 && binding.SessionID == session.ID && binding.UpdatedAt.Equal(at) {
			matches, err := sessionAndOwnerMatchTx(ctx, tx, session, owner)
			if err != nil {
				return err
			}
			if matches {
				result = binding
				return nil
			}
			return ErrSessionConflict
		}
		if binding.Revision != expectedRevision {
			return domain.StaleBindingError{Expected: expectedRevision, Actual: binding.Revision}
		}
		nextBinding := binding
		if err := nextBinding.Switch(expectedRevision, session.ID, at); err != nil {
			return err
		}
		if err := createSessionTx(ctx, tx, session, owner); err != nil {
			return err
		}
		if err := writeBindingTx(ctx, tx, nextBinding); err != nil {
			return err
		}
		result = nextBinding
		return nil
	})
	return result, err
}

func (store *Store) GetSession(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (domain.Session, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.Session{}, false, err
	}
	if err := sessionID.Validate(); err != nil {
		return domain.Session{}, false, err
	}
	return readJSON[domain.Session](ctx, store.db,
		`SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
		tenantID, sessionID,
	)
}

func (store *Store) BindFrontend(ctx context.Context, binding domain.FrontendBinding) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	return store.Transact(ctx, binding.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		session, found, err := readSessionTx(ctx, tx, binding.SessionID)
		if err != nil {
			return err
		}
		if !found || session.Status != domain.SessionActive {
			return domain.ValidationError{Field: "frontend_binding.session_id", Reason: "must reference an active session"}
		}
		existing, found, err := readBindingTx(ctx, tx, binding.ID)
		if err != nil {
			return err
		}
		if found {
			if reflect.DeepEqual(existing, binding) {
				return nil
			}
			return ErrBindingConflict
		}
		var existingID string
		err = tx.sqlTx.QueryRowContext(ctx,
			`SELECT binding_id FROM frontend_binding_keys
			 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
			binding.TenantID, binding.Frontend, binding.ExternalConversationID,
		).Scan(&existingID)
		switch {
		case err == nil && existingID != string(binding.ID):
			return ErrBindingConflict
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}
		return writeBindingTx(ctx, tx, binding)
	})
}

func (store *Store) ResolveFrontendBinding(
	ctx context.Context,
	tenantID domain.TenantID,
	frontend domain.Frontend,
	externalConversationID string,
) (domain.FrontendBinding, bool, error) {
	if err := tenantID.Validate(); err != nil {
		return domain.FrontendBinding{}, false, err
	}
	if err := domain.ValidateOpaqueID("frontend", string(frontend)); err != nil {
		return domain.FrontendBinding{}, false, err
	}
	if strings.TrimSpace(externalConversationID) == "" {
		return domain.FrontendBinding{}, false, domain.ValidationError{Field: "external_conversation_id", Reason: "must not be empty"}
	}
	var bindingID string
	err := store.db.QueryRowContext(ctx,
		`SELECT binding_id FROM frontend_binding_keys
		 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
		tenantID, frontend, externalConversationID,
	).Scan(&bindingID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.FrontendBinding{}, false, nil
	}
	if err != nil {
		return domain.FrontendBinding{}, false, err
	}
	return readJSON[domain.FrontendBinding](ctx, store.db,
		`SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2`,
		tenantID, bindingID,
	)
}

func (store *Store) SwitchFrontendBinding(
	ctx context.Context,
	tenantID domain.TenantID,
	bindingID domain.FrontendBindingID,
	expectedRevision uint64,
	sessionID domain.SessionID,
	at time.Time,
) (result domain.FrontendBinding, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, err
	}
	if err := bindingID.Validate(); err != nil {
		return result, err
	}
	if err := sessionID.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		binding, found, err := readBindingTx(ctx, tx, bindingID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("frontend binding %q not found", bindingID)
		}
		session, found, err := readSessionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found || session.Status != domain.SessionActive {
			return domain.ValidationError{Field: "frontend_binding.session_id", Reason: "must reference an active session"}
		}
		if err := binding.Switch(expectedRevision, sessionID, at); err != nil {
			return err
		}
		if err := writeBindingTx(ctx, tx, binding); err != nil {
			return err
		}
		result = binding
		return nil
	})
	return result, err
}

func (store *Store) AppendSessionEvent(
	ctx context.Context,
	event domain.SessionEvent,
) (created bool, err error) {
	if err := event.TenantID.Validate(); err != nil {
		return false, err
	}
	if err := event.SessionID.Validate(); err != nil {
		return false, err
	}
	if err := event.IdempotencyKey.Validate(); err != nil {
		return false, err
	}
	err = store.Transact(ctx, event.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		candidate := event
		session, found, err := readSessionTx(ctx, tx, candidate.SessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("session %q not found", candidate.SessionID)
		}
		var sequence uint64
		var eventID string
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT sequence, event_id FROM session_event_idempotency
			 WHERE tenant_id = $1 AND session_id = $2 AND idempotency_key = $3`,
			candidate.TenantID, candidate.SessionID, candidate.IdempotencyKey,
		).Scan(&sequence, &eventID)
		switch {
		case lookupErr == nil:
			existing, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
				`SELECT record FROM session_events
				 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
				candidate.TenantID, candidate.SessionID, sequence,
			)
			if err != nil {
				return err
			}
			if !found || string(existing.ID) != eventID {
				return fmt.Errorf("session event idempotency index is inconsistent")
			}
			if candidate.Sequence == 0 {
				candidate.Sequence = sequence
			}
			_, err = domain.AppendSessionEvent(&session, candidate, &existing)
			return err
		case !errors.Is(lookupErr, sql.ErrNoRows):
			return lookupErr
		}

		if candidate.Sequence == 0 {
			candidate.Sequence = session.LastEventSequence + 1
		}
		previous := session
		created, err = domain.AppendSessionEvent(&session, candidate, nil)
		if err != nil {
			return err
		}
		record, err := marshal(candidate)
		if err != nil {
			return err
		}
		authorID, runID := "", ""
		if candidate.AuthorUserID != nil {
			authorID = string(*candidate.AuthorUserID)
		}
		if candidate.RunID != nil {
			runID = string(*candidate.RunID)
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO session_events
			 (tenant_id, session_id, sequence, event_id, kind, author_user_id,
			  run_id, idempotency_key, blob_key, created_at, record)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS JsonDocument))`,
			candidate.TenantID, candidate.SessionID, candidate.Sequence, candidate.ID, candidate.Kind,
			authorID, runID, candidate.IdempotencyKey, candidate.Payload.Key, candidate.CreatedAt, record,
		); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO session_event_idempotency
			 (tenant_id, session_id, idempotency_key, sequence, event_id, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			candidate.TenantID, candidate.SessionID, candidate.IdempotencyKey,
			candidate.Sequence, candidate.ID, candidate.CreatedAt,
		); err != nil {
			return err
		}
		return updateSessionTx(ctx, tx, previous, session)
	})
	return created, err
}

func (store *Store) PutSessionSnapshot(ctx context.Context, snapshot domain.SessionSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	return store.Transact(ctx, snapshot.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		session, found, err := readSessionTx(ctx, tx, snapshot.SessionID)
		if err != nil {
			return err
		}
		if !found || snapshot.ThroughSequence > session.LastEventSequence {
			return domain.ValidationError{Field: "session_snapshot.through_sequence", Reason: "must reference persisted session history"}
		}
		existing, found, err := readJSON[domain.SessionSnapshot](ctx, tx.sqlTx,
			`SELECT record FROM session_snapshots
			 WHERE tenant_id = $1 AND session_id = $2 AND version = $3`,
			snapshot.TenantID, snapshot.SessionID, snapshot.Version,
		)
		if err != nil {
			return err
		}
		if found {
			if reflect.DeepEqual(existing, snapshot) {
				return nil
			}
			return ErrSnapshotConflict
		}
		var latestVersion, throughSequence uint64
		err = tx.sqlTx.QueryRowContext(ctx,
			`SELECT version, through_sequence FROM session_snapshots
			 WHERE tenant_id = $1 AND session_id = $2
			 ORDER BY version DESC LIMIT 1`,
			snapshot.TenantID, snapshot.SessionID,
		).Scan(&latestVersion, &throughSequence)
		if errors.Is(err, sql.ErrNoRows) {
			latestVersion, throughSequence = 0, 0
		} else if err != nil {
			return err
		}
		if snapshot.Version != latestVersion+1 || snapshot.ThroughSequence < throughSequence {
			return domain.ValidationError{Field: "session_snapshot.version", Reason: "must append the next non-regressing snapshot version"}
		}
		record, err := marshal(snapshot)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(ctx,
			`INSERT INTO session_snapshots
			 (tenant_id, session_id, version, snapshot_id, through_sequence,
			  blob_key, created_at, record)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, CAST($8 AS JsonDocument))`,
			snapshot.TenantID, snapshot.SessionID, snapshot.Version, snapshot.ID,
			snapshot.ThroughSequence, snapshot.Payload.Key, snapshot.CreatedAt, record,
		)
		return err
	})
}

func (store *Store) ListSessionSnapshots(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	afterVersion uint64,
	limit uint64,
) ([]domain.SessionSnapshot, error) {
	if err := validateSessionRead(tenantID, sessionID, limit); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT record FROM session_snapshots
		 WHERE tenant_id = $1 AND session_id = $2 AND version > $3
		 ORDER BY version ASC LIMIT $4`,
		tenantID, sessionID, afterVersion, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return decodeRows[domain.SessionSnapshot](rows)
}

func (store *Store) ArchiveSession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, at time.Time) error {
	return store.transitionSession(ctx, tenantID, sessionID, func(session *domain.Session) error { return session.Archive(at) })
}

func (store *Store) UnarchiveSession(ctx context.Context, tenantID domain.TenantID, sessionID domain.SessionID, at time.Time) error {
	return store.transitionSession(ctx, tenantID, sessionID, func(session *domain.Session) error { return session.Unarchive(at) })
}

func (store *Store) transitionSession(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	transition func(*domain.Session) error,
) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := sessionID.Validate(); err != nil {
		return err
	}
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		session, found, err := readSessionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("session %q not found", sessionID)
		}
		previous := session
		if err := transition(&session); err != nil {
			return err
		}
		return updateSessionTx(ctx, tx, previous, session)
	})
}

func (store *Store) ListSessions(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	limit uint64,
) ([]domain.Session, error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	if err := userID.Validate(); err != nil {
		return nil, err
	}
	if err := validatePageLimit(limit); err != nil {
		return nil, err
	}
	type candidate struct {
		id domain.SessionID
		at time.Time
	}
	byID := make(map[domain.SessionID]time.Time)
	for _, status := range []domain.SessionStatus{domain.SessionActive, domain.SessionArchived} {
		for _, bucket := range ydbpartition.BucketsV1() {
			rows, err := store.db.QueryContext(ctx,
				`SELECT session_id, updated_at FROM session_activity
				 WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4
				 ORDER BY updated_at DESC, session_id DESC LIMIT $5`,
				tenantID, userID, status, bucket, limit,
			)
			if err != nil {
				return nil, err
			}
			for rows.Next() {
				var id domain.SessionID
				var at time.Time
				if err := rows.Scan(&id, &at); err != nil {
					rows.Close()
					return nil, err
				}
				if previous, ok := byID[id]; !ok || at.After(previous) {
					byID[id] = at
				}
			}
			if err := rows.Close(); err != nil {
				return nil, err
			}
		}
	}
	candidates := make([]candidate, 0, len(byID))
	for id, at := range byID {
		candidates = append(candidates, candidate{id: id, at: at})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].at.Equal(candidates[j].at) {
			return candidates[i].id > candidates[j].id
		}
		return candidates[i].at.After(candidates[j].at)
	})
	if uint64(len(candidates)) > limit {
		candidates = candidates[:limit]
	}
	result := make([]domain.Session, 0, len(candidates))
	for _, item := range candidates {
		session, found, err := store.GetSession(ctx, tenantID, item.id)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("session activity references missing session %q", item.id)
		}
		result = append(result, session)
	}
	return result, nil
}

func (store *Store) ListSessionHistory(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	afterSequence uint64,
	limit uint64,
) ([]domain.SessionEvent, error) {
	if err := validateSessionRead(tenantID, sessionID, limit); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
		 ORDER BY sequence ASC LIMIT $4`,
		tenantID, sessionID, afterSequence, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return decodeRows[domain.SessionEvent](rows)
}

func (store *Store) ListRunsBySession(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
	limit uint64,
) ([]domain.Run, error) {
	if err := validateSessionRead(tenantID, sessionID, limit); err != nil {
		return nil, err
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT run_id FROM runs_by_session
		 WHERE tenant_id = $1 AND session_id = $2
		 ORDER BY created_at DESC, run_id DESC LIMIT $3`,
		tenantID, sessionID, limit,
	)
	if err != nil {
		return nil, err
	}
	var runIDs []domain.RunID
	for rows.Next() {
		var runID domain.RunID
		if err := rows.Scan(&runID); err != nil {
			rows.Close()
			return nil, err
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var result []domain.Run
	for _, runID := range runIDs {
		run, found, err := readJSON[domain.Run](ctx, store.db,
			`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`,
			tenantID, runID,
		)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("runs_by_session references missing run %q", runID)
		}
		result = append(result, run)
	}
	return result, nil
}

func validateSessionOwner(session domain.Session, owner domain.SessionParticipant) error {
	if err := session.Validate(); err != nil {
		return err
	}
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(session.TenantID, owner.TenantID); err != nil {
		return err
	}
	if owner.SessionID != session.ID || owner.UserID != session.CreatedBy ||
		owner.Role != domain.SessionParticipantOwner || owner.Status != domain.SessionParticipantActive {
		return domain.ValidationError{Field: "session.owner", Reason: "must be the active owner and creator of the session"}
	}
	return nil
}

func createSessionTx(ctx context.Context, tx *stateTx, session domain.Session, owner domain.SessionParticipant) error {
	existing, found, err := readSessionTx(ctx, tx, session.ID)
	if err != nil {
		return err
	}
	if found {
		if !reflect.DeepEqual(existing, session) {
			return ErrSessionConflict
		}
		matches, err := sessionOwnerMatchesTx(ctx, tx, owner)
		if err != nil {
			return err
		}
		if !matches {
			return ErrSessionConflict
		}
		return nil
	}
	record, err := marshal(session)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`INSERT INTO sessions
		 (tenant_id, session_id, created_by, status, last_event_sequence,
		  created_at, updated_at, archived_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		session.TenantID, session.ID, session.CreatedBy, session.Status,
		session.LastEventSequence, session.CreatedAt, session.UpdatedAt,
		nullableTime(session.ArchivedAt), record,
	); err != nil {
		return err
	}
	participantRecord, err := marshal(owner)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`INSERT INTO session_participants
		 (tenant_id, session_id, user_id, role, status, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, CAST($8 AS JsonDocument))`,
		owner.TenantID, owner.SessionID, owner.UserID, owner.Role, owner.Status,
		owner.CreatedAt, owner.UpdatedAt, participantRecord,
	); err != nil {
		return err
	}
	return insertActivityTx(ctx, tx, session, owner.UserID)
}

func sessionAndOwnerMatchTx(
	ctx context.Context,
	tx *stateTx,
	session domain.Session,
	owner domain.SessionParticipant,
) (bool, error) {
	existingSession, found, err := readSessionTx(ctx, tx, session.ID)
	if err != nil || !found || !reflect.DeepEqual(existingSession, session) {
		return false, err
	}
	return sessionOwnerMatchesTx(ctx, tx, owner)
}

func sessionOwnerMatchesTx(
	ctx context.Context,
	tx *stateTx,
	owner domain.SessionParticipant,
) (bool, error) {
	existingOwner, found, err := readJSON[domain.SessionParticipant](ctx, tx.sqlTx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
		owner.TenantID, owner.SessionID, owner.UserID,
	)
	if err != nil {
		return false, err
	}
	return found && reflect.DeepEqual(existingOwner, owner), nil
}

func updateSessionTx(ctx context.Context, tx *stateTx, previous, session domain.Session) error {
	participants, err := activeSessionParticipants(ctx, tx, session.ID)
	if err != nil {
		return err
	}
	for _, participant := range participants {
		if err := deleteActivityTx(ctx, tx, previous, participant.UserID); err != nil {
			return err
		}
	}
	record, err := marshal(session)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPDATE sessions SET status = $1, last_event_sequence = $2,
		 updated_at = $3, archived_at = $4, record = CAST($5 AS JsonDocument)
		 WHERE tenant_id = $6 AND session_id = $7`,
		session.Status, session.LastEventSequence, session.UpdatedAt,
		nullableTime(session.ArchivedAt), record, session.TenantID, session.ID,
	); err != nil {
		return err
	}
	for _, participant := range participants {
		if err := insertActivityTx(ctx, tx, session, participant.UserID); err != nil {
			return err
		}
	}
	return nil
}

func readSessionTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID) (domain.Session, bool, error) {
	return readJSON[domain.Session](ctx, tx.sqlTx,
		`SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
		tx.tenantID, sessionID,
	)
}

func readBindingTx(ctx context.Context, tx *stateTx, bindingID domain.FrontendBindingID) (domain.FrontendBinding, bool, error) {
	return readJSON[domain.FrontendBinding](ctx, tx.sqlTx,
		`SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2`,
		tx.tenantID, bindingID,
	)
}

func writeBindingTx(ctx context.Context, tx *stateTx, binding domain.FrontendBinding) error {
	if err := tx.validateTenant(binding.TenantID); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return err
	}
	record, err := marshal(binding)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO frontend_bindings
		 (tenant_id, binding_id, frontend, external_conversation_id, session_id,
		  revision, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		binding.TenantID, binding.ID, binding.Frontend, binding.ExternalConversationID,
		binding.SessionID, binding.Revision, binding.CreatedAt, binding.UpdatedAt, record,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO frontend_binding_keys
		 (tenant_id, frontend, external_conversation_id, binding_id)
		 VALUES ($1, $2, $3, $4)`,
		binding.TenantID, binding.Frontend, binding.ExternalConversationID, binding.ID,
	)
	return err
}

func activeSessionParticipants(ctx context.Context, tx *stateTx, sessionID domain.SessionID) ([]domain.SessionParticipant, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND status = $3`,
		tx.tenantID, sessionID, domain.SessionParticipantActive,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return decodeRows[domain.SessionParticipant](rows)
}

func insertActivityTx(ctx context.Context, tx *stateTx, session domain.Session, userID domain.UserID) error {
	bucket, err := ydbpartition.BucketV1(string(session.ID))
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO session_activity
		 (tenant_id, user_id, status, activity_bucket, updated_at, session_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		session.TenantID, userID, session.Status, bucket, session.UpdatedAt, session.ID,
	)
	return err
}

func deleteActivityTx(ctx context.Context, tx *stateTx, session domain.Session, userID domain.UserID) error {
	bucket, err := ydbpartition.BucketV1(string(session.ID))
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`DELETE FROM session_activity
		 WHERE tenant_id = $1 AND user_id = $2 AND status = $3
		 AND activity_bucket = $4 AND updated_at = $5 AND session_id = $6`,
		session.TenantID, userID, session.Status, bucket, session.UpdatedAt, session.ID,
	)
	return err
}

func nullableTime(value *time.Time) any {
	// Preserve the pointer's concrete type even when it is nil. YDB's query
	// binder then infers Optional<Timestamp>; a plain nil is inferred as Void.
	return value
}

func validateSessionRead(tenantID domain.TenantID, sessionID domain.SessionID, limit uint64) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := sessionID.Validate(); err != nil {
		return err
	}
	return validatePageLimit(limit)
}

func validatePageLimit(limit uint64) error {
	if limit == 0 || limit > maxSessionPageSize {
		return domain.ValidationError{Field: "limit", Reason: fmt.Sprintf("must be between 1 and %d", maxSessionPageSize)}
	}
	return nil
}

type jsonRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func decodeRows[T any](rows jsonRows) ([]T, error) {
	var result []T
	for rows.Next() {
		var record string
		if err := rows.Scan(&record); err != nil {
			return nil, err
		}
		var value T
		if err := unmarshalRecord(record, &value); err != nil {
			return nil, err
		}
		result = append(result, value)
	}
	return result, rows.Err()
}

func unmarshalRecord(record string, target any) error {
	if err := json.Unmarshal([]byte(record), target); err != nil {
		return fmt.Errorf("decode stored JSON: %w", err)
	}
	return nil
}
