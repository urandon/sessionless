package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func (store *Store) CreateSessionForUser(
	ctx context.Context,
	request ports.SessionCreateRequest,
) (result domain.Session, created bool, err error) {
	if err := validateSessionOwner(request.Session, request.Owner); err != nil {
		return result, false, err
	}
	if err := request.IdempotencyKey.Validate(); err != nil {
		return result, false, err
	}
	err = store.Transact(ctx, request.Session.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.Owner.UserID); err != nil {
			return err
		}
		var existingID domain.SessionID
		err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT session_id FROM session_api_idempotency
			 WHERE tenant_id = $1 AND user_id = $2 AND idempotency_key = $3`,
			request.Session.TenantID, request.Owner.UserID, request.IdempotencyKey,
		).Scan(&existingID)
		if err == nil {
			if existingID != request.Session.ID {
				return ErrSessionConflict
			}
			existing, found, err := readSessionTx(ctx, tx, existingID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("session API idempotency references missing session %q", existingID)
			}
			if err := authorizeSessionForUserTx(ctx, tx, existingID, request.Owner.UserID, false); err != nil {
				return err
			}
			result, created = existing, false
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := createSessionTx(ctx, tx, request.Session, request.Owner); err != nil {
			return err
		}
		display := domain.SessionDisplay{
			TenantID: request.Session.TenantID, SessionID: request.Session.ID,
			UpdatedAt: request.Session.UpdatedAt,
		}
		if err := writeSessionDisplayTx(ctx, tx, display); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO session_api_idempotency
			 (tenant_id, user_id, idempotency_key, session_id, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			request.Session.TenantID, request.Owner.UserID, request.IdempotencyKey,
			request.Session.ID, request.Session.CreatedAt,
		); err != nil {
			return err
		}
		result, created = request.Session, true
		return nil
	})
	return result, created, err
}

func (store *Store) GetSessionForUser(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	write bool,
) (record ports.SessionRecord, found bool, err error) {
	if err := validateSessionAPIIdentity(tenantID, userID, sessionID); err != nil {
		return record, false, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		session, exists, err := readSessionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !exists {
			return nil
		}
		if err := authorizeSessionForUserTx(ctx, tx, sessionID, userID, write); err != nil {
			return err
		}
		record, err = readSessionRecordTx(ctx, tx, session)
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return record, found, err
}

func (store *Store) ListSessionsForUser(
	ctx context.Context,
	request ports.SessionListRequest,
) ([]ports.SessionRecord, error) {
	if err := request.TenantID.Validate(); err != nil {
		return nil, err
	}
	if err := request.UserID.Validate(); err != nil {
		return nil, err
	}
	if !request.Status.Valid() {
		return nil, domain.ValidationError{Field: "sessions.status", Reason: "is unknown"}
	}
	if err := validatePageLimit(request.Limit); err != nil {
		return nil, err
	}
	if request.Before != nil {
		if request.Before.UpdatedAt.IsZero() {
			return nil, domain.ValidationError{Field: "sessions.cursor.updated_at", Reason: "must not be zero"}
		}
		if err := request.Before.SessionID.Validate(); err != nil {
			return nil, err
		}
	}
	if err := store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		return authorizeTenantReadTx(ctx, state.(*stateTx), request.UserID)
	}); err != nil {
		return nil, err
	}
	type candidate struct {
		id domain.SessionID
		at time.Time
	}
	byID := make(map[domain.SessionID]time.Time)
	for _, bucket := range ydbpartition.BucketsV1() {
		query := `SELECT session_id, updated_at FROM session_activity
			 WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4
			 ORDER BY updated_at DESC, session_id DESC LIMIT $5`
		args := []any{request.TenantID, request.UserID, request.Status, bucket, request.Limit}
		if request.Before != nil {
			beforeAt := request.Before.UpdatedAt.UTC().Truncate(time.Microsecond)
			query = `SELECT session_id, updated_at FROM session_activity
				 WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4
				 AND (updated_at < $5 OR (updated_at = $5 AND session_id < $6))
				 ORDER BY updated_at DESC, session_id DESC LIMIT $7`
			args = []any{request.TenantID, request.UserID, request.Status, bucket,
				beforeAt, request.Before.SessionID, request.Limit}
		}
		rows, err := store.db.QueryContext(ctx, query, args...)
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
	if uint64(len(candidates)) > request.Limit {
		candidates = candidates[:request.Limit]
	}
	result := make([]ports.SessionRecord, 0, len(candidates))
	for _, item := range candidates {
		record, found, err := store.GetSessionForUser(ctx, request.TenantID, request.UserID, item.id, false)
		if errors.Is(err, domain.ErrMembershipDenied) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// A concurrent append/archive/participant removal may move or remove an
		// activity row between the bucket read and its authorized point read.
		// Skip that stale candidate; a later request sees its new key. The work
		// remains bounded by fan-out and page size.
		if !found || record.Session.Status != request.Status ||
			!record.Session.UpdatedAt.UTC().Truncate(time.Microsecond).Equal(item.at) {
			continue
		}
		result = append(result, record)
	}
	return result, nil
}

func (store *Store) ListSessionHistoryForUser(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	afterSequence uint64,
	limit uint64,
) (events []domain.SessionEvent, err error) {
	if err := validateSessionAPIRead(tenantID, userID, sessionID, limit); err != nil {
		return nil, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeSessionForUserTx(ctx, tx, sessionID, userID, false); err != nil {
			return err
		}
		rows, err := tx.sqlTx.QueryContext(ctx,
			`SELECT record FROM session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3
			 ORDER BY sequence ASC LIMIT $4`,
			tenantID, sessionID, afterSequence, limit,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		events, err = decodeRows[domain.SessionEvent](rows)
		return err
	})
	return events, err
}

func (store *Store) ListRunsForUser(
	ctx context.Context,
	request ports.RunListRequest,
) (runs []ports.RunRecord, err error) {
	if err := validateSessionAPIRead(request.TenantID, request.UserID, request.SessionID, request.Limit); err != nil {
		return nil, err
	}
	if request.Before != nil {
		if request.Before.CreatedAt.IsZero() {
			return nil, domain.ValidationError{Field: "runs.cursor.created_at", Reason: "must not be zero"}
		}
		if err := request.Before.RunID.Validate(); err != nil {
			return nil, err
		}
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeSessionForUserTx(ctx, tx, request.SessionID, request.UserID, false); err != nil {
			return err
		}
		query := `SELECT run_id FROM runs_by_session
			 WHERE tenant_id = $1 AND session_id = $2
			 ORDER BY created_at DESC, run_id DESC LIMIT $3`
		args := []any{request.TenantID, request.SessionID, request.Limit}
		if request.Before != nil {
			query = `SELECT run_id FROM runs_by_session
				 WHERE tenant_id = $1 AND session_id = $2
				 AND (created_at < $3 OR (created_at = $3 AND run_id < $4))
				 ORDER BY created_at DESC, run_id DESC LIMIT $5`
			args = []any{request.TenantID, request.SessionID,
				request.Before.CreatedAt, request.Before.RunID, request.Limit}
		}
		rows, err := tx.sqlTx.QueryContext(ctx, query, args...)
		if err != nil {
			return err
		}
		var ids []domain.RunID
		for rows.Next() {
			var id domain.RunID
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, id := range ids {
			run, found, err := readJSON[domain.Run](ctx, tx.sqlTx,
				`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, request.TenantID, id)
			if err != nil {
				return err
			}
			if !found || run.SessionID != request.SessionID {
				return fmt.Errorf("runs_by_session references inconsistent run %q", id)
			}
			provider, err := readRunProviderTx(ctx, tx, run)
			if err != nil {
				return err
			}
			runs = append(runs, ports.RunRecord{Run: run, Provider: provider})
		}
		return nil
	})
	return runs, err
}

func (store *Store) BindOrSwitchFrontendForUser(
	ctx context.Context,
	request ports.FrontendBindingRequest,
) (result domain.FrontendBinding, err error) {
	if err := validateFrontendBindingRequest(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		if err := authorizeSessionForUserTx(ctx, tx, request.SessionID, request.UserID, true); err != nil {
			return err
		}
		target, found, err := readSessionTx(ctx, tx, request.SessionID)
		if err != nil {
			return err
		}
		if !found || target.Status != domain.SessionActive {
			return domain.ValidationError{Field: "frontend_binding.session_id", Reason: "must reference an active session"}
		}
		var indexedID domain.FrontendBindingID
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT binding_id FROM frontend_binding_keys
			 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
			request.TenantID, request.Frontend, request.ExternalConversationID,
		).Scan(&indexedID)
		if errors.Is(lookupErr, sql.ErrNoRows) {
			if request.ExpectedRevision != 0 {
				return domain.StaleBindingError{Expected: request.ExpectedRevision, Actual: 0}
			}
			binding := domain.FrontendBinding{
				ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
				ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
				Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
			}
			if err := writeBindingTx(ctx, tx, binding); err != nil {
				return err
			}
			result = binding
			return nil
		}
		if lookupErr != nil {
			return lookupErr
		}
		// Existing bindings may have been created by an older frontend adapter
		// with a different deterministic-ID key/algorithm. The unique
		// (tenant, frontend, external conversation) index is authoritative;
		// never fork it merely because a caller's candidate ID differs.
		binding, found, err := readBindingTx(ctx, tx, indexedID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("frontend binding index references missing binding %q", indexedID)
		}
		if err := authorizeSessionForUserTx(ctx, tx, binding.SessionID, request.UserID, true); err != nil {
			return err
		}
		if binding.Revision == request.ExpectedRevision+1 && binding.SessionID == request.SessionID {
			result = binding
			return nil
		}
		if err := binding.Switch(request.ExpectedRevision, request.SessionID, request.At); err != nil {
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

func (store *Store) SetSessionArchivedForUser(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	archived bool,
	idempotencyKey domain.IdempotencyKey,
	at time.Time,
) (result domain.Session, err error) {
	if err := validateSessionAPIIdentity(tenantID, userID, sessionID); err != nil {
		return result, err
	}
	if at.IsZero() {
		return result, domain.ValidationError{Field: "session.updated_at", Reason: "must not be zero"}
	}
	if err := idempotencyKey.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeSessionForUserTx(ctx, tx, sessionID, userID, true); err != nil {
			return err
		}
		session, found, err := readSessionTx(ctx, tx, sessionID)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrMembershipDenied
		}
		var existingSessionID domain.SessionID
		var existingArchived bool
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT session_id, archived FROM session_api_mutations
			 WHERE tenant_id = $1 AND user_id = $2 AND idempotency_key = $3 AND kind = $4`,
			tenantID, userID, idempotencyKey, "archive",
		).Scan(&existingSessionID, &existingArchived)
		if lookupErr == nil {
			if existingSessionID != sessionID || existingArchived != archived {
				return domain.ErrSessionMutationConflict
			}
			if (archived && session.Status != domain.SessionArchived) || (!archived && session.Status != domain.SessionActive) {
				return domain.ErrSessionMutationConflict
			}
			result = session
			return nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return lookupErr
		}
		previous := session
		if archived {
			err = session.Archive(at)
		} else {
			err = session.Unarchive(at)
		}
		if err != nil {
			return err
		}
		if err := updateSessionTx(ctx, tx, previous, session); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO session_api_mutations
			 (tenant_id, user_id, idempotency_key, kind, session_id, archived, created_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tenantID, userID, idempotencyKey, "archive", sessionID, archived, at,
		); err != nil {
			return err
		}
		result = session
		return nil
	})
	return result, err
}

func authorizeSessionForUserTx(
	ctx context.Context,
	tx *stateTx,
	sessionID domain.SessionID,
	userID domain.UserID,
	write bool,
) error {
	if write {
		if err := ensureSessionWritableTx(ctx, tx, sessionID); err != nil {
			return err
		}
	}
	participant, found, err := readJSON[domain.SessionParticipant](ctx, tx.sqlTx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
		tx.tenantID, sessionID, userID,
	)
	if err != nil {
		return err
	}
	if !found {
		return domain.ErrMembershipDenied
	}
	return participant.Authorize(tx.tenantID, sessionID, userID, write)
}

func readSessionRecordTx(ctx context.Context, tx *stateTx, session domain.Session) (ports.SessionRecord, error) {
	record := ports.SessionRecord{Session: session, Display: domain.SessionDisplay{
		TenantID: session.TenantID, SessionID: session.ID, UpdatedAt: session.UpdatedAt,
	}}
	display, found, err := readJSON[domain.SessionDisplay](ctx, tx.sqlTx,
		`SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2`,
		session.TenantID, session.ID,
	)
	if err != nil {
		return record, err
	}
	if found {
		if err := display.Validate(); err != nil {
			return record, err
		}
		record.Display = display
	}
	var runID domain.RunID
	err = tx.sqlTx.QueryRowContext(ctx,
		`SELECT run_id FROM runs_by_session
		 WHERE tenant_id = $1 AND session_id = $2
		 ORDER BY created_at DESC, run_id DESC LIMIT 1`,
		session.TenantID, session.ID,
	).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return record, nil
	}
	if err != nil {
		return record, err
	}
	run, found, err := readJSON[domain.Run](ctx, tx.sqlTx,
		`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, session.TenantID, runID)
	if err != nil {
		return record, err
	}
	if !found || run.SessionID != session.ID {
		return record, fmt.Errorf("runs_by_session references inconsistent run %q", runID)
	}
	record.Run = &run
	record.Provider, err = readRunProviderTx(ctx, tx, run)
	if err != nil {
		return record, err
	}
	return record, nil
}

func readRunProviderTx(ctx context.Context, tx *stateTx, run domain.Run) (string, error) {
	var provider string
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT provider FROM subscription_connections
		 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
		run.TenantID, run.SubscriptionConnectionID,
	).Scan(&provider)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return provider, err
}

func (store *Store) GetSessionAdminMetadata(
	ctx context.Context,
	tenantID domain.TenantID,
	sessionID domain.SessionID,
) (metadata ports.SessionAdminMetadata, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return metadata, false, err
	}
	if err := sessionID.Validate(); err != nil {
		return metadata, false, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		session, exists, err := readSessionTx(ctx, tx, sessionID)
		if err != nil || !exists {
			return err
		}
		record, err := readSessionRecordTx(ctx, tx, session)
		if err != nil {
			return err
		}
		metadata = ports.SessionAdminMetadata{
			Session: record.Session, Display: record.Display, Run: record.Run, Provider: record.Provider,
		}
		found = true
		return nil
	})
	return metadata, found, err
}

func writeSessionDisplayTx(ctx context.Context, tx *stateTx, display domain.SessionDisplay) error {
	if err := display.Validate(); err != nil {
		return err
	}
	record, err := marshal(display)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO session_displays (tenant_id, session_id, updated_at, record)
		 VALUES ($1, $2, $3, CAST($4 AS JsonDocument))`,
		display.TenantID, display.SessionID, display.UpdatedAt, record,
	)
	return err
}

func updateSessionDisplayMessageTx(
	ctx context.Context,
	tx *stateTx,
	session domain.Session,
	text string,
	origin *domain.FrontendEventOrigin,
	run *domain.Run,
) error {
	display, found, err := readJSON[domain.SessionDisplay](ctx, tx.sqlTx,
		`SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2`,
		session.TenantID, session.ID,
	)
	if err != nil {
		return err
	}
	if !found {
		display = domain.SessionDisplay{TenantID: session.TenantID, SessionID: session.ID}
	}
	preview := domain.BoundedSessionText(text, domain.MaxSessionPreviewRunes)
	if display.Title == "" && preview != "" {
		display.Title = domain.BoundedSessionText(preview, domain.MaxSessionTitleRunes)
	}
	if preview != "" {
		display.Preview = preview
	}
	if origin != nil {
		frontend := origin.Frontend
		display.Origin = &frontend
	}
	if run != nil {
		runID, status := run.ID, run.Status
		display.CurrentRunID, display.CurrentStatus = &runID, &status
	}
	display.UpdatedAt = session.UpdatedAt
	return writeSessionDisplayTx(ctx, tx, display)
}

func validateSessionAPIIdentity(tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if err := userID.Validate(); err != nil {
		return err
	}
	return sessionID.Validate()
}

func validateSessionAPIRead(tenantID domain.TenantID, userID domain.UserID, sessionID domain.SessionID, limit uint64) error {
	if err := validateSessionAPIIdentity(tenantID, userID, sessionID); err != nil {
		return err
	}
	return validatePageLimit(limit)
}

func validateFrontendBindingRequest(request ports.FrontendBindingRequest) error {
	if err := validateSessionAPIIdentity(request.TenantID, request.UserID, request.SessionID); err != nil {
		return err
	}
	if err := request.Frontend.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(request.ExternalConversationID) == "" || len(request.ExternalConversationID) > 512 {
		return domain.ValidationError{Field: "external_conversation_id", Reason: "must contain between 1 and 512 bytes"}
	}
	if err := request.BindingID.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "frontend_binding.updated_at", Reason: "must not be zero"}
	}
	return nil
}

var _ ports.SessionAPIStore = (*Store)(nil)
var _ ports.SessionAdminMetadataStore = (*Store)(nil)
