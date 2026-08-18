package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) EnsureFrontendSession(
	ctx context.Context,
	request ports.FrontendSessionRequest,
) (result ports.FrontendSessionState, err error) {
	if err := request.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		var existingBindingID string
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT binding_id FROM frontend_binding_keys
			 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
			request.TenantID, request.Frontend, request.ExternalConversationID,
		).Scan(&existingBindingID)
		switch {
		case lookupErr == nil:
			if domain.FrontendBindingID(existingBindingID) != request.BindingID {
				return ErrBindingConflict
			}
			binding, found, err := readBindingTx(ctx, tx, request.BindingID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("frontend binding index references missing binding %q", request.BindingID)
			}
			if err := authorizeSessionWriteTx(ctx, tx, binding.SessionID, request.UserID); err != nil {
				return err
			}
			session, found, err := readSessionTx(ctx, tx, binding.SessionID)
			if err != nil {
				return err
			}
			if !found || session.Status != domain.SessionActive {
				return domain.ValidationError{Field: "frontend_binding.session_id", Reason: "must reference an active session"}
			}
			result = ports.FrontendSessionState{Session: session, Binding: binding}
			return nil
		case !errors.Is(lookupErr, sql.ErrNoRows):
			return lookupErr
		}

		session := domain.Session{
			ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
			Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
		}
		owner := domain.SessionParticipant{
			TenantID: request.TenantID, SessionID: request.SessionID, UserID: request.UserID,
			Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
			CreatedAt: request.At, UpdatedAt: request.At,
		}
		if err := createSessionTx(ctx, tx, session, owner); err != nil {
			return err
		}
		binding := domain.FrontendBinding{
			ID: request.BindingID, TenantID: request.TenantID, Frontend: request.Frontend,
			ExternalConversationID: request.ExternalConversationID, SessionID: request.SessionID,
			Revision: 1, CreatedAt: request.At, UpdatedAt: request.At,
		}
		if err := writeBindingTx(ctx, tx, binding); err != nil {
			return err
		}
		result = ports.FrontendSessionState{Session: session, Binding: binding}
		return nil
	})
	return result, err
}

func (store *Store) CreateAndSwitchFrontendSession(
	ctx context.Context,
	request ports.CanonicalSessionSwitchRequest,
) (result ports.FrontendSessionState, err error) {
	if err := request.Validate(); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		binding, found, err := readBindingTx(ctx, tx, request.BindingID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("frontend binding %q not found", request.BindingID)
		}
		session := domain.Session{
			ID: request.SessionID, TenantID: request.TenantID, CreatedBy: request.UserID,
			Status: domain.SessionActive, CreatedAt: request.At, UpdatedAt: request.At,
		}
		owner := domain.SessionParticipant{
			TenantID: request.TenantID, SessionID: request.SessionID, UserID: request.UserID,
			Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
			CreatedAt: request.At, UpdatedAt: request.At,
		}
		if binding.Revision == request.ExpectedRevision+1 && binding.SessionID == request.SessionID && binding.UpdatedAt.Equal(request.At) {
			matches, err := sessionAndOwnerMatchTx(ctx, tx, session, owner)
			if err != nil {
				return err
			}
			if !matches {
				return ErrSessionConflict
			}
			result = ports.FrontendSessionState{Session: session, Binding: binding}
			return nil
		}
		if binding.Revision != request.ExpectedRevision {
			return domain.StaleBindingError{Expected: request.ExpectedRevision, Actual: binding.Revision}
		}
		if err := authorizeSessionWriteTx(ctx, tx, binding.SessionID, request.UserID); err != nil {
			return err
		}
		if err := createSessionTx(ctx, tx, session, owner); err != nil {
			return err
		}
		if err := binding.Switch(request.ExpectedRevision, request.SessionID, request.At); err != nil {
			return err
		}
		if err := writeBindingTx(ctx, tx, binding); err != nil {
			return err
		}
		result = ports.FrontendSessionState{Session: session, Binding: binding}
		return nil
	})
	return result, err
}

func (store *Store) LookupCanonicalUserEvent(
	ctx context.Context,
	request ports.CanonicalUserEventLookup,
) (result ports.CanonicalUserEventLookupResult, err error) {
	if err := validateCanonicalLookup(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		var sessionID string
		var sequence uint64
		var eventID, runID string
		err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT session_id, sequence, event_id, run_id
			 FROM frontend_ingress_idempotency
			 WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3
			 AND expire_at > CurrentUtcTimestamp()`,
			request.TenantID, request.BindingID, request.IdempotencyKey,
		).Scan(&sessionID, &sequence, &eventID, &runID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		binding, found, err := readBindingTx(ctx, tx, request.BindingID)
		if err != nil {
			return err
		}
		if !found || binding.Frontend != request.Frontend ||
			binding.ExternalConversationID != request.ExternalConversationID {
			return domain.ErrEventIdempotencyConflict
		}
		originalSessionID := domain.SessionID(sessionID)
		if err := authorizeSessionWriteTx(ctx, tx, originalSessionID, request.UserID); err != nil {
			return err
		}
		event, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
			`SELECT record FROM session_events
			 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
			request.TenantID, originalSessionID, sequence,
		)
		if err != nil {
			return err
		}
		if !found || event.ID != domain.SessionEventID(eventID) || event.ID != request.EventID ||
			event.AuthorUserID == nil || *event.AuthorUserID != request.UserID ||
			event.RunID == nil || *event.RunID != request.RunID ||
			event.IdempotencyKey != request.IdempotencyKey {
			return domain.ErrEventIdempotencyConflict
		}
		run, found, err := tx.GetRun(ctx, domain.RunID(runID))
		if err != nil {
			return err
		}
		if !found || run.ID != request.RunID || run.TriggerEventID != event.ID {
			return fmt.Errorf("frontend ingress idempotency index is inconsistent")
		}
		result = ports.CanonicalUserEventLookupResult{
			Found: true,
			Result: ports.CanonicalUserEventResult{
				SessionID: event.SessionID, EventID: event.ID, Sequence: event.Sequence,
				RunID: run.ID, Created: false,
			},
		}
		return nil
	})
	return result, err
}

func (store *Store) CommitCanonicalUserEvent(
	ctx context.Context,
	request ports.CanonicalUserEventCommit,
) (result ports.CanonicalUserEventResult, err error) {
	if err := validateCanonicalCommit(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		var existingSessionID string
		var existingSequence uint64
		var existingEventID, existingRunID string
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT session_id, sequence, event_id, run_id
			 FROM frontend_ingress_idempotency
			 WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3
			 AND expire_at > CurrentUtcTimestamp()`,
			request.TenantID, request.BindingID, request.IdempotencyKey,
		).Scan(&existingSessionID, &existingSequence, &existingEventID, &existingRunID)
		switch {
		case lookupErr == nil:
			if err := authorizeSessionWriteTx(
				ctx, tx, domain.SessionID(existingSessionID), request.UserID,
			); err != nil {
				return err
			}
			event, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
				`SELECT record FROM session_events
				 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
				request.TenantID, existingSessionID, existingSequence,
			)
			if err != nil {
				return err
			}
			if !found || event.ID != domain.SessionEventID(existingEventID) || event.ID != request.EventID ||
				event.AuthorUserID == nil || *event.AuthorUserID != request.UserID ||
				event.RunID == nil || *event.RunID != request.RunID || event.IdempotencyKey != request.IdempotencyKey {
				return domain.ErrEventIdempotencyConflict
			}
			// The original payload, origin, and commit timestamp remain canonical.
			// This path also closes the race where two deliveries both miss the
			// application-level lookup before the first transaction commits.
			run, found, err := tx.GetRun(ctx, domain.RunID(existingRunID))
			if err != nil {
				return err
			}
			if !found || run.ID != request.RunID || run.TriggerEventID != event.ID {
				return fmt.Errorf("frontend ingress idempotency index is inconsistent")
			}
			result = ports.CanonicalUserEventResult{
				SessionID: event.SessionID, EventID: event.ID, Sequence: event.Sequence,
				RunID: run.ID, Created: false,
			}
			return nil
		case !errors.Is(lookupErr, sql.ErrNoRows):
			return lookupErr
		}

		binding, found, err := readBindingTx(ctx, tx, request.BindingID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("frontend binding %q not found", request.BindingID)
		}
		if binding.Revision != request.ExpectedBindingRevision {
			return domain.StaleBindingError{Expected: request.ExpectedBindingRevision, Actual: binding.Revision}
		}
		if request.Origin.Frontend != binding.Frontend || request.Origin.ExternalConversationID != binding.ExternalConversationID {
			return domain.ValidationError{Field: "frontend_event_origin", Reason: "must match the resolved binding"}
		}
		if err := authorizeSessionWriteTx(ctx, tx, binding.SessionID, request.UserID); err != nil {
			return err
		}
		session, found, err := readSessionTx(ctx, tx, binding.SessionID)
		if err != nil {
			return err
		}
		if !found || session.Status != domain.SessionActive {
			return domain.ValidationError{Field: "session", Reason: "must be active for canonical ingress"}
		}
		if err := domain.ValidateSessionEventBlob(request.TenantID, session.ID, request.EventID, request.Payload); err != nil {
			return err
		}
		for _, artifact := range request.Artifacts {
			if err := domain.ValidateSessionEventBlob(request.TenantID, session.ID, request.EventID, artifact.Blob); err != nil {
				return err
			}
		}

		sequence := session.LastEventSequence + 1
		runID := request.RunID
		event := domain.SessionEvent{
			ID: request.EventID, TenantID: request.TenantID, SessionID: session.ID,
			Sequence: sequence, Kind: domain.SessionEventUserMessage, AuthorUserID: &request.UserID,
			RunID: &runID, IdempotencyKey: request.IdempotencyKey,
			Payload: request.Payload, CreatedAt: request.CommittedAt,
		}
		previous := session
		if _, err := domain.AppendSessionEvent(&session, event, nil); err != nil {
			return err
		}
		if err := insertSessionEventTx(ctx, tx, event); err != nil {
			return err
		}
		if err := updateSessionTx(ctx, tx, previous, session); err != nil {
			return err
		}

		run := domain.Run{
			ID: request.RunID, TenantID: request.TenantID, SessionID: session.ID,
			TriggerEventID: request.EventID, SubscriptionConnectionID: request.SubscriptionConnectionID,
			Status: domain.RunCreated, IdempotencyKey: request.IdempotencyKey,
			CreatedAt: request.CommittedAt, UpdatedAt: request.CommittedAt,
		}
		attempt := domain.Attempt{
			ID: request.AttemptID, TenantID: request.TenantID, RunID: request.RunID,
			Number: 1, Status: domain.AttemptCreated,
			CreatedAt: request.CommittedAt, UpdatedAt: request.CommittedAt,
		}
		manifest := domain.ArtifactManifest{
			ID: request.ManifestID, TenantID: request.TenantID, RunID: request.RunID,
			Artifacts: append([]domain.Artifact(nil), request.Artifacts...), CreatedAt: request.CommittedAt,
		}
		origin := request.Origin
		dispatch := domain.DispatchOutbox{
			ID: request.DispatchID, TenantID: request.TenantID, RunID: request.RunID,
			AttemptID: request.AttemptID, InputManifestID: request.ManifestID,
			AllowedMCPServers: append([]string(nil), request.AllowedMCPServers...),
			ContextWindow:     &domain.SessionContextWindow{ThroughSequence: sequence},
			Origin:            &origin, Status: domain.DispatchPending, IdempotencyKey: request.IdempotencyKey,
			CreatedAt: request.CommittedAt, UpdatedAt: request.CommittedAt,
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, attempt); err != nil {
			return err
		}
		if err := state.PutArtifactManifest(ctx, manifest); err != nil {
			return err
		}
		if err := state.PutDispatchOutbox(ctx, dispatch); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO frontend_ingress_idempotency
			 (tenant_id, binding_id, idempotency_key, session_id, sequence,
			  event_id, run_id, origin_digest, created_at, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			request.TenantID, request.BindingID, request.IdempotencyKey, session.ID,
			sequence, request.EventID, request.RunID, frontendOriginDigest(request.Origin),
			request.CommittedAt, request.ExpireAt,
		); err != nil {
			return err
		}
		result = ports.CanonicalUserEventResult{
			SessionID: session.ID, EventID: request.EventID, Sequence: sequence,
			RunID: request.RunID, Created: true,
		}
		return nil
	})
	return result, err
}

func authorizeTenantWriteTx(ctx context.Context, tx *stateTx, userID domain.UserID) error {
	membership, found, err := readMembershipTx(ctx, tx.sqlTx, userID, tx.tenantID)
	if err != nil {
		return err
	}
	if !found {
		return domain.ErrMembershipDenied
	}
	return membership.Authorize(userID, tx.tenantID, domain.TenantPermissionWrite)
}

func authorizeSessionWriteTx(ctx context.Context, tx *stateTx, sessionID domain.SessionID, userID domain.UserID) error {
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
	return participant.Authorize(tx.tenantID, sessionID, userID, true)
}

func insertSessionEventTx(ctx context.Context, tx *stateTx, event domain.SessionEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	record, err := marshal(event)
	if err != nil {
		return err
	}
	authorID, runID := "", ""
	if event.AuthorUserID != nil {
		authorID = string(*event.AuthorUserID)
	}
	if event.RunID != nil {
		runID = string(*event.RunID)
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`INSERT INTO session_events
		 (tenant_id, session_id, sequence, event_id, kind, author_user_id,
		  run_id, idempotency_key, blob_key, created_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS JsonDocument))`,
		event.TenantID, event.SessionID, event.Sequence, event.ID, event.Kind,
		authorID, runID, event.IdempotencyKey, event.Payload.Key, event.CreatedAt, record,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO session_event_idempotency
		 (tenant_id, session_id, idempotency_key, sequence, event_id, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		event.TenantID, event.SessionID, event.IdempotencyKey,
		event.Sequence, event.ID, event.CreatedAt,
	)
	return err
}

func validateCanonicalCommit(request ports.CanonicalUserEventCommit) error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.UserID.Validate(); err != nil {
		return err
	}
	if err := request.BindingID.Validate(); err != nil {
		return err
	}
	if request.ExpectedBindingRevision == 0 {
		return domain.ValidationError{Field: "frontend_binding.expected_revision", Reason: "must be positive"}
	}
	if err := request.Origin.Validate(); err != nil {
		return err
	}
	if request.Origin.BindingID != request.BindingID || request.Origin.BindingRevision != request.ExpectedBindingRevision {
		return domain.ValidationError{Field: "frontend_event_origin", Reason: "must identify the expected binding revision"}
	}
	for _, validate := range []error{
		request.IdempotencyKey.Validate(), request.EventID.Validate(), request.RunID.Validate(),
		request.AttemptID.Validate(), request.SubscriptionConnectionID.Validate(),
		request.ManifestID.Validate(), request.DispatchID.Validate(),
	} {
		if validate != nil {
			return validate
		}
	}
	if request.CommittedAt.IsZero() {
		return domain.ValidationError{Field: "canonical_ingress.committed_at", Reason: "must not be zero"}
	}
	if !request.ExpireAt.After(request.CommittedAt) {
		return domain.ValidationError{Field: "canonical_ingress.expire_at", Reason: "must be after committed_at"}
	}
	if err := request.Payload.Validate(); err != nil {
		return err
	}
	return nil
}

func validateCanonicalLookup(request ports.CanonicalUserEventLookup) error {
	for _, validate := range []error{
		request.TenantID.Validate(), request.UserID.Validate(), request.BindingID.Validate(),
		request.Frontend.Validate(), request.IdempotencyKey.Validate(),
		request.EventID.Validate(), request.RunID.Validate(),
	} {
		if validate != nil {
			return validate
		}
	}
	if request.ExternalConversationID == "" {
		return domain.ValidationError{
			Field: "external_conversation_id", Reason: "must not be empty",
		}
	}
	return nil
}

func frontendOriginDigest(origin domain.FrontendEventOrigin) string {
	payload, _ := json.Marshal(origin)
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

var _ ports.CanonicalIngressStore = (*Store)(nil)
