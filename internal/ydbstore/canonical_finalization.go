package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

var ErrRunFinalizationConflict = errors.New("run finalization conflict")

type finalizationEventIdentity struct {
	ID             domain.SessionEventID   `json:"id"`
	Kind           domain.SessionEventKind `json:"kind"`
	IdempotencyKey domain.IdempotencyKey   `json:"idempotency_key"`
	Payload        domain.BlobRef          `json:"payload"`
	DisplayText    string                  `json:"display_text,omitempty"`
}

func runFinalizationDigest(
	status domain.RunStatus,
	manifest *domain.ArtifactManifest,
	events []domain.SessionEventDraft,
) (string, error) {
	identities := make([]finalizationEventIdentity, 0, len(events))
	for _, event := range events {
		identities = append(identities, finalizationEventIdentity{
			ID: event.ID, Kind: event.Kind, IdempotencyKey: event.IdempotencyKey,
			Payload: event.Payload, DisplayText: event.DisplayText,
		})
	}
	payload, err := json.Marshal(struct {
		Status   domain.RunStatus            `json:"status"`
		Manifest *domain.ArtifactManifest    `json:"manifest,omitempty"`
		Events   []finalizationEventIdentity `json:"events"`
	}{Status: status, Manifest: manifest, Events: identities})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateCanonicalFinalizationEvents(
	status domain.RunStatus,
	events []domain.SessionEventDraft,
) error {
	switch status {
	case domain.RunSucceeded:
		assistantCount := 0
		for _, event := range events {
			switch event.Kind {
			case domain.SessionEventAssistantMessage:
				assistantCount++
			case domain.SessionEventToolCall, domain.SessionEventToolResult:
			default:
				return domain.ValidationError{
					Field: "worker_completion.events", Reason: "may contain only tool and assistant events",
				}
			}
		}
		if assistantCount != 1 {
			return domain.ValidationError{
				Field: "worker_completion.events", Reason: "must contain exactly one assistant event",
			}
		}
	case domain.RunFailed, domain.RunCancelled:
		if len(events) != 1 || events[0].Kind != domain.SessionEventSystemNotice {
			return domain.ValidationError{
				Field: "worker_failure.events", Reason: "must contain exactly one system notice",
			}
		}
	default:
		return domain.ValidationError{
			Field: "worker_finalization.status", Reason: "must be a terminal worker status",
		}
	}
	return nil
}

func matchingRunFinalizationTx(
	ctx context.Context,
	tx *stateTx,
	runID domain.RunID,
	status domain.RunStatus,
	digest string,
) (bool, error) {
	var storedStatus, storedDigest string
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT terminal_status, content_digest FROM run_finalizations
		 WHERE tenant_id = $1 AND run_id = $2`,
		tx.tenantID, runID,
	).Scan(&storedStatus, &storedDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if domain.RunStatus(storedStatus) != status || storedDigest != digest {
		return false, ErrRunFinalizationConflict
	}
	return true, nil
}

func appendCanonicalFinalizationTx(
	ctx context.Context,
	tx *stateTx,
	run domain.Run,
	status domain.RunStatus,
	digest string,
	drafts []domain.SessionEventDraft,
	at time.Time,
) error {
	if len(drafts) == 0 {
		return domain.ValidationError{Field: "worker_finalization.events", Reason: "must not be empty for a canonical run"}
	}
	if err := ensureSessionWritableTx(ctx, tx, run.SessionID); err != nil {
		return err
	}
	session, found, err := readSessionTx(ctx, tx, run.SessionID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("session %q not found", run.SessionID)
	}
	seenEvents := make(map[domain.SessionEventID]struct{}, len(drafts))
	seenKeys := make(map[domain.IdempotencyKey]struct{}, len(drafts))
	for _, draft := range drafts {
		if err := draft.ValidateForRun(run); err != nil {
			return err
		}
		if _, duplicate := seenEvents[draft.ID]; duplicate {
			return domain.ValidationError{Field: "worker_finalization.events", Reason: "event IDs must be unique"}
		}
		if _, duplicate := seenKeys[draft.IdempotencyKey]; duplicate {
			return domain.ValidationError{Field: "worker_finalization.events", Reason: "idempotency keys must be unique"}
		}
		seenEvents[draft.ID] = struct{}{}
		seenKeys[draft.IdempotencyKey] = struct{}{}
	}

	bindings, err := currentSessionBindingsTx(ctx, tx, run.SessionID, maxSessionDeletionRows)
	if err != nil {
		return err
	}
	previous := session
	for _, draft := range drafts {
		eventAt := draft.CreatedAt
		if eventAt.Before(session.UpdatedAt) {
			eventAt = session.UpdatedAt
		}
		runID := run.ID
		event := domain.SessionEvent{
			ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID,
			Sequence: session.LastEventSequence + 1, Kind: draft.Kind, RunID: &runID,
			IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, CreatedAt: eventAt,
		}
		if _, err := domain.AppendSessionEvent(&session, event, nil); err != nil {
			return err
		}
		if err := insertSessionEventTx(ctx, tx, event); err != nil {
			return err
		}
		if draft.ProjectionEligible() {
			for _, binding := range bindings {
				if err := insertFrontendProjectionTx(ctx, tx, event, binding, eventAt); err != nil {
					return err
				}
			}
		}
	}
	if err := updateSessionTx(ctx, tx, previous, session); err != nil {
		return err
	}
	displayText := ""
	for _, draft := range drafts {
		if draft.DisplayText != "" {
			displayText = draft.DisplayText
		}
	}
	terminal := run
	terminal.Status, terminal.UpdatedAt = status, at
	if status.Terminal() {
		terminal.FinishedAt = &at
	}
	if err := updateSessionDisplayMessageTx(ctx, tx, session, displayText, nil, &terminal); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO run_finalizations
		 (tenant_id, run_id, terminal_status, content_digest, created_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		run.TenantID, run.ID, status, digest, at,
	)
	return err
}

func currentSessionBindingsTx(
	ctx context.Context,
	tx *stateTx,
	sessionID domain.SessionID,
	maxRows uint64,
) ([]domain.FrontendBinding, error) {
	rows, err := tx.sqlTx.QueryContext(ctx,
		`SELECT binding_id FROM frontend_bindings_by_session
		 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`,
		tx.tenantID, sessionID, maxRows+1,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var bindingIDs []domain.FrontendBindingID
	for rows.Next() {
		if uint64(len(bindingIDs)) >= maxRows {
			return nil, domain.ValidationError{Field: "frontend_bindings", Reason: "exceeds the configured bound"}
		}
		var bindingID domain.FrontendBindingID
		if err := rows.Scan(&bindingID); err != nil {
			return nil, err
		}
		bindingIDs = append(bindingIDs, bindingID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	ready, err := sessionLifecycleIndexesReady(ctx, tx.sqlTx)
	if err != nil {
		return nil, err
	}
	if !ready {
		legacyRows, err := tx.sqlTx.QueryContext(ctx,
			`SELECT binding_id FROM frontend_bindings
			 WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`,
			tx.tenantID, sessionID, maxRows+1,
		)
		if err != nil {
			return nil, err
		}
		seen := make(map[domain.FrontendBindingID]struct{}, len(bindingIDs))
		for _, id := range bindingIDs {
			seen[id] = struct{}{}
		}
		for legacyRows.Next() {
			var id domain.FrontendBindingID
			if err := legacyRows.Scan(&id); err != nil {
				legacyRows.Close()
				return nil, err
			}
			if _, exists := seen[id]; !exists {
				bindingIDs = append(bindingIDs, id)
				seen[id] = struct{}{}
			}
			if uint64(len(bindingIDs)) > maxRows {
				legacyRows.Close()
				return nil, domain.ValidationError{Field: "frontend_bindings", Reason: "exceeds the configured bound"}
			}
		}
		if err := legacyRows.Close(); err != nil {
			return nil, err
		}
		slices.Sort(bindingIDs)
	}
	var bindings []domain.FrontendBinding
	for _, bindingID := range bindingIDs {
		binding, found, err := readBindingTx(ctx, tx, bindingID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("frontend binding session index references missing binding %q", bindingID)
		}
		if err := binding.Validate(); err != nil {
			return nil, err
		}
		if binding.TenantID != tx.tenantID || binding.SessionID != sessionID {
			return nil, domain.ValidationError{Field: "frontend_binding", Reason: "query returned a cross-boundary binding"}
		}
		bindings = append(bindings, binding)
	}
	return bindings, nil
}

func insertFrontendProjectionTx(
	ctx context.Context,
	tx *stateTx,
	event domain.SessionEvent,
	binding domain.FrontendBinding,
	at time.Time,
) error {
	if event.RunID == nil {
		return domain.ValidationError{Field: "frontend_projection.run_id", Reason: "projectable event must reference a run"}
	}
	projection := domain.FrontendProjection{
		ID: domain.FrontendProjectionID(stableCanonicalID(
			"prj_", string(event.TenantID)+":"+string(event.ID)+":"+string(binding.ID),
		)),
		TenantID: event.TenantID, SessionID: event.SessionID,
		EventID: event.ID, EventSequence: event.Sequence, EventKind: event.Kind,
		BindingID: binding.ID, BindingRevision: binding.Revision, Frontend: binding.Frontend,
		Status: domain.FrontendProjectionPending,
		IdempotencyKey: domain.IdempotencyKey(stableCanonicalID(
			"pik_", string(event.TenantID)+":"+string(event.ID)+":"+string(binding.ID),
		)),
		CreatedAt: at, UpdatedAt: at,
	}
	if err := projection.ValidateFor(event, binding); err != nil {
		return err
	}
	bucket, err := ydbpartition.BucketV1(string(projection.ID))
	if err != nil {
		return err
	}
	record, err := marshal(projection)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO frontend_projection_outbox
		 (tenant_id, frontend_projection_id, session_id, event_id, event_sequence,
		  binding_id, binding_revision, frontend, status, created_at, updated_at, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS JsonDocument))`,
		projection.TenantID, projection.ID, projection.SessionID, projection.EventID,
		projection.EventSequence, projection.BindingID, projection.BindingRevision,
		projection.Frontend, projection.Status, projection.CreatedAt, projection.UpdatedAt, record,
	)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO frontend_projections_by_session
		 (tenant_id, session_id, frontend_projection_id) VALUES ($1, $2, $3)`,
		projection.TenantID, projection.SessionID, projection.ID,
	)
	if err != nil {
		return err
	}
	if _, err := tx.sqlTx.ExecContext(ctx,
		`INSERT INTO frontend_projections_by_run
		 (tenant_id, run_id, frontend, frontend_projection_id) VALUES ($1, $2, $3, $4)`,
		projection.TenantID, *event.RunID, projection.Frontend, projection.ID,
	); err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO frontend_projection_ready_v1
		 (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id, run_id)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		projection.Frontend, bucket, projection.CreatedAt, projection.TenantID,
		projection.ID, *event.RunID,
	)
	return err
}
