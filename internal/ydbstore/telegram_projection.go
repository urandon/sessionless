package ydbstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type telegramProjectionContext struct {
	projection domain.FrontendProjection
	event      domain.SessionEvent
	run        domain.Run
	trigger    domain.SessionEvent
	binding    domain.FrontendBinding
}

func (store *Store) ListRunTelegramProjections(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	limit uint64,
) (result []ports.TelegramProjectionReady, err error) {
	if err := tenantID.Validate(); err != nil {
		return nil, err
	}
	if err := runID.Validate(); err != nil {
		return nil, err
	}
	if limit == 0 {
		return nil, domain.ValidationError{Field: "Telegram projection limit", Reason: "must be positive"}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT frontend_projection_id
		 FROM frontend_projections_by_run
		 WHERE tenant_id = $1 AND run_id = $2 AND frontend = $3
		 ORDER BY frontend_projection_id
		 LIMIT $4`,
		tenantID, runID, domain.FrontendTelegram, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		item := ports.TelegramProjectionReady{TenantID: tenantID, RunID: runID}
		if err := rows.Scan(&item.ProjectionID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) ListReadyTelegramProjections(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	limit uint64,
) (result []ports.TelegramProjectionReady, err error) {
	if bucket >= ydbpartition.BucketCountV1 {
		return nil, domain.ValidationError{Field: "Telegram projection bucket", Reason: "must be within the v1 bucket range"}
	}
	if before.IsZero() || limit == 0 {
		return nil, domain.ValidationError{Field: "Telegram projection listing", Reason: "requires a non-zero time and positive limit"}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT tenant_id, frontend_projection_id, run_id
		 FROM frontend_projection_ready_v1
		 WHERE frontend = $1 AND shard_bucket = $2 AND created_at <= $3
		 ORDER BY created_at, tenant_id, frontend_projection_id
		 LIMIT $4`,
		domain.FrontendTelegram, bucket, before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ports.TelegramProjectionReady
		if err := rows.Scan(&item.TenantID, &item.ProjectionID, &item.RunID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) MaterializeTelegramProjection(
	ctx context.Context,
	tenantID domain.TenantID,
	projectionID domain.FrontendProjectionID,
	content *ports.TelegramProjectionContent,
	at time.Time,
) (result ports.TelegramProjectionResult, err error) {
	if err := projectionID.Validate(); err != nil {
		return result, err
	}
	if at.IsZero() {
		return result, domain.ValidationError{Field: "Telegram projection time", Reason: "must not be zero"}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		resolved, terminalCode, found, err := resolveTelegramProjectionTx(ctx, tx, projectionID)
		if err != nil {
			return err
		}
		if !found {
			result = ports.TelegramProjectionResult{
				Outcome: ports.TelegramProjectionNoop,
				Code:    "projection_missing",
			}
			return nil
		}
		result = ports.TelegramProjectionResult{
			RunID:          resolved.run.ID,
			DeliveryID:     outboxwake.TelegramProjectionDeliveryID(resolved.projection.ID),
			EventKind:      resolved.event.Kind,
			EventPayload:   resolved.event.Payload,
			TriggerPayload: resolved.trigger.Payload,
		}
		if resolved.projection.Frontend != domain.FrontendTelegram {
			result.Outcome, result.Code = ports.TelegramProjectionNoop, "frontend_not_telegram"
			return nil
		}
		if terminalCode != "" {
			if err := deleteFrontendProjectionTx(ctx, tx, resolved); err != nil {
				return err
			}
			result.Outcome, result.Code = ports.TelegramProjectionTerminal, terminalCode
			return nil
		}
		if content == nil {
			result.Outcome, result.Code = ports.TelegramProjectionNeedsContent, "canonical_content_required"
			return nil
		}
		if content.EventPayload != resolved.event.Payload || content.TriggerPayload != resolved.trigger.Payload {
			return domain.ValidationError{Field: "Telegram projection payload", Reason: "must preserve the exact canonical blob references"}
		}
		if content.ReplyToMessageID < 0 || content.TriggerChatID == 0 {
			return domain.ValidationError{Field: "Telegram projection origin", Reason: "contains an invalid Telegram message or chat identifier"}
		}
		chatID, parseErr := strconv.ParseInt(resolved.binding.ExternalConversationID, 10, 64)
		if parseErr != nil || chatID == 0 || chatID != content.TriggerChatID {
			return domain.ValidationError{Field: "Telegram projection binding", Reason: "does not match the canonical trigger origin"}
		}
		switch resolved.event.Kind {
		case domain.SessionEventAssistantMessage:
			if content.ArtifactManifestID == nil {
				return domain.ValidationError{Field: "Telegram projection artifact manifest", Reason: "is required for an assistant event"}
			}
			manifest, manifestFound, err := readJSON[domain.ArtifactManifest](ctx, tx.sqlTx,
				`SELECT payload FROM artifact_manifests
				 WHERE tenant_id = $1 AND artifact_manifest_id = $2`,
				tenantID, *content.ArtifactManifestID,
			)
			if err != nil {
				return err
			}
			if !manifestFound {
				return fmt.Errorf("artifact manifest %q not found", *content.ArtifactManifestID)
			}
			if err := manifest.ValidateForRun(resolved.run); err != nil {
				return err
			}
		case domain.SessionEventSystemNotice:
			if content.ArtifactManifestID != nil {
				return domain.ValidationError{Field: "Telegram projection artifact manifest", Reason: "is not allowed for a system notice"}
			}
		default:
			return domain.ValidationError{Field: "Telegram projection event kind", Reason: "is not projectable to Telegram"}
		}
		delivery := domain.TelegramDeliveryOutbox{
			ID:                 result.DeliveryID,
			TenantID:           tenantID,
			RunID:              resolved.run.ID,
			Chat:               domain.TelegramChatRef{TenantID: tenantID, ChatID: chatID},
			ReplyToMessageID:   content.ReplyToMessageID,
			Payload:            resolved.event.Payload,
			ArtifactManifestID: content.ArtifactManifestID,
			Projection: &domain.TelegramProjectionRef{
				ProjectionID: resolved.projection.ID, SessionID: resolved.projection.SessionID,
				EventID: resolved.projection.EventID, EventSequence: resolved.projection.EventSequence,
				BindingID: resolved.projection.BindingID, BindingRevision: resolved.projection.BindingRevision,
				TriggerEventID: resolved.run.TriggerEventID,
			},
			Status: domain.DeliveryPending, IdempotencyKey: resolved.projection.IdempotencyKey,
			CreatedAt: at, UpdatedAt: at,
		}
		if err := state.PutTelegramDeliveryOutbox(ctx, delivery); err != nil {
			return err
		}
		if err := deleteFrontendProjectionTx(ctx, tx, resolved); err != nil {
			return err
		}
		result.Outcome, result.Code, result.Created = ports.TelegramProjectionMaterialized, "delivery_created", true
		return nil
	})
	return result, err
}

func resolveTelegramProjectionTx(
	ctx context.Context,
	tx *stateTx,
	projectionID domain.FrontendProjectionID,
) (resolved telegramProjectionContext, terminalCode string, found bool, err error) {
	resolved.projection, found, err = readJSON[domain.FrontendProjection](ctx, tx.sqlTx,
		`SELECT record FROM frontend_projection_outbox
		 WHERE tenant_id = $1 AND frontend_projection_id = $2`, tx.tenantID, projectionID)
	if err != nil || !found {
		return resolved, "", found, err
	}
	resolved.event, found, err = readJSON[domain.SessionEvent](ctx, tx.sqlTx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
		tx.tenantID, resolved.projection.SessionID, resolved.projection.EventSequence)
	if err != nil {
		return resolved, "", false, err
	}
	if !found {
		return resolved, "", false, fmt.Errorf("projection %q canonical event not found", projectionID)
	}
	if resolved.event.ID != resolved.projection.EventID || resolved.event.Kind != resolved.projection.EventKind || resolved.event.RunID == nil {
		return resolved, "", false, domain.ValidationError{Field: "frontend_projection.event", Reason: "does not match the canonical event"}
	}
	resolved.run, found, err = tx.GetRun(ctx, *resolved.event.RunID)
	if err != nil {
		return resolved, "", false, err
	}
	if !found || resolved.run.SessionID != resolved.projection.SessionID {
		return resolved, "", false, fmt.Errorf("projection %q owning run not found", projectionID)
	}
	if err := resolved.run.Validate(); err != nil {
		return resolved, "", false, err
	}
	if !resolved.run.Status.Terminal() {
		return resolved, "", false, domain.ValidationError{Field: "frontend_projection.run", Reason: "must be terminal"}
	}
	resolved.trigger, found, err = readJSON[domain.SessionEvent](ctx, tx.sqlTx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND event_id = $3
		 LIMIT 1`, tx.tenantID, resolved.run.SessionID, resolved.run.TriggerEventID)
	if err != nil {
		return resolved, "", false, err
	}
	if !found || resolved.trigger.ID != resolved.run.TriggerEventID ||
		resolved.trigger.Kind != domain.SessionEventUserMessage || resolved.trigger.AuthorUserID == nil {
		return resolved, "", false, fmt.Errorf("projection %q canonical trigger event not found", projectionID)
	}
	if err := resolved.trigger.Validate(); err != nil {
		return resolved, "", false, err
	}
	resolved.binding, found, err = readBindingTx(ctx, tx, resolved.projection.BindingID)
	if err != nil {
		return resolved, "", false, err
	}
	if !found {
		return resolved, "binding_missing", true, nil
	}
	snapshotBinding := resolved.binding
	snapshotBinding.SessionID = resolved.projection.SessionID
	snapshotBinding.Revision = resolved.projection.BindingRevision
	if err := resolved.projection.ValidateFor(resolved.event, snapshotBinding); err != nil {
		return resolved, "", false, err
	}
	if resolved.binding.SessionID != resolved.projection.SessionID ||
		resolved.binding.Revision != resolved.projection.BindingRevision {
		return resolved, "binding_stale", true, nil
	}
	session, sessionFound, err := readSessionTx(ctx, tx, resolved.projection.SessionID)
	if err != nil {
		return resolved, "", false, err
	}
	if !sessionFound {
		return resolved, "session_missing", true, nil
	}
	if err := session.Validate(); err != nil {
		return resolved, "", false, err
	}
	if session.TenantID != tx.tenantID || session.ID != resolved.projection.SessionID {
		return resolved, "", false, domain.ValidationError{Field: "frontend_projection.session", Reason: "does not match the canonical session"}
	}
	if session.Status == domain.SessionArchived {
		return resolved, "session_archived", true, nil
	}
	membership, membershipFound, err := readMembershipTx(ctx, tx.sqlTx, *resolved.trigger.AuthorUserID, tx.tenantID)
	if err != nil {
		return resolved, "", false, err
	}
	if !membershipFound || membership.Status != domain.TenantMembershipActive {
		return resolved, "membership_revoked", true, nil
	}
	if err := membership.Authorize(*resolved.trigger.AuthorUserID, tx.tenantID, domain.TenantPermissionRead); err != nil {
		return resolved, "membership_revoked", true, nil
	}
	participant, participantFound, err := readJSON[domain.SessionParticipant](ctx, tx.sqlTx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
		tx.tenantID, resolved.projection.SessionID, *resolved.trigger.AuthorUserID)
	if err != nil {
		return resolved, "", false, err
	}
	if !participantFound || participant.Status != domain.SessionParticipantActive {
		return resolved, "participant_removed", true, nil
	}
	if err := participant.Authorize(tx.tenantID, resolved.projection.SessionID, *resolved.trigger.AuthorUserID, false); err != nil {
		return resolved, "participant_removed", true, nil
	}
	return resolved, "", true, nil
}

func deleteFrontendProjectionTx(ctx context.Context, tx *stateTx, resolved telegramProjectionContext) error {
	bucket, err := ydbpartition.BucketV1(string(resolved.projection.ID))
	if err != nil {
		return err
	}
	statements := []struct {
		query string
		args  []any
	}{
		{`DELETE FROM frontend_projection_ready_v1
		  WHERE frontend = $1 AND shard_bucket = $2 AND created_at = $3
		  AND tenant_id = $4 AND frontend_projection_id = $5`, []any{
			resolved.projection.Frontend, bucket, resolved.projection.CreatedAt,
			resolved.projection.TenantID, resolved.projection.ID,
		}},
		{`DELETE FROM frontend_projections_by_run
		  WHERE tenant_id = $1 AND run_id = $2 AND frontend = $3 AND frontend_projection_id = $4`, []any{
			resolved.projection.TenantID, resolved.run.ID, resolved.projection.Frontend, resolved.projection.ID,
		}},
		{`DELETE FROM frontend_projections_by_session
		  WHERE tenant_id = $1 AND session_id = $2 AND frontend_projection_id = $3`, []any{
			resolved.projection.TenantID, resolved.projection.SessionID, resolved.projection.ID,
		}},
		{`DELETE FROM frontend_projection_outbox
		  WHERE tenant_id = $1 AND frontend_projection_id = $2`, []any{
			resolved.projection.TenantID, resolved.projection.ID,
		}},
	}
	for _, statement := range statements {
		if _, err := tx.sqlTx.ExecContext(ctx, statement.query, statement.args...); err != nil {
			return err
		}
	}
	return nil
}

func authorizeTelegramProjectionDeliveryTx(
	ctx context.Context,
	tx *stateTx,
	delivery domain.TelegramDeliveryOutbox,
) (string, error) {
	ref := delivery.Projection
	if ref == nil {
		return "", nil
	}
	if err := ref.Validate(); err != nil {
		return "", err
	}
	event, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3`,
		tx.tenantID, ref.SessionID, ref.EventSequence)
	if err != nil {
		return "", err
	}
	if !found || event.ID != ref.EventID || event.RunID == nil || *event.RunID != delivery.RunID ||
		event.Payload != delivery.Payload ||
		(event.Kind != domain.SessionEventAssistantMessage && event.Kind != domain.SessionEventSystemNotice) {
		return "", domain.ValidationError{Field: "Telegram delivery canonical event", Reason: "does not match its immutable projection reference"}
	}
	if err := event.Validate(); err != nil {
		return "", err
	}
	run, found, err := tx.GetRun(ctx, delivery.RunID)
	if err != nil {
		return "", err
	}
	if !found || run.SessionID != ref.SessionID || run.TriggerEventID != ref.TriggerEventID {
		return "", domain.ValidationError{Field: "Telegram delivery run", Reason: "does not match its immutable projection reference"}
	}
	if err := run.Validate(); err != nil {
		return "", err
	}
	if !run.Status.Terminal() {
		return "", domain.ValidationError{Field: "Telegram delivery run", Reason: "must remain terminal"}
	}
	binding, found, err := readBindingTx(ctx, tx, ref.BindingID)
	if err != nil {
		return "", err
	}
	if !found {
		return "binding_missing", nil
	}
	if binding.Frontend != domain.FrontendTelegram || binding.SessionID != ref.SessionID || binding.Revision != ref.BindingRevision {
		return "binding_stale", nil
	}
	chatID, parseErr := strconv.ParseInt(binding.ExternalConversationID, 10, 64)
	if parseErr != nil || chatID == 0 || chatID != delivery.Chat.ChatID || delivery.Chat.TenantID != tx.tenantID {
		return "", domain.ValidationError{Field: "Telegram delivery chat", Reason: "does not match the live binding"}
	}
	session, found, err := readSessionTx(ctx, tx, ref.SessionID)
	if err != nil {
		return "", err
	}
	if !found {
		return "session_missing", nil
	}
	if err := session.Validate(); err != nil {
		return "", err
	}
	if session.TenantID != tx.tenantID || session.ID != ref.SessionID {
		return "", domain.ValidationError{Field: "Telegram delivery session", Reason: "does not match its immutable projection reference"}
	}
	if session.Status == domain.SessionArchived {
		return "session_archived", nil
	}
	trigger, found, err := readJSON[domain.SessionEvent](ctx, tx.sqlTx,
		`SELECT record FROM session_events
		 WHERE tenant_id = $1 AND session_id = $2 AND event_id = $3
		 LIMIT 1`, tx.tenantID, ref.SessionID, ref.TriggerEventID)
	if err != nil {
		return "", err
	}
	if !found || trigger.ID != ref.TriggerEventID || trigger.Kind != domain.SessionEventUserMessage || trigger.AuthorUserID == nil {
		return "", domain.ValidationError{Field: "Telegram delivery trigger", Reason: "does not reference a canonical user event"}
	}
	if err := trigger.Validate(); err != nil {
		return "", err
	}
	membership, found, err := readMembershipTx(ctx, tx.sqlTx, *trigger.AuthorUserID, tx.tenantID)
	if err != nil {
		return "", err
	}
	if !found || membership.Status != domain.TenantMembershipActive {
		return "membership_revoked", nil
	}
	if err := membership.Authorize(*trigger.AuthorUserID, tx.tenantID, domain.TenantPermissionRead); err != nil {
		return "membership_revoked", nil
	}
	participant, found, err := readJSON[domain.SessionParticipant](ctx, tx.sqlTx,
		`SELECT record FROM session_participants
		 WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`,
		tx.tenantID, ref.SessionID, *trigger.AuthorUserID)
	if err != nil {
		return "", err
	}
	if !found || participant.Status != domain.SessionParticipantActive {
		return "participant_removed", nil
	}
	if err := participant.Authorize(tx.tenantID, ref.SessionID, *trigger.AuthorUserID, false); err != nil {
		return "participant_removed", nil
	}
	return "", nil
}

var _ ports.TelegramDeliveryStore = (*Store)(nil)
