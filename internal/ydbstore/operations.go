package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

var (
	ErrLeaseHeld = errors.New("run already has an active lease")
	ErrLeaseLost = errors.New("lease fence no longer owns the run")
)

type TelegramIngress = ports.TelegramIngress
type TelegramIngressResult = ports.TelegramIngressResult

// IngestTelegram is the pre-canonical compatibility transaction retained for
// migration and upgrade tests. Production ordinary-message ingress uses
// CommitCanonicalUserEvent. A duplicate returns the already-associated run
// without emitting a second outbox row.
func (store *Store) IngestTelegram(
	ctx context.Context,
	request ports.TelegramIngress,
) (result ports.TelegramIngressResult, err error) {
	if err := request.TenantID.Validate(); err != nil {
		return result, err
	}
	if err := domain.ValidateOpaqueID("telegram.source_id", request.SourceID); err != nil {
		return result, err
	}
	if request.UpdateID < 0 {
		return result, domain.ValidationError{
			Field: "telegram.update_id", Reason: "must not be negative",
		}
	}
	if request.ExpireAt.IsZero() {
		return result, domain.ValidationError{
			Field: "telegram.expire_at", Reason: "must not be zero",
		}
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var existing string
		queryErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT run_id FROM telegram_updates
			 WHERE tenant_id = $1 AND source_id = $2 AND update_id = $3`,
			request.TenantID, request.SourceID, request.UpdateID,
		).Scan(&existing)
		switch {
		case queryErr == nil:
			result = ports.TelegramIngressResult{RunID: domain.RunID(existing), Created: false}
			return nil
		case !errors.Is(queryErr, sql.ErrNoRows):
			return queryErr
		}
		if err := state.PutRun(ctx, request.Run); err != nil {
			return err
		}
		if err := state.PutAttempt(ctx, request.Attempt); err != nil {
			return err
		}
		if err := state.PutArtifactManifest(ctx, request.InputManifest); err != nil {
			return err
		}
		if err := state.PutDispatchOutbox(ctx, request.Dispatch); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO telegram_updates
			 (tenant_id, source_id, update_id, run_id, received_at, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			request.TenantID, request.SourceID, request.UpdateID, request.Run.ID,
			request.Run.CreatedAt, request.ExpireAt,
		); err != nil {
			return err
		}
		result = ports.TelegramIngressResult{RunID: request.Run.ID, Created: true}
		return nil
	})
	return result, err
}

// ExecuteTelegramCommand atomically deduplicates a Telegram update, applies
// the requested control-plane state change, records a terminal command run,
// and enqueues its inline Telegram reply. Commands never emit a dispatch row.
func (store *Store) ExecuteTelegramCommand(
	ctx context.Context,
	request ports.TelegramCommandRequest,
) (result ports.TelegramIngressResult, err error) {
	if err := validateTelegramCommand(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		userID := telegramUserID(request.Actor.ID)
		binding, found, err := readTelegramConversationBindingTx(ctx, tx, request.Conversation)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("Telegram frontend binding for conversation %q not found", request.Conversation.ID)
		}
		if err := authorizeTenantWriteTx(ctx, tx, userID); err != nil {
			return err
		}
		if err := authorizeSessionWriteTx(ctx, tx, binding.SessionID, userID); err != nil {
			return err
		}

		var existing string
		queryErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT run_id FROM telegram_updates
			 WHERE tenant_id = $1 AND source_id = $2 AND update_id = $3`,
			request.TenantID, request.SourceID, request.UpdateID,
		).Scan(&existing)
		switch {
		case queryErr == nil:
			result = ports.TelegramIngressResult{
				RunID: domain.RunID(existing), Created: false,
			}
			return nil
		case !errors.Is(queryErr, sql.ErrNoRows):
			return queryErr
		}
		if request.Kind != ports.TelegramCommandNewContext {
			request.SessionID = binding.SessionID
		}
		reply, err := executeTelegramCommandState(ctx, tx, request, binding)
		if err != nil {
			return err
		}
		finishedAt := request.RequestedAt
		run := domain.Run{
			ID: request.RunID, TenantID: request.TenantID,
			SessionID:                request.SessionID,
			TriggerEventID:           request.TriggerEventID,
			SubscriptionConnectionID: request.SubscriptionConnectionID,
			Status:                   domain.RunSucceeded,
			IdempotencyKey:           request.IdempotencyKey,
			FinishedAt:               &finishedAt, CreatedAt: request.RequestedAt,
			UpdatedAt: request.RequestedAt,
		}
		if err := state.PutRun(ctx, run); err != nil {
			return err
		}
		delivery := domain.TelegramDeliveryOutbox{
			ID: request.DeliveryID, TenantID: request.TenantID,
			RunID: run.ID, Chat: request.Chat,
			ReplyToMessageID: request.ReplyToMessageID,
			Text:             reply, Status: domain.DeliveryPending,
			IdempotencyKey: request.IdempotencyKey,
			CreatedAt:      request.RequestedAt, UpdatedAt: request.RequestedAt,
		}
		if err := state.PutTelegramDeliveryOutbox(ctx, delivery); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO telegram_updates
			 (tenant_id, source_id, update_id, run_id, received_at, expire_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			request.TenantID, request.SourceID, request.UpdateID, run.ID,
			request.RequestedAt, request.ExpireAt,
		); err != nil {
			return err
		}
		result = ports.TelegramIngressResult{RunID: run.ID, Created: true}
		return nil
	})
	return result, err
}

func executeTelegramCommandState(
	ctx context.Context,
	tx *stateTx,
	request ports.TelegramCommandRequest,
	binding domain.FrontendBinding,
) (reply string, err error) {
	switch request.Kind {
	case ports.TelegramCommandConnectCodex:
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPDATE subscription_connections
			 SET entitlement_state = $1, quota_state = $2,
			     observed_at = $3, updated_at = $4
			 WHERE tenant_id = $5 AND subscription_connection_id = $6`,
			domain.EntitlementReauthRequired, "unknown",
			request.RequestedAt, request.RequestedAt,
			request.TenantID, request.SubscriptionConnectionID,
		); err != nil {
			return "", err
		}
		if err := setSchedulerSlotState(
			ctx, tx, request.SubscriptionConnectionID,
			domain.SchedulerReauthRequired, nil, request.RequestedAt,
		); err != nil {
			return "", err
		}
		return "Codex connection: authorization required. No credential has been stored; provider authorization will be completed by the isolated Codex adapter.", nil
	case ports.TelegramCommandComputeStatus:
		var provider, entitlement, quota string
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT provider, entitlement_state, quota_state
			 FROM subscription_connections
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, request.SubscriptionConnectionID,
		).Scan(&provider, &entitlement, &quota); err != nil {
			return "", err
		}
		var schedulerState string
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT state FROM subscription_scheduler_slots
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, request.SubscriptionConnectionID,
		).Scan(&schedulerState); err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"Compute provider: %s\nConnection: %s\nQuota: %s\nScheduler: %s",
			provider, entitlement, quota, schedulerState,
		), nil
	case ports.TelegramCommandDisconnectCodex:
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPDATE subscription_connections
			 SET credential_ref = $1, entitlement_state = $2, quota_state = $3,
			     observed_at = $4, updated_at = $5
			 WHERE tenant_id = $6 AND subscription_connection_id = $7`,
			"", domain.EntitlementDisconnected, "unknown",
			request.RequestedAt, request.RequestedAt,
			request.TenantID, request.SubscriptionConnectionID,
		); err != nil {
			return "", err
		}
		if err := setSchedulerSlotState(
			ctx, tx, request.SubscriptionConnectionID,
			domain.SchedulerReauthRequired, nil, request.RequestedAt,
		); err != nil {
			return "", err
		}
		return "Codex connection disconnected. Stored credential references were removed and no API-billing fallback was enabled.", nil
	case ports.TelegramCommandNewContext:
		userID := telegramUserID(request.Actor.ID)
		session := domain.Session{
			ID: request.SessionID, TenantID: request.TenantID, CreatedBy: userID,
			Status: domain.SessionActive, CreatedAt: request.RequestedAt, UpdatedAt: request.RequestedAt,
		}
		owner := domain.SessionParticipant{
			TenantID: request.TenantID, SessionID: request.SessionID, UserID: userID,
			Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
			CreatedAt: request.RequestedAt, UpdatedAt: request.RequestedAt,
		}
		if err := createSessionTx(ctx, tx, session, owner); err != nil {
			return "", err
		}
		if err := binding.Switch(request.ExpectedBindingRevision, request.SessionID, request.RequestedAt); err != nil {
			return "", err
		}
		if err := writeBindingTx(ctx, tx, binding); err != nil {
			return "", err
		}
		return "A new session was created and this Telegram chat now points to it.", nil
	case ports.TelegramCommandHelp:
		return "Supported commands:\n/connect codex\n/compute status\n/compute disconnect codex\n/new", nil
	default:
		return "", domain.ValidationError{
			Field: "telegram.command", Reason: "is unknown",
		}
	}
}

func validateTelegramCommand(request ports.TelegramCommandRequest) error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("telegram.source_id", request.SourceID); err != nil {
		return err
	}
	if request.UpdateID <= 0 {
		return domain.ValidationError{
			Field: "telegram.update_id", Reason: "must be positive",
		}
	}
	if request.ExpireAt.IsZero() || !request.ExpireAt.After(request.RequestedAt) {
		return domain.ValidationError{
			Field: "telegram.expire_at", Reason: "must be after requested_at",
		}
	}
	if !request.Kind.Valid() {
		return domain.ValidationError{Field: "telegram.command", Reason: "is unknown"}
	}
	if err := domain.ValidateOpaqueID("provider", request.Provider); err != nil {
		return err
	}
	if (request.Kind == ports.TelegramCommandConnectCodex ||
		request.Kind == ports.TelegramCommandComputeStatus ||
		request.Kind == ports.TelegramCommandDisconnectCodex) &&
		request.Provider != "codex" {
		return domain.ValidationError{
			Field:  "telegram.command.provider",
			Reason: "must be codex for the requested command",
		}
	}
	if err := request.Actor.Validate(); err != nil {
		return err
	}
	if err := request.Conversation.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Actor.TenantID); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Conversation.TenantID); err != nil {
		return err
	}
	if request.Actor.Frontend != request.Conversation.Frontend {
		return domain.ValidationError{
			Field:  "telegram.command.frontend",
			Reason: "actor and conversation must use the same frontend",
		}
	}
	if request.ExpectedBindingRevision == 0 {
		return domain.ValidationError{
			Field: "telegram.command.expected_binding_revision", Reason: "must be positive",
		}
	}
	if err := request.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := request.RunID.Validate(); err != nil {
		return err
	}
	if err := request.SessionID.Validate(); err != nil {
		return err
	}
	if err := request.TriggerEventID.Validate(); err != nil {
		return err
	}
	if err := request.DeliveryID.Validate(); err != nil {
		return err
	}
	if err := request.Chat.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Chat.TenantID); err != nil {
		return err
	}
	if request.ReplyToMessageID <= 0 {
		return domain.ValidationError{
			Field: "telegram.reply_to_message_id", Reason: "must be positive",
		}
	}
	if err := request.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if request.RequestedAt.IsZero() {
		return domain.ValidationError{
			Field: "telegram.requested_at", Reason: "must not be zero",
		}
	}
	return nil
}

// EnsureTelegramIdentity materializes the deterministic Telegram identity and
// resolves the frontend conversation to its current canonical session.
func (store *Store) EnsureTelegramIdentity(
	ctx context.Context,
	request ports.TelegramIdentityRequest,
) (result ports.TelegramIdentityState, err error) {
	if err := validateTelegramIdentity(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		userID := telegramUserID(request.Actor.ID)
		bindingID := telegramBindingID(request.Conversation.ID)
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPSERT INTO tenants
			 (tenant_id, status, created_at, updated_at)
			 VALUES ($1, $2, $3, $4)`,
			request.TenantID, "active", request.ObservedAt, request.ObservedAt,
		); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPSERT INTO actors
			 (tenant_id, actor_id, user_id, frontend, external_id, created_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			request.TenantID, request.Actor.ID, userID, request.Actor.Frontend,
			request.Actor.ExternalID, request.ObservedAt, request.ObservedAt,
		); err != nil {
			return err
		}
		externalIdentity, _, err := resolveOrCreateExternalIdentityTx(
			ctx,
			tx.sqlTx,
			domain.ExternalSubject{
				Provider: domain.IdentityProviderTelegram,
				Subject:  request.Actor.ExternalID,
			},
			userID,
			request.ObservedAt,
		)
		if err != nil {
			return err
		}
		if externalIdentity.UserID != userID {
			return domain.ErrExternalIdentityConflict
		}
		membership, membershipFound, err := readMembershipTx(
			ctx, tx.sqlTx, externalIdentity.UserID, request.TenantID,
		)
		if err != nil {
			return err
		}
		if !membershipFound {
			membership = domain.TenantMembership{
				TenantID: request.TenantID, UserID: externalIdentity.UserID,
				Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive,
				SecurityVersion: 1, CreatedAt: request.ObservedAt, UpdatedAt: request.ObservedAt,
			}
			if err := putMembershipTx(ctx, tx.sqlTx, membership); err != nil {
				return err
			}
		}
		if err := membership.Authorize(userID, request.TenantID, domain.TenantPermissionWrite); err != nil {
			return err
		}
		binding, found, err := readTelegramConversationBindingTx(ctx, tx, request.Conversation)
		if err != nil {
			return err
		}
		createdBinding := !found
		if createdBinding {
			sessionID := telegramInitialSessionID(request.Conversation.ID)
			session := domain.Session{
				ID: sessionID, TenantID: request.TenantID, CreatedBy: userID,
				Status: domain.SessionActive, CreatedAt: request.ObservedAt, UpdatedAt: request.ObservedAt,
			}
			owner := domain.SessionParticipant{
				TenantID: request.TenantID, SessionID: sessionID, UserID: userID,
				Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive,
				CreatedAt: request.ObservedAt, UpdatedAt: request.ObservedAt,
			}
			if err := createSessionTx(ctx, tx, session, owner); err != nil {
				return err
			}
			binding = domain.FrontendBinding{
				ID: bindingID, TenantID: request.TenantID, Frontend: request.Conversation.Frontend,
				ExternalConversationID: request.Conversation.ExternalID, SessionID: sessionID,
				Revision: 1, CreatedAt: request.ObservedAt, UpdatedAt: request.ObservedAt,
			}
			if err := writeBindingTx(ctx, tx, binding); err != nil {
				return err
			}
		}
		if !createdBinding {
			if err := authorizeSessionWriteTx(ctx, tx, binding.SessionID, userID); err != nil {
				return err
			}
		}
		var existingConnection string
		connectionErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT subscription_connection_id FROM subscription_connections
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, request.SubscriptionConnectionID,
		).Scan(&existingConnection)
		switch {
		case errors.Is(connectionErr, sql.ErrNoRows):
			if _, err := tx.sqlTx.ExecContext(ctx,
				`INSERT INTO subscription_connections
				 (tenant_id, subscription_connection_id, actor_id, provider,
				  credential_ref, entitlement_state, quota_state, observed_at,
				  created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
				request.TenantID, request.SubscriptionConnectionID, request.Actor.ID,
				request.Provider, "", "disconnected", "unknown", request.ObservedAt,
				request.ObservedAt, request.ObservedAt,
			); err != nil {
				return err
			}
		case connectionErr != nil:
			return connectionErr
		}
		var existingSlot string
		slotErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT subscription_connection_id FROM subscription_scheduler_slots
			 WHERE tenant_id = $1 AND subscription_connection_id = $2`,
			request.TenantID, request.SubscriptionConnectionID,
		).Scan(&existingSlot)
		switch {
		case errors.Is(slotErr, sql.ErrNoRows):
			if _, err := tx.sqlTx.ExecContext(ctx,
				`INSERT INTO subscription_scheduler_slots
				 (tenant_id, subscription_connection_id, state, active_run_id,
				  active_reservation_id, blocked_until, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				request.TenantID, request.SubscriptionConnectionID,
				domain.SchedulerReauthRequired, "", "",
				time.Unix(0, 0).UTC(), request.ObservedAt,
			); err != nil {
				return err
			}
		case slotErr != nil:
			return slotErr
		}
		var counterTenant string
		counterErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT tenant_id FROM tenant_scheduler_counters WHERE tenant_id = $1`,
			request.TenantID,
		).Scan(&counterTenant)
		switch {
		case errors.Is(counterErr, sql.ErrNoRows):
			if _, err := tx.sqlTx.ExecContext(ctx,
				`INSERT INTO tenant_scheduler_counters
				 (tenant_id, queue_depth, active_runs, updated_at)
				 VALUES ($1, $2, $3, $4)`,
				request.TenantID, uint32(0), uint32(0), request.ObservedAt,
			); err != nil {
				return err
			}
		case counterErr != nil:
			return counterErr
		}
		result = ports.TelegramIdentityState{
			UserID: userID, SessionID: binding.SessionID,
			BindingID: binding.ID, BindingRevision: binding.Revision,
		}
		return nil
	})
	return result, err
}

func readTelegramConversationBindingTx(
	ctx context.Context,
	tx *stateTx,
	conversation domain.ConversationRef,
) (domain.FrontendBinding, bool, error) {
	var indexedBindingID domain.FrontendBindingID
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT binding_id FROM frontend_binding_keys
		 WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3`,
		tx.tenantID, conversation.Frontend, conversation.ExternalID,
	).Scan(&indexedBindingID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return domain.FrontendBinding{}, false, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		indexedBindingID = telegramBindingID(conversation.ID)
	}
	binding, found, err := readBindingTx(ctx, tx, indexedBindingID)
	if err != nil || !found {
		return binding, found, err
	}
	if binding.TenantID != tx.tenantID || binding.Frontend != conversation.Frontend ||
		binding.ExternalConversationID != conversation.ExternalID {
		return domain.FrontendBinding{}, false, ErrBindingConflict
	}
	return binding, true, nil
}

func telegramUserID(actorID domain.ActorID) domain.UserID {
	return domain.UserID(stableCanonicalID("usr_", string(actorID)))
}

func telegramBindingID(conversationID domain.ConversationID) domain.FrontendBindingID {
	return domain.FrontendBindingID(stableCanonicalID("fbd_", string(conversationID)))
}

func telegramInitialSessionID(conversationID domain.ConversationID) domain.SessionID {
	return domain.SessionID(stableCanonicalID("ses_", "initial:"+string(conversationID)))
}

func stableCanonicalID(prefix, material string) string {
	digest := sha256.Sum256([]byte(material))
	return fmt.Sprintf("%s%x", prefix, digest[:16])
}

func setSchedulerSlotState(
	ctx context.Context,
	tx *stateTx,
	connectionID domain.SubscriptionConnectionID,
	state domain.SchedulerState,
	blockedUntil *time.Time,
	at time.Time,
) error {
	blocked := time.Unix(0, 0).UTC()
	if blockedUntil != nil {
		blocked = *blockedUntil
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`UPDATE subscription_scheduler_slots
		 SET state = $1, blocked_until = $2, updated_at = $3
		 WHERE tenant_id = $4 AND subscription_connection_id = $5`,
		state, blocked, at, tx.tenantID, connectionID,
	)
	return err
}

func validateTelegramIdentity(request ports.TelegramIdentityRequest) error {
	if err := request.TenantID.Validate(); err != nil {
		return err
	}
	if err := request.Actor.Validate(); err != nil {
		return err
	}
	if err := request.Conversation.Validate(); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Actor.TenantID); err != nil {
		return err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Conversation.TenantID); err != nil {
		return err
	}
	if err := request.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("provider", request.Provider); err != nil {
		return err
	}
	if request.ObservedAt.IsZero() {
		return domain.ValidationError{Field: "observed_at", Reason: "must not be zero"}
	}
	return nil
}

type LeaseClaim struct {
	TenantID  domain.TenantID
	RunID     domain.RunID
	AttemptID domain.AttemptID
	LeaseID   domain.LeaseID
	WorkerID  string
	Now       time.Time
	ExpiresAt time.Time
}

// ClaimLease uses the tenant/run lease head as the contention point. YDB's
// serializable conflict retry makes concurrent claimers re-read the winning
// head; exactly one distinct lease can remain active.
func (store *Store) ClaimLease(
	ctx context.Context,
	claim LeaseClaim,
) (result domain.Lease, err error) {
	if err := validateLeaseClaim(claim); err != nil {
		return result, err
	}
	err = store.Transact(ctx, claim.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var currentLeaseID, currentAttemptID, currentWorker string
		var fence uint64
		var expiresAt time.Time
		queryErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT lease_id, attempt_id, worker_id, fence_token, expires_at
			 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
			claim.TenantID, claim.RunID,
		).Scan(&currentLeaseID, &currentAttemptID, &currentWorker, &fence, &expiresAt)
		switch {
		case queryErr == nil && currentLeaseID == string(claim.LeaseID) &&
			expiresAt.After(claim.Now):
			lease, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
				`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
				claim.TenantID, claim.LeaseID,
			)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("lease head %q has no lease row", currentLeaseID)
			}
			result = lease
			return nil
		case queryErr == nil && expiresAt.After(claim.Now):
			return fmt.Errorf(
				"%w: lease=%s worker=%s attempt=%s expires_at=%s",
				ErrLeaseHeld, currentLeaseID, currentWorker, currentAttemptID,
				expiresAt.Format(time.RFC3339),
			)
		case queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows):
			return queryErr
		}
		if queryErr == nil {
			bucket, err := ydbpartition.BucketV1(string(claim.RunID))
			if err != nil {
				return err
			}
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM lease_expiry
				 WHERE tenant_id = $1 AND expires_at = $2 AND run_id = $3`,
				claim.TenantID, expiresAt, claim.RunID,
			); err != nil {
				return err
			}
			if _, err := tx.sqlTx.ExecContext(ctx,
				`DELETE FROM lease_expiry_v2
				 WHERE shard_bucket = $1 AND expires_at = $2
				 AND tenant_id = $3 AND run_id = $4`,
				bucket, expiresAt, claim.TenantID, claim.RunID,
			); err != nil {
				return err
			}
		}
		result = domain.Lease{
			ID:         claim.LeaseID,
			TenantID:   claim.TenantID,
			RunID:      claim.RunID,
			AttemptID:  claim.AttemptID,
			WorkerID:   claim.WorkerID,
			FenceToken: fence + 1,
			AcquiredAt: claim.Now,
			ExpiresAt:  claim.ExpiresAt,
		}
		if err := state.PutLease(ctx, result); err != nil {
			return err
		}
		_, err := tx.sqlTx.ExecContext(ctx,
			`UPSERT INTO lease_heads
			 (tenant_id, run_id, lease_id, attempt_id, worker_id,
			  fence_token, expires_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			result.TenantID, result.RunID, result.ID, result.AttemptID,
			result.WorkerID, result.FenceToken, result.ExpiresAt, claim.Now,
		)
		return err
	})
	return result, err
}

func (store *Store) RenewLease(
	ctx context.Context,
	tenantID domain.TenantID,
	leaseID domain.LeaseID,
	fence uint64,
	now time.Time,
	newExpiry time.Time,
) (result domain.Lease, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, err
	}
	if err := leaseID.Validate(); err != nil {
		return result, err
	}
	if fence == 0 || now.IsZero() || !newExpiry.After(now) {
		return result, domain.ValidationError{
			Field: "lease renewal", Reason: "requires a positive fence and future expiry",
		}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		lease, found, err := readJSON[domain.Lease](ctx, tx.sqlTx,
			`SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`,
			tenantID, leaseID,
		)
		if err != nil {
			return err
		}
		if !found {
			return ErrLeaseLost
		}
		var currentLeaseID string
		var currentFence uint64
		var currentExpiry time.Time
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT lease_id, fence_token, expires_at
			 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
			tenantID, lease.RunID,
		).Scan(&currentLeaseID, &currentFence, &currentExpiry); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrLeaseLost
			}
			return err
		}
		if currentLeaseID != string(leaseID) || currentFence != fence || !currentExpiry.After(now) {
			return ErrLeaseLost
		}
		lease.ExpiresAt = newExpiry
		if err := state.PutLease(ctx, lease); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`UPDATE lease_heads SET expires_at = $1, updated_at = $2
			 WHERE tenant_id = $3 AND run_id = $4
			 AND lease_id = $5 AND fence_token = $6`,
			newExpiry, now, tenantID, lease.RunID, leaseID, fence,
		); err != nil {
			return err
		}
		result = lease
		return nil
	})
	return result, err
}

func (store *Store) TransitionQuotaReservation(
	ctx context.Context,
	tenantID domain.TenantID,
	reservationID domain.QuotaReservationID,
	to domain.ReservationStatus,
	at time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		reservation, found, err := readJSON[domain.QuotaReservation](ctx, tx.sqlTx,
			`SELECT payload FROM quota_reservations
			 WHERE tenant_id = $1 AND quota_reservation_id = $2`,
			tenantID, reservationID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("quota reservation %q not found", reservationID)
		}
		if err := reservation.Transition(to, at); err != nil {
			return err
		}
		return state.PutQuotaReservation(ctx, reservation)
	})
}

func (store *Store) AcknowledgeDispatch(
	ctx context.Context,
	tenantID domain.TenantID,
	outboxID domain.DispatchOutboxID,
	at time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		outbox, found, err := readJSON[domain.DispatchOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM dispatch_outbox
			 WHERE tenant_id = $1 AND dispatch_outbox_id = $2`,
			tenantID, outboxID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("dispatch outbox %q not found", outboxID)
		}
		if outbox.Status == domain.DispatchPublished {
			return nil
		}
		if err := outbox.Transition(domain.DispatchPublished, at); err != nil {
			return err
		}
		return state.PutDispatchOutbox(ctx, outbox)
	})
}

func (store *Store) TransitionTelegramDelivery(
	ctx context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	to domain.DeliveryStatus,
	at time.Time,
	retryAt *time.Time,
) error {
	return store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		delivery, found, err := readJSON[domain.TelegramDeliveryOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM telegram_delivery_outbox
			 WHERE tenant_id = $1 AND telegram_delivery_id = $2`,
			tenantID, deliveryID,
		)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("Telegram delivery %q not found", deliveryID)
		}
		if delivery.Status == to {
			return nil
		}
		if err := delivery.Transition(to, at, retryAt); err != nil {
			return err
		}
		return state.PutTelegramDeliveryOutbox(ctx, delivery)
	})
}

func (store *Store) ListReadyTelegramDeliveries(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	limit uint64,
) (result []ports.TelegramDeliveryReady, err error) {
	if bucket >= ydbpartition.BucketCountV1 {
		return nil, domain.ValidationError{
			Field: "Telegram delivery bucket", Reason: "must be within the v1 bucket range",
		}
	}
	if before.IsZero() || limit == 0 {
		return nil, domain.ValidationError{
			Field: "Telegram delivery listing", Reason: "requires a non-zero time and positive limit",
		}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT tenant_id, telegram_delivery_id
		 FROM telegram_delivery_ready_v2
		 WHERE shard_bucket = $1 AND available_at <= $2
		 ORDER BY available_at, tenant_id, telegram_delivery_id
		 LIMIT $3`,
		bucket, before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ports.TelegramDeliveryReady
		if err := rows.Scan(&item.TenantID, &item.DeliveryID); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *Store) GetTelegramDelivery(
	ctx context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
) (result domain.TelegramDeliveryOutbox, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, false, err
	}
	if err := deliveryID.Validate(); err != nil {
		return result, false, err
	}
	result, found, err = readJSON[domain.TelegramDeliveryOutbox](ctx, store.db,
		`SELECT payload FROM telegram_delivery_outbox
		 WHERE tenant_id = $1 AND telegram_delivery_id = $2`,
		tenantID, deliveryID,
	)
	return result, found, err
}

func (store *Store) ClaimTelegramDelivery(
	ctx context.Context,
	tenantID domain.TenantID,
	deliveryID domain.TelegramDeliveryID,
	at time.Time,
) (result domain.TelegramDeliveryOutbox, claimed bool, err error) {
	if at.IsZero() {
		return result, false, domain.ValidationError{
			Field: "Telegram delivery claim time", Reason: "must not be zero",
		}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		delivery, found, err := readJSON[domain.TelegramDeliveryOutbox](ctx, tx.sqlTx,
			`SELECT payload FROM telegram_delivery_outbox
			 WHERE tenant_id = $1 AND telegram_delivery_id = $2`,
			tenantID, deliveryID,
		)
		if err != nil || !found {
			return err
		}
		switch delivery.Status {
		case domain.DeliveryPending, domain.DeliveryRetryWait:
			if delivery.NextAttemptAt != nil && delivery.NextAttemptAt.After(at) {
				return nil
			}
			if err := delivery.Transition(domain.DeliverySending, at, nil); err != nil {
				return err
			}
		case domain.DeliverySending:
			if delivery.UpdatedAt.Add(telegramDeliveryClaimTimeout).After(at) {
				return nil
			}
			delivery.AttemptCount++
			delivery.UpdatedAt = at
		default:
			return nil
		}
		if err := state.PutTelegramDeliveryOutbox(ctx, delivery); err != nil {
			return err
		}
		result, claimed = delivery, true
		return nil
	})
	return result, claimed, err
}

func (store *Store) GetArtifactManifest(
	ctx context.Context,
	tenantID domain.TenantID,
	manifestID domain.ArtifactManifestID,
) (result domain.ArtifactManifest, found bool, err error) {
	if err := tenantID.Validate(); err != nil {
		return result, false, err
	}
	if err := manifestID.Validate(); err != nil {
		return result, false, err
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		result, found, err = readJSON[domain.ArtifactManifest](
			ctx, state.(*stateTx).sqlTx,
			`SELECT payload FROM artifact_manifests
			 WHERE tenant_id = $1 AND artifact_manifest_id = $2`,
			tenantID, manifestID,
		)
		return err
	})
	return result, found, err
}

type ExpiredLease struct {
	TenantID  domain.TenantID
	RunID     domain.RunID
	LeaseID   domain.LeaseID
	Fence     uint64
	ExpiresAt time.Time
}

// ListExpiredLeasesByBucket is the scheduler-facing bounded global traversal.
// A complete pass fans out over exactly BucketCountV1 buckets and never scans
// arbitrary tenant prefixes.
func (store *Store) ListExpiredLeasesByBucket(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	limit uint64,
) (result []ExpiredLease, err error) {
	if bucket >= ydbpartition.BucketCountV1 {
		return nil, domain.ValidationError{
			Field: "lease expiry bucket", Reason: "must be within the v1 bucket range",
		}
	}
	if limit == 0 {
		return nil, domain.ValidationError{Field: "lease expiry limit", Reason: "must be positive"}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT tenant_id, run_id, lease_id, fence_token, expires_at
		 FROM lease_expiry_v2
		 WHERE shard_bucket = $1 AND expires_at <= $2
		 ORDER BY expires_at, tenant_id, run_id
		 LIMIT $3`,
		bucket, before, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item ExpiredLease
		if err := rows.Scan(
			&item.TenantID, &item.RunID, &item.LeaseID, &item.Fence, &item.ExpiresAt,
		); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// ListExpiredLeases uses the tenant/time primary-key range; it never scans
// payloads or another tenant's rows.
func (store *Store) ListExpiredLeases(
	ctx context.Context,
	tenantID domain.TenantID,
	before time.Time,
	limit uint64,
) (result []ExpiredLease, err error) {
	if limit == 0 {
		return nil, domain.ValidationError{Field: "lease expiry limit", Reason: "must be positive"}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		rows, err := tx.sqlTx.QueryContext(ctx,
			`SELECT run_id, lease_id, fence_token, expires_at
			 FROM lease_expiry
			 WHERE tenant_id = $1 AND expires_at <= $2
			 ORDER BY expires_at, run_id
			 LIMIT $3`,
			tenantID, before, limit,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item ExpiredLease
			if err := rows.Scan(&item.RunID, &item.LeaseID, &item.Fence, &item.ExpiresAt); err != nil {
				return err
			}
			item.TenantID = tenantID
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

type CompletedRun struct {
	Run      domain.Run
	LeaseID  domain.LeaseID
	Fence    uint64
	At       time.Time
	Manifest domain.ArtifactManifest
	Delivery domain.TelegramDeliveryOutbox
	Usage    []domain.UsageObservation
}

// CompleteRun persists the terminal run, immutable result manifest, usage
// observations, and delivery outbox in one transaction.
func (store *Store) CompleteRun(ctx context.Context, result CompletedRun) error {
	return store.Transact(ctx, result.Run.TenantID, func(state ports.StateTx) error {
		if err := requireLeaseOwnership(
			ctx, state.(*stateTx), result.Run.ID, result.LeaseID, result.Fence, result.At,
		); err != nil {
			return err
		}
		if err := state.PutRun(ctx, result.Run); err != nil {
			return err
		}
		for _, observation := range result.Usage {
			if err := state.AppendUsageObservation(ctx, observation); err != nil {
				return err
			}
		}
		if err := state.PutArtifactManifest(ctx, result.Manifest); err != nil {
			return err
		}
		return state.PutTelegramDeliveryOutbox(ctx, result.Delivery)
	})
}

func (store *Store) SaveCheckpoint(
	ctx context.Context,
	checkpoint domain.Checkpoint,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
) error {
	return store.Transact(ctx, checkpoint.TenantID, func(state ports.StateTx) error {
		if err := requireLeaseOwnership(
			ctx, state.(*stateTx), checkpoint.RunID, leaseID, fence, at,
		); err != nil {
			return err
		}
		return state.PutCheckpoint(ctx, checkpoint)
	})
}

func validateLeaseClaim(claim LeaseClaim) error {
	if err := claim.TenantID.Validate(); err != nil {
		return err
	}
	if err := claim.RunID.Validate(); err != nil {
		return err
	}
	if err := claim.AttemptID.Validate(); err != nil {
		return err
	}
	if err := claim.LeaseID.Validate(); err != nil {
		return err
	}
	if err := domain.ValidateOpaqueID("lease.worker_id", claim.WorkerID); err != nil {
		return err
	}
	if claim.Now.IsZero() || !claim.ExpiresAt.After(claim.Now) {
		return domain.ValidationError{
			Field: "lease.expires_at", Reason: "must follow a non-zero claim time",
		}
	}
	return nil
}

func requireLeaseOwnership(
	ctx context.Context,
	tx *stateTx,
	runID domain.RunID,
	leaseID domain.LeaseID,
	fence uint64,
	at time.Time,
) error {
	if err := leaseID.Validate(); err != nil {
		return err
	}
	if fence == 0 || at.IsZero() {
		return domain.ValidationError{
			Field: "lease ownership", Reason: "requires a positive fence and non-zero time",
		}
	}
	var currentLeaseID string
	var currentFence uint64
	var expiresAt time.Time
	if err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT lease_id, fence_token, expires_at
		 FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`,
		tx.tenantID, runID,
	).Scan(&currentLeaseID, &currentFence, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrLeaseLost
		}
		return err
	}
	if currentLeaseID != string(leaseID) || currentFence != fence || !expiresAt.After(at) {
		return ErrLeaseLost
	}
	return nil
}
