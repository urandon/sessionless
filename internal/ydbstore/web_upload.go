package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func (store *Store) CreateWebUploadIntent(
	ctx context.Context,
	request ports.WebUploadCreateRequest,
) (result domain.UploadIntent, created bool, err error) {
	if err := validateWebUploadCreate(request); err != nil {
		return result, false, err
	}
	err = store.Transact(ctx, request.Intent.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.Intent.UserID); err != nil {
			return err
		}
		if err := authorizeSessionForUserTx(ctx, tx, request.Intent.SessionID, request.Intent.UserID, true); err != nil {
			return err
		}

		var existingID domain.UploadIntentID
		lookupErr := tx.sqlTx.QueryRowContext(ctx,
			`SELECT upload_id FROM web_upload_intent_creations
			 WHERE tenant_id = $1 AND user_id = $2 AND creation_idempotency_key = $3`,
			request.Intent.TenantID, request.Intent.UserID, request.IdempotencyKey,
		).Scan(&existingID)
		if lookupErr == nil {
			existing, found, err := readWebUploadIntentTx(ctx, tx, existingID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("web upload creation references missing intent %q", existingID)
			}
			if !sameUploadCreation(existing, request.Intent) {
				return domain.ErrUploadIntentConflict
			}
			result, created = existing, false
			return nil
		}
		if !errors.Is(lookupErr, sql.ErrNoRows) {
			return lookupErr
		}
		if _, found, err := readWebUploadIntentTx(ctx, tx, request.Intent.ID); err != nil {
			return err
		} else if found {
			return domain.ErrUploadIntentConflict
		}
		if err := writeWebUploadIntentTx(ctx, tx, request.Intent, request.IdempotencyKey); err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`INSERT INTO web_upload_intent_creations
			 (tenant_id, user_id, creation_idempotency_key, upload_id, created_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			request.Intent.TenantID, request.Intent.UserID, request.IdempotencyKey,
			request.Intent.ID, request.Intent.CreatedAt,
		); err != nil {
			return err
		}
		result, created = request.Intent, true
		return nil
	})
	return result, created, err
}

func (store *Store) CommitWebUploadIntent(
	ctx context.Context,
	request ports.WebUploadCommitRequest,
) (result domain.UploadIntent, err error) {
	if err := validateWebUploadCommit(request); err != nil {
		return result, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		intent, found, err := readWebUploadIntentTx(ctx, tx, request.UploadID)
		if err != nil {
			return err
		}
		if !found || intent.UserID != request.UserID {
			return domain.ErrMembershipDenied
		}
		if err := authorizeSessionForUserTx(ctx, tx, intent.SessionID, request.UserID, true); err != nil {
			return err
		}
		if intent.Status == domain.UploadIntentCommitted {
			if uploadObservedMatches(intent, request.Observed) {
				result = intent
				return nil
			}
			return domain.ErrUploadIntentConflict
		}
		if err := intent.RecordObservedMetadata(
			request.Observed.Blob, request.Observed.MediaType, request.Observed.ETag, request.At,
		); err != nil {
			return err
		}
		if err := writeWebUploadIntentTx(ctx, tx, intent, ""); err != nil {
			return err
		}
		result = intent
		return nil
	})
	return result, err
}

func (store *Store) ClaimWebUploadIntents(
	ctx context.Context,
	request ports.WebUploadClaimRequest,
) (result []domain.UploadIntent, err error) {
	if err := validateWebUploadClaim(request); err != nil {
		return nil, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeTenantWriteTx(ctx, tx, request.UserID); err != nil {
			return err
		}
		if err := authorizeSessionForUserTx(ctx, tx, request.SessionID, request.UserID, true); err != nil {
			return err
		}
		claimed := make([]domain.UploadIntent, 0, len(request.UploadIDs))
		for _, uploadID := range request.UploadIDs {
			intent, found, err := readWebUploadIntentTx(ctx, tx, uploadID)
			if err != nil {
				return err
			}
			if !found || intent.UserID != request.UserID || intent.SessionID != request.SessionID {
				return domain.ErrMembershipDenied
			}
			if err := intent.Claim(request.MessageIdempotencyKey, request.At); err != nil {
				return err
			}
			claimed = append(claimed, intent)
		}
		for _, intent := range claimed {
			if err := writeWebUploadIntentTx(ctx, tx, intent, ""); err != nil {
				return err
			}
		}
		result = claimed
		return nil
	})
	return result, err
}

func (store *Store) GetRunForUser(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	runID domain.RunID,
) (record ports.RunRecord, found bool, err error) {
	for _, validationErr := range []error{tenantID.Validate(), userID.Validate(), runID.Validate()} {
		if validationErr != nil {
			return record, false, validationErr
		}
	}
	err = store.Transact(ctx, tenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		run, exists, err := readJSON[domain.Run](ctx, tx.sqlTx,
			`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenantID, runID)
		if err != nil || !exists {
			return err
		}
		if err := authorizeSessionForUserTx(ctx, tx, run.SessionID, userID, false); err != nil {
			return err
		}
		provider, err := readRunProviderTx(ctx, tx, run)
		if err != nil {
			return err
		}
		record, found = ports.RunRecord{Run: run, Provider: provider}, true
		return nil
	})
	return record, found, err
}

func (store *Store) ResolveComputeConnectionsForUser(
	ctx context.Context,
	request ports.ComputeConnectionResolveRequest,
) (result []ports.ComputeConnectionState, err error) {
	if err := validateSessionAPIIdentity(request.TenantID, request.UserID, request.SessionID); err != nil {
		return nil, err
	}
	err = store.Transact(ctx, request.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		if err := authorizeSessionForUserTx(ctx, tx, request.SessionID, request.UserID, true); err != nil {
			return err
		}
		rows, err := tx.sqlTx.QueryContext(ctx,
			`SELECT subscription_connection_id, provider, entitlement_state, quota_state, observed_at
			 FROM subscription_connections
			 WHERE tenant_id = $1
			 ORDER BY subscription_connection_id ASC LIMIT 2`,
			request.TenantID,
		)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var item ports.ComputeConnectionState
			if err := rows.Scan(&item.ID, &item.Provider, &item.Entitlement, &item.Quota, &item.ObservedAt); err != nil {
				return err
			}
			if err := validateComputeConnectionState(item); err != nil {
				return err
			}
			result = append(result, item)
		}
		return rows.Err()
	})
	return result, err
}

func readWebUploadIntentTx(
	ctx context.Context,
	tx *stateTx,
	uploadID domain.UploadIntentID,
) (domain.UploadIntent, bool, error) {
	return readJSON[domain.UploadIntent](ctx, tx.sqlTx,
		`SELECT record FROM web_upload_intents WHERE tenant_id = $1 AND upload_id = $2`,
		tx.tenantID, uploadID,
	)
}

func writeWebUploadIntentTx(
	ctx context.Context,
	tx *stateTx,
	intent domain.UploadIntent,
	creationKey domain.IdempotencyKey,
) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	if err := tx.validateTenant(intent.TenantID); err != nil {
		return err
	}
	if creationKey == "" {
		if err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT creation_idempotency_key FROM web_upload_intents
			 WHERE tenant_id = $1 AND upload_id = $2`,
			intent.TenantID, intent.ID,
		).Scan(&creationKey); err != nil {
			return err
		}
	}
	payload, err := marshal(intent)
	if err != nil {
		return err
	}
	claimedBy := ""
	if intent.ClaimedBy != nil {
		claimedBy = string(*intent.ClaimedBy)
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO web_upload_intents
		 (tenant_id, upload_id, user_id, session_id, creation_idempotency_key,
		  status, expires_at, claimed_by_message_idempotency_key, record)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))`,
		intent.TenantID, intent.ID, intent.UserID, intent.SessionID, creationKey,
		intent.Status, intent.ExpiresAt, claimedBy, payload,
	)
	return err
}

func validateWebUploadCreate(request ports.WebUploadCreateRequest) error {
	if err := request.Intent.Validate(); err != nil {
		return err
	}
	if err := request.IdempotencyKey.Validate(); err != nil {
		return err
	}
	if request.Intent.Status != domain.UploadIntentPending || request.Intent.CommittedAt != nil ||
		request.Intent.ObservedBlob != nil || request.Intent.ClaimedBy != nil {
		return domain.ValidationError{Field: "upload_intent.status", Reason: "creation requires a pristine pending intent"}
	}
	return nil
}

func validateWebUploadCommit(request ports.WebUploadCommitRequest) error {
	for _, err := range []error{
		request.TenantID.Validate(), request.UserID.Validate(), request.UploadID.Validate(),
		request.Observed.Blob.Validate(),
	} {
		if err != nil {
			return err
		}
	}
	if request.Observed.Blob.TenantID != request.TenantID {
		return domain.TenantMismatchError{Expected: request.TenantID, Actual: request.Observed.Blob.TenantID}
	}
	if strings.TrimSpace(request.Observed.MediaType) == "" || strings.TrimSpace(request.Observed.ETag) == "" {
		return domain.ValidationError{Field: "upload_intent.observed_metadata", Reason: "media type and ETag are required"}
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "upload_intent.committed_at", Reason: "must not be zero"}
	}
	return nil
}

func validateWebUploadClaim(request ports.WebUploadClaimRequest) error {
	if err := validateSessionAPIIdentity(request.TenantID, request.UserID, request.SessionID); err != nil {
		return err
	}
	if err := request.MessageIdempotencyKey.Validate(); err != nil {
		return err
	}
	if request.At.IsZero() {
		return domain.ValidationError{Field: "upload_intent.claimed_at", Reason: "must not be zero"}
	}
	if len(request.UploadIDs) == 0 || len(request.UploadIDs) > ports.MaxWebMessageUploads {
		return domain.ValidationError{Field: "upload_intent.ids", Reason: "must contain between 1 and 8 IDs"}
	}
	seen := make(map[domain.UploadIntentID]struct{}, len(request.UploadIDs))
	for _, uploadID := range request.UploadIDs {
		if err := uploadID.Validate(); err != nil {
			return err
		}
		if _, exists := seen[uploadID]; exists {
			return domain.ValidationError{Field: "upload_intent.ids", Reason: "must not contain duplicates"}
		}
		seen[uploadID] = struct{}{}
	}
	return nil
}

func uploadObservedMatches(intent domain.UploadIntent, observed ports.ObjectMetadata) bool {
	return intent.ObservedBlob != nil && *intent.ObservedBlob == observed.Blob &&
		intent.ObservedMediaType == observed.MediaType && intent.ObservedETag == observed.ETag
}

func sameUploadCreation(left, right domain.UploadIntent) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID && left.UserID == right.UserID &&
		left.SessionID == right.SessionID && left.ObjectKey == right.ObjectKey && left.Name == right.Name &&
		left.MediaType == right.MediaType && left.ExpectedSize == right.ExpectedSize &&
		left.ExpectedSHA256 == right.ExpectedSHA256 && right.Status == domain.UploadIntentPending
}

func validateComputeConnectionState(state ports.ComputeConnectionState) error {
	if err := state.ID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(state.Provider) == "" {
		return domain.ValidationError{Field: "compute_connection.provider", Reason: "must not be empty"}
	}
	if !state.Entitlement.Valid() {
		return domain.ValidationError{Field: "compute_connection.entitlement", Reason: "is unknown"}
	}
	if !state.Quota.Valid() {
		return domain.ValidationError{Field: "compute_connection.quota", Reason: "is unknown"}
	}
	if state.ObservedAt.IsZero() {
		return domain.ValidationError{Field: "compute_connection.observed_at", Reason: "must not be zero"}
	}
	return nil
}

var _ ports.WebUploadStore = (*Store)(nil)
var _ ports.WebResourceStore = (*Store)(nil)
