package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const maxProviderCredentialCleanupLimit uint64 = 64

var (
	ErrProviderCredentialConflict = errors.New("provider credential conflicts with durable authority")
	ErrProviderCredentialNotFound = errors.New("provider credential is not available")
)

func (store *Store) LoadProviderCredential(
	ctx context.Context,
	locator ports.ProviderCredentialLocatorV1,
) (domain.ProviderCredentialBindingV1, bool, error) {
	if err := locator.Validate(); err != nil {
		return domain.ProviderCredentialBindingV1{}, false, err
	}
	return readProviderCredential(ctx, store.db, locator)
}

func (store *Store) CompareAndSwapProviderCredential(
	ctx context.Context,
	expectedRevision uint64,
	next domain.ProviderCredentialBindingV1,
) (result ports.ProviderCredentialSwapV1, err error) {
	next = canonicalProviderCredential(next)
	if err := validateProviderCredentialCAS(expectedRevision, next); err != nil {
		return result, err
	}
	locator := providerCredentialLocator(next)
	err = store.Transact(ctx, next.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readProviderCredential(ctx, tx.sqlTx, locator)
		if err != nil {
			return err
		}
		if found && sameProviderCredentialBinding(current, next) {
			audit, err := domain.NewProviderCredentialAuditEventV1(current, providerCredentialCASAuditAction(current))
			if err != nil {
				return err
			}
			if err := requireProviderCredentialAuditTx(ctx, tx, audit); err != nil {
				return err
			}
			result = ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: current, AuditReceiptID: audit.ReceiptID}
			return nil
		}
		if (!found && expectedRevision != 0) || (found && current.ResourceRevision != expectedRevision) {
			result = ports.ProviderCredentialSwapV1{}
			return nil
		}
		fenced, err := providerCredentialCandidateFencedTx(ctx, tx, locator, next.CandidateMutationID)
		if err != nil {
			return err
		}
		if fenced {
			result = ports.ProviderCredentialSwapV1{}
			return nil
		}
		if err := validateProviderCredentialTransition(current, found, expectedRevision, next); err != nil {
			return err
		}
		if found && current.State == domain.ProviderCredentialActiveV1 {
			if err := insertProviderCredentialCleanupTx(ctx, tx, providerCredentialCleanup(current), next.UpdatedAt); err != nil {
				return err
			}
		}
		if err := upsertProviderCredentialTx(ctx, tx, next); err != nil {
			return err
		}
		audit, err := domain.NewProviderCredentialAuditEventV1(next, providerCredentialCASAuditAction(next))
		if err != nil {
			return err
		}
		if err := insertProviderCredentialAuditTx(ctx, tx, audit); err != nil {
			return err
		}
		result = ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: next, AuditReceiptID: audit.ReceiptID}
		return nil
	})
	return result, err
}

func (store *Store) FenceProviderCredentialCandidate(ctx context.Context, candidate ports.ProviderCredentialSecretCandidateV1, at time.Time) (result ports.ProviderCredentialCandidateFenceV1, err error) {
	if err := validateProviderCredentialCandidate(candidate); err != nil {
		return result, err
	}
	at = canonicalProviderCredentialTime(at)
	if at.IsZero() {
		return result, domain.ValidationError{Field: "provider_credential.candidate_fenced_at", Reason: "must not be zero"}
	}
	err = store.Transact(ctx, candidate.Scope.Locator.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readProviderCredential(ctx, tx.sqlTx, candidate.Scope.Locator)
		if err != nil {
			return err
		}
		if found && providerCredentialBindingMatchesCandidate(current, candidate) {
			result = ports.ProviderCredentialCandidateFenceV1{Authoritative: true, Binding: current}
			return nil
		}
		return upsertProviderCredentialCandidateFenceTx(ctx, tx, candidate, at)
	})
	return result, err
}

func (store *Store) RevokeProviderCredential(
	ctx context.Context,
	locator ports.ProviderCredentialLocatorV1,
	at time.Time,
) (result ports.ProviderCredentialSwapV1, err error) {
	if err := locator.Validate(); err != nil {
		return result, err
	}
	at = canonicalProviderCredentialTime(at)
	if at.IsZero() {
		return result, domain.ValidationError{Field: "provider_credential.revoke_at", Reason: "must not be zero"}
	}
	err = store.Transact(ctx, locator.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		current, found, err := readProviderCredential(ctx, tx.sqlTx, locator)
		if err != nil {
			return err
		}
		if !found {
			result = ports.ProviderCredentialSwapV1{Found: false}
			return nil
		}
		if current.State == domain.ProviderCredentialRevokedV1 {
			audit, err := domain.NewProviderCredentialAuditEventV1(current, domain.ProviderCredentialAuditRevokedV1)
			if err != nil {
				return err
			}
			if err := requireProviderCredentialAuditTx(ctx, tx, audit); err != nil {
				return err
			}
			result = ports.ProviderCredentialSwapV1{Applied: false, Found: true, Binding: current, AuditReceiptID: audit.ReceiptID}
			return nil
		}
		if current.ResourceRevision == math.MaxUint64 || current.CredentialGeneration == math.MaxUint64 {
			return ErrProviderCredentialConflict
		}
		if at.Before(current.UpdatedAt) {
			return ErrProviderCredentialConflict
		}
		next := current
		next.ResourceRevision++
		next.CredentialGeneration++
		next.State = domain.ProviderCredentialRevokedV1
		next.SecretRef = domain.CredentialSecretRef{}
		next.SecretFingerprint = ""
		next.UpdatedAt = at
		if err := next.Validate(); err != nil {
			return err
		}
		if err := insertProviderCredentialCleanupTx(ctx, tx, providerCredentialCleanup(current), at); err != nil {
			return err
		}
		if err := upsertProviderCredentialTx(ctx, tx, next); err != nil {
			return err
		}
		audit, err := domain.NewProviderCredentialAuditEventV1(next, domain.ProviderCredentialAuditRevokedV1)
		if err != nil {
			return err
		}
		if err := insertProviderCredentialAuditTx(ctx, tx, audit); err != nil {
			return err
		}
		result = ports.ProviderCredentialSwapV1{Applied: true, Found: true, Binding: next, AuditReceiptID: audit.ReceiptID}
		return nil
	})
	return result, err
}

func (store *Store) ListProviderCredentialCleanups(
	ctx context.Context,
	locator ports.ProviderCredentialLocatorV1,
	limit uint64,
) ([]ports.ProviderCredentialCleanupV1, error) {
	if err := locator.Validate(); err != nil {
		return nil, err
	}
	if limit == 0 || limit > maxProviderCredentialCleanupLimit {
		return nil, domain.ValidationError{Field: "provider_credential.cleanup_limit", Reason: "must be between 1 and 64"}
	}
	rows, err := store.db.QueryContext(ctx,
		`SELECT credential_generation, secret_ref, created_at
		 FROM provider_credential_cleanups
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4
		 ORDER BY credential_generation ASC, secret_ref ASC LIMIT $5`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ports.ProviderCredentialCleanupV1, 0, limit)
	for rows.Next() {
		var generation uint64
		var rawReference string
		var createdAt time.Time
		if err := rows.Scan(&generation, &rawReference, &createdAt); err != nil {
			return nil, err
		}
		ref, err := domain.NewCredentialSecretRef(rawReference)
		if err != nil || generation == 0 || createdAt.IsZero() {
			return nil, ErrProviderCredentialConflict
		}
		result = append(result, ports.ProviderCredentialCleanupV1{
			Locator: locator, CredentialGeneration: generation, Reference: ref,
		})
	}
	return result, rows.Err()
}

func (store *Store) AcknowledgeProviderCredentialCleanup(
	ctx context.Context,
	cleanup ports.ProviderCredentialCleanupV1,
) error {
	if err := validateProviderCredentialCleanup(cleanup); err != nil {
		return err
	}
	return store.Transact(ctx, cleanup.Locator.TenantID, func(state ports.StateTx) error {
		tx := state.(*stateTx)
		var createdAt time.Time
		err := tx.sqlTx.QueryRowContext(ctx,
			`SELECT created_at FROM provider_credential_cleanups
			 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4
			 AND credential_generation=$5 AND secret_ref=$6`,
			cleanup.Locator.TenantID, cleanup.Locator.OwnerUserID, cleanup.Locator.ResourceKind,
			cleanup.Locator.ResourceID, cleanup.CredentialGeneration, cleanup.Reference.StorageValue(),
		).Scan(&createdAt)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.sqlTx.ExecContext(ctx,
			`DELETE FROM provider_credential_cleanups
			 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4
			 AND credential_generation=$5 AND secret_ref=$6`,
			cleanup.Locator.TenantID, cleanup.Locator.OwnerUserID, cleanup.Locator.ResourceKind,
			cleanup.Locator.ResourceID, cleanup.CredentialGeneration, cleanup.Reference.StorageValue(),
		); err != nil {
			return err
		}
		bucket, err := providerCredentialCleanupBucket(cleanup.Locator)
		if err != nil {
			return err
		}
		_, err = tx.sqlTx.ExecContext(ctx,
			`DELETE FROM provider_credential_cleanup_ready_v1
			 WHERE shard_bucket=$1 AND created_at=$2 AND tenant_id=$3 AND owner_user_id=$4
			 AND resource_kind=$5 AND resource_id=$6 AND credential_generation=$7 AND secret_ref=$8`,
			bucket, canonicalProviderCredentialTime(createdAt), cleanup.Locator.TenantID, cleanup.Locator.OwnerUserID,
			cleanup.Locator.ResourceKind, cleanup.Locator.ResourceID, cleanup.CredentialGeneration, cleanup.Reference.StorageValue(),
		)
		return err
	})
}

func (store *Store) ListDueProviderCredentialCleanups(
	ctx context.Context,
	bucket uint32,
	before time.Time,
	cursor ports.ProviderCredentialCleanupCursorV1,
	limit uint64,
) (ports.ProviderCredentialCleanupPageV1, error) {
	if bucket >= ydbpartition.BucketCountV1 || before.IsZero() || limit == 0 || limit > maxProviderCredentialCleanupLimit {
		return ports.ProviderCredentialCleanupPageV1{}, domain.ValidationError{Field: "provider_credential.cleanup_ready", Reason: "requires a valid bucket, timestamp, and bounded positive limit"}
	}
	if err := validateProviderCredentialCleanupCursor(cursor); err != nil {
		return ports.ProviderCredentialCleanupPageV1{}, err
	}
	before = canonicalProviderCredentialTime(before)
	var rows *sql.Rows
	var err error
	const selectColumns = `SELECT created_at,tenant_id,owner_user_id,resource_kind,resource_id,credential_generation,secret_ref
		 FROM provider_credential_cleanup_ready_v1`
	if !cursor.Present {
		rows, err = store.db.QueryContext(ctx, selectColumns+`
		 WHERE shard_bucket=$1 AND created_at <= $2
		 ORDER BY created_at,tenant_id,owner_user_id,resource_kind,resource_id,credential_generation,secret_ref LIMIT $3`, bucket, before, limit)
	} else {
		rows, err = store.db.QueryContext(ctx, selectColumns+`
		 WHERE shard_bucket=$1 AND created_at <= $2 AND
		 (created_at > $3
		 OR (created_at=$3 AND tenant_id > $4)
		 OR (created_at=$3 AND tenant_id=$4 AND owner_user_id > $5)
		 OR (created_at=$3 AND tenant_id=$4 AND owner_user_id=$5 AND resource_kind > $6)
		 OR (created_at=$3 AND tenant_id=$4 AND owner_user_id=$5 AND resource_kind=$6 AND resource_id > $7)
		 OR (created_at=$3 AND tenant_id=$4 AND owner_user_id=$5 AND resource_kind=$6 AND resource_id=$7 AND credential_generation > $8)
		 OR (created_at=$3 AND tenant_id=$4 AND owner_user_id=$5 AND resource_kind=$6 AND resource_id=$7 AND credential_generation=$8 AND secret_ref > $9))
		 ORDER BY created_at,tenant_id,owner_user_id,resource_kind,resource_id,credential_generation,secret_ref LIMIT $10`,
			bucket, before, canonicalProviderCredentialTime(cursor.CreatedAt), cursor.TenantID, cursor.OwnerUserID,
			cursor.ResourceKind, cursor.ResourceID, cursor.CredentialGeneration, cursor.Reference.StorageValue(), limit)
	}
	if err != nil {
		return ports.ProviderCredentialCleanupPageV1{}, err
	}
	defer rows.Close()
	page := ports.ProviderCredentialCleanupPageV1{Items: make([]ports.ProviderCredentialCleanupItemV1, 0, limit)}
	var scanned uint64
	for rows.Next() {
		var createdAt time.Time
		var tenantID, ownerUserID, resourceKind, resourceID, rawReference string
		var generation uint64
		if err := rows.Scan(&createdAt, &tenantID, &ownerUserID, &resourceKind, &resourceID, &generation, &rawReference); err != nil {
			return ports.ProviderCredentialCleanupPageV1{}, err
		}
		scanned++
		ref, err := domain.NewCredentialSecretRef(rawReference)
		if err != nil {
			return ports.ProviderCredentialCleanupPageV1{}, ErrProviderCredentialConflict
		}
		cleanup := ports.ProviderCredentialCleanupV1{Locator: ports.ProviderCredentialLocatorV1{
			TenantID: domain.TenantID(tenantID), OwnerUserID: domain.UserID(ownerUserID), ResourceKind: domain.ProviderResourceKindV1(resourceKind), ResourceID: resourceID,
		}, CredentialGeneration: generation, Reference: ref}
		createdAt = canonicalProviderCredentialTime(createdAt)
		page.NextCursor = ports.ProviderCredentialCleanupCursorV1{Present: true, CreatedAt: createdAt,
			TenantID: cleanup.Locator.TenantID, OwnerUserID: cleanup.Locator.OwnerUserID, ResourceKind: cleanup.Locator.ResourceKind,
			ResourceID: cleanup.Locator.ResourceID, CredentialGeneration: cleanup.CredentialGeneration, Reference: cleanup.Reference}
		if validateProviderCredentialCleanup(cleanup) != nil || createdAt.IsZero() {
			page.SkippedInvalid++
			continue
		}
		page.Items = append(page.Items, ports.ProviderCredentialCleanupItemV1{Cleanup: cleanup, CreatedAt: createdAt})
	}
	if err := rows.Err(); err != nil {
		return ports.ProviderCredentialCleanupPageV1{}, err
	}
	page.HasMore = scanned == limit
	return page, nil
}

func readProviderCredential(
	ctx context.Context,
	query rowQuery,
	locator ports.ProviderCredentialLocatorV1,
) (domain.ProviderCredentialBindingV1, bool, error) {
	var resourceRevision, credentialGeneration uint64
	var candidateMutationID, state, secretRef, fingerprint, record string
	var updatedAt time.Time
	err := query.QueryRowContext(ctx,
		`SELECT resource_revision, credential_generation, candidate_mutation_id, state, secret_ref,
		        secret_fingerprint, updated_at, record
		 FROM provider_credential_bindings
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID,
	).Scan(&resourceRevision, &credentialGeneration, &candidateMutationID, &state, &secretRef, &fingerprint, &updatedAt, &record)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ProviderCredentialBindingV1{}, false, nil
	}
	if err != nil {
		return domain.ProviderCredentialBindingV1{}, false, err
	}
	var binding domain.ProviderCredentialBindingV1
	decoder := json.NewDecoder(strings.NewReader(record))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&binding); err != nil {
		return domain.ProviderCredentialBindingV1{}, false, fmt.Errorf("decode provider credential record: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.ProviderCredentialBindingV1{}, false, ErrProviderCredentialConflict
	}
	if secretRef != "" {
		binding.SecretRef, err = domain.NewCredentialSecretRef(secretRef)
		if err != nil {
			return domain.ProviderCredentialBindingV1{}, false, ErrProviderCredentialConflict
		}
	}
	if err := binding.Validate(); err != nil {
		return domain.ProviderCredentialBindingV1{}, false, err
	}
	if binding.TenantID != locator.TenantID || binding.OwnerUserID != locator.OwnerUserID ||
		binding.ResourceKind != locator.ResourceKind || binding.ResourceID != locator.ResourceID ||
		binding.ResourceRevision != resourceRevision || binding.CredentialGeneration != credentialGeneration ||
		binding.CandidateMutationID != candidateMutationID ||
		string(binding.State) != state || string(binding.SecretFingerprint) != fingerprint ||
		!binding.UpdatedAt.Equal(canonicalProviderCredentialTime(updatedAt)) ||
		binding.UpdatedAt != canonicalProviderCredentialTime(binding.UpdatedAt) ||
		(binding.State == domain.ProviderCredentialRevokedV1) != (secretRef == "") {
		return domain.ProviderCredentialBindingV1{}, false, ErrProviderCredentialConflict
	}
	return binding, true, nil
}

func upsertProviderCredentialTx(ctx context.Context, tx *stateTx, binding domain.ProviderCredentialBindingV1) error {
	record, err := marshal(binding)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO provider_credential_bindings
		 (tenant_id, owner_user_id, resource_kind, resource_id, resource_revision,
		  credential_generation, candidate_mutation_id, state, secret_ref, secret_fingerprint, updated_at, record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,CAST($12 AS JsonDocument))`,
		binding.TenantID, binding.OwnerUserID, binding.ResourceKind, binding.ResourceID,
		binding.ResourceRevision, binding.CredentialGeneration, binding.CandidateMutationID, binding.State,
		binding.SecretRef.StorageValue(), binding.SecretFingerprint, binding.UpdatedAt, record,
	)
	return err
}

func insertProviderCredentialAuditTx(ctx context.Context, tx *stateTx, event domain.ProviderCredentialAuditEventV1) error {
	if err := event.Validate(); err != nil {
		return err
	}
	if existing, found, err := readProviderCredentialAuditTx(ctx, tx, event.OwnerUserID, event.ResourceKind, event.ResourceID, event.ResourceRevision); err != nil {
		return err
	} else if found {
		if existing != event {
			return ErrProviderCredentialConflict
		}
		return nil
	}
	record, err := marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO provider_credential_audit_events
		 (tenant_id,owner_user_id,resource_kind,resource_id,resource_revision,candidate_mutation_id,receipt_id,action,occurred_at,record)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,CAST($10 AS JsonDocument))`,
		event.TenantID, event.OwnerUserID, event.ResourceKind, event.ResourceID, event.ResourceRevision,
		event.CandidateMutationID, event.ReceiptID, event.Action, event.OccurredAt, record)
	return err
}

func requireProviderCredentialAuditTx(ctx context.Context, tx *stateTx, expected domain.ProviderCredentialAuditEventV1) error {
	actual, found, err := readProviderCredentialAuditTx(ctx, tx, expected.OwnerUserID, expected.ResourceKind, expected.ResourceID, expected.ResourceRevision)
	if err != nil {
		return err
	}
	if !found || actual != expected {
		return ErrProviderCredentialConflict
	}
	return nil
}

func readProviderCredentialAuditTx(ctx context.Context, tx *stateTx, owner domain.UserID, kind domain.ProviderResourceKindV1, resourceID string, revision uint64) (domain.ProviderCredentialAuditEventV1, bool, error) {
	var candidateMutationID, receiptID, action, record string
	var occurredAt time.Time
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT candidate_mutation_id,receipt_id,action,occurred_at,record FROM provider_credential_audit_events
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND resource_revision=$5`,
		tx.tenantID, owner, kind, resourceID, revision).Scan(&candidateMutationID, &receiptID, &action, &occurredAt, &record)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ProviderCredentialAuditEventV1{}, false, nil
	}
	if err != nil {
		return domain.ProviderCredentialAuditEventV1{}, false, err
	}
	var event domain.ProviderCredentialAuditEventV1
	decoder := json.NewDecoder(strings.NewReader(record))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return domain.ProviderCredentialAuditEventV1{}, false, ErrProviderCredentialConflict
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.ProviderCredentialAuditEventV1{}, false, ErrProviderCredentialConflict
	}
	if err := event.Validate(); err != nil || event.TenantID != tx.tenantID || event.OwnerUserID != owner || event.ResourceKind != kind || event.ResourceID != resourceID || event.ResourceRevision != revision ||
		event.CandidateMutationID != candidateMutationID || string(event.ReceiptID) != receiptID || string(event.Action) != action || !event.OccurredAt.Equal(canonicalProviderCredentialTime(occurredAt)) || event.OccurredAt != canonicalProviderCredentialTime(event.OccurredAt) {
		return domain.ProviderCredentialAuditEventV1{}, false, ErrProviderCredentialConflict
	}
	return event, true, nil
}

func insertProviderCredentialCleanupTx(
	ctx context.Context,
	tx *stateTx,
	cleanup ports.ProviderCredentialCleanupV1,
	createdAt time.Time,
) error {
	if err := validateProviderCredentialCleanup(cleanup); err != nil {
		return err
	}
	createdAt = canonicalProviderCredentialTime(createdAt)
	if createdAt.IsZero() {
		return domain.ValidationError{Field: "provider_credential.cleanup_created_at", Reason: "must not be zero"}
	}
	_, err := tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO provider_credential_cleanups
		 (tenant_id, owner_user_id, resource_kind, resource_id, credential_generation, secret_ref, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		cleanup.Locator.TenantID, cleanup.Locator.OwnerUserID, cleanup.Locator.ResourceKind,
		cleanup.Locator.ResourceID, cleanup.CredentialGeneration, cleanup.Reference.StorageValue(), createdAt,
	)
	if err != nil {
		return err
	}
	bucket, err := providerCredentialCleanupBucket(cleanup.Locator)
	if err != nil {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`UPSERT INTO provider_credential_cleanup_ready_v1
		 (shard_bucket,created_at,tenant_id,owner_user_id,resource_kind,resource_id,credential_generation,secret_ref)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		bucket, createdAt, cleanup.Locator.TenantID, cleanup.Locator.OwnerUserID, cleanup.Locator.ResourceKind,
		cleanup.Locator.ResourceID, cleanup.CredentialGeneration, cleanup.Reference.StorageValue(),
	)
	return err
}

func validateProviderCredentialCAS(expectedRevision uint64, next domain.ProviderCredentialBindingV1) error {
	if err := next.Validate(); err != nil {
		return err
	}
	if next.State != domain.ProviderCredentialActiveV1 || expectedRevision == math.MaxUint64 || next.ResourceRevision != expectedRevision+1 {
		return domain.ValidationError{Field: "provider_credential.cas", Reason: "must advance one active resource revision"}
	}
	return nil
}

func validateProviderCredentialTransition(current domain.ProviderCredentialBindingV1, found bool, expectedRevision uint64, next domain.ProviderCredentialBindingV1) error {
	if !found {
		if expectedRevision != 0 || next.ResourceRevision != 1 || next.CredentialGeneration != 1 {
			return ErrProviderCredentialConflict
		}
		return nil
	}
	if current.TenantID != next.TenantID || current.OwnerUserID != next.OwnerUserID ||
		current.ResourceKind != next.ResourceKind || current.ResourceID != next.ResourceID ||
		current.ResourceRevision == math.MaxUint64 || current.CredentialGeneration == math.MaxUint64 ||
		next.ResourceRevision != current.ResourceRevision+1 || next.CredentialGeneration != current.CredentialGeneration+1 ||
		next.UpdatedAt.Before(current.UpdatedAt) {
		return ErrProviderCredentialConflict
	}
	return nil
}

func validateProviderCredentialCleanup(cleanup ports.ProviderCredentialCleanupV1) error {
	if err := cleanup.Locator.Validate(); err != nil {
		return err
	}
	if cleanup.CredentialGeneration == 0 {
		return domain.ValidationError{Field: "provider_credential.cleanup_generation", Reason: "must be positive"}
	}
	return cleanup.Reference.Validate()
}

func validateProviderCredentialCandidate(candidate ports.ProviderCredentialSecretCandidateV1) error {
	if err := candidate.Scope.Locator.Validate(); err != nil {
		return err
	}
	if candidate.Scope.ResourceRevision == 0 || candidate.Scope.CredentialGeneration == 0 || candidate.CreatedAt.IsZero() {
		return domain.ValidationError{Field: "provider_credential.candidate", Reason: "requires revisions and creation time"}
	}
	if err := domain.ValidateOpaqueID("provider_credential.candidate.mutation_id", candidate.Scope.MutationID); err != nil {
		return err
	}
	if err := candidate.Reference.Validate(); err != nil {
		return err
	}
	return candidate.Fingerprint.Validate()
}

func providerCredentialBindingMatchesCandidate(binding domain.ProviderCredentialBindingV1, candidate ports.ProviderCredentialSecretCandidateV1) bool {
	return binding.Validate() == nil && binding.State == domain.ProviderCredentialActiveV1 &&
		providerCredentialLocator(binding) == candidate.Scope.Locator && binding.ResourceRevision == candidate.Scope.ResourceRevision &&
		binding.CredentialGeneration == candidate.Scope.CredentialGeneration && binding.CandidateMutationID == candidate.Scope.MutationID &&
		binding.SecretRef == candidate.Reference && binding.SecretFingerprint == candidate.Fingerprint
}

func providerCredentialCandidateFencedTx(ctx context.Context, tx *stateTx, locator ports.ProviderCredentialLocatorV1, mutationID string) (bool, error) {
	var foundMutation string
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT mutation_id FROM provider_credential_candidate_fences
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND mutation_id=$5`,
		locator.TenantID, locator.OwnerUserID, locator.ResourceKind, locator.ResourceID, mutationID).Scan(&foundMutation)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if foundMutation != mutationID {
		return false, ErrProviderCredentialConflict
	}
	return true, nil
}

func upsertProviderCredentialCandidateFenceTx(ctx context.Context, tx *stateTx, candidate ports.ProviderCredentialSecretCandidateV1, at time.Time) error {
	var revision, generation uint64
	var reference, fingerprint string
	var createdAt, fencedAt time.Time
	err := tx.sqlTx.QueryRowContext(ctx,
		`SELECT resource_revision,credential_generation,secret_ref,secret_fingerprint,candidate_created_at,fenced_at
		 FROM provider_credential_candidate_fences
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND resource_kind=$3 AND resource_id=$4 AND mutation_id=$5`,
		candidate.Scope.Locator.TenantID, candidate.Scope.Locator.OwnerUserID, candidate.Scope.Locator.ResourceKind, candidate.Scope.Locator.ResourceID, candidate.Scope.MutationID,
	).Scan(&revision, &generation, &reference, &fingerprint, &createdAt, &fencedAt)
	if err == nil {
		if revision != candidate.Scope.ResourceRevision || generation != candidate.Scope.CredentialGeneration || reference != candidate.Reference.StorageValue() || fingerprint != string(candidate.Fingerprint) || !canonicalProviderCredentialTime(createdAt).Equal(canonicalProviderCredentialTime(candidate.CreatedAt)) || fencedAt.IsZero() {
			return ErrProviderCredentialConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	_, err = tx.sqlTx.ExecContext(ctx,
		`INSERT INTO provider_credential_candidate_fences
		 (tenant_id,owner_user_id,resource_kind,resource_id,mutation_id,resource_revision,credential_generation,secret_ref,secret_fingerprint,candidate_created_at,fenced_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		candidate.Scope.Locator.TenantID, candidate.Scope.Locator.OwnerUserID, candidate.Scope.Locator.ResourceKind, candidate.Scope.Locator.ResourceID,
		candidate.Scope.MutationID, candidate.Scope.ResourceRevision, candidate.Scope.CredentialGeneration, candidate.Reference.StorageValue(), candidate.Fingerprint,
		canonicalProviderCredentialTime(candidate.CreatedAt), at)
	return err
}

func validateProviderCredentialCleanupCursor(cursor ports.ProviderCredentialCleanupCursorV1) error {
	if !cursor.Present {
		if !cursor.CreatedAt.IsZero() || cursor.TenantID != "" || cursor.OwnerUserID != "" || cursor.ResourceKind != "" || cursor.ResourceID != "" || cursor.CredentialGeneration != 0 || !cursor.Reference.IsZero() {
			return domain.ValidationError{Field: "provider_credential.cleanup_cursor", Reason: "absent cursor must be zero"}
		}
		return nil
	}
	cleanup := ports.ProviderCredentialCleanupV1{Locator: ports.ProviderCredentialLocatorV1{
		TenantID: cursor.TenantID, OwnerUserID: cursor.OwnerUserID, ResourceKind: cursor.ResourceKind, ResourceID: cursor.ResourceID,
	}, CredentialGeneration: cursor.CredentialGeneration, Reference: cursor.Reference}
	if cursor.CreatedAt.IsZero() {
		return domain.ValidationError{Field: "provider_credential.cleanup_cursor", Reason: "present cursor requires a timestamp"}
	}
	return validateProviderCredentialCleanup(cleanup)
}

func providerCredentialCleanupBucket(locator ports.ProviderCredentialLocatorV1) (uint32, error) {
	if err := locator.Validate(); err != nil {
		return 0, err
	}
	return ydbpartition.BucketV1(strings.Join([]string{string(locator.TenantID), string(locator.OwnerUserID), string(locator.ResourceKind), locator.ResourceID}, "\x00"))
}

func providerCredentialCASAuditAction(binding domain.ProviderCredentialBindingV1) domain.ProviderCredentialAuditActionV1 {
	if binding.ResourceRevision == 1 {
		return domain.ProviderCredentialAuditIngestedV1
	}
	return domain.ProviderCredentialAuditRotatedV1
}

func providerCredentialCleanup(binding domain.ProviderCredentialBindingV1) ports.ProviderCredentialCleanupV1 {
	return ports.ProviderCredentialCleanupV1{
		Locator: providerCredentialLocator(binding), CredentialGeneration: binding.CredentialGeneration, Reference: binding.SecretRef,
	}
}

func providerCredentialLocator(binding domain.ProviderCredentialBindingV1) ports.ProviderCredentialLocatorV1 {
	return ports.ProviderCredentialLocatorV1{
		TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID,
		ResourceKind: binding.ResourceKind, ResourceID: binding.ResourceID,
	}
}

func canonicalProviderCredential(binding domain.ProviderCredentialBindingV1) domain.ProviderCredentialBindingV1 {
	binding.UpdatedAt = canonicalProviderCredentialTime(binding.UpdatedAt)
	return binding
}

func canonicalProviderCredentialTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Truncate(time.Microsecond)
}

func sameProviderCredentialBinding(left, right domain.ProviderCredentialBindingV1) bool {
	return left == right
}

var _ ports.ProviderCredentialBindingStore = (*Store)(nil)
