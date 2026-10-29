package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

const quotaReservationStorageSQLV1 = `SELECT SUBSTRING(CAST(payload AS String),0,32769),
 SUBSTRING(CAST(quota_reservation_id AS String),0,161), SUBSTRING(CAST(tenant_id AS String),0,161),
 SUBSTRING(CAST(run_id AS String),0,161), SUBSTRING(CAST(subscription_connection_id AS String),0,161),
 SUBSTRING(CAST(status AS String),0,17), capacity_units, held_at, expires_at, retention_expire_at
 FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`

const quotaExpiryStorageSQLV1 = `SELECT shard_bucket, expires_at,
 SUBSTRING(CAST(tenant_id AS String),0,161), SUBSTRING(CAST(quota_reservation_id AS String),0,161),
 SUBSTRING(CAST(run_id AS String),0,161), SUBSTRING(CAST(subscription_connection_id AS String),0,161),
 SUBSTRING(local_capacity_association,0,32769), SUBSTRING(CAST(local_capacity_digest AS String),0,65)
 FROM quota_expiry_v2 WHERE shard_bucket=$1 AND expires_at=$2 AND tenant_id=$3 AND quota_reservation_id=$4`

// Point-read the complete original reservation and its physical scalars in the
// caller's transaction. This establishes retained correspondence, not funding,
// current resource authority, a fresh hold or permission to execute.
func readQuotaReservationStorageV1(ctx context.Context, tx *stateTx, run domain.Run, id domain.QuotaReservationID) (domain.QuotaReservation, bool, error) {
	deny := func() (domain.QuotaReservation, bool, error) {
		return domain.QuotaReservation{}, false, domain.ErrMembershipDenied
	}
	if tx == nil || tx.sqlTx == nil || run.Validate() != nil || run.TenantID != tx.tenantID || id.Validate() != nil {
		return deny()
	}
	var raw, rowID, tenant, runID, subscription, status string
	var units uint32
	var held, expires, retained time.Time
	destinations := []any{&raw, &rowID, &tenant, &runID, &subscription, &status, &units, &held, &expires, &retained}
	var err error
	if q, ok := tx.sqlTx.(*ownedAuthorityQuery); ok {
		err = q.scanMetadata(ctx, quotaReservationStorageSQLV1, []any{run.TenantID, id}, destinations...)
	} else {
		_, err = scanOwnedCurrentRow(tx.sqlTx.QueryRowContext(ctx, quotaReservationStorageSQLV1, run.TenantID, id), destinations...)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return domain.QuotaReservation{}, false, nil
	}
	if err != nil || len(raw) > ownedAuthorityHeadBytes || len(rowID) > 160 || len(tenant) > 160 || len(runID) > 160 || len(subscription) > 160 || len(status) > 16 || retained.Before(expires) ||
		strictjson.ValidateDocument([]byte(raw), ownedAuthorityHeadBytes, 3, 16) != nil {
		return deny()
	}
	// JSONDocument may reorder/format members. Reject unknown/case aliases and
	// duplicate fields without imposing the physical String codec on this JSON.
	var members map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &members) != nil {
		return deny()
	}
	for key := range members {
		switch key {
		case "id", "tenant_id", "run_id", "subscription_connection_id", "local_capacity_association", "status", "capacity_units", "held_at", "expires_at", "updated_at":
		default:
			return deny()
		}
	}
	// Presence selects the original closed arm. Typed decoding alone erases a
	// forbidden null local field or null/empty subscription field; those are
	// not alternate producer forms even though JSONDocument may reorder keys.
	local, hasLocal := members["local_capacity_association"]
	subscriptionMember, hasSubscription := members["subscription_connection_id"]
	if run.LocalCapacityAssociation != nil {
		local = bytes.TrimSpace(local)
		if hasSubscription || !hasLocal || len(local) == 0 || local[0] != '{' {
			return deny()
		}
	} else {
		subscriptionMember = bytes.TrimSpace(subscriptionMember)
		if hasLocal || !hasSubscription || len(subscriptionMember) == 0 || subscriptionMember[0] != '"' {
			return deny()
		}
	}
	var reservation domain.QuotaReservation
	d := json.NewDecoder(bytes.NewReader([]byte(raw)))
	d.DisallowUnknownFields()
	if d.Decode(&reservation) != nil || reservation.ValidateForRun(run) != nil || reservation.ID != id ||
		rowID != string(reservation.ID) || tenant != string(reservation.TenantID) || runID != string(reservation.RunID) || subscription != string(reservation.SubscriptionConnectionID) ||
		status != string(reservation.Status) || units != reservation.CapacityUnits || !held.Equal(reservation.HeldAt) || !expires.Equal(reservation.ExpiresAt) {
		return deny()
	}
	return reservation.Clone(), true, nil
}

// Absence is returned distinctly; only the writer's explicit fresh/legacy arm
// may decide whether it is acceptable. Any present row must be the exact
// selected-arm projection of this original reservation, never a repair target.
func readQuotaExpiryStorageV1(ctx context.Context, tx *stateTx, reservation domain.QuotaReservation) (bool, error) {
	if tx == nil || tx.sqlTx == nil || reservation.TenantID != tx.tenantID || reservation.ID.Validate() != nil || reservation.RunID.Validate() != nil || reservation.ExpiresAt.IsZero() {
		return false, domain.ErrMembershipDenied
	}
	want, err := encodeCapacityStorageV1(reservation.TenantID, reservation.SubscriptionConnectionID, reservation.LocalCapacityAssociation)
	if err != nil {
		return false, err
	}
	bucket, err := ydbpartition.BucketV1(string(reservation.ID))
	if err != nil {
		return false, err
	}
	var rowBucket uint32
	var expires time.Time
	var tenant, id, run string
	var stored capacityStorageV1
	destinations := []any{&rowBucket, &expires, &tenant, &id, &run, &stored.key, &stored.association, &stored.digest}
	if q, ok := tx.sqlTx.(*ownedAuthorityQuery); ok {
		err = q.scanMetadata(ctx, quotaExpiryStorageSQLV1, []any{bucket, reservation.ExpiresAt, reservation.TenantID, reservation.ID}, destinations...)
	} else {
		_, err = scanOwnedCurrentRow(tx.sqlTx.QueryRowContext(ctx, quotaExpiryStorageSQLV1, bucket, reservation.ExpiresAt, reservation.TenantID, reservation.ID), destinations...)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil || rowBucket != bucket || !expires.Equal(reservation.ExpiresAt) || tenant != string(reservation.TenantID) || id != string(reservation.ID) || run != string(reservation.RunID) || stored != want {
		return false, domain.ErrMembershipDenied
	}
	if _, _, err := decodeCapacityStorageV1(reservation.TenantID, stored); err != nil {
		return false, err
	}
	return true, nil
}

func validateQuotaReservationUpdateV1(previous, next domain.QuotaReservation) error {
	if previous.ID != next.ID || previous.TenantID != next.TenantID || previous.RunID != next.RunID || previous.SubscriptionConnectionID != next.SubscriptionConnectionID ||
		previous.CapacityUnits != next.CapacityUnits || !previous.HeldAt.Equal(next.HeldAt) || !equalLocalCapacityV1(previous.LocalCapacityAssociation, next.LocalCapacityAssociation) {
		return domain.ErrMembershipDenied
	}
	if previous.LocalCapacityAssociation == nil {
		return nil // Preserve delivered subscription extension/history semantics.
	}
	if !previous.ExpiresAt.Equal(next.ExpiresAt) || next.UpdatedAt.Before(previous.UpdatedAt) {
		return domain.ErrMembershipDenied
	}
	if previous.Status == next.Status {
		if !previous.UpdatedAt.Equal(next.UpdatedAt) {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	if !domain.CanTransitionReservation(previous.Status, next.Status) {
		return domain.ErrMembershipDenied
	}
	return nil
}
