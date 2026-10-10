package ydbstore

import (
	"context"
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

func readExplanationAttachedTx(ctx context.Context, tx *stateTx, snapshot *runexplanation.Snapshot) error {
	head := snapshot.Head
	owner, workerID := head.Placement.OwnerUserID, head.Placement.WorkerID
	query := explanationQueryTx(tx)
	// #9 bounded receipt. It has only typed locators/digests/counters, no
	// WorkerJob, terminal body or secret. Reject replacement identity before
	// reading any of its related sources.
	receipt, found, err := readExplanationJSON[domain.AttachedWorkerAttemptV1](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM attached_worker_attempt_heads WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, tx.tenantID, owner, workerID)
	if err != nil || !found {
		return err
	}
	if receipt.RunID != snapshot.Run.ID || receipt.AttemptID != head.SelectedAttemptID || receipt.TenantID != tx.tenantID || receipt.OwnerUserID != owner || receipt.WorkerID != workerID || receipt.LeaseID.Validate() != nil {
		return nil
	}
	snapshot.Attached = &receipt
	// #10 exact canonical lease; the pure join checks numeric generation and
	// expiry against this source, not an opaque protocol token or timestamp TTL.
	lease, found, err := readExplanationJSON[domain.Lease](ctx, query, `SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM leases WHERE tenant_id=$1 AND lease_id=$2`, tx.tenantID, receipt.LeaseID)
	if err != nil || !found {
		return err
	}
	snapshot.Lease = &lease
	// #11 bounded worker columns. Exclude identity key/display/record.
	worker := domain.AttachedWorker{TenantID: tx.tenantID, OwnerUserID: owner, ID: workerID}
	err = query.QueryRowContext(ctx, `SELECT enrollment_generation,connection_generation,desired_state,observed_state,revision,created_at,updated_at FROM attached_workers WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, tx.tenantID, owner, workerID).Scan(&worker.EnrollmentGeneration, &worker.ConnectionGeneration, &worker.DesiredState, &worker.ObservedState, &worker.Revision, &worker.CreatedAt, &worker.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	snapshot.Worker = &worker
	// #12 bounded current connection columns. No secret, signature, channel
	// binding, manifest or presence/ACK update is read or written.
	connection := domain.AttachedWorkerConnection{TenantID: tx.tenantID, OwnerUserID: owner, WorkerID: workerID}
	err = query.QueryRowContext(ctx, `SELECT connection_id,enrollment_generation,connection_generation,revision,state,connected_at,last_checkpoint_at,manifest_observed_at FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, tx.tenantID, owner, workerID).Scan(&connection.ID, &connection.EnrollmentGeneration, &connection.ConnectionGeneration, &connection.Revision, &connection.State, &connection.ConnectedAt, &connection.LastCheckpointAt, &connection.ManifestObservedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	snapshot.Connection = &connection
	return nil
}
