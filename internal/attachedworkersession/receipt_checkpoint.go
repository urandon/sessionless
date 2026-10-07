package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

type receiptCheckpointLease interface {
	PersistReceiptCheckpoint(context.Context, attachedworkerlocal.ReceiptCheckpointV1) error
	LoadReceiptCheckpoint(context.Context) (attachedworkerlocal.ReceiptCheckpointV1, error)
	RetireReceiptCheckpoint(context.Context, attachedworkerlocal.ReceiptCheckpointV1) error
}

// SealReceiptSubmission stores the exact bounded post-run submission through
// the session's exclusive local runtime lease. No receipt request may cross
// the network if this seal fails or conflicts with an earlier outcome.
func (session *Session) SealReceiptSubmission(ctx context.Context, payload []byte) error {
	return session.withReceiptCheckpoint(ctx, payload, false)
}

// RetireReceiptSubmission removes only the exact sealed submission after a
// verified TerminalAck. A failed retirement leaves the restart fence intact.
func (session *Session) RetireReceiptSubmission(ctx context.Context, payload []byte) error {
	return session.withReceiptCheckpoint(ctx, payload, true)
}

func (session *Session) withReceiptCheckpoint(ctx context.Context, payload []byte, retire bool) error {
	if session == nil || ctx == nil || ctx.Err() != nil || len(payload) == 0 || len(payload) > 128<<10 {
		return ErrInvalidConfiguration
	}
	if err := session.acquire(ctx); err != nil {
		return err
	}
	defer session.release()
	snapshot := session.Snapshot()
	if snapshot.State != StateReady || snapshot.ConnectionID.Validate() != nil {
		return ErrSessionFenced
	}
	session.mu.Lock()
	lease, revision := session.lease, session.manifestRevision
	session.mu.Unlock()
	journal, ok := lease.(receiptCheckpointLease)
	if !ok || revision == 0 {
		return ErrReconciliationRequired
	}
	local, err := lease.LoadSnapshot(ctx)
	if err != nil {
		return errors.Join(ErrReconciliationRequired, err)
	}
	manifest := local.Manifest
	if manifest.Revision != revision || manifest.TenantID != snapshot.TenantID ||
		manifest.OwnerUserID != snapshot.OwnerUserID || manifest.WorkerID != snapshot.WorkerID ||
		manifest.EnrollmentGeneration != snapshot.EnrollmentGeneration ||
		manifest.ConnectionGeneration != snapshot.ConnectionGeneration {
		return ErrReconciliationRequired
	}
	digest := sha256.Sum256(payload)
	checkpoint := attachedworkerlocal.ReceiptCheckpointV1{
		Version:          attachedworkerlocal.ReceiptCheckpointVersionV1,
		ManifestRevision: revision, TenantID: snapshot.TenantID,
		OwnerUserID: snapshot.OwnerUserID, WorkerID: snapshot.WorkerID,
		EnrollmentGeneration: snapshot.EnrollmentGeneration,
		ConnectionGeneration: snapshot.ConnectionGeneration,
		ConnectionID:         snapshot.ConnectionID,
		PayloadSHA256:        hex.EncodeToString(digest[:]), Payload: bytes.Clone(payload),
	}
	defer clearBytes(checkpoint.Payload)
	if retire {
		return journal.RetireReceiptCheckpoint(ctx, checkpoint)
	}
	return journal.PersistReceiptCheckpoint(ctx, checkpoint)
}
