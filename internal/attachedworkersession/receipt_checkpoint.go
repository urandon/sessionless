package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
)

type receiptCheckpointLease interface {
	PersistReceiptCheckpoint(context.Context, attachedworkerlocal.ReceiptCheckpointV1) error
	LoadReceiptCheckpoint(context.Context) (attachedworkerlocal.ReceiptCheckpointV1, error)
	RetireReceiptCheckpoint(context.Context, attachedworkerlocal.ReceiptCheckpointV1) error
}

type inlineReceiptCheckpointLeaseV2 interface {
	PersistInlineReceiptCheckpointV2(context.Context, attachedworkerlocal.ReceiptCheckpointV1) error
	AdvanceInlineReceiptCheckpointV2(context.Context, attachedworkerlocal.ReceiptCheckpointV1, attachedworkerlocal.ReceiptCheckpointV1) (bool, error)
}

// SealInlineReceiptSubmissionV2 seals only an initial compact commitment. Its
// immutable plan reserves the whole original export; private live-only tokens,
// not successful idempotent sealing, decide whether a first send may occur.
func (session *Session) SealInlineReceiptSubmissionV2(ctx context.Context, payload []byte) error {
	value, err := attachedworkeroutput.DecodeInlineCheckpointV2(payload)
	if err != nil || value.Progress.Revision != 1 {
		return ErrInvalidConfiguration
	}
	_, err = session.changeReceiptCheckpoint(ctx, payload, nil, false, true)
	return err
}

// AdvanceInlineReceiptSubmissionV2 changes only monotonic progress attached to
// the exact initial plan. A false result is recovery evidence, never a fresh
// Prepare/body/reconcile token. Ambiguous failure remains fenced.
func (session *Session) AdvanceInlineReceiptSubmissionV2(ctx context.Context, previous, next []byte) (bool, error) {
	before, err := attachedworkeroutput.DecodeInlineCheckpointV2(previous)
	if err != nil {
		return false, ErrInvalidConfiguration
	}
	after, err := attachedworkeroutput.DecodeInlineCheckpointV2(next)
	if err != nil || after.AdvanceFrom(before) != nil {
		return false, ErrInvalidConfiguration
	}
	return session.changeReceiptCheckpoint(ctx, next, previous, false, true)
}

// RetireInlineReceiptSubmissionV2 accepts only a compact record whose ACK fact
// was durably recorded. The original adapter must have verified that exact ACK;
// this metadata bit is not itself product or terminal authority.
func (session *Session) RetireInlineReceiptSubmissionV2(ctx context.Context, payload []byte) error {
	value, err := attachedworkeroutput.DecodeInlineCheckpointV2(payload)
	if err != nil || !value.Progress.TerminalAcked {
		return ErrInvalidConfiguration
	}
	_, err = session.changeReceiptCheckpoint(ctx, payload, nil, true, true)
	return err
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
	_, err := session.changeReceiptCheckpoint(ctx, payload, nil, retire, false)
	return err
}

func (session *Session) changeReceiptCheckpoint(ctx context.Context, payload, previous []byte, retire, inlineV2 bool) (bool, error) {
	if session == nil || ctx == nil || ctx.Err() != nil || len(payload) == 0 || len(payload) > 128<<10 {
		return false, ErrInvalidConfiguration
	}
	if err := session.acquire(ctx); err != nil {
		return false, err
	}
	defer session.release()
	snapshot := session.Snapshot()
	if snapshot.State != StateReady || snapshot.ConnectionID.Validate() != nil {
		return false, ErrSessionFenced
	}
	session.mu.Lock()
	lease, revision := session.lease, session.manifestRevision
	session.mu.Unlock()
	journal, ok := lease.(receiptCheckpointLease)
	if !ok || revision == 0 {
		return false, ErrReconciliationRequired
	}
	local, err := lease.LoadSnapshot(ctx)
	if err != nil {
		return false, errors.Join(ErrReconciliationRequired, err)
	}
	manifest := local.Manifest
	if manifest.Revision != revision || manifest.TenantID != snapshot.TenantID ||
		manifest.OwnerUserID != snapshot.OwnerUserID || manifest.WorkerID != snapshot.WorkerID ||
		manifest.EnrollmentGeneration != snapshot.EnrollmentGeneration ||
		manifest.ConnectionGeneration != snapshot.ConnectionGeneration {
		return false, ErrReconciliationRequired
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
	if inlineV2 {
		value, err := attachedworkeroutput.DecodeInlineCheckpointV2(payload)
		if err != nil {
			return false, ErrInvalidConfiguration
		}
		a := value.Plan.Authorization
		if a.TenantID != snapshot.TenantID || a.OwnerUserID != snapshot.OwnerUserID || a.WorkerID != snapshot.WorkerID ||
			a.ConnectionID != snapshot.ConnectionID || a.EnrollmentGeneration != snapshot.EnrollmentGeneration || a.ConnectionGeneration != snapshot.ConnectionGeneration ||
			a.CapabilityDigest != snapshot.CapabilityDigest {
			return false, ErrSessionFenced
		}
	}
	if previous != nil {
		advance, ok := lease.(inlineReceiptCheckpointLeaseV2)
		if !ok {
			return false, ErrReconciliationRequired
		}
		expected := checkpoint
		expected.Payload = bytes.Clone(previous)
		defer clearBytes(expected.Payload)
		digest := sha256.Sum256(previous)
		expected.PayloadSHA256 = hex.EncodeToString(digest[:])
		return advance.AdvanceInlineReceiptCheckpointV2(ctx, expected, checkpoint)
	}
	if retire {
		return false, journal.RetireReceiptCheckpoint(ctx, checkpoint)
	}
	if inlineV2 {
		inline, ok := lease.(inlineReceiptCheckpointLeaseV2)
		if !ok {
			return false, ErrReconciliationRequired
		}
		return false, inline.PersistInlineReceiptCheckpointV2(ctx, checkpoint)
	}
	return false, journal.PersistReceiptCheckpoint(ctx, checkpoint)
}
