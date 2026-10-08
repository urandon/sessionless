package attachedworkerlocal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

const maxReceiptCheckpointPayloadBytes = 128 << 10

func (checkpoint ReceiptCheckpointV1) Validate(manifest ManifestV1) error {
	if manifest.Validate() != nil || manifest.Lifecycle != LifecycleActive ||
		checkpoint.Version != ReceiptCheckpointVersionV1 ||
		checkpoint.ManifestRevision != manifest.Revision ||
		checkpoint.TenantID != manifest.TenantID || checkpoint.OwnerUserID != manifest.OwnerUserID ||
		checkpoint.WorkerID != manifest.WorkerID ||
		checkpoint.EnrollmentGeneration != manifest.EnrollmentGeneration ||
		checkpoint.ConnectionGeneration != manifest.ConnectionGeneration ||
		checkpoint.ConnectionID.Validate() != nil ||
		len(checkpoint.Payload) == 0 || len(checkpoint.Payload) > maxReceiptCheckpointPayloadBytes ||
		!hexDigestPattern.MatchString(checkpoint.PayloadSHA256) {
		return ErrInvalidState
	}
	digest := sha256.Sum256(checkpoint.Payload)
	if checkpoint.PayloadSHA256 != hex.EncodeToString(digest[:]) {
		return ErrInvalidState
	}
	return nil
}

// PersistReceiptCheckpoint is called through the one runtime lease before a
// receipt network request. It never overwrites a divergent or ambiguous prior
// submission. A crash after this write leaves evidence for a safe recovery or
// an explicit fence, never implicit provider re-execution.
func (lease *RuntimeLease) PersistReceiptCheckpoint(ctx context.Context, next ReceiptCheckpointV1) (resultErr error) {
	if lease == nil || lease.file == nil || lease.store == nil || ctx == nil || ctx.Err() != nil {
		return ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	manifest, err := store.loadConsistentLocked()
	if err != nil {
		return err
	}
	if err := next.Validate(manifest); err != nil {
		return err
	}
	prior, found, err := store.loadReceiptCheckpointOptionalLocked(manifest)
	if err != nil {
		return err
	}
	if found {
		if prior.Version != next.Version || prior.ManifestRevision != next.ManifestRevision ||
			prior.TenantID != next.TenantID || prior.OwnerUserID != next.OwnerUserID ||
			prior.WorkerID != next.WorkerID || prior.EnrollmentGeneration != next.EnrollmentGeneration ||
			prior.ConnectionGeneration != next.ConnectionGeneration || prior.ConnectionID != next.ConnectionID ||
			prior.PayloadSHA256 != next.PayloadSHA256 || !bytes.Equal(prior.Payload, next.Payload) {
			return ErrStateConflict
		}
		return nil
	}
	encoded, err := encodeStrictJSON(next)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.writeAtomic(ReceiptCheckpointFileName, encoded)
}

func (lease *RuntimeLease) LoadReceiptCheckpoint(ctx context.Context) (ReceiptCheckpointV1, error) {
	if lease == nil || lease.file == nil || lease.store == nil || ctx == nil || ctx.Err() != nil {
		return ReceiptCheckpointV1{}, ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return ReceiptCheckpointV1{}, err
	}
	defer lock.Close()
	manifest, err := store.loadConsistentLocked()
	if err != nil {
		return ReceiptCheckpointV1{}, err
	}
	checkpoint, found, err := store.loadReceiptCheckpointOptionalLocked(manifest)
	if err != nil {
		return ReceiptCheckpointV1{}, err
	}
	if !found {
		return ReceiptCheckpointV1{}, ErrStateMissing
	}
	return checkpoint, nil
}

func (lease *RuntimeLease) RetireReceiptCheckpoint(ctx context.Context, expected ReceiptCheckpointV1) (resultErr error) {
	if lease == nil || lease.file == nil || lease.store == nil || ctx == nil || ctx.Err() != nil {
		return ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	manifest, err := store.loadConsistentLocked()
	if err != nil || expected.Validate(manifest) != nil {
		if err != nil {
			return err
		}
		return ErrInvalidState
	}
	prior, found, err := store.loadReceiptCheckpointOptionalLocked(manifest)
	if err != nil {
		return err
	}
	if !found || prior.PayloadSHA256 != expected.PayloadSHA256 || !bytes.Equal(prior.Payload, expected.Payload) ||
		prior.ConnectionID != expected.ConnectionID || prior.ConnectionGeneration != expected.ConnectionGeneration {
		return ErrStateConflict
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.removeDurable(ReceiptCheckpointFileName)
}

func (store *Store) loadReceiptCheckpointOptionalLocked(manifest ManifestV1) (ReceiptCheckpointV1, bool, error) {
	present, err := store.filePresentSecure(ReceiptCheckpointFileName)
	if err != nil || !present {
		return ReceiptCheckpointV1{}, present, err
	}
	encoded, err := store.readFileSecure(ReceiptCheckpointFileName)
	if err != nil {
		return ReceiptCheckpointV1{}, false, err
	}
	var checkpoint ReceiptCheckpointV1
	if decodeStrictJSON(encoded, &checkpoint) != nil || checkpoint.Validate(manifest) != nil {
		return ReceiptCheckpointV1{}, false, ErrInvalidState
	}
	return checkpoint, true, nil
}
