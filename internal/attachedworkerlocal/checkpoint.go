package attachedworkerlocal

import (
	"context"
	"errors"
	"math"
)

func (lease *RuntimeLease) LoadReconnectCheckpoint(ctx context.Context) (ReconnectCheckpointV1, error) {
	if lease == nil || lease.file == nil || lease.store == nil {
		return ReconnectCheckpointV1{}, ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return ReconnectCheckpointV1{}, err
	}
	defer lock.Close()
	if ctx == nil || ctx.Err() != nil {
		return ReconnectCheckpointV1{}, ErrInvalidState
	}
	manifest, err := store.loadConsistentLocked()
	if err != nil {
		return ReconnectCheckpointV1{}, err
	}
	return store.loadReconnectCheckpointLocked(manifest)
}

func (lease *RuntimeLease) PersistReconnectCheckpoint(
	ctx context.Context,
	expectedRevision uint64,
	next ReconnectCheckpointV1,
) (resultErr error) {
	if lease == nil || lease.file == nil || lease.store == nil || ctx == nil || ctx.Err() != nil ||
		expectedRevision == math.MaxUint64 {
		return ErrInvalidState
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	manifest, err := store.loadConsistentLocked()
	if err != nil || next.Validate(manifest) != nil || next.Revision != expectedRevision+1 {
		if err != nil {
			return err
		}
		return ErrStateConflict
	}
	current, present, err := store.loadReconnectCheckpointOptionalLocked(manifest)
	if err != nil || present != (expectedRevision != 0) || present && current.Revision != expectedRevision {
		if err != nil {
			return err
		}
		return ErrStateConflict
	}
	encoded, err := encodeStrictJSON(next)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return store.writeAtomic(ReconnectCheckpointFileName, encoded)
}

func (lease *RuntimeLease) RetireReconnectCheckpoint(ctx context.Context, expectedRevision uint64) (resultErr error) {
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
	current, present, err := store.loadReconnectCheckpointOptionalLocked(manifest)
	if err != nil {
		return err
	}
	if !present {
		if expectedRevision == 0 {
			return nil
		}
		return ErrStateConflict
	}
	if current.Revision != expectedRevision {
		return ErrStateConflict
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return store.removeDurable(ReconnectCheckpointFileName)
}

func (store *Store) loadReconnectCheckpointLocked(manifest ManifestV1) (ReconnectCheckpointV1, error) {
	checkpoint, present, err := store.loadReconnectCheckpointOptionalLocked(manifest)
	if err != nil {
		return ReconnectCheckpointV1{}, err
	}
	if !present {
		return ReconnectCheckpointV1{}, ErrStateMissing
	}
	return checkpoint, nil
}

func (store *Store) loadReconnectCheckpointOptionalLocked(manifest ManifestV1) (ReconnectCheckpointV1, bool, error) {
	present, err := store.filePresentSecure(ReconnectCheckpointFileName)
	if err != nil || !present {
		return ReconnectCheckpointV1{}, present, err
	}
	encoded, err := store.readFileSecure(ReconnectCheckpointFileName)
	if err != nil {
		return ReconnectCheckpointV1{}, false, err
	}
	var checkpoint ReconnectCheckpointV1
	if decodeStrictJSON(encoded, &checkpoint) != nil || checkpoint.Validate(manifest) != nil {
		return ReconnectCheckpointV1{}, false, ErrInvalidState
	}
	return checkpoint, true, nil
}
