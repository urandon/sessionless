package attachedworkerlocal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// PersistInlineReceiptCheckpointV2 selects the bounded V2 payload explicitly
// while retaining the existing outer file envelope and immutable V1 seal.
func (lease *RuntimeLease) PersistInlineReceiptCheckpointV2(ctx context.Context, next ReceiptCheckpointV1) error {
	value, err := decodeInlineReceiptCheckpointV2(next)
	if err != nil || value.Progress.Revision != 1 {
		return ErrInvalidState
	}
	return lease.PersistReceiptCheckpoint(ctx, next)
}

// AdvanceInlineReceiptCheckpointV2 is a closed monotonic progress CAS inside
// the existing private receipt file/runtime lease. The original immutable plan
// cannot change; V1 PersistReceiptCheckpoint still forbids divergent overwrite.
// applied is true only for this newly committed transition. An exact retry
// returns false: durable observation must never mint a second send token.
func (lease *RuntimeLease) AdvanceInlineReceiptCheckpointV2(ctx context.Context, expected, next ReceiptCheckpointV1) (applied bool, resultErr error) {
	if lease == nil || lease.file == nil || lease.store == nil || ctx == nil || ctx.Err() != nil {
		return false, ErrInvalidState
	}
	before, err := decodeInlineReceiptCheckpointV2(expected)
	if err != nil {
		return false, err
	}
	after, err := decodeInlineReceiptCheckpointV2(next)
	if err != nil || after.AdvanceFrom(before) != nil || !sameInlineCheckpointScopeV2(expected, next) {
		return false, ErrStateConflict
	}
	store := lease.store
	lock, err := store.acquireStateLock(false)
	if err != nil {
		return false, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, lock.Close())
		if resultErr != nil {
			applied = false
		}
	}()
	manifest, err := store.loadConsistentLocked()
	if err != nil {
		return false, err
	}
	if expected.Validate(manifest) != nil || next.Validate(manifest) != nil {
		return false, ErrInvalidState
	}
	prior, found, err := store.loadReceiptCheckpointOptionalLocked(manifest)
	if err != nil {
		return false, err
	}
	if !found || !sameInlineCheckpointScopeV2(prior, expected) {
		return false, ErrStateConflict
	}
	if prior.PayloadSHA256 == next.PayloadSHA256 && bytes.Equal(prior.Payload, next.Payload) {
		return false, nil // already observed, not newly acquired authority
	}
	if prior.PayloadSHA256 != expected.PayloadSHA256 || !bytes.Equal(prior.Payload, expected.Payload) {
		return false, ErrStateConflict
	}
	encoded, err := encodeInlineReceiptCheckpointFileV2(next)
	if err != nil {
		return false, err
	}
	defer clear(encoded)
	if len(encoded) > attachedworkeroutput.MaxInlineCheckpointBytesV2 {
		return false, ErrInvalidState
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if err := store.writeAtomic(ReceiptCheckpointFileName, encoded); err != nil {
		return false, err
	}
	return true, nil
}

func sameInlineCheckpointScopeV2(a, b ReceiptCheckpointV1) bool {
	return a.Version == b.Version && a.ManifestRevision == b.ManifestRevision &&
		a.TenantID == b.TenantID && a.OwnerUserID == b.OwnerUserID && a.WorkerID == b.WorkerID &&
		a.EnrollmentGeneration == b.EnrollmentGeneration && a.ConnectionGeneration == b.ConnectionGeneration && a.ConnectionID == b.ConnectionID
}

func decodeInlineReceiptCheckpointV2(record ReceiptCheckpointV1) (attachedworkeroutput.InlineReceiptCheckpointV2, error) {
	v, err := attachedworkeroutput.DecodeInlineCheckpointV2(record.Payload)
	if err != nil || record.Version != ReceiptCheckpointVersionV1 {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, ErrInvalidState
	}
	a := v.Plan.Authorization
	if a.TenantID != record.TenantID || a.OwnerUserID != record.OwnerUserID || a.WorkerID != record.WorkerID ||
		a.EnrollmentGeneration != record.EnrollmentGeneration || a.ConnectionGeneration != record.ConnectionGeneration || a.ConnectionID != record.ConnectionID {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, ErrInvalidState
	}
	return v, nil
}

// Legacy submissions have no top-level version discriminator. An explicitly
// versioned payload must use the closed V2 schema and ACK-gated retirement;
// callers cannot route a valid V2 plan through generic V1 retirement instead.
func validateInlineReceiptRetirementV2(payload []byte) error {
	value, versioned, err := decodeVersionedInlineReceiptV2(payload)
	if !versioned {
		return nil
	}
	if err != nil || !value.Progress.TerminalAcked {
		return ErrStateConflict
	}
	return nil
}

func validateInlineReceiptInitialV2(record ReceiptCheckpointV1) (bool, error) {
	value, versioned, err := decodeVersionedInlineReceiptV2(record.Payload)
	if !versioned {
		return false, nil
	}
	if err != nil || value.Progress.Revision != 1 {
		return true, ErrInvalidState
	}
	if _, err := decodeInlineReceiptCheckpointV2(record); err != nil {
		return true, err
	}
	return true, nil
}

func decodeVersionedInlineReceiptV2(payload []byte) (attachedworkeroutput.InlineReceiptCheckpointV2, bool, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, false, nil // inherited opaque V1
	}
	var versioned bool
	for name := range fields {
		if strings.EqualFold(name, "version") {
			versioned = true
		}
	}
	if !versioned {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, false, nil
	}
	value, err := attachedworkeroutput.DecodeInlineCheckpointV2(payload)
	return value, true, err
}

// V2 keeps the same outer fields/file, but its bounded base64 payload can
// exceed V1's 8192-byte scalar ceiling at valid metadata maxima. Select this
// closed codec only for an explicit V2 inner payload; never widen V1's codec.
func encodeInlineReceiptCheckpointFileV2(record ReceiptCheckpointV1) ([]byte, error) {
	if _, err := decodeInlineReceiptCheckpointV2(record); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(record)
	if err != nil || len(encoded)+1 > attachedworkeroutput.MaxInlineCheckpointBytesV2 {
		clear(encoded)
		return nil, ErrInvalidState
	}
	return append(encoded, '\n'), nil
}

func decodeInlineReceiptCheckpointFileV2(encoded []byte) (ReceiptCheckpointV1, bool, error) {
	var record ReceiptCheckpointV1
	if len(encoded) > attachedworkeroutput.MaxInlineCheckpointBytesV2 ||
		strictjson.ValidateDocument(encoded, attachedworkeroutput.MaxInlineCheckpointBytesV2, 12, 64) != nil {
		return record, false, nil // caller applies the unchanged V1 codec/dispatch
	}
	if json.Unmarshal(encoded, &record) != nil {
		return ReceiptCheckpointV1{}, false, nil
	}
	_, versioned, err := decodeVersionedInlineReceiptV2(record.Payload)
	if !versioned {
		return ReceiptCheckpointV1{}, false, nil
	}
	if err != nil {
		return ReceiptCheckpointV1{}, true, ErrInvalidState
	}
	canonical, err := encodeInlineReceiptCheckpointFileV2(record)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return ReceiptCheckpointV1{}, true, ErrInvalidState
	}
	return record, true, nil
}
