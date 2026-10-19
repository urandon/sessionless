package operationbody

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// The existing policy bounds MaxOperations to twelve. This inventory records
// attempted ordinary/consent keys, not retained/public output or another quota.
type namespaceState struct {
	bodies                 [24]domain.BlobRef
	count                  uint64
	fenced, eraseAttempted bool
	deadline               time.Time
	receipt                *NamespaceErasureV1
}

func (n *namespaceState) remember(ref domain.BlobRef, max uint64) bool {
	if n.fenced || ref.Validate() != nil || max > uint64(len(n.bodies)) {
		return false
	}
	for i := uint64(0); i < n.count; i++ {
		if n.bodies[i].Key == ref.Key {
			return n.bodies[i] == ref
		}
	}
	if n.count >= max || n.count >= uint64(len(n.bodies)) {
		return false
	}
	n.bodies[n.count] = ref
	n.count++
	return true
}

// NamespaceErasureV1 is an opaque original physical observation. It grants no
// Store authority and says nothing about durable retirement, output custody,
// billing or canonical completion. JSON/copies cannot mint an original receipt.
type NamespaceErasureV1 struct {
	self                             *NamespaceErasureV1
	state                            *bodyState
	scope                            domain.AttemptOperationScopeV1
	namespaceDigest, inventoryDigest string
	deletedBodies, deletedBytes      uint64
	closedAt                         time.Time
}

func (r *NamespaceErasureV1) original() bool { return r != nil && r.self == r && r.state != nil }
func (r *NamespaceErasureV1) Scope() (domain.AttemptOperationScopeV1, error) {
	if !r.original() {
		return domain.AttemptOperationScopeV1{}, ErrBody
	}
	return r.scope, nil
}
func (r *NamespaceErasureV1) NamespaceDigest() (string, error) {
	if !r.original() {
		return "", ErrBody
	}
	return r.namespaceDigest, nil
}
func (r *NamespaceErasureV1) InventoryDigest() (string, error) {
	if !r.original() {
		return "", ErrBody
	}
	return r.inventoryDigest, nil
}
func (r *NamespaceErasureV1) DeletedBodies() (uint64, error) {
	if !r.original() {
		return 0, ErrBody
	}
	return r.deletedBodies, nil
}
func (r *NamespaceErasureV1) DeletedBytes() (uint64, error) {
	if !r.original() {
		return 0, ErrBody
	}
	return r.deletedBytes, nil
}
func (r *NamespaceErasureV1) ClosedAt() (time.Time, error) {
	if !r.original() {
		return time.Time{}, ErrBody
	}
	return r.closedAt, nil
}
func (*NamespaceErasureV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("original namespace erasure cannot be serialized")
}
func (*NamespaceErasureV1) String() string     { return "NamespaceErasureV1{private:[redacted]}" }
func (r *NamespaceErasureV1) GoString() string { return r.String() }

// CloseOperationNamespace is for the original trusted terminal producer AFTER
// it independently proves stop/join, retained custody and durable retirement.
// The physical holder accepts no caller inventory/path/approval. It deletes
// only its own known ordinary/consent keys. All calls are synchronous under the same
// adapter gate and one retained absolute deadline, never a per-object timeout.
//
// A failed attempt is irreversible. Its exact inventory remains in this state
// for later bounded original-owner reconciliation, NOT implemented by retries
// here. In particular absence cannot resolve an earlier ambiguous PUT that may
// still commit later; writeFailed always denies erasure before any Delete.
func (t *TerminalOutputV1) CloseOperationNamespace(ctx context.Context) (*NamespaceErasureV1, error) {
	if t.OperationBodyLimit() == 0 || ctx == nil || ctx.Err() != nil {
		return nil, ErrBody
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, ErrBody
	}
	st := t.state
	select {
	case st.active <- struct{}{}:
	default:
		return nil, ErrBusy
	}
	defer func() { <-st.active }()
	n := &st.namespace
	if n.eraseAttempted {
		return nil, ErrBody
	}
	// Fence EVERY physical path, including copies, even when this attempt is
	// unsupported/tainted. The normal begin gate cannot acquire new work later.
	n.fenced = true
	n.eraseAttempted = true
	n.deadline = minDeadline(deadline, time.Now().Add(st.cleanupTimeout))
	call, cancel := context.WithDeadline(ctx, n.deadline)
	defer cancel()
	absent, supported := st.blobs.(ports.BlobAbsenceVerifierV1)
	if !supported || st.writeFailed || call.Err() != nil || n.count > st.maxWrites || n.count > uint64(len(n.bodies)) {
		return nil, ErrBody
	}
	var total uint64
	for i := uint64(0); i < n.count; i++ {
		ref := n.bodies[i]
		var expected domain.BlobRef
		var e error
		if strings.HasPrefix(ref.Key, st.prefix) {
			var parsed ReferenceV1
			parsed, e = parseReference(ref.Key[len(st.prefix):], st.maxBytes)
			expected = st.blobRef(parsed)
		} else {
			expected, e = domain.ParseAttemptOperationConsentArgumentsRefV1(st.scope, ref.Key, st.maxBytes)
		}
		if e != nil || expected != ref || uint64(ref.Size) > st.maxBytes-total {
			return nil, ErrBody
		}
		total += uint64(ref.Size)
		// Verify all immutable objects before the first destructive call. Neither
		// a random read error nor pre-existing absence counts as a deletion.
		body, e := st.readVerified(call, ref)
		clear(body)
		if e != nil {
			return nil, ErrBody
		}
	}
	ns := sha256.Sum256([]byte("sessionless.operation-body-namespace.v1\x00" + st.prefix))
	namespaceDigest := hex.EncodeToString(ns[:])
	body, e := json.Marshal(struct {
		Version         uint32                         `json:"version"`
		Scope           domain.AttemptOperationScopeV1 `json:"scope"`
		NamespaceDigest string                         `json:"namespace_digest"`
		Bodies          []domain.BlobRef               `json:"bodies"`
	}{1, st.scope, namespaceDigest, n.bodies[:n.count]})
	if e != nil {
		return nil, ErrBody
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.operation-body-inventory.v1\x00"))
	_, _ = h.Write(body)
	clear(body)
	inventoryDigest := hex.EncodeToString(h.Sum(nil))
	for i := uint64(0); i < n.count; i++ {
		if call.Err() != nil || st.blobs.Delete(call, st.scope.TenantID, n.bodies[i]) != nil || call.Err() != nil || absent.RequireBlobAbsent(call, st.scope.TenantID, n.bodies[i]) != nil || call.Err() != nil {
			return nil, ErrBody
		}
	}
	if call.Err() != nil {
		return nil, ErrBody
	}
	r := &NamespaceErasureV1{state: st, scope: st.scope, namespaceDigest: namespaceDigest, inventoryDigest: inventoryDigest, deletedBodies: n.count, deletedBytes: total, closedAt: time.Now().UTC().Truncate(time.Microsecond)}
	r.self = r
	n.receipt = r
	return r, nil
}

func minDeadline(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// VerifyNamespaceErasure authenticates only this original physical holder's
// actual completed observation. It cannot reconstruct one from digest DATA.
func (t *TerminalOutputV1) VerifyNamespaceErasure(r *NamespaceErasureV1) error {
	if t.OperationBodyLimit() == 0 || !r.original() || r.state != t.state {
		return ErrBody
	}
	st := t.state
	select {
	case st.active <- struct{}{}:
	default:
		return ErrBusy
	}
	defer func() { <-st.active }()
	if !st.namespace.fenced || !st.namespace.eraseAttempted || st.namespace.receipt != r {
		return ErrBody
	}
	return nil
}
