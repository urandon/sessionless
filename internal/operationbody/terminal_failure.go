package operationbody

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// WriteKnownMCPFailureV1 consumes the SAME single canonical batch and protected
// notice slots as successful publication. No assistant, extra notice or reset.
func (t *TerminalOutputV1) WriteKnownMCPFailureV1(ctx context.Context, run domain.Run, drafts []domain.SessionEventDraft, bodies [][]byte) error {
	return t.writeCanonicalEvents(ctx, run, drafts, bodies, true)
}

// RetainedNamespaceV1 is an original physical fence, NOT NamespaceErasureV1.
// It proves only that these exact attempted ordinary keys can no longer gain
// another original adapter writer; bodies remain retained, not deleted.
type RetainedNamespaceV1 struct {
	self   *RetainedNamespaceV1
	state  *bodyState
	bodies []domain.BlobRef
	digest string
}

func (RetainedNamespaceV1) MarshalJSON() ([]byte, error) { return nil, ErrBody }
func (RetainedNamespaceV1) String() string               { return "RetainedNamespaceV1{private:[redacted]}" }
func (r RetainedNamespaceV1) GoString() string           { return r.String() }

func RetainedBodiesDigestV1(refs []domain.BlobRef) (string, error) {
	if len(refs) > 24 {
		return "", ErrBody
	}
	body, err := json.Marshal(refs)
	if err != nil {
		return "", ErrBody
	}
	sum := sha256.Sum256(append([]byte("sessionless.retained-operation-namespace.v1\x00"), body...))
	return hex.EncodeToString(sum[:]), nil
}

// ValidateRetainedBodiesV1 is scoped DATA validation only. It cannot recreate
// the opaque fence receipt or authorize deletion/writes.
func ValidateRetainedBodiesV1(scope domain.AttemptOperationScopeV1, refs []domain.BlobRef) error {
	if scope.Validate() != nil || len(refs) == 0 || len(refs) > 24 {
		return ErrBody
	}
	prefix, err := retainedBodyPrefixV1(scope)
	if err != nil {
		return err
	}
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		if ref.Validate() != nil || ref.TenantID != scope.TenantID || !strings.HasPrefix(ref.Key, prefix) || seen[ref.Key] {
			return ErrBody
		}
		seen[ref.Key] = true
		parsed, err := parseReference(strings.TrimPrefix(ref.Key, prefix), 1<<20)
		if err != nil || int64(parsed.Bytes) != ref.Size || parsed.Digest != ref.SHA256 {
			return ErrBody
		}
	}
	return nil
}

func retainedBodyPrefixV1(scope domain.AttemptOperationScopeV1) (string, error) {
	if scope.Validate() != nil {
		return "", ErrBody
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return "", ErrBody
	}
	hash := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	return domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempt-operations-v1/" + hex.EncodeToString(hash[:]) + "/", nil
}

// RetainedOperationBodyRefV1 derives DATA from exact persisted operation
// reference scalars. It grants no writer, namespace receipt or deletion.
func RetainedOperationBodyRefV1(scope domain.AttemptOperationScopeV1, ref string, n uint64, digest string) (domain.BlobRef, error) {
	prefix, err := retainedBodyPrefixV1(scope)
	if err != nil {
		return domain.BlobRef{}, err
	}
	parsed, err := parseReference(ref, 1<<20)
	if err != nil || parsed.Bytes != n || parsed.Digest != digest {
		return domain.BlobRef{}, ErrBody
	}
	return domain.BlobRef{TenantID: scope.TenantID, Key: prefix + ref, Size: int64(n), SHA256: digest}, nil
}

// SnapshotRetainedBodies returns bounded DATA only. The later same-gate fence
// must compare it exactly AFTER all terminal writes; a changed inventory denies
// commitment rather than leaving an unrecorded attempted key behind.
func (t *TerminalOutputV1) SnapshotRetainedBodies(ctx context.Context) ([]domain.BlobRef, error) {
	if t.OperationBodyLimit() == 0 {
		return nil, ErrBody
	}
	s := StoreV1{state: t.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	if call.Err() != nil || t.state.namespace.count > 24 {
		return nil, ErrBody
	}
	return append([]domain.BlobRef(nil), t.state.namespace.bodies[:t.state.namespace.count]...), nil
}

func (t *TerminalOutputV1) FenceRetainedBodies(ctx context.Context, expected []domain.BlobRef) (*RetainedNamespaceV1, error) {
	if t.OperationBodyLimit() == 0 || len(expected) > 24 {
		return nil, ErrBody
	}
	s := StoreV1{state: t.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	n := &t.state.namespace
	if call.Err() != nil || t.state.writeFailed || !t.owner.assistantUsed || !t.owner.failure.used || n.count > 24 || !reflect.DeepEqual(expected, append([]domain.BlobRef(nil), n.bodies[:n.count]...)) {
		return nil, ErrBody
	}
	n.fenced = true
	d, err := RetainedBodiesDigestV1(expected)
	if err != nil {
		return nil, err
	}
	r := &RetainedNamespaceV1{state: t.state, bodies: append([]domain.BlobRef(nil), expected...), digest: d}
	r.self = r
	return r, nil
}

func (t *TerminalOutputV1) VerifyRetainedBodies(r *RetainedNamespaceV1) error {
	if t.OperationBodyLimit() == 0 || r == nil || r.self != r || r.state != t.state {
		return ErrBody
	}
	st := t.state
	select {
	case st.active <- struct{}{}:
	default:
		return ErrBusy
	}
	defer func() { <-st.active }()
	if !st.namespace.fenced || st.namespace.eraseAttempted || st.namespace.count > 24 || !reflect.DeepEqual(r.bodies, append([]domain.BlobRef(nil), st.namespace.bodies[:st.namespace.count]...)) {
		return ErrBody
	}
	return nil
}
func (r *RetainedNamespaceV1) Digest() (string, error) {
	if r == nil || r.self != r {
		return "", ErrBody
	}
	return r.digest, nil
}
