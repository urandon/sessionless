// Package operationbody supplies bounded, scoped operation-body DATA over the
// existing BlobStore. It grants no execution, observation or deletion authority.
package operationbody

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrBody = errors.New("operation body unavailable")
	ErrBusy = errors.New("operation body I/O already active")
)

// ReferenceV1 is content metadata, not permission or a storage/erasure receipt.
// Digest is raw SHA256 of bytes, not the configured inference request digest.
type ReferenceV1 struct {
	Ref    string
	Digest string
	Bytes  uint64
}

// StoreV1 copies share the same fixed scope, I/O gate and physical write bounds.
// The trusted session/composer owns its lifetime and joins synchronous calls;
// neither this adapter nor the underlying BlobStore is model/tool accessible.
type StoreV1 struct{ state *bodyState }

type bodyState struct {
	scope                domain.AttemptOperationScopeV1
	prefix               string
	blobs                ports.BlobStore
	active               chan struct{}
	timeout              time.Duration
	cleanupTimeout       time.Duration
	maxWrites, maxBytes  uint64
	writes, chargedBytes uint64
	writeFailed          bool
	maxRetainedPairs     uint64
	retainedCount        uint64
	retained             [2]retainedSlot
	terminal             *terminalOutputState
	namespace            namespaceState
}

var _ ports.AttemptOperationBodyStoreV1 = (*StoreV1)(nil)

func NewV1(scope domain.AttemptOperationScopeV1, limits domain.AgentLoopLimitsV1, blobs ports.BlobStore) (*StoreV1, error) {
	if scope.Validate() != nil || limits.Validate() != nil || blobs == nil {
		return nil, ErrBody
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return nil, ErrBody
	}
	namespace := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	prefix := domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) +
		"attempt-operations-v1/" + hex.EncodeToString(namespace[:]) + "/"
	// Reject non-normalized identities; never path.Clean them into another scope.
	probe := domain.BlobRef{TenantID: scope.TenantID, Key: prefix + "probe", Size: 1, SHA256: strings.Repeat("0", 64)}
	if probe.Validate() != nil {
		return nil, ErrBody
	}
	timeout := min(10*time.Second, limits.MaxNetworkOperationDuration)
	return &StoreV1{state: &bodyState{scope: scope, prefix: prefix, blobs: blobs, active: make(chan struct{}, 1),
		timeout: timeout, cleanupTimeout: limits.MaxCleanupDuration, maxWrites: 2 * limits.MaxOperations, maxBytes: limits.MaxAggregateBodyBytes,
		maxRetainedPairs: limits.MaxCompactionCalls}}, nil
}

// begin fails rather than queuing behind active I/O. BlobStore and its readers
// must honor contexts and join synchronous Close; no goroutine is abandoned to
// manufacture bounded completion for a noncooperative implementation.
func (s *StoreV1) begin(ctx context.Context) (context.Context, func(), error) {
	if s == nil || s.state == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, ErrBody
	}
	st := s.state
	select {
	case st.active <- struct{}{}:
	default:
		return nil, nil, ErrBusy
	}
	call, cancel := context.WithTimeout(ctx, st.timeout)
	finish := func() { cancel(); <-st.active }
	if call.Err() != nil || st.namespace.fenced {
		finish()
		return nil, nil, ErrBody
	}
	return call, finish, nil
}

func (s *StoreV1) Write(ctx context.Context, body []byte) (ReferenceV1, error) {
	ref, err := s.writeBody(ctx, body, false)
	if err != nil {
		return ReferenceV1{}, err
	}
	return ReferenceV1{Ref: ref.Key[len(s.state.prefix):], Digest: ref.SHA256, Bytes: uint64(ref.Size)}, nil
}

// Both closed DATA arms share this one physical reservation and uncertainty
// path. The flag selects encoding only, never policy, consent or send authority.
func (s *StoreV1) writeBody(ctx context.Context, body []byte, consent bool) (domain.BlobRef, error) {
	if s == nil || s.state == nil || len(body) == 0 || uint64(len(body)) > s.state.maxBytes ||
		(consent && uint64(len(body)) > domain.AttemptOperationConsentArgumentsMaxBytesV1) {
		return domain.BlobRef{}, ErrBody
	}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return domain.BlobRef{}, err
	}
	defer finish()
	st := s.state
	size := uint64(len(body))
	if st.writeFailed || st.writes >= st.maxWrites || size > st.maxBytes-st.chargedBytes {
		return domain.BlobRef{}, ErrBody
	}
	// Physical capacity, not Store quota: reserve before any copy or PUT and
	// never refund an attempted write. Reconstructing an adapter restores no
	// durable budget or execution permission.
	st.writes++
	st.chargedBytes += size
	st.writeFailed = true
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	ref := ReferenceV1{Ref: "opbody-v1." + strconv.FormatUint(size, 10) + "." + digest, Digest: digest, Bytes: size}
	expected := st.blobRef(ref)
	if consent {
		expected, err = domain.AttemptOperationConsentArgumentsRefV1(st.scope, size, digest)
		if err != nil {
			return domain.BlobRef{}, ErrBody
		}
	}
	// Keep the predicted key before copying, even when PUT/readback fails or no
	// durable operation ever references it. This is bounded physical inventory,
	// not a quota ledger, registration or permission to erase.
	if !st.namespace.remember(expected, st.maxWrites) || call.Err() != nil {
		return domain.BlobRef{}, ErrBody
	}
	sealed := bytes.Clone(body)
	defer clear(sealed)
	actual, err := st.blobs.Put(call, st.scope.TenantID, expected.Key, bytes.NewReader(sealed))
	if err != nil || call.Err() != nil || actual != expected || actual.Validate() != nil {
		return domain.BlobRef{}, ErrBody
	}
	confirmed, err := st.readVerified(call, expected)
	clear(confirmed)
	if err != nil || call.Err() != nil {
		return domain.BlobRef{}, ErrBody
	}
	st.writeFailed = false
	return expected, nil
}

func (s *StoreV1) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, value string, cap uint64) ([]byte, error) {
	if s == nil || s.state == nil || scope != s.state.scope || cap == 0 || cap > 1<<20 {
		return nil, ErrBody
	}
	ref, err := s.state.readReference(value, min(cap, s.state.maxBytes))
	if err != nil {
		return nil, err
	}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	// Reads remain available for reconciliation after an uncertain write. DATA
	// alone cannot mint a new send or a completed-operation observation.
	return s.state.readVerified(call, ref)
}

// Ordinary operations retain relative references; consent arguments use only
// the complete closed key. This is not a general historical BlobRef reader.
func (st *bodyState) readReference(value string, cap uint64) (domain.BlobRef, error) {
	if strings.HasPrefix(value, "opbody-v1.") {
		ref, err := parseReference(value, cap)
		if err != nil {
			return domain.BlobRef{}, err
		}
		return st.blobRef(ref), nil
	}
	ref, err := domain.ParseAttemptOperationConsentArgumentsRefV1(st.scope, value, cap)
	if err != nil {
		return domain.BlobRef{}, ErrBody
	}
	return ref, nil
}

func parseReference(value string, cap uint64) (ReferenceV1, error) {
	if len(value) < 76 || len(value) > 82 {
		return ReferenceV1{}, ErrBody
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 || parts[0] != "opbody-v1" || len(parts[2]) != 64 {
		return ReferenceV1{}, ErrBody
	}
	size, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || size == 0 || size > cap || strconv.FormatUint(size, 10) != parts[1] {
		return ReferenceV1{}, ErrBody
	}
	digest, err := hex.DecodeString(parts[2])
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != parts[2] {
		return ReferenceV1{}, ErrBody
	}
	return ReferenceV1{Ref: value, Digest: parts[2], Bytes: size}, nil
}

func (st *bodyState) blobRef(ref ReferenceV1) domain.BlobRef {
	return domain.BlobRef{TenantID: st.scope.TenantID, Key: st.prefix + ref.Ref, Size: int64(ref.Bytes), SHA256: ref.Digest}
}

// readVerified returns fresh caller-owned bytes, never a retained body buffer.
// Raw BlobStore metadata or a successful PUT is not WORM/integrity evidence.
func (st *bodyState) readVerified(ctx context.Context, ref domain.BlobRef) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ErrBody
	}
	reader, err := st.blobs.Open(ctx, st.scope.TenantID, ref)
	if err != nil || reader == nil {
		if reader != nil {
			_ = reader.Close()
		}
		return nil, ErrBody
	}
	body, readErr := io.ReadAll(io.LimitReader(reader, ref.Size+1))
	closeErr := reader.Close()
	sum := sha256.Sum256(body)
	if readErr != nil || closeErr != nil || ctx.Err() != nil || int64(len(body)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		clear(body)
		return nil, ErrBody
	}
	return body, nil
}
