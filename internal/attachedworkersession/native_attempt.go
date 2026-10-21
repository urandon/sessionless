package attachedworkersession

import (
	"bytes"
	"context"
	"encoding/hex"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// NativeAttemptV2 borrows the original connected Session's claimed attempt.
// It owns cancellation only, NOT another execution reservation or permission
// to connect. The existing prepared process boundary still consumes execution.
// Losing this original child, Session or claim cannot reconstruct a tunnel.
type NativeAttemptV2 struct {
	self     *NativeAttemptV2
	owner    *Session
	attempt  domain.AttachedWorkerAttemptV1
	resource domain.ProviderResourceBindingV1
	ctx      context.Context
	cancel   context.CancelFunc
	stop     func() bool
	entry    NativePreparedEntryV2
}

// NativePreparedEntryV2 is a passive dependency bridge to the one original
// prepared boundary. The marker is NEVER called as an authority predicate.
// The concrete producer must validate the returned original identity/config.
type NativePreparedEntryV2 interface{ NativePreparedEntryIdentityV2() }

// BindPreparedEntryV2 joins concurrent constructors to ONE original consumed
// entry. It does not consume execution or create a second reservation/latch.
func (v *NativeAttemptV2) BindPreparedEntryV2(candidate NativePreparedEntryV2) (NativePreparedEntryV2, error) {
	if v == nil || v.self != v || v.owner == nil || candidate == nil {
		return nil, ErrInvalidAuthority
	}
	value := reflect.ValueOf(candidate)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, ErrInvalidAuthority
	}
	s := v.owner
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nativeAttempt != v || v.ctx.Err() != nil || !s.nativeCurrentLocked(v.attempt) {
		return nil, ErrInvalidAuthority
	}
	if v.entry == nil {
		v.entry = candidate
	}
	return v.entry, nil
}

func (*NativeAttemptV2) String() string     { return "NativeAttemptV2{original:[redacted]}" }
func (v *NativeAttemptV2) GoString() string { return v.String() }

// BorrowNativeAttemptV2 compares metadata with the actual original Session
// machine. A structurally valid accepted-attempt DTO or copied Session is not
// sufficient. The job/manifest digest binds the selected resource as well as
// the original input; no source paths or credentials enter this child.
func (s *Session) BorrowNativeAttemptV2(ctx context.Context, a domain.AttachedWorkerAttemptV1, job domain.WorkerJob, manifest domain.ArtifactManifest) (*NativeAttemptV2, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || a.Validate() != nil || a.State != domain.AttachedWorkerAttemptClaimed ||
		job.HarnessBinding.Resource.Validate() != nil || job.HarnessBinding.Resource.Kind != domain.ProviderResourceSubscriptionV1 ||
		job.HarnessBinding.Resource.OwnerUserID != a.OwnerUserID || job.TenantID != a.TenantID || job.RunID != a.RunID ||
		job.AttemptID != a.AttemptID || job.ReservationID != a.ReservationID || job.CredentialOwnerUserID != a.OwnerUserID ||
		job.ExecutionPlacementV2.WorkerID != a.WorkerID || job.ExecutionPlacementV2.OwnerUserID != a.OwnerUserID ||
		job.ExecutionPlacementV2.CapabilityDigest != a.CapabilityDigest || job.ExecutionPlacementV2.PolicyDigest != a.PolicyDigest ||
		job.HarnessBinding.ValidateForScope(a.TenantID, a.OwnerUserID, a.RunID, a.AttemptID, job.ExecutionPlacementV2) != nil {
		return nil, ErrInvalidAuthority
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil || digest != a.ContextDigest {
		return nil, ErrInvalidAuthority
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.nativeCurrentLocked(a) {
		return nil, ErrInvalidAuthority
	}
	if old := s.nativeAttempt; old != nil {
		if old.attempt == a && old.resource == job.HarnessBinding.Resource && old.ctx.Err() == nil {
			return old, nil // borrow the same owner, never mint a replacement
		}
		if old.attempt.RunID == a.RunID && old.attempt.AttemptID == a.AttemptID {
			return nil, ErrInvalidAuthority
		}
		old.cancel()
	}
	deadline := a.LeaseExpiresAt
	if s.binding.AuthenticationExpires.Before(deadline) {
		deadline = s.binding.AuthenticationExpires
	}
	remaining := deadline.Sub(s.now().UTC())
	if remaining <= 0 {
		return nil, ErrInvalidAuthority
	}
	// A borrow/constructor context is only a preflight liveness check. It must
	// not become the cancellation owner of a different constructor's winning
	// original entry. The actual Session/claim invalidation and original expiry
	// own this child; the prepared entry separately links its actual Run ctx.
	life, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(remaining, cancel)
	v := &NativeAttemptV2{owner: s, attempt: a, resource: job.HarnessBinding.Resource, ctx: life, cancel: cancel}
	v.self = v
	v.stop = context.AfterFunc(life, func() { timer.Stop() })
	s.nativeAttempt = v
	return v, nil
}

func (s *Session) nativeCurrentLocked(a domain.AttachedWorkerAttemptV1) bool {
	if s.nativeIdentity != s || !s.nativeFresh || s.state != StateReady || s.machine == nil || s.now == nil ||
		s.binding.TenantID != a.TenantID || s.binding.OwnerUserID != a.OwnerUserID || s.binding.WorkerID != a.WorkerID ||
		s.binding.ConnectionID != a.ConnectionID || s.binding.EnrollmentGeneration != a.EnrollmentGeneration ||
		s.binding.ConnectionGeneration != a.ConnectionGeneration || s.binding.CapabilityDigest != a.CapabilityDigest ||
		!s.now().UTC().Before(s.binding.AuthenticationExpires) || !s.now().UTC().Before(a.LeaseExpiresAt) {
		return false
	}
	snapshot, err := s.machine.Snapshot()
	if err != nil || snapshot.Connection != attachedworkerprotocol.ConnectionReady || snapshot.Revoke != nil ||
		snapshot.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed {
		return false
	}
	b := snapshot.Attempt.Summary.Binding
	contextDigest, ce := hex.DecodeString(string(a.ContextDigest))
	capability, ke := hex.DecodeString(string(a.CapabilityDigest))
	policy, pe := hex.DecodeString(string(a.PolicyDigest))
	return ce == nil && ke == nil && pe == nil && b.RunID == string(a.RunID) && b.AttemptID == string(a.AttemptID) &&
		b.LeaseID == string(a.LeaseID) && b.LeaseGeneration == a.LeaseGeneration && b.FenceToken == string(a.FenceToken) &&
		b.ExpiresAtUnixMicro == a.LeaseExpiresAt.UTC().UnixMicro() && bytes.Equal(b.ContextDigest, contextDigest) &&
		bytes.Equal(b.CapabilityDigest, capability) && bytes.Equal(b.PolicyDigest, policy)
}

func (s *Session) invalidateNativeLocked() {
	if v := s.nativeAttempt; v != nil && !s.nativeCurrentLocked(v.attempt) {
		v.cancel()
	}
}

// Current performs a bounded local recheck, never network exchange or renewal.
func (v *NativeAttemptV2) Current() error {
	if v == nil || v.self != v || v.owner == nil || v.ctx == nil || v.ctx.Err() != nil {
		return ErrInvalidAuthority
	}
	s := v.owner
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nativeAttempt != v || !s.nativeCurrentLocked(v.attempt) {
		v.cancel()
		return ErrInvalidAuthority
	}
	return nil
}

func (v *NativeAttemptV2) Lifetime() (context.Context, error) {
	if v.Current() != nil {
		return nil, ErrInvalidAuthority
	}
	return v.ctx, nil
}

func (v *NativeAttemptV2) Matches(a domain.AttachedWorkerAttemptV1, resource domain.ProviderResourceBindingV1) bool {
	return v.Current() == nil && v.attempt == a && v.resource == resource
}

// Close retires this child only, never the shared Session or model server.
func (v *NativeAttemptV2) Close() error {
	if v == nil || v.self != v || v.cancel == nil {
		return ErrInvalidAuthority
	}
	v.cancel()
	return nil
}
