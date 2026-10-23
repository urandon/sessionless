package localloopback

import (
	"context"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This private scope exists only inside the original synchronous materializer.
// borrowed aliases its caller-owned buffer; only per-connection wire copies are
// owned here. Neither the secret nor a string conversion enters a public DTO.
type credentialScopeV1 struct {
	ctx      context.Context
	cancel   context.CancelFunc
	handle   ports.ProviderInvocationCredentialV1
	borrowed []byte
	admitted bool
	ending   bool
	done     chan struct{}
	err      error
	// AttemptOperationV1.Validate already bounds original sequences to twelve.
	// Retain only join signals, not completed connection/credential metadata.
	joins []<-chan struct{}
}

// NewCredentialedKeeperV1 is the explicit direct-delivery local Bearer arm.
// It shares the exact Source/issuer/bootstrap identity checks with the none arm
// but does not admit a send or acquire/materialize a credential.
func NewCredentialedKeeperV1(source *localoperator.Source, issuer *serverlessharness.OperationCapabilityIssuer, bootstrap *ydbstore.LocalOwnedStoreBootstrapV1) (*KeeperV1, error) {
	return newKeeperV1(source, issuer, bootstrap, true)
}

func validBearerBytes(b []byte) bool {
	if len(b) == 0 || len(b) > 4096 {
		return false
	}
	for _, v := range b {
		if v <= ' ' || v >= 0x7f {
			return false
		}
	}
	return true
}

// BeginCredentialScope borrows exactly once. The session must call it only from
// its one materialization callback after genuine original invocation entry.
// Failed first entry is also burned; it never permits another materialization.
func (k *KeeperV1) BeginCredentialScope(ctx context.Context, handle ports.ProviderInvocationCredentialV1, borrowed []byte) error {
	if k == nil || k.self != k || !k.credentialed || ctx == nil {
		return domain.ErrMembershipDenied
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed || k.credentialScopeUsed || k.active != nil || k.sourceLifetime == nil || k.sourceLifetime.Err() != nil {
		return domain.ErrMembershipDenied
	}
	k.credentialScopeUsed = true
	now := time.Now()
	if ctx.Err() != nil || handle.Validate() != nil || handle.TenantID != k.config.TenantID || handle.OwnerUserID != k.config.OwnerUserID || handle.ProviderResource != k.config.Resource || !now.Before(handle.ExpiresAt) || !validBearerBytes(borrowed) {
		k.credentialTainted = true
		return domain.ErrMembershipDenied
	}
	call, cancel := context.WithDeadline(ctx, handle.ExpiresAt)
	k.credentialScope = &credentialScopeV1{ctx: call, cancel: cancel, handle: handle, borrowed: borrowed, admitted: true, done: make(chan struct{})}
	return nil
}

// credentialAdmittedLocked authenticates the original scope, never the key as
// authority. The actual Store still revalidates canonical generation/revocation
// and the one-use operation capability immediately before each physical send.
func (k *KeeperV1) credentialAdmittedLocked(scope *credentialScopeV1, a domain.RegisteredAllocationV2, s domain.AttemptExecutionSourceV1, op domain.AttemptOperationV1, now time.Time) bool {
	if !k.credentialed {
		return scope == nil && k.credentialScope == nil
	}
	if k.credentialTainted || k.credentialEnded || scope == nil || scope != k.credentialScope || !scope.admitted || scope.ending || scope.ctx == nil || scope.ctx.Err() != nil || !validBearerBytes(scope.borrowed) {
		return false
	}
	h := scope.handle
	return h.Validate() == nil && h.TenantID == k.config.TenantID && h.OwnerUserID == k.config.OwnerUserID && h.ProviderResource == k.config.Resource &&
		h.TenantID == a.Scope.TenantID && h.OwnerUserID == a.Scope.OwnerUserID && h.RunID == a.Scope.RunID && h.AttemptID == a.Scope.AttemptID && a.Scope == s.Scope &&
		h.WorkerID == a.Lease.WorkerID && h.LeaseID == a.Lease.ID && h.LeaseFence == a.Lease.FenceToken && a.Backend.CredentialDeliveryKind == domain.ProviderCredentialDeliveryDirectV1 &&
		now.Before(h.ExpiresAt) && !op.Deadline.After(h.ExpiresAt) && !h.ExpiresAt.After(a.Window.ExpiresAt) && !h.ExpiresAt.After(a.Lease.ExpiresAt) && !h.ExpiresAt.After(a.InvocationDeadline) && !h.ExpiresAt.After(s.ExpiresAt)
}

// requestWire allocates once with exact capacity, so no obsolete secret-bearing
// backing buffers are abandoned during append growth. All key-bearing bytes
// remain mutable and the connection monitor clears them only after I/O joins.
func requestWire(address string, payload, credential []byte) []byte {
	prefix := "POST /v1/chat/completions HTTP/1.1\r\nHost: " + address + "\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\n"
	suffix := "Content-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n"
	extra := 0
	if len(credential) != 0 {
		extra = len("Authorization: Bearer \r\n") + len(credential)
	}
	wire := make([]byte, 0, len(prefix)+extra+len(suffix)+len(payload))
	wire = append(wire, prefix...)
	if len(credential) != 0 {
		wire = append(wire, "Authorization: Bearer "...)
		wire = append(wire, credential...)
		wire = append(wire, '\r', '\n')
	}
	wire = append(wire, suffix...)
	return append(wire, payload...)
}

// EndCredentialScope permanently closes credential admission, narrows ordinary
// Close waiters to the original session-owned absolute deadline, and observes
// the actual monitor join before returning to the materializer. If that bound
// expires, TCP is still closed/cancelled and we synchronously await disposal:
// late completion is a sticky failure, never an on-time cleanup receipt. This
// relies on the owned cooperative TCP/context I/O, not an injected transport.
func (k *KeeperV1) EndCredentialScope(ctx context.Context, originalCleanupDeadline time.Time) error {
	if k == nil || k.self != k || !k.credentialed {
		return domain.ErrMembershipDenied
	}
	invalidContext := ctx == nil
	if invalidContext {
		ctx = context.Background()
	}
	k.mu.Lock()
	if invalidContext {
		k.credentialTainted = true
	}
	scope := k.credentialScope
	if scope == nil {
		k.credentialTainted = true
		k.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	if scope.ending {
		if !originalCleanupDeadline.Equal(k.credentialDeadline) {
			k.credentialTainted = true
		}
		k.mu.Unlock()
		<-scope.done
		k.mu.Lock()
		err := scope.err
		if k.credentialTainted || ctx.Err() != nil {
			err = domain.ErrMembershipDenied
		}
		k.mu.Unlock()
		return err
	}
	scope.ending, scope.admitted = true, false
	k.credentialDeadline = originalCleanupDeadline
	now := time.Now()
	limit := min(k.config.Limits.CleanupTimeout, k.policy.Limits.CleanupTimeout, 30*time.Second)
	if originalCleanupDeadline.IsZero() || !now.Before(originalCleanupDeadline) || limit <= 0 || originalCleanupDeadline.After(now.Add(limit)) || ctx.Err() != nil {
		k.credentialTainted = true
	}
	close(k.credentialSealed)
	scope.borrowed = nil // caller/materializer owns and clears the original bytes
	scope.cancel()
	joins := scope.joins
	scope.joins = nil
	c := k.active
	if c != nil && (len(joins) == 0 || joins[len(joins)-1] != c.cleanupDone) {
		// Impossible for a genuine registered connection, but still join it on
		// a contradictory state rather than returning with a retained header.
		k.credentialTainted = true
		joins = append(joins, c.cleanupDone)
	}
	k.mu.Unlock()
	var err error
	if c != nil {
		err = c.closeUntil(ctx, originalCleanupDeadline)
	}
	for _, joined := range joins {
		// A deadline verdict cannot let the materializer clear borrowed bytes
		// while preflight/invoke/monitor still owns a credential-bearing copy.
		<-joined
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err != nil || ctx.Err() != nil || !time.Now().Before(originalCleanupDeadline) || k.active != nil {
		k.credentialTainted = true
	}
	k.credentialEnded = true
	scope.handle = ports.ProviderInvocationCredentialV1{}
	scope.ctx, scope.cancel = nil, nil
	if k.credentialTainted {
		scope.err = domain.ErrMembershipDenied
	}
	close(scope.done)
	return scope.err
}
