package attachedworkerdaemon

import (
	"bytes"
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedAPISendBorrowV1 is an opaque borrow of the original runner's one
// physical-send authority. It is not permission to read a key, open a socket,
// select a destination or accept a response. Its zero value is unusable; copies
// share the same irreversible state. The owner retains all retirement duties.
type PreparedAPISendBorrowV1 struct{ send *preparedAPIPhysicalSend }

func (PreparedAPISendBorrowV1) String() string     { return "PreparedAPISendBorrowV1{private:[redacted]}" }
func (b PreparedAPISendBorrowV1) GoString() string { return b.String() }

// BindPreparedAPISendV1 accepts only the daemon's private original sender, not
// an implementation of its structural interface. The file owner must supply
// its exact active Materialize context, handle and plan. Binding happens before
// any blocking Source call, permitting revocation before callback cleanup joins.
// All comparisons and the embedded borrow were prepaid before original Issue.
func BindPreparedAPISendV1(sender PreparedAPIPhysicalSendV1, ctx context.Context, identity InvocationIdentity,
	ledger PreparedAPILedgerV1, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1,
	body []byte, profile string) (*PreparedAPISendBorrowV1, error) {
	send, ok := sender.(*preparedAPIPhysicalSend)
	if !ok || send == nil {
		return nil, ErrCredentialUnavailable
	}
	if !send.mu.TryLock() {
		send.invalid.Store(true)
		send.revoke()
		return nil, ErrCredentialUnavailable
	}
	defer send.mu.Unlock()
	if send.retired.Load() || send.invalid.Load() || send.bound || send.consumed ||
		!send.handleSealed || !send.planSealed || !sameOriginalAPIContext(ctx, send.ownerCtx) ||
		send.ctx == nil || send.ctx.Err() != nil || ctx.Err() != nil || identity != send.identity ||
		!samePiLedger(ledger, send.ledger) || handle != send.handle || plan != send.plan ||
		profile != send.profile || !bytes.Equal(body, send.body) {
		send.invalid.Store(true)
		send.revoke()
		return nil, ErrCredentialUnavailable
	}
	send.bound = true
	return &send.borrow, nil
}

func sameOriginalAPIContext(a, b context.Context) bool {
	return a != nil && b != nil && reflect.TypeOf(a).Comparable() && reflect.TypeOf(b).Comparable() && a == b
}

// Authorize consumes the SAME original Source authorization. No caller body,
// callback, reset, fresh deadline or replacement ledger enters this method.
// The fixed transport validates HMAC/file/body and its sealed sink construction
// before calling it. Actual DNS/TCP/TLS follows Source, under F; verified peer
// identity must precede any secret write. This borrow proves none of that IO.
func (b *PreparedAPISendBorrowV1) Authorize() (time.Time, error) {
	_, cutoff, err := b.AuthorizeNetworkUse()
	return cutoff, err
}

// AuthorizeNetworkUse consumes the SAME once-only transition as Authorize.
// The returned context is the original revocable Source/network context, not
// the supervisor's separate bounded Final-drain context. It is cancellation
// DATA, not a replacement send grant, caller-selected deadline or eligibility.
func (b *PreparedAPISendBorrowV1) AuthorizeNetworkUse() (context.Context, time.Time, error) {
	if b == nil || b.send == nil {
		return nil, time.Time{}, ErrCredentialUnavailable
	}
	send := b.send
	if !send.mu.TryLock() {
		b.Revoke()
		return nil, time.Time{}, ErrCredentialUnavailable
	}
	defer send.mu.Unlock()
	if !send.bound {
		return nil, time.Time{}, ErrCredentialUnavailable
	}
	cutoff, err := send.authorizeLocked(send.body, send.profile)
	if err != nil {
		send.invalid.Store(true)
		send.revoke()
		return nil, time.Time{}, err
	}
	return send.ctx, cutoff, nil
}

// Revoke is deliberately nonblocking. Cleanup calls it BEFORE joining any
// file-use gate or held Source RPC. It cancels model use, not immutable-Finalize
// or the separate original resource retirement context.
func (b *PreparedAPISendBorrowV1) Revoke() {
	if b != nil && b.send != nil {
		b.send.invalid.Store(true)
		b.send.revoke()
	}
}

// RevokePreparedAPISendV1 only withdraws the original concrete send. It grants
// nothing and lets a file-consumer comparison failure burn the supplied attempt
// even when a foreign locator prevented resolving its private Materialize use.
func RevokePreparedAPISendV1(sender PreparedAPIPhysicalSendV1) {
	if send, ok := sender.(*preparedAPIPhysicalSend); ok && send != nil {
		send.invalid.Store(true)
		send.revoke()
	}
}

func (send *preparedAPIPhysicalSend) sealHandle(handle ports.ProviderInvocationCredentialV1) error {
	send.mu.Lock()
	defer send.mu.Unlock()
	if send.retired.Load() || send.handleSealed || send.data == nil || !apiHandleMatches(handle, send.data.Issue) {
		send.invalid.Store(true)
		send.revoke()
		return ErrCredentialUnavailable
	}
	send.handle, send.handleSealed = handle, true
	return nil
}

func (send *preparedAPIPhysicalSend) sealPlan(plan ports.ProviderCredentialMaterializationV1) error {
	send.mu.Lock()
	defer send.mu.Unlock()
	// Both paths must fit the original frame that carries their locator. This
	// is a prepaid comparison bound, NOT complete Init/native fit certification.
	if send.retired.Load() || !send.handleSealed || send.planSealed || send.data == nil ||
		plan.Kind != domain.ProviderCredentialDeliveryFileV1 || plan.EnvironmentName != "" ||
		uint64(len(plan.RootDir)) > send.data.ParentRequest.Profile.Limits.ProcessFrameBytes ||
		uint64(len(plan.FilePath)) > send.data.ParentRequest.Profile.Limits.ProcessFrameBytes {
		send.invalid.Store(true)
		send.revoke()
		return ErrCredentialUnavailable
	}
	send.plan, send.planSealed = plan, true
	return nil
}
