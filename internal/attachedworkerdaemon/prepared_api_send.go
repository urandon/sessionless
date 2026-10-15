package attachedworkerdaemon

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// The SAME invocation owns these borrowed originals until retire joins any
// outstanding call, before custody clearing or immutable Finalize. No clone,
// replenished ledger, new context or new lifetime is created here.
type preparedAPIPhysicalSend struct {
	mu         sync.Mutex
	ctx        context.Context
	ownerCtx   context.Context // original outer model-use context, distinct from revocable Source call
	cancel     context.CancelFunc
	body       []byte
	profile    string
	authorize  func() (time.Time, error)
	consumed   bool
	authorized bool
	cutoff     time.Time
	failed     bool
	retired    atomic.Bool
}

func (send *preparedAPIPhysicalSend) Authorize(body []byte, profile string) (time.Time, error) {
	if send == nil {
		return time.Time{}, ErrCredentialUnavailable
	}
	send.mu.Lock()
	defer send.mu.Unlock()
	if send.retired.Load() {
		return time.Time{}, ErrCredentialUnavailable
	}
	if send.consumed {
		send.failed = true
		return time.Time{}, ErrCredentialUnavailable
	}
	// Consume before comparison and BEFORE the one measured Source RPC. Any
	// failure, lost reply or concurrent/repeated request leaves this spent.
	send.consumed = true
	send.failed = true
	if send.ctx == nil || send.ctx.Err() != nil || send.authorize == nil ||
		len(send.body) == 0 || profile != send.profile || !bytes.Equal(body, send.body) {
		return time.Time{}, ErrCredentialUnavailable
	}
	cutoff, err := send.authorize()
	if err != nil || send.retired.Load() || send.ctx.Err() != nil || cutoff == cutoff.Round(0) || !time.Now().Before(cutoff) {
		return time.Time{}, ErrCredentialUnavailable
	}
	send.authorized = true
	send.cutoff = cutoff
	send.failed = false
	return cutoff, nil
}

func (send *preparedAPIPhysicalSend) retire() time.Time {
	// Revoke before joining: a Source reply held across executor return must
	// not publish a usable cutoff, even though custody cannot clear until join.
	send.revoke()
	send.mu.Lock()
	defer send.mu.Unlock()
	send.body = nil
	send.authorize = nil
	if !send.authorized || send.failed {
		return time.Time{}
	}
	return send.cutoff
}

// Revoke model use before joining an outstanding Source call. The cancellation
// only tightens this SAME original phase context; it never cancels the distinct
// immutable-Finalize/retirement owner or creates a fresh deadline.
func (send *preparedAPIPhysicalSend) revoke() {
	send.retired.Store(true)
	if send.cancel != nil {
		send.cancel()
	}
}
