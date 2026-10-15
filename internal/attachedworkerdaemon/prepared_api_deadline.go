package attachedworkerdaemon

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Embedded in the SAME pre-Issue send. Only its original authorizeLocked may
// arm it. In particular a process/transport driver cannot choose, omit, renew
// or acknowledge a deadline. Runtime timer/scheduler allocation fit is still a
// separate qualification from this prospectively funded Go state.
type preparedAPIDeadline struct {
	mu           sync.Mutex
	timer        *time.Timer
	done         chan struct{}
	ready, armed bool
	stopped      bool
	expired      atomic.Bool
	cutoff       time.Time
	clock        func() time.Time // private test-only elapsed projection; nil in production
	ctx          context.Context  // original owner ancestor, NOT normal-exit Source cancellation
	cancel       atomic.Pointer[preparedAPICancel]
}

// bindOwner runs before Issue, after the original pre-Issue Source cutoff is
// known. A normal child exit revokes Source/network, but must still permit its
// already-written Final/EOF to drain under F. Expiry/invalid use cancels both.
func (d *preparedAPIDeadline) bindOwner(owner context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.armed || d.stopped || d.ctx != nil || owner == nil || owner.Err() != nil {
		return ErrCredentialUnavailable
	}
	ctx, cancel := context.WithCancel(owner)
	d.ctx = ctx
	d.cancel.Store(&preparedAPICancel{cancel: cancel})
	return nil
}

func (d *preparedAPIDeadline) ioContext() context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.stopped {
		return nil
	}
	return d.ctx
}

func (d *preparedAPIDeadline) cancelUse() {
	if c := d.cancel.Load(); c != nil {
		c.cancel()
	}
}

// prepareDeadline is called only after the original ledger reservation and
// before original Issue. Construct and stop the callback timer now; arm reuses
// it without constructing a timer, channel or callback at physical send.
func (send *preparedAPIPhysicalSend) prepareDeadline() error {
	d := &send.deadline
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ready || d.stopped || send.retired.Load() {
		return ErrCredentialUnavailable
	}
	done := make(chan struct{})
	d.done = done
	d.timer = time.AfterFunc(time.Duration(1<<63-1), func() {
		defer close(done)
		// Never acquire send.mu: Source may still be returning while this
		// callback withdraws model use. Immutable Finalize/Release have their
		// own owner context and are not cancelled here.
		d.expired.Store(true)
		send.invalid.Store(true)
		send.revoke()
	})
	if !d.timer.Stop() {
		// Defensive failed setup retains/join its callback; no Issue followed.
		d.stopped = true
		<-done
		d.timer, d.done = nil, nil
		return ErrCredentialUnavailable
	}
	d.ready = true
	return nil
}

func (d *preparedAPIDeadline) currentTime() time.Time {
	if d.clock != nil {
		return d.clock()
	}
	return time.Now()
}

// arm runs with send.mu held, AFTER the original Source reply. stopAndJoin
// does not acquire send.mu; a concurrent revoker therefore cannot queue behind
// Source just to stop this timer. All negative paths leave the send spent.
func (d *preparedAPIDeadline) arm(send *preparedAPIPhysicalSend, cutoff time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.ready || d.armed || d.stopped || d.timer == nil || d.ctx == nil || d.ctx.Err() != nil || send.retired.Load() || send.invalid.Load() ||
		send.ctx == nil || send.ctx.Err() != nil || cutoff == cutoff.Round(0) {
		return ErrCredentialUnavailable
	}
	q := d.currentTime()
	n := cutoff.Sub(q)
	if q == q.Round(0) || n <= 0 || !q.Add(n).Equal(cutoff) {
		return ErrCredentialUnavailable
	}
	d.armed, d.cutoff = true, cutoff
	// The timer was stopped before publication and can be armed just once.
	// An unexpectedly active timer is not a renewable allowance.
	if d.timer.Reset(n) {
		return ErrCredentialUnavailable
	}
	return nil
}

// stopAndJoin is idempotent, including before arm and concurrent retirement.
// Cancellation happens at the send owner BEFORE entry. Stop alone does not
// join an AfterFunc callback; wait for its last action before dropping aliases.
func (d *preparedAPIDeadline) stopAndJoin() {
	d.cancelUse()
	d.mu.Lock()
	if !d.ready {
		d.mu.Unlock()
		return
	}
	if !d.stopped {
		d.stopped = true
		if !d.armed || d.timer.Stop() {
			close(d.done)
		}
	}
	done := d.done
	d.mu.Unlock()
	// Catch a bindOwner publication that won the mutex after the first
	// cancellation load. stopped now prevents any later owner publication.
	d.cancelUse()
	<-done
	d.mu.Lock()
	d.timer, d.done, d.clock = nil, nil, nil
	d.ctx = nil
	d.cancel.Store(nil)
	d.ready = false
	d.mu.Unlock()
}
