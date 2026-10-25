package ydbstore

import (
	"context"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is the original entry's in-memory cleanup window, not another durable
// lifecycle or permission. U is fixed before credential Issue from the original
// active deadline plus its existing cleanup allowance. The first final cleanup
// narrows U once to D=min(U, now+C). No later branch/Release/terminal renews D.
// Ordinary model-operation Close does not start this final cleanup window.
type localCredentialCleanup struct {
	mu                        sync.Mutex
	active, ceiling, deadline time.Time
	duration                  time.Duration
	prepared                  bool
}

// PrepareCredentialedExecution fixes only lifetime DATA for the genuine entered
// Bearer invocation. The caller supplies no deadline or approval callback.
// Canonical entry and per-operation Store checks remain the sole authorities.
func (entry *LocalInvocationEntryV1) PrepareCredentialedExecution(ctx context.Context) (time.Time, error) {
	if !entry.original() || ctx == nil || ctx.Err() != nil || entry.invocationContext == nil || entry.invocationContext.Err() != nil || entry.credentialCleanup == nil || entry.terminal.Load() != nil || entry.owner.configuration.Auth != domain.ConfiguredInferenceAuthBearerV1 {
		return time.Time{}, domain.ErrMembershipDenied
	}
	p := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if p == nil || p.Validate() != nil {
		return time.Time{}, domain.ErrMembershipDenied
	}
	c := min(p.Limits.MaxCleanupDuration, entry.owner.configuration.Limits.CleanupTimeout, 30*time.Second)
	a := entry.allocation.Window.ExpiresAt
	for _, d := range []time.Time{entry.allocation.InvocationDeadline} {
		if d.Before(a) {
			a = d
		}
	}
	for _, call := range []context.Context{entry.invocationContext, ctx} {
		if d, ok := call.Deadline(); ok && d.Before(a) {
			a = d
		}
	}
	// The existing credential service seals canonical microsecond time. Floor
	// rather than round so an intersected caller deadline is never extended.
	a = a.UTC().Truncate(time.Microsecond)
	if c <= 0 || !time.Now().Before(a) {
		return time.Time{}, domain.ErrMembershipDenied
	}
	w := entry.credentialCleanup
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.prepared {
		return time.Time{}, domain.ErrMembershipDenied
	}
	w.active, w.ceiling, w.duration, w.prepared = a, a.Add(c), c, true
	return a, nil
}

// BeginCredentialedCleanup is independent of active caller cancellation. It
// narrows, never renews, the same original entry's pre-Issue outer ceiling.
// An expired result is still returned as historical DATA for failure cleanup;
// it never authorizes a send or proves completed cleanup.
func (entry *LocalInvocationEntryV1) BeginCredentialedCleanup() (time.Time, error) {
	if !entry.original() || entry.credentialCleanup == nil || entry.owner.configuration.Auth != domain.ConfiguredInferenceAuthBearerV1 {
		return time.Time{}, domain.ErrMembershipDenied
	}
	w := entry.credentialCleanup
	d := w.begin()
	if d.IsZero() {
		return time.Time{}, domain.ErrMembershipDenied
	}
	return d, nil
}

// Pure deadline DATA narrowing, reached only after the entry's identity checks.
func (w *localCredentialCleanup) begin() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.prepared || w.duration <= 0 || w.ceiling.IsZero() {
		return time.Time{}
	}
	if w.deadline.IsZero() {
		w.deadline = time.Now().Add(w.duration)
		if w.ceiling.Before(w.deadline) {
			w.deadline = w.ceiling
		}
	}
	return w.deadline
}

func (entry *LocalInvocationEntryV1) credentialedCleanupDeadline() (time.Time, bool) {
	if entry.credentialCleanup == nil {
		return time.Time{}, false
	}
	w := entry.credentialCleanup
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.deadline, w.prepared && !w.deadline.IsZero()
}
