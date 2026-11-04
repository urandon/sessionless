package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This marker reports ONLY a healthy before-I/O scheduling boundary. It is not
// expiry evidence: only the negative transaction's DB time may persist a stop.
var errLocalSearchWaitExpiry = errors.New("original SEARCH wait reached its sealed cutoff")

func localSearchWaitReachedDeadline(ctx context.Context, deadline time.Time) bool {
	if ctx == nil || deadline.IsZero() || time.Now().Before(deadline) {
		return false
	}
	if ctx.Err() == nil {
		return true
	}
	end, bounded := ctx.Deadline()
	return errors.Is(ctx.Err(), context.DeadlineExceeded) && bounded && end.Equal(deadline)
}

// readExpiredTx is selected before any current-permission loader. It can only
// authenticate the original private holder and persist an actual expiry or
// recover an ALREADY committed immutable stop/denial. It never retags a retained
// decision. A newly reduced different stop or unknown authority rolls back.
func (p *localSearchConsentV1) readExpiredTx(ctx context.Context, q *ownedAuthorityQuery) (domain.WebSearchConsentStateV1, error) {
	deny := func() (domain.WebSearchConsentStateV1, error) {
		return domain.WebSearchConsentStateV1{}, q.budget.deny()
	}
	if !p.original() || !p.created || p.failed || ctx.Err() != nil || q.at.Before(p.request.ExpiresAt) || p.entry.invocationContext.Err() != nil || p.entry.owner.source.VerifyOwnedAttemptEffectSource(p.entry.source) != nil {
		return deny()
	}
	d, err := p.entry.BeginCredentialedCleanup()
	if err != nil || !time.Now().Before(d) {
		return deny()
	}
	ctx, cancel := context.WithDeadline(ctx, d)
	defer cancel()
	if q.metadataCache == nil {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
	}
	entry, store := p.entry, p.entry.owner.store
	parent, source, err := store.ownedConsentParent(ctx, q, consentLocator(p.request))
	if err != nil || !reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) {
		return deny()
	}
	row, err := readOwnedOperation(ctx, q, p.model.Operation)
	if err != nil || row == nil || row.observation == nil || !reflect.DeepEqual(row.reservation, p.model) || !reflect.DeepEqual(*row.observation, p.observation) {
		return deny()
	}
	states, err := readOwnedConsentStates(ctx, q, parent)
	if err != nil || q.budget.materialized(q.callback, 2*ownedAuthorityHeadBytes) != nil {
		return deny()
	}
	state, found := findOwnedConsent(states, p.request.ID)
	want, _ := p.request.Digest()
	actual, _ := state.Request.Digest()
	if !found || actual != want || state.Request.InitiatingUserID != source.Scope.OwnerUserID {
		return deny()
	}
	retainedStop := state.Status != domain.WebSearchConsentPendingV1 && state.Status != domain.WebSearchConsentApprovedV1
	result, err := store.reconcileOwnedConsentStateTx(ctx, q, parent, source, states, state, state.Revision)
	if err != nil || !retainedStop && result.Status != domain.WebSearchConsentExpiredV1 || retainedStop && !reflect.DeepEqual(result, state) || ctx.Err() != nil || entry.invocationContext.Err() != nil {
		return deny()
	}
	return result, nil
}

// A healthy local cutoff may schedule ONE final negative call under the SAME
// original cleanup D. It spends one of 24 polls and the SAME aggregate byte
// pool; positive waiting stays sticky-closed even if the stop succeeds.
func (p *localSearchConsentV1) reconcileSealedExpiry(ctx context.Context) (domain.WebSearchConsentStateV1, error) {
	deny := func() (domain.WebSearchConsentStateV1, error) {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	if !p.original() || !p.created || p.failed || !p.waited || !localSearchWaitReachedDeadline(ctx, p.request.ExpiresAt) || p.entry.invocationContext.Err() != nil {
		return deny()
	}
	d, err := p.entry.BeginCredentialedCleanup()
	if err != nil || !time.Now().Before(d) {
		return deny()
	}
	cleanup, cancel := context.WithDeadline(context.WithoutCancel(ctx), d)
	defer cancel()
	call, finish, err := p.entry.searchWait.stopContext(cleanup)
	if err != nil {
		return deny()
	}
	state, readErr := p.readCurrent(call)
	if finish(readErr) != nil || state.Validate() != nil || state.Status == domain.WebSearchConsentPendingV1 || state.Status == domain.WebSearchConsentApprovedV1 {
		return deny()
	}
	return state.Clone(), nil
}

func (w *localSearchWaitBudget) stopContext(ctx context.Context) (context.Context, func(error) error, error) {
	if w == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, domain.ErrMembershipDenied
	}
	w.mu.Lock()
	if !w.failed || !w.expiryPending || w.active || w.polls >= 24 || w.bytes >= ownedAuthorityMaxBytes {
		w.mu.Unlock()
		return nil, nil, domain.ErrMembershipDenied
	}
	w.expiryPending = false // consume once even if the negative SQL later fails
	w.active = true
	w.polls++ // terminal read is NOT an uncounted twenty-fifth poll
	// This is a one-shot negative cleanup transaction, not a healthy poll.
	// Do not consume D on positive cadence or update positive lastStart. Its
	// original counter/byte charge remains conservative and never refunded.
	budget := &ownedAuthorityBudget{bytes: w.bytes}
	w.mu.Unlock()
	call, stop := ownedAuthorityCallContext(context.WithValue(ctx, ownedAuthorityCallContextKey{}, budget))
	var finishMu sync.Mutex
	finished := false
	finish := func(result error) error {
		finishMu.Lock()
		defer finishMu.Unlock()
		if finished {
			return domain.ErrMembershipDenied
		}
		finished = true
		budget.mu.Lock()
		spent, failed := budget.bytes, budget.failed
		budget.mu.Unlock()
		w.mu.Lock()
		if spent < w.bytes || spent > ownedAuthorityMaxBytes {
			failed = true
		} else {
			w.bytes = spent
		}
		w.active = false
		w.mu.Unlock()
		// Deliberately leave w.failed CLOSED. A successful negative-only call
		// never restores positive polling or a fresh shared allowance.
		failed = failed || result != nil || call.Err() != nil
		stop()
		if failed {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	return call, finish, nil
}
