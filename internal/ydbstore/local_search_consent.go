package ydbstore

import (
	"bytes"
	"context"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// One original-entry wait budget spans both requests. Copies of public entry
// pointers cannot create another budget or renew the original active window.
type localSearchWaitBudget struct {
	mu             sync.Mutex
	polls          uint32
	bytes          uint64
	lastStart      time.Time
	active, failed bool
	expiryPending  bool // one healthy pre-I/O cutoff selection, not stop evidence
}

// This private holder originates only from the durable MODEL preparation arm.
// Its values are DATA; identity plus the original Store's current joins, not a
// supplied state/receipt/argument body, authorize the following transition.
type localSearchConsentV1 struct {
	self                    *localSearchConsentV1
	entry                   *LocalInvocationEntryV1
	model                   domain.AttemptOperationReservationV1
	observation             domain.AttemptOperationObservationV1
	request                 domain.WebSearchConsentRequestV1
	grant                   *ownedWebSearchResourceGrantEvidence
	declaration             ports.SearchCredentialDeclarationV1
	arguments               []byte
	state                   domain.WebSearchConsentStateV1
	created, failed, waited bool // original entry mutation barrier protects these
	preparationAttempted    bool
}

func (*localSearchConsentV1) String() string { return "LocalSearchConsentV1{private:[redacted]}" }

func (p *localSearchConsentV1) original() bool {
	return p != nil && p.self == p && p.entry.original() && p.entry.pendingSearch != nil &&
		p.entry.pendingSearch.Load() == p && p.entry.searchWait != nil && p.request.Validate() == nil &&
		p.request.Scope == ownedConsentScope(p.entry.parent) && p.grant != nil && p.declaration.Validate() == nil
}

func (entry *LocalInvocationEntryV1) createLocalSearchConsent(caller, ctx context.Context, source *LocalCompactionSourceV1, model LocalCompactionOperationV1, call *configuredinference.FunctionCallV1) (*localSearchConsentV1, error) {
	deny := func() (*localSearchConsentV1, error) { return nil, domain.ErrMembershipDenied }
	tuple := entry.owner.ready.Load()
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	if tuple == nil || tuple.search == nil || tuple.searchTransport == nil || tuple.search.ValidateConstructor() != nil || !entry.credentialExecutionActive() || policy == nil || policy.Profile != domain.AgentLoopOwnedPublicV1 || len(entry.source.Spending) != 4 || entry.pendingSearch == nil || entry.searchWait == nil || call == nil || call.Name != "web_search" || len(call.Arguments) == 0 || len(call.Arguments) > 4<<10 {
		return deny()
	}
	if chargeOwnedAuthorityBody(ctx, 2*uint64(len(model.Request))+2*ownedAuthorityHeadBytes) != nil || decodedCallInCatalogV3(model.Request, call.Name, entry.owner.configuration.Limits) != nil {
		return deny()
	}
	if old := entry.pendingSearch.Load(); old != nil && (!old.original() || !old.created || old.failed || old.state.Status != domain.WebSearchConsentApprovedV1 || model.Reservation.Operation.Sequence <= old.model.Operation.Sequence) {
		return deny()
	}
	var profile domain.OwnedAgentProfileCurrentV1
	var actor domain.UserID
	var at time.Time
	store := entry.owner.store
	// Callback two of the SAME capture/create logical call, never public Create.
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		profile, actor, at = domain.OwnedAgentProfileCurrentV1{}, "", time.Time{}
		if source.rejoinLocked(ctx, q, nil, nil) != nil {
			return q.budget.deny()
		}
		var err error
		profile, err = readOwnedProfileCurrent(ctx, q, entry.source.Profile)
		if err != nil || profile.WebSearchResourceGrant == nil {
			return q.budget.deny()
		}
		actor, err = readOwnedConsentCanonicalActor(ctx, q, entry.source.Scope, source.state.snapshot.Job.Run, source.state.snapshot.Job.Job)
		if err != nil || actor != entry.source.Scope.OwnerUserID {
			return q.budget.deny()
		}
		at = q.at
		return nil
	})
	if err != nil || source.materializeHistoryLocked(ctx) != nil || source.state.history.unresolvedSearchV4 {
		return deny()
	}
	grant, err := store.readOwnedWebSearchResourceGrant(ctx, profile, at)
	if err != nil {
		return deny()
	}
	declaration, err := entry.owner.source.ReadSearchCredentialDeclaration(ctx, entry.source.Profile)
	if err != nil || declaration.Resource.ResourceID != grant.grant.CredentialRef || declaration.Audience != grant.grant.CredentialAudience {
		return deny()
	}
	if chargeOwnedAuthorityBody(ctx, 2*ownedAuthorityHeadBytes+4*domain.WebSearchArgumentsBytesV1+2*domain.WebSearchWireBytesV1) != nil {
		return deny()
	}
	compiled, err := websearch.CompileV2(grant.grant, *profile.CostCeiling.WebSearchCost, websearch.BindingV1{Scope: ownedOperationScope(entry.parent), Sequence: model.Reservation.Operation.Sequence + 1, CallID: call.ID, ModelResponseDigest: model.Observation.ResponseDigest, InitiatingUserID: actor, ParentCostCeilingDigestV2: entry.source.AdmissionCostCeilingDigest, ParentPriceRevision: entry.parent.Authority.AdmissionCostCeiling.PriceRevision}, call.Arguments, at)
	if err != nil {
		return deny()
	}
	argsDigest, queryDigest, _ := compiled.Digests()
	ref, err := domain.AttemptOperationConsentArgumentsRefV1(ownedOperationScope(entry.parent), uint64(len(call.Arguments)), argsDigest)
	if err != nil {
		return deny()
	}
	expires := grant.grant.ExpiresAt
	for _, end := range []time.Time{entry.parent.Authority.InvocationDeadline, entry.parent.Authority.Lease.ExpiresAt, entry.parent.Reservation.ReservedAt.Add(policy.Limits.MaxActiveDuration), entry.allocation.Window.ExpiresAt, profile.Window.ExpiresAt, profile.CostCeiling.WebSearchCost.PriceExpiresAt, policy.ExpiresAt, entry.owner.configuration.ExpiresAt} {
		if end.Before(expires) {
			expires = end
		}
	}
	for _, original := range []context.Context{entry.invocationContext, caller} {
		if end, ok := original.Deadline(); ok && end.Before(expires) {
			expires = end
		}
	}
	expires = expires.UTC().Truncate(time.Microsecond)
	r := domain.WebSearchConsentRequestV1{Version: 1, ID: domain.WebSearchConsentRequestIDV1("consent-" + rawLocalFailureHash([]byte(string(model.Reservation.OperationDigest)+call.ID+argsDigest))), Scope: ownedConsentScope(entry.parent), InitiatingUserID: actor, ModelResponseDigest: model.Observation.ResponseDigest, CallID: call.ID, ArgumentsDigest: argsDigest, QueryDigest: queryDigest, GrantDigest: profile.WebSearchGrantDigest, ConsentRevision: grant.grant.ConsentRevision, RevocationGeneration: grant.grant.RevocationGeneration, Arguments: ref, CreatedAt: at, ExpiresAt: expires}
	if r.Validate() != nil || r.ValidateArguments(call.Arguments) != nil || compiled.ValidateConsentRequest(r) != nil || chargeOwnedAuthorityBody(ctx, 2*ownedAuthorityHeadBytes+uint64(len(call.Arguments))) != nil {
		return deny()
	}
	if err := entry.searchConsentStopFitLocked(ctx, source, r); err != nil {
		return nil, err
	}
	p := &localSearchConsentV1{entry: entry, model: model.Reservation.Clone(), observation: model.Observation.Clone(), request: r, grant: grant, declaration: declaration, arguments: bytes.Clone(call.Arguments)}
	p.self = p
	old := entry.pendingSearch.Load()
	if !entry.pendingSearch.CompareAndSwap(old, p) {
		clear(p.arguments)
		return deny()
	}
	succeeded := false
	defer func() {
		if !succeeded {
			p.failed = true
		}
	}()
	actualRef, err := entry.bodies.WriteConsentArguments(ctx, p.arguments)
	if err != nil || actualRef != ref {
		return deny()
	}
	// Callback three owns the exact append, charge and readback atomically. A
	// lost/ambiguous acknowledgement never becomes a reset public Create call.
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		p.state = domain.WebSearchConsentStateV1{}
		if source.rejoinLocked(ctx, q, nil, nil) != nil || entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil {
			return q.budget.deny()
		}
		if _, err := store.ownedConsentCurrentAuthorityData(ctx, q, entry.parent, entry.source, r, ownedCurrentConsentGrant{resource: grant}); err != nil {
			return err
		}
		states, err := readOwnedConsentStates(ctx, q, entry.parent)
		if err != nil {
			return err
		}
		next, accounting, fresh, err := appendOwnedConsentState(states, source.state.snapshot.Accounting, entry.parent, r, q.at)
		if err != nil {
			return q.budget.deny()
		}
		if fresh && (writeOwnedAttemptAccounting(ctx, q, entry.parent, entry.source, accounting) != nil || writeOwnedConsentStates(ctx, q, entry.parent, next) != nil) {
			return q.budget.deny()
		}
		actual, err := readOwnedConsentStates(ctx, q, entry.parent)
		if err != nil || q.budget.materialized(q.callback, 2*ownedAuthorityHeadBytes) != nil {
			return q.budget.deny()
		}
		state, found := findOwnedConsent(actual, r.ID)
		want, _ := r.Digest()
		got, _ := state.Request.Digest()
		if !found || want != got {
			return q.budget.deny()
		}
		p.state = state.Clone()
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return deny()
	}
	p.created, succeeded = true, true
	return p, nil
}

func decodedCallInCatalogV3(request []byte, name string, limits domain.ConfiguredInferenceLimitsV1) error {
	catalog, err := compactioncontext.ReadModelCatalogV3(1, request)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(catalog.Tools)
	tools, err := compactioncontext.DecodeModelCatalogV3(catalog.Tools, limits)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	for _, tool := range tools {
		if tool.Name == name {
			return nil
		}
	}
	return domain.ErrMembershipDenied
}

// readCurrent authenticates the retained request against current original
// parent/Source/actor/claim and actual durable MODEL. No body/secret I/O occurs
// in this poll and no caller-selected browser identity enters the read.
func (p *localSearchConsentV1) readCurrent(ctx context.Context) (domain.WebSearchConsentStateV1, error) {
	var result domain.WebSearchConsentStateV1
	err := p.entry.owner.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var err error
		result, err = p.readCurrentTx(ctx, q)
		return err
	})
	return result, err
}

// Same private correspondence inside capture/create/reserve/send callbacks;
// never another public method, logical budget, or nested SQL callback.
func (p *localSearchConsentV1) readCurrentTx(ctx context.Context, q *ownedAuthorityQuery) (domain.WebSearchConsentStateV1, error) {
	// Select the negative-only transaction BEFORE the current grant loader.
	// Its authoritative DB time and original sealed expiry are not permission
	// to catch a failed current join or resume dispatch.
	if !q.at.Before(p.request.ExpiresAt) {
		return p.readExpiredTx(ctx, q)
	}
	var result domain.WebSearchConsentStateV1
	entry, store := p.entry, p.entry.owner.store
	mode := ownedConsentIdentityMode{config: store.webIdentityV2}
	err := func() error {
		result = domain.WebSearchConsentStateV1{}
		if q.metadataCache == nil {
			q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		}
		if !p.original() || !p.created || p.failed || entry.invocationContext.Err() != nil || !p.grant.sourceCurrentAt(q.at) || entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil {
			return q.budget.deny()
		}
		_, parent, source, _, err := store.operationParent(ctx, q, ownedOperationScope(entry.parent))
		if err != nil || !reflect.DeepEqual(parent, entry.parent) || !reflect.DeepEqual(source, entry.source) {
			return q.budget.deny()
		}
		if _, err := readOwnedProfileCurrent(ctx, q, entry.source.Profile); err != nil {
			return err
		}
		row, err := readOwnedOperation(ctx, q, p.model.Operation)
		if err != nil || row == nil || row.observation == nil || !reflect.DeepEqual(row.reservation, p.model) || !reflect.DeepEqual(*row.observation, p.observation) {
			return q.budget.deny()
		}
		states, err := readOwnedConsentStates(ctx, q, parent)
		if err != nil || q.budget.materialized(q.callback, 2*ownedAuthorityHeadBytes) != nil {
			return q.budget.deny()
		}
		state, found := findOwnedConsent(states, p.request.ID)
		want, _ := p.request.Digest()
		got, _ := state.Request.Digest()
		if err != nil || !found || got != want {
			return q.budget.deny()
		}
		authority, err := store.ownedConsentCurrentAuthorityData(ctx, q, parent, source, p.request, ownedCurrentConsentGrant{resource: p.grant})
		if err != nil {
			return err
		}
		if state.Decision != nil && loadOwnedConsentActor(ctx, q, p.request, state.Decision.WebSessionDigest, &authority, mode) != nil {
			return q.budget.deny()
		}
		if state.Status == domain.WebSearchConsentApprovedV1 {
			rd, _ := state.Request.Digest()
			dd, _ := state.Decision.Digest()
			if domain.ValidateWebSearchConsentReceiptV1(state, domain.WebSearchConsentReceiptBindingV1{Request: state.Request, RequestDigest: rd, DecisionDigest: dd}, authority, q.at) != nil {
				return q.budget.deny()
			}
		}
		result = state.Clone()
		return nil
	}()
	return result, err
}

// Only the immediately preceding original source/SEARCH rejoin in this SAME
// callback supplies joined. Those readers authenticate parent, Source, current
// claim/lease, burn and allocation before comparing the entire snapshot. Reuse
// that actual read, rather than spending a second set of statements on it.
// This is not a poll path or a cache that survives a callback or SQL write.
func (p *localSearchConsentV1) readJoinedCurrentTx(ctx context.Context, q *ownedAuthorityQuery, joined localCompactionSnapshot) (domain.WebSearchConsentStateV1, error) {
	deny := func() (domain.WebSearchConsentStateV1, error) {
		return domain.WebSearchConsentStateV1{}, q.budget.deny()
	}
	if !p.original() || !p.created || p.failed || ctx.Err() != nil || p.entry.invocationContext.Err() != nil || !p.grant.sourceCurrentAt(q.at) {
		return deny()
	}
	entry, store := p.entry, p.entry.owner.store
	mode := ownedConsentIdentityMode{config: store.webIdentityV2}
	if q.currentParentStore != store || entry.owner.source.VerifyOwnedAttemptEffectSource(entry.source) != nil || store.validateCurrentOwnedAttemptParent(ctx, q, ownedAllocationParentRequest(entry.parent.Reservation), entry.parent) != nil {
		return deny()
	}
	if _, err := readOwnedProfileCurrent(ctx, q, entry.source.Profile); err != nil {
		return deny()
	}
	if len(joined.Operations) == 0 {
		return deny()
	}
	model := joined.Operations[len(joined.Operations)-1]
	if model.observation == nil || !reflect.DeepEqual(model.reservation, p.model) || !reflect.DeepEqual(*model.observation, p.observation) {
		return deny()
	}
	// Reusing the physical read does not waive the state/request copies below.
	if q.budget.materialized(q.callback, 2*ownedAuthorityHeadBytes) != nil {
		return deny()
	}
	state, found := findOwnedConsent(joined.Consents, p.request.ID)
	want, _ := p.request.Digest()
	got, _ := state.Request.Digest()
	if !found || got != want {
		return deny()
	}
	revision, generation, expiry, err := (ownedCurrentConsentGrant{resource: p.grant}).validate(entry.parent, entry.source, p.request, q.at)
	if err != nil {
		return deny()
	}
	actor, err := readOwnedConsentCanonicalActor(ctx, q, entry.source.Scope, joined.Job.Run, joined.Job.Job)
	if err != nil || actor != p.request.InitiatingUserID || actor != entry.source.Scope.OwnerUserID || joined.Job.Job.CredentialOwnerUserID != actor {
		return deny()
	}
	policy := entry.parent.Authority.HarnessBinding.AgentLoopPolicy
	cost := entry.parent.Authority.AdmissionCostCeiling.WebSearchCost
	if policy == nil || cost == nil {
		return deny()
	}
	// The current rejoin requires the current lease/window to contain these
	// original ceilings. Never use a heartbeat or renewal to extend the receipt.
	authority := domain.WebSearchConsentAuthorityV1{Scope: ownedConsentScope(entry.parent), Run: joined.Job.Run, Attempt: joined.Job.Attempt, Lease: entry.parent.Authority.Lease,
		InitiatingUserID: actor, GrantDigest: p.request.GrantDigest, ConsentRevision: revision, RevocationGeneration: generation,
		DataClass: entry.parent.Authority.HarnessBinding.InputDataClass, ClaimState: domain.WebSearchConsentClaimActiveV1,
		InvocationDeadline: entry.parent.Authority.InvocationDeadline, ActiveDeadline: entry.parent.Authority.Lease.AcquiredAt.Add(policy.Limits.MaxActiveDuration),
		GrantExpiresAt: expiry, PolicyExpiresAt: policy.ExpiresAt, PriceExpiresAt: cost.PriceExpiresAt, AllocationExpiresAt: entry.allocation.Window.ExpiresAt}
	if authority.ValidateCurrent(p.request, q.at) != nil || (state.Decision != nil && loadOwnedConsentActor(ctx, q, p.request, state.Decision.WebSessionDigest, &authority, mode) != nil) {
		return deny()
	}
	if state.Status == domain.WebSearchConsentApprovedV1 {
		rd, _ := state.Request.Digest()
		dd, _ := state.Decision.Digest()
		if domain.ValidateWebSearchConsentReceiptV1(state, domain.WebSearchConsentReceiptBindingV1{Request: state.Request, RequestDigest: rd, DecisionDigest: dd}, authority, q.at) != nil {
			return deny()
		}
	}
	if ctx.Err() != nil {
		return deny()
	}
	return state.Clone(), nil
}

// pollContext is scheduling and accounting DATA only, never a current-state
// assertion. The original holder supplies its intersected immutable deadline.
// One reservation stays active through cadence and SQL, rejecting concurrency.
func (w *localSearchWaitBudget) pollContext(ctx context.Context, deadline time.Time) (context.Context, func(error) error, error) {
	if w == nil || ctx == nil || deadline.IsZero() {
		return nil, nil, domain.ErrMembershipDenied
	}
	w.mu.Lock()
	if w.failed || w.active || w.polls >= 24 || w.bytes >= ownedAuthorityMaxBytes {
		w.mu.Unlock()
		return nil, nil, domain.ErrMembershipDenied
	}
	if !time.Now().Before(deadline) && localSearchWaitReachedDeadline(ctx, deadline) {
		w.failed = true // keep positive waiting closed; never reset this latch
		w.expiryPending = true
		w.mu.Unlock()
		return nil, nil, errLocalSearchWaitExpiry
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		w.mu.Unlock()
		return nil, nil, domain.ErrMembershipDenied
	}
	w.active = true
	due := w.lastStart.Add(5 * time.Second)
	w.mu.Unlock()
	call, cancel := context.WithDeadline(ctx, deadline)
	if delay := time.Until(due); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-call.Done():
			timer.Stop()
			expired := localSearchWaitReachedDeadline(ctx, deadline)
			w.mu.Lock()
			w.active, w.failed = false, true
			w.expiryPending = expired
			w.mu.Unlock()
			cancel()
			if expired {
				return nil, nil, errLocalSearchWaitExpiry
			}
			return nil, nil, domain.ErrMembershipDenied
		}
	}
	w.mu.Lock()
	if call.Err() != nil || !time.Now().Before(deadline) {
		w.active, w.failed = false, true
		expired := localSearchWaitReachedDeadline(ctx, deadline)
		w.expiryPending = expired
		w.mu.Unlock()
		cancel()
		if expired {
			return nil, nil, errLocalSearchWaitExpiry
		}
		return nil, nil, domain.ErrMembershipDenied
	}
	w.polls++ // before any SQL/Source/read, including a failing poll
	w.lastStart = time.Now()
	budget := &ownedAuthorityBudget{bytes: w.bytes}
	w.mu.Unlock()
	// Waiting is a DISTINCT logical call. Replace only this private budget,
	// seeded with actual aggregate usage; never reset its retained byte pool.
	call = context.WithValue(call, ownedAuthorityCallContextKey{}, budget)
	call, stop := ownedAuthorityCallContext(call)
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
		if result != nil || failed || call.Err() != nil {
			w.failed = true
		}
		denied := w.failed
		w.mu.Unlock()
		stop()
		cancel()
		if denied {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	return call, finish, nil
}

// Wait keeps the same claim and returns only after an authenticated decision
// or actual existing stop. An error/exhaustion does not manufacture a terminal
// cause: the unreserved pending custody remains on the original reconcile path.
func (p *localSearchConsentV1) Wait(ctx context.Context) (domain.WebSearchConsentStateV1, error) {
	deny := func() (domain.WebSearchConsentStateV1, error) {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	if !p.original() {
		return deny()
	}
	unlock, err := p.entry.beginLocalMutation(ctx)
	if err != nil {
		return deny()
	}
	defer unlock()
	if !p.original() || !p.created || p.failed || p.waited {
		return deny()
	}
	p.waited = true
	deadline := p.request.ExpiresAt
	for _, end := range []time.Time{p.grant.sourceDeadline, p.grant.grant.ExpiresAt, p.entry.allocation.Window.ExpiresAt, p.entry.parent.Authority.InvocationDeadline, p.entry.parent.Authority.Lease.ExpiresAt} {
		if end.Before(deadline) {
			deadline = end
		}
	}
	caller, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.entry.invocationContext, cancel)
	defer func() { stop(); cancel() }()
	for {
		call, finish, err := p.entry.searchWait.pollContext(caller, deadline)
		if err != nil {
			if err == errLocalSearchWaitExpiry && deadline.Equal(p.request.ExpiresAt) {
				if state, stopErr := p.reconcileSealedExpiry(ctx); stopErr == nil {
					p.state = state.Clone()
					return state.Clone(), nil
				}
			}
			p.failed = true
			return deny()
		}
		state, readErr := p.readCurrent(call)
		if readErr == nil && p.entry.invocationContext.Err() != nil {
			readErr = domain.ErrMembershipDenied
		}
		if finish(readErr) != nil {
			p.failed = true
			return deny()
		}
		if state.Validate() != nil {
			p.failed = true
			return deny()
		}
		p.state = state
		if state.Status != domain.WebSearchConsentPendingV1 {
			return state.Clone(), nil
		}
	}
}
