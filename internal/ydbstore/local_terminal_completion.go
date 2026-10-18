package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// One immutable private package from the ORIGINAL cleanup producer. This is
// neither a second execution runner nor a caller-authored completion DTO.
type localTerminalCompletionV2 struct {
	self         *localTerminalCompletionV2
	cleanup      *LocalTerminalCleanupV1
	run          domain.Run
	attempt      domain.Attempt
	reservation  domain.QuotaReservation
	manifest     domain.ArtifactManifest
	drafts       []domain.SessionEventDraft
	cancellation domain.SessionEventDraft
	evidence     domain.ExecutionEvidenceV2
	custody      domain.RunTerminalCustodyV1
}

func localTerminalHash(namespace string, value any) (string, error) {
	body, err := json.Marshal(value)
	if err != nil || len(body) > 256<<10 {
		return "", domain.ErrMembershipDenied
	}
	h := sha256.New()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func terminalCanonicalEventsDigestV1(events []domain.SessionEventDraft) (string, error) {
	if len(events) == 0 || len(events) > 25 {
		return "", domain.ErrMembershipDenied
	}
	identities := make([]finalizationEventIdentity, len(events))
	for i, e := range events {
		identities[i] = finalizationEventIdentity{ID: e.ID, Kind: e.Kind, IdempotencyKey: e.IdempotencyKey, Payload: e.Payload, DisplayText: e.DisplayText}
	}
	return localTerminalHash("sessionless.terminal-canonical-events.v1\x00", identities)
}

// Caller holds source.state.mu, p.mu and the original entry barrier. All actual physical
// phases have completed; no field here can manufacture or repeat them.
func prepareLocalTerminalCompletion(r *LocalTerminalCleanupV1) (*localTerminalCompletionV2, error) {
	if r == nil || r.self != r || !r.owner.original() {
		return nil, domain.ErrMembershipDenied
	}
	p := r.owner
	h := p.parent
	if p.cleared || p.cleanup != r || p.accounting == nil || h.failed.Load() || h.source == nil || h.source.state == nil || h.source.state.cleared || !p.published || h.entry.terminalOutputs.VerifyNamespaceErasure(r.erasure) != nil {
		return nil, domain.ErrMembershipDenied
	}
	residual, err := p.accounting.Residual(r.allocation.Current, r.erasure)
	if err != nil {
		return nil, err
	}
	inventory, err := residual.Digest()
	if err != nil {
		return nil, err
	}
	accounting, err := p.accounting.Digest()
	if err != nil {
		return nil, err
	}
	namespace, ne := r.erasure.NamespaceDigest()
	physical, pe := r.erasure.InventoryDigest()
	erasedAt, te := r.erasure.ClosedAt()
	bodies, be := r.erasure.DeletedBodies()
	bytes, ze := r.erasure.DeletedBytes()
	if ne != nil || pe != nil || te != nil || be != nil || ze != nil {
		return nil, domain.ErrMembershipDenied
	}
	receipt, err := localTerminalHash("sessionless.local-residual-receipt.v2\x00", struct {
		Version   uint32                         `json:"version"`
		Scope     domain.AttemptOperationScopeV1 `json:"scope"`
		Inventory string                         `json:"inventory_digest"`
		Physical  string                         `json:"physical_inventory_digest"`
		Namespace string                         `json:"namespace_digest"`
		ClosedAt  time.Time                      `json:"closed_at"`
		Bodies    uint64                         `json:"deleted_bodies"`
		Bytes     uint64                         `json:"deleted_bytes"`
	}{2, p.plan.Scope, inventory, physical, namespace, erasedAt, bodies, bytes})
	if err != nil {
		return nil, err
	}
	parent, e1 := h.entry.parent.Digest()
	source, e2 := h.entry.source.Digest()
	original, e3 := r.allocation.Original.Digest()
	retired, e4 := r.allocation.Current.Digest()
	profile, e5 := h.entry.owner.configuration.Digest()
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || !reflect.DeepEqual(r.allocation.Original, h.entry.allocation) {
		return nil, domain.ErrMembershipDenied
	}
	local := domain.LocalExecutionEvidenceV1{Version: 1, Scope: p.plan.Scope, ParentDigest: parent, SourceDigest: source, ProfileDigest: profile,
		OriginalAllocationDigest: string(original), RetiredAllocationDigest: string(retired), FrontierDigest: p.plan.FrontierDigest, Capsule: p.plan.Capsule,
		ResidualInventoryDigest: inventory, ResidualReceiptDigest: receipt, AccountingDispositionDigest: accounting,
		StoppedAt: r.stoppedAt, RetiredAt: r.allocation.Current.UpdatedAt, ErasedAt: erasedAt}
	local.EvidenceDigest, err = local.Digest()
	if err != nil || local.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	evidence := domain.ExecutionEvidenceV2{Version: 2, Kind: domain.ExecutionEvidenceLocalV1, LocalV1: &local}
	if evidence.ValidateForPersistedAuthority(h.entry.parent.Authority, h.entry.parent.Reservation) != nil {
		return nil, domain.ErrMembershipDenied
	}
	derived, de := domain.TerminalDerivedContextDigestV1(p.publicationRun.DerivedContext)
	events, ee := terminalCanonicalEventsDigestV1(p.drafts)
	if de != nil || ee != nil || validateCanonicalFinalizationEvents(domain.RunSucceeded, p.drafts) != nil {
		return nil, domain.ErrMembershipDenied
	}
	var assistant domain.SessionEventID
	for _, draft := range p.drafts {
		if draft.Kind == domain.SessionEventAssistantMessage {
			assistant = draft.ID
		}
	}
	custody := p.publicationRun.TerminalCustody.Clone()
	custody.Correspondence = &domain.TerminalCorrespondenceV1{Version: 1, CapsuleDigest: p.plan.Capsule.SHA256, TerminalAssistantEventID: assistant,
		CanonicalEventsDigest: events, DerivedContextDigest: derived, AccountingDispositionDigest: accounting}
	custody.LocalEvidence = &local
	if custody.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	// Clone through the closed bounded domain representation; output buffers may
	// later be cleared, whereas historical replay retains these identities only.
	manifestBytes, err := json.Marshal(p.manifest)
	if err != nil || len(manifestBytes) > ownedAuthorityHeadBytes {
		return nil, domain.ErrMembershipDenied
	}
	var manifest domain.ArtifactManifest
	if json.Unmarshal(manifestBytes, &manifest) != nil {
		return nil, domain.ErrMembershipDenied
	}
	job := h.source.state.snapshot.Job
	c := &localTerminalCompletionV2{cleanup: r, run: p.publicationRun.Clone(), attempt: job.Attempt, reservation: job.Reservation,
		manifest: manifest, drafts: append([]domain.SessionEventDraft(nil), p.drafts...), cancellation: p.cancellation, evidence: evidence, custody: custody}
	c.self = c
	return c, nil
}

func (c *localTerminalCompletionV2) original() bool {
	return c != nil && c.self == c && c.cleanup != nil && c.cleanup.self == c.cleanup && c.cleanup.owner.original()
}

// Complete accepts no events, usage, evidence, approval or caller-selected
// digests. First commit needs this original owner; exact committed replay is
// observation-only and can outlive Source/lease and the physical cleanup bound.
func (r *LocalTerminalCleanupV1) Complete(ctx context.Context) (err error) {
	if r == nil || r.self != r || !r.owner.original() || ctx == nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	p := r.owner
	started := false
	defer func() {
		if !started {
			return
		}
		p.mu.Lock()
		p.publicationActive = false
		p.publicationCancel = nil
		if p.cleared {
			p.releaseOutputLocked()
		}
		close(p.publicationDone)
		p.mu.Unlock()
	}()
	if !p.parent.entry.preparationMu.TryLock() {
		return domain.ErrMembershipDenied
	}
	defer p.parent.entry.preparationMu.Unlock()
	call, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	source := p.parent.source
	if source == nil || source.state == nil {
		return domain.ErrMembershipDenied
	}
	// Clear owns this same mutex. Capture original snapshot facts under the
	// established entry -> source -> output order, never p -> source.
	source.state.mu.Lock()
	p.mu.Lock()
	if p.cleanup != r || p.publicationActive || call.Err() != nil {
		p.mu.Unlock()
		source.state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	if p.completion == nil {
		c, e := prepareLocalTerminalCompletion(r)
		if e != nil {
			p.mu.Unlock()
			source.state.mu.Unlock()
			return e
		}
		p.completion = c
	}
	c := p.completion
	if !c.original() || c.cleanup != r {
		p.mu.Unlock()
		source.state.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	p.publicationActive, p.publicationCancel, p.publicationDone = true, cancel, make(chan struct{})
	started = true
	p.mu.Unlock()
	source.state.mu.Unlock()
	materials, e := c.readRetained(call)
	if e != nil {
		return e
	}
	defer materials.Release()
	store := p.parent.entry.owner.store
	matched := false
	var replayError error
	// A bounded observation first allows exact lost-ACK replay after the original
	// cleanup deadline, WITHOUT granting another first-commit lifetime.
	if e = store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		matched, replayError = c.observeReplay(ctx, q)
		return replayError
	}); e != nil || matched {
		// Preserve only this fixed content-free canonical conflict. The shared
		// authority adapter otherwise deliberately collapses backend diagnostics;
		// no provider error, record body or authority detail escapes here.
		if e != nil && errors.Is(replayError, ErrRunFinalizationConflict) {
			return ErrRunFinalizationConflict
		}
		return e
	}
	p.mu.Lock()
	live := !p.cleared && p.completion == c && !p.parent.failed.Load()
	p.mu.Unlock()
	if !live || !time.Now().Before(p.parent.deadline) {
		return domain.ErrMembershipDenied
	}
	first, finish := context.WithDeadline(call, p.parent.deadline)
	defer finish()
	e = store.ownedAuthorityTx(first, func(ctx context.Context, q *ownedAuthorityQuery) error {
		matched, replayError = c.observeReplay(ctx, q)
		if replayError != nil || matched {
			return replayError
		}
		return c.firstCommit(ctx, q, materials)
	})
	if e != nil && errors.Is(replayError, ErrRunFinalizationConflict) {
		return ErrRunFinalizationConflict
	}
	return e
}
