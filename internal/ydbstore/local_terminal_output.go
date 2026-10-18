package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// LocalPreparedOutputV1 owns the original producer's immutable encoding buffers.
// Predicted references are DATA: no PUT, registration, cleanup or completion is
// attested. Only a future same-owner publisher may consume these private bytes.
type LocalPreparedOutputV1 struct {
	self    *LocalPreparedOutputV1
	parent  *LocalTerminalV1
	mu      *sync.Mutex
	cleared bool
	// Shared original-owner state under mu. Any publication attempt is burned,
	// including uncertain registration or storage outcomes; copies cannot retry.
	publicationAttempted bool
	publicationActive    bool
	publicationCancel    context.CancelFunc
	publicationDone      chan struct{}
	published            bool
	publicationRun       domain.Run
	cleanupAttempted     bool
	cleanup              *LocalTerminalCleanupV1
	plan                 domain.TerminalCustodyPlanV1
	manifest             domain.ArtifactManifest
	drafts               []domain.SessionEventDraft
	canonical            *canonicaloutput.PlanV1
	capsule              []byte
}

type localCapsuleEventV1 struct {
	ID             domain.SessionEventID   `json:"id"`
	Kind           domain.SessionEventKind `json:"kind"`
	IdempotencyKey domain.IdempotencyKey   `json:"idempotency_key"`
	Payload        domain.BlobRef          `json:"payload"`
	DisplayText    string                  `json:"display_text"`
}

// PrepareOutput accepts no caller output/frontier/ref. It consumes the exact
// source privately installed by Capture, while the original bridge is alive.
// Failure burns this planning allowance; it never authorizes retry or erasure.
func (h *LocalTerminalV1) PrepareOutput(ctx context.Context) (_ *LocalPreparedOutputV1, err error) {
	deny := func() (*LocalPreparedOutputV1, error) { return nil, domain.ErrMembershipDenied }
	if !h.original() || h.failed.Load() || ctx == nil || ctx.Err() != nil || !h.entry.preparationMu.TryLock() {
		return deny()
	}
	defer h.entry.preparationMu.Unlock()
	if h.outputAttempted || !h.captured.Load() || h.source == nil || h.source.self != h.source || h.source.entry != h.entry || h.entry.invocationContext.Err() != nil {
		return deny()
	}
	h.outputAttempted = true
	defer func() {
		if err != nil {
			h.failed.Store(true)
		}
	}()
	// Root actual I/O in the ORIGINAL Source, not its asynchronous invocation
	// bridge. Closing Source after a current verifier must cancel the remainder
	// of that same SQL/blob phase synchronously for direct-entry callers too.
	root, finishRoot, e := h.entry.localCompactionLifetimeContext(ctx)
	if e != nil {
		return deny()
	}
	defer finishRoot()
	call, cancel := context.WithDeadline(root, h.deadline)
	defer cancel()
	call, finish := ownedAuthorityCallContext(call)
	defer finish()
	s := h.source
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	if s.state.cleared || s.state.view == nil || call.Err() != nil || h.entry.terminalOutputs == nil {
		return deny()
	}
	cap := h.entry.terminalOutputs.CapsuleLimit()
	if cap == 0 || cap > domain.MaxTerminalCapsuleBytesV1 {
		return deny()
	}
	store := h.entry.owner.store
	var run domain.Run
	var at time.Time
	if store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		var e error
		run, e = s.rejoinRunLocked(ctx, q, nil, nil)
		at = q.at
		return e
	}) != nil || run.Status != domain.RunRunning || run.CancellationRequestedAt != nil || run.TerminalCustody != nil {
		return deny()
	}
	// Reads the original complete retained closure without requiring a previous
	// generation's now-stale operation frontier to become current again.
	if _, e := h.entry.capturedRetainedParent(call, s, nil); e != nil {
		return deny()
	}
	ops := s.state.operations
	if len(ops) == 0 || len(ops) > 12 || len(ops) != len(s.state.snapshot.Operations) || uint64(len(ops)) != s.state.snapshot.Accounting.Aggregate.Operations {
		return deny()
	}
	scope := ownedOperationScope(h.entry.parent)
	profile := h.entry.owner.configuration
	pd, e := profile.Digest()
	if e != nil {
		return deny()
	}
	var normalized uint64
	var answer string
	for i, o := range ops {
		op := o.Reservation.Operation
		row := s.state.snapshot.Operations[i]
		if row.observation == nil || !reflect.DeepEqual(o.Reservation, row.reservation) || !reflect.DeepEqual(o.Observation, *row.observation) ||
			op.Scope != scope || op.Sequence != uint64(i+1) || o.Reservation.ValidateForParent(h.entry.parent.Authority, h.entry.parent.Reservation) != nil ||
			o.Observation.ValidateForReservation(o.Reservation) != nil || o.Observation.Outcome != domain.AttemptOperationCompletedV1 ||
			(op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) || len(o.Response) == 0 || len(o.Response) > 64<<10 ||
			uint64(len(o.Response)) > (384<<10)-normalized || uint64(len(o.Response)) != o.Observation.ResponseBytes || uint64(len(o.Request)) != op.RequestBytes {
			return deny()
		}
		normalized += uint64(len(o.Response))
		rs, ss := sha256.Sum256(o.Request), sha256.Sum256(o.Response)
		if hex.EncodeToString(rs[:]) != op.RequestDigest || hex.EncodeToString(ss[:]) != o.Observation.ResponseDigest {
			return deny()
		}
		decoded, e := configuredinference.DecodeNormalizedResponseBodyV1(profile, o.Request, o.Response, op.Kind)
		if e != nil || decoded.Stop != configuredinference.StopFinalV1 || decoded.Call != nil {
			// A concrete original typed tool composer is required by #191. Never
			// guess a tool pair from call IDs or silently discard tool effects.
			if decoded.Call != nil {
				clear(decoded.Call.Arguments)
			}
			return deny()
		}
		usage, e := decoded.Usage.OperationUsage()
		if e != nil || !reflect.DeepEqual(usage, o.Observation.Usage) {
			return deny()
		}
		if i == len(ops)-1 {
			if op.Kind != domain.AttemptOperationModelV1 {
				return deny()
			}
			answer = decoded.Text
		}
	}
	manifest := domain.ArtifactManifest{ID: canonicaloutput.ManifestIDV1(run.ID), TenantID: run.TenantID, RunID: run.ID, CreatedAt: at}
	if manifest.ValidateForRun(run) != nil {
		return deny()
	}
	encoded, e := canonicaloutput.EncodeCompletionV1(run.ID, s.state.snapshot.Job.Job.Limits, ports.ExecutionResult{Summary: answer}, manifest.ID)
	if e != nil {
		return deny()
	}
	success := false
	defer func() {
		if !success {
			encoded.Release()
		}
	}()
	events := encoded.Events()
	if len(events) != 1 || len(events) > 25 || events[0].Kind != domain.SessionEventAssistantMessage {
		return deny()
	}
	drafts := make([]domain.SessionEventDraft, len(events))
	identities := make([]localCapsuleEventV1, len(events))
	for i, event := range events {
		draft, e := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, at)
		if e != nil {
			return deny()
		}
		draft.DisplayText = event.DisplayText
		drafts[i] = draft
		identities[i] = localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}
	}
	body, e := encodeLocalTerminalCapsule(scope, pd, s.state.digest, ops, run.DerivedContext, identities, manifest, cap)
	if e != nil {
		return deny()
	}
	defer func() {
		if !success {
			clear(body)
		}
	}()
	// Keep the same complete current snapshot through all original closure reads
	// and encoding. A changed cancellation/deletion/head/lease cannot be hidden
	// by the previously captured DATA.
	if store.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, e := s.rejoinRunLocked(ctx, q, nil, nil)
		return e
	}) != nil || ctx.Err() != nil || call.Err() != nil || h.entry.invocationContext.Err() != nil {
		return deny()
	}
	life, lifeErr := h.entry.owner.source.ExecutionLifetime()
	if lifeErr != nil || life.Err() != nil || ctx.Err() != nil || call.Err() != nil {
		return deny()
	}
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	key, e := domain.TerminalCustodyObjectKeyV1(scope, sha)
	if e != nil {
		return deny()
	}
	plan := domain.TerminalCustodyPlanV1{Scope: scope, FrontierDigest: s.state.digest, Capsule: domain.BlobRef{TenantID: run.TenantID, Key: key, Size: int64(len(body)), SHA256: sha}}
	if plan.Validate() != nil {
		return deny()
	}
	p := &LocalPreparedOutputV1{parent: h, mu: new(sync.Mutex), plan: plan, manifest: manifest, drafts: drafts, canonical: encoded, capsule: body}
	p.self = p
	h.output = p
	success = true
	return p, nil
}

// Emit exact normalized JSON bytes, not a remarshal/base64 copy. Every other
// value is already bounded domain material. The builder rejects the encoded
// first-over boundary before copying it into the admitted capsule buffer.
func encodeLocalTerminalCapsule(scope domain.AttemptOperationScopeV1, profile, frontier string, ops []LocalCompactionOperationV1, derived *domain.RunDerivedContextV1, events []localCapsuleEventV1, manifest domain.ArtifactManifest, cap uint64) ([]byte, error) {
	materials := make([]localCapsuleOperationMaterial, len(ops))
	for i, o := range ops {
		materials[i] = localCapsuleOperationMaterial{reservation: o.Reservation, observation: o.Observation, normalized: o.Response, bytes: uint64(len(o.Response))}
	}
	sink := localCapsuleSink{limit: cap, emit: true}
	if err := encodeLocalTerminalCapsuleTo(&sink, scope, profile, frontier, materials, derived, events, 0, manifest); err != nil {
		clear(sink.body)
		return nil, err
	}
	return sink.body, nil
}

// Both real encoding and prospective counting traverse this exact closed wire.
// Prospective lengths are private arithmetic DATA, never signed observations.
type localCapsuleOperationMaterial struct {
	reservation domain.AttemptOperationReservationV1
	observation domain.AttemptOperationObservationV1
	normalized  []byte
	bytes       uint64
}
type localCapsuleSink struct {
	limit, size  uint64
	emit, failed bool
	body         []byte
}

func (s *localCapsuleSink) count(n uint64) {
	if s.failed || n > s.limit-s.size {
		s.failed = true
		return
	}
	s.size += n
}
func (s *localCapsuleSink) add(raw []byte) {
	s.count(uint64(len(raw)))
	if s.emit && !s.failed {
		s.body = append(s.body, raw...)
	}
}
func (s *localCapsuleSink) value(v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		s.failed = true
		return
	}
	s.add(raw)
}
func encodeLocalTerminalCapsuleTo(sink *localCapsuleSink, scope domain.AttemptOperationScopeV1, profile, frontier string, ops []localCapsuleOperationMaterial, derived *domain.RunDerivedContextV1, events []localCapsuleEventV1, finalDisplayUpper uint64, manifest domain.ArtifactManifest) error {
	if sink.limit == 0 || sink.limit > domain.MaxTerminalCapsuleBytesV1 || len(ops) == 0 || len(ops) > 12 || len(events) == 0 || len(events) > 25 || (sink.emit && finalDisplayUpper != 0) {
		return domain.ErrMembershipDenied
	}
	add, value := sink.add, sink.value
	add([]byte(`{"schema":"sessionless.terminal-custody.v1","scope":`))
	value(scope)
	add([]byte(`,"profile_digest":`))
	value(profile)
	add([]byte(`,"codec_version":1,"frontier_digest":`))
	value(frontier)
	add([]byte(`,"operations":[`))
	var normalized uint64
	for i, o := range ops {
		if o.bytes == 0 || o.bytes > 64<<10 || o.bytes > (384<<10)-normalized || (sink.emit && uint64(len(o.normalized)) != o.bytes) {
			sink.failed = true
			break
		}
		normalized += o.bytes
		if i > 0 {
			add([]byte(","))
		}
		add([]byte(`{"reservation":`))
		value(o.reservation)
		add([]byte(`,"observation":`))
		value(o.observation)
		add([]byte(`,"normalized_body":`))
		if sink.emit {
			add(o.normalized)
		} else {
			sink.count(o.bytes)
		}
		projection := "retained_only"
		if o.reservation.Operation.Kind == domain.AttemptOperationCompactionV1 {
			projection = "compaction"
		}
		if i == len(ops)-1 {
			projection = "assistant"
		}
		add([]byte(`,"projection":`))
		value(projection)
		add([]byte(`,"event_ids":`))
		if projection == "assistant" {
			value([]domain.SessionEventID{events[len(events)-1].ID})
		} else {
			add([]byte("[]"))
		}
		add([]byte("}"))
	}
	add([]byte(`],"derived_context":`))
	value(derived)
	add([]byte(`,"events":`))
	// Keep field order identical to localCapsuleEventV1's canonical JSON. The
	// private counting arm substitutes only the future escaped display length.
	add([]byte("["))
	for i, e := range events {
		if i > 0 {
			add([]byte(","))
		}
		add([]byte(`{"id":`))
		value(e.ID)
		add([]byte(`,"kind":`))
		value(e.Kind)
		add([]byte(`,"idempotency_key":`))
		value(e.IdempotencyKey)
		add([]byte(`,"payload":`))
		value(e.Payload)
		add([]byte(`,"display_text":`))
		if i == len(events)-1 && finalDisplayUpper != 0 {
			sink.count(finalDisplayUpper)
		} else {
			value(e.DisplayText)
		}
		add([]byte("}"))
	}
	add([]byte("]"))
	add([]byte(`,"public_manifest":`))
	value(manifest)
	add([]byte("}"))
	if sink.failed {
		return ErrTerminalCapacity
	}
	return nil
}

func (p *LocalPreparedOutputV1) original() bool {
	return p != nil && p.self == p && p.mu != nil && p.parent.original() && p.parent.output == p
}
func (p *LocalPreparedOutputV1) Plan() (domain.TerminalCustodyPlanV1, error) {
	if !p.original() {
		return domain.TerminalCustodyPlanV1{}, domain.ErrMembershipDenied
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cleared {
		return domain.TerminalCustodyPlanV1{}, domain.ErrMembershipDenied
	}
	return p.plan, nil
}
func (p *LocalPreparedOutputV1) Drafts() ([]domain.SessionEventDraft, error) {
	if !p.original() {
		return nil, domain.ErrMembershipDenied
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cleared {
		return nil, domain.ErrMembershipDenied
	}
	return append([]domain.SessionEventDraft(nil), p.drafts...), nil
}
func (p *LocalPreparedOutputV1) Clear() {
	if !p.original() {
		return
	}
	p.mu.Lock()
	p.cleared = true
	cancel := p.publicationCancel
	if !p.publicationActive {
		p.releaseOutputLocked()
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Only the actual writer may release live buffers. Clear revokes immediately,
// but leaves those bytes owned until that writer has actually returned.
func (p *LocalPreparedOutputV1) releaseOutputLocked() {
	clear(p.capsule)
	p.capsule = nil
	p.canonical.Release()
	p.canonical = nil
	p.drafts = nil
	clear(p.manifest.Artifacts)
	p.manifest.Artifacts = nil
}

// StopAndJoin revokes even an unused original owner. It joins the actual
// publisher, not a free session gate, and never renews the original terminal
// deadline. Timeout is not evidence that storage or its borrowed dependencies
// have stopped; the live writer retains its buffers until actual completion.
func (p *LocalPreparedOutputV1) StopAndJoin(ctx context.Context) error {
	if !p.original() || ctx == nil {
		return domain.ErrMembershipDenied
	}
	p.mu.Lock()
	p.cleared = true
	cancel, done, active := p.publicationCancel, p.publicationDone, p.publicationActive
	if !active {
		p.releaseOutputLocked()
	}
	p.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	if !active {
		return nil
	}
	call, finish := context.WithDeadline(ctx, p.parent.deadline)
	defer finish()
	select {
	case <-done:
		return nil
	case <-call.Done():
		select {
		case <-done:
			return nil
		default:
			return domain.ErrMembershipDenied
		}
	}
}
func (LocalPreparedOutputV1) String() string     { return "LocalPreparedOutputV1{private:[redacted]}" }
func (p LocalPreparedOutputV1) GoString() string { return p.String() }
func (LocalPreparedOutputV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("local prepared output is transient ownership")
}
