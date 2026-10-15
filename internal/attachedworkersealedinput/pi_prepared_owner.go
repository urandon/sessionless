package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// PiPreparedOwnerConfigV1 is bounded installed recipe DATA, not a qualified
// native/model/fit manifest or an activation flag. The ordinary stack does not
// select this explicit preparation composition.
type PiPreparedOwnerConfigV1 struct {
	Request             pisdkmedia.RequestDataV1
	ModelJSON           []byte
	TimeoutMilliseconds uint64
	CredentialFileName  string
	RetirementGrace     time.Duration
}

// PiPreparedOwnerV1 joins the genuine preparation components and original
// runner retirement. Full local installed-native/complete-fit admission is
// STILL absent: every complete preparation returns OWNED DATA + MissingProducer
// before any phase/Issue/execution. There is no fake qualifier/config switch.
// Neither a successful HTTP reply nor internally consistent DATA can elevate
// this owner. It is connected engineering work, not a runnable Pi profile.
type PiPreparedOwnerV1 struct {
	mu          sync.Mutex
	source      *SessionSourceFactory
	supervisor  *attachedworkerdaemon.Supervisor
	credentials *providercredential.InvocationService
	config      PiPreparedOwnerConfigV1
	current     *piOwnedPreparation
}

type piOwnedPreparation struct {
	owner      *PiPreparedOwnerV1
	identity   attachedworkerdaemon.InvocationIdentity
	ledger     *codexexec.CanonicalMediaLedgerV1
	baseline   uint64
	stageLive  uint64 // frozen at Prepare return, excludes later original-runner/output reservations
	data       attachedworkerdaemon.PreparedAPIDataV1
	inputs     [2]*attachedworkerdaemon.VerifiedAttachedInputV1
	requests   [2]attachedworkerdaemon.PreparedAPIRequestV1
	processes  [2]attachedworkerdaemon.AttemptSpec
	projection *pisdkmedia.RequestProjectionV1
	file       *providercredential.PreparedImmutableAPIFileV1
	init       *pisdkmedia.InitPlanV1
	execution  *pisdkmedia.PreparedPiExecutionV1
	original   context.Context
	grace      time.Duration
	mu         sync.Mutex
	retirement context.Context
	cancel     context.CancelFunc
	cleared    bool
	cleanupErr error
}

func (*PiPreparedOwnerV1) String() string      { return "PiPreparedOwnerV1{private:[redacted]}" }
func (p *PiPreparedOwnerV1) GoString() string  { return p.String() }
func (*piOwnedPreparation) String() string     { return "piOwnedPreparation{private:[redacted]}" }
func (r *piOwnedPreparation) GoString() string { return r.String() }

// The shared credential filesystem is runtime-owned. Preparation neither
// constructs another namespace nor closes/removes this one during retirement.
func NewPiPreparedOwnerV1(source *SessionSourceFactory, supervisor *attachedworkerdaemon.Supervisor, credentials *providercredential.InvocationService,
	files *credentiallifecycle.PinnedSecretFileSystem, c PiPreparedOwnerConfigV1) (*PiPreparedOwnerV1, error) {
	if source == nil || source.phaseEndpoint == "" || supervisor == nil || supervisor.PreparedProcessTimeoutV1() <= 0 ||
		credentials == nil || !credentials.UsesPreparedFileSystemV1(files) || c.Request.Profile == nil || c.Request.Model == nil ||
		c.Request.Profile.Validate() != nil || c.TimeoutMilliseconds == 0 || c.TimeoutMilliseconds > 2147483647 ||
		c.RetirementGrace <= 0 || c.RetirementGrace > 30*time.Second || len(c.Request.Headers) != 11 ||
		len(c.ModelJSON) == 0 || uint64(len(c.ModelJSON)) > min(c.Request.Profile.Limits.NativeRequestBytes, c.Request.Profile.Limits.ProcessFrameBytes) ||
		len(c.Request.FinalSchema) == 0 || uint64(len(c.Request.FinalSchema)) > min(c.Request.Profile.Limits.NativeRequestBytes, c.Request.Profile.Limits.ProcessFrameBytes) ||
		len(c.CredentialFileName) == 0 || len(c.CredentialFileName) > 128 {
		return nil, ErrInvalid
	}
	var headerBytes uint64
	for _, h := range c.Request.Headers {
		cap := c.Request.Profile.Limits.RequestHeaderBytes
		if uint64(len(h.Name)) > cap-headerBytes {
			return nil, ErrInvalid
		}
		headerBytes += uint64(len(h.Name))
		if uint64(len(h.Value)) > cap-headerBytes {
			return nil, ErrInvalid
		}
		headerBytes += uint64(len(h.Value))
	}
	// Lifetime-owned immutable recipe, not an invocation input copy. Every
	// per-attempt representation below is separately charged before creation.
	profile, model := *c.Request.Profile, *c.Request.Model
	c.Request.Profile, c.Request.Model = &profile, &model
	if model.ThinkingOff != nil {
		v := *model.ThinkingOff
		model.ThinkingOff = &v
	}
	if c.Request.ReasoningEffort != nil {
		v := *c.Request.ReasoningEffort
		c.Request.ReasoningEffort = &v
	}
	if c.Request.ReasoningSummary != nil {
		v := *c.Request.ReasoningSummary
		c.Request.ReasoningSummary = &v
	}
	if c.Request.Temperature != nil {
		v := *c.Request.Temperature
		c.Request.Temperature = &v
	}
	c.Request.Headers = append([]pisdkmedia.HeaderDataV1(nil), c.Request.Headers...)
	c.Request.FinalSchema = bytes.Clone(c.Request.FinalSchema)
	c.ModelJSON = bytes.Clone(c.ModelJSON)
	return &PiPreparedOwnerV1{source: source, supervisor: supervisor, credentials: credentials, config: c}, nil
}

func (p *PiPreparedOwnerV1) PrepareOriginalAPIInvocation(ctx context.Context, id attachedworkerdaemon.InvocationIdentity,
	input *attachedworkerdaemon.VerifiedAttachedInputV1, process attachedworkerdaemon.AttemptSpec,
	acquisition attachedworkerdaemon.PreparedInputAcquisitionV1) (*attachedworkerdaemon.PreparedAPIDataV1, error) {
	if p == nil || ctx == nil || ctx.Err() != nil || input == nil || id.Validate() != nil || len(process.Stdin) != 0 {
		return nil, ErrInvalid
	}
	a, ok := acquisition.(*attachedworkerdaemontransport.PiInputAcquisitionV1)
	if !ok || a == nil || a.Profile() != *p.config.Request.Profile || !p.source.OwnsOriginalPiInput(a) {
		return nil, ErrInvalid
	}
	ledger, ok := a.OriginalLedger().(*codexexec.CanonicalMediaLedgerV1)
	original, entry := a.OriginalContext(), a.OriginalEntry()
	if !ok || ledger == nil || original == nil || original.Err() != nil || entry.IsZero() || entry == entry.Round(0) {
		return nil, ErrInvalid
	}
	deadline, hasDeadline := ctx.Deadline()
	originalDeadline, hasOriginalDeadline := original.Deadline()
	if !hasDeadline || !hasOriginalDeadline || deadline != originalDeadline || deadline == deadline.Round(0) || !time.Now().Before(deadline) {
		return nil, ErrInvalid
	}
	p.mu.Lock()
	if p.current != nil {
		p.mu.Unlock()
		return nil, ErrUnavailable
	}
	if a.BeginPreparation(input) != nil {
		p.mu.Unlock()
		return nil, ErrInvalid
	}
	baseline := ledger.Accounting().LiveBytes
	metadata := uint64(reflect.TypeOf(piOwnedPreparation{}).Size())
	if ledger.Reserve(metadata, metadata, 0) != nil {
		p.mu.Unlock()
		return nil, ErrUnavailable
	}
	r := &piOwnedPreparation{owner: p, identity: id, ledger: ledger, baseline: baseline, original: original, grace: p.config.RetirementGrace}
	r.data.Ledger, r.data.Retirement, r.data.Custody = ledger, r, r
	p.current = r
	p.mu.Unlock()
	defer func() { r.stageLive = ledger.Accounting().LiveBytes - baseline }()
	// From here every partial error transfers this private resource record to
	// the ORIGINAL runner. Retirement must not depend on DTO alias eligibility.
	data := &r.data
	var digest [64]byte
	hex.Encode(digest[:], process.ExecutableDigest[:])
	if string(digest[:]) != p.config.Request.Profile.Sources.NativeArtifactDigest {
		return data, ErrInvalid
	}
	for i := range r.inputs {
		if err := r.fundClone(input); err != nil {
			return data, err
		}
		v, err := input.Clone()
		if err != nil {
			return data, err
		}
		r.inputs[i] = v
	}
	data.ParentInput, data.ChildInput = r.inputs[0], r.inputs[1]
	projection, err := pisdkmedia.CompileOriginalRequestV1(ctx, id, r.inputs[0], p.config.Request, ledger)
	if err != nil {
		return data, err
	}
	r.projection = projection
	for i := range r.requests {
		v, err := projection.PrepareRequestDataV1(p.config.ModelJSON, p.config.Request.FinalSchema)
		if err != nil {
			return data, err
		}
		r.requests[i] = v
	}
	data.ParentRequest, data.ChildRequest = r.requests[0], r.requests[1]
	b := r.inputs[0].Job.HarnessBinding
	expires := r.inputs[0].AcceptedAttempt.LeaseExpiresAt
	if b.EvidenceExpiresAt == nil {
		return data, ErrInvalid
	}
	if b.EvidenceExpiresAt.Before(expires) {
		expires = *b.EvidenceExpiresAt
	}
	data.Issue = ports.ProviderCredentialIssueRequestV1{HarnessBinding: b, WorkerID: string(id.WorkerID), LeaseID: id.LeaseID, LeaseFence: id.FenceToken, ExpiresAt: expires}
	h := r.inputs[0].AcceptedAttempt
	data.Authorization = ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID,
		EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1,
		LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest,
		CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}
	fileMetadata := providercredential.PreparedImmutableAPIFileMetadataBytesV1()
	// Its fixed binding validation is a subset of the already sealed job. The
	// real service owns exact retained plan layout; native pathname/allocator
	// fit remains explicitly unproven and cannot be inferred from this debit.
	if ledger.Reserve(8*(1<<20)+fileMetadata, fileMetadata, 0) != nil {
		return data, ErrUnavailable
	}
	r.file, err = p.credentials.PrepareImmutableAPIFileV1(ctx, data.Issue, p.config.CredentialFileName, int64(p.config.Request.CredentialMaxBytes))
	if err != nil {
		return data, err
	}
	r.init, err = pisdkmedia.PrepareInitV1(ctx, projection, p.config.ModelJSON, p.config.TimeoutMilliseconds, r.file)
	if err != nil {
		return data, err
	}
	args := r.init.EntryArguments()
	for i := range r.processes {
		v, err := r.copyProcess(process, args, i == 1)
		if err != nil {
			return data, err
		}
		r.processes[i] = v
	}
	data.ParentProcess, data.ChildProcess, data.InitArguments = r.processes[0], r.processes[1], args
	r.execution, err = pisdkmedia.PreparePiExecutionV1(p.supervisor, projection, r.init, data.ParentRequest, data.ParentProcess)
	if err != nil {
		return data, err
	}
	data.Entry, data.CallerDeadline = entry, originalDeadline
	data.InvocationDeadline = entry.Add(r.inputs[0].Job.Limits.MaxRuntime)
	data.ProcessDeadline = entry.Add(p.supervisor.PreparedProcessTimeoutV1())
	if ctx.Err() != nil || original.Err() != nil || !p.source.OwnsOriginalPiInput(a) {
		return data, ErrUnavailable
	}
	// Do NOT fabricate native-fit, installed artifacts, model/context/vision
	// calibration, egress or conservative safety slack from structural P. The
	// real Source remains authoritative for owner/API/tariff decisions but an
	// Applied reply cannot repair absent local full-input/native admission.
	// Consequently no phase, Issue or execution is reachable in this version.
	return data, pisdkmedia.ErrMissingProducer
}

func (r *piOwnedPreparation) fundClone(input *attachedworkerdaemon.VerifiedAttachedInputV1) error {
	if input.Job.ContextWindow == nil || input.Job.ContextWindow.ThroughSequence == 0 || input.Job.ContextWindow.ThroughSequence > attachedworkerdaemontransport.MaxSealedContextRecordsV1 {
		return ErrInvalid
	}
	l := min(uint64(1<<20), r.owner.config.Request.Profile.Limits.CanonicalInputBytes, input.Job.Limits.MaxInputBytes, input.Job.Limits.MaxContextBytes)
	c := input.CanonicalContext
	// Same collection ceilings as VerifiedAttachedInputV1.bounded and the
	// sealed canonical carrier. Pointer identity does not freeze caller DATA.
	if l == 0 || len(input.Artifacts) > 64 || c == nil || len(c.EventBodies) > attachedworkerdaemontransport.MaxSealedContextRecordsV1 ||
		len(c.Input.Events) > attachedworkerdaemontransport.MaxSealedContextRecordsV1 || len(c.Attachments) > 64 {
		return ErrInvalid
	}
	e := max(input.Job.ContextWindow.ThroughSequence, uint64(len(c.Input.Events)), uint64(len(c.EventBodies)))
	if c.Input.Snapshot != nil {
		if c.Input.Snapshot.EventCount > attachedworkerdaemontransport.MaxSealedContextRecordsV1 {
			return ErrInvalid
		}
		e = max(e, c.Input.Snapshot.EventCount)
	}
	items := uint64(len(input.Artifacts) + len(c.EventBodies) + len(c.Attachments) + 1)
	if r.ledger.Reserve(64*items, 0, items) != nil {
		return ErrUnavailable
	}
	// Same conservative canonical Stage-A logical schedule BEFORE ValidateFor,
	// copyShape, Marshal/Unmarshal and exact artifact arrays. Native/Go allocator
	// object/heap fit is not certified by these retained payload appearances.
	arrays := 3 * uint64(len(input.Artifacts)) * uint64(reflect.TypeOf(attachedworkerdaemon.VerifiedAttachedArtifactV1{}).Size())
	h := uint64(0)
	consume := func(n int) bool {
		if uint64(n) > l-h {
			return false
		}
		h += uint64(n)
		return true
	}
	if !consume(len(input.Context)) || !consume(len(c.SnapshotBytes)) {
		return ErrInvalid
	}
	for _, v := range input.Artifacts {
		if !consume(len(v.Body)) {
			return ErrInvalid
		}
	}
	for _, v := range c.EventBodies {
		if !consume(len(v)) {
			return ErrInvalid
		}
	}
	for _, v := range c.Attachments {
		if !consume(len(v.Body)) {
			return ErrInvalid
		}
	}
	t := uint64(len(c.Attachments))
	return r.ledger.Reserve((6*e+14)*l+arrays, 8*l+h+24*(e+t+1)+arrays, 3*l)
}

func (r *piOwnedPreparation) copyProcess(original attachedworkerdaemon.AttemptSpec, args [10]string, child bool) (attachedworkerdaemon.AttemptSpec, error) {
	l := r.owner.config.Request.Profile.Limits
	count := uint64(len(original.Arguments)) + uint64(len(original.Environment)) + uint64(len(original.AdditionalReadRoots)) + 11
	if count > l.JSONNodes || len(original.Arguments) == 0 {
		return attachedworkerdaemon.AttemptSpec{}, ErrInvalid
	}
	if r.ledger.Reserve(64*count, 64*count, count) != nil {
		return attachedworkerdaemon.AttemptSpec{}, ErrUnavailable
	}
	bytes := uint64(len(original.Executable))
	add := func(s string) bool {
		if bytes > l.ProcessFrameBytes || uint64(len(s)) > l.ProcessFrameBytes-bytes {
			return false
		}
		bytes += uint64(len(s))
		return true
	}
	for _, v := range original.Arguments {
		if !add(v) {
			return attachedworkerdaemon.AttemptSpec{}, ErrInvalid
		}
	}
	for _, v := range args {
		if !add(v) {
			return attachedworkerdaemon.AttemptSpec{}, ErrInvalid
		}
	}
	for _, v := range original.Environment {
		if !add(v.Name) || !add(v.Value) {
			return attachedworkerdaemon.AttemptSpec{}, ErrInvalid
		}
	}
	for _, v := range original.AdditionalReadRoots {
		if !add(v) {
			return attachedworkerdaemon.AttemptSpec{}, ErrInvalid
		}
	}
	if r.ledger.Reserve(4*bytes, bytes, 0) != nil {
		return attachedworkerdaemon.AttemptSpec{}, ErrUnavailable
	}
	v := original
	v.Arguments = make([]string, len(original.Arguments)+10)
	copy(v.Arguments, original.Arguments)
	copy(v.Arguments[len(original.Arguments):], args[:])
	v.Environment = append([]attachedworkerdaemon.EnvironmentVariable(nil), original.Environment...)
	rootSlots := len(original.AdditionalReadRoots)
	if child {
		rootSlots++
	}
	if rootSlots != 0 {
		v.AdditionalReadRoots = make([]string, rootSlots)
		copy(v.AdditionalReadRoots, original.AdditionalReadRoots)
	}
	return v, nil
}

// No native qualification producer exists yet; neither direct calls nor an
// independently manufactured Applied reply can bypass the preparation error.
func (*PiPreparedOwnerV1) ObserveOriginalAPIPhase(context.Context, pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	return pisdkphasewire.Reply{}, pisdkmedia.ErrMissingProducer
}
func (*PiPreparedOwnerV1) RunPrepared(context.Context, attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	return attachedworkerdaemon.AttemptResult{}, pisdkmedia.ErrMissingProducer
}

func (r *piOwnedPreparation) Context() (context.Context, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retirement == nil {
		if r.cleared {
			return nil, ErrUnavailable
		}
		r.retirement, r.cancel = context.WithTimeout(context.WithoutCancel(r.original), r.grace)
	}
	return r.retirement, nil
}

func (p *PiPreparedOwnerV1) RetireOriginalAPIStaging(ctx context.Context, id attachedworkerdaemon.InvocationIdentity, data *attachedworkerdaemon.PreparedAPIDataV1) error {
	if p == nil {
		return ErrInvalid
	}
	p.mu.Lock()
	r := p.current
	p.mu.Unlock()
	if r == nil || id != r.identity || data != &r.data {
		return ErrInvalid
	}
	r.mu.Lock()
	valid := ctx != nil && ctx == r.retirement && ctx.Err() == nil
	r.mu.Unlock()
	// All records in this version are explicitly ineligible before Issue. Own
	// actual partial cleanup even when the runner cannot accept DTO.Custody.
	r.Clear()
	r.mu.Lock()
	defer r.mu.Unlock()
	if !valid {
		return errors.Join(ErrUnavailable, r.cleanupErr)
	}
	return r.cleanupErr
}

func (r *piOwnedPreparation) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cleared {
		return
	}
	before := r.ledger.Accounting().LiveBytes
	if r.execution != nil {
		r.cleanupErr = r.execution.Close()
	}
	if r.cleanupErr != nil {
		return
	} // do not clear borrowed buffers under unjoined execution
	if r.init != nil {
		r.init.Clear()
	}
	if r.projection != nil {
		r.projection.Clear()
	}
	for _, v := range r.inputs {
		if v != nil {
			v.Clear()
		}
	}
	for _, v := range r.requests {
		clear(v.Body)
		clear(v.Model)
		clear(v.Schema)
		clear(v.Headers)
	}
	for _, v := range r.processes {
		clear(v.Arguments)
		clear(v.Environment)
		clear(v.AdditionalReadRoots)
		clear(v.Stdin)
	}
	r.inputs, r.requests, r.processes = [2]*attachedworkerdaemon.VerifiedAttachedInputV1{}, [2]attachedworkerdaemon.PreparedAPIRequestV1{}, [2]attachedworkerdaemon.AttemptSpec{}
	r.file, r.init, r.projection, r.execution = nil, nil, nil, nil
	r.data = attachedworkerdaemon.PreparedAPIDataV1{} // drop every transferred buffer alias, not caller data
	// Synchronous failed stages can reserve before returning nil. After all
	// actual owned objects are relinquished, retire ONLY the remaining stage
	// delta, never source/active-control baseline or cumulative work/nodes.
	now := r.ledger.Accounting().LiveBytes
	if now > before || before-now > r.stageLive {
		r.cleanupErr = ErrUnavailable
	} else {
		r.cleanupErr = r.ledger.EndLive(r.stageLive - (before - now))
	}
	if r.cancel != nil {
		r.cancel()
	}
	r.owner.mu.Lock()
	if r.owner.current == r && r.cleanupErr == nil {
		r.owner.current = nil
	}
	r.owner.mu.Unlock()
	r.cleared = r.cleanupErr == nil
}

var _ attachedworkerdaemon.PreparedAPIParentV1 = (*PiPreparedOwnerV1)(nil)
var _ attachedworkerdaemon.PreparedProcessRunner = (*PiPreparedOwnerV1)(nil)
