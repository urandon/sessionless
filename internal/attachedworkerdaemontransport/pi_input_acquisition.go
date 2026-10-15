package attachedworkerdaemontransport

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PiInputAcquisitionV1 is local accounting and ownership, NEVER source,
// credential, native-fit or profile-activation authority. Its limits come from
// the explicitly installed profile, not the HTTP response. It precedes every
// attempt-owned input copy and survives temporary materialization contexts.
// There is one sequential borrower; Clear is called only after that borrower
// has actually joined (including a late materialization result).
type PiInputAcquisitionV1 struct {
	mu                     sync.Mutex
	ctx                    context.Context
	entry                  time.Time
	profile                domain.AttachedPiSDKNativeProfileV1
	request                MaterializationRequestV1 // private funded digest copy
	ledger                 *codexexec.CanonicalMediaLedgerV1
	live                   uint64
	controlLive            uint64
	closed                 bool
	requestIssued          bool
	sourceStarted          bool
	sourceOwner            SealedInputSource // actual one-shot reader, identity only; never wire authority
	materializationStarted bool
	preparationStarted     bool
	input                  *attachedworkerdaemon.VerifiedAttachedInputV1
	inputBytes             int
}

func (*PiInputAcquisitionV1) String() string     { return "PiInputAcquisitionV1{data:[redacted]}" }
func (a *PiInputAcquisitionV1) GoString() string { return a.String() }

func NewPiInputAcquisitionV1(ctx context.Context, request MaterializationRequestV1, profile domain.AttachedPiSDKNativeProfileV1) (*PiInputAcquisitionV1, error) {
	entry := time.Now()
	if ctx == nil || ctx.Err() != nil || profile.Validate() != nil || request.acquisition != nil ||
		request.TenantID.Validate() != nil || request.OwnerUserID.Validate() != nil || request.WorkerID.Validate() != nil ||
		request.ConnectionID.Validate() != nil || request.EnrollmentGeneration == 0 || request.ConnectionGeneration == 0 ||
		request.AttemptSequence != 1 || request.Attempt.Validate() != nil {
		return nil, ErrInvalidAuthority
	}
	ledger, err := codexexec.NewCanonicalMediaLedgerV1(profile.Limits)
	if err != nil {
		return nil, ErrInvalidAuthority
	}
	a := &PiInputAcquisitionV1{ctx: ctx, entry: entry, profile: profile, request: request, ledger: ledger}
	// Six digest-copy cohorts: private owner, request transfer, claim, active
	// attempt, materialization handoff and source handoff. Metadata strings are immutable
	// borrowed values, not copied payloads. No cumulative debit is refunded.
	n := uint64(len(request.Attempt.ContextDigest)+len(request.Attempt.CapabilityDigest)+len(request.Attempt.PolicyDigest)) * 6
	if a.Reserve(n, n, 0) != nil {
		a.Clear()
		return nil, ErrInvalidAuthority
	}
	a.request = cloneMaterializationRequest(request)
	return a, nil
}

// Request transfers one funded request copy. Reentry cannot acquire a second
// input or reset the same attempt's accumulated work.
func (a *PiInputAcquisitionV1) Request() (MaterializationRequestV1, error) {
	if a == nil {
		return MaterializationRequestV1{}, ErrInvalidAuthority
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.requestIssued || a.ctx.Err() != nil {
		return MaterializationRequestV1{}, ErrInvalidAuthority
	}
	a.requestIssued = true
	r := cloneMaterializationRequest(a.request)
	r.acquisition = a
	return r, nil
}

func (a *PiInputAcquisitionV1) Reserve(copyBytes, liveBytes, nodes uint64) error {
	if a == nil {
		return ErrInvalidAuthority
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx.Err() != nil || a.ledger.Reserve(copyBytes, liveBytes, nodes) != nil {
		return ErrInvalidAuthority
	}
	a.live += liveBytes
	return nil
}
func (a *PiInputAcquisitionV1) Profile() domain.AttachedPiSDKNativeProfileV1 {
	if a == nil {
		return domain.AttachedPiSDKNativeProfileV1{}
	}
	return a.profile
}
func (a *PiInputAcquisitionV1) OriginalContext() context.Context {
	if a == nil {
		return nil
	}
	return a.ctx
}
func (a *PiInputAcquisitionV1) OriginalEntry() time.Time {
	if a == nil {
		return time.Time{}
	}
	return a.entry
}
func (a *PiInputAcquisitionV1) OriginalLedger() attachedworkerdaemon.PreparedAPILedgerV1 {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	return a.ledger
}
func (a *PiInputAcquisitionV1) MatchesRequest(request MaterializationRequestV1) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.matchesRequestLocked(request)
}

func (a *PiInputAcquisitionV1) matchesRequestLocked(request MaterializationRequestV1) bool {
	return !a.closed && a.ctx.Err() == nil && request.acquisition == a &&
		a.request.TenantID == request.TenantID && a.request.OwnerUserID == request.OwnerUserID &&
		a.request.WorkerID == request.WorkerID && a.request.ConnectionID == request.ConnectionID &&
		a.request.EnrollmentGeneration == request.EnrollmentGeneration && a.request.ConnectionGeneration == request.ConnectionGeneration &&
		a.request.AttemptSequence == request.AttemptSequence && sameAttemptBinding(a.request.Attempt, request.Attempt)
}

// BeginSourceRead spends the single original HTTP acquisition before copies
// or Do. A failure, ambiguity or exhausted budget never restores this slot.
func (a *PiInputAcquisitionV1) BeginSourceRead(request MaterializationRequestV1, source SealedInputSource) error {
	if a == nil {
		return ErrInvalidAuthority
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.matchesRequestLocked(request) || a.sourceStarted || source == nil || reflect.ValueOf(source).Kind() != reflect.Pointer || reflect.ValueOf(source).IsNil() {
		return ErrInvalidAuthority
	}
	a.sourceStarted = true
	a.sourceOwner = source
	return nil
}

// MatchesOriginalSource compares the concrete reader captured at the actual
// first read. The private Session factory supplies its own ClientSource; no
// wrapper/token/bearer or matching metadata substitutes for pointer identity.
// This is custody association DATA, not authenticated source/currentness proof.
func (a *PiInputAcquisitionV1) MatchesOriginalSource(source SealedInputSource) bool {
	if a == nil || source == nil || reflect.ValueOf(source).Kind() != reflect.Pointer || reflect.ValueOf(source).IsNil() {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return !a.closed && a.ctx.Err() == nil && a.sourceStarted && a.sourceOwner == source
}

// BeginPreparation spends this moved original input once, before another
// owner clones it or compiles a request. Failure never replenishes the slot.
func (a *PiInputAcquisitionV1) BeginPreparation(input *attachedworkerdaemon.VerifiedAttachedInputV1) error {
	if a == nil {
		return ErrInvalidAuthority
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.ctx.Err() != nil || a.preparationStarted || !a.sourceStarted || a.sourceOwner == nil || !a.materializationStarted || input == nil || input != a.input {
		return ErrInvalidAuthority
	}
	a.preparationStarted = true
	return nil
}

func (a *PiInputAcquisitionV1) beginMaterialization(request MaterializationRequestV1) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.matchesRequestLocked(request) || a.materializationStarted {
		return false
	}
	a.materializationStarted = true
	return true
}
func (a *PiInputAcquisitionV1) Clear() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	_ = a.ledger.EndLive(a.live - a.controlLive)
	a.live = 0
	clear(a.request.Attempt.ContextDigest)
	clear(a.request.Attempt.CapabilityDigest)
	clear(a.request.Attempt.PolicyDigest)
	a.request = MaterializationRequestV1{}
	a.input = nil
	a.sourceOwner = nil
}

// The accepted active request outlives Run, including ambiguous Terminal/ACK.
// Its already-funded digest partition transfers to the original adapter slot.
func (a *PiInputAcquisitionV1) retainActiveControl(request MaterializationRequestV1) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	n := uint64(len(request.Attempt.ContextDigest) + len(request.Attempt.CapabilityDigest) + len(request.Attempt.PolicyDigest))
	if !a.matchesRequestLocked(request) || a.controlLive != 0 || n == 0 || n > a.live {
		return ErrInvalidAuthority
	}
	a.controlLive = n
	return nil
}

// Called only after exact ACK and original journal/drain/receipt retirement,
// after the adapter relinquishes its active slot. No cumulative refund.
func (a *PiInputAcquisitionV1) retireActiveControl(request *MaterializationRequestV1) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.controlLive == 0 {
		return nil
	}
	clear(request.Attempt.ContextDigest)
	clear(request.Attempt.CapabilityDigest)
	clear(request.Attempt.PolicyDigest)
	if err := a.ledger.EndLive(a.controlLive); err != nil {
		return err
	}
	if !a.closed {
		a.live -= a.controlLive
	}
	a.controlLive = 0
	request.acquisition = nil
	return nil
}

func (r MaterializationRequestV1) PiInputAcquisitionV1() *PiInputAcquisitionV1 { return r.acquisition }

// NewPiBoundMaterializer selects only an explicit default-off accounting/input
// arm. Structural profile validation does not qualify a native implementation.
func NewPiBoundMaterializer(source SealedInputSource, profile domain.AttachedPiSDKNativeProfileV1, maxBytes int, now func() time.Time) (*BoundMaterializer, error) {
	if profile.Validate() != nil {
		return nil, ErrInvalidConfiguration
	}
	m, err := NewBoundMaterializer(source, maxBytes, now)
	if err != nil {
		return nil, err
	}
	m.piProfile = &profile
	return m, nil
}

func (m *BoundMaterializer) beginOriginalInput(ctx context.Context, request MaterializationRequestV1) (*PiInputAcquisitionV1, error) {
	if m.piProfile == nil {
		return nil, nil
	}
	return NewPiInputAcquisitionV1(ctx, request, *m.piProfile)
}

func (m *BoundMaterializer) takePiInput(ctx context.Context, request MaterializationRequestV1, input *SealedInputV1) (MaterializedInputV1, error) {
	a := request.acquisition
	if m.piProfile == nil || a == nil || !a.MatchesRequest(request) || a.profile != *m.piProfile || ctx.Err() != nil ||
		input == nil || input.Credential != nil || input.CanonicalContext == nil || input.Job.ContextWindow == nil ||
		m.now().UTC().UnixMicro() >= request.Attempt.ExpiresAtUnixMicro {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	job := input.Job
	limit := uint64(m.maxBytes)
	for _, n := range []uint64{a.profile.Limits.CanonicalInputBytes, job.Limits.MaxInputBytes, job.Limits.MaxContextBytes} {
		if n < limit {
			limit = n
		}
	}
	if limit == 0 || limit > 1<<20 || !BoundedSealedInputMetadataV1(job, input.Manifest, int(limit)) ||
		!boundedSealedMetadata(reflect.ValueOf(input.AcceptedAttempt), int(limit)) ||
		!BoundedCanonicalProofV1(input.CanonicalContext, int(limit)) || len(input.Artifacts) > maxSealedCollectionItems {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	// The same conservative canonical validation schedule used by Stage A:
	// source history projection/verification, complete encoded seal and digest
	// walks. Debit before any maps, growable history, metadata marshal or index.
	// This is logical work/payload accounting, not a native heap-fit certificate.
	events := job.ContextWindow.ThroughSequence
	if events == 0 || events > MaxSealedContextRecordsV1 || events > job.Limits.EffectiveMaxContextEvents() ||
		a.Reserve((6*events+14)*limit, 8*limit+uint64(len(input.Artifacts))*uint64(reflect.TypeOf(attachedworkerdaemon.VerifiedAttachedArtifactV1{}).Size()), 3*limit) != nil {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	if !attachedworkerdaemon.BoundedJSONAllocationV1(*input, int(limit)) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	encoded, encodeErr := json.Marshal(*input)
	encodedBytes := len(encoded)
	clear(encoded)
	if encodeErr != nil || encodedBytes > int(limit) ||
		a.profile.ValidateBindingIdentity(job.HarnessBinding, job.ExecutionPlacementV2) != nil ||
		job.HarnessBinding.Resource.Kind != domain.ProviderResourceAPIAccountV1 ||
		job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest == "" || job.WorkspaceSnapshot != nil || job.SkillBundle != nil ||
		len(job.AllowedMCPServers) != 0 {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	authorization := ports.AuthorizationForAttachedInput(request.TenantID, request.OwnerUserID, request.WorkerID, request.ConnectionID,
		request.EnrollmentGeneration, request.ConnectionGeneration, request.Attempt, request.AttemptSequence)
	if !(ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: input.AcceptedAttempt.Revision, Attempt: input.AcceptedAttempt}).Matches(authorization) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	verified := &attachedworkerdaemon.VerifiedAttachedInputV1{AcceptedAttempt: input.AcceptedAttempt, Job: job, Manifest: input.Manifest,
		Context: input.Context, CanonicalContext: input.CanonicalContext,
		Artifacts: make([]attachedworkerdaemon.VerifiedAttachedArtifactV1, len(input.Artifacts))}
	for i, v := range input.Artifacts {
		verified.Artifacts[i] = attachedworkerdaemon.VerifiedAttachedArtifactV1{Name: v.Name, Body: v.Body}
	}
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		RunID: domain.RunID(request.Attempt.RunID), AttemptID: domain.AttemptID(request.Attempt.AttemptID), LeaseID: domain.LeaseID(request.Attempt.LeaseID), FenceToken: request.Attempt.LeaseGeneration}
	if verified.ValidateFor(identity) != nil || ctx.Err() != nil || a.ctx.Err() != nil {
		// Input still owns all byte buffers; its existing defer clears them.
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	// Move, not Clone: this is the one owned source result. Clearing its old
	// container cannot clear the buffers now transferred to the invocation.
	*input = SealedInputV1{}
	a.input = verified
	a.inputBytes = encodedBytes
	return MaterializedInputV1{VerifiedInput: verified, acquisition: a}, nil
}

func (adapter *Adapter) takePiInvocation(identity attachedworkerdaemon.InvocationIdentity, request MaterializationRequestV1, input *MaterializedInputV1) (attachedworkerdaemon.Invocation, error) {
	a := input.acquisition
	if !a.MatchesRequest(request) || input.VerifiedInput == nil || input.VerifiedInput != a.input || input.Credential != nil || len(input.Stdin) != 0 || len(input.ReadRoots) != 0 ||
		input.VerifiedInput.AcceptedAttempt.RunID != identity.RunID || input.VerifiedInput.AcceptedAttempt.AttemptID != identity.AttemptID {
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	profile := adapter.config.Profile
	if hex.EncodeToString(profile.ExecutableDigest[:]) != a.profile.Sources.NativeArtifactDigest || a.inputBytes <= 0 || a.inputBytes > adapter.config.MaxInputBytes {
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	// Existing local-profile bounds are validated when Adapter is constructed.
	// Fund its process slices plus all immutable string payload appearances
	// before copying. This does not supply native artifact/egress authority.
	bytes := uint64(len(profile.Executable)) + uint64(len(profile.Arguments))*uint64(reflect.TypeOf("").Size()) +
		uint64(len(profile.Environment))*uint64(reflect.TypeOf(attachedworkerdaemon.EnvironmentVariable{}).Size())
	for _, v := range profile.Arguments {
		bytes += uint64(len(v))
	}
	for _, v := range profile.Environment {
		bytes += uint64(len(v.Name) + len(v.Value))
	}
	if a.Reserve(bytes, bytes, 0) != nil {
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	invocation := attachedworkerdaemon.Invocation{Identity: identity, VerifiedInput: input.VerifiedInput, InputAcquisition: a,
		Process: attachedworkerdaemon.AttemptSpec{Executable: profile.Executable, ExecutableDigest: profile.ExecutableDigest,
			Arguments: append([]string(nil), profile.Arguments...), Environment: append([]attachedworkerdaemon.EnvironmentVariable(nil), profile.Environment...)}}
	if invocation.Validate() != nil {
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	*input = MaterializedInputV1{}
	return invocation, nil
}
