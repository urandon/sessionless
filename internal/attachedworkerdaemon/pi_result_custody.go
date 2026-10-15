package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// One slot belongs to the SAME API InvocationRunner. Before cleanup raw is
// pending DATA only. No public result, DATA parser or caller factory can set
// frozen. Declared artifact contents are retained, never converted to BlobRefs
// or silently omitted while #200's upload/custody implementation is absent.
type piResultCustody struct {
	send        preparedAPIPhysicalSend // prepaid SAME original owner, not a post-Issue allocation
	identity    InvocationIdentity
	head        domain.AttachedWorkerAttemptV1
	profile     domain.AttachedPiSDKNativeProfileV1
	requestHash [4][sha256.Size]byte
	ledger      PreparedAPILedgerV1
	live        uint64
	raw         []byte
	rawSize     int
	finalHash   [sha256.Size]byte
	stamp       *piObservationStamp
	parse       piResultParsePartition
	final       attachedworkeroutput.StructuredFinalV1
	result      InvocationResult
	runErr      error
	clearErr    error
	candidate   attachedworkeroutput.Candidate

	issued           bool
	captured         bool
	callbackReturned bool
	fileFinalized    bool
	frozen           bool
	finished         bool
	candidateBuilt   bool
	canonicalSchema  bool
	stagingFailed    bool
}

// A partition already reserved on the ORIGINAL ledger before Issue, not a
// second full-P budget. Every parser debit consumes this ONE retained remainder
// without reset/refund; its full live reservation lasts until exact retirement.
type piResultParsePartition struct{ copy, live, nodes uint64 }

func (p *piResultParsePartition) Reserve(c, l, n uint64) error {
	if c > p.copy || l > p.live || n > p.nodes {
		return ErrCredentialUnavailable
	}
	p.copy -= c
	p.live -= l
	p.nodes -= n
	return nil
}

func preparePiResultCustody(id InvocationIdentity, data *PreparedAPIDataV1) (*piResultCustody, error) {
	if data == nil || data.ParentInput == nil || nilPreparedAPIDependency(data.Ledger) {
		return nil, ErrCredentialUnavailable
	}
	p := data.ParentRequest.Profile
	f := p.Limits.StructuredFinalBytes
	if p.Validate() != nil || f == 0 || f > 128<<10 || f > p.Limits.RawFinalBytes ||
		p.Limits.ProcessFrameBytes > pisdkphasewire.TransportMaxFrameBytes {
		return nil, ErrCredentialUnavailable
	}
	// Parser's explicit debits:4F decoded/decoder appearances, at most the
	// original artifact/answer bytes, two artifact metadata structs; lexical
	// walks of raw plus at most two JSON contents, and two artifact nodes.
	// Each lexical node consumes at least one original byte. Intersect the
	// existing node limit with those source byte caps BEFORE adding; JSONNodes
	// itself is not given a guessed finite maximum or allowed to wrap3*N.
	metadata := uint64(reflect.TypeOf(piResultCustody{}).Size()) + uint64(reflect.TypeOf(preparedAPICancel{}).Size())
	// Timer's exposed Go state, completion channel and callback cell are
	// prepared here, not at physical send. This is NOT a claim about complete
	// runtime scheduler/closure/stack heap fit or native qualification.
	metadata += uint64(reflect.TypeOf(time.Timer{}).Size()) + uint64(reflect.TypeOf(preparedAPICancel{}).Size()) + 256
	artifacts := 2 * uint64(reflect.TypeOf(attachedworkeroutput.StructuredArtifactV1{}).Size())
	parseBytes := 4*f + p.Limits.ArtifactTotalBytes + p.Limits.AnswerBytes + artifacts
	nodes := min(p.Limits.JSONNodes, f) + 2*min(p.Limits.JSONNodes, p.Limits.ArtifactBytes) + 2
	live := f + parseBytes + metadata
	// Pending hash/copy/hash, post-cleanup hash, parser work, immutable receipt
	// comparisons/answer use, and exact request hashes at preflight+Supervisor.
	work := 4*f + parseBytes + p.Limits.AnswerBytes + metadata
	for _, v := range [][]byte{data.ParentRequest.Body, data.ParentRequest.Model, data.ParentRequest.Schema, data.ParentRequest.Headers} {
		n := 2 * uint64(len(v)) // len<=MaxInt, so doubling fits uint64
		if n > ^uint64(0)-work {
			return nil, ErrCredentialUnavailable
		}
		work += n
	}
	comparison := uint64(len(data.ParentRequest.Schema)) + 64
	if comparison > ^uint64(0)-work {
		return nil, ErrCredentialUnavailable
	}
	work += comparison // fixed-schema and digest comparison
	// One binder comparison and one physical authorization body/profile scan;
	// fixed scalar IDs plus at most four original-frame-sized path comparisons.
	// This funds Go state/comparison work only, not HTTP/native qualification.
	extra := 2*uint64(len(data.ParentRequest.Body)) + 128 + 16384 + 4*p.Limits.ProcessFrameBytes
	if extra > ^uint64(0)-work {
		return nil, ErrCredentialUnavailable
	}
	work += extra
	if data.Ledger.Reserve(work, live, nodes+5) != nil {
		return nil, ErrCredentialUnavailable
	}
	s := &piResultCustody{identity: id, head: data.ParentInput.AcceptedAttempt, profile: p, ledger: data.Ledger, live: live,
		raw: make([]byte, int(f)), parse: piResultParsePartition{copy: parseBytes, live: parseBytes, nodes: nodes}}
	s.send.identity, s.send.ledger, s.send.data = id, data.Ledger, data
	s.send.borrow.send = &s.send
	// Existing DATA-only API orchestration with a different schema may still
	// exercise its lifecycle, but it can NEVER mint this private producer.
	s.canonicalSchema = canonicalPiFinalSchema(data.ParentRequest.Schema)
	for i, v := range [][]byte{data.ParentRequest.Body, data.ParentRequest.Model, data.ParentRequest.Schema, data.ParentRequest.Headers} {
		s.requestHash[i] = sha256.Sum256(v)
	}
	if !piExecutableMatches(ExecutableDigest(s.requestHash[2]), p.Sources.FinalSchemaDigest) {
		s.clear()
		return nil, ErrCredentialUnavailable
	}
	if s.send.prepareDeadline() != nil {
		s.clear()
		return nil, ErrCredentialUnavailable
	}
	return s, nil
}

func canonicalPiFinalSchema(raw []byte) bool {
	if len(raw) != len(attachedworkeroutput.StructuredFinalSchemaV1) {
		return false
	}
	for i, b := range raw {
		if b != attachedworkeroutput.StructuredFinalSchemaV1[i] {
			return false
		}
	}
	return true
}

func (*InvocationRunner) String() string     { return "InvocationRunner{private:[redacted]}" }
func (r *InvocationRunner) GoString() string { return r.String() }

func (s *piResultCustody) capture(result AttemptResult, send *preparedAPIPhysicalSend, data *PreparedAPIDataV1,
	handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) {
	o := result.piStdio
	if o != nil && o.send == send {
		defer func() {
			// Consume only this original association, never an unrelated result.
			o.send, o.ledger = nil, nil
			o.credential, o.materialization = ports.ProviderInvocationCredentialV1{}, ports.ProviderCredentialMaterializationV1{}
		}()
	}
	if !s.canonicalSchema || s.captured || o == nil || o.stamp == nil || o.send != send || o.identity != s.identity || o.head != s.head ||
		o.profile != s.profile || !samePiLedger(o.ledger, s.ledger) || o.requestHash != s.requestHash ||
		o.credential != handle || o.materialization != plan ||
		!samePiLedger(data.Ledger, s.ledger) || len(result.Stdout) != 0 || len(o.Final) == 0 || len(o.Final) > len(s.raw) ||
		sha256.Sum256(o.Final) != o.FinalDigest {
		return
	}
	send.mu.Lock()
	authorized := send.authorized && !send.failed
	send.mu.Unlock()
	if !authorized {
		return
	}
	copy(s.raw, o.Final)
	if sha256.Sum256(s.raw[:len(o.Final)]) != o.FinalDigest {
		clear(s.raw)
		return
	}
	s.rawSize, s.finalHash, s.stamp, s.captured = len(o.Final), o.FinalDigest, o.stamp, true
}

func (s *piResultCustody) complete(ctx context.Context, result InvocationResult, runErr error) {
	if !s.captured || !s.callbackReturned || !s.fileFinalized || !result.Succeeded(runErr) ||
		ctx == nil || ctx.Err() != nil || s.rawSize == 0 || sha256.Sum256(s.raw[:s.rawSize]) != s.finalHash {
		return
	}
	// This is the first immutable semantic producer transition, AFTER all
	// actual owner cleanup. No borrowed child buffers are parsed or trusted now.
	final, err := attachedworkeroutput.ParseStructuredFinalV1(ctx, s.raw[:s.rawSize], s.profile.Limits, &s.parse)
	if err != nil || final.FinalDigest != s.finalHash {
		return
	}
	s.final, s.frozen = final, true
	if len(final.Artifacts) == 0 && strings.TrimSpace(final.Answer) != "" {
		s.candidate = attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: final.Answer}
	}
}

func (s *piResultCustody) clear() error {
	if s.clearErr != nil {
		return s.clearErr
	} // never relabel failed relinquishment clean on retry
	s.send.retire() // join/drop its original borrows before their live reservation
	clear(s.raw)
	s.raw, s.final, s.candidate, s.stamp = nil, attachedworkeroutput.StructuredFinalV1{}, attachedworkeroutput.Candidate{}, nil
	if s.live == 0 {
		return nil
	}
	s.clearErr = s.ledger.EndLive(s.live)
	if s.clearErr == nil {
		s.live = 0
	}
	return s.clearErr
}

func (r *InvocationRunner) beginPiInvocation(ctx context.Context, id InvocationIdentity) (context.Context, func(InvocationResult, error, bool), error) {
	r.piMu.Lock()
	if r.piClosed || r.piCleanupFailed || r.piRunning || r.piSlot != nil || r.piLast == id {
		r.piMu.Unlock()
		return nil, nil, ErrCredentialUnavailable
	}
	r.piRunning, r.piDone = true, make(chan struct{})
	runCtx, cancel := context.WithCancel(ctx)
	r.piCancel = cancel
	r.piMu.Unlock()
	return runCtx, func(result InvocationResult, runErr error, returned bool) {
		cancel()
		r.piMu.Lock()
		defer r.piMu.Unlock()
		if !returned {
			r.piCleanupFailed = true
		}
		if s := r.piSlot; s != nil {
			if returned && !s.issued && !s.stagingFailed {
				if s.clear() == nil {
					r.piSlot = nil
				} // failed relinquishment remains pending
			} else {
				s.result, s.runErr, s.finished = piReceiptResult(result), runErr, returned
			}
		}
		r.piRunning, r.piCancel = false, nil
		close(r.piDone)
	}, nil
}

func (r *InvocationRunner) retirePiResources(id InvocationIdentity, data *PreparedAPIDataV1, handle ports.ProviderInvocationCredentialV1,
	s *piResultCustody, result *InvocationResult, runErr *error) {
	var retirement context.Context
	var err error
	if data == nil || nilPreparedAPIDependency(data.Retirement) {
		err = ErrInvocationInvalid
	} else {
		retirement, err = data.Retirement.Context()
	}
	valid := err == nil && !nilPreparedAPIDependency(retirement) && retirement.Done() != nil
	if valid {
		deadline, ok := retirement.Deadline()
		valid = ok && deadline != deadline.Round(0)
	}
	if !valid {
		retirement = nil
	} // never replace invalid original with a fresh grace
	var releaseErr error
	if handle != (ports.ProviderInvocationCredentialV1{}) {
		releaseErr = r.apiCredentials.ReleaseProviderCredential(retirement, handle)
		result.CredentialReleased = valid && releaseErr == nil
		if !result.CredentialReleased {
			result.FailureCode = "credential_release_failed"
			*runErr = errors.Join(*runErr, ErrCredentialFinalization)
		}
	}
	// The same original cutoff is passed to the actual staging owner exactly
	// once, independently of Release success; Clear is not its substitute.
	stagingErr := r.apiParent.RetireOriginalAPIStaging(retirement, id, data)
	if !valid || stagingErr != nil {
		r.piMu.Lock()
		r.piCleanupFailed = true // also retain failure when pre-Issue allocation denied
		r.piMu.Unlock()
		if s != nil {
			s.stagingFailed = true
		}
		if result.FailureCode == "" {
			result.FailureCode = "api_staging_cleanup_failed"
		}
		*runErr = errors.Join(*runErr, ErrCredentialFinalization)
	}
	if s != nil && valid && releaseErr == nil && stagingErr == nil {
		s.complete(retirement, *result, *runErr)
	}
}

// ReceiptCandidate is the existing adapter contract. It accepts only this
// owner's privately finalized slot and exact original result; public cleanup
// flags cannot mint it. No model/provider execution occurs on retries.
func (r *InvocationRunner) ReceiptCandidate(id InvocationIdentity, result InvocationResult, runErr error, status domain.AttachedWorkerTerminalStatus) (attachedworkeroutput.Candidate, error) {
	if r == nil {
		return attachedworkeroutput.Candidate{}, ErrInvocationInvalid
	}
	r.piMu.Lock()
	defer r.piMu.Unlock()
	s := r.piSlot
	if r.piClosed || r.piRunning || s == nil || !s.finished || !s.frozen || id != s.identity ||
		result.Process.piStdio == nil || result.Process.piStdio.stamp != s.stamp ||
		len(result.Process.Stdout) != 0 || len(result.CommittedArtifactDigests) != 0 || len(result.CommittedEventDigests) != 0 ||
		!reflect.DeepEqual(piReceiptResult(result), s.result) || !reflect.DeepEqual(runErr, s.runErr) ||
		status != domain.AttachedWorkerTerminalSucceeded || !s.result.Succeeded(s.runErr) ||
		len(s.final.Artifacts) != 0 || s.candidate.Status != status || s.candidate.Summary == "" {
		return attachedworkeroutput.Candidate{}, ErrCredentialUnavailable
	}
	// Nonempty declared artifacts stay private. The genuine bounded pre-Put
	// capability/custody path must supply BlobRefs before success can be built.
	// The immutable value was built once at the post-cleanup transition;
	// retries neither parse/hash/clone contents nor spend a fresh copy pool.
	s.candidateBuilt = true
	return s.candidate, nil
}

// Store content-free result values only. The private stamp above binds the
// real observation; retaining its pointer here would retain the plan's old
// borrowed Final allocation after its own live reservation was relinquished.
func piReceiptResult(v InvocationResult) InvocationResult {
	v.Process.piStdio = nil
	v.Process.Stdout = nil
	v.CommittedArtifactDigests = nil
	v.CommittedEventDigests = nil
	return v
}

// Only the trusted adapter calls this AFTER exact journal/ACK retirement.
// Identity alone is not a public server acknowledgement or upload authority.
func (r *InvocationRunner) RetireReceipt(id InvocationIdentity) error {
	if r == nil {
		return ErrInvocationInvalid
	}
	r.piMu.Lock()
	defer r.piMu.Unlock()
	s := r.piSlot
	if r.piRunning || s == nil || s.identity != id || !s.finished || !s.candidateBuilt {
		return ErrCredentialUnavailable
	}
	if err := s.clear(); err != nil {
		return err
	}
	r.piLast, r.piSlot = id, nil
	return nil
}

// Close revokes/joins owned work but preserves unresolved output custody. It
// never calls a timeout quiescence, clears ambiguous publication, or retries a
// provider. Exact receipt retirement or independent disposition remains needed.
func (r *InvocationRunner) Close(ctx context.Context) error {
	if r == nil || ctx == nil || nilPreparedAPIDependency(r.apiParent) {
		return ErrInvocationInvalid
	}
	r.piMu.Lock()
	r.piClosed = true
	done, running, cancel := r.piDone, r.piRunning, r.piCancel
	r.piMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if running {
		select {
		case <-done:
		case <-ctx.Done():
			select {
			case <-done:
			default:
				return ctx.Err()
			}
		}
	}
	r.piMu.Lock()
	defer r.piMu.Unlock()
	if r.piSlot != nil || r.piCleanupFailed {
		return ErrCredentialUnavailable
	}
	return nil
}
