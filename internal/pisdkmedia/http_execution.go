package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// PreparedPiHTTPExecutionV1 is the separate SPU3 PreparedProcessRunner arm.
// It borrows the original HTTP Init/request until actual consumers join. It
// never owns Issue/Finalize/Release, installs trust or manufactures custody.
// Copies share the SAME one-shot execution and retirement state.
type PreparedPiHTTPExecutionV1 struct{ state *piHTTPExecutionState }

type piHTTPExecutionState struct {
	mu           sync.Mutex
	stage        uint8 // prepared0, running1, returned2, closed3
	closeErr     error
	metadata     *preparedExecutionSnapshot
	supervisor   *attachedworkerdaemon.Supervisor
	init         *HTTPInitPlanV1
	fileUse      *providercredential.PreparedPiFileUseV1
	stdio        *attachedworkerdaemon.PreparedPiHTTPStdioV1
	arguments    [17]string
	metadataLive uint64
}

var _ attachedworkerdaemon.PreparedProcessRunner = (*PreparedPiHTTPExecutionV1)(nil)

func (PreparedPiHTTPExecutionV1) String() string {
	return "PreparedPiHTTPExecutionV1{private:[redacted]}"
}
func (p PreparedPiHTTPExecutionV1) GoString() string { return p.String() }

// PreparePiHTTPExecutionV1 fixes only the SAME original file owner's concrete
// HTTP factory. No callback, root policy, endpoint, response or approval is
// accepted here. The installed private transport MUST already be prepared;
// its absence is not repaired by structurally valid request/Init DATA.
func PreparePiHTTPExecutionV1(supervisor *attachedworkerdaemon.Supervisor, request *RequestProjectionV1,
	init *HTTPInitPlanV1, fileUse *providercredential.PreparedPiFileUseV1, expected attachedworkerdaemon.PreparedAPIRequestV1,
	process attachedworkerdaemon.AttemptSpec) (_ *PreparedPiHTTPExecutionV1, err error) {
	if supervisor == nil || request == nil || request.ctx == nil || request.ctx.Err() != nil || request.ledger == nil || init == nil || init.state == nil || fileUse == nil {
		return nil, ErrProjection
	}
	i := init.state
	if !i.mu.TryLock() {
		return nil, ErrProjection
	}
	defer i.mu.Unlock()
	if i.closed || i.base == nil || i.base.request != request || !i.base.http || i.emitted.Load() || i.executionClaimed {
		return nil, ErrProjection
	}
	i.executionClaimed = true // even a failed constructor cannot replay the plan
	// Fund state and original profile/binding comparisons before shared helpers
	// walk immutable DATA. These are Go logical costs, NOT native qualification.
	live := uint64(reflect.TypeOf(PreparedPiHTTPExecutionV1{}).Size() + reflect.TypeOf(piHTTPExecutionState{}).Size())
	if request.ledger.Reserve(live+8192, live, 2) != nil {
		return nil, ErrProjection
	}
	keep := false
	defer func() {
		if !keep {
			err = errors.Join(err, request.ledger.EndLive(live))
		}
	}()
	comparison, e := httpInitComparisonBytes(request)
	if e != nil || request.ledger.Reserve(4*comparison+16*domain.AttachedPiSDKCanonicalMaxBytesV1, 0, 0) != nil {
		return nil, ErrProjection
	}
	args := i.base.httpArguments
	metadata, err := prepareExecutionSnapshot(request, i.base, args[:], expected, process)
	if err != nil {
		return nil, err
	}
	stdio, err := fileUse.PrepareHTTPIOV1(supervisor, expected, i.base.credentialFile, request.ledger)
	if err != nil {
		return nil, errors.Join(err, metadata.clear())
	}
	i.executionBorrowed = true
	keep = true
	return &PreparedPiHTTPExecutionV1{state: &piHTTPExecutionState{metadata: metadata, supervisor: supervisor, init: init,
		fileUse: fileUse, stdio: stdio, arguments: args, metadataLive: live}}, nil
}

// RunPrepared is called INSIDE the original synchronous Materialize callback.
// Its private file binder resolves the actual original owner/context/handle
// before the SAME Supervisor can launch; no public DATA becomes send authority.
func (p *PreparedPiHTTPExecutionV1) RunPrepared(ctx context.Context, in attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	deny := func() (attachedworkerdaemon.AttemptResult, error) {
		attachedworkerdaemon.RevokePreparedAPISendV1(in.PhysicalSend)
		return attachedworkerdaemon.AttemptResult{}, ErrProjection
	}
	if p == nil || p.state == nil {
		return deny()
	}
	s := p.state
	if !s.mu.TryLock() {
		return deny()
	}
	if s.stage != 0 {
		s.mu.Unlock()
		return deny()
	}
	s.stage = 1
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.stage = 2; s.mu.Unlock() }()
	m := s.metadata
	if ctx == nil || ctx.Err() != nil || m.request.ctx.Err() != nil || in.APIRequest == nil || in.APIRequest.Profile != m.profile ||
		in.Ledger != m.request.ledger || in.VerifiedInput == nil || in.VerifiedInput.AcceptedAttempt != m.request.head ||
		len(in.APIRequest.Body) != len(m.request.body) || sha256.Sum256(m.request.body) != m.bodyDigest || !bytes.Equal(in.APIRequest.Body, m.request.body) ||
		len(in.Process.Stdin) != 0 || !m.sameProcess(in.Process, in.Materialization.RootDir) {
		return deny()
	}
	for i, data := range [3][]byte{in.APIRequest.Model, in.APIRequest.Schema, in.APIRequest.Headers} {
		if len(data) != m.metadataSizes[i] || sha256.Sum256(data) != m.metadataDigests[i] {
			return deny()
		}
	}
	for i, value := range s.arguments {
		if in.Process.Arguments[len(in.Process.Arguments)-17+i] != value {
			return deny()
		}
	}
	if err := s.fileUse.BindMaterialized(ctx, in.Credential, in.Materialization, in.PhysicalSend); err != nil {
		return attachedworkerdaemon.AttemptResult{}, err
	}
	body, err := s.init.MaterializedBody(ctx, in.Credential, in.Materialization)
	if err != nil {
		attachedworkerdaemon.RevokePreparedAPISendV1(in.PhysicalSend)
		return attachedworkerdaemon.AttemptResult{}, err
	}
	in.Process.Stdin = body // existing funded arena, held until actual Run joins
	return s.supervisor.RunPreparedPiHTTP(ctx, in, s.stdio)
}

// Close retires execution DATA only, never the original file/Init/request.
// Unjoined borrowers retain aliases/debits and permit cleanup after actual
// join. Accounting failures are terminal: no copied wrapper can retry their
// EndLive against later owners. A running executor
// denies Close without starting retirement, allowing actual Run to join first.
func (p *PreparedPiHTTPExecutionV1) Close() error {
	if p == nil || p.state == nil {
		return ErrProjection
	}
	s := p.state
	if !s.mu.TryLock() {
		return ErrProjection
	}
	defer s.mu.Unlock()
	if s.stage == 1 {
		return ErrProjection
	}
	if s.stage == 3 {
		return s.closeErr
	}
	s.stage = 3
	return s.finishRetirement(s.stdio.Close())
}

// Called under the shared owner lock after the concrete stdio retirement.
// Keeping the transition separate also permits pure state-machine regressions;
// this unexported helper cannot install a transport or mint execution authority.
func (s *piHTTPExecutionState) finishRetirement(err error) error {
	if s.closeErr = err; s.closeErr != nil {
		if errors.Is(s.closeErr, attachedworkerdaemon.ErrPiHTTPRetirementPending) {
			s.stage = 2 // no refund; permit cleanup after actual borrower join
		}
		return s.closeErr
	}
	if s.closeErr = s.metadata.clear(); s.closeErr != nil {
		return s.closeErr
	}
	if s.closeErr = s.metadata.request.ledger.EndLive(s.metadataLive); s.closeErr != nil {
		return s.closeErr
	}
	i := s.init.state
	i.mu.Lock()
	i.executionBorrowed = false
	i.mu.Unlock()
	s.metadata, s.stdio, s.fileUse, s.init = nil, nil, nil, nil
	s.metadataLive = 0
	return nil
}
