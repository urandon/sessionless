package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// PreparedPiExecutionV1 implements the EXISTING PreparedProcessRunner seam.
// It neither wraps another InvocationRunner nor owns Issue/Finalize/Release.
// Prepare in the original full producer before Issue, borrowing its exact Init,
// request and ledger until original lifecycle retirement and semantic collection
// have joined. Full Source/native/argv/fit admission still belongs to that
// producer; this disconnected default-off component supplies none of it.
type PreparedPiExecutionV1 struct {
	supervisor      *attachedworkerdaemon.Supervisor
	request         *RequestProjectionV1
	init            *InitPlanV1
	stdio           *attachedworkerdaemon.PreparedPiStdioV1
	profile         domain.AttachedPiSDKNativeProfileV1
	arguments       [10]string
	bodyDigest      [sha256.Size]byte
	metadataDigests [3][sha256.Size]byte
	metadataSizes   [3]int
	process         attachedworkerdaemon.AttemptSpec
	live            uint64
	observation     attachedworkerdaemon.PiStdioObservationV1
	state           atomic.Uint32 // 0 prepared, 1 running, 2 returned, 3 closed, 4 collecting, 5 collected
}

var _ attachedworkerdaemon.PreparedProcessRunner = (*PreparedPiExecutionV1)(nil)

func (*PreparedPiExecutionV1) String() string     { return "PreparedPiExecutionV1{data:[redacted]}" }
func (p *PreparedPiExecutionV1) GoString() string { return p.String() }

func PreparePiExecutionV1(s *attachedworkerdaemon.Supervisor, request *RequestProjectionV1, init *InitPlanV1, expected attachedworkerdaemon.PreparedAPIRequestV1, process attachedworkerdaemon.AttemptSpec) (*PreparedPiExecutionV1, error) {
	if s == nil || request == nil || init == nil || request.ctx == nil || request.ctx.Err() != nil || init.request != request ||
		init.spent || init.ledger != request.ledger || request.ledger == nil || init.credentialFile == nil || len(request.body) == 0 ||
		len(process.Stdin) != 0 || len(process.Arguments) < 10 || expected.Profile != request.config.profile || len(expected.Body) != len(request.body) {
		return nil, ErrProjection
	}
	args := init.EntryArguments()
	for i, value := range args {
		if process.Arguments[len(process.Arguments)-10+i] != value {
			return nil, ErrProjection
		}
	}
	var argumentBytes uint64
	for _, value := range args {
		if value == "" {
			return nil, ErrProjection
		}
		argumentBytes += uint64(len(value))
	}
	l := request.config.profile.Limits
	parts := [3][]byte{expected.Model, expected.Schema, expected.Headers}
	for i, v := range parts {
		cap := l.NativeRequestBytes
		if i == 2 {
			cap = l.RequestHeaderBytes
		}
		if len(v) == 0 || uint64(len(v)) > cap {
			return nil, ErrProjection
		}
	}
	// Bound metadata BEFORE walking/copying it; caller-owned capacities do not
	// become shared backing arrays in this retained process snapshot.
	count := uint64(len(process.Arguments)) + uint64(len(process.Environment)) + uint64(len(process.AdditionalReadRoots))
	if count > l.JSONNodes || uint64(len(process.Executable)) > l.ProcessFrameBytes {
		return nil, ErrProjection
	}
	processBytes := uint64(len(process.Executable))
	add := func(s string) bool {
		if uint64(len(s)) > l.ProcessFrameBytes-processBytes {
			return false
		}
		processBytes += uint64(len(s))
		return true
	}
	for _, v := range process.Arguments {
		if !add(v) {
			return nil, ErrProjection
		}
	}
	for _, v := range process.Environment {
		if !add(v.Name) || !add(v.Value) {
			return nil, ErrProjection
		}
	}
	for _, v := range process.AdditionalReadRoots {
		if !add(v) {
			return nil, ErrProjection
		}
	}
	var headerVisit uint64
	for _, h := range request.config.headers {
		headerVisit += uint64(len(h.Name) + len(h.Value))
	}
	// Measuring quoted header bytes is itself a byte walk, funded before it
	// begins rather than retroactively included in the later render reserve.
	if err := request.ledger.Reserve(8*headerVisit+128, 0, 45); err != nil {
		return nil, ErrProjection
	}
	measure := wireWriter{cap: l.RequestHeaderBytes, maxDepth: 1, maxNodes: l.JSONNodes}
	writeExecutionHeaders(&measure, request.config.headers)
	if measure.err != nil {
		return nil, ErrProjection
	}
	live := 64*count + 512
	work := 4*uint64(len(request.body)) + 2*argumentBytes + 4*processBytes + 4*uint64(len(expected.Model)+len(expected.Schema)+len(expected.Headers)) + 2*measure.bytes
	// Extra executor comparisons/hash visits are prospectively funded in the
	// original cumulative ledger. No new payload arena or native-fit claim.
	if err := request.ledger.Reserve(work, live+measure.bytes, count+measure.nodes); err != nil {
		return nil, ErrProjection
	}
	defer request.ledger.EndLive(measure.bytes)
	keep := false
	defer func() {
		if !keep {
			_ = request.ledger.EndLive(live)
		}
	}()
	header := wireWriter{out: make([]byte, 0, int(measure.bytes)), cap: measure.bytes, maxDepth: 1, maxNodes: l.JSONNodes}
	writeExecutionHeaders(&header, request.config.headers)
	defer clear(header.out)
	if header.err != nil || !bytes.Equal(header.out, expected.Headers) || sha256.Sum256(expected.Model) != init.modelDigest ||
		hex.EncodeToString(sha256sumBytes(expected.Schema)) != request.config.schemaDigest || sha256.Sum256(request.body) != request.bodyDigest ||
		!bytes.Equal(expected.Body, request.body) {
		return nil, ErrProjection
	}
	process.Arguments = append([]string(nil), process.Arguments...)
	process.Environment = append([]attachedworkerdaemon.EnvironmentVariable(nil), process.Environment...)
	process.AdditionalReadRoots = append([]string(nil), process.AdditionalReadRoots...)
	stdio, err := s.PreparePiStdioV1(request.config.profile, request.ledger)
	if err != nil {
		return nil, err
	}
	keep = true
	return &PreparedPiExecutionV1{supervisor: s, request: request, init: init, stdio: stdio,
		profile: request.config.profile, arguments: args, bodyDigest: request.bodyDigest, process: process, live: live,
		metadataDigests: [3][sha256.Size]byte{sha256.Sum256(expected.Model), sha256.Sum256(expected.Schema), sha256.Sum256(expected.Headers)},
		metadataSizes:   [3]int{len(expected.Model), len(expected.Schema), len(expected.Headers)}}, nil
}

func sha256sumBytes(v []byte) []byte { sum := sha256.Sum256(v); return sum[:] }
func writeExecutionHeaders(w *wireWriter, headers [11]HeaderDataV1) {
	w.literal("{")
	for i, h := range headers {
		if i > 0 {
			w.literal(",")
		}
		w.quote(h.Name, nil)
		w.literal(":")
		w.quote(h.Value, nil)
	}
	w.literal("}")
}

// Called only by the original InvocationRunner inside its genuine synchronous
// Materialize callback. The original private physical-send capability is passed
// unmodified: RunPreparedPi rejects replacements or another owner/context.
func (p *PreparedPiExecutionV1) RunPrepared(ctx context.Context, in attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	if p == nil || !p.state.CompareAndSwap(0, 1) {
		return attachedworkerdaemon.AttemptResult{}, ErrProjection
	}
	defer p.state.Store(2)
	if ctx == nil || ctx.Err() != nil || p.request.ctx == nil || p.request.ctx.Err() != nil ||
		in.APIRequest == nil || in.APIRequest.Profile != p.profile || in.Ledger != p.request.ledger ||
		in.VerifiedInput == nil || in.VerifiedInput.AcceptedAttempt != p.request.head ||
		len(in.APIRequest.Body) != len(p.request.body) || sha256.Sum256(p.request.body) != p.bodyDigest ||
		!bytes.Equal(in.APIRequest.Body, p.request.body) || len(in.Process.Stdin) != 0 || len(in.Process.Arguments) < 10 {
		return attachedworkerdaemon.AttemptResult{}, ErrProjection
	}
	for i, v := range [3][]byte{in.APIRequest.Model, in.APIRequest.Schema, in.APIRequest.Headers} {
		if len(v) != p.metadataSizes[i] || sha256.Sum256(v) != p.metadataDigests[i] {
			return attachedworkerdaemon.AttemptResult{}, ErrProjection
		}
	}
	if !p.sameProcess(in.Process, in.Materialization.RootDir) {
		return attachedworkerdaemon.AttemptResult{}, ErrProjection
	}
	// These exact suffix strings were already emitted before Issue. The full
	// original producer still pins executable/complete argv/artifact/isolation;
	// this check does not bless a caller-selected script or an unqualified SDK.
	start := len(in.Process.Arguments) - 10
	for i, value := range p.arguments {
		if in.Process.Arguments[start+i] != value {
			return attachedworkerdaemon.AttemptResult{}, ErrProjection
		}
	}
	body, err := p.init.MaterializedBody(ctx, in.Credential, in.Materialization)
	if err != nil {
		return attachedworkerdaemon.AttemptResult{}, err
	}
	in.Process.Stdin = body // borrowed original funded arena, no post-Issue clone
	result, err := p.supervisor.RunPreparedPi(ctx, in, p.stdio)
	if err == nil {
		if observation, ok := result.PiStdioObservationV1(); ok {
			p.observation = observation
		}
	}
	return result, err
}

func (p *PreparedPiExecutionV1) sameProcess(v attachedworkerdaemon.AttemptSpec, root string) bool {
	w := p.process
	if v.Executable != w.Executable || v.ExecutableDigest != w.ExecutableDigest || len(v.Arguments) != len(w.Arguments) ||
		len(v.Environment) != len(w.Environment) || len(v.AdditionalReadRoots) != len(w.AdditionalReadRoots)+1 ||
		v.AdditionalReadRoots[len(v.AdditionalReadRoots)-1] != root {
		return false
	}
	for i, a := range w.Arguments {
		if v.Arguments[i] != a {
			return false
		}
	}
	for i, e := range w.Environment {
		if v.Environment[i] != e {
			return false
		}
	}
	for i, r := range w.AdditionalReadRoots {
		if v.AdditionalReadRoots[i] != r {
			return false
		}
	}
	return true
}

// FinalData is called after the original InvocationRunner returns. It requires
// its successful result DATA and this execution's actual supervised observation.
// Public result flags do not prove credential finalization: #200 must bind the
// original owner's private completion/custody independently. This returns DATA,
// never a private producer/upload proof.
// Do not call concurrently with Close or before original lifecycle completion.
func (p *PreparedPiExecutionV1) FinalData(ctx context.Context, result attachedworkerdaemon.InvocationResult, runErr error) (attachedworkeroutput.StructuredFinalV1, error) {
	if p == nil || !p.state.CompareAndSwap(2, 4) {
		return attachedworkeroutput.StructuredFinalV1{}, ErrProjection
	}
	defer p.state.Store(5)
	if ctx == nil || ctx.Err() != nil || !result.Succeeded(runErr) ||
		!result.CredentialReleaseRequired || !result.CredentialReleased || len(p.observation.Final) == 0 {
		return attachedworkeroutput.StructuredFinalV1{}, ErrProjection
	}
	actual, ok := result.Process.PiStdioObservationV1()
	if !ok || actual.StdoutDigest != p.observation.StdoutDigest || actual.FinalDigest != p.observation.FinalDigest ||
		!bytes.Equal(actual.Final, p.observation.Final) {
		return attachedworkeroutput.StructuredFinalV1{}, ErrProjection
	}
	return attachedworkeroutput.ParseStructuredFinalV1(ctx, actual.Final, p.profile.Limits, p.request.ledger)
}

// Close only retires this component's own stdio buffers after all borrowed
// output consumers have joined. Original custody owns Init/request Clear after
// its whole lifecycle; cumulative work is never refunded.
func (p *PreparedPiExecutionV1) Close() error {
	if p == nil {
		return ErrProjection
	}
	for {
		state := p.state.Load()
		if state == 3 {
			return nil
		}
		if state == 1 || state == 4 {
			return ErrProjection
		}
		if p.state.CompareAndSwap(state, 3) {
			break
		}
	}
	if err := p.stdio.Close(); err != nil {
		return err
	}
	p.observation = attachedworkerdaemon.PiStdioObservationV1{}
	clear(p.process.Arguments)
	clear(p.process.Environment)
	clear(p.process.AdditionalReadRoots)
	return p.request.ledger.EndLive(p.live)
}
