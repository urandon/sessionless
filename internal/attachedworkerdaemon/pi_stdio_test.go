//go:build darwin || linux

package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

const piSyntheticRequest = `{"model":"synthetic","input":[{"role":"user","content":[{"type":"input_text","text":"whole history 9007199254740993"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}],"tools":[]}`
const piSyntheticFinal = `{"answer":"synthetic","artifacts":[]}`

type piOneByteReader struct{ io.Reader }

func (r piOneByteReader) Read(b []byte) (int, error) { return r.Reader.Read(b[:min(len(b), 1)]) }

func piProcessFixture(t *testing.T, mode string) (*Supervisor, *fixtureLauncher, PreparedInvocationV1, *PreparedPiStdioV1, *apiTestLedger, *atomic.Int32, context.CancelFunc) {
	t.Helper()
	s, launcher, exe, digest := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: maxSupervisorGrace})
	// Positive resources outlive the120s outer suite; these are NOT expiry tests.
	s.timeout = time.Hour
	p := apiOwnerProfile(t, digest)
	desc, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	sendCtx, cancelSend := context.WithCancel(ctx)
	t.Cleanup(cancelSend)
	calls := &atomic.Int32{}
	send := &preparedAPIPhysicalSend{ctx: sendCtx, ownerCtx: ctx, body: []byte(piSyntheticRequest), profile: desc.BackendProfileDigest, authorize: func() (time.Time, error) { calls.Add(1); return time.Now().Add(30 * time.Minute), nil }}
	send.cancel.Store(&preparedAPICancel{cancel: cancelSend})
	ledger := &apiTestLedger{}
	plan, err := s.PreparePiStdioV1(p, ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := plan.Close(); err != nil {
			t.Errorf("planClose: %v", err)
		}
	})
	input := PreparedInvocationV1{APIRequest: &PreparedAPIRequestV1{Profile: p, Body: []byte(piSyntheticRequest)}, PhysicalSend: send, Ledger: ledger, apiIssueStarted: time.Now(),
		Process: AttemptSpec{Executable: exe, ExecutableDigest: digest, Arguments: []string{"-test.run=^TestPiStdioHelperProcess$"},
			Environment: []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: mode}}, Stdin: []byte(desc.BackendProfileDigest)}}
	return s, launcher, input, plan, ledger, calls, cancel
}

func TestPiStdioSameSupervisorFragmentedSuccess(t *testing.T) {
	s, launcher, in, plan, ledger, calls, _ := piProcessFixture(t, "ok")
	s.outputReader = func(stream string, r io.Reader) io.Reader {
		if stream == "stdout" {
			return piOneByteReader{r}
		}
		return r
	}
	result, err := s.RunPreparedPi(in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx, in, plan)
	if err != nil || result.FailureCode != "" || result.ExitCode != 0 || !result.CleanupSucceeded || !result.DescendantsReaped || !result.BoundaryReleased {
		t.Fatalf("sameRun=%+v err=%v", result, err)
	}
	obs, ok := result.PiStdioObservationV1()
	if !ok || string(obs.Final) != piSyntheticFinal || obs.FinalDigest != sha256.Sum256([]byte(piSyntheticFinal)) {
		t.Fatalf("missing actual final observation: ok=%v", ok)
	}
	req, _ := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.PhysicalRequest, BackendProfileDigest: string(in.Process.Stdin), Body: []byte(piSyntheticRequest)}, plan.limits)
	fin, _ := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.StructuredFinal, Body: []byte(piSyntheticFinal)}, plan.limits)
	raw := append(req, fin...)
	if obs.StdoutDigest != sha256.Sum256(raw) || obs.StdoutDigest == obs.FinalDigest || result.StdoutBytes != len(raw) || len(result.Stdout) != 0 || calls.Load() != 1 {
		t.Fatalf("stdout/callback mismatch bytes=%d want=%d calls=%d", result.StdoutBytes, len(raw), calls.Load())
	}
	if launch := launcher.lastSpec(); len(launch.Arguments) != 1 || strings.Contains(fmt.Sprintf("%+v", launch), piSyntheticRequest) {
		t.Fatal("private payload leaked to LaunchSpec")
	}
	if _, err := s.RunPreparedPi(in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx, in, plan); err == nil || calls.Load() != 1 {
		t.Fatal("completed plan reentry replenished invocation")
	}
	work := ledger.copy
	if err := plan.Close(); err != nil || ledger.live != 0 || ledger.copy != work {
		t.Fatalf("live-only close live=%d err=%v", ledger.live, err)
	}
	if !bytes.Equal(obs.Final, make([]byte, len(obs.Final))) {
		t.Fatal("Close did not clear borrowed Final")
	}
}

func TestPiStdioInsideOriginalAPICredentialOwnerNotPublication(t *testing.T) {
	// Actual local process and immutable FILE lifecycle, but synthetic Source,
	// profile/request/Init. This is NOT SDK/native fit or semantic publication.
	inv, parent, life := longLivedAPIOwnerFixture(t)
	s, _, exe, digest := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: maxSupervisorGrace})
	s.timeout = time.Hour
	parent.profile = apiOwnerProfile(t, digest)
	desc, err := parent.profile.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	inv.Process.Executable = exe
	inv.Process.ExecutableDigest = digest
	inv.Process.Arguments = []string{"-test.run=^TestPiStdioHelperProcess$"}
	inv.Process.Environment = []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: "ok"}}
	inv.Process.Stdin = nil
	var fixtureInit []byte
	inv.VerifiedInput.Job.HarnessBinding.Backend = desc
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.AcceptedAttempt.ContextDigest = contextDigest
	parent.retirement.selected, parent.retirement.cancel = context.WithTimeout(context.Background(), 2*time.Hour)
	var plan *PreparedPiStdioV1
	parent.mutate = func(data *PreparedAPIDataV1) {
		data.ParentRequest.Body = []byte(piSyntheticRequest)
		data.ChildRequest.Body = bytes.Clone(data.ParentRequest.Body)
		data.InitArguments[9] = strconv.Itoa(len(data.ParentRequest.Body))
		data.ParentProcess.Arguments[len(data.ParentProcess.Arguments)-1] = data.InitArguments[9]
		data.ChildProcess.Arguments[len(data.ChildProcess.Arguments)-1] = data.InitArguments[9]
		fixtureInit = []byte(desc.BackendProfileDigest)
		parent.ownedBuffers = append(parent.ownedBuffers, fixtureInit)
		parent.ownedBuffers = append(parent.ownedBuffers, data.ParentRequest.Body, data.ChildRequest.Body)
		var planErr error
		plan, planErr = s.PreparePiStdioV1(parent.profile, data.Ledger)
		if planErr != nil {
			t.Fatalf("pre-Issue buffer preparation: %v", planErr)
		}
	}
	observed := false
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		parent.order = append(parent.order, "execute")
		if !life.active || input.Ledger != parent.data.Ledger {
			t.Error("not inside SAME original Materialize/ledger")
		}
		// Exact owned synthetic file root only; fixture launcher is NOT a real
		// filesystem/egress profile and child does not read this marker file.
		s.allowedReadRoots = []string{filepath.Dir(input.Materialization.RootDir)}
		input.Process.Stdin = fixtureInit // borrowed pre-Issue synthetic Init
		result, runErr := s.RunPreparedPi(ctx, input, plan)
		obs, ok := result.PiStdioObservationV1()
		observed = ok && string(obs.Final) == piSyntheticFinal
		if !input.PhysicalSend.(*preparedAPIPhysicalSend).retired.Load() {
			t.Error("returned before physical-use revocation")
		}
		parent.order = append(parent.order, "process-joined")
		// This test has no #200 producer. Consume/clear its local DATA before
		// return, rather than advertising a cleared alias as publication proof.
		result.piStdio = nil
		if closeErr := plan.Close(); closeErr != nil {
			return result, closeErr
		}
		return result, runErr
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	result, runErr := runner.Run(ctx, inv)
	if !result.Succeeded(runErr) || !observed || life.finalizeCount != 1 || life.releaseCount != 1 || !result.CredentialReleased || !parent.cleared {
		t.Fatalf("original owner result=%+v observed=%v finalize=%d release=%d err=%v", result, observed, life.finalizeCount, life.releaseCount, runErr)
	}
	want := []string{"prepare", "pre_issue", "issue", "materialize", "execute", "physical_send", "process-joined", "unlocked", "immutable_finalize", "finalize", "release"}
	if !reflect.DeepEqual(parent.order, want) {
		t.Fatalf("owner order=%v want=%v", parent.order, want)
	}
}

func TestPiStdioRejectsProtocolAndPreservesOneCall(t *testing.T) {
	for _, tc := range []struct {
		mode  string
		calls int32
	}{
		{"wrong-direction", 0}, {"early-final", 0}, {"wrong-profile", 0}, {"wrong-body", 0}, {"truncated", 0}, {"header-overflow", 0},
		{"header-first-over", 0},
		{"duplicate", 1}, {"denied", 1}, {"trailing", 1}, {"missing-final", 1}, {"nonzero", 1}, {"final-overflow", 1},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			s, _, in, plan, _, calls, _ := piProcessFixture(t, tc.mode)
			if tc.mode == "denied" {
				send := in.PhysicalSend.(*preparedAPIPhysicalSend)
				send.authorize = func() (time.Time, error) {
					calls.Add(1)
					return time.Time{}, errors.New("synthetic private diagnostic")
				}
			}
			result, err := s.RunPreparedPi(in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx, in, plan)
			if err == nil && result.FailureCode == "" && result.ExitCode == 0 {
				t.Fatalf("mode=%s accepted", tc.mode)
			}
			if _, ok := result.PiStdioObservationV1(); ok {
				t.Fatal("failed attempt exposed success observation")
			}
			if calls.Load() != tc.calls {
				t.Fatalf("mode=%s Sourcecalls=%d want=%d", tc.mode, calls.Load(), tc.calls)
			}
			if !in.PhysicalSend.(*preparedAPIPhysicalSend).retired.Load() {
				t.Fatal("malformed attempt not revoked")
			}
			if !result.BoundaryReleased || !result.DescendantsReaped {
				t.Fatalf("failed process not joined: %+v", result)
			}
		})
	}
}

func TestPiStdioAttestationFailureHasNoPhaseCall(t *testing.T) {
	for _, mode := range []string{"attestation", "extra-fd", "started-stdin"} {
		t.Run(mode, func(t *testing.T) {
			s, l, in, plan, _, calls, _ := piProcessFixture(t, "ok")
			if mode == "attestation" {
				l.mutateAttestation = func(a *WorkloadAttestation) { a.Arguments = []string{"wrong"} }
			}
			if mode == "extra-fd" || mode == "started-stdin" {
				l.wrapCommand = func(spec LaunchSpec) *exec.Cmd {
					c := exec.Command(spec.Executable, spec.Arguments...)
					if mode == "extra-fd" {
						c.ExtraFiles = []*os.File{os.Stdin}
					} else {
						c.Stdin = strings.NewReader("untrusted")
					}
					return c
				}
			}
			result, err := s.RunPreparedPi(in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx, in, plan)
			if err == nil || calls.Load() != 0 || !result.BoundaryReleased || !result.CleanupSucceeded {
				t.Fatalf("mode=%s result=%+v calls=%d err=%v", mode, result, calls.Load(), err)
			}
			if !in.PhysicalSend.(*preparedAPIPhysicalSend).retired.Load() {
				t.Fatal("pre-start failure permitted retry")
			}
		})
	}
}

func TestPiStdioCancellationJoinsHeldSource(t *testing.T) {
	s, _, in, plan, _, calls, cancel := piProcessFixture(t, "ok")
	entered, exited := make(chan struct{}), make(chan struct{})
	send := in.PhysicalSend.(*preparedAPIPhysicalSend)
	send.authorize = func() (time.Time, error) {
		calls.Add(1)
		close(entered)
		<-send.ctx.Done()
		close(exited)
		return time.Time{}, send.ctx.Err()
	}
	ctx := in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx
	defer cancel()
	done := make(chan struct{})
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.RunPreparedPi(ctx, in, plan) }()
	t.Cleanup(func() { cancel(); apiWait(t, done) })
	apiWait(t, entered)
	if err := plan.Close(); err == nil {
		t.Fatal("cleared beneath held Source")
	}
	cancel()
	apiWait(t, done)
	select {
	case <-exited:
	default:
		t.Fatal("returned before Source callback joined")
	}
	// Closing the cancelled use pipe can make the child exit on EOF before
	// kill(2). ESRCH/no KillSent is legitimate once the actual process is gone;
	// the invariant is revoked Source + joined process, not a signal flag.
	if runErr == nil || calls.Load() != 1 || send.ctx.Err() == nil || !result.BoundaryReleased || !result.DescendantsReaped {
		t.Fatalf("cancel result=%+v calls=%d err=%v", result, calls.Load(), runErr)
	}
	if _, ok := result.PiStdioObservationV1(); ok {
		t.Fatal("cancelled attempt minted success observation")
	}
}

func TestPiStdioCancellationUnblocksInitBackpressure(t *testing.T) {
	s, _, in, plan, _, calls, cancel := piProcessFixture(t, "no-read")
	ready := filepath.Join(newCanonicalTempDir(t), "child-ready")
	in.Process.Environment = append(in.Process.Environment, EnvironmentVariable{Name: helperReady, Value: ready})
	in.Process.Stdin = bytes.Repeat([]byte("x"), int(plan.limits.InitializeBodyBytes))
	ctx := in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx
	defer cancel()
	done := make(chan struct{})
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.RunPreparedPi(ctx, in, plan) }()
	t.Cleanup(func() { cancel(); apiWait(t, done) })
	// Actual child barrier, not a sleep pretending its stdin is blocked.
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	waiting := true
	for waiting {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		select {
		case <-done:
			t.Fatalf("child exited before readiness: %+v %v", result, runErr)
		case <-timer.C:
			if _, err := os.Stat(ready); err != nil {
				t.Fatal("child readiness timeout")
			}
			waiting = false
		case <-tick.C:
		}
	}
	cancel()
	apiWait(t, done)
	if runErr == nil || calls.Load() != 0 || !result.KillSent || !result.DescendantsReaped || plan.state.Load() != 2 {
		t.Fatalf("held init writer survived result=%+v err=%v", result, runErr)
	}
}

func TestPiStdioOriginalCutoffNoRenewalAndTimerJoin(t *testing.T) {
	// synctest's wall clock has no original monotonic component. Keep a real
	// monotonic anchor OUTSIDE the bubble, then project ONLY synthetic elapsed
	// time onto it. Production's lost-monotonic denial remains unchanged.
	monoStart := time.Now()
	synctest.Test(t, func(t *testing.T) {
		fakeStart := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sourceCtx, cancelSource := context.WithCancel(ctx)
		send := &preparedAPIPhysicalSend{ctx: sourceCtx}
		send.cancel.Store(&preparedAPICancel{cancel: cancelSource})
		b := &piStdioBridge{ctx: ctx, cancel: cancel, send: send, processDeadline: monoStart.Add(90 * time.Minute), clock: func() time.Time { return monoStart.Add(time.Since(fakeStart)) }}
		t.Cleanup(b.stopAndJoin)
		original := monoStart.Add(30 * time.Minute)
		n, err := b.arm(original)
		if err != nil || n != 30*time.Minute || b.cutoff != original {
			t.Fatalf("original bound n=%v err=%v", n, err)
		}
		if _, err := b.arm(monoStart.Add(time.Hour)); err == nil {
			t.Fatal("renewed physical F")
		}
		time.Sleep(30 * time.Minute)
		synctest.Wait()
		if !b.expired.Load() || ctx.Err() == nil || sourceCtx.Err() == nil || !b.send.retired.Load() {
			t.Fatal("F did not revoke both original use and IO")
		}
		b.stopAndJoin() // actual timer callback must have joined
	})
}

func TestPiStdioProspectiveLedgerAndFormat(t *testing.T) {
	s, _, in, plan, ledger, _, _ := piProcessFixture(t, "ok")
	if ledger.live != plan.live || ledger.copy == 0 || ledger.nodes != 7 || len(plan.init) != int(plan.limits.FrameBytes) || len(plan.assembly) != int(plan.limits.FrameBytes) {
		t.Fatal("Go arenas not prospectively funded")
	}
	deny := &apiTestLedger{deny: true}
	if p, err := s.PreparePiStdioV1(in.APIRequest.Profile, deny); err == nil || p != nil {
		t.Fatal("allocated plan despite failed original reservation")
	}
	if text := fmt.Sprintf("%+v %#v", plan, plan); strings.Contains(text, string(in.Process.Stdin)) {
		t.Fatal("plan formatter leaked private input")
	}
}

func TestPiStdioMaximumWholeRequest(t *testing.T) {
	s, _, in, plan, _, calls, _ := piProcessFixture(t, "max-request")
	body := bytes.Repeat([]byte("x"), int(plan.limits.RequestBodyBytes))
	in.APIRequest.Body = body
	in.PhysicalSend.(*preparedAPIPhysicalSend).body = body
	result, err := s.RunPreparedPi(in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx, in, plan)
	if err != nil || result.FailureCode != "" || calls.Load() != 1 {
		t.Fatalf("max original whole request result=%+v calls=%d err=%v", result, calls.Load(), err)
	}
	obs, ok := result.PiStdioObservationV1()
	if !ok || string(obs.Final) != piSyntheticFinal || result.StdoutBytes != int(plan.limits.FrameBytes)+17+len(piSyntheticFinal) {
		t.Fatal("maximum frame was truncated or substituted")
	}
}

func TestPiStdioRejectsLateOrDifferentOriginalLedger(t *testing.T) {
	for _, mode := range []string{"new-ledger", "missing-ledger", "late-plan", "wall-issue", "body-mismatch", "parent-context", "profile", "executable"} {
		t.Run(mode, func(t *testing.T) {
			s, _, in, plan, _, calls, _ := piProcessFixture(t, "ok")
			ctx := in.PhysicalSend.(*preparedAPIPhysicalSend).ownerCtx
			switch mode {
			case "new-ledger":
				in.Ledger = &apiTestLedger{}
			case "missing-ledger":
				in.Ledger = nil
			case "late-plan":
				in.apiIssueStarted = plan.preparedAt.Add(-time.Nanosecond)
			case "wall-issue":
				in.apiIssueStarted = in.apiIssueStarted.Round(0)
			case "body-mismatch":
				in.APIRequest.Body = []byte("foreign whole body")
			case "parent-context":
				ctx = context.Background()
			case "profile":
				in.APIRequest.Profile.Model.ModelID = "foreign-model"
			case "executable":
				in.Process.ExecutableDigest[0] ^= 1
			}
			result, err := s.RunPreparedPi(ctx, in, plan)
			if err == nil || calls.Load() != 0 {
				t.Fatalf("mode=%s passed before-start guard result=%+v err=%v", mode, result, err)
			}
			if !in.PhysicalSend.(*preparedAPIPhysicalSend).retired.Load() {
				t.Fatal("invalid early attempt retained a permit")
			}
		})
	}
}

func TestPiStdioWholeEnvelopeDeniedBeforeAllocation(t *testing.T) {
	s, _, in, _, _, _, _ := piProcessFixture(t, "ok")
	for _, mode := range []string{"events", "stdout"} {
		t.Run(mode, func(t *testing.T) {
			p := in.APIRequest.Profile
			if mode == "events" {
				p.Limits.ProcessEvents = 3
			} else {
				p.Limits.ProcessStdoutBytes = 128
			}
			ledger := &apiTestLedger{}
			plan, err := s.PreparePiStdioV1(p, ledger)
			if err == nil || plan != nil || ledger.live != 0 || ledger.copy != 0 {
				t.Fatal("unfitting whole envelope allocated a partial plan")
			}
		})
	}
}

func TestPiStdioHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	mode := os.Getenv(helperMode)
	if mode == "no-read" {
		signal.Ignore(syscall.SIGTERM)
		events := make(chan os.Signal, 1)
		signal.Notify(events, syscall.SIGUSR1)
		if err := os.WriteFile(os.Getenv(helperReady), []byte("ready"), 0600); err != nil {
			os.Exit(70)
		}
		for {
			<-events
		}
	}
	l := pisdkphasewire.FrameLimits{InitializeBodyBytes: (1 << 20) - 17, RequestBodyBytes: (1 << 20) - 146, FinalBodyBytes: 128 << 10, FrameBytes: 1 << 20}
	read := func() pisdkphasewire.Frame {
		var h [17]byte
		if _, err := io.ReadFull(piOneByteReader{os.Stdin}, h[:]); err != nil {
			os.Exit(71)
		}
		head, err := pisdkphasewire.InspectHeader(h[:], l)
		if err != nil {
			os.Exit(72)
		}
		b := make([]byte, int(head.TotalBytes))
		copy(b, h[:])
		if _, err = io.ReadFull(piOneByteReader{os.Stdin}, b[17:]); err != nil {
			os.Exit(73)
		}
		f, err := pisdkphasewire.DecodeFrame(b, l)
		if err != nil {
			os.Exit(74)
		}
		return f
	}
	init := read()
	if init.Kind != pisdkphasewire.Initialize {
		os.Exit(75)
	}
	profile := string(init.Body)
	req := pisdkphasewire.Frame{Kind: pisdkphasewire.PhysicalRequest, BackendProfileDigest: profile, Body: []byte(piSyntheticRequest)}
	if mode == "max-request" {
		req.Body = bytes.Repeat([]byte("x"), int(l.RequestBodyBytes))
	}
	if mode == "wrong-profile" {
		req.BackendProfileDigest = strings.Repeat("a", 64)
	}
	if mode == "wrong-body" {
		req.Body[0] = '!'
	}
	encode := func(f pisdkphasewire.Frame) []byte {
		b, err := pisdkphasewire.EncodeFrame(f, l)
		if err != nil {
			os.Exit(76)
		}
		return b
	}
	write := func(b []byte) {
		if mode == "max-request" {
			if n, err := os.Stdout.Write(b); err != nil || n != len(b) {
				os.Exit(77)
			}
			return
		}
		for _, v := range b {
			if _, err := os.Stdout.Write([]byte{v}); err != nil {
				os.Exit(77)
			}
		}
	}
	final := encode(pisdkphasewire.Frame{Kind: pisdkphasewire.StructuredFinal, Body: []byte(piSyntheticFinal)})
	if mode == "wrong-direction" {
		write(encode(pisdkphasewire.Frame{Kind: pisdkphasewire.UseResult}))
		os.Exit(0)
	}
	if mode == "early-final" {
		write(final)
		os.Exit(0)
	}
	request := encode(req)
	if mode == "truncated" {
		write(request[:18])
		os.Exit(0)
	}
	if mode == "header-overflow" {
		binary.BigEndian.PutUint64(request[9:17], ^uint64(0))
		write(request[:17])
		os.Exit(0)
	}
	if mode == "header-first-over" {
		binary.BigEndian.PutUint64(request[9:17], l.RequestBodyBytes+1)
		write(request[:17])
		os.Exit(0)
	}
	write(request)
	use := read()
	if use.Kind != pisdkphasewire.UseResult || !use.Use.Allowed {
		os.Exit(78)
	}
	if mode == "duplicate" {
		write(request)
		os.Exit(0)
	}
	if mode == "missing-final" {
		os.Exit(0)
	}
	if mode == "final-overflow" {
		binary.BigEndian.PutUint64(final[9:17], uint64(128<<10)+1)
		write(final[:17])
		os.Exit(0)
	}
	write(final)
	if mode == "trailing" {
		write([]byte("x"))
	}
	if mode == "nonzero" {
		os.Exit(4)
	}
	os.Exit(0)
}
