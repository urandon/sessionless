//go:build darwin || linux

package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

// These fixtures use the SAME real Supervisor/process/pipes and a test-only
// launcher/Go driver. They are NOT Pi, real Source, HTTP/provider, OCI isolation,
// original prepared producer eligibility or MVP demo evidence.
func piHTTPProcessFixture(t *testing.T, mode string) (*Supervisor, *piHTTPStdio, AttemptSpec, *apiTestLedger, context.CancelFunc) {
	t.Helper()
	s, _, exe, digest := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: 5 * time.Second})
	s.timeout = time.Hour
	l := piHTTPTestLimits()
	frames := piHTTPTestFrames(l, []byte("0123456789"))
	if mode == "blocked_init" {
		l.FrameBytes = 1 << 20
		l.InitializeBytes = (1 << 20) - 113
		frames[0].Body = bytes.Repeat([]byte{'x'}, int(l.InitializeBytes))
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	use, cancelUse := context.WithCancel(ctx)
	t.Cleanup(cancelUse)
	ledger := &apiTestLedger{}
	send := &preparedAPIPhysicalSend{ctx: use, ownerCtx: ctx, ledger: ledger, body: frames[1].Body, profile: "fixture-profile",
		authorize: func() (time.Time, error) { return time.Now().Add(30 * time.Minute), nil }}
	send.cancel.Store(&preparedAPICancel{cancel: cancelUse})
	if err := send.prepareDeadline(); err != nil {
		t.Fatal(err)
	}
	if err := send.deadline.bindOwner(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { send.retire() })
	driver := func(ctx context.Context, v *piHTTPIO) error {
		if mode == "no_request" {
			return nil
		}
		request, d, err := v.request()
		if err != nil {
			return err
		}
		if !bytes.Equal(request.Body, frames[1].Body) || d == [32]byte{} {
			return ErrSupervisorConfig
		}
		if mode == "driver_error" {
			return errors.New("synthetic driver error")
		}
		if mode == "early_return" {
			return nil
		}
		if mode == "request_twice" {
			_, _, err = v.request()
			return err
		}
		if mode == "mutated_request" {
			request.Body[0] ^= 1
		}
		if mode == "quarantine" || mode == "cancel_read" {
			<-ctx.Done()
			return ctx.Err()
		}
		if _, err = send.Authorize(request.Body, "fixture-profile"); err != nil {
			return err
		}
		for _, f := range frames[2:] {
			if f.Kind == pisdkhttpwire.Pull || f.Kind == pisdkhttpwire.StructuredFinal {
				continue
			}
			if err = v.write(f); err != nil {
				return err
			}
		}
		return nil
	}
	b, err := preparePiHTTPStdio(s, l, ledger, send, frames[0], driver)
	if err != nil {
		t.Fatal("prepare actual-pipe mechanics", err)
	}
	t.Cleanup(func() {
		cancel()
		if err := b.clear(); err != nil {
			t.Errorf("pipe plan cleanup: %v", err)
		}
	})
	spec := AttemptSpec{Executable: exe, ExecutableDigest: digest, Arguments: []string{"-test.run=^TestPiHTTPStdioHelperProcess$"},
		Environment: []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: mode}}, piHTTP: b}
	return s, b, spec, ledger, cancel
}

func requirePiHTTPMechanicsSuccess(t *testing.T, b *piHTTPStdio, result AttemptResult, err error) {
	t.Helper()
	if err != nil || result.FailureCode != "" || result.ExitCode != 0 || !result.CleanupSucceeded || !result.BoundaryReleased || !result.DescendantsReaped || !b.complete || b.failure.Load() {
		t.Fatalf("actual mechanics result=%+v err=%v complete=%t failure=%t", result, err, b.complete, b.failure.Load())
	}
	if result.piStdio != nil || len(result.Stdout) != 0 || result.StdoutBytes == 0 {
		t.Fatal("mechanics minted producer/public raw stdout")
	}
}

func TestPiHTTPStdioActualSupervisorFragmentedRoundTrip(t *testing.T) {
	s, b, spec, ledger, _ := piHTTPProcessFixture(t, "ok")
	s.outputReader = func(stream string, r io.Reader) io.Reader {
		if stream == "stdout" {
			return piOneByteReader{r}
		}
		return r
	}
	result, err := s.Run(b.ctx, spec)
	requirePiHTTPMechanicsSuccess(t, b, result, err)
	frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
	var raw []byte
	for _, f := range frames {
		if f.Kind == pisdkhttpwire.FetchRequest || f.Kind == pisdkhttpwire.Pull || f.Kind == pisdkhttpwire.StructuredFinal {
			raw = append(raw, piHTTPTestEncode(t, f, b.core.limits)...)
		}
	}
	wantRequest := piHTTPTestEncode(t, frames[1], b.core.limits)
	if b.stdoutDigest != sha256.Sum256(raw) || b.requestDigest != sha256.Sum256(wantRequest) || result.StdoutBytes != len(raw) || !bytes.Equal(b.core.final[:b.core.finalSize], frames[len(frames)-1].Body) {
		t.Fatal("actual stdout/request/Final digest or byte count mismatch")
	}
	if _, err := s.Run(b.ctx, spec); err == nil {
		t.Fatal("used pipe plan launched another process")
	}
	work := ledger.copy
	final := b.core.final
	leaseCopy := b.lease
	if err := b.clear(); err != nil || ledger.live != 0 || ledger.copy != work {
		t.Fatalf("retirement live=%d err=%v", ledger.live, err)
	}
	if !bytes.Equal(final, make([]byte, len(final))) {
		t.Fatal("retained Final not zeroed")
	}
	if err := leaseCopy.write(frames[2]); err == nil {
		t.Fatal("copied retired lease revived write")
	}
}

func TestPiHTTPStdioRejectsActualMalformedAndSpeculativeBytes(t *testing.T) {
	for _, mode := range []string{"quarantine", "pending_speculative", "bad_header", "bad_hq", "extra_final", "truncated", "driver_error", "early_return", "no_request", "request_twice", "mutated_request"} {
		t.Run(mode, func(t *testing.T) {
			s, b, spec, _, _ := piHTTPProcessFixture(t, mode)
			if mode == "pending_speculative" {
				b.afterWrite = func(k pisdkhttpwire.Kind) {
					if k == pisdkhttpwire.ResponseStart {
						<-b.ctx.Done()
					}
				}
			}
			result, err := s.Run(b.ctx, spec)
			if err == nil || result.FailureCode == "" || b.complete || result.piStdio != nil || len(result.Stdout) != 0 {
				t.Fatalf("malformed stream successful mode=%s result=%+v err=%v", mode, result, err)
			}
			frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
			if mode == "bad_header" && b.childBytes != 17 {
				t.Fatalf("oversized header consumed declared body: bytes=%d", b.childBytes)
			}
			if mode == "bad_hq" {
				raw := piHTTPTestEncode(t, frames[1], b.core.limits)
				want := uint64(17 + binary.BigEndian.Uint32(raw[5:9]))
				if b.childBytes != want {
					t.Fatalf("bad Hq consumed body: bytes=%d want=%d", b.childBytes, want)
				}
			}
			if mode == "pending_speculative" && b.lastParent.Load() != uint32(pisdkhttpwire.Initialize) {
				t.Fatal("speculative prefix waited until replay credit became legal")
			}
			if mode == "mutated_request" {
				if b.requestDigest != sha256.Sum256(piHTTPTestEncode(t, frames[1], b.core.limits)) || b.send.authorized {
					t.Fatal("borrowed request mutation changed frozen actual-read digest or obtained authorization")
				}
			}
		})
	}
}

func TestPiHTTPStdioEndReturnHeldAcrossChildFinalExit(t *testing.T) {
	s, b, spec, _, cancel := piHTTPProcessFixture(t, "ok")
	written, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	b.afterWrite = func(k pisdkhttpwire.Kind) {
		if k == pisdkhttpwire.ResponseEnd {
			close(written)
			select {
			case <-release:
			case <-b.ctx.Done():
			}
		}
	}
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { cancel(); unblock(); apiWait(t, done) })
	apiWait(t, written)
	apiWait(t, b.send.ctx.Done()) // actual command.Wait performed normal Source withdrawal
	if b.ctx.Err() != nil || b.send.invalid.Load() {
		t.Fatal("normal exit cancelled in-flight End/Final drain")
	}
	b.core.mu.Lock()
	earlyFinal := b.core.finalReceived
	earlyBytes := b.core.finalSize
	b.core.mu.Unlock()
	if earlyFinal || earlyBytes != 0 {
		t.Fatal("Final body read before actual End Write committed")
	}
	unblock()
	apiWait(t, done)
	requirePiHTTPMechanicsSuccess(t, b, result, runErr)
}

func TestPiHTTPStdioDriverReturnDoesNotCancelFinalDrain(t *testing.T) {
	s, b, spec, _, cancel := piHTTPProcessFixture(t, "ok")
	frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
	remaining := len(piHTTPTestEncode(t, frames[1], b.core.limits)) + 2*21
	entered, release, done, returned := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	underlying := b.driver
	b.driver = func(ctx context.Context, v *piHTTPIO) error { defer close(returned); return underlying(ctx, v) }
	s.outputReader = func(stream string, r io.Reader) io.Reader {
		if stream == "stdout" {
			return &piHeldFinalReader{reader: r, remaining: remaining, entered: entered, release: release, ctx: b.ctx}
		}
		return r
	}
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { cancel(); unblock(); apiWait(t, done) })
	apiWait(t, entered)
	apiWait(t, returned)
	if b.ctx.Err() != nil {
		t.Fatal("nil driver return cancelled reader")
	}
	select {
	case <-done:
		t.Fatal("Run returned before actual Final/EOF")
	default:
	}
	unblock()
	apiWait(t, done)
	requirePiHTTPMechanicsSuccess(t, b, result, runErr)
}

func TestPiHTTPStdioCancellationUnblocksReadAndInitializeWrite(t *testing.T) {
	for _, mode := range []string{"cancel_read", "blocked_init"} {
		t.Run(mode, func(t *testing.T) {
			s, b, spec, _, cancel := piHTTPProcessFixture(t, mode)
			started, done := make(chan struct{}), make(chan struct{})
			s.outputDone = func(stream string, _ <-chan struct{}) {
				if stream == "stdout" {
					close(started)
				}
			}
			var result AttemptResult
			var runErr error
			go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
			t.Cleanup(func() { cancel(); apiWait(t, done) })
			apiWait(t, started)
			if mode == "cancel_read" {
				apiWait(t, b.requestReady)
			}
			cancel()
			apiWait(t, done)
			if runErr == nil || result.FailureCode == "" || b.complete || result.piStdio != nil {
				t.Fatalf("cancelled run success: %+v err=%v", result, runErr)
			}
			if b.core.writer.Load() || b.writing.Load() {
				t.Fatal("actual writer not joined after cancel")
			}
		})
	}
}

func TestPiHTTPStdioCancelBeforeAttachClosesLateDescriptors(t *testing.T) {
	_, b, _, _, _ := piHTTPProcessFixture(t, "ok")
	b.stopIO()
	inRead, inWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inRead.Close(); _ = inWrite.Close() })
	outRead, outWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = outRead.Close(); _ = outWrite.Close() })
	b.start(outRead, outRead, inWrite, &boundedBuffer{limit: 4096, discard: true}, make(chan string, 1))
	if _, err := inWrite.Write([]byte{'x'}); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("late stdin remained open: %v", err)
	}
	if _, err := outRead.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("late stdout remained open: %v", err)
	}
}

func TestPiHTTPStdioRejectsForeignAndConflictingArms(t *testing.T) {
	for _, mode := range []string{"foreign_owner", "ordinary_stdin", "legacy_arm", "wrong_context"} {
		t.Run(mode, func(t *testing.T) {
			s, b, spec, _, _ := piHTTPProcessFixture(t, "ok")
			ctx := b.ctx
			switch mode {
			case "foreign_owner":
				b.owner = &Supervisor{}
				t.Cleanup(func() { b.owner = s })
			case "ordinary_stdin":
				spec.Stdin = []byte("forbidden")
			case "legacy_arm":
				spec.piStdio = &piStdioBridge{}
			case "wrong_context":
				ctx = context.Background()
			}
			if _, err := s.Run(ctx, spec); err == nil {
				t.Fatal("foreign/conflicting pipe arm accepted")
			}
		})
	}
}

func TestPiHTTPStdioSetupAndStartFailureRetireInitBorrow(t *testing.T) {
	for _, mode := range []string{"attestation", "start"} {
		t.Run(mode, func(t *testing.T) {
			s, b, spec, ledger, _ := piHTTPProcessFixture(t, "ok")
			launcher := s.launcher.(*fixtureLauncher)
			if mode == "attestation" {
				launcher.mutateAttestation = func(a *WorkloadAttestation) { a.ExecutableDigest[0] ^= 1 }
			} else {
				missing := filepath.Join(newCanonicalTempDir(t), "not-installed-test-client")
				launcher.wrapCommand = func(LaunchSpec) *exec.Cmd { return exec.Command(missing) }
			}
			result, err := s.Run(b.ctx, spec)
			if err == nil || b.started || b.complete || result.piStdio != nil || b.core.writer.Load() {
				t.Fatalf("setup/start failure leaked arm: %+v err=%v", result, err)
			}
			if err := b.clear(); err != nil || ledger.live != 0 {
				t.Fatalf("unstarted Init retirement live=%d err=%v", ledger.live, err)
			}
		})
	}
}

func TestPiHTTPStdioFragmentedPrefixKeepsEpochAcrossWriteCompletion(t *testing.T) {
	s, b, spec, _, cancel := piHTTPProcessFixture(t, "ok")
	frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
	headerHeld, releaseHeader, releaseWrite, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var headerOnce, writeOnce sync.Once
	unblockHeader := func() { headerOnce.Do(func() { close(releaseHeader) }) }
	unblockWrite := func() { writeOnce.Do(func() { close(releaseWrite) }) }
	s.outputReader = func(stream string, r io.Reader) io.Reader {
		if stream == "stdout" {
			return &piHeldFinalReader{reader: r,
				remaining: len(piHTTPTestEncode(t, frames[1], b.core.limits)) + 1, entered: headerHeld, release: releaseHeader, ctx: b.ctx}
		}
		return r
	}
	b.afterWrite = func(k pisdkhttpwire.Kind) {
		if k == pisdkhttpwire.ResponseStart {
			select {
			case <-releaseWrite:
			case <-b.ctx.Done():
			}
		}
	}
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { unblockHeader(); unblockWrite(); cancel(); apiWait(t, done) })
	apiWait(t, headerHeld) // next Read started; first positive prefix already reserved
	b.core.mu.Lock()
	frozen := b.core.input
	pendingEpoch := b.core.write.epoch
	b.core.mu.Unlock()
	if frozen.stage != 1 || frozen.prerequisite != pendingEpoch || frozen.kind != pisdkhttpwire.Pull || frozen.index != 0 {
		t.Fatal("first observed prefix did not freeze pending Start epoch")
	}
	unblockWrite()
	waitPiHTTPCondition(t, "Start actual write committed", func() bool { return b.lastParent.Load() == uint32(pisdkhttpwire.ResponseStart) })
	b.core.mu.Lock()
	after := b.core.input
	credit := b.core.credit
	b.core.mu.Unlock()
	if after != frozen || credit {
		t.Fatal("incomplete old header changed epoch or acquired credit after Write completion")
	}
	unblockHeader()
	apiWait(t, done)
	requirePiHTTPMechanicsSuccess(t, b, result, runErr)
}

func TestPiHTTPStdioOriginalSourceDeadlineCancelsActualPipe(t *testing.T) {
	s, b, spec, _, cancel := piHTTPProcessFixture(t, "ok")
	authorized, done := make(chan struct{}), make(chan struct{})
	// This is an intentional real-clock expiry test around an OS process, not
	// a short positive lifetime or an elapsed sleep used as readiness evidence.
	b.send.authorize = func() (time.Time, error) { return time.Now().Add(time.Second), nil }
	b.driver = func(ctx context.Context, v *piHTTPIO) error {
		req, _, err := v.request()
		if err != nil {
			return err
		}
		if _, err = b.send.Authorize(req.Body, "fixture-profile"); err != nil {
			return err
		}
		close(authorized)
		<-ctx.Done()
		return ctx.Err()
	}
	var result AttemptResult
	var runErr error
	go func() { defer close(done); result, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { cancel(); apiWait(t, done) })
	apiWait(t, authorized)
	apiWait(t, b.send.deadline.ioContext().Done())
	apiWait(t, done)
	if !b.send.deadline.expired.Load() || !b.send.invalid.Load() || !result.Deadline || runErr == nil || b.complete || b.core.writer.Load() {
		t.Fatalf("original Source/F did not cancel/join actual pipe: %+v err=%v", result, runErr)
	}
}

func TestPiHTTPStdioHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	mode := os.Getenv(helperMode)
	if mode == "blocked_init" {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
		<-signals
		os.Exit(0)
	}
	l := piHTTPTestLimits()
	read := func() pisdkhttpwire.Frame {
		var h [17]byte
		if _, err := io.ReadFull(os.Stdin, h[:]); err != nil {
			os.Exit(31)
		}
		head, err := pisdkhttpwire.InspectHeader(h[:], pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(32)
		}
		raw := make([]byte, int(head.TotalBytes))
		copy(raw, h[:])
		if _, err = io.ReadFull(os.Stdin, raw[17:]); err != nil {
			os.Exit(33)
		}
		f, err := pisdkhttpwire.DecodeView(raw, pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(34)
		}
		return f
	}
	write := func(f pisdkhttpwire.Frame) {
		raw := piHTTPTestEncode(t, f, l)
		if _, err := os.Stdout.Write(raw); err != nil {
			os.Exit(35)
		}
	}
	frames := piHTTPTestFrames(l, []byte("0123456789"))
	if read().Kind != pisdkhttpwire.Initialize {
		os.Exit(36)
	}
	rawRequest := piHTTPTestEncode(t, frames[1], l)
	if mode == "bad_header" {
		binary.BigEndian.PutUint64(rawRequest[9:17], math.MaxUint64)
		_, _ = os.Stdout.Write(rawRequest)
		os.Exit(0)
	}
	if mode == "bad_hq" {
		binary.BigEndian.PutUint32(rawRequest[17+64:], math.MaxUint32)
		_, _ = os.Stdout.Write(rawRequest)
		os.Exit(0)
	}
	write(frames[1])
	if mode == "truncated" {
		os.Exit(0)
	}
	if mode == "quarantine" {
		_, _ = os.Stdout.Write([]byte{'S'})
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
	if read().Kind != pisdkhttpwire.ResponseStart {
		os.Exit(37)
	}
	for i := 3; i < len(frames)-2; i += 2 {
		write(frames[i])
		if mode == "pending_speculative" {
			_, _ = os.Stdout.Write([]byte{'S'})
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(0)
		}
		f := read()
		if f.Kind != pisdkhttpwire.ResponsePart || f.Index != frames[i+1].Index || !bytes.Equal(f.Body, frames[i+1].Body) {
			os.Exit(38)
		}
	}
	if read().Kind != pisdkhttpwire.ResponseEnd {
		os.Exit(39)
	}
	write(frames[len(frames)-1])
	if mode == "extra_final" {
		_, _ = os.Stdout.Write([]byte{'S'})
	}
	os.Exit(0)
}
