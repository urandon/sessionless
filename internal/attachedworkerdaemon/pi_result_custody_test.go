//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const custodyArtifactFinal = `{"answer":"useful answer","artifacts":[{"name":"report.txt","media_type":"text/plain","content":"provider authored text"},{"name":"numbers.json","media_type":"application/json","content":"{\"n\":9007199254740993}"}]}`

// Every positive below gets its observation from actual SAME Supervisor.Run,
// with a public local helper and original file lifecycle. Source/profile/OS
// isolation are SYNTHETIC, not real SDK/Source/native/serving evidence. No test
// constructs a success observation or public witness factory.
func piCustodyFixture(t *testing.T, mode string, mutate func(*AttemptResult), retire func(context.Context) error) (*InvocationRunner, Invocation, *apiTestParent, *apiTestLifecycle) {
	t.Helper()
	inv, parent, life := longLivedAPIOwnerFixture(t)
	s, _, exe, digest := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: maxSupervisorGrace})
	s.timeout = time.Hour
	parent.profile = apiOwnerProfile(t, digest)
	hash := sha256.Sum256([]byte(attachedworkeroutput.StructuredFinalSchemaV1))
	parent.profile.Sources.FinalSchemaDigest = hex.EncodeToString(hash[:])
	desc, err := parent.profile.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	inv.Process.Executable, inv.Process.ExecutableDigest = exe, digest
	inv.Process.Arguments = []string{"-test.run=^TestPiCustodyHelperProcess$"}
	inv.Process.Environment = []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: mode}}
	inv.Process.Stdin = nil
	// The helper's public synthetic Init is prepared/funded by its fixture
	// parent, not smuggled through the original API invocation's empty stdin.
	var fixtureInit []byte
	inv.VerifiedInput.Job.HarnessBinding.Backend = desc
	contextHash, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.AcceptedAttempt.ContextDigest = contextHash
	parent.retirement.selected, parent.retirement.cancel = context.WithTimeout(context.Background(), 2*time.Hour)
	var plan *PreparedPiStdioV1
	parent.mutate = func(data *PreparedAPIDataV1) {
		data.ParentRequest.Body, data.ChildRequest.Body = []byte(piSyntheticRequest), []byte(piSyntheticRequest)
		data.ParentRequest.Schema, data.ChildRequest.Schema = []byte(attachedworkeroutput.StructuredFinalSchemaV1), []byte(attachedworkeroutput.StructuredFinalSchemaV1)
		data.InitArguments[9] = strconv.Itoa(len(data.ParentRequest.Body))
		data.ParentProcess.Arguments[len(data.ParentProcess.Arguments)-1] = data.InitArguments[9]
		data.ChildProcess.Arguments[len(data.ChildProcess.Arguments)-1] = data.InitArguments[9]
		fixtureInit = []byte(desc.BackendProfileDigest)
		parent.ownedBuffers = append(parent.ownedBuffers, fixtureInit)
		parent.ownedBuffers = append(parent.ownedBuffers, data.ParentRequest.Body, data.ChildRequest.Body, data.ParentRequest.Schema, data.ChildRequest.Schema)
		plan, err = s.PreparePiStdioV1(parent.profile, data.Ledger)
		if err != nil {
			t.Fatal("original pre-Issue stdio allocation:", err)
		}
	}
	parent.retireStage = func(ctx context.Context, id InvocationIdentity, data *PreparedAPIDataV1) error {
		parent.order = append(parent.order, "staging_cleanup")
		if ctx == nil || ctx != parent.retirement.selected || ctx.Err() != nil || id != inv.Identity || data != parent.data || life.active {
			return ErrInvocationInvalid
		}
		if life.releaseErr == nil && life.file != nil {
			if _, err := os.Lstat(life.plan.FilePath); !errors.Is(err, os.ErrNotExist) {
				return ErrInvocationInvalid
			}
		}
		if retire != nil {
			if err := retire(ctx); err != nil {
				return err
			}
		}
		if plan != nil {
			return plan.Close()
		}
		return nil
	}
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		s.allowedReadRoots = []string{filepath.Dir(input.Materialization.RootDir)}
		input.Process.Stdin = fixtureInit // borrowed already-funded synthetic Init
		result, runErr := s.RunPreparedPi(ctx, input, plan)
		if mutate != nil {
			mutate(&result)
		} // negative controls mutate a GENUINE result only
		return result, runErr
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if plan != nil {
			if err := plan.Close(); err != nil {
				t.Error("owned plan cleanup:", err)
			}
		}
	})
	return runner, inv, parent, life
}

func TestPiResultCustodyActualOwnerRetryAndRetirement(t *testing.T) {
	r, inv, parent, life := piCustodyFixture(t, "answer", nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	result, runErr := r.Run(ctx, inv)
	if !result.Succeeded(runErr) {
		t.Fatalf("actual owner failed: result=%+v err=%v order=%v", result, runErr, parent.order)
	}
	if result.Process.piStdio == nil || result.Process.piStdio.send != nil || result.Process.piStdio.ledger != nil ||
		result.Process.piStdio.credential != (ports.ProviderInvocationCredentialV1{}) || result.Process.piStdio.materialization != (ports.ProviderCredentialMaterializationV1{}) {
		t.Fatal("receipt-held observation retained send/context/ledger/credential/locator capability")
	}
	if r.piSlot == nil || r.piSlot.result.Process.piStdio != nil || r.piSlot.stamp == nil {
		t.Fatal("private slot retained borrowed final/plan instead of non-secret original stamp")
	}
	first, err := r.ReceiptCandidate(inv.Identity, result, runErr, domain.AttachedWorkerTerminalSucceeded)
	if err != nil || first.Summary != "synthetic" || len(first.Artifacts) != 0 {
		t.Fatalf("candidate=%+v err=%v", first, err)
	}
	second, err := r.ReceiptCandidate(inv.Identity, result, runErr, domain.AttachedWorkerTerminalSucceeded)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("lost ACK retry changed original candidate")
	}
	foreign := inv.Identity
	foreign.AttemptID = "foreign-attempt"
	forged := result
	forged.Process.StdoutBytes++
	if _, err := r.ReceiptCandidate(inv.Identity, forged, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("caller result replaced private original")
	}
	if err := r.RetireReceipt(foreign); err == nil {
		t.Fatal("foreign journal retirement cleared original")
	}
	if _, err := r.Run(ctx, inv); err == nil {
		t.Fatal("pending receipt allowed another model invocation")
	}
	if life.callbackCount != 1 || life.releaseCount != 1 || life.finalizeCount != 1 {
		t.Fatal("receipt retry replayed lifecycle")
	}
	if err := r.RetireReceipt(inv.Identity); err != nil {
		t.Fatal("local trusted-adapter retirement:", err)
	} // NOT transport ACK/#201 proof
	if parent.ledger.live != 0 {
		t.Fatalf("owner live reservation survived exact retirement: %d", parent.ledger.live)
	}
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPiResultCustodyPendingUntilActualStagingCleanup(t *testing.T) {
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	r, inv, _, _ := piCustodyFixture(t, "answer", nil, func(ctx context.Context) error {
		close(entered)
		select {
		case <-resume:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	var result InvocationResult
	var runErr error
	t.Cleanup(func() { unblock(); apiWait(t, done) })
	go func() { defer close(done); result, runErr = r.Run(ctx, inv) }()
	apiWait(t, entered)
	if _, err := r.ReceiptCandidate(inv.Identity, InvocationResult{}, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("pending raw became eligible before actual staging cleanup")
	}
	r.piMu.Lock()
	slot := r.piSlot
	pending := slot != nil && slot.captured && !slot.frozen
	r.piMu.Unlock()
	if !pending {
		t.Fatal("genuine pending raw capture missing while cleanup held")
	}
	unblock()
	apiWait(t, done)
	if _, err := r.ReceiptCandidate(inv.Identity, result, runErr, domain.AttachedWorkerTerminalSucceeded); err != nil {
		t.Fatal("cleaned original lost eligibility:", err)
	}
	if err := r.RetireReceipt(inv.Identity); err != nil {
		t.Fatal(err)
	}
}

func TestPiResultCustodyRejectsForgeryAndCleanupFailures(t *testing.T) {
	for _, mode := range []string{"raw mutation", "foreign head", "foreign credential", "wrong schema", "release failure", "staging failure", "public flags"} {
		t.Run(mode, func(t *testing.T) {
			mutate := func(p *AttemptResult) {
				if p.piStdio == nil {
					return
				}
				switch mode {
				case "raw mutation":
					p.piStdio.Final[0] = '!'
				case "foreign head":
					p.piStdio.head.Revision++
				case "foreign credential":
					p.piStdio.credential.LeaseFence++
				case "wrong schema":
					p.piStdio.requestHash[2][0] ^= 1
				case "public flags":
					p.piStdio = nil
				}
			}
			var retire func(context.Context) error
			if mode == "staging failure" {
				retire = func(context.Context) error { return ErrInvocationInvalid }
			}
			r, inv, _, life := piCustodyFixture(t, "answer", mutate, retire)
			if mode == "release failure" {
				life.releaseErr = ErrInvocationInvalid
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			result, runErr := r.Run(ctx, inv)
			forged := result
			forged.FailureCode = ""
			forged.CredentialReleased = true
			forged.Process.Cancelled = false
			forged.Process.Deadline = false
			forged.Process.ExitCode = 0
			forged.Process.FailureCode = ""
			forged.Process.DescendantsReaped = true
			forged.Process.BoundaryReleased = true
			forged.Process.CleanupSucceeded = true
			if _, err := r.ReceiptCandidate(inv.Identity, forged, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
				t.Fatalf("%s forged private completion; original err=%v", mode, runErr)
			}
			if err := r.Close(ctx); err == nil {
				t.Fatal("Close silently discarded pending/reconciliation output")
			}
		})
	}
}

func TestPiResultCustodyRequiresNormalLifecycleReturn(t *testing.T) {
	var actualProcess AttemptResult
	r, inv, parent, life := piCustodyFixture(t, "answer", func(p *AttemptResult) { actualProcess = *p }, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	sentinel := errors.New("test-owned post-clear panic")
	parent.afterClear = func() { panic(sentinel) }
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = r.Run(ctx, inv)
	}()
	parent.afterClear = nil
	if recovered != sentinel || !parent.cleared || life.releaseCount != 1 || life.finalizeCount != 1 {
		t.Fatalf("actual post-cleanup panic not reached: recovered=%v cleared=%v release=%d finalize=%d", recovered, parent.cleared, life.releaseCount, life.finalizeCount)
	}
	// Rebuild public success flags around the GENUINE earlier process result.
	// Prior deferred finish incorrectly accepted this after Clear panicked,
	// even though the original Run never returned a result to its caller.
	forged := InvocationResult{Process: actualProcess, CredentialReleaseRequired: true, CredentialReleased: true}
	if _, err := r.ReceiptCandidate(inv.Identity, forged, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("unreturned lifecycle result became eligible through public flags")
	}
	r.piMu.Lock()
	pending := r.piSlot != nil && !r.piSlot.finished && r.piCleanupFailed
	r.piMu.Unlock()
	if !pending {
		t.Fatal("abnormal lifecycle return lost pending cleanup/custody state")
	}
	if _, err := r.Run(ctx, inv); err == nil || life.callbackCount != 1 {
		t.Fatal("post-panic custody permitted another model invocation")
	}
	if err := r.Close(ctx); err == nil {
		t.Fatal("post-panic custody falsely reported definitive cleanup")
	}
}

func TestPiResultCustodyDeclaredArtifactsNeverDropped(t *testing.T) {
	r, inv, _, _ := piCustodyFixture(t, "artifacts", nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	result, runErr := r.Run(ctx, inv)
	if !result.Succeeded(runErr) {
		t.Fatal("actual completed process/lifecycle:", runErr)
	}
	r.piMu.Lock()
	slot := r.piSlot
	if slot == nil || !slot.frozen || len(slot.final.Artifacts) != 2 || slot.final.Artifacts[0].Content != "provider authored text" ||
		slot.final.Artifacts[1].Content != `{"n":9007199254740993}` {
		r.piMu.Unlock()
		t.Fatal("private declared semantic content lost")
	}
	r.piMu.Unlock()
	if _, err := r.ReceiptCandidate(inv.Identity, result, runErr, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("missing authenticated pre-Put custody fabricated BlobRefs or dropped artifacts")
	}
	if err := r.RetireReceipt(inv.Identity); err == nil {
		t.Fatal("unbuilt artifact receipt retired private bytes")
	}
	if err := r.Close(ctx); err == nil {
		t.Fatal("unresolved artifact custody falsely discarded")
	}
}

func TestPiResultCustodyPreIssueAccountingAndPartition(t *testing.T) {
	inv, p, _ := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	data, err := p.PrepareOriginalAPIInvocation(ctx, inv.Identity, inv.VerifiedInput, inv.Process, inv.InputAcquisition)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Clear)
	p.ledger.deny = true
	if s, err := preparePiResultCustody(inv.Identity, data); err == nil || s != nil {
		t.Fatal("unfunded result allocation admitted")
	}
	p.ledger.deny = false
	before := p.ledger.live
	s, err := preparePiResultCustody(inv.Identity, data)
	if err != nil {
		t.Fatal(err)
	}
	if p.ledger.live != before+s.live || uint64(len(s.raw)) != data.ParentRequest.Profile.Limits.StructuredFinalBytes {
		t.Fatal("result funded on wrong/fresh ledger")
	}
	work, nodes := p.ledger.copy, p.ledger.nodes
	if err := s.clear(); err != nil || p.ledger.live != before || p.ledger.copy != work || p.ledger.nodes != nodes {
		t.Fatal("retirement refunded cumulative work/nodes")
	}
	// JSONNodes is positive but not globally numerically capped by the profile.
	// Byte-derived intersections must precede multiplication/addition.
	data.ParentRequest.Profile.Limits.JSONNodes = ^uint64(0)
	beforeNodes := p.ledger.nodes
	wide, err := preparePiResultCustody(inv.Identity, data)
	if err != nil {
		t.Fatal("legal symbolic node cap:", err)
	}
	wantNodes := data.ParentRequest.Profile.Limits.StructuredFinalBytes + 2*data.ParentRequest.Profile.Limits.ArtifactBytes + 2
	if p.ledger.nodes-beforeNodes != wantNodes {
		t.Fatal("node reserve wrapped or borrowed an independent full profile pool")
	}
	if err := wide.clear(); err != nil {
		t.Fatal(err)
	}
	partition := piResultParsePartition{copy: 4, live: 3, nodes: 2}
	if partition.Reserve(4, 3, 2) != nil || partition.Reserve(1, 0, 0) == nil || partition.Reserve(0, 0, 1) == nil {
		t.Fatal("prepaid parser remainder reset or overdrawn")
	}
}

// These are original-clock expiry/ownership tests, not process/SDK tests. The
// deadline channel is the expiry oracle (never a readiness sleep). Real Go
// monotonic samples are necessary here: wall-only fake samples would instead
// trigger the unrelated original provenance rejection. Staging is a test-owned
// file retained privately by the parent, never a cleanup path taken from DATA.
type piReturnedRetirementContext struct {
	ctx   context.Context
	calls int
}

func (r *piReturnedRetirementContext) Context() (context.Context, error) {
	r.calls++
	return r.ctx, nil
}

type piNilRetirementContext struct{ context.Context }

func TestPiResultCustodyEarlyReturnedRecordRetirement(t *testing.T) {
	for _, mode := range []string{"expired-original", "expired-cleanup-failed", "partial-record-error", "nil-retirement", "typed-nil-retirement", "nil-context", "typed-nil-context", "caller-alias"} {
		t.Run(mode, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			parent.retirement.selected, parent.retirement.cancel = context.WithTimeout(context.Background(), 2*time.Hour)
			originalRetirement := parent.retirement.selected
			stageDir := filepath.Join(t.TempDir(), "owned-staging")
			stageFile := filepath.Join(stageDir, "public-fixture.txt")
			var retireCalls, clearCalls, executeCalls int
			var cleanupContext context.Context
			malformedContext := &piReturnedRetirementContext{}
			var events []string
			callerHash := sha256.Sum256(inv.VerifiedInput.Context)
			if mode == "partial-record-error" || mode == "nil-context" || mode == "typed-nil-context" {
				parent.returnedErr = ErrInvocationInvalid
			}
			parent.mutate = func(data *PreparedAPIDataV1) {
				if err := os.Mkdir(stageDir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(stageFile, []byte("public owned staging"), 0600); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "expired-original", "expired-cleanup-failed":
					// Select this exact process bound ONCE inside the original
					// preparation, then actually exhaust it before returning owned
					// data. Caller and original retirement remain independently live.
					expiry, stop := context.WithDeadline(ctx, time.Now().Add(time.Millisecond))
					defer stop()
					data.ProcessDeadline, _ = expiry.Deadline()
					<-expiry.Done()
					if !errors.Is(expiry.Err(), context.DeadlineExceeded) || time.Now().Before(data.ProcessDeadline) {
						t.Fatal("original process bound did not actually expire")
					}
				case "nil-retirement":
					data.Retirement = nil
				case "typed-nil-retirement":
					var missing *apiTestRetirement
					data.Retirement = missing
				case "nil-context":
					data.Retirement = malformedContext
				case "typed-nil-context":
					var missing *piNilRetirementContext
					malformedContext.ctx = missing
					data.Retirement = malformedContext
				case "caller-alias":
					data.ParentInput = inv.VerifiedInput
				}
			}
			parent.retireStage = func(retirement context.Context, id InvocationIdentity, data *PreparedAPIDataV1) error {
				retireCalls++
				cleanupContext = retirement
				events = append(events, "retire")
				if id != inv.Identity || data != parent.data {
					return ErrInvocationInvalid
				}
				if mode == "nil-retirement" || mode == "typed-nil-retirement" || mode == "nil-context" || mode == "typed-nil-context" {
					if retirement != nil {
						t.Error("missing original retirement replaced with new context")
					}
					return ErrInvocationInvalid
				}
				if retirement != originalRetirement || retirement.Err() != nil {
					return ErrInvocationInvalid
				}
				if mode == "expired-cleanup-failed" {
					return ErrInvocationInvalid
				}
				if err := os.Remove(stageFile); err != nil {
					return err
				}
				return os.Remove(stageDir)
			}
			parent.afterClear = func() { clearCalls++; events = append(events, "clear") }
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				executeCalls++
				return AttemptResult{}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, inv)
			if runErr == nil || executeCalls != 0 || life.handle.HandleID != "" || life.releaseCount != 0 || result.CredentialReleaseRequired {
				t.Fatalf("%s crossed Issue/executor: result=%+v err=%v calls=%d order=%v", mode, result, runErr, executeCalls, parent.order)
			}
			if retireCalls != 1 {
				t.Fatalf("%s actual staging retirement calls=%d, want1 before buffer Clear; events=%v", mode, retireCalls, events)
			}
			wantClear := 1
			if mode == "caller-alias" {
				wantClear = 0
			}
			if clearCalls != wantClear || parent.cleared != (wantClear == 1) {
				t.Fatalf("%s Clear=%d want%d events=%v", mode, clearCalls, wantClear, events)
			}
			wantEvents := []string{"retire"}
			if wantClear == 1 {
				wantEvents = append(wantEvents, "clear")
			}
			if !reflect.DeepEqual(events, wantEvents) {
				t.Fatalf("%s cleanup order=%v want%v", mode, events, wantEvents)
			}
			if sha256.Sum256(inv.VerifiedInput.Context) != callerHash {
				t.Fatal("caller-aliased buffers were cleared")
			}
			failed := mode == "expired-cleanup-failed" || mode == "nil-retirement" || mode == "typed-nil-retirement" || mode == "nil-context" || mode == "typed-nil-context"
			_, statErr := os.Stat(stageFile)
			if failed && statErr != nil || !failed && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("%s real staging residual=%v cleanupFailed=%t", mode, statErr, failed)
			}
			if (runner.Close(ctx) != nil) != failed {
				t.Fatalf("%s Close did not preserve actual cleanup outcome", mode)
			}
			if mode == "nil-retirement" || mode == "typed-nil-retirement" {
				if cleanupContext != nil || parent.retirement.calls != 0 {
					t.Fatal("typed-nil original retirement invoked/replaced")
				}
			} else if mode == "nil-context" || mode == "typed-nil-context" {
				if cleanupContext != nil || malformedContext.calls != 1 || parent.retirement.calls != 0 {
					t.Fatal("malformed original context was invoked or renewed")
				}
			} else if cleanupContext != originalRetirement || parent.retirement.calls != 1 {
				t.Fatal("original once-selected retirement identity/count changed")
			}
		})
	}
}

func TestPiResultCustodyNilPreparationHasNoRetirement(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	parent.prepareErr = ErrInvocationInvalid
	retired := 0
	parent.retireStage = func(context.Context, InvocationIdentity, *PreparedAPIDataV1) error {
		retired++
		return ErrInvocationInvalid
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
		t.Fatal("nil preparation executed")
		return AttemptResult{}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, inv); err == nil || retired != 0 || parent.cleared || life.releaseCount != 0 {
		t.Fatal("nil/no-resource preparation fabricated retirement or execution")
	}
	if err := runner.Close(ctx); err != nil {
		t.Fatal("nil/no-resource preparation poisoned clean closure:", err)
	}
}

func TestPiCustodyHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	limits := pisdkphasewire.FrameLimits{InitializeBodyBytes: (1 << 20) - 17, RequestBodyBytes: (1 << 20) - 146, FinalBodyBytes: 128 << 10, FrameBytes: 1 << 20}
	read := func() pisdkphasewire.Frame {
		var h [17]byte
		if _, err := io.ReadFull(os.Stdin, h[:]); err != nil {
			os.Exit(71)
		}
		head, err := pisdkphasewire.InspectHeader(h[:], limits)
		if err != nil {
			os.Exit(72)
		}
		b := make([]byte, int(head.TotalBytes))
		copy(b, h[:])
		if _, err = io.ReadFull(os.Stdin, b[17:]); err != nil {
			os.Exit(73)
		}
		f, err := pisdkphasewire.DecodeFrame(b, limits)
		if err != nil {
			os.Exit(74)
		}
		return f
	}
	write := func(f pisdkphasewire.Frame) {
		b, err := pisdkphasewire.EncodeFrame(f, limits)
		if err != nil {
			os.Exit(75)
		}
		if _, err = os.Stdout.Write(b); err != nil {
			os.Exit(76)
		}
	}
	init := read()
	if init.Kind != pisdkphasewire.Initialize || len(init.Body) != 64 {
		os.Exit(77)
	}
	write(pisdkphasewire.Frame{Kind: pisdkphasewire.PhysicalRequest, BackendProfileDigest: string(init.Body), Body: []byte(piSyntheticRequest)})
	use := read()
	if use.Kind != pisdkphasewire.UseResult || !use.Use.Allowed {
		os.Exit(78)
	}
	final := piSyntheticFinal
	if os.Getenv(helperMode) == "artifacts" {
		final = custodyArtifactFinal
	}
	write(pisdkphasewire.Frame{Kind: pisdkphasewire.StructuredFinal, Body: []byte(final)})
	os.Exit(0)
}
