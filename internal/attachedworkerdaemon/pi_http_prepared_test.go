//go:build darwin || linux

package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

type piHTTPFailNthRetirement struct {
	PreparedAPILedgerV1
	attempts, failAt int
	err              error
}

func (l *piHTTPFailNthRetirement) EndLive(n uint64) error {
	l.attempts++
	if l.failAt != 0 && l.attempts == l.failAt {
		return l.err
	}
	return l.PreparedAPILedgerV1.EndLive(n)
}

func TestPreparedPiHTTPAccountingFailureCannotRefundLaterOwner(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failAt int
	}{{"coordinator", 1}, {"bridge", 2}, {"wrapper", 3}} {
		t.Run(tc.name, func(t *testing.T) {
			s, inv, parent, _ := piHTTPOriginalPlanFixture(t, "ok")
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			data, err := parent.PrepareOriginalAPIInvocation(ctx, inv.Identity, inv.VerifiedInput, inv.Process, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := errors.New("test-owned retirement accounting failure")
			ledger := &piHTTPFailNthRetirement{PreparedAPILedgerV1: data.Ledger, err: want}
			plan, err := s.PreparePiHTTPStdioV1(inv.Identity, data.ParentRequest, ledger, [32]byte{1}, [32]byte{2}, func(context.Context, *PiHTTPIOLeaseV1) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			ledger.attempts, ledger.failAt = 0, tc.failAt
			if err := plan.Close(); !errors.Is(err, want) || errors.Is(err, ErrPiHTTPRetirementPending) {
				t.Fatal("accounting failure lost or became retryable", err)
			}
			if err := ledger.Reserve(0, 193, 0); err != nil {
				t.Fatal(err)
			}
			before, calls := parent.ledger.live, ledger.attempts
			copyPlan := *plan
			if err := copyPlan.Close(); !errors.Is(err, want) || ledger.attempts != calls || parent.ledger.live != before {
				t.Fatal("accounting retry debited later owner", err, ledger.attempts, calls, parent.ledger.live, before)
			}
		})
	}
}

// Original InvocationRunner and real Supervisor pipes, with synthetic parent,
// credential lifecycle, Go child and trusted driver. Never Pi SDK, real provider,
// production isolation, genuine file quarantine or #200 producer qualification.
func piHTTPOriginalPlanFixture(t *testing.T, mode string) (*Supervisor, Invocation, *apiTestParent, *apiTestLifecycle) {
	t.Helper()
	s, _, executable, digest := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: 5 * time.Second})
	s.timeout = time.Hour
	inv, parent, life := longLivedAPIOwnerFixture(t)
	inv.Process.Executable, inv.Process.ExecutableDigest = executable, digest
	inv.Process.Arguments = []string{"-test.run=^TestPiHTTPPreparedHelperProcess$"}
	inv.Process.Environment = []EnvironmentVariable{{Name: helperEnabled, Value: "1"}, {Name: helperMode, Value: mode}}
	parent.profile = apiOwnerProfile(t, digest)
	descriptor, err := parent.profile.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.Job.HarnessBinding.Backend = descriptor
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.AcceptedAttempt.ContextDigest = contextDigest
	return s, inv, parent, life
}

func TestPreparedPiHTTPLateBindSameOriginalRunnerAndPipes(t *testing.T) {
	for _, mode := range []string{"ok", "mutated request after use", "larger model after use", "wrong request before use", "repeat request", "copied request", "close during driver"} {
		t.Run(mode, func(t *testing.T) {
			s, inv, parent, life := piHTTPOriginalPlanFixture(t, mode)
			var plan *PreparedPiHTTPStdioV1
			var borrow *PreparedAPISendBorrowV1
			var saved PiHTTPIOLeaseV1
			var preparedWork, preparedNodes uint64
			requestChecks := 0
			parent.mutate = func(d *PreparedAPIDataV1) {
				var err error
				plan, err = s.PreparePiHTTPStdioV1(inv.Identity, d.ChildRequest, d.Ledger, [32]byte{1}, [32]byte{2},
					func(ctx context.Context, lease *PiHTTPIOLeaseV1) error {
						saved = *lease
						request, _, err := lease.Request()
						if err != nil {
							return err
						}
						if mode == "repeat request" || mode == "copied request" {
							second := lease
							if mode == "copied request" {
								second = &saved
							}
							_, _, err := second.Request()
							if err == nil {
								t.Error("second exported Request was admitted")
								return ErrSupervisorConfig
							}
							return err
						}
						if mode == "close during driver" && plan.Close() == nil {
							return errors.New("active driver lost ownership")
						}
						if _, err := borrow.Authorize(); err != nil {
							return err
						}
						for _, f := range piHTTPTestFrames(plan.bridge.core.limits, []byte("0123456789"))[2:] {
							if f.Kind == pisdkhttpwire.Pull || f.Kind == pisdkhttpwire.StructuredFinal {
								continue
							}
							if err := lease.Write(f); err != nil {
								return err
							}
						}
						if mode == "mutated request after use" {
							request.Body[0] ^= 1 // causal mutant after authorization
						}
						if mode == "larger model after use" {
							parent.data.ChildRequest.Model = bytes.Repeat([]byte{'x'}, len(parent.data.ChildRequest.Model)+1024)
						}
						return nil
					})
				if err != nil {
					t.Fatalf("prepare before Issue: %v", err)
				}
				plan.bridge.afterRequest = func() { requestChecks++ }
				t.Cleanup(func() {
					if err := plan.Close(); err != nil {
						t.Errorf("plan retirement: %v", err)
					}
				})
				if plan.bridge.send != nil || plan.bridge.ctx != nil || plan.bridge.initWire != nil || plan.bridge.core.write.epoch != 0 {
					t.Fatal("pre-Issue plan fabricated a send/context/materialized Init")
				}
				preparedWork, preparedNodes = parent.ledger.copy, parent.ledger.nodes
			}
			var actual AttemptResult
			var runErr error
			entered := false
			var replay PreparedInvocationV1
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, in PreparedInvocationV1) (AttemptResult, error) {
				entered = true
				if !life.active || plan == nil || !plan.preparedAt.Before(in.apiIssueStarted) || parent.ledger.copy < preparedWork || parent.ledger.nodes < preparedNodes {
					t.Fatal("plan did not precede original Issue/Materialize")
				}
				var err error
				borrow, err = BindPreparedAPISendV1(in.PhysicalSend, ctx, in.Identity, in.Ledger, in.Credential, in.Materialization, in.APIRequest.Body, plan.profileID)
				if err != nil {
					return AttemptResult{}, err
				}
				in.Process.Stdin = in.APIRequest.Body                                   // synthetic complete Init, no real key/path
				s.allowedReadRoots = []string{filepath.Dir(in.Materialization.RootDir)} // test-owned parent; no host roots
				replay = in
				beforeWork, beforeNodes, beforeLive := parent.ledger.copy, parent.ledger.nodes, parent.ledger.live
				actual, runErr = s.RunPreparedPiHTTP(ctx, in, plan)
				if parent.ledger.copy != beforeWork || parent.ledger.nodes != beforeNodes || parent.ledger.live != beforeLive {
					t.Error("late bind/IO replenished the original ledger")
				}
				return actual, runErr
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			_, outerErr := runner.Run(ctx, inv)
			if !entered || plan == nil {
				t.Fatalf("original callback not reached: order=%v outer=%v", parent.order, outerErr)
			}
			wantSuccess := mode == "ok" || mode == "close during driver"
			if (runErr == nil) != wantSuccess || actual.piStdio != nil || len(actual.Stdout) != 0 {
				t.Fatalf("mode=%s process=%+v err=%v outer=%v", mode, actual, runErr, outerErr)
			}
			if wantSuccess && (!actual.CleanupSucceeded || !actual.DescendantsReaped || !actual.BoundaryReleased || !plan.bridge.complete) {
				t.Fatalf("actual process not fully joined: %+v", actual)
			}
			if _, err := s.RunPreparedPiHTTP(ctx, replay, plan); err == nil {
				t.Error("replayed plan started a second process")
			}
			if mode == "wrong request before use" && strings.Contains(strings.Join(parent.order, ","), "physical_send") {
				t.Fatal("wrong actual child request obtained Source authorization")
			}
			if requestChecks != 1 {
				t.Fatalf("exported request validation walks=%d want exactly one", requestChecks)
			}
			if _, _, err := saved.Request(); err == nil || saved.Write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseEnd}) == nil {
				t.Fatal("retained lease copy remained active after driver return")
			}
			wantFinalize := 0
			if wantSuccess {
				wantFinalize = 1
			}
			if life.callbackCount != 1 || life.finalizeCount != wantFinalize || life.releaseCount != 1 {
				t.Fatalf("duplicated/missing original lifecycle: callbacks=%d finalize=%d release=%d", life.callbackCount, life.finalizeCount, life.releaseCount)
			}
		})
	}
}

func TestPreparedPiHTTPRejectsForeignLateBindingBeforeProcess(t *testing.T) {
	for _, mode := range []string{"context", "identity", "ledger", "handle", "plan", "body", "model", "larger model within P", "schema", "headers", "profile", "late preparation", "unbound", "structural sender"} {
		t.Run(mode, func(t *testing.T) {
			s, inv, parent, life := piHTTPOriginalPlanFixture(t, "ok")
			var plan *PreparedPiHTTPStdioV1
			called := false
			entered := false
			parent.mutate = func(d *PreparedAPIDataV1) {
				var err error
				plan, err = s.PreparePiHTTPStdioV1(inv.Identity, d.ChildRequest, d.Ledger, [32]byte{1}, [32]byte{2}, func(context.Context, *PiHTTPIOLeaseV1) error { called = true; return nil })
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := plan.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				})
			}
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, in PreparedInvocationV1) (AttemptResult, error) {
				entered = true
				if mode != "unbound" {
					if _, err := BindPreparedAPISendV1(in.PhysicalSend, ctx, in.Identity, in.Ledger, in.Credential, in.Materialization, in.APIRequest.Body, plan.profileID); err != nil {
						t.Fatal(err)
					}
				}
				in.Process.Stdin = in.APIRequest.Body
				switch mode {
				case "context":
					ctx = context.WithValue(ctx, struct{}{}, true)
				case "identity":
					in.Identity.FenceToken++
				case "ledger":
					in.Ledger = &apiTestLedger{}
				case "handle":
					in.Credential.HandleID = "foreign"
				case "plan":
					in.Materialization.FilePath += "foreign"
				case "body":
					in.APIRequest.Body = bytes.Repeat([]byte{'x'}, len(in.APIRequest.Body))
				case "model":
					in.APIRequest.Model = []byte("{}")
				case "larger model within P":
					in.APIRequest.Model = bytes.Repeat([]byte{'x'}, len(in.APIRequest.Model)+1024)
				case "schema":
					in.APIRequest.Schema = []byte("{}")
				case "headers":
					in.APIRequest.Headers = []byte("{}")
				case "profile":
					in.APIRequest.Profile.Model.ModelID = "foreign"
				case "late preparation":
					plan.preparedAt = time.Now()
				case "structural sender":
					in.PhysicalSend = &structuralAPISender{}
				}
				result, err := s.RunPreparedPiHTTP(ctx, in, plan)
				if err == nil || result.StdoutBytes != 0 || called {
					t.Fatalf("foreign %s reached process/driver: %+v %v", mode, result, err)
				}
				return result, err
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			if _, err := runner.Run(ctx, inv); err == nil {
				t.Fatal("foreign binding succeeded")
			}
			if !entered || plan == nil {
				t.Fatalf("negative never reached binding: order=%v", parent.order)
			}
		})
	}
}

// A Go subprocess speaks only structural SPU3. It receives synthetic request
// bytes as Init; it does not read the materialized credential or use a network.
func TestPiHTTPPreparedHelperProcess(t *testing.T) {
	if os.Getenv(helperEnabled) != "1" {
		return
	}
	l := pisdkhttpwire.Limits{FrameBytes: 1 << 20, InitializeBytes: (1 << 20) - 113, RequestBytes: (1 << 20) - 85 - 4096,
		FinalBytes: 96 << 10, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBytes: 1 << 20,
		ResponseChunkBytes: 32 << 10, ParentBytes: 4 << 20, ChildBytes: 8 << 20, Frames: 2048}
	read := func() pisdkhttpwire.Frame {
		var header [17]byte
		if _, err := io.ReadFull(os.Stdin, header[:]); err != nil {
			os.Exit(41)
		}
		h, err := pisdkhttpwire.InspectHeader(header[:], pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(42)
		}
		raw := make([]byte, int(h.TotalBytes))
		copy(raw, header[:])
		if _, err := io.ReadFull(os.Stdin, raw[17:]); err != nil {
			os.Exit(43)
		}
		f, err := pisdkhttpwire.DecodeView(raw, pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(44)
		}
		return f
	}
	write := func(f pisdkhttpwire.Frame) {
		raw := piHTTPTestEncode(t, f, l)
		if _, err := os.Stdout.Write(raw); err != nil {
			os.Exit(45)
		}
	}
	init := read()
	if init.Kind != pisdkhttpwire.Initialize {
		os.Exit(46)
	}
	f := piHTTPTestFrames(l, []byte("0123456789"))[1]
	f.Profile, f.Body = init.Profile, init.Body
	if os.Getenv(helperMode) == "wrong request before use" {
		f.Body[0] ^= 1
	}
	write(f)
	start := read()
	if start.Kind != pisdkhttpwire.ResponseStart {
		os.Exit(47)
	}
	for i := uint32(0); i < start.Count; i++ {
		write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.Pull, Index: i})
		if read().Kind != pisdkhttpwire.ResponsePart {
			os.Exit(48)
		}
	}
	if read().Kind != pisdkhttpwire.ResponseEnd {
		os.Exit(49)
	}
	write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.StructuredFinal, Body: []byte(`{"answer":"synthetic","artifacts":[]}`)})
	os.Exit(0)
}

func TestPreparedPiHTTPZeroLeaseAndProfileLimits(t *testing.T) {
	for _, lease := range []*PiHTTPIOLeaseV1{nil, {}} {
		lease.Abort()
		if _, _, err := lease.Request(); err == nil || lease.Write(pisdkhttpwire.Frame{}) == nil {
			t.Fatal("zero lease became IO authority")
		}
	}
	s, inv, parent, _ := piHTTPOriginalPlanFixture(t, "ok")
	request := PreparedAPIRequestV1{Profile: parent.profile, Body: []byte("{}"), Model: []byte("{}"), Schema: []byte("{}"), Headers: []byte("{}")}
	ledger := &apiTestLedger{}
	p, err := s.PreparePiHTTPStdioV1(inv.Identity, request, ledger, [32]byte{1}, [32]byte{2}, func(context.Context, *PiHTTPIOLeaseV1) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	copyPlan := *p
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	profile, err := request.Profile.Digest()
	if err != nil || hex.EncodeToString(p.profileSHA[:]) != profile {
		t.Fatal("profile commitment changed", err)
	}
	work, nodes := ledger.copy, ledger.nodes
	if err := p.Close(); err != nil || ledger.live != 0 || ledger.copy != work || ledger.nodes != nodes {
		t.Fatal("unused plan leaked/refunded cumulative ledger", err)
	}
	// Preserve another owner's debit so a duplicate refund could look valid.
	if err := ledger.Reserve(0, 1024, 0); err != nil {
		t.Fatal(err)
	}
	if err := copyPlan.Close(); err != nil || ledger.live != 1024 {
		t.Fatal("copied plan refunded another owner", err, ledger.live)
	}
	if err := ledger.EndLive(1024); err != nil {
		t.Fatal(err)
	}
	if err := (&PreparedPiHTTPStdioV1{}).Close(); err != nil {
		t.Fatal("zero cleanup", err)
	}
	request.Profile.Limits.ResponseBodyBytes = 4 << 20 // exact P that cannot fit stdin with Init/header/framing
	before := ledger.copy
	if plan, err := s.PreparePiHTTPStdioV1(inv.Identity, request, ledger, [32]byte{1}, [32]byte{2}, func(context.Context, *PiHTTPIOLeaseV1) error { return nil }); err == nil || plan != nil || ledger.copy <= before || ledger.live != 0 {
		t.Fatal("infeasible P was lowered or validation failed to retire", err)
	}
}

func TestPreparedPiHTTPValidationReservationPrecedesRejectingValidators(t *testing.T) {
	for _, mode := range []string{"malformed endpoint", "invalid identity"} {
		t.Run(mode, func(t *testing.T) {
			s, inv, parent, _ := piHTTPOriginalPlanFixture(t, "ok")
			request := PreparedAPIRequestV1{Profile: parent.profile, Body: []byte("{}"), Model: []byte("{}"), Schema: []byte("{}"), Headers: []byte("{}")}
			if mode == "malformed endpoint" {
				request.Profile.Model.Endpoint = "https://[invalid"
			} else {
				inv.Identity.TenantID = "invalid identity"
			}
			// The first reservation denies all work/live/nodes. Even rejecting
			// domain validators must not run before this prospective admission.
			ledger := &piHTTPPlanBudget{exact: true}
			p, err := s.PreparePiHTTPStdioV1(inv.Identity, request, ledger, [32]byte{1}, [32]byte{2}, func(context.Context, *PiHTTPIOLeaseV1) error { return nil })
			if err == nil || p != nil || ledger.calls != 1 || ledger.live != 0 || ledger.copy != 0 {
				t.Fatal("validator ran before denied reservation", err, ledger.calls)
			}
		})
	}
}

func TestPreparedPiHTTPAdmittedRequestCheckJoinsBeforeRetirement(t *testing.T) {
	s, b, spec, ledger, cancel := piHTTPProcessFixture(t, "early_return")
	frames := piHTTPTestFrames(b.core.limits, []byte("0123456789"))
	state := &preparedPiHTTPState{profileSHA: frames[1].Profile, requestSHA: [4][32]byte{sha256.Sum256(frames[1].Body)}, requestLen: [4]int{len(frames[1].Body)}}
	plan := &PreparedPiHTTPStdioV1{state}
	lease := PiHTTPIOLeaseV1{io: &b.lease, plan: plan}
	admitted, release, driverReturned, requestDone, runDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	b.afterRequest = func() { close(admitted); <-release }
	b.driver = func(context.Context, *piHTTPIO) error {
		go func() { defer close(requestDone); _, _, _ = lease.Request() }()
		<-admitted
		close(driverReturned)
		return nil // deliberately leaves one admitted wrapper check outstanding
	}
	var runErr error
	go func() { defer close(runDone); _, runErr = s.Run(b.ctx, spec) }()
	t.Cleanup(func() { cancel(); unblock(); apiWait(t, runDone); apiWait(t, requestDone) })
	apiWait(t, driverReturned)
	select {
	case <-b.done:
		t.Fatal("driver return published completion beneath admitted request check")
	default:
	}
	before := ledger.live
	if err := b.clear(); err == nil || ledger.live != before || len(b.core.request) == 0 {
		t.Fatal("retirement cleared/refunded under admitted check", err)
	}
	unblock()
	apiWait(t, requestDone)
	apiWait(t, runDone)
	if runErr == nil {
		t.Fatal("incomplete driver became success")
	}
	if err := b.clear(); err != nil || ledger.live != 0 {
		t.Fatal("post-join retirement", err, ledger.live)
	}
}

type piHTTPPlanBudget struct {
	apiTestLedger
	calls                 int
	grants                [3][3]uint64
	exact                 bool
	shortCall, shortField int
}

func (l *piHTTPPlanBudget) Reserve(c, v, n uint64) error {
	l.calls++
	got := [3]uint64{c, v, n}
	if l.calls > len(l.grants) {
		return ErrSupervisorConfig
	}
	if l.exact {
		want := l.grants[l.calls-1]
		if l.calls == l.shortCall {
			want[l.shortField]--
		}
		for i := range got {
			if got[i] > want[i] {
				return ErrSupervisorConfig
			}
		}
	} else {
		l.grants[l.calls-1] = got
	}
	return l.apiTestLedger.Reserve(c, v, n)
}

func TestPreparedPiHTTPAllReservationsBeforeIssueAndDenialRetirement(t *testing.T) {
	s, inv, parent, _ := piHTTPOriginalPlanFixture(t, "ok")
	request := PreparedAPIRequestV1{Profile: parent.profile, Body: []byte("{}"), Model: []byte("{}"), Schema: []byte("{}"), Headers: []byte("{}")}
	driver := func(context.Context, *PiHTTPIOLeaseV1) error { t.Error("unused plan called driver"); return nil }
	record := &piHTTPPlanBudget{}
	p, err := s.PreparePiHTTPStdioV1(inv.Identity, request, record, [32]byte{1}, [32]byte{2}, driver)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil || record.live != 0 || record.calls != 3 {
		t.Fatal("record pre-Issue schedule", err, record.calls, record.live)
	}
	for call := 0; call <= 3; call++ {
		for field, name := range []string{"work", "live", "nodes"} {
			if call == 0 && field != 0 {
				continue
			}
			t.Run(string(rune('0'+call))+"/"+name, func(t *testing.T) {
				ledger := &piHTTPPlanBudget{exact: true, grants: record.grants, shortCall: call, shortField: field}
				p, err := s.PreparePiHTTPStdioV1(inv.Identity, request, ledger, [32]byte{1}, [32]byte{2}, driver)
				if call == 0 {
					if err != nil || p == nil {
						t.Fatal("exact schedule denied", err)
					}
					if err := p.Close(); err != nil {
						t.Fatal(err)
					}
				} else if err == nil || p != nil || ledger.calls != call {
					t.Fatalf("short %s stage=%d admitted or advanced: plan=%v err=%v calls=%d", name, call, p, err, ledger.calls)
				}
				if ledger.live != 0 {
					t.Fatalf("denial left live debit=%d", ledger.live)
				}
				if call > 1 && (ledger.copy == 0 || ledger.nodes == 0) {
					t.Fatal("denial refunded cumulative work/nodes")
				}
			})
		}
	}
}

func TestPreparedPiHTTPCancellationAndRetainedAbortJoin(t *testing.T) {
	s, inv, parent, life := piHTTPOriginalPlanFixture(t, "ok")
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	entered, done := make(chan struct{}), make(chan struct{})
	var plan *PreparedPiHTTPStdioV1
	var saved PiHTTPIOLeaseV1
	parent.mutate = func(d *PreparedAPIDataV1) {
		var err error
		plan, err = s.PreparePiHTTPStdioV1(inv.Identity, d.ChildRequest, d.Ledger, [32]byte{1}, [32]byte{2}, func(ctx context.Context, lease *PiHTTPIOLeaseV1) error {
			if _, _, err := lease.Request(); err != nil {
				return err
			}
			saved = *lease
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := plan.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, in PreparedInvocationV1) (AttemptResult, error) {
		if _, err := BindPreparedAPISendV1(in.PhysicalSend, ctx, in.Identity, in.Ledger, in.Credential, in.Materialization, in.APIRequest.Body, plan.profileID); err != nil {
			return AttemptResult{}, err
		}
		in.Process.Stdin = in.APIRequest.Body
		s.allowedReadRoots = []string{filepath.Dir(in.Materialization.RootDir)}
		return s.RunPreparedPiHTTP(ctx, in, plan)
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	var runErr error
	go func() { defer close(done); _, runErr = runner.Run(ctx, inv) }()
	t.Cleanup(func() { cancel(); apiWait(t, done) })
	apiWait(t, entered)
	if plan.Close() == nil {
		t.Fatal("closed plan under active borrower")
	}
	cancel()
	apiWait(t, done)
	if runErr == nil || !plan.bridge.failure.Load() || life.releaseCount != 1 || life.active {
		t.Fatal("cancel did not join original callback/release", runErr)
	}
	// Retained copies may attempt cancellation concurrently with retirement.
	// They cannot access the cleared send or revive the closed lease.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			saved.Abort()
		}
	}()
	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if _, _, err := saved.Request(); err == nil {
		t.Fatal("retired lease revived request")
	}
}
