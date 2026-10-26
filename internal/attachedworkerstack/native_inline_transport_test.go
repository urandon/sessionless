//go:build darwin || linux

package attachedworkerstack_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	transport "gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

const nativeInlineHTTPBearer = "fixed-synthetic-inline-http-bearer"
const nativeInlineOriginalBody = " { \"n\": 9007199254740993123456789 } original text\n"

func nativeInlineCheckpointAt(path string) (attachedworkeroutput.InlineReceiptCheckpointV2, error) {
	file, err := os.Open(path)
	if err != nil {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, attachedworkeroutput.MaxInlineCheckpointBytesV2+1))
	closeErr := file.Close()
	defer clear(raw)
	if readErr != nil || closeErr != nil || len(raw) > attachedworkeroutput.MaxInlineCheckpointBytesV2 {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, errors.New("actual journal read failed or exceeded compact bound")
	}
	var outer attachedworkerlocal.ReceiptCheckpointV1
	if json.Unmarshal(raw, &outer) != nil {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, errors.New("actual journal outer envelope malformed")
	}
	defer clear(outer.Payload)
	digest := sha256.Sum256(outer.Payload)
	if outer.PayloadSHA256 != hex.EncodeToString(digest[:]) || bytes.Contains(outer.Payload, []byte(`"summary"`)) || bytes.Contains(outer.Payload, []byte(`"capability"`)) || bytes.Contains(outer.Payload, []byte(nativeInlineHTTPBearer)) {
		return attachedworkeroutput.InlineReceiptCheckpointV2{}, errors.New("actual journal changed payload digest or retained forbidden content")
	}
	return attachedworkeroutput.DecodeInlineCheckpointV2(outer.Payload)
}

// This is a scoped in-memory HTTP server peer, not a receipt store or authority.
// The actual InlineClientV2 does all framing/response validation. No TCP, engine,
// native harness, YDB, provider, model or real credential is involved.
type nativeInlineHTTPPeer struct {
	mu                      sync.Mutex
	fixture                 *attachedworkerstack.NativeInlineFixtureV2ForTest
	fault                   attachedworkerstack.NativeInlineFaultV2ForTest
	prepare                 attachedworkeroutput.AttachedWorkerInlinePrepareV2
	fingerprint             string
	paths                   []string
	failures                []string
	observed                []byte
	prepareCalls, bodyCalls int
	reconcileCalls          int
}

func (p *nativeInlineHTTPPeer) fail(reason string) (*http.Response, error) {
	p.failures = append(p.failures, reason)
	return nil, fmt.Errorf("synthetic HTTP peer: %s", reason)
}
func (p *nativeInlineHTTPPeer) RoundTrip(request *http.Request) (*http.Response, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paths = append(p.paths, request.URL.Path)
	if request.Context().Err() != nil || request.URL.Scheme != "https" || request.URL.Host != "native-inline.invalid" || request.URL.RawQuery != "" || request.Method != http.MethodPost || request.GetBody != nil || request.Header.Get("Authorization") != "Bearer "+nativeInlineHTTPBearer || request.ContentLength <= 0 || len(request.TransferEncoding) != 0 {
		return p.fail("fixed-origin one-exchange request framing changed")
	}
	// Observe the actual durable burn before consuming even the first byte of
	// this physical request, not merely after its EOF has become visible.
	checkpoint, err := nativeInlineCheckpointAt(p.fixture.JournalPath)
	if err != nil {
		return p.fail("network dispatch preceded actual durable checkpoint: " + err.Error())
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, (256<<10)+1))
	closeErr := request.Body.Close()
	defer clear(raw)
	if err != nil || closeErr != nil || int64(len(raw)) != request.ContentLength {
		return p.fail("original request bytes/declared length mismatched")
	}
	var result attachedworkeroutput.AttachedWorkerInlineResultV2
	switch request.URL.Path {
	case attachedworkerreceipt.PathInlinePrepareV2:
		p.prepareCalls++
		if p.prepareCalls != 1 || request.Header.Get("Content-Type") != "application/json" || json.Unmarshal(raw, &p.prepare) != nil {
			return p.fail("prepare repeated or malformed")
		}
		p.fingerprint, err = attachedworkeroutput.InlineOutputRequestFingerprintV2(p.prepare)
		events, launches, _ := p.fixture.Events()
		wantEvents := []string{"issue", "materialize", "boundary-release", "writeback-entered", "writeback", "release"}
		harness, harnessErr := p.fixture.Original.VerifiedInput.Job.HarnessBinding.Digest()
		if err != nil || harnessErr != nil || !reflect.DeepEqual(events, wantEvents) || launches != 1 || p.prepare.Producer.HarnessBindingDigest != harness || p.prepare.Producer.SessionID != p.fixture.Original.VerifiedInput.Job.SessionID || !p.prepare.Producer.AcceptedAt.Equal(p.fixture.Original.VerifiedInput.AcceptedAttempt.CreatedAt) || p.prepare.Candidate.Summary != "two requested files" || len(p.prepare.Candidate.Artifacts) != 2 {
			return p.fail("prepare did not follow original producer and full finalization")
		}
		if checkpoint.Progress.Revision != 1 || !checkpoint.Progress.PrepareBurned || checkpoint.Progress.BodyBurned || checkpoint.Plan.Fingerprint != p.fingerprint || checkpoint.Plan.Producer != p.prepare.Producer || !reflect.DeepEqual(checkpoint.Plan.Artifacts, p.prepare.Candidate.Artifacts) {
			return p.fail("prepare initial journal identity/burn missing")
		}
		result = attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Stage: attachedworkeroutput.InlinePreparedV2, Nonce: p.prepare.Nonce, Fingerprint: p.fingerprint, Capability: strings.Repeat("c", 64), ExpiresAt: p.prepare.ExpiresAt}
	case attachedworkerreceipt.PathInlineBodyV2:
		p.bodyCalls++
		metadata, decodeErr := base64.RawStdEncoding.Strict().DecodeString(request.Header.Get(attachedworkerreceipt.InlineRequestHeaderV2))
		defer clear(metadata)
		var bodyRequest attachedworkeroutput.AttachedWorkerInlineBodyV2
		if p.bodyCalls != 1 || p.prepareCalls != 1 || request.Header.Get("Content-Type") != attachedworkerreceipt.InlineBodyMediaTypeV2 || decodeErr != nil || json.Unmarshal(metadata, &bodyRequest) != nil || bodyRequest.Authorization != p.prepare.Authorization || bodyRequest.Nonce != p.prepare.Nonce || bodyRequest.Fingerprint != p.fingerprint || bodyRequest.Capability != strings.Repeat("c", 64) || !bodyRequest.ExpiresAt.Equal(p.prepare.ExpiresAt) {
			return p.fail("body send repeated or immutable metadata changed")
		}
		if checkpoint.Progress.Revision != 2 || !checkpoint.Progress.BodyBurned || checkpoint.Progress.ReconcileBurned || checkpoint.Plan.Fingerprint != p.fingerprint || string(raw) != nativeInlineOriginalBody {
			return p.fail("body preceded durable body burn or lost original ordered bytes")
		}
		verified, verifyErr := attachedworkeroutput.VerifyInlineV2(p.prepare.Candidate, raw)
		if verifyErr != nil {
			return p.fail("original artifact hashes/media/order failed verification")
		}
		verified.Clear()
		p.observed = bytes.Clone(raw)
		if p.fault == attachedworkerstack.NativeInlineExportExpiryV2ForTest {
			// Actual immutable request expiry, not a sleep or a virtual clock
			// jump. The complete original body has already reached EOF once.
			<-request.Context().Done()
			return nil, request.Context().Err()
		}
		// The body was fully received, but this peer loses READY. Only one
		// subsequent metadata reconciliation is permitted; never resend body.
		return nil, errors.New("controlled lost READY after original body EOF")
	case attachedworkerreceipt.PathInlineReconcileV2:
		p.reconcileCalls++
		var reconcile attachedworkeroutput.AttachedWorkerInlineReconcileV2
		if p.reconcileCalls != 1 || p.bodyCalls != 1 || json.Unmarshal(raw, &reconcile) != nil || reconcile.Authorization != p.prepare.Authorization || reconcile.Nonce != p.prepare.Nonce || reconcile.Fingerprint != p.fingerprint || !reconcile.BodySendBurned || checkpoint.Progress.Revision != 3 || !checkpoint.Progress.BodyBurned || !checkpoint.Progress.ReconcileBurned || checkpoint.Progress.CanonicalDigest != "" {
			return p.fail("reconcile did not follow exact one-shot journal progress")
		}
		result = attachedworkeroutput.AttachedWorkerInlineResultV2{Version: 2, Stage: attachedworkeroutput.InlineReadyV2, Nonce: p.prepare.Nonce, Fingerprint: p.fingerprint, ExpiresAt: p.prepare.ExpiresAt, CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("d", 64))}
	default:
		return p.fail("unexpected path or V1 fallback")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > 32<<10 {
		return p.fail("synthetic response exceeded closed compact shape")
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "Content-Length": {strconv.Itoa(len(encoded))}}, ContentLength: int64(len(encoded)), Body: io.NopCloser(bytes.NewReader(encoded))}, nil
}
func (p *nativeInlineHTTPPeer) snapshot() (int, int, int, []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.prepareCalls, p.bodyCalls, p.reconcileCalls, append([]string(nil), p.failures...)
}

// Genuine original native prepared owner -> actual prepared adapter/journal ->
// actual InlineClientV2 -> HTTP peer -> original Session ACK/retirement. This
// qualifies local orchestration only, never actual native/provider/YDB custody.
func TestNativeInlineTransportV2(t *testing.T) {
	testNativeInlineTransportV2(t, attachedworkerstack.NativeInlineHeldACKV2ForTest)
}

func TestNativeInlineTransportV2OwnRefresh(t *testing.T) {
	testNativeInlineTransportV2(t, attachedworkerstack.NativeInlineOwnCredentialRefreshV2ForTest)
}

func testNativeInlineTransportV2(t *testing.T, fault attachedworkerstack.NativeInlineFaultV2ForTest) {
	t.Helper()
	f := attachedworkerstack.NewNativeInlineFixtureV2ForTest(t, fault)
	peer := &nativeInlineHTTPPeer{fixture: f}
	client, err := attachedworkerreceipt.NewInlineClientV2("https://native-inline.invalid", &http.Client{Transport: peer}, []byte(nativeInlineHTTPBearer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		peer.mu.Lock()
		clear(peer.observed)
		peer.observed = nil
		peer.mu.Unlock()
	})
	v1 := &preparedReceiptPublisher{}
	adapter, err := transport.NewPreparedAdapter(f.Session, &preparedReceiptMaterializer{invocation: f.Original}, transport.Config{Profile: transport.LocalProfileV1{Name: "fixed-native-inline-fixture", CapabilityDigest: f.Session.Snapshot().CapabilityDigest, Executable: f.Original.Process.Executable, ExecutableDigest: f.Original.Process.ExecutableDigest, Arguments: f.Original.Process.Arguments},
		CapabilityManifest: &f.Capability, ReceiptPublisher: v1, InlineReceiptPublisher: client, MaterializationRoot: f.MaterializationRoot, MaxInputBytes: 1 << 20, ReportTimeout: 5 * time.Second, Now: f.CurrentTime}, f.Stack)
	if err != nil {
		t.Fatalf("join original Stack/Session: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	invocation, available, err := adapter.Next(ctx)
	if err != nil || !available {
		t.Fatalf("original offer/claim/materialization available=%v err=%v", available, err)
	}
	t.Cleanup(invocation.VerifiedInput.Clear)
	originalBinding := invocation.VerifiedInput.Job.HarnessBinding.Clone()
	type runOutcome struct {
		result attachedworkerdaemon.InvocationResult
		err    error
	}
	runDone, runFinished := make(chan runOutcome, 1), make(chan struct{})
	go func() {
		result, err := f.Stack.Run(ctx, invocation)
		runDone <- runOutcome{result, err}
		close(runFinished)
	}()
	t.Cleanup(func() {
		cancel()
		f.ContinueTeardown()
		f.ContinueWriteBack()
		f.ContinueACK()
		joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-runFinished:
		case <-joined.Done():
			t.Error("owned run did not join cleanup")
		}
	})
	for _, barrier := range []struct {
		ready   <-chan struct{}
		name    string
		proceed func()
	}{{f.TeardownEntered, "boundary teardown", f.ContinueTeardown}, {f.WriteBackEntered, "credential writeback", f.ContinueWriteBack}} {
		select {
		case <-barrier.ready:
		case early := <-runDone:
			t.Fatalf("original Run failed before %s: %+v err=%v", barrier.name, early.result, early.err)
		case <-ctx.Done():
			t.Fatalf("original Run did not reach %s: %v", barrier.name, ctx.Err())
		}
		select {
		case <-runFinished:
			t.Fatalf("Run returned before %s finished", barrier.name)
		default:
		}
		if prepares, bodies, reconciles, failures := peer.snapshot(); prepares+bodies+reconciles != 0 || len(failures) != 0 {
			t.Fatalf("export before %s finished: calls=%d/%d/%d failures=%v", barrier.name, prepares, bodies, reconciles, failures)
		}
		if _, err := os.Stat(f.JournalPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("journal existed before original finalization: %v", err)
		}
		barrier.proceed()
	}
	var completed runOutcome
	select {
	case completed = <-runDone:
	case <-ctx.Done():
		t.Fatal("original Run did not finish", ctx.Err())
	}
	if !completed.result.Succeeded(completed.err) || !completed.result.CleanupComplete() {
		t.Fatalf("original native prepared run failed: %+v err=%v", completed.result, completed.err)
	}
	expectedGeneration := originalBinding.Resource.CredentialGeneration
	ownRefresh := fault == attachedworkerstack.NativeInlineOwnCredentialRefreshV2ForTest
	if ownRefresh {
		expectedGeneration++
	}
	if completed.result.CredentialChanged != ownRefresh || completed.result.CredentialGeneration != expectedGeneration ||
		!reflect.DeepEqual(invocation.VerifiedInput.Job.HarnessBinding, originalBinding) ||
		!reflect.DeepEqual(f.Original.VerifiedInput.Job.HarnessBinding, originalBinding) {
		t.Fatalf("own WriteBack result or immutable original binding changed: changed=%t generation=%d want=%d", completed.result.CredentialChanged, completed.result.CredentialGeneration, expectedGeneration)
	}
	source, err := f.Stack.InlineReceiptCandidate(ctx, invocation.Identity, completed.result, completed.err)
	if err != nil {
		t.Fatalf("original completed producer: %v", err)
	}
	declaration, producer, expiry, err := source.Declaration()
	if err != nil || len(declaration.Artifacts) != 2 || producer.FinalDigest == "" || expiry.IsZero() {
		t.Fatalf("original declaration malformed: %v", err)
	}
	completeDone, completeFinished := make(chan error, 1), make(chan struct{})
	go func() {
		completeDone <- adapter.Complete(ctx, invocation.Identity, completed.result, completed.err)
		close(completeFinished)
	}()
	t.Cleanup(func() {
		cancel()
		f.ContinueACK()
		joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-completeFinished:
		case <-joined.Done():
			t.Error("owned Complete did not join cleanup")
		}
	})
	select {
	case <-f.AckEntered:
	case earlyErr := <-completeDone:
		_, _, _, failures := peer.snapshot()
		t.Fatalf("original Complete failed before TerminalAck: %v HTTP-peer=%v", earlyErr, failures)
	case <-ctx.Done():
		t.Fatal("original Complete did not reach TerminalAck", ctx.Err())
	}
	checkpoint, err := nativeInlineCheckpointAt(f.JournalPath)
	if err != nil || checkpoint.Progress.Revision != 4 || !checkpoint.Progress.BodyBurned || !checkpoint.Progress.ReconcileBurned || checkpoint.Progress.TerminalAcked || checkpoint.Progress.CanonicalDigest == "" || checkpoint.Plan.Producer != producer {
		t.Fatalf("actual pre-ACK durable journal progress=%+v err=%v", checkpoint.Progress, err)
	}
	if err := source.CheckCurrent(ctx); err != nil {
		t.Fatalf("HTTP body Close retired original source before ACK: %v", err)
	}
	if _, _, err := source.OpenBody(ctx); err == nil {
		t.Fatal("completed body send reopened before ACK")
	}
	if _, _, _, err := source.Declaration(); err != nil {
		t.Fatal("lost READY/held ACK erased original declaration", err)
	}
	f.ContinueACK()
	select {
	case err = <-completeDone:
	case <-ctx.Done():
		t.Fatal("original ACK completion did not finish", ctx.Err())
	}
	if err != nil {
		_, _, _, failures := peer.snapshot()
		t.Fatalf("original Complete/ACK failed: %v HTTP-peer=%v", err, failures)
	}
	if _, err := os.Stat(f.JournalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("acknowledged journal not retired: %v", err)
	}
	if err := source.CheckCurrent(ctx); err == nil {
		t.Fatal("ACK left original producer current")
	}
	if _, _, _, err := source.Declaration(); err == nil {
		t.Fatal("ACK left retired original declaration usable")
	}
	if _, err := f.Stack.Run(ctx, invocation); err == nil {
		t.Fatal("ACK/recovery allowed a second process execution")
	}
	if err := adapter.Complete(ctx, invocation.Identity, completed.result, completed.err); err != nil {
		t.Fatalf("identical completion not locally idempotent: %v", err)
	}
	prepares, bodies, reconciles, failures := peer.snapshot()
	events, launches, terminals := f.Events()
	if prepares != 1 || bodies != 1 || reconciles != 1 || len(failures) != 0 || launches != 1 || terminals != 1 || v1.calls != 0 {
		t.Fatalf("original pipeline counts prepare/body/reconcile=%d/%d/%d launches=%d terminals=%d v1=%d events=%v failures=%v", prepares, bodies, reconciles, launches, terminals, v1.calls, events, failures)
	}
	peer.mu.Lock()
	if string(peer.observed) != nativeInlineOriginalBody || !reflect.DeepEqual(peer.paths, []string{attachedworkerreceipt.PathInlinePrepareV2, attachedworkerreceipt.PathInlineBodyV2, attachedworkerreceipt.PathInlineReconcileV2}) {
		t.Error("original byte order or finite HTTP exchange sequence changed")
	}
	peer.mu.Unlock()
}

type nativeInlineFaultScenario struct {
	fixture    *attachedworkerstack.NativeInlineFixtureV2ForTest
	peer       *nativeInlineHTTPPeer
	adapter    *transport.Adapter
	v1         *preparedReceiptPublisher
	invocation attachedworkerdaemon.Invocation
	result     attachedworkerdaemon.InvocationResult
	source     *codexexec.NativeInlineOutputV2
}

// The same fixed genuine constructor supplies every fault scenario. These
// helpers never replace a prepared owner or synthesize a successful result.
func newNativeInlineFaultScenario(t *testing.T, fault attachedworkerstack.NativeInlineFaultV2ForTest) *nativeInlineFaultScenario {
	t.Helper()
	s, ctx := newNativeInlineFaultAdmission(t, fault)
	f := s.fixture
	f.ContinueTeardown()
	f.ContinueWriteBack()
	var err error
	s.result, err = f.Stack.Run(ctx, s.invocation)
	if !s.result.Succeeded(err) || !s.result.CleanupComplete() {
		t.Fatalf("original fault run failed: %+v err=%v", s.result, err)
	}
	s.source, err = f.Stack.InlineReceiptCandidate(ctx, s.invocation.Identity, s.result, nil)
	if err != nil {
		t.Fatalf("original fault producer unavailable: %v", err)
	}
	return s
}

func newNativeInlineFaultAdmission(t *testing.T, fault attachedworkerstack.NativeInlineFaultV2ForTest) (*nativeInlineFaultScenario, context.Context) {
	t.Helper()
	f := attachedworkerstack.NewNativeInlineFixtureV2ForTest(t, fault)
	s := &nativeInlineFaultScenario{fixture: f, peer: &nativeInlineHTTPPeer{fixture: f, fault: fault}, v1: &preparedReceiptPublisher{}}
	client, err := attachedworkerreceipt.NewInlineClientV2("https://native-inline.invalid", &http.Client{Transport: s.peer}, []byte(nativeInlineHTTPBearer))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		s.peer.mu.Lock()
		clear(s.peer.observed)
		s.peer.observed = nil
		s.peer.mu.Unlock()
	})
	s.adapter, err = transport.NewPreparedAdapter(f.Session, &preparedReceiptMaterializer{invocation: f.Original}, transport.Config{Profile: transport.LocalProfileV1{Name: "fixed-native-inline-fault-fixture", CapabilityDigest: f.Session.Snapshot().CapabilityDigest, Executable: f.Original.Process.Executable, ExecutableDigest: f.Original.Process.ExecutableDigest, Arguments: f.Original.Process.Arguments},
		CapabilityManifest: &f.Capability, ReceiptPublisher: s.v1, InlineReceiptPublisher: client, MaterializationRoot: f.MaterializationRoot, MaxInputBytes: 1 << 20, ReportTimeout: 5 * time.Second, Now: f.CurrentTime}, f.Stack)
	if err != nil {
		t.Fatalf("join original fault Stack/Session: %v", err)
	}
	window := 20 * time.Second
	if fault == attachedworkerstack.NativeInlineExportExpiryV2ForTest {
		window = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(t.Context(), window)
	t.Cleanup(cancel)
	var available bool
	s.invocation, available, err = s.adapter.Next(ctx)
	if err != nil || !available {
		t.Fatalf("original fault offer/claim/materialization available=%v err=%v", available, err)
	}
	t.Cleanup(s.invocation.VerifiedInput.Clear)
	return s, ctx
}

// These are genuine original-owner finalization controls, not native isolation,
// real credential or persistence proofs. The fixed shell completes its original
// protocol; only the closed lower-boundary/lifecycle fault is synthetic. Failed
// results are never relabelled as success or turned into an inline producer.
// Only complete cleanup may reach the existing V1 failed-receipt/ACK path.
func TestNativeInlineTransportV2FinalizationFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		fault   attachedworkerstack.NativeInlineFaultV2ForTest
		failure string
		events  []string
	}{
		{"boundary_release", attachedworkerstack.NativeInlineBoundaryReleaseFailureV2ForTest, "", []string{"issue", "materialize", "boundary-release-failed", "writeback-entered", "writeback", "release"}},
		{"writeback", attachedworkerstack.NativeInlineWriteBackFailureV2ForTest, "credential_writeback_failed", []string{"issue", "materialize", "boundary-release", "writeback-entered", "writeback-failed", "release"}},
		{"credential_release", attachedworkerstack.NativeInlineCredentialReleaseFailureV2ForTest, "credential_release_failed", []string{"issue", "materialize", "boundary-release", "writeback-entered", "writeback", "release-failed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newNativeInlineFaultAdmission(t, tc.fault)
			f := s.fixture
			f.ContinueTeardown()
			f.ContinueWriteBack()
			result, runErr := f.Stack.Run(ctx, s.invocation)
			if runErr == nil || result.Succeeded(runErr) || result.Process.ExitCode != 0 || !result.Process.DescendantsReaped || result.Process.Cancelled || result.Process.Deadline || ctx.Err() != nil {
				t.Fatalf("original completed process did not fail at finalization: result=%+v err=%v context=%v", result, runErr, ctx.Err())
			}
			if tc.failure != "" && (!errors.Is(runErr, attachedworkerdaemon.ErrCredentialFinalization) || result.FailureCode != tc.failure) {
				t.Fatalf("original finalization classification=%q err=%v", result.FailureCode, runErr)
			}
			if tc.fault == attachedworkerstack.NativeInlineBoundaryReleaseFailureV2ForTest {
				var classified *domain.ClassifiedError
				if result.Process.BoundaryReleased || result.Process.CleanupSucceeded || result.CleanupComplete() || !result.CredentialReleased ||
					!errors.Is(runErr, codexexec.ErrContract) || !errors.As(runErr, &classified) || classified.Kind != domain.ErrorTerminal || classified.Code != string(sessionlessharness.FailureHarnessBackendFailed) {
					t.Fatalf("original supervisor certified failed release: result=%+v err=%v", result, runErr)
				}
			} else if !result.Process.BoundaryReleased || !result.Process.CleanupSucceeded || result.CredentialReleased != (tc.fault == attachedworkerstack.NativeInlineWriteBackFailureV2ForTest) {
				t.Fatalf("original cleanup/handback observations changed: %+v", result)
			}
			if source, err := f.Stack.InlineReceiptCandidate(ctx, s.invocation.Identity, result, runErr); source != nil || !errors.Is(err, codexexec.ErrContract) {
				t.Fatalf("failed original finalization minted inline authority: present=%t err=%v", source != nil, err)
			}
			if _, err := f.Stack.Run(ctx, s.invocation); err == nil {
				t.Fatal("failed original invocation permitted another run")
			}
			if tc.fault != attachedworkerstack.NativeInlineWriteBackFailureV2ForTest {
				if err := s.adapter.Complete(ctx, s.invocation.Identity, result, runErr); !errors.Is(err, transport.ErrTerminalEvidenceInvalid) {
					t.Fatalf("incomplete cleanup did not deny normal failed publication: %v", err)
				}
			}
			events, launches, terminals := f.Events()
			if !reflect.DeepEqual(events, tc.events) || launches != 1 || terminals != 0 {
				t.Fatalf("original lifecycle effects events=%v launches=%d terminals=%d", events, launches, terminals)
			}
			prepare, body, reconcile, failures := s.peer.snapshot()
			s.peer.mu.Lock()
			paths, observed, capability := len(s.peer.paths), len(s.peer.observed), s.peer.prepare.Nonce
			s.peer.mu.Unlock()
			if prepare != 0 || body != 0 || reconcile != 0 || paths != 0 || observed != 0 || capability != "" || len(failures) != 0 || s.v1.calls != 0 {
				t.Fatalf("failed finalization reached receipt peers: prepare=%d body=%d reconcile=%d paths=%d observed=%d v1=%d failures=%v", prepare, body, reconcile, paths, observed, s.v1.calls, failures)
			}
			if _, err := os.Stat(f.JournalPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed finalization created an output journal: %v", err)
			}
			if tc.fault == attachedworkerstack.NativeInlineWriteBackFailureV2ForTest {
				completeNativeInlineWriteBackFailure(t, s, ctx, result, runErr)
			}
		})
	}
}

func readNativeInlineFailureCheckpoint(t *testing.T, path string) []byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("open actual failure checkpoint", err)
	}
	const limit = 256 << 10
	raw, readErr := io.ReadAll(io.LimitReader(file, limit+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil || len(raw) > limit {
		clear(raw)
		t.Fatalf("actual failure checkpoint read: size=%d read=%v close=%v", len(raw), readErr, closeErr)
	}
	t.Cleanup(func() { clear(raw) })
	return raw
}

// This joins the genuine original failure/owner/adapter/Session/file-journal
// path. The existing V1 publisher and ACK peer remain synthetic commitments,
// not canonical YDB materialization or real provider/credential evidence.
func completeNativeInlineWriteBackFailure(t *testing.T, s *nativeInlineFaultScenario, parent context.Context,
	result attachedworkerdaemon.InvocationResult, runErr error,
) {
	t.Helper()
	if !result.CleanupComplete() {
		t.Fatal("writeback failure lacks required original cleanup")
	}
	f := s.fixture
	head := s.invocation.VerifiedInput.AcceptedAttempt
	ctx, cancel := context.WithCancel(parent)
	done, joined := make(chan error, 1), make(chan struct{})
	go func() {
		done <- s.adapter.Complete(ctx, s.invocation.Identity, result, runErr)
		close(joined)
	}()
	t.Cleanup(func() {
		cancel()
		f.ContinueACK()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-cleanup.Done():
			t.Error("normal failed Complete did not join owned cleanup")
		}
	})
	select {
	case <-f.AckEntered:
	case err := <-done:
		t.Fatal("normal writeback failure Complete ended before original ACK barrier", err)
	case <-ctx.Done():
		t.Fatal("normal writeback failure Complete did not reach ACK barrier", ctx.Err())
	}
	var journal attachedworkerlocal.ReceiptCheckpointV1
	if err := json.Unmarshal(readNativeInlineFailureCheckpoint(t, f.JournalPath), &journal); err != nil {
		t.Fatal("decode actual V1 failure journal", err)
	}
	t.Cleanup(func() { clear(journal.Payload) })
	digest := sha256.Sum256(journal.Payload)
	connection := f.Session.Snapshot()
	var submission transport.ReceiptSubmissionV1
	if s.v1.calls != 1 || journal.Version != attachedworkerlocal.ReceiptCheckpointVersionV1 || journal.ConnectionID != connection.ConnectionID ||
		journal.TenantID != connection.TenantID || journal.OwnerUserID != connection.OwnerUserID || journal.WorkerID != connection.WorkerID ||
		journal.EnrollmentGeneration != connection.EnrollmentGeneration || journal.ConnectionGeneration != connection.ConnectionGeneration ||
		journal.PayloadSHA256 != hex.EncodeToString(digest[:]) || !bytes.Equal(journal.Payload, s.v1.first) ||
		json.Unmarshal(journal.Payload, &submission) != nil || submission.Candidate.Status != domain.AttachedWorkerTerminalFailed ||
		submission.Candidate.FailureCode != "codex_exec_finalization_failed" || submission.Candidate.Summary != "" || len(submission.Candidate.Artifacts) != 0 ||
		submission.Observation.ValidateFor(domain.AttachedWorkerTerminalFailed, true) != nil || !submission.Observation.RunnerFailed ||
		submission.Observation.InvocationFailureCode != "credential_writeback_failed" || submission.Observation.ExitCode != 0 {
		t.Fatalf("original failure was not sealed/published as one V1 finalization failure: calls=%d status=%s code=%s", s.v1.calls, submission.Candidate.Status, submission.Candidate.FailureCode)
	}
	if candidate, err := f.Stack.ReceiptCandidate(s.invocation.Identity, result, runErr, domain.AttachedWorkerTerminalFailed); err != nil || !reflect.DeepEqual(candidate, submission.Candidate) {
		t.Fatal("ACK barrier lost original private failed candidate", err)
	}
	f.ContinueACK()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("normal failed receipt/exact original Session ACK/retirement", err)
		}
	case <-ctx.Done():
		t.Fatal("normal failed receipt ACK did not finish", ctx.Err())
	}
	var reconnect attachedworkerlocal.ReconnectCheckpointV1
	checkpointPath := filepath.Join(filepath.Dir(f.JournalPath), attachedworkerlocal.ReconnectCheckpointFileName)
	if err := json.Unmarshal(readNativeInlineFailureCheckpoint(t, checkpointPath), &reconnect); err != nil {
		t.Fatal("decode original Session's durable terminal state", err)
	}
	summary := reconnect.MachineSnapshot.Attempt.Summary
	if f.Session.Snapshot().State != attachedworkersession.StateReady || summary.State != attachedworkerprotocol.AttemptTerminalCommitted ||
		!reflect.DeepEqual(summary.Binding, submission.Request.Attempt) || summary.WorkerSequence != head.WorkerAttemptSequence+1 || summary.PlatformSequence != head.PlatformAttemptSequence+1 ||
		summary.TerminalSequence != 1 || summary.TerminalStatus != attachedworkerprotocol.TerminalFailed || summary.TerminalResult != attachedworkerprotocol.TerminalResultFailed ||
		!bytes.Equal(summary.TerminalEvidenceDigest, digest[:]) {
		t.Fatalf("exact failed Terminal/ACK was not durably committed: state=%s worker=%d platform=%d status=%s result=%s", summary.State, summary.WorkerSequence, summary.PlatformSequence, summary.TerminalStatus, summary.TerminalResult)
	}
	if _, err := os.Stat(f.JournalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("exact failed ACK did not retire the original receipt journal", err)
	}
	if _, err := f.Stack.ReceiptCandidate(s.invocation.Identity, result, runErr, domain.AttachedWorkerTerminalFailed); !errors.Is(err, codexexec.ErrContract) {
		t.Fatal("exact failed ACK/journal retirement retained the original candidate owner", err)
	}
	if err := s.adapter.Complete(ctx, s.invocation.Identity, result, runErr); err != nil {
		t.Fatal("acknowledged failed completion not locally idempotent", err)
	}
	if _, err := f.Stack.Run(ctx, s.invocation); err == nil {
		t.Fatal("failed ACK completion permitted another original invocation")
	}
	prepares, bodies, reconciles, failures := s.peer.snapshot()
	events, launches, terminals := f.Events()
	exchanges, replay := f.TerminalExchanges()
	s.peer.mu.Lock()
	paths, observed, nonce := len(s.peer.paths), len(s.peer.observed), s.peer.prepare.Nonce
	s.peer.mu.Unlock()
	if prepares != 0 || bodies != 0 || reconciles != 0 || paths != 0 || observed != 0 || nonce != "" || len(failures) != 0 || s.v1.calls != 1 || launches != 1 || terminals != 1 || exchanges != 1 || replay ||
		!reflect.DeepEqual(events, []string{"issue", "materialize", "boundary-release", "writeback-entered", "writeback-failed", "release"}) {
		t.Fatalf("failed ACK path duplicated work or reached V2: V2=%d/%d/%d paths=%d v1=%d launches=%d terminal=%d/%d replay=%t events=%v failures=%v", prepares, bodies, reconciles, paths, s.v1.calls, launches, terminals, exchanges, replay, events, failures)
	}
}

func (s *nativeInlineFaultScenario) completeUntilACK(t *testing.T, ctx context.Context) <-chan error {
	t.Helper()
	done, joined := make(chan error, 1), make(chan struct{})
	go func() { done <- s.adapter.Complete(ctx, s.invocation.Identity, s.result, nil); close(joined) }()
	t.Cleanup(func() {
		s.fixture.ContinueACK()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		select {
		case <-joined:
		case <-cleanup.Done():
			t.Error("fault Complete did not join owned cleanup")
		}
	})
	select {
	case <-s.fixture.AckEntered:
	case err := <-done:
		prepares, bodies, reconciles, failures := s.peer.snapshot()
		snapshot := s.fixture.Session.Snapshot()
		checkpoint, checkpointErr := nativeInlineCheckpointAt(s.fixture.JournalPath)
		_, _, originalExpiry, sourceErr := s.source.Declaration()
		t.Fatalf("fault Complete failed before ACK barrier: %v session-state=%s session-auth-expires=%v checkpoint-progress=%+v checkpoint-err=%v peer-counts=%d/%d/%d HTTP-peer=%v original-expiry=%v current-time=%v source-err=%v", err, snapshot.State, snapshot.AuthenticationExpires, checkpoint.Progress, checkpointErr, prepares, bodies, reconciles, failures, originalExpiry, s.fixture.CurrentTime(), sourceErr)
	case <-ctx.Done():
		t.Fatal("fault Complete did not reach ACK barrier", ctx.Err())
	}
	return done
}

func (s *nativeInlineFaultScenario) assertOneOriginalPipeline(t *testing.T, exchanges int, replay bool) {
	t.Helper()
	prepares, bodies, reconciles, failures := s.peer.snapshot()
	events, launches, logicalTerminals := s.fixture.Events()
	physicalTerminals, exactReplay := s.fixture.TerminalExchanges()
	if prepares != 1 || bodies != 1 || reconciles != 1 || len(failures) != 0 || launches != 1 || logicalTerminals != 1 || physicalTerminals != exchanges || exactReplay != replay || s.v1.calls != 0 {
		t.Errorf("fault counts prepare/body/reconcile=%d/%d/%d launches=%d terminals=%d/%d exactReplay=%v v1=%d events=%v HTTP-peer=%v", prepares, bodies, reconciles, launches, logicalTerminals, physicalTerminals, exactReplay, s.v1.calls, events, failures)
	}
}

func TestNativeInlineTransportV2LostACKExactSessionReplay(t *testing.T) {
	s := newNativeInlineFaultScenario(t, attachedworkerstack.NativeInlineLostACKV2ForTest)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := s.completeUntilACK(t, ctx)
	select {
	case <-s.fixture.LostACKEntered:
	default:
		t.Fatal("ACK replay did not follow the server-accepted lost first response")
	}
	checkpoint, err := nativeInlineCheckpointAt(s.fixture.JournalPath)
	if err != nil || checkpoint.Progress.Revision != 4 || checkpoint.Progress.TerminalAcked || checkpoint.Progress.CanonicalDigest == "" {
		t.Fatalf("lost ACK did not retain exact READY journal: progress=%+v err=%v", checkpoint.Progress, err)
	}
	if err := s.source.CheckCurrent(ctx); err != nil {
		t.Fatal("lost ACK prematurely retired original source", err)
	}
	if _, _, err := s.source.OpenBody(ctx); err == nil {
		t.Fatal("lost ACK reopened the consumed original body")
	}
	if _, _, _, err := s.source.Declaration(); err != nil {
		t.Fatal("lost ACK erased original declaration", err)
	}
	s.fixture.ContinueACK()
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("exact cached ACK replay did not finish", ctx.Err())
	}
	if err != nil {
		t.Fatalf("original Session exact ACK replay: %v", err)
	}
	if _, err := os.Stat(s.fixture.JournalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("valid replayed ACK did not retire journal: %v", err)
	}
	if _, _, _, err := s.source.Declaration(); err == nil {
		t.Fatal("valid replayed ACK retained retired original declaration")
	}
	if _, err := s.fixture.Stack.Run(ctx, s.invocation); err == nil {
		t.Fatal("lost ACK permitted another original process")
	}
	if err := s.adapter.Complete(ctx, s.invocation.Identity, s.result, nil); err != nil {
		t.Fatalf("completed ACK replay not locally idempotent: %v", err)
	}
	s.assertOneOriginalPipeline(t, 2, true)
}

func assertNativeInlineForgedACK(t *testing.T, fault attachedworkerstack.NativeInlineFaultV2ForTest) {
	t.Helper()
	s := newNativeInlineFaultScenario(t, fault)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := s.completeUntilACK(t, ctx)
	before, err := nativeInlineCheckpointAt(s.fixture.JournalPath)
	if err != nil {
		t.Fatal("load READY journal before forged ACK", err)
	}
	_, producer, expiry, err := s.source.Declaration()
	if err != nil {
		t.Fatal("original source before forged ACK", err)
	}
	s.fixture.ContinueACK()
	select {
	case err = <-done:
	case <-ctx.Done():
		t.Fatal("forged ACK did not fail within original Session window", ctx.Err())
	}
	if !errors.Is(err, transport.ErrReconciliationRequired) || s.fixture.Session.Snapshot().State != attachedworkersession.StateReconciliationRequired {
		t.Fatalf("forged ACK did not fail original Session closed: err=%v state=%s", err, s.fixture.Session.Snapshot().State)
	}
	after, err := nativeInlineCheckpointAt(s.fixture.JournalPath)
	if err != nil || !reflect.DeepEqual(before, after) || after.Progress.TerminalAcked {
		t.Fatalf("forged ACK changed or retired actual journal: progress=%+v err=%v", after.Progress, err)
	}
	_, retainedProducer, retainedExpiry, err := s.source.Declaration()
	if err != nil || retainedProducer != producer || !retainedExpiry.Equal(expiry) {
		t.Fatalf("forged ACK discarded retained declaration: %v", err)
	}
	// The Session is no longer ready: retention is not current export authority.
	if err := s.source.CheckCurrent(ctx); err == nil {
		t.Fatal("fenced Session retained current export authority")
	}
	if err := s.adapter.Complete(ctx, s.invocation.Identity, s.result, nil); !errors.Is(err, transport.ErrReconciliationRequired) {
		t.Fatalf("forged ACK allowed another completion dispatch: %v", err)
	}
	if _, err := s.fixture.Stack.Run(ctx, s.invocation); err == nil {
		t.Fatal("forged ACK allowed another original process")
	}
	s.assertOneOriginalPipeline(t, 1, false)
}

func TestNativeInlineTransportV2ForgedDigestACK(t *testing.T) {
	assertNativeInlineForgedACK(t, attachedworkerstack.NativeInlineForgedDigestACKV2ForTest)
}

func TestNativeInlineTransportV2ForgedSequenceACK(t *testing.T) {
	assertNativeInlineForgedACK(t, attachedworkerstack.NativeInlineForgedSequenceACKV2ForTest)
}

func TestNativeInlineTransportV2ExpiredExportMetadataRecovery(t *testing.T) {
	s := newNativeInlineFaultScenario(t, attachedworkerstack.NativeInlineExportExpiryV2ForTest)
	_, producer, originalExpiry, err := s.source.Declaration()
	if err != nil {
		t.Fatal("original expiry declaration unavailable", err)
	}
	// The actual successful Run retained its original ten-second caller bound.
	// Complete cannot renew it. The peer reads one body, then waits for exactly
	// that HTTP context to expire; it never advances the production clock.
	first, cancelFirst := context.WithTimeout(t.Context(), 20*time.Second)
	err = s.adapter.Complete(first, s.invocation.Identity, s.result, nil)
	cancelFirst()
	if !errors.Is(err, transport.ErrReconciliationRequired) || s.fixture.CurrentTime().Before(originalExpiry) {
		t.Fatalf("original export did not expire at immutable deadline: err=%v expiry=%v", err, originalExpiry)
	}
	checkpoint, err := nativeInlineCheckpointAt(s.fixture.JournalPath)
	if err != nil || checkpoint.Progress.Revision != 2 || !checkpoint.Progress.PrepareBurned || !checkpoint.Progress.BodyBurned || checkpoint.Progress.ReconcileBurned || checkpoint.Progress.CanonicalDigest != "" || checkpoint.Progress.TerminalAcked || !checkpoint.Plan.ExpiresAt.Equal(originalExpiry) {
		t.Fatalf("expired first Complete consumed metadata recovery or changed original plan: progress=%+v err=%v", checkpoint.Progress, err)
	}
	prepares, bodies, reconciles, failures := s.peer.snapshot()
	_, launches, terminals := s.fixture.Events()
	if prepares != 1 || bodies != 1 || reconciles != 0 || len(failures) != 0 || launches != 1 || terminals != 0 || s.fixture.Session.Snapshot().State != attachedworkersession.StateReady {
		t.Fatalf("expired export effects prepare/body/reconcile=%d/%d/%d launches=%d terminals=%d session=%s failures=%v", prepares, bodies, reconciles, launches, terminals, s.fixture.Session.Snapshot().State, failures)
	}
	recovery, cancelRecovery := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelRecovery()
	if err := s.source.CheckCurrent(recovery); err == nil {
		t.Fatal("expired source retained fresh export authority")
	}
	if _, _, err := s.source.OpenBody(recovery); err == nil {
		t.Fatal("expired source reopened original body")
	}
	_, retainedProducer, retainedExpiry, err := s.source.Declaration()
	if err != nil || retainedProducer != producer || !retainedExpiry.Equal(originalExpiry) {
		t.Fatalf("expiry discarded or renewed original metadata: %v", err)
	}
	done := s.completeUntilACK(t, recovery)
	ready, err := nativeInlineCheckpointAt(s.fixture.JournalPath)
	if err != nil || ready.Progress.Revision != 4 || !ready.Progress.ReconcileBurned || ready.Progress.CanonicalDigest == "" || ready.Progress.TerminalAcked || ready.PlanSHA256 != checkpoint.PlanSHA256 || !ready.Plan.ExpiresAt.Equal(originalExpiry) {
		t.Fatalf("fresh metadata-only recovery changed immutable plan: progress=%+v err=%v", ready.Progress, err)
	}
	s.fixture.ContinueACK()
	select {
	case err = <-done:
	case <-recovery.Done():
		t.Fatal("metadata-only recovery did not finish original ACK", recovery.Err())
	}
	if err != nil {
		t.Fatalf("expired export metadata recovery: %v", err)
	}
	if _, err := os.Stat(s.fixture.JournalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("metadata recovery ACK did not retire journal: %v", err)
	}
	if _, _, _, err := s.source.Declaration(); err == nil {
		t.Fatal("metadata recovery ACK did not retire original source")
	}
	if _, err := s.fixture.Stack.Run(recovery, s.invocation); err == nil {
		t.Fatal("expired export recovery permitted another process")
	}
	s.assertOneOriginalPipeline(t, 1, false)
}
