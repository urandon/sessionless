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
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/domain"
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
	f := attachedworkerstack.NewNativeInlineFixtureV2ForTest(t)
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
		CapabilityManifest: &f.Capability, ReceiptPublisher: v1, InlineReceiptPublisher: client, MaterializationRoot: f.MaterializationRoot, MaxInputBytes: 1 << 20, ReportTimeout: 5 * time.Second, Now: func() time.Time { return f.Now }}, f.Stack)
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
