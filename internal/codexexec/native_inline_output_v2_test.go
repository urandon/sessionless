//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const nativeInlineFinalFixtureV2 = `{"answer":"two requested files","artifacts":[{"name":"data.json","media_type":"application/json","content":" { \"n\": 9007199254740993123456789 } "},{"name":"note.txt","media_type":"text/plain","content":"original text\n"}]}`

func nativeInlineFixtureV2(t *testing.T, final string) (*PreparedAttachedWorkerRunnerV1, attachedworkerdaemon.Invocation, attachedworkerdaemon.InvocationResult, *nativeCredentialMetadataReaderV2) {
	t.Helper()
	r, invocation, life, launcher, metadata := nativeJoinedFixtureMediaV2(t, final, false, nativeJoinedOptionsV2{inlineOutput: true})
	result, err := r.Run(context.Background(), invocation)
	if !result.Succeeded(err) || len(launcher.envelopes) != 1 || !reflect.DeepEqual(life.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("inline original lifecycle result=%+v err=%v launches=%d life=%v", result, err, len(launcher.envelopes), life.order)
	}
	return r, invocation, result, metadata
}

func TestNativeInlineOutputV2OriginalCompletedOwner(t *testing.T) {
	r, invocation, result, _ := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
	native := r.slot.original.native
	if native.child.Current() == nil {
		t.Fatal("execution child must already be closed before export")
	}
	v, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, producer, expiry, err := v.Declaration()
	if err != nil || len(candidate.Artifacts) != 2 || candidate.Summary != "two requested files" || !expiry.Equal(r.nativeNowV2().Add(15*time.Second)) {
		t.Fatalf("declaration=%+v expiry=%v err=%v", candidate, expiry, err)
	}
	harness, _ := invocation.VerifiedInput.Job.HarnessBinding.Digest()
	answer := sha256.Sum256([]byte(candidate.Summary))
	if producer.SessionID != invocation.VerifiedInput.Job.SessionID || producer.HarnessBindingDigest != harness ||
		producer.ReservationID != invocation.VerifiedInput.Job.ReservationID || !producer.AcceptedAt.Equal(invocation.VerifiedInput.AcceptedAttempt.CreatedAt) ||
		producer.SchemaDigest != r.nativeProfileV2.launch.Schema.SHA256 || producer.FinalDigest != hex.EncodeToString(r.slot.original.rawFinalDigestV2[:]) ||
		producer.AnswerDigest != hex.EncodeToString(answer[:]) {
		t.Fatalf("original producer binding changed: %+v", producer)
	}
	if r.slot.credentialDeadline.IsZero() || !r.slot.credentialDeadline.Equal(invocation.Credential.IssueRequest.ExpiresAt) || r.slot.callerDeadline.IsZero() {
		t.Fatal("original credential/caller deadline was not retained after cleanup")
	}
	// Caller metadata mutation cannot affect the retained original plan.
	candidate.Artifacts[0].Name = "forged.txt"
	canonical, _, _, _ := v.Declaration()
	if canonical.Artifacts[0].Name != "data.json" {
		t.Fatal("metadata alias changed private plan")
	}
	if err := v.CheckCurrent(context.Background()); err != nil {
		t.Fatalf("original receipt scope after execution child close: %v", err)
	}
	body, size, err := v.OpenBody(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	raw, err := io.ReadAll(body)
	if err != nil || int64(len(raw)) != size || string(raw) != " { \"n\": 9007199254740993123456789 } original text\n" {
		t.Fatalf("original ordered stream len=%d declared=%d err=%v", len(raw), size, err)
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(canonical, raw)
	if err != nil {
		t.Fatalf("full original content verifier: %v", err)
	}
	verified.Clear()
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := v.OpenBody(context.Background()); err == nil {
		t.Fatal("body reopened after successful read/close")
	}
	if _, err := r.ReceiptCandidate(invocation.Identity, result, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("V1 receipt silently dropped V2 artifact bytes")
	}
	if native.child.Current() == nil {
		t.Fatal("receipt metadata reactivated execution child")
	}
	f := r.slot.original.nativeFinalV2
	retained := f.artifacts[0].content
	if len(f.artifacts) != 2 || !bytes.Contains(retained, []byte("9007199254740993123456789")) {
		t.Fatal("HTTP close retired original bytes before ACK")
	}
	if err := r.RetireReceipt(invocation.Identity); err != nil {
		t.Fatal(err)
	}
	if err := v.CheckCurrent(context.Background()); err == nil {
		t.Fatal("retired original result retained current receipt authority")
	}
	if len(f.artifacts) != 0 || !bytes.Equal(retained, make([]byte, len(retained))) {
		t.Fatal("retirement did not clear owned semantic bytes")
	}
	if _, _, _, err := v.Declaration(); err == nil {
		t.Fatal("retired producer metadata remained usable")
	}
	if _, err := body.Read(make([]byte, 1)); err == nil {
		t.Fatal("retired reader remained usable")
	}
}

func TestNativeInlineOutputV2RequiresOriginalResult(t *testing.T) {
	for _, kind := range []string{"not-selected", "before-run", "changed-result", "foreign-identity", "copied-owner", "cleanup", "final-owner"} {
		t.Run(kind, func(t *testing.T) {
			r, invocation, _, _, _ := nativeJoinedFixtureMediaV2(t, `{"answer":"original","artifacts":[]}`, false, nativeJoinedOptionsV2{inlineOutput: kind != "not-selected"})
			var result attachedworkerdaemon.InvocationResult
			var err error
			if kind != "before-run" {
				result, err = r.Run(context.Background(), invocation)
				if !result.Succeeded(err) {
					t.Fatalf("original fixture failed: %v", err)
				}
			}
			switch kind {
			case "changed-result":
				result.Process.ExitCode = 1
			case "foreign-identity":
				invocation.Identity.RunID = "foreign-run"
			case "copied-owner":
				r = &PreparedAttachedWorkerRunnerV1{nativeIdentityV2: r, nativeConfigV2: r.nativeConfigV2, slot: r.slot}
			case "cleanup":
				r.slot.stageClean = false
				t.Cleanup(func() { r.slot.stageClean = true })
			case "final-owner":
				f := r.slot.original.nativeFinalV2
				f.self = nil
				t.Cleanup(func() { f.self = f })
			}
			if _, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil); err == nil {
				t.Fatal("foreign/non-final result minted producer")
			}
		})
	}
}

func TestNativeInlineOutputV2ExpiryAndRevocationDoNotRenew(t *testing.T) {
	for _, kind := range []string{"before-export-revoked", "after-export-revoked", "generation", "expired", "cancelled-context", "mid-body"} {
		t.Run(kind, func(t *testing.T) {
			r, invocation, result, metadata := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
			if kind == "before-export-revoked" {
				metadata.binding.Revoked = true
				if _, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil); err == nil {
					t.Fatal("revoked resource exported")
				}
				return
			}
			v, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, _, originalExpiry, _ := v.Declaration()
			ctx := context.Background()
			switch kind {
			case "after-export-revoked":
				metadata.binding.Revoked = true
			case "generation":
				metadata.binding.Generation++
			case "expired":
				r.config.Now = func() time.Time { return originalExpiry }
			case "cancelled-context":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			case "mid-body":
				reader, _, err := v.OpenBody(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer reader.Close()
				if n, err := reader.Read(make([]byte, 1)); n != 1 || err != nil {
					t.Fatalf("first bounded byte n=%d err=%v", n, err)
				}
				metadata.binding.Revoked = true
				if n, err := reader.Read(make([]byte, 1)); n != 0 || err == nil {
					t.Fatal("revoked resource continued original body")
				}
			}
			if _, _, err := v.OpenBody(ctx); err == nil {
				t.Fatal("expired/revoked/cancelled body authority accepted")
			}
			again, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
			if err != nil || again != v {
				t.Fatalf("metadata-only identity changed: %v", err)
			}
			_, _, expiry, err := again.Declaration()
			if err != nil || !expiry.Equal(originalExpiry) {
				t.Fatal("metadata recovery renewed export window")
			}
		})
	}
}

func TestNativeInlineOutputV2OneConcurrentBody(t *testing.T) {
	r, invocation, result, _ := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
	v, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	outcomes := make(chan bool, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			<-start
			reader, _, err := v.OpenBody(context.Background())
			if err == nil {
				_ = reader.Close()
			}
			outcomes <- err == nil
		})
	}
	close(start)
	workers.Wait()
	close(outcomes)
	opened := 0
	for ok := range outcomes {
		if ok {
			opened++
		}
	}
	if opened != 1 {
		t.Fatalf("opened streams=%d want1", opened)
	}
}

func TestNativeInlineOutputV2AnswerOnlyAndCopiedCarrier(t *testing.T) {
	r, invocation, result, _ := nativeInlineFixtureV2(t, `{"answer":"answer only","artifacts":[]}`)
	v, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := v.OpenBody(context.Background()); err == nil {
		t.Fatal("answer-only acquired empty body send")
	}
	copied := *v // carrier has no mutex; identity guard must reject a copied value
	if _, _, _, err := copied.Declaration(); err == nil {
		t.Fatal("copied producer accepted")
	}
	if err := copied.CheckCurrent(context.Background()); err == nil {
		t.Fatal("copied producer acquired current receipt authority")
	}
	if strings.Contains(fmt.Sprintf("%+v", v), "answer only") {
		t.Fatal("producer logging disclosed content")
	}
}

func TestNativeInlineOutputV2OriginalDeadlineMinimum(t *testing.T) {
	for _, bound := range []string{"caller", "credential", "lease", "runtime", "evidence"} {
		t.Run(bound, func(t *testing.T) {
			r, invocation, result, _ := nativeInlineFixtureV2(t, nativeInlineFinalFixtureV2)
			// Controlled source metadata for the post-run expiry comparison. No
			// sleep or narrow real timer decides this test's expected result.
			expected := r.nativeNowV2().Add(3 * time.Second)
			switch bound {
			case "caller":
				r.slot.callerDeadline = expected
			case "credential":
				r.slot.credentialDeadline = expected
			case "lease":
				r.slot.accepted.LeaseExpiresAt = expected
			case "runtime":
				r.slot.original.native.runtimeDeadlineV2 = expected
			case "evidence":
				r.slot.request.HarnessBinding.EvidenceExpiresAt = &expected
			}
			v, err := r.InlineReceiptCandidate(context.Background(), invocation.Identity, result, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, _, expiry, err := v.Declaration()
			if err != nil || !expiry.Equal(expected) {
				t.Fatalf("bound=%s expiry=%v want=%v err=%v", bound, expiry, expected, err)
			}
		})
	}
}
