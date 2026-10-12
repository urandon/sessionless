//go:build ydbintegration && (darwin || linux)

package attachedworkerstack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func addPreparedStackMedia(t *testing.T, invocation *attachedworkerdaemon.Invocation) {
	t.Helper()
	input := invocation.VerifiedInput
	job := input.Job
	proof := input.CanonicalContext
	var pngBytes, jpegBytes bytes.Buffer
	img := image.NewNRGBA(image.Rect(0, 0, 64, 32))
	if err := png.Encode(&pngBytes, img); err != nil {
		t.Fatal(err)
	}
	if err := jpeg.Encode(&jpegBytes, img, nil); err != nil {
		t.Fatal(err)
	}
	bodies := [][]byte{[]byte(`{"large":900719925474099312345}`), pngBytes.Bytes(), []byte("public synthetic task data"), jpegBytes.Bytes()}
	types := []string{"application/json", "image/png", "text/plain", "image/jpeg"}
	names := []string{"input.txt", "image.png", "input.txt", "image.png"}
	var refs []sessioncontext.AttachmentRef
	for i, body := range bodies {
		digest := sha256.Sum256(body)
		ref := sessioncontext.AttachmentRef{Name: names[i], MediaType: types[i], Blob: domain.BlobRef{TenantID: job.TenantID, Key: domain.SessionEventObjectPrefix(job.TenantID, job.SessionID, job.TriggerEventID) + string(rune('a'+i)), Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}}
		refs = append(refs, ref)
		qualified := ref
		qualified.Name = "00000000000000000001-0" + string(rune('1'+i)) + "-" + ref.Name
		proof.Attachments = append(proof.Attachments, sessioncontext.AttachmentPayload{AttachmentRef: qualified, Body: body})
	}
	payload, err := json.Marshal(struct {
		Version     uint32                         `json:"version"`
		Text        string                         `json:"text"`
		Attachments []sessioncontext.AttachmentRef `json:"attachments"`
	}{1, "Use all four canonical synthetic inputs", refs})
	if err != nil {
		t.Fatal(err)
	}
	clear(proof.EventBodies[0])
	proof.EventBodies[0] = payload
	digest := sha256.Sum256(payload)
	proof.Input.Events[0].Payload.Size = int64(len(payload))
	proof.Input.Events[0].Payload.SHA256 = hex.EncodeToString(digest[:])
	clear(input.Context)
	input.Context, _, err = sessioncontext.ProjectCanonical(job, proof, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := input.ValidateFor(invocation.Identity); err != nil {
		t.Fatalf("canonical synthetic media fixture=%v", err)
	}
}

func TestNormalStackMediaCancelAndFinishOriginalFinalization(t *testing.T) {
	for _, cancelWins := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel-%t", cancelWins), func(t *testing.T) {
			stack, invocation, lifecycle, launcher, _, _, _ := newPreparedStackSessionFixturePolicy(t, true, true, nil, codexexec.NewSyntheticMediaFixtureConfigV1)
			addPreparedStackMedia(t, &invocation)
			defer func() {
				select {
				case <-launcher.continueTeardown:
				default:
					close(launcher.continueTeardown)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()
			type outcome struct {
				result attachedworkerdaemon.InvocationResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() { result, err := stack.Run(runCtx, invocation); done <- outcome{result, err} }()
			select {
			case <-launcher.teardownEntered:
			case <-ctx.Done():
				t.Fatal("actual media supervisor did not enter natural-exit teardown")
			}
			if cancelWins {
				cancelRun()
			}
			close(launcher.continueTeardown)
			var actual outcome
			select {
			case actual = <-done:
			case <-ctx.Done():
				t.Fatal("media cancel/finish did not settle")
			}
			if actual.result.Process.Cancelled != cancelWins || (cancelWins && actual.err == nil) || (!cancelWins && !actual.result.Succeeded(actual.err)) {
				t.Fatalf("cancel/finish result=%+v err=%v", actual.result, actual.err)
			}
			status := domain.AttachedWorkerTerminalSucceeded
			if cancelWins {
				status = domain.AttachedWorkerTerminalCancelled
			}
			if _, err := stack.ReceiptCandidate(invocation.Identity, actual.result, actual.err, status); err != nil {
				t.Fatalf("same original finalized media receipt=%v", err)
			}
			lifecycle.mu.Lock()
			order := append([]string(nil), lifecycle.order...)
			lifecycle.mu.Unlock()
			if !reflect.DeepEqual(order, []string{"issue", "materialize", "writeback", "release"}) {
				t.Fatalf("cancel lifecycle=%v", order)
			}
		})
	}
}

func TestNormalStackMediaFinalizationFailureNeverSucceeds(t *testing.T) {
	for _, name := range []string{"writeback", "release"} {
		t.Run(name, func(t *testing.T) {
			stack, invocation, lifecycle, launcher, _, _, _ := newPreparedStackSessionFixturePolicy(t, false, true, nil, codexexec.NewSyntheticMediaFixtureConfigV1)
			addPreparedStackMedia(t, &invocation)
			if name == "writeback" {
				lifecycle.writeBackErr = errors.New("fixture bounded CAS failure")
			} else {
				lifecycle.releaseErr = errors.New("fixture release failure")
			}
			result, err := stack.Run(context.Background(), invocation)
			if err == nil || result.Succeeded(err) {
				t.Fatalf("failed media finalization became success result=%+v err=%v", result, err)
			}
			if _, candidateErr := stack.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded); candidateErr == nil {
				t.Fatal("failed media finalization minted success")
			}
			if _, retryErr := stack.Run(context.Background(), invocation); retryErr == nil {
				t.Fatal("finalization failure reran media process")
			}
			launcher.mu.Lock()
			calls := launcher.calls
			launcher.mu.Unlock()
			if calls != 1 {
				t.Fatalf("finalization replay launches=%d", calls)
			}
		})
	}
}

func TestNormalStackMediaSyntheticSessionLifecycleReceipt(t *testing.T) {
	stack, invocation, lifecycle, launcher, _, _, _ := newPreparedStackSessionFixturePolicy(t, false, true, nil, codexexec.NewSyntheticMediaFixtureConfigV1)
	addPreparedStackMedia(t, &invocation)
	result, err := stack.Run(context.Background(), invocation)
	if !result.Succeeded(err) {
		t.Fatalf("normal selected synthetic media result=%+v err=%v", result, err)
	}
	lifecycle.mu.Lock()
	order := append([]string(nil), lifecycle.order...)
	lifecycle.mu.Unlock()
	launcher.mu.Lock()
	launch, calls := launcher.last, launcher.calls
	launcher.mu.Unlock()
	if calls != 1 || !reflect.DeepEqual(order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("same original owner lifecycle=%v actual supervisors=%d", order, calls)
	}
	var paths []string
	for i, arg := range launch.Arguments {
		if arg == "--image" {
			paths = append(paths, launch.Arguments[i+1])
		}
	}
	if len(paths) != 2 || filepath.Base(paths[0]) != "image-01.png" || filepath.Base(paths[1]) != "image-02.jpg" || strings.Contains(launch.Directory, "codex-history-") {
		t.Fatalf("original image order=%v cwd=%s", paths, launch.Directory)
	}
	candidate, candidateErr := stack.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded)
	if candidateErr != nil || candidate.Summary != "normal prepared answer" || len(candidate.Artifacts) != 0 {
		t.Fatalf("private finalized answer-only candidate=%+v err=%v", candidate, candidateErr)
	}
	if _, err := stack.Run(context.Background(), invocation); err == nil {
		t.Fatal("normal selected media invocation replayed")
	}
	for _, path := range paths {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("original owned image not cleaned path=%q err=%v", path, err)
		}
	}
}

func TestNormalStackMediaGenuineConnectionAndSelectedDigestDenials(t *testing.T) {
	for _, name := range []string{"unbound", "closed", "owner", "generation", "profile", "body", "text-profile"} {
		t.Run(name, func(t *testing.T) {
			stack, invocation, lifecycle, launcher, session, _, _ := newPreparedStackSessionFixturePolicy(t, false, name != "unbound", nil, codexexec.NewSyntheticMediaFixtureConfigV1)
			addPreparedStackMedia(t, &invocation)
			switch name {
			case "closed":
				if err := session.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "owner":
				invocation.VerifiedInput.AcceptedAttempt.OwnerUserID = "foreign-owner"
			case "generation":
				invocation.VerifiedInput.AcceptedAttempt.ConnectionGeneration++
			case "profile":
				invocation.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("a", 64)
			case "body":
				invocation.VerifiedInput.CanonicalContext.Attachments[1].Body[0] ^= 1
			case "text-profile":
				config := codexexec.Config{Executable: invocation.Process.Executable, ExecutableDigest: invocation.Process.ExecutableDigest, ExecutableVersion: "fixture-v1", Model: "sessionless-media-fixture"}
				adapter, err := codexexec.New(config, &fakeInvocationRunner{})
				if err != nil {
					t.Fatal(err)
				}
				invocation.VerifiedInput.Job.HarnessBinding.Backend = adapter.DescriptorV1()
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil {
				t.Fatal("foreign/changed selected media accepted")
			}
			lifecycle.mu.Lock()
			effects := len(lifecycle.order)
			lifecycle.mu.Unlock()
			launcher.mu.Lock()
			launches := launcher.calls
			launcher.mu.Unlock()
			if effects != 0 || launches != 0 {
				t.Fatalf("denial after effect lifecycle=%d launches=%d", effects, launches)
			}
		})
	}
}
