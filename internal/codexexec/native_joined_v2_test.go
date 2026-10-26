//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Explicit synthetic metadata is rebound before the real test Session receives
// its offer/claim. This helper is not a production evidence issuer or grant.
func nativeJoinedEvidenceV2(t *testing.T, profile *nativeFrozenProfileV2, bundle *domain.ProviderEvidenceBundleV1, binding *domain.HarnessBindingV2, now time.Time) {
	t.Helper()
	scope := domain.ProviderEvidenceScopeV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, Resource: binding.Resource, Backend: profile.descriptor}
	bundle.Catalog.Scope, bundle.Route.Scope, bundle.Capability.Scope = scope, scope, scope
	bundle.Privacy.Scope, bundle.Price.Scope, bundle.Policy.Scope = scope, scope, scope
	bundle.Catalog.Models[0].ModelID = binding.ModelID
	bundle.Capability.ModelID, bundle.Privacy.ModelID, bundle.Price.ModelID = binding.ModelID, binding.ModelID, binding.ModelID
	observed, expires := now.Add(-time.Minute), now.Add(time.Hour)
	bundle.Catalog.ObservedAt, bundle.Route.ObservedAt, bundle.Capability.ObservedAt, bundle.Privacy.ObservedAt, bundle.Price.ObservedAt, bundle.Policy.ObservedAt = observed, observed, observed, observed, observed, observed
	bundle.Catalog.ExpiresAt, bundle.Route.ExpiresAt, bundle.Capability.ExpiresAt, bundle.Privacy.ExpiresAt, bundle.Price.ExpiresAt, bundle.Policy.ExpiresAt = expires, expires, expires, expires, expires, expires
	nativePolicyRebindV2(t, bundle, binding)
	if bundle.ValidateForBinding(*binding) != nil {
		t.Fatal("rebound synthetic original bundle is invalid")
	}
}

type nativeJoinedLifecycleV2 struct {
	*preparedOwnerLifecycle
	now time.Time
}

func (l *nativeJoinedLifecycleV2) Issue(ctx context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	l.order = append(l.order, "issue")
	if ctx.Err() != nil || request.ValidateAt(l.now) != nil || request.ProviderResource != l.handle.ProviderResource {
		return ports.CredentialHandle{}, ErrContract
	}
	return l.handle, nil
}

type nativeJoinedLauncherV3 struct {
	envelopes []attachedworkerdaemon.LaunchEnvelopeV3
	prepare   func(context.Context)
	release   func()
}

func (*nativeJoinedLauncherV3) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: "owned-fake-process-v3", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (l *nativeJoinedLauncherV3) PrepareV3(ctx context.Context, e attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	if e.Execution.ValidateV3(ctx, e.Native) != nil || !e.Socket.MatchesExecutionV3(e.Execution) {
		return nil, ErrContract
	}
	l.envelopes = append(l.envelopes, e)
	if l.prepare != nil {
		l.prepare(ctx)
	}
	cmd := exec.Command(e.Workload.Executable, e.Workload.Arguments...)
	cmd.Dir, cmd.Env = e.Workload.Directory, append([]string(nil), e.Workload.Environment...)
	return &nativeJoinedBoundaryV3{fixtureIsolationBoundary: &fixtureIsolationBoundary{command: cmd, attestation: attachedworkerdaemon.WorkloadAttestation{Executable: e.Workload.Executable, ExecutableDigest: e.Workload.ExecutableDigest, Arguments: append([]string(nil), e.Workload.Arguments...)}}, envelope: e, release: l.release}, nil
}

type nativeJoinedBoundaryV3 struct {
	*fixtureIsolationBoundary
	envelope attachedworkerdaemon.LaunchEnvelopeV3
	release  func()
}

func (b *nativeJoinedBoundaryV3) Release(ctx context.Context) error {
	err := b.fixtureIsolationBoundary.Release(ctx)
	if b.release != nil {
		b.release()
	}
	return err
}

func (b *nativeJoinedBoundaryV3) AttestedWorkloadV3() attachedworkerdaemon.WorkloadAttestationV3 {
	return attachedworkerdaemon.WorkloadAttestationV3{Native: b.AttestedWorkload(), Bundle: b.envelope.Native.Clone(), Execution: b.envelope.Execution, ExecutionDigest: b.envelope.Execution.DigestV3(), Socket: b.envelope.Socket}
}

// Real Session/claim, concrete V3 supervisor and owned shell fixture only.
// No native binary, bootstrap, engine, model or provider connection is executed.
func nativeJoinedFixtureV2(t *testing.T, final string) (*PreparedAttachedWorkerRunnerV1, attachedworkerdaemon.Invocation, *nativeJoinedLifecycleV2, *nativeJoinedLauncherV3, *nativeCredentialMetadataReaderV2) {
	return nativeJoinedFixtureMediaV2(t, final, false)
}

type nativeJoinedOptionsV2 struct {
	setup        func(*attachedworkerdaemon.VerifiedAttachedInputV1)
	park         bool
	inlineOutput bool
}

func nativeJoinedFixtureMediaV2(t *testing.T, final string, media bool, options ...nativeJoinedOptionsV2) (*PreparedAttachedWorkerRunnerV1, attachedworkerdaemon.Invocation, *nativeJoinedLifecycleV2, *nativeJoinedLauncherV3, *nativeCredentialMetadataReaderV2) {
	t.Helper()
	text, _ := json.Marshal(final)
	protocol := "{\"type\":\"thread.started\",\"thread_id\":\"synthetic\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":" + string(text) + "}}\n{\"type\":\"turn.completed\"}\n"
	var configure []func(*attachedworkerdaemon.VerifiedAttachedInputV1)
	park := ""
	inlineOutput := false
	for _, option := range options {
		inlineOutput = inlineOutput || option.inlineOutput
		if option.setup != nil {
			configure = append(configure, option.setup)
		}
		if option.park {
			// Parking an owned workload is not test readiness synchronization.
			// The supervisor must terminate/reap this whole process group.
			park = "trap 'exit 0' TERM\nwhile :; do /bin/sleep 10; done\n"
		}
	}
	executable := writePreparedCodexFixture(t, "#!/bin/sh\nwhile IFS= read -r line; do :; done\n"+park+"printf '%s' '"+strings.ReplaceAll(protocol, "'", "'\\''")+"'\n")
	now := time.Now().UTC() // One sampled clock for this actual fake-process run.
	p := nativeProfileFixtureV2(t)
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	p.Backend.Enabled, p.Backend.Executable, p.Backend.ExecutableDigest, p.Backend.Now = true, executable, digest, func() time.Time { return now }
	p.Launch.Workload = attachedworkerdaemon.NativeArtifactV2{Executable: executable, Digest: digest}
	p.Launch.Companion = nativeHostArtifact(t)
	_, evidence, _, _ := nativePolicyFixtureV2(t)
	metadata := &nativeCredentialMetadataReaderV2{found: true}
	native := NativePreparedConfigV2{Profile: p, Evidence: evidence, CredentialBindings: metadata, InlineOutputV2: inlineOutput}
	f := newNativeConstructorFixtureAtProfileMedia(t, executable, nil, now, &native, media, configure...)
	request, _ := validDriverRequestAt(t, now)
	root := canonicalTempDir(t)
	if err := os.Mkdir(filepath.Join(root, "auth"), 0o700); err != nil {
		t.Fatal(err)
	}
	authRoot := filepath.Join(root, "auth")
	authFile := filepath.Join(authRoot, "auth.json")
	if err := os.WriteFile(authFile, []byte(`{"synthetic":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	credential := request.Credential
	life := &nativeJoinedLifecycleV2{now: now, preparedOwnerLifecycle: &preparedOwnerLifecycle{handle: ports.CredentialHandle{
		HandleID: credential.HandleID, TenantID: credential.TenantID, OwnerUserID: credential.OwnerUserID, SubscriptionConnectionID: domain.SubscriptionConnectionID(credential.ProviderResource.ResourceID),
		RunID: credential.RunID, AttemptID: credential.AttemptID, WorkerID: credential.WorkerID, LeaseID: credential.LeaseID, LeaseFence: credential.LeaseFence, BindingGeneration: credential.ProviderResource.CredentialGeneration, ProviderResource: credential.ProviderResource, ExpiresAt: credential.ExpiresAt}, materialized: ports.CredentialMaterialization{RootDir: authRoot, AuthFile: authFile}}}
	_, binding, _ := nativeCredentialFixtureV2(t)
	binding.TenantID, binding.OwnerUserID, binding.SubscriptionConnectionID, binding.Generation, binding.UpdatedAt = credential.TenantID, credential.OwnerUserID, domain.SubscriptionConnectionID(credential.ProviderResource.ResourceID), credential.ProviderResource.CredentialGeneration, now.Add(-time.Minute)
	metadata.binding = binding
	scratch, err := os.MkdirTemp("", "nj-")
	if err != nil {
		t.Fatal(err)
	}
	scratch, err = filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(scratch); err != nil {
			t.Errorf("owned supervisor root residue: %v", err)
		}
	})
	launcher := &nativeJoinedLauncherV3{}
	sup, err := attachedworkerdaemon.NewSupervisorV3(attachedworkerdaemon.SupervisorConfigV3{Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: scratch, Timeout: 10 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1, AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1}, AllowedReadRoots: []string{root}}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewNativePreparedAttachedWorkerRunnerV2(native, privatePreparedScratch(t), sup, life, attachedworkerdaemon.InvocationRunnerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.BindNativeSessionV2(f.session); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runner.Close(ctx); errors.Is(err, ErrInlineReceiptPendingV2) {
			// A completed inline test without ACK intentionally leaves semantic
			// custody. This is not successful retirement or synthetic ACK cleanup.
			runner.mu.Lock()
			retained := runner.closed && runner.pendingInlineReceiptLockedV2()
			runner.mu.Unlock()
			if !retained {
				t.Error("pending Close did not retain the original closed inline owner")
			}
		} else if err != nil {
			t.Errorf("close owned joined runner: %v", err)
		}
	})
	a := f.config.Authority
	started := f.input.AcceptedAttempt.CreatedAt
	invocation := attachedworkerdaemon.Invocation{Identity: attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID, RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration},
		Process: attachedworkerdaemon.AttemptSpec{Executable: executable, ExecutableDigest: digest, Arguments: f.config.Arguments}, VerifiedInput: f.input,
		Credential: &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: CredentialHomeEnvironmentV1, ExpectedBindingGeneration: credential.ProviderResource.CredentialGeneration, IssueRequest: ports.CredentialIssueRequest{OwnerUserID: a.OwnerUserID, ProviderResource: a.ProviderResource, ExpiresAt: credential.ExpiresAt,
			Run:     domain.Run{ID: a.RunID, TenantID: a.TenantID, SessionID: f.input.Job.SessionID, TriggerEventID: f.input.Job.TriggerEventID, SubscriptionConnectionID: life.handle.SubscriptionConnectionID, Status: domain.RunRunning, IdempotencyKey: "native-joined-run", StartedAt: &started, CreatedAt: started, UpdatedAt: started},
			Attempt: domain.Attempt{ID: a.AttemptID, TenantID: a.TenantID, RunID: a.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(a.WorkerID), CreatedAt: started, UpdatedAt: started},
			Lease:   domain.Lease{ID: a.LeaseID, TenantID: a.TenantID, RunID: a.RunID, AttemptID: a.AttemptID, WorkerID: string(a.WorkerID), FenceToken: a.LeaseGeneration, AcquiredAt: started, ExpiresAt: a.LeaseExpiresAt}}}}
	return runner, invocation, life, launcher, metadata
}

func TestNativeJoinedV2OriginalOwnerAnswerReceiptAndNoReplay(t *testing.T) {
	r, invocation, life, launcher, _ := nativeJoinedFixtureV2(t, `{"answer":"useful native answer","artifacts":[]}`)
	result, err := r.Run(context.Background(), invocation)
	if !result.Succeeded(err) || len(launcher.envelopes) != 1 {
		t.Fatalf("joined result=%+v err=%v launches=%d lifecycle=%v", result, err, len(launcher.envelopes), life.order)
	}
	if !reflect.DeepEqual(life.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("sole lifecycle=%v", life.order)
	}
	candidate, err := r.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded)
	if err != nil || candidate.Summary != "useful native answer" || len(candidate.Artifacts) != 0 {
		t.Fatalf("typed original candidate=%+v err=%v", candidate, err)
	}
	native := r.slot.driver.boundary.(*PreparedProcessBoundaryV1).native
	if !native.stageV2.dispatched || !native.stageV2.quiesced || native.executionV3 != launcher.envelopes[0].Execution {
		t.Fatal("original consumed readset/custody was not joined")
	}
	if r.slot.original.process.Stdout != nil || r.slot.semantic.ProviderEvidence.RouteState != domain.ProviderEvidenceUnknownV1 {
		t.Fatal("native raw frames or invented direct route escaped")
	}
	if _, err := r.Run(context.Background(), invocation); err == nil || len(launcher.envelopes) != 1 || len(life.order) != 4 {
		t.Fatal("native original invocation replayed")
	}
	if err := r.RetireReceipt(invocation.Identity); err != nil {
		t.Fatal(err)
	}
}

func TestNativeJoinedV2RejectsMetadataAndCopiesBeforeIssue(t *testing.T) {
	for _, kind := range []string{"revoked", "copied-owner", "changed-argv", "changed-policy"} {
		t.Run(kind, func(t *testing.T) {
			r, invocation, life, launcher, metadata := nativeJoinedFixtureV2(t, `{"answer":"not run","artifacts":[]}`)
			switch kind {
			case "revoked":
				metadata.binding.Revoked = true
			case "copied-owner":
				// A foreign wrapper cannot reuse the original owner identity;
				// do not copy the live owner's mutex to exercise that guard.
				r = &PreparedAttachedWorkerRunnerV1{nativeIdentityV2: r, nativeConfigV2: r.nativeConfigV2}
			case "changed-argv":
				invocation.Process.Arguments = append(invocation.Process.Arguments, "--unknown")
			case "changed-policy":
				r.nativeConfigV2.Evidence.Policy.PolicyID = "other-policy"
			}
			if _, err := r.Run(context.Background(), invocation); err == nil || len(life.order) != 0 || len(launcher.envelopes) != 0 {
				t.Fatalf("invalid preflight effects: err=%v order=%v launches=%d", err, life.order, len(launcher.envelopes))
			}
		})
	}
}

func TestNativeJoinedV2HistoricalTextJSONAndOriginalResizeMedia(t *testing.T) {
	r, invocation, life, launcher, _ := nativeJoinedFixtureMediaV2(t, `{"answer":"synthetic media plumbing","artifacts":[]}`, true)
	result, err := r.Run(context.Background(), invocation)
	if !result.Succeeded(err) || len(launcher.envelopes) != 1 || len(life.order) != 4 {
		t.Fatalf("media join failed: %+v %v", result, err)
	}
	e := launcher.envelopes[0]
	d := e.Execution.DescriptorV3()
	if d.Variant != 4 || len(d.Images) != 2 || len(e.Execution.MappingsV3()) != 4 {
		t.Fatalf("original PNG/JPEG order lost: %+v", d)
	}
	want := []string{"/sessionless/input/image-01.png", "/sessionless/input/image-02.jpg"}
	var actual []string
	for i, arg := range e.Workload.Arguments {
		if arg == "--image" {
			actual = append(actual, e.Workload.Arguments[i+1])
		}
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("selected fixed native image argv=%v", actual)
	}
	// The copied manifest descriptor retains original digests after owned
	// files are removed. This proves composition, not actual native vision.
	var images [][]byte
	for _, source := range invocation.VerifiedInput.CanonicalContext.Attachments {
		if source.MediaType == "image/png" || source.MediaType == "image/jpeg" {
			images = append(images, source.Body)
		}
	}
	for i, image := range images {
		digest := sha256.Sum256(image)
		if d.Images[i].SHA256 != hex.EncodeToString(digest[:]) || d.Images[i].Size != uint64(len(image)) {
			t.Fatalf("original image%d changed", i)
		}
	}
	if candidate, err := r.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded); err != nil || candidate.Summary != "synthetic media plumbing" {
		t.Fatalf("joined media receipt=%+v %v", candidate, err)
	}
}

func TestNativeJoinedV2StructuredArtifactsAreNotSilentlyDropped(t *testing.T) {
	r, invocation, life, launcher, _ := nativeJoinedFixtureV2(t, `{"answer":"two requested files","artifacts":[{"name":"data.json","media_type":"application/json","content":" { \"n\": 9007199254740993123456789 } "},{"name":"note.txt","media_type":"text/plain","content":"original text\n"}]}`)
	result, err := r.Run(context.Background(), invocation)
	if err == nil || result.Succeeded(err) || len(launcher.envelopes) != 1 || len(life.order) != 4 || r.slot == nil || r.slot.original == nil || r.slot.original.nativeFinalV2 == nil {
		t.Fatalf("typed export failure/retention missing: %+v %v", result, err)
	}
	f := r.slot.original.nativeFinalV2
	if len(f.artifacts) != 2 || !bytes.Equal(f.artifacts[0].content, []byte(` { "n": 9007199254740993123456789 } `)) || string(f.artifacts[1].content) != "original text\n" {
		t.Fatal("provider-authored artifact bytes changed")
	}
	if _, candidateErr := r.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded); candidateErr == nil {
		t.Fatal("unuploaded artifact output became answer-only success")
	}
	if closeErr := r.Close(context.Background()); closeErr != nil {
		t.Fatal(closeErr)
	}
	if len(f.artifacts) != 0 || f.answer != "" {
		t.Fatal("retired private semantic bytes retained")
	}
}

func TestNativeJoinedV2RejectsStructuredFinalSchemaDrift(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"duplicate-field", `{"answer":"a","Answer":"b","artifacts":[]}`},
		{"wrong-case", `{"Answer":"a","artifacts":[]}`},
		{"unknown-field", `{"answer":"a","artifacts":[],"extra":true}`},
		{"missing-required-list", `{"answer":"a"}`},
		{"null-list", `{"answer":"a","artifacts":null}`},
		{"artifact-path", `{"answer":"a","artifacts":[{"name":"../auth.json","media_type":"text/plain","content":"x"}]}`},
		{"case-fold-collision", `{"answer":"a","artifacts":[{"name":"a.txt","media_type":"text/plain","content":"x"},{"name":"A.TXT","media_type":"text/plain","content":"y"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, invocation, _, _, _ := nativeJoinedFixtureV2(t, tc.raw)
			result, err := r.Run(context.Background(), invocation)
			if err == nil || result.Succeeded(err) {
				t.Fatal("invalid structured final accepted")
			}
			if _, candidateErr := r.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded); candidateErr == nil {
				t.Fatal("invalid structured final reached success receipt")
			}
		})
	}
}

func TestNativeJoinedV2MaximumValidStructuredOutput(t *testing.T) {
	answer := strings.Repeat("界", 10_922) + "ab" // Exactly 32 KiB, below 32000 runes.
	content := strings.Repeat("x", 32<<10)
	body, err := json.Marshal(map[string]any{"answer": answer, "artifacts": []map[string]string{
		{"name": "first.txt", "media_type": "text/plain", "content": content},
		{"name": "second.txt", "media_type": "text/plain", "content": content},
	}})
	if err != nil || len(body) > 128<<10 {
		t.Fatal("invalid maximum fixture", err)
	}
	// Exact encoded-final boundary, without changing any semantic byte.
	body = append(body[:len(body)-1], append(bytes.Repeat([]byte(" "), (128<<10)-len(body)), '}')...)
	r, invocation, life, launcher, _ := nativeJoinedFixtureV2(t, string(body))
	result, err := r.Run(t.Context(), invocation)
	if err == nil || result.Succeeded(err) || len(life.order) != 4 || len(launcher.envelopes) != 1 || r.slot == nil || r.slot.original == nil || r.slot.original.nativeFinalV2 == nil {
		t.Fatalf("maximum valid semantic result missing (upload remains unavailable): result=%+v err=%v", result, err)
	}
	f := r.slot.original.nativeFinalV2
	if f.answer != answer || len(f.artifacts) != 2 || string(f.artifacts[0].content) != content || string(f.artifacts[1].content) != content {
		t.Fatal("maximum semantic bytes changed")
	}
}

func TestNativeJoinedV2AdmittedRuntimeDoesNotResetAfterPreparation(t *testing.T) {
	r, invocation, life, launcher, _ := nativeJoinedFixtureMediaV2(t, `{"answer":"not completed","artifacts":[]}`, false,
		nativeJoinedOptionsV2{park: true, setup: func(input *attachedworkerdaemon.VerifiedAttachedInputV1) {
			input.Job.Limits.MaxRuntime = 5 * time.Second
			input.AcceptedAttempt.CreatedAt = input.AcceptedAttempt.CreatedAt.Add(-time.Second)
		}})
	want := invocation.VerifiedInput.AcceptedAttempt.CreatedAt.Add(invocation.VerifiedInput.Job.Limits.MaxRuntime)
	var actual time.Time
	launcher.prepare = func(ctx context.Context) { actual, _ = ctx.Deadline() }
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	result, err := r.Run(ctx, invocation)
	if !actual.Equal(want) || !result.Process.Deadline || !result.CleanupComplete() || result.Succeeded(err) || err == nil || len(launcher.envelopes) != 1 {
		t.Fatalf("admitted runtime not enforced: deadline=%v want=%v result=%+v err=%v", actual, want, result, err)
	}
	if !reflect.DeepEqual(life.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("runtime cancellation lost independent finalization: %v", life.order)
	}
	if _, err := r.Run(ctx, invocation); err == nil || len(launcher.envelopes) != 1 || len(life.order) != 4 {
		t.Fatal("expired native job replayed")
	}
}

func TestNativeJoinedV2LateEligibilityLossCannotPublishSuccess(t *testing.T) {
	for _, state := range []domain.EntitlementState{domain.EntitlementInactive, domain.EntitlementReauthRequired} {
		t.Run(string(state), func(t *testing.T) {
			r, invocation, life, launcher, metadata := nativeJoinedFixtureV2(t, `{"answer":"must not publish","artifacts":[]}`)
			entered, proceed := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			launcher.release = func() {
				close(entered)
				select {
				case <-proceed:
				case <-ctx.Done():
				}
			}
			type completed struct {
				result attachedworkerdaemon.InvocationResult
				err    error
			}
			done := make(chan completed, 1)
			go func() { result, err := r.Run(ctx, invocation); done <- completed{result, err} }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("original release barrier not reached", ctx.Err())
			}
			metadata.binding.Entitlement = state
			if metadata.binding.Validate() != nil {
				t.Fatal("late eligibility fixture is not structurally valid")
			}
			close(proceed)
			var got completed
			select {
			case got = <-done:
			case <-ctx.Done():
				t.Fatal("original Run did not join", ctx.Err())
			}
			if got.err == nil || got.result.Succeeded(got.err) || !got.result.CleanupComplete() || len(life.order) != 4 {
				t.Fatalf("lost eligibility published success: %+v err=%v lifecycle=%v", got.result, got.err, life.order)
			}
			if _, err := r.ReceiptCandidate(invocation.Identity, got.result, got.err, domain.AttachedWorkerTerminalSucceeded); err == nil {
				t.Fatal("lost eligibility acquired success candidate")
			}
			if r.slot == nil || r.slot.semantic.ProviderEvidence == nil || r.slot.semantic.ProviderEvidence.AcceptanceClass != domain.ProviderAcceptanceAcceptedV1 {
				t.Fatal("late eligibility loss erased observed provider acceptance")
			}
			if _, err := r.Run(ctx, invocation); err == nil || len(launcher.envelopes) != 1 || len(life.order) != 4 {
				t.Fatal("late eligibility loss reopened consumed invocation")
			}
		})
	}
}

func TestNativeJoinedV2ExpiredAdmittedRuntimeDeniesBeforeIssue(t *testing.T) {
	r, invocation, life, launcher, _ := nativeJoinedFixtureMediaV2(t, `{"answer":"not started","artifacts":[]}`, false,
		nativeJoinedOptionsV2{setup: func(input *attachedworkerdaemon.VerifiedAttachedInputV1) {
			input.AcceptedAttempt.CreatedAt = input.AcceptedAttempt.CreatedAt.Add(-2 * time.Minute)
			input.Job.Limits.MaxRuntime = time.Minute
		}})
	if _, err := r.Run(t.Context(), invocation); err == nil || len(life.order) != 0 || len(launcher.envelopes) != 0 {
		t.Fatalf("expired admitted runtime started effects: err=%v lifecycle=%v launches=%d", err, life.order, len(launcher.envelopes))
	}
}

func TestNativeJoinedV2StructuredOutputDesignBounds(t *testing.T) {
	encode := func(answer, content string) string {
		value := map[string]any{"answer": answer, "artifacts": []map[string]string{}}
		if content != "" {
			value["artifacts"] = []map[string]string{{"name": "data.json", "media_type": "application/json", "content": content}}
		}
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	for _, tc := range []struct{ name, raw string }{
		{"answer-utf8-bytes", encode(strings.Repeat("界", 11_000), "")},
		{"artifact-bytes", encode("bounded answer", `"`+strings.Repeat("x", 32<<10)+`"`)},
		{"artifact-depth", encode("bounded answer", strings.Repeat("[", 33)+"0"+strings.Repeat("]", 33))},
		{"encoded-final", `{"answer":"bounded answer","artifacts":[]` + strings.Repeat(" ", 128<<10) + `}`},
		{"answer-lone-surrogate", `{"answer":"\ud800","artifacts":[]}`},
		{"artifact-lone-surrogate", encode("bounded answer", `"\ud800"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, invocation, _, _, _ := nativeJoinedFixtureV2(t, tc.raw)
			result, err := r.Run(t.Context(), invocation)
			if err == nil || result.Succeeded(err) || r.slot == nil || r.slot.original == nil {
				t.Fatalf("invalid bounded result: result=%+v err=%v", result, err)
			}
			if r.slot.original.nativeFinalV2 != nil {
				t.Fatal("over-limit native final acquired private producer identity")
			}
		})
	}
}
