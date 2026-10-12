//go:build darwin || linux

package attachedworkerstack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type preparedStackLifecycle struct {
	mu                                  sync.Mutex
	order                               []string
	root                                string
	writeBackEntered, continueWriteBack chan struct{}
	writeBackErr, releaseErr            error
}

func (owner *preparedStackLifecycle) record(name string) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	owner.order = append(owner.order, name)
}
func (owner *preparedStackLifecycle) Issue(_ context.Context, r ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	owner.record("issue")
	return ports.CredentialHandle{HandleID: "prepared-stack-handle", TenantID: r.Run.TenantID, OwnerUserID: r.OwnerUserID,
		SubscriptionConnectionID: r.Run.SubscriptionConnectionID, RunID: r.Run.ID, AttemptID: r.Attempt.ID, WorkerID: r.Lease.WorkerID,
		LeaseID: r.Lease.ID, LeaseFence: r.Lease.FenceToken, BindingGeneration: r.ProviderResource.CredentialGeneration, ProviderResource: r.ProviderResource, ExpiresAt: r.ExpiresAt}, nil
}
func (owner *preparedStackLifecycle) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	owner.record("materialize")
	return ports.CredentialMaterialization{RootDir: owner.root, AuthFile: filepath.Join(owner.root, "auth.json")}, nil
}
func (owner *preparedStackLifecycle) WriteBack(ctx context.Context, h ports.CredentialHandle, m ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	owner.record("writeback")
	if owner.writeBackEntered != nil {
		close(owner.writeBackEntered)
		select {
		case <-owner.continueWriteBack:
		case <-ctx.Done():
			return ports.CredentialWriteBackResult{}, ctx.Err()
		}
	}
	body, err := os.ReadFile(m.AuthFile)
	if err != nil || string(body) != `{"rotated":true}` {
		return ports.CredentialWriteBackResult{}, errors.New("fixture actual credential rotation absent")
	}
	return ports.CredentialWriteBackResult{Changed: true, Generation: h.BindingGeneration + 1}, owner.writeBackErr
}
func (owner *preparedStackLifecycle) Release(context.Context, ports.CredentialHandle) error {
	owner.record("release")
	return owner.releaseErr
}
func (*preparedStackLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return errors.New("unexpected fixture connection revoke")
}

type preparedStackLauncher struct {
	mu                                sync.Mutex
	calls                             int
	last                              attachedworkerdaemon.LaunchSpec
	teardownEntered, continueTeardown chan struct{}
}

func (*preparedStackLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{Name: "prepared-stack-fixture", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (*preparedStackLauncher) Reconcile(context.Context) (int, error) { return 0, nil }
func (launcher *preparedStackLauncher) Prepare(_ context.Context, spec attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	launcher.mu.Lock()
	launcher.calls++
	launcher.last = spec
	launcher.mu.Unlock()
	command := exec.Command(spec.Executable, spec.Arguments...)
	command.Dir = spec.Directory
	command.Env = spec.Environment
	return &preparedStackBoundary{command: command, spec: spec, owner: launcher}, nil
}

type preparedStackBoundary struct {
	command *exec.Cmd
	spec    attachedworkerdaemon.LaunchSpec
	owner   *preparedStackLauncher
}

func (b *preparedStackBoundary) Command() *exec.Cmd { return b.command }
func (b *preparedStackBoundary) AttestedWorkload() attachedworkerdaemon.WorkloadAttestation {
	return attachedworkerdaemon.WorkloadAttestation{Executable: b.spec.Executable, ExecutableDigest: b.spec.ExecutableDigest, Arguments: b.spec.Arguments}
}
func (*preparedStackBoundary) GracefulStop(context.Context) error  { return nil }
func (*preparedStackBoundary) ForceStop(context.Context) error     { return nil }
func (*preparedStackBoundary) Alive(context.Context) (bool, error) { return false, nil }
func (b *preparedStackBoundary) Release(ctx context.Context) error {
	if b.owner.teardownEntered != nil {
		close(b.owner.teardownEntered)
		select {
		case <-b.owner.continueTeardown:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func newPreparedStackFixture(t *testing.T) (*Stack, attachedworkerdaemon.Invocation, *preparedStackLifecycle, *preparedStackLauncher) {
	t.Helper()
	return newPreparedStackFixtureMode(t, false)
}
func newPreparedStackFixtureMode(t *testing.T, holdTeardown bool) (*Stack, attachedworkerdaemon.Invocation, *preparedStackLifecycle, *preparedStackLauncher) {
	stack, invocation, owner, launcher, _, _, _ := newPreparedStackSessionFixture(t, holdTeardown, true, nil)
	return stack, invocation, owner, launcher
}
func newPreparedStackSessionFixture(t *testing.T, holdTeardown, bind bool, clock func() time.Time) (*Stack, attachedworkerdaemon.Invocation, *preparedStackLifecycle, *preparedStackLauncher, *attachedworkersession.Session, *attachedworkerlocal.Store, *preparedSessionPeer) {
	t.Helper()
	return newPreparedStackSessionFixturePolicy(t, holdTeardown, bind, clock, nil)
}

func newPreparedStackSessionFixturePolicy(t *testing.T, holdTeardown, bind bool, clock func() time.Time, selectMedia func(codexexec.Config) (codexexec.MediaConfigV1, error)) (*Stack, attachedworkerdaemon.Invocation, *preparedStackLifecycle, *preparedStackLauncher, *attachedworkersession.Session, *attachedworkerlocal.Store, *preparedSessionPeer) {
	t.Helper()
	fixture := newFixture(t)
	fixture.manifest.ConnectionGeneration = 0
	script := `#!/bin/sh
[ -f "$CODEX_HOME/auth.json" ] || exit 81
[ "$HOME" != "$CODEX_HOME" ] || exit 82
[ -z "$OPENAI_API_KEY" ] || exit 83
while IFS= read -r line; do :; done
printf '%s' '{"rotated":true}' >"$CODEX_HOME/auth.json"
printf '%s\n' '{"type":"thread.started","thread_id":"private"}' '{"type":"turn.started"}' '{"type":"item.completed","item":{"type":"agent_message","text":"normal prepared answer"}}' '{"type":"turn.completed"}'
`
	if err := os.WriteFile(fixture.manifest.Harness.Executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(fixture.manifest.Harness.Executable)
	if err != nil {
		t.Fatal(err)
	}
	fixture.manifest.Harness.SHA256 = hex.EncodeToString(digest[:])
	fixture.manifest.Harness.Arguments = []string{"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--strict-config", "--sandbox", "read-only", "--skip-git-repo-check", "--color", "never", "--model", "gpt-fixture", "-"}
	now := time.Now().UTC()
	if clock != nil {
		now = clock()
	}
	expires := now.Add(time.Hour)
	config := codexexec.Config{Enabled: true, Executable: fixture.manifest.Harness.Executable, ExecutableDigest: digest, ExecutableVersion: "fixture-v1", Model: "gpt-fixture", Now: func() time.Time { return now }}
	if clock != nil {
		config.Now = clock
	}
	fixture.config.PreparedCodex = &config
	var media *codexexec.MediaConfigV1
	if selectMedia != nil {
		config.Model = "sessionless-media-fixture"
		selected, err := selectMedia(config)
		if err != nil {
			t.Fatal(err)
		}
		media = &selected
		fixture.config.PreparedCodex = nil
		fixture.config.PreparedCodexMedia = media
		fixture.manifest.Harness.Arguments[len(fixture.manifest.Harness.Arguments)-2] = config.Model
	}
	fixture.config.Timeout = 20 * time.Second
	fixture.config.TerminationGrace = time.Second
	fixture.config.MaxStdoutBytes = 4 << 20
	fixture.config.MaxStderrBytes = 1 << 20
	credentialRoot := filepath.Join(filepath.Dir(fixture.scratch), "credential")
	if err := os.Mkdir(credentialRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credentialRoot, "auth.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.config.AllowedReadRoots = []string{filepath.Dir(credentialRoot)}
	owner := &preparedStackLifecycle{root: credentialRoot}
	launcher := &preparedStackLauncher{}
	if holdTeardown {
		launcher.teardownEntered, launcher.continueTeardown = make(chan struct{}), make(chan struct{})
	}
	deps := fixture.dependencies(nil)
	deps.newLauncher = func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error) { return launcher, nil }
	deps.newSupervisor = func(c attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error) {
		return attachedworkerdaemon.NewSupervisor(c)
	}
	var stack *Stack
	session, store, peer := connectPreparedFixture(t, fixture.manifest, now, func(ctx context.Context, manifest attachedworkerlocal.ManifestV1) error {
		var err error
		stack, err = newStack(ctx, manifest, fixture.config, owner, deps)
		return err
	})
	if bind {
		if err := stack.BindPreparedConnection(session); err != nil {
			t.Fatal(err)
		}
	}
	connection := session.Snapshot()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := stack.Close(ctx); err != nil {
			t.Errorf("prepared Stack.Close: %v", err)
		}
	})
	// Canonical unit admission fixture; not a production sealed-input issuer.
	i := attachedworkerdaemon.InvocationIdentity{TenantID: fixture.manifest.TenantID, OwnerUserID: fixture.manifest.OwnerUserID, WorkerID: fixture.manifest.WorkerID, RunID: "prepared-run", AttemptID: "prepared-attempt", LeaseID: "prepared-lease", FenceToken: 7}
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "prepared-subscription", OwnerUserID: i.OwnerUserID, Revision: 3, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 7}
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID, CapabilityDigest: connection.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	placementDigest, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := codexexec.New(config, &fakeInvocationRunner{})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := adapter.DescriptorV1()
	if media != nil {
		descriptor, err = media.DescriptorV1()
		if err != nil {
			t.Fatal(err)
		}
	}
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, RunID: i.RunID, AttemptID: i.AttemptID, Backend: descriptor, Resource: resource, ModelVendorID: codexexec.ModelVendorIDV1, ModelID: config.Model, InputDataClass: domain.ProviderDataPrivateV1, ProviderCatalogDigest: strings.Repeat("1", 64), ProviderRouteDigest: strings.Repeat("2", 64), PrivacyPolicyDigest: strings.Repeat("3", 64), CapabilityEvidenceDigest: strings.Repeat("4", 64), EffectivePolicyDigest: strings.Repeat("5", 64), ExecutionPlacementDigest: string(placementDigest), EvidenceExpiresAt: &expires}
	job := domain.WorkerJob{TenantID: i.TenantID, RunID: i.RunID, SessionID: "prepared-session", TriggerEventID: "prepared-trigger", AttemptID: i.AttemptID, ReservationID: "prepared-reservation", InputManifestID: "prepared-manifest", ContextWindow: &domain.SessionContextWindow{ThroughSequence: 1}, CredentialOwnerUserID: i.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: binding, Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Hour, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 10, MaxToolEvents: 10, MaxToolEventBytes: 4096}}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: i.TenantID, RunID: i.RunID, CreatedAt: now}
	payload := []byte(`{"version":1,"text":"public unit input"}`)
	payloadDigest := sha256.Sum256(payload)
	event := domain.SessionEvent{ID: job.TriggerEventID, TenantID: i.TenantID, SessionID: job.SessionID, Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &i.OwnerUserID, IdempotencyKey: "prepared-trigger-key", Payload: domain.BlobRef{TenantID: i.TenantID, Key: domain.SessionEventObjectPrefix(i.TenantID, job.SessionID, job.TriggerEventID) + "message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(payloadDigest[:])}, CreatedAt: now}
	proof := &sessioncontext.CanonicalProof{Input: domain.SessionContextInput{TenantID: i.TenantID, SessionID: job.SessionID, Events: []domain.SessionEvent{event}}, EventBodies: [][]byte{payload}}
	history, _, err := sessioncontext.ProjectCanonical(job, proof, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(i.TenantID, i.OwnerUserID, i.WorkerID, i.RunID, i.AttemptID, i.LeaseID, i.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	head := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: i.TenantID, OwnerUserID: i.OwnerUserID, WorkerID: i.WorkerID, ConnectionID: connection.ConnectionID, EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration, RunID: i.RunID, AttemptID: i.AttemptID, ReservationID: job.ReservationID, LeaseID: i.LeaseID, LeaseGeneration: i.FenceToken, FenceToken: fence, ContextDigest: contextDigest, CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest, State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1, Revision: 1, CreatedAt: now, UpdatedAt: now, LeaseExpiresAt: expires}
	started := now
	credential := &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: codexexec.CredentialHomeEnvironmentV1, ExpectedBindingGeneration: resource.CredentialGeneration, IssueRequest: ports.CredentialIssueRequest{OwnerUserID: i.OwnerUserID, ProviderResource: resource, ExpiresAt: expires,
		Run:     domain.Run{ID: i.RunID, TenantID: i.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID, SubscriptionConnectionID: domain.SubscriptionConnectionID(resource.ResourceID), Status: domain.RunRunning, IdempotencyKey: "prepared-run-key", StartedAt: &started, CreatedAt: now, UpdatedAt: now},
		Attempt: domain.Attempt{ID: i.AttemptID, TenantID: i.TenantID, RunID: i.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(i.WorkerID), CreatedAt: now, UpdatedAt: now}, Lease: domain.Lease{ID: i.LeaseID, TenantID: i.TenantID, RunID: i.RunID, AttemptID: i.AttemptID, WorkerID: string(i.WorkerID), FenceToken: i.FenceToken, AcquiredAt: now, ExpiresAt: expires}}}
	input := &attachedworkerdaemon.VerifiedAttachedInputV1{AcceptedAttempt: head, Job: job, Manifest: manifest, Context: history, CanonicalContext: proof}
	t.Cleanup(input.Clear)
	invocation := attachedworkerdaemon.Invocation{Identity: i, Process: attachedworkerdaemon.AttemptSpec{Executable: config.Executable, ExecutableDigest: digest, Arguments: fixture.manifest.Harness.Arguments, Stdin: []byte("NOT AUTHORITY; MUST NOT BECOME PROMPT")}, Credential: credential, VerifiedInput: input}
	if input.ValidateFor(i) != nil || invocation.Validate() != nil {
		t.Fatal("invalid admitted unit fixture")
	}
	return stack, invocation, owner, launcher, session, store, peer
}

func TestNormalStackMediaProductionUnknownGuardDeniesBeforeIssue(t *testing.T) {
	stack, invocation, lifecycle, launcher, _, _, _ := newPreparedStackSessionFixturePolicy(t, false, true, nil, func(config codexexec.Config) (codexexec.MediaConfigV1, error) {
		return codexexec.MediaConfigV1{Backend: config}, nil
	})
	result, err := stack.Run(context.Background(), invocation)
	if !errors.Is(err, codexexec.ErrMediaNativeGuardUnavailable) || result.Succeeded(err) {
		t.Fatalf("normal genuine Session unknown native guard result=%+v err=%v", result, err)
	}
	lifecycle.mu.Lock()
	effects := len(lifecycle.order)
	lifecycle.mu.Unlock()
	launcher.mu.Lock()
	launches := launcher.calls
	launcher.mu.Unlock()
	if effects != 0 || launches != 0 {
		t.Fatalf("unknown native guard lifecycle=%d launches=%d", effects, launches)
	}
}

func TestNormalStackPreparedCodexHoldsCandidateUntilOuterFinalization(t *testing.T) {
	stack, invocation, owner, launcher := newPreparedStackFixture(t)
	owner.writeBackEntered = make(chan struct{})
	owner.continueWriteBack = make(chan struct{})
	defer func() {
		select {
		case <-owner.continueWriteBack:
		default:
			close(owner.continueWriteBack)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type outcome struct {
		result attachedworkerdaemon.InvocationResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() { r, e := stack.Run(ctx, invocation); done <- outcome{r, e} }()
	select {
	case <-owner.writeBackEntered:
	case <-ctx.Done():
		t.Fatal("actual process did not reach WriteBack")
	}
	if _, err := stack.ReceiptCandidate(invocation.Identity, attachedworkerdaemon.InvocationResult{}, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("candidate escaped held finalization")
	}
	close(owner.continueWriteBack)
	var got outcome
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("outer finalization did not return")
	}
	if !got.result.Succeeded(got.err) {
		t.Fatalf("normal selected Stack result=%+v err=%v", got.result, got.err)
	}
	for n := 0; n < 3; n++ {
		candidate, err := stack.ReceiptCandidate(invocation.Identity, got.result, got.err, domain.AttachedWorkerTerminalSucceeded)
		if err != nil || candidate.Summary != "normal prepared answer" {
			t.Fatalf("same candidate[%d]=%+v err=%v", n, candidate, err)
		}
	}
	owner.mu.Lock()
	order := append([]string(nil), owner.order...)
	owner.mu.Unlock()
	launcher.mu.Lock()
	calls, last := launcher.calls, launcher.last
	launcher.mu.Unlock()
	if !reflect.DeepEqual(order, []string{"issue", "materialize", "writeback", "release"}) || calls != 1 {
		t.Fatalf("actual lifecycle=%v preparations=%d", order, calls)
	}
	if filepath.Base(last.Directory) != "work" || !strings.Contains(last.Directory, "attempt-") {
		t.Fatalf("supervisor working directory replaced: %q", last.Directory)
	}
	if _, err := stack.Run(context.Background(), invocation); err == nil {
		t.Fatal("repeat Stack invocation accepted")
	}
	if err := stack.RetireReceipt(invocation.Identity); err != nil {
		t.Fatal(err)
	}
	if _, err := stack.ReceiptCandidate(invocation.Identity, got.result, got.err, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("retired candidate reused")
	}
}

func TestNormalStackPreparedCodexRejectsForeignHeadAndPinsBeforeIssue(t *testing.T) {
	for _, name := range []string{"owner", "generation", "argv", "digest"} {
		t.Run(name, func(t *testing.T) {
			stack, invocation, owner, launcher := newPreparedStackFixture(t)
			switch name {
			case "owner":
				invocation.VerifiedInput.AcceptedAttempt.OwnerUserID = "foreign-owner"
			case "generation":
				invocation.VerifiedInput.AcceptedAttempt.ConnectionGeneration++
			case "argv":
				invocation.Process.Arguments = append([]string(nil), invocation.Process.Arguments...)
				invocation.Process.Arguments[0] = "wrong"
			case "digest":
				invocation.Process.ExecutableDigest[0] ^= 1
			}
			if _, err := stack.Run(context.Background(), invocation); err == nil {
				t.Fatal("foreign head/pins accepted")
			}
			if len(owner.order) != 0 || launcher.calls != 0 {
				t.Fatalf("foreign effects lifecycle=%v prepares=%d", owner.order, launcher.calls)
			}
		})
	}
}

func TestNormalStackPreparedCodexContextCancelArbitration(t *testing.T) {
	for _, cancelWins := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel-wins=%t", cancelWins), func(t *testing.T) {
			stack, invocation, owner, launcher := newPreparedStackFixtureMode(t, cancelWins)
			if !cancelWins {
				owner.writeBackEntered, owner.continueWriteBack = make(chan struct{}), make(chan struct{})
			}
			defer func() {
				var gate chan struct{}
				if cancelWins {
					gate = launcher.continueTeardown
				} else {
					gate = owner.continueWriteBack
				}
				select {
				case <-gate:
				default:
					close(gate)
				}
			}()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type outcome struct {
				result attachedworkerdaemon.InvocationResult
				err    error
			}
			done := make(chan outcome, 1)
			go func() { result, err := stack.Run(ctx, invocation); done <- outcome{result, err} }()
			bound, finish := context.WithTimeout(context.Background(), 10*time.Second)
			defer finish()
			var entered, proceed chan struct{}
			if cancelWins {
				entered, proceed = launcher.teardownEntered, launcher.continueTeardown
			} else {
				entered, proceed = owner.writeBackEntered, owner.continueWriteBack
			}
			select {
			case <-entered:
			case <-bound.Done():
				t.Fatal("real process did not reach causal gate")
			}
			cancel()
			close(proceed)
			var got outcome
			select {
			case got = <-done:
			case <-bound.Done():
				t.Fatal("real Stack did not finalize")
			}
			if got.result.Process.Cancelled != cancelWins || !got.result.CleanupComplete() {
				t.Fatalf("original final observation=%+v err=%v", got.result, got.err)
			}
			status := domain.AttachedWorkerTerminalSucceeded
			if cancelWins {
				status = domain.AttachedWorkerTerminalCancelled
			}
			candidate, err := stack.ReceiptCandidate(invocation.Identity, got.result, got.err, status)
			if err != nil || candidate.Status != status {
				t.Fatalf("final-arbitrated candidate=%+v err=%v", candidate, err)
			}
			if cancelWins && got.err == nil || !cancelWins && !got.result.Succeeded(got.err) {
				t.Fatalf("causal winner result=%+v err=%v", got.result, got.err)
			}
			if launcher.calls != 1 || !reflect.DeepEqual(owner.order, []string{"issue", "materialize", "writeback", "release"}) {
				t.Fatalf("physical repeats=%d lifecycle=%v", launcher.calls, owner.order)
			}
		})
	}
}
