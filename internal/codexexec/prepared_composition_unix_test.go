//go:build darwin || linux

package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func preparedCompositionFixture(t *testing.T, protocol string) (*PreparedAttachedWorkerRunnerV1, attachedworkerdaemon.Invocation, *preparedOwnerLifecycle, *fixtureIsolationLauncher) {
	t.Helper()
	executable := writePreparedCodexFixture(t, "#!/bin/sh\nwhile IFS= read -r line; do :; done\nprintf '%s' '{\"rotated\":true}' >\"$CODEX_HOME/auth.json\"\nprintf '%s' '"+protocol+"'\n")
	request, authority, driver, launcher, _, _ := preparedDriverFixture(t, executable)
	input := preparedOwnerTypedInput(t, request, authority)
	credential := request.Credential
	owner := &preparedOwnerLifecycle{handle: ports.CredentialHandle{
		HandleID: credential.HandleID, TenantID: credential.TenantID, OwnerUserID: credential.OwnerUserID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(credential.ProviderResource.ResourceID),
		RunID:                    credential.RunID, AttemptID: credential.AttemptID, WorkerID: credential.WorkerID,
		LeaseID: credential.LeaseID, LeaseFence: credential.LeaseFence, BindingGeneration: credential.ProviderResource.CredentialGeneration,
		ProviderResource: credential.ProviderResource, ExpiresAt: credential.ExpiresAt},
		materialized: ports.CredentialMaterialization{RootDir: request.CredentialMaterialization.RootDir, AuthFile: request.CredentialMaterialization.FilePath}}
	started := input.AcceptedAttempt.CreatedAt
	invocation := attachedworkerdaemon.Invocation{Identity: attachedworkerdaemon.InvocationIdentity{
		TenantID: authority.TenantID, OwnerUserID: authority.OwnerUserID, WorkerID: authority.WorkerID,
		RunID: authority.RunID, AttemptID: authority.AttemptID, LeaseID: authority.LeaseID, FenceToken: authority.LeaseGeneration},
		Process:       attachedworkerdaemon.AttemptSpec{Executable: executable, ExecutableDigest: driver.config.ExecutableDigest, Arguments: processArguments(driver.config.Model)},
		VerifiedInput: input, Credential: &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: CredentialHomeEnvironmentV1,
			ExpectedBindingGeneration: credential.ProviderResource.CredentialGeneration, IssueRequest: ports.CredentialIssueRequest{
				OwnerUserID: authority.OwnerUserID, ProviderResource: authority.ProviderResource, ExpiresAt: credential.ExpiresAt,
				Run: domain.Run{ID: authority.RunID, TenantID: authority.TenantID, SessionID: request.SessionID, TriggerEventID: request.TriggerEventID,
					SubscriptionConnectionID: owner.handle.SubscriptionConnectionID, Status: domain.RunRunning, IdempotencyKey: "composition-run", StartedAt: &started, CreatedAt: started, UpdatedAt: started},
				Attempt: domain.Attempt{ID: authority.AttemptID, TenantID: authority.TenantID, RunID: authority.RunID, Number: 1,
					Status: domain.AttemptRunning, WorkerID: string(authority.WorkerID), CreatedAt: started, UpdatedAt: started},
				Lease: domain.Lease{ID: authority.LeaseID, TenantID: authority.TenantID, RunID: authority.RunID, AttemptID: authority.AttemptID,
					WorkerID: string(authority.WorkerID), FenceToken: authority.LeaseGeneration, AcquiredAt: started, ExpiresAt: authority.LeaseExpiresAt}}}}
	// This is an explicit admitted unit fixture, not a source issuer or live profile.
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: canonicalTempDir(t), Launcher: launcher, Timeout: 20 * time.Second, TerminationGrace: time.Second,
		MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1,
		AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1}, AllowedReadRoots: []string{filepath.Dir(owner.materialized.RootDir)}})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewPreparedAttachedWorkerRunnerV1(driver.config, privatePreparedScratch(t), supervisor, owner, attachedworkerdaemon.InvocationRunnerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runner.Close(ctx); err != nil {
			t.Errorf("owned close: %v", err)
		}
	})
	return runner, invocation, owner, launcher
}

func TestPreparedCompositionAuthenticObservationFinalizationAndImmutableCandidate(t *testing.T) {
	runner, invocation, owner, launcher := preparedCompositionFixture(t, successfulJSONL)
	if _, err := runner.ReceiptCandidate(invocation.Identity, attachedworkerdaemon.InvocationResult{}, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("candidate before execution")
	}
	result, err := runner.Run(context.Background(), invocation)
	if !result.Succeeded(err) || launcher.prepareCount() != 1 || len(result.Process.Stdout) != 0 {
		t.Fatalf("actual joined result=%+v err=%v preparations=%d", result, err, launcher.prepareCount())
	}
	if !reflect.DeepEqual(owner.order, []string{"issue", "materialize", "writeback", "release"}) {
		t.Fatalf("single lifecycle=%v", owner.order)
	}
	for i := 0; i < 3; i++ {
		candidate, candidateErr := runner.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded)
		if candidateErr != nil || candidate.Summary != "bounded result" {
			t.Fatalf("immutable candidate[%d]=%+v err=%v", i, candidate, candidateErr)
		}
	}
	tampered := result
	tampered.Process.StdoutBytes++
	if _, err := runner.ReceiptCandidate(invocation.Identity, tampered, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("tampered actual observation accepted")
	}
	foreign := invocation.Identity
	foreign.OwnerUserID = "foreign-owner"
	if _, err := runner.ReceiptCandidate(foreign, result, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("foreign result accepted")
	}
	if _, err := runner.Run(context.Background(), invocation); err == nil {
		t.Fatal("repeated invocation accepted")
	}
	if launcher.prepareCount() != 1 || len(owner.order) != 4 {
		t.Fatalf("replay effects=%d lifecycle=%v", launcher.prepareCount(), owner.order)
	}
	entries, err := os.ReadDir(runner.scratch)
	if err != nil || len(entries) != 0 {
		t.Fatalf("owned staged history residual=%v err=%v", entries, err)
	}
	if err := runner.RetireReceipt(invocation.Identity); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.ReceiptCandidate(invocation.Identity, result, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
		t.Fatal("retired private result reused")
	}
	if _, err := runner.Run(context.Background(), invocation); err == nil {
		t.Fatal("retired invocation replayed")
	}
}

func TestPreparedCompositionUnknownAndFinalizationFailureNeverSucceed(t *testing.T) {
	for _, tc := range []struct {
		name, protocol     string
		writeBack, release bool
	}{
		{name: "unknown", protocol: codexAcceptedJSONL()}, {name: "writeback", protocol: successfulJSONL, writeBack: true}, {name: "release", protocol: successfulJSONL, release: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner, invocation, owner, launcher := preparedCompositionFixture(t, tc.protocol)
			if tc.writeBack {
				owner.writeBackErr = errors.New("fixture CAS denied")
			}
			if tc.release {
				owner.releaseErr = errors.New("fixture release denied")
			}
			result, err := runner.Run(context.Background(), invocation)
			if err == nil || result.Succeeded(err) || launcher.prepareCount() != 1 {
				t.Fatalf("result=%+v err=%v effects=%d", result, err, launcher.prepareCount())
			}
			if _, candidateErr := runner.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalSucceeded); candidateErr == nil {
				t.Fatal("failed/unknown became success candidate")
			}
			if tc.name == "unknown" || tc.release {
				if _, candidateErr := runner.ReceiptCandidate(invocation.Identity, result, err, domain.AttachedWorkerTerminalFailed); candidateErr == nil {
					t.Fatal("unknown/unreleased became committable failure")
				}
			}
			if _, retryErr := runner.Run(context.Background(), invocation); retryErr == nil {
				t.Fatal("failed provider invocation retried")
			}
		})
	}
}

func TestPreparedCompositionUnsupportedOrChangedInputDeniesBeforeIssue(t *testing.T) {
	for _, name := range []string{"body", "owner", "argv", "workspace", "skills", "mcp", "backend", "artifact"} {
		t.Run(name, func(t *testing.T) {
			runner, invocation, owner, launcher := preparedCompositionFixture(t, successfulJSONL)
			switch name {
			case "body":
				invocation.VerifiedInput.Context[0] ^= 1
			case "owner":
				invocation.VerifiedInput.AcceptedAttempt.OwnerUserID = "foreign-owner"
			case "argv":
				invocation.Process.Arguments[0] = "foreign-command"
			case "workspace":
				ref := preparedUnsupportedBlob(invocation.VerifiedInput)
				invocation.VerifiedInput.Job.WorkspaceSnapshot = &ref
			case "skills":
				ref := preparedUnsupportedBlob(invocation.VerifiedInput)
				invocation.VerifiedInput.Job.SkillBundle = &ref
			case "mcp":
				invocation.VerifiedInput.Job.AllowedMCPServers = []string{"unsupported"}
			case "backend":
				invocation.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest = strings.Repeat("0", 64)
			case "artifact":
				input := invocation.VerifiedInput
				input.Manifest.Artifacts = append(input.Manifest.Artifacts, domain.Artifact{Name: "unsupported-artifact", MediaType: "text/plain", Blob: preparedUnsupportedBlob(input)})
				input.Artifacts = append(input.Artifacts, attachedworkerdaemon.VerifiedAttachedArtifactV1{Name: "unsupported-artifact", Body: append([]byte(nil), input.Context...)})
			}
			if name == "workspace" || name == "skills" || name == "mcp" || name == "backend" || name == "artifact" {
				input := invocation.VerifiedInput
				digest, err := domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
				if err != nil {
					t.Fatal(err)
				}
				input.AcceptedAttempt.ContextDigest = digest
				if err := input.ValidateFor(invocation.Identity); err != nil {
					t.Fatalf("unsupported fixture is not coherently admitted: %v", err)
				}
			}
			if _, err := runner.Run(context.Background(), invocation); err == nil {
				t.Fatal("changed/unsupported input accepted")
			}
			if len(owner.order) != 0 || launcher.prepareCount() != 0 {
				t.Fatalf("denial occurred after effects: lifecycle=%v preparations=%d", owner.order, launcher.prepareCount())
			}
		})
	}
}

func preparedUnsupportedBlob(input *attachedworkerdaemon.VerifiedAttachedInputV1) domain.BlobRef {
	digest := sha256.Sum256(input.Context)
	return domain.BlobRef{TenantID: input.Job.TenantID, Key: "tenants/" + string(input.Job.TenantID) + "/unsupported-fixture", Size: int64(len(input.Context)), SHA256: hex.EncodeToString(digest[:])}
}

func TestPreparedHistoryPrivateBoundedAndNoFollow(t *testing.T) {
	root := privatePreparedScratch(t)
	stage, err := stagePreparedHistory(root, []byte("private"))
	if err != nil {
		t.Fatal(err)
	}
	defer stage.remove()
	for _, path := range []string{stage.path, filepath.Join(stage.path, "context")} {
		info, err := os.Lstat(path)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("private directory %q info=%v err=%v", path, info, err)
		}
	}
	info, err := os.Lstat(filepath.Join(stage.path, "context", "history.jsonl"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("history permissions info=%v err=%v", info, err)
	}
	if err := stage.remove(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(canonicalTempDir(t), "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if _, err := stagePreparedHistory(link, []byte("private")); err == nil {
		t.Fatal("symlink staging root accepted")
	}
	if _, err := stagePreparedHistory(root, make([]byte, maxInstructionBytes+1)); err == nil {
		t.Fatal("oversized staging accepted")
	}
}

func privatePreparedScratch(t *testing.T) string {
	t.Helper()
	root := filepath.Join(canonicalTempDir(t), "private-staging")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("private staging root mode: info=%v err=%v", info, err)
	}
	return root
}

func TestPreparedHistoryReplacedObjectIsNotDeletedOrAttestedClean(t *testing.T) {
	root := privatePreparedScratch(t)
	stage, err := stagePreparedHistory(root, []byte("owned original history"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(stage.path, "context", "history.jsonl")
	if err := os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement outside original ownership"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := stage.remove(); err == nil {
		t.Fatal("replacement attested as successful original cleanup")
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "replacement outside original ownership" {
		t.Fatalf("replacement deleted: %v", err)
	}
}
