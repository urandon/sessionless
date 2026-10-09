package codexexec

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type preparedOwnerLifecycle struct {
	handle       ports.CredentialHandle
	materialized ports.CredentialMaterialization
	order        []string
	writeBackErr error
	releaseErr   error
}

func (owner *preparedOwnerLifecycle) Issue(ctx context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	owner.order = append(owner.order, "issue")
	if ctx.Err() != nil || request.ValidateAt(driverNow) != nil || request.ProviderResource != owner.handle.ProviderResource {
		return ports.CredentialHandle{}, errors.New("fixture issue authority mismatch")
	}
	return owner.handle, nil
}

func (owner *preparedOwnerLifecycle) Materialize(ctx context.Context, handle ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	owner.order = append(owner.order, "materialize")
	if ctx.Err() != nil || handle != owner.handle {
		return ports.CredentialMaterialization{}, errors.New("fixture materialization authority mismatch")
	}
	return owner.materialized, nil
}

func (owner *preparedOwnerLifecycle) WriteBack(ctx context.Context, handle ports.CredentialHandle, materialized ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	owner.order = append(owner.order, "writeback")
	if ctx.Err() != nil || handle != owner.handle || materialized != owner.materialized {
		return ports.CredentialWriteBackResult{}, errors.New("fixture writeback authority mismatch")
	}
	return ports.CredentialWriteBackResult{Generation: owner.handle.BindingGeneration}, owner.writeBackErr
}

func (owner *preparedOwnerLifecycle) Release(ctx context.Context, handle ports.CredentialHandle) error {
	owner.order = append(owner.order, "release")
	if ctx.Err() != nil || handle != owner.handle {
		return errors.New("fixture release authority mismatch")
	}
	return owner.releaseErr
}

func (*preparedOwnerLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return errors.New("fixture must not revoke a whole connection")
}

type preparedOwnerExecutor struct {
	t        *testing.T
	driver   *Driver
	request  ports.ExecutionRequest
	sink     *fixtureEventSink
	owner    *preparedOwnerLifecycle
	result   ports.ExecutionResult
	observed attachedworkerdaemon.AttemptResult
	calls    int
}

func (execute *preparedOwnerExecutor) RunPrepared(ctx context.Context, prepared attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	execute.calls++
	execute.owner.order = append(execute.owner.order, "execute")
	request := execute.request
	if request.Credential.HandleID != "" || request.CredentialMaterialization.Kind != "" {
		execute.t.Error("execution template already contained a credential")
	}
	if prepared.Identity.RunID != request.RunID || prepared.Identity.AttemptID != request.AttemptID ||
		prepared.Credential != execute.owner.handle.ProviderInvocationCredential() ||
		prepared.Materialization != execute.owner.materialized.ProviderMaterialization() {
		execute.t.Error("executor did not receive the exact once-issued projection")
	}
	request.Credential = prepared.Credential
	request.CredentialMaterialization = prepared.Materialization
	result, err := execute.driver.Execute(ctx, request, execute.sink)
	execute.result = result
	// Process observations come from the configured process fixture, not from
	// driver success. Production must retain actual supervisor observations.
	return execute.observed, err
}

func TestPreparedOwnerFeedsCodexDriverWithoutAnotherCredentialLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name         string
		protocol     string
		writeBackErr error
		releaseErr   error
		wantSuccess  bool
	}{
		{name: "completed", protocol: successfulJSONL, wantSuccess: true},
		{name: "ambiguous protocol", protocol: codexAcceptedJSONL()},
		{name: "writeback failed", protocol: successfulJSONL, writeBackErr: errors.New("fixture writeback denied")},
		{name: "release failed", protocol: successfulJSONL, releaseErr: errors.New("fixture release denied")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, authority := validDriverRequest(t)
			workDir, err := filepath.EvalSymlinks(request.WorkDir)
			if err != nil {
				t.Fatal(err)
			}
			request.WorkDir = workDir
			credentialRoot := filepath.Join(workDir, "credential")
			if err := os.Mkdir(credentialRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			authFile := filepath.Join(credentialRoot, "auth.json")
			if err := os.WriteFile(authFile, []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
			credential := request.Credential
			owner := &preparedOwnerLifecycle{
				handle: ports.CredentialHandle{
					HandleID: credential.HandleID, TenantID: credential.TenantID, OwnerUserID: credential.OwnerUserID,
					SubscriptionConnectionID: domain.SubscriptionConnectionID(credential.ProviderResource.ResourceID),
					RunID:                    credential.RunID, AttemptID: credential.AttemptID, WorkerID: credential.WorkerID,
					LeaseID: credential.LeaseID, LeaseFence: credential.LeaseFence,
					BindingGeneration: credential.ProviderResource.CredentialGeneration,
					ProviderResource:  credential.ProviderResource, ExpiresAt: credential.ExpiresAt,
				},
				materialized: ports.CredentialMaterialization{RootDir: credentialRoot, AuthFile: authFile},
				writeBackErr: tc.writeBackErr, releaseErr: tc.releaseErr,
			}
			request.Credential = ports.ProviderInvocationCredentialV1{}
			request.CredentialMaterialization = ports.ProviderCredentialMaterializationV1{}
			boundary := &fixtureProcessBoundary{result: successfulDriverProcess([]byte(tc.protocol))}
			driver := mustDriverWithResolver(t, true, &fixtureAuthorityResolver{authority: authority}, boundary)
			executor := &preparedOwnerExecutor{
				t: t, driver: driver, request: request, sink: &fixtureEventSink{}, owner: owner,
				observed: attachedworkerdaemon.AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true},
			}
			runner, err := attachedworkerdaemon.NewPreparedInvocationRunner(attachedworkerdaemon.InvocationRunnerConfig{}, executor, owner)
			if err != nil {
				t.Fatal(err)
			}
			started := driverNow.Add(-time.Minute)
			invocation := attachedworkerdaemon.Invocation{
				Identity: attachedworkerdaemon.InvocationIdentity{
					TenantID: authority.TenantID, OwnerUserID: authority.OwnerUserID, WorkerID: authority.WorkerID,
					RunID: authority.RunID, AttemptID: authority.AttemptID, LeaseID: authority.LeaseID, FenceToken: authority.LeaseGeneration,
				},
				Process: attachedworkerdaemon.AttemptSpec{Executable: "/opt/sessionless/codex", ExecutableDigest: attachedworkerdaemon.ExecutableDigest{1}},
				Credential: &attachedworkerdaemon.CredentialInvocation{
					HomeEnvironment: CredentialHomeEnvironmentV1, ExpectedBindingGeneration: owner.handle.BindingGeneration,
					IssueRequest: ports.CredentialIssueRequest{
						OwnerUserID: authority.OwnerUserID, ProviderResource: authority.ProviderResource, ExpiresAt: owner.handle.ExpiresAt,
						Run: domain.Run{
							ID: authority.RunID, TenantID: authority.TenantID, SessionID: request.SessionID, TriggerEventID: request.TriggerEventID,
							SubscriptionConnectionID: owner.handle.SubscriptionConnectionID, Status: domain.RunRunning,
							IdempotencyKey: "prepared-owner-run", StartedAt: &started, CreatedAt: started, UpdatedAt: started,
						},
						Attempt: domain.Attempt{
							ID: authority.AttemptID, TenantID: authority.TenantID, RunID: authority.RunID, Number: 1,
							Status: domain.AttemptRunning, WorkerID: string(authority.WorkerID), CreatedAt: started, UpdatedAt: started,
						},
						Lease: domain.Lease{
							ID: authority.LeaseID, TenantID: authority.TenantID, RunID: authority.RunID, AttemptID: authority.AttemptID,
							WorkerID: string(authority.WorkerID), FenceToken: authority.LeaseGeneration, AcquiredAt: started, ExpiresAt: authority.LeaseExpiresAt,
						},
					},
				},
			}
			result, runErr := runner.Run(context.Background(), invocation)
			if executor.calls != 1 || boundary.runs != 1 || result.Succeeded(runErr) != tc.wantSuccess ||
				!result.CredentialReleaseRequired || result.CredentialReleased != (tc.releaseErr == nil) {
				t.Errorf("joined owner: callbacks=%d process=%d result=%+v err=%v want success=%t", executor.calls, boundary.runs, result, runErr, tc.wantSuccess)
			}
			if !reflect.DeepEqual(owner.order, []string{"issue", "materialize", "execute", "writeback", "release"}) {
				t.Errorf("joined owner lifecycle order=%v", owner.order)
			}
			if tc.wantSuccess && (executor.result.Summary != "bounded result" || len(executor.sink.events) != 1) {
				t.Errorf("semantic driver result=%+v events=%d", executor.result, len(executor.sink.events))
			}
			if tc.protocol == codexAcceptedJSONL() && (runErr == nil || executor.result.Summary != "") {
				t.Error("ambiguous provider response became success or retained a final summary")
			}
		})
	}
}
