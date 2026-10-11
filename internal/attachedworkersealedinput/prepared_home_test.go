package attachedworkersealedinput

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type preparedCredentialJobFixture struct {
	*jobFixture
	credential ports.WorkerCredentialInvocationState
}

func (jobs preparedCredentialJobFixture) LoadWorkerCredentialInvocation(_ context.Context, tenant domain.TenantID, run domain.RunID, attempt domain.AttemptID, lease domain.LeaseID) (ports.WorkerCredentialInvocationState, bool, error) {
	v := jobs.credential
	return v, tenant == v.Run.TenantID && run == v.Run.ID && attempt == v.Attempt.ID && lease == v.Lease.ID, nil
}

func TestTestProviderSourceSelectsCanonicalBackendHomeOnly(t *testing.T) {
	for _, backend := range []domain.HarnessBackendKindV1{domain.HarnessBackendCodexExecV1, domain.HarnessBackendDirectOpenRouterV1} {
		t.Run(string(backend), func(t *testing.T) {
			service, _, jobs, _, request := fixtureInput(t)
			job := jobs.state.Job
			job.HarnessBinding.Backend.BackendKind = backend
			job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
			job.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
			job.HarnessBinding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
			at := time.Unix(20, 0).UTC()
			evidenceExpiry := at.Add(time.Hour)
			job.HarnessBinding.EvidenceExpiresAt = &evidenceExpiry
			expires := time.UnixMicro(request.Attempt.ExpiresAtUnixMicro).UTC()
			service.jobs = preparedCredentialJobFixture{jobFixture: jobs, credential: ports.WorkerCredentialInvocationState{
				Run:     domain.Run{ID: job.RunID, TenantID: job.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID, SubscriptionConnectionID: "subscription-1", Status: domain.RunRunning, IdempotencyKey: "fixture-run", StartedAt: &at, CreatedAt: at, UpdatedAt: at},
				Attempt: domain.Attempt{ID: job.AttemptID, TenantID: job.TenantID, RunID: job.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(request.WorkerID), CreatedAt: at, UpdatedAt: at},
				Lease:   domain.Lease{ID: domain.LeaseID(request.Attempt.LeaseID), TenantID: job.TenantID, RunID: job.RunID, AttemptID: job.AttemptID, WorkerID: string(request.WorkerID), FenceToken: request.Attempt.LeaseGeneration, AcquiredAt: at, ExpiresAt: expires}}}
			// Shipped constructor cannot produce provider credential metadata.
			if _, err := service.testProviderCredential(t.Context(), request, job); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("default enabled provider: %v", err)
			}
			service.allowTestProvider = true
			service.testProviderNow = func() time.Time { return at }
			credential, err := service.testProviderCredential(t.Context(), request, job)
			want := "SESSIONLESS_PROVIDER_HOME"
			if backend == domain.HarnessBackendCodexExecV1 {
				want = codexexec.CredentialHomeEnvironmentV1
			}
			if err != nil || credential == nil || credential.HomeEnvironment != want {
				t.Fatalf("backend-home selection: %v", err)
			}
		})
	}
}
