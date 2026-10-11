package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestTestProviderHomeIsExactBackendAndDefaultDenied(t *testing.T) {
	for _, backend := range []domain.HarnessBackendKindV1{domain.HarnessBackendCodexExecV1, domain.HarnessBackendDirectOpenRouterV1} {
		for _, mode := range []string{"accepted", "wrong-home", "default-denied"} {
			t.Run(string(backend)+"/"+mode, func(t *testing.T) {
				m, source, request := sealedMaterializerFixture(t)
				job := &source.input.Job
				job.HarnessBinding.Backend.BackendKind = backend
				job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
				job.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
				resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1", OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
				job.HarnessBinding.Resource = resource
				evidenceExpiry := time.Unix(20, 0).UTC().Add(time.Hour)
				job.HarnessBinding.EvidenceExpiresAt = &evidenceExpiry
				digest, err := domain.AttachedWorkerJobContextDigestV1(*job, source.input.Manifest)
				if err != nil {
					t.Fatal(err)
				}
				source.input.AcceptedAttempt.ContextDigest = digest
				request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
				at := time.Unix(20, 0).UTC()
				expires := time.UnixMicro(request.Attempt.ExpiresAtUnixMicro).UTC()
				home := "SESSIONLESS_PROVIDER_HOME"
				if backend == domain.HarnessBackendCodexExecV1 {
					home = codexexec.CredentialHomeEnvironmentV1
				}
				if mode == "wrong-home" {
					if home == codexexec.CredentialHomeEnvironmentV1 {
						home = "SESSIONLESS_PROVIDER_HOME"
					} else {
						home = codexexec.CredentialHomeEnvironmentV1
					}
				}
				source.input.Credential = &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: home, ExpectedBindingGeneration: 3,
					IssueRequest: ports.CredentialIssueRequest{OwnerUserID: request.OwnerUserID, ProviderResource: resource, ExpiresAt: expires,
						Run:     domain.Run{ID: job.RunID, TenantID: job.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID, SubscriptionConnectionID: "subscription-1", Status: domain.RunRunning, IdempotencyKey: "fixture-run", StartedAt: &at, CreatedAt: at, UpdatedAt: at},
						Attempt: domain.Attempt{ID: job.AttemptID, TenantID: job.TenantID, RunID: job.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(request.WorkerID), CreatedAt: at, UpdatedAt: at},
						Lease:   domain.Lease{ID: domain.LeaseID(request.Attempt.LeaseID), TenantID: job.TenantID, RunID: job.RunID, AttemptID: job.AttemptID, WorkerID: string(request.WorkerID), FenceToken: request.Attempt.LeaseGeneration, AcquiredAt: at, ExpiresAt: expires}}}
				if source.input.Credential.IssueRequest.ValidateAt(at) != nil {
					t.Fatal("invalid bounded fixture issue")
				}
				m.allowTestProvider = mode != "default-denied"
				result, err := m.Materialize(context.Background(), request)
				defer clearMaterializedInput(&result)
				switch mode {
				case "accepted":
					if err != nil || result.Credential == nil || result.Credential.HomeEnvironment != home {
						t.Fatalf("exact backend/home denied: %v", err)
					}
				case "wrong-home":
					if !errors.Is(err, ErrSealedInputInvalid) || result.Credential != nil {
						t.Fatalf("wrong-home escaped: %v", err)
					}
				case "default-denied":
					if !errors.Is(err, ErrSealedInputUnsupported) || result.Credential != nil {
						t.Fatalf("default provider was admitted: %v", err)
					}
				}
			})
		}
	}
}
