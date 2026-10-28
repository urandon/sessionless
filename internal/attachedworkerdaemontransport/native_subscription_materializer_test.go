package attachedworkerdaemontransport

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func nativeSubscriptionMaterializerFixture(t *testing.T) (*BoundMaterializer, *sealedSourceFixture, MaterializationRequestV1) {
	t.Helper()
	_, source, request := sealedMaterializerFixture(t)
	job := &source.input.Job
	job.HarnessBinding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	job.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-1",
		OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
	job.HarnessBinding.Resource = resource
	at := time.Unix(20, 0).UTC()
	evidenceExpiry := at.Add(time.Hour)
	job.HarnessBinding.EvidenceExpiresAt = &evidenceExpiry
	digest, err := domain.AttachedWorkerJobContextDigestV1(*job, source.input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	source.input.AcceptedAttempt.ContextDigest = digest
	request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
	expires := time.UnixMicro(request.Attempt.ExpiresAtUnixMicro).UTC()
	source.input.Credential = &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: codexexec.CredentialHomeEnvironmentV1, ExpectedBindingGeneration: 3,
		IssueRequest: ports.CredentialIssueRequest{OwnerUserID: request.OwnerUserID, ProviderResource: resource, ExpiresAt: expires,
			Run: domain.Run{ID: job.RunID, TenantID: job.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID,
				SubscriptionConnectionID: "subscription-1", Status: domain.RunRunning, IdempotencyKey: "fixture-run", StartedAt: &at, CreatedAt: at, UpdatedAt: at},
			Attempt: domain.Attempt{ID: job.AttemptID, TenantID: job.TenantID, RunID: job.RunID, Number: 1, Status: domain.AttemptRunning,
				WorkerID: string(request.WorkerID), CreatedAt: at, UpdatedAt: at},
			Lease: domain.Lease{ID: domain.LeaseID(request.Attempt.LeaseID), TenantID: job.TenantID, RunID: job.RunID, AttemptID: job.AttemptID,
				WorkerID: string(request.WorkerID), FenceToken: request.Attempt.LeaseGeneration, AcquiredAt: at, ExpiresAt: expires}}}
	m, err := NewNativeSubscriptionBoundMaterializerV1(source, 8192, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	return m, source, request
}

func TestNativeSubscriptionMaterializerUsesOrdinaryExactCodexMetadata(t *testing.T) {
	m, source, request := nativeSubscriptionMaterializerFixture(t)
	original := *source.input.Credential
	contextBytes, artifactBytes := source.input.Context, source.input.Artifacts[0].Body
	result, err := m.Materialize(t.Context(), request)
	defer clearMaterializedInput(&result)
	if err != nil || result.Credential == nil || !reflect.DeepEqual(*result.Credential, original) ||
		result.VerifiedInput == nil || m.allowTestProvider || len(result.ReadRoots) != 0 || source.calls != 1 {
		t.Fatalf("ordinary native materialization err=%v credential=%t verified=%t calls=%d", err, result.Credential != nil, result.VerifiedInput != nil, source.calls)
	}
	if bytes.Contains(result.Stdin, []byte("CODEX_HOME")) || bytes.Contains(result.Stdin, []byte("subscription-1")) ||
		!bytes.Equal(contextBytes, make([]byte, len(contextBytes))) || !bytes.Equal(artifactBytes, make([]byte, len(artifactBytes))) ||
		string(result.VerifiedInput.Context) != "synthetic canonical context" {
		t.Fatal("credential/path serialized or source content ownership was not transferred and cleared")
	}
}

func TestNativeSubscriptionMaterializerDeniesWrongModesAndMetadata(t *testing.T) {
	for _, mode := range []string{"default", "fixture", "other backend", "wrong delivery", "test promotion", "missing metadata", "wrong home", "wrong generation", "wrong resource", "wrong fence", "wrong run", "wrong accepted head", "expired during load", "changed bytes"} {
		t.Run(mode, func(t *testing.T) {
			m, source, request := nativeSubscriptionMaterializerFixture(t)
			switch mode {
			case "default":
				m.nativeSubscription = false
			case "fixture":
				source.input.Job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractCredentiallessFixtureV1
			case "other backend":
				source.input.Job.HarnessBinding.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
			case "wrong delivery":
				source.input.Job.HarnessBinding.Backend.CredentialDeliveryKind = "unsupported"
			case "test promotion":
				m.allowTestProvider = true
			case "missing metadata":
				source.input.Credential = nil
			case "wrong home":
				source.input.Credential.HomeEnvironment = "SESSIONLESS_PROVIDER_HOME"
			case "wrong generation":
				source.input.Credential.ExpectedBindingGeneration++
			case "wrong resource":
				source.input.Credential.IssueRequest.ProviderResource.ResourceID = "foreign"
			case "wrong fence":
				source.input.Credential.IssueRequest.Lease.FenceToken++
			case "wrong run":
				source.input.Credential.IssueRequest.Run.ID = "foreign"
			case "wrong accepted head":
				source.input.AcceptedAttempt.ConnectionGeneration++
			case "expired during load":
				source.afterLoad = func() { m.now = func() time.Time { return time.UnixMicro(request.Attempt.ExpiresAtUnixMicro).UTC() } }
			case "changed bytes":
				source.input.Context[0] ^= 1
			}
			if mode == "fixture" || mode == "other backend" || mode == "wrong delivery" {
				digest, err := domain.AttachedWorkerJobContextDigestV1(source.input.Job, source.input.Manifest)
				if err == nil {
					source.input.AcceptedAttempt.ContextDigest = digest
					request.Attempt.ContextDigest = mustSealedDigest(t, string(digest))
				}
			}
			contextBytes, artifactBytes := source.input.Context, source.input.Artifacts[0].Body
			result, err := m.Materialize(t.Context(), request)
			if err == nil || !(errors.Is(err, ErrSealedInputInvalid) || errors.Is(err, ErrSealedInputUnsupported)) ||
				!reflect.DeepEqual(result, MaterializedInputV1{}) || source.calls != 1 ||
				!bytes.Equal(contextBytes, make([]byte, len(contextBytes))) || !bytes.Equal(artifactBytes, make([]byte, len(artifactBytes))) {
				t.Fatalf("mode=%s err=%v leaked-result=%t source-calls=%d", mode, err, !reflect.DeepEqual(result, MaterializedInputV1{}), source.calls)
			}
		})
	}
}
