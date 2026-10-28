package attachedworkersealedinput

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type nativeCredentialJobFixture struct {
	*preparedCredentialJobFixture
	credentialCalls int
}

func (jobs *nativeCredentialJobFixture) LoadWorkerCredentialInvocation(_ context.Context, _ domain.TenantID, _ domain.RunID, _ domain.AttemptID, _ domain.LeaseID) (ports.WorkerCredentialInvocationState, bool, error) {
	jobs.credentialCalls++
	return jobs.credential, true, nil
}

func nativeSubscriptionFixture(t *testing.T) (*Service, *authorizerFixture, *nativeCredentialJobFixture, *blobFixture, attachedworkerdaemontransport.MaterializationRequestV1) {
	t.Helper()
	_, authorizer, jobs, blobs, request := fixtureInput(t)
	job := &jobs.state.Job
	job.HarnessBinding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	job.HarnessBinding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	job.HarnessBinding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1,
		ResourceID: "subscription-1", OwnerUserID: request.OwnerUserID, Revision: 1,
		CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 3}
	at := time.Unix(20, 0).UTC()
	expires := time.UnixMicro(request.Attempt.ExpiresAtUnixMicro).UTC()
	evidenceExpiry := at.Add(time.Hour)
	job.HarnessBinding.EvidenceExpiresAt = &evidenceExpiry
	digest, err := domain.AttachedWorkerJobContextDigestV1(*job, jobs.state.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt.ContextDigest = decodeDigest(t, string(digest))
	canonical := &preparedCredentialJobFixture{jobFixture: jobs, credential: ports.WorkerCredentialInvocationState{
		Run: domain.Run{ID: job.RunID, TenantID: job.TenantID, SessionID: job.SessionID, TriggerEventID: job.TriggerEventID,
			SubscriptionConnectionID: "subscription-1", Status: domain.RunRunning, IdempotencyKey: "fixture-run", StartedAt: &at, CreatedAt: at, UpdatedAt: at},
		Attempt: domain.Attempt{ID: job.AttemptID, TenantID: job.TenantID, RunID: job.RunID, Number: 1, Status: domain.AttemptRunning,
			WorkerID: string(request.WorkerID), CreatedAt: at, UpdatedAt: at},
		Lease: domain.Lease{ID: domain.LeaseID(request.Attempt.LeaseID), TenantID: job.TenantID, RunID: job.RunID, AttemptID: job.AttemptID,
			WorkerID: string(request.WorkerID), FenceToken: request.Attempt.LeaseGeneration, AcquiredAt: at, ExpiresAt: expires}}}
	store := &nativeCredentialJobFixture{preparedCredentialJobFixture: canonical}
	service, err := NewNativeSubscriptionServiceV1(authorizer, store, blobs, func() time.Time { return at })
	if err != nil {
		t.Fatal(err)
	}
	return service, authorizer, store, blobs, request
}

func TestNativeSubscriptionSourceUsesCanonicalRequestAndOriginalContentFences(t *testing.T) {
	service, auth, jobs, blobs, request := nativeSubscriptionFixture(t)
	input, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
	defer clearResult(&input)
	if err != nil || input.Credential == nil {
		t.Fatalf("native subscription input err=%v credential=%t", err, input.Credential != nil)
	}
	credential := input.Credential
	if service.allowTestProvider || credential.HomeEnvironment != codexexec.CredentialHomeEnvironmentV1 ||
		credential.ExpectedBindingGeneration != 3 || !reflect.DeepEqual(credential.IssueRequest.Run, jobs.credential.Run) ||
		credential.IssueRequest.Attempt != jobs.credential.Attempt || credential.IssueRequest.Lease != jobs.credential.Lease ||
		credential.IssueRequest.ProviderResource != input.Job.HarnessBinding.Resource ||
		jobs.credentialCalls != 1 || auth.calls != 2 || auth.requests[0].ExpectedAttemptRevision != 0 || auth.requests[1].ExpectedAttemptRevision != 7 || blobs.calls != 2 {
		t.Fatal("native path changed canonical metadata or original pre/post-content fences")
	}
}

func TestNativeSubscriptionSourceRejectsDefaultAndNonCodexModes(t *testing.T) {
	for _, mode := range []string{"default", "fixture", "other backend", "wrong delivery", "test promotion", "missing canonical port"} {
		t.Run(mode, func(t *testing.T) {
			service, auth, jobs, blobs, request := nativeSubscriptionFixture(t)
			job := &jobs.state.Job
			switch mode {
			case "default":
				service.nativeSubscriptionNow = nil
			case "fixture":
				job.HarnessBinding.Backend.ProviderContractKind = domain.ProviderContractCredentiallessFixtureV1
			case "other backend":
				job.HarnessBinding.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
			case "wrong delivery":
				job.HarnessBinding.Backend.CredentialDeliveryKind = "unsupported"
			case "test promotion":
				service.allowTestProvider = true
			case "missing canonical port":
				built, err := NewNativeSubscriptionServiceV1(auth, jobs.jobFixture, blobs, nil)
				if !errors.Is(err, ErrInvalid) || built != nil {
					t.Fatalf("missing canonical port accepted: %v", err)
				}
				return
			}
			// Keep the accepted job/context binding valid while changing mode.
			digest, err := domain.AttachedWorkerJobContextDigestV1(*job, jobs.state.InputManifest)
			if err == nil {
				request.Attempt.ContextDigest = decodeDigest(t, string(digest))
			}
			input, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
			if err == nil || !reflect.DeepEqual(input, attachedworkerdaemontransport.SealedInputV1{}) || blobs.calls != 0 {
				t.Fatalf("mode=%s err=%v leaked-input=%t blob-reads=%d", mode, err, !reflect.DeepEqual(input, attachedworkerdaemontransport.SealedInputV1{}), blobs.calls)
			}
		})
	}
}

func TestNativeSubscriptionSourceReturnsZeroOnLostAuthorityOrExpiry(t *testing.T) {
	for _, mode := range []string{"before content", "after content", "changed revision", "expired metadata", "expires during content", "wrong canonical fence", "wrong canonical resource", "wrong canonical run", "wrong canonical tenant", "wrong canonical session", "wrong canonical trigger", "wrong canonical attempt", "wrong canonical lease"} {
		t.Run(mode, func(t *testing.T) {
			service, auth, jobs, blobs, request := nativeSubscriptionFixture(t)
			wantBlobs := 0
			switch mode {
			case "before content":
				auth.denyAt = 1
			case "after content":
				auth.denyAt, wantBlobs = 2, 2
			case "changed revision":
				auth.secondRevision, wantBlobs = 8, 2
			case "expired metadata":
				service.nativeSubscriptionNow = func() time.Time { return jobs.credential.Lease.ExpiresAt }
			case "expires during content":
				wantBlobs = 2
				service.nativeSubscriptionNow = func() time.Time {
					if auth.calls == 2 {
						return jobs.credential.Lease.ExpiresAt
					}
					return time.Unix(20, 0).UTC()
				}
			case "wrong canonical fence":
				jobs.credential.Lease.FenceToken++
			case "wrong canonical resource":
				jobs.credential.Run.SubscriptionConnectionID = "foreign"
			case "wrong canonical run":
				jobs.credential.Run.ID = "foreign"
				jobs.credential.Attempt.RunID, jobs.credential.Lease.RunID = "foreign", "foreign"
			case "wrong canonical tenant":
				jobs.credential.Run.TenantID, jobs.credential.Attempt.TenantID, jobs.credential.Lease.TenantID = "foreign", "foreign", "foreign"
			case "wrong canonical session":
				jobs.credential.Run.SessionID = "foreign"
			case "wrong canonical trigger":
				jobs.credential.Run.TriggerEventID = "foreign"
			case "wrong canonical attempt":
				jobs.credential.Attempt.ID, jobs.credential.Lease.AttemptID = "foreign", "foreign"
			case "wrong canonical lease":
				jobs.credential.Lease.ID = "foreign"
			}
			input, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
			if !errors.Is(err, ErrUnauthorized) || !reflect.DeepEqual(input, attachedworkerdaemontransport.SealedInputV1{}) || blobs.calls != wantBlobs {
				t.Fatalf("mode=%s err=%v leaked-input=%t blob-reads=%d want%d", mode, err, !reflect.DeepEqual(input, attachedworkerdaemontransport.SealedInputV1{}), blobs.calls, wantBlobs)
			}
		})
	}
}
