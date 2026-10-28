package attachedworkersealedinput

import (
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// NewNativeSubscriptionServiceV1 admits only ordinary Codex invocation-scoped
// subscription/file metadata. It preserves the original pre/post-content
// authorization and does not enable fixtures, issue credentials or start work.
func NewNativeSubscriptionServiceV1(authorizer Authorizer, jobs JobStore, blobs BlobStore, now func() time.Time) (*Service, error) {
	if _, ok := jobs.(credentialJobStore); !ok {
		return nil, ErrInvalid
	}
	service, err := NewService(authorizer, jobs, blobs)
	if err != nil {
		return nil, err
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	service.nativeSubscriptionNow = now
	return service, nil
}

func (service *Service) supportsProvider(job domain.WorkerJob) bool {
	if service.nativeSubscriptionNow != nil {
		return !service.allowTestProvider &&
			job.HarnessBinding.Backend.BackendKind == domain.HarnessBackendCodexExecV1 &&
			job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractInvocationV1 &&
			job.HarnessBinding.Backend.CredentialDeliveryKind == domain.ProviderCredentialDeliveryFileV1 &&
			job.HarnessBinding.Resource.Kind == domain.ProviderResourceSubscriptionV1
	}
	return job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractCredentiallessFixtureV1 || service.allowTestProvider
}
