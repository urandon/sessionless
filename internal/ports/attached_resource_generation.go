package ports

import (
	"context"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const AttachedResourceGenerationOperationTimeoutV1 = 15 * time.Second

// AttachedResourceGenerationAuthorizationV1 crosses only the original local
// service/store boundary. The authenticated transport derives the digest;
// remote JSON must never supply it. This contains no credential bytes.
type AttachedResourceGenerationAuthorizationV1 struct {
	TenantID                 domain.TenantID                             `json:"tenant_id"`
	OwnerUserID              domain.UserID                               `json:"owner_user_id"`
	SubscriptionConnectionID domain.SubscriptionConnectionID             `json:"subscription_connection_id"`
	WorkerID                 domain.AttachedWorkerID                     `json:"worker_id"`
	EnrollmentGeneration     uint64                                      `json:"enrollment_generation"`
	BaselineRevision         uint64                                      `json:"baseline_revision"`
	ConnectionID             domain.AttachedWorkerConnectionID           `json:"connection_id"`
	ConnectionGeneration     uint64                                      `json:"connection_generation"`
	PresentedSecretDigest    domain.AttachedWorkerConnectionSecretDigest `json:"-"`
}

func (auth AttachedResourceGenerationAuthorizationV1) Validate() error {
	for _, validate := range []func() error{auth.TenantID.Validate, auth.OwnerUserID.Validate, auth.SubscriptionConnectionID.Validate, auth.WorkerID.Validate, auth.ConnectionID.Validate, auth.PresentedSecretDigest.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	if auth.EnrollmentGeneration == 0 || auth.BaselineRevision == 0 || auth.ConnectionGeneration == 0 {
		return domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(auth)
	if err != nil || len(raw) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return domain.ErrMembershipDenied
	}
	return nil
}

type AttachedResourceGenerationPublicationV1 struct {
	Authorization                AttachedResourceGenerationAuthorizationV1 `json:"authorization"`
	ExpectedCredentialGeneration uint64                                    `json:"expected_credential_generation"`
	Target                       domain.AttachedResourceGenerationHeadV1   `json:"target"`
}

func (request AttachedResourceGenerationPublicationV1) Validate() error {
	if request.Authorization.Validate() != nil || request.Target.Validate() != nil ||
		request.Target.OwnerUserID != request.Authorization.OwnerUserID || request.Target.WorkerID != request.Authorization.WorkerID ||
		request.Target.EnrollmentGeneration != request.Authorization.EnrollmentGeneration || request.Target.BaselineRevision != request.Authorization.BaselineRevision {
		return domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Head is zero for denial, missing metadata, failure and unknown commit. Only
// an authenticated applied/replayed result may expose the authoritative head.
type AttachedResourceGenerationResultV1 struct {
	Status AttachedWorkerExecutionStatus           `json:"status"`
	Head   domain.AttachedResourceGenerationHeadV1 `json:"head"`
}

type AttachedResourceGenerationStoreV1 interface {
	PublishAttachedResourceGenerationV1(context.Context, AttachedResourceGenerationPublicationV1) (AttachedResourceGenerationResultV1, error)
	ReadAttachedResourceGenerationV1(context.Context, AttachedResourceGenerationAuthorizationV1) (AttachedResourceGenerationResultV1, error)
}

// The BFF has its own authenticated owner, not a worker bearer. This preflight
// point read still rechecks current membership and resource/worker ownership;
// it is not admission or permission to execute a projected binding.
type AttachedResourceGenerationHeadStoreV1 interface {
	LoadAttachedResourceGenerationHeadV1(context.Context, domain.TenantID, domain.UserID, domain.SubscriptionConnectionID) (domain.AttachedResourceGenerationHeadV1, bool, error)
}
