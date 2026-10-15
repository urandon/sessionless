// Package attachedworkerupload connects authenticated bounded upload control
// to the existing custody transactions. It is not a storage body proxy or a
// replacement Source authorizer. No serving/profile constructor registers it.
package attachedworkerupload

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeruploadwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var (
	ErrInvalid      = errors.New("invalid attached-worker upload control")
	ErrUnauthorized = errors.New("unauthorized attached-worker upload control")
	ErrUnavailable  = errors.New("unavailable attached-worker upload control")
)

type BearerBinder interface {
	BindOutputReceiptBearer([]byte, ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error)
}

type CustodyStore interface {
	AdmitAttachedWorkerOutputUpload(context.Context, ydbstore.AttachedWorkerOutputUploadAdmissionV1) (ydbstore.AttachedWorkerOutputUploadAdmissionResultV1, error)
	ClaimAttachedWorkerOutputUpload(context.Context, ydbstore.AttachedWorkerOutputUploadClaimV1) (ydbstore.AttachedWorkerOutputUploadClaimResultV1, error)
	ReconcileAttachedWorkerOutputUpload(context.Context, ydbstore.AttachedWorkerOutputUploadReconciliationV1) (ydbstore.AttachedWorkerOutputUploadAdmissionResultV1, error)
}

type Service struct {
	binder BearerBinder
	store  CustodyStore
}

func NewService(binder BearerBinder, store CustodyStore) (*Service, error) {
	if binder == nil || store == nil {
		return nil, ErrInvalid
	}
	return &Service{binder: binder, store: store}, nil
}

// Execute returns custody metadata only. In particular a successful claim with
// capability:null is NOT permission for a body. There is deliberately no signer
// dependency, callback or Boolean Source certificate. A future effect owner
// must supply the genuine current authority and preserve claim.ExpiresAt;
// it cannot recover that narrower bound from the plan after a lost reply.
func (service *Service) Execute(ctx context.Context, bearer []byte, request attachedworkeruploadwire.Request) (attachedworkeruploadwire.Reply, error) {
	denied := attachedworkeruploadwire.Reply{Status: ports.AttachedWorkerExecutionDenied}
	if service == nil || ctx == nil || ctx.Err() != nil || len(bearer) == 0 || len(bearer) > attachedworkertransport.ConnectionBearerMaxBytes {
		return denied, ErrUnauthorized
	}
	if attachedworkeruploadwire.ValidateRequest(request) != nil {
		return denied, ErrInvalid
	}
	bound, err := service.binder.BindOutputReceiptBearer(bearer, request.Authorization)
	if err != nil || !sameUploadBoundAuthorization(request.Authorization, bound) {
		return denied, ErrUnauthorized
	}
	var result ydbstore.AttachedWorkerOutputUploadAdmissionResultV1
	switch request.Operation {
	case attachedworkeruploadwire.Admit:
		result, err = service.store.AdmitAttachedWorkerOutputUpload(ctx, ydbstore.AttachedWorkerOutputUploadAdmissionV1{
			Authorization: bound, SessionID: request.SessionID, HarnessBindingDigest: request.HarnessBindingDigest,
			ProfileDigest: request.ProfileDigest, SchemaDigest: request.SchemaDigest, FinalDigest: request.FinalDigest,
			AnswerDigest: request.AnswerDigest, ProducerDigest: request.ProducerDigest, Nonce: request.Nonce,
			RemainingNanoseconds: request.CallerDeadline.RemainingNanoseconds, Observation: request.Observation,
			ObservationDigest: request.ObservationDigest, Artifacts: request.Artifacts,
		})
	case attachedworkeruploadwire.Claim:
		var claimed ydbstore.AttachedWorkerOutputUploadClaimResultV1
		claimed, err = service.store.ClaimAttachedWorkerOutputUpload(ctx, ydbstore.AttachedWorkerOutputUploadClaimV1{
			Version: request.Version, Authorization: bound, Nonce: request.Nonce, PlanDigest: request.PlanDigest,
			Ordinal: request.Ordinal, RemainingNanoseconds: request.CallerDeadline.RemainingNanoseconds,
		})
		result = ydbstore.AttachedWorkerOutputUploadAdmissionResultV1{Status: claimed.Status, AcceptedAttempt: claimed.AcceptedAttempt,
			CheckedAt: claimed.CheckedAt, Plan: claimed.Plan}
	case attachedworkeruploadwire.Reconcile:
		result, err = service.store.ReconcileAttachedWorkerOutputUpload(ctx, ydbstore.AttachedWorkerOutputUploadReconciliationV1{
			Version: request.Version, Authorization: bound, Nonce: request.Nonce, ProducerDigest: request.ProducerDigest,
			FinalDigest: request.FinalDigest, RemainingNanoseconds: request.CallerDeadline.RemainingNanoseconds,
		})
	default:
		return denied, ErrInvalid
	}
	if err != nil || ctx.Err() != nil {
		return denied, ErrUnavailable
	}
	if result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed {
		// Missing/expired/spent/conflicting observations never grant or erase
		// custody. Do not return stored scope or diagnostics on denial.
		return denied, nil
	}
	if !uploadSnapshotMatchesRequest(request, result) {
		return denied, ErrUnavailable
	}
	ordinals := make([]attachedworkeruploadwire.Ordinal, len(result.Plan.Ordinals))
	for i, ordinal := range result.Plan.Ordinals {
		ordinals[i] = attachedworkeruploadwire.Ordinal{Ordinal: ordinal.Metadata.Ordinal, State: ordinal.State}
	}
	reply := attachedworkeruploadwire.Reply{Status: result.Status, AcceptedAttempt: result.AcceptedAttempt, PlanDigest: result.Plan.PlanDigest,
		CreatedAt: result.Plan.Identity.CreatedAt, CheckedAt: result.CheckedAt, ExpiresAt: result.Plan.Identity.ExpiresAt, Ordinals: ordinals}
	if attachedworkeruploadwire.ValidateReply(reply) != nil {
		return denied, ErrUnavailable
	}
	return reply, nil
}

func sameUploadBoundAuthorization(original, bound ports.AttachedWorkerSealedInputAuthorization) bool {
	if original.PresentedSecretDigest != "" || bound.PresentedSecretDigest.Validate() != nil {
		return false
	}
	bound.PresentedSecretDigest = ""
	return original == bound
}

func uploadSnapshotMatchesRequest(request attachedworkeruploadwire.Request, result ydbstore.AttachedWorkerOutputUploadAdmissionResultV1) bool {
	plan := result.Plan
	if plan.Validate() != nil || plan.Phase != attachedworkeroutput.UploadPlanActiveV1 || plan.Identity.Binding != request.Authorization ||
		plan.Identity.Nonce != request.Nonce || result.CheckedAt.Before(plan.Identity.CreatedAt) || !result.CheckedAt.Before(plan.Identity.ExpiresAt) ||
		result.AcceptedAttempt.Revision != request.Authorization.ExpectedAttemptRevision ||
		!(ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionApplied,
			AttemptRevision: result.AcceptedAttempt.Revision, Attempt: result.AcceptedAttempt}).Matches(request.Authorization) {
		return false
	}
	id := plan.Identity
	switch request.Operation {
	case attachedworkeruploadwire.Admit:
		if id.SessionID != request.SessionID || id.HarnessBindingDigest != request.HarnessBindingDigest || id.ProfileDigest != request.ProfileDigest ||
			id.SchemaDigest != request.SchemaDigest || id.FinalDigest != request.FinalDigest || id.AnswerDigest != request.AnswerDigest ||
			id.ProducerDigest != request.ProducerDigest || len(plan.Ordinals) != len(request.Artifacts) {
			return false
		}
		for i, ordinal := range plan.Ordinals {
			if ordinal.Metadata != request.Artifacts[i] {
				return false
			}
		}
		return true
	case attachedworkeruploadwire.Claim:
		return plan.PlanDigest == request.PlanDigest && request.Ordinal < uint32(len(plan.Ordinals)) &&
			plan.Ordinals[request.Ordinal].State == attachedworkeroutput.UploadOrdinalWritePendingV1
	case attachedworkeruploadwire.Reconcile:
		return id.ProducerDigest == request.ProducerDigest && id.FinalDigest == request.FinalDigest
	default:
		return false
	}
}

var _ CustodyStore = (*ydbstore.Store)(nil)
var _ BearerBinder = (*attachedworkertransport.Service)(nil)
