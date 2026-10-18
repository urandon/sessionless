package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Only useful known local model results use this representation. Error/unknown
// observations retain their existing honest shape and cannot continue. Fixture
// grammar is not promoted and a normalized call grants no tool dispatch.
func (store *Store) localNormalizedModelSuccess(op domain.AttemptOperationV1, o domain.AttemptOperationObservationV1) bool {
	return store.localBootstrap != nil && o.Outcome == domain.AttemptOperationCompletedV1 && o.Reason == domain.AttemptOperationReasonNoneV1 &&
		(op.Kind == domain.AttemptOperationModelV1 || op.Kind == domain.AttemptOperationCompactionV1)
}

// Blob I/O happens outside retryable SQL callbacks. Existing authenticated
// grants, exact original reservation/history and immutable observation CAS
// remain the only Store authority. Decoding with a retained configuration after
// Source retirement cannot authorize a fresh effect or extend cleanup time.
func (store *Store) validateLocalNormalizedModel(ctx context.Context, op domain.AttemptOperationV1, o domain.AttemptOperationObservationV1) error {
	b := store.localBootstrap
	if b == nil || b.self != b || b.store != store || b.configuration.TenantID != op.Scope.TenantID || b.configuration.OwnerUserID != op.Scope.OwnerUserID {
		return domain.ErrMembershipDenied
	}
	request, err := store.readOwnedOperationBody(ctx, op.Scope, op.RequestRef, op.RequestDigest, op.RequestBytes, 1<<20)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(request)
	body, err := store.readOwnedOperationBody(ctx, op.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes, min(op.ResultBytes, 64<<10))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	defer clear(body)
	response, err := configuredinference.DecodeNormalizedResponseBodyV1(b.configuration, request, body, op.Kind)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	if response.Call != nil {
		defer clear(response.Call.Arguments)
	}
	usage, err := response.Usage.OperationUsage()
	expected, _ := json.Marshal(o.Usage)
	actual, _ := json.Marshal(usage)
	if err != nil || !bytes.Equal(actual, expected) || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}
