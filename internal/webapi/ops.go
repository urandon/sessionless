package webapi

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

// ExpectedOperationalDenial recognizes typed admission/validation outcomes,
// never error text. It does not grant access or change the returned error.
func ExpectedOperationalDenial(err error) bool {
	if err == nil {
		return true
	}
	var validation domain.ValidationError
	var stale domain.StaleBindingError
	if errors.As(err, &validation) || errors.As(err, &stale) {
		return true
	}
	if kind, ok := domain.ClassifyError(err); ok && (kind == domain.ErrorQuotaBlocked || kind == domain.ErrorPolicyDenied || kind == domain.ErrorReauthRequired) {
		return true
	}
	for _, expected := range []error{
		ErrResourceUnavailable, ErrComputeUnavailable, sessionapi.ErrSessionUnavailable,
		domain.ErrMembershipDenied, domain.ErrMembershipVersionChanged,
		domain.ErrWebSessionExpired, domain.ErrWebSessionRevoked, domain.ErrWebSessionRotation,
		domain.ErrLoginChallengeConsumed, domain.ErrLoginChallengeExpired,
		domain.ErrExternalIdentityConflict, domain.ErrEnrollmentGrantRequired,
		domain.ErrSessionMutationConflict, domain.ErrEventIdempotencyConflict,
		domain.ErrUploadIntentConflict, domain.ErrUploadIntentExpired, domain.ErrUploadIntentCommitted,
		domain.ErrUploadIntentNotCommitted, domain.ErrUploadIntentClaimed, domain.ErrUploadMismatch,
	} {
		if errors.Is(err, expected) {
			return true
		}
	}
	return false
}

func (service *Service) observeFailure(ctx context.Context, signal opssignal.SignalID, stage opssignal.Stage, class opssignal.ErrorClass, err error) {
	if ExpectedOperationalDenial(err) {
		return
	}
	if errors.Is(err, context.Canceled) {
		class = opssignal.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		class = opssignal.DeadlineExceeded
	}
	opssignal.Emit(ctx, service.opsSignals, signal, stage, class)
}
