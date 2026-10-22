package attachedworkertransport

import (
	"context"
	"encoding/hex"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func expectedAttachedFailure(err error) bool {
	var validation domain.ValidationError
	var validationPointer *domain.ValidationError
	kind, classified := domain.ClassifyError(err)
	return err == nil || errors.Is(err, context.Canceled) || errors.As(err, &validation) || errors.As(err, &validationPointer) ||
		errors.Is(err, ErrTransportUnauthorized) || errors.Is(err, ErrTransportConflict) || errors.Is(err, ErrTransportConfig) ||
		errors.Is(err, domain.ErrMembershipDenied) || errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) ||
		(classified && (kind == domain.ErrorPolicyDenied || kind == domain.ErrorQuotaBlocked || kind == domain.ErrorReauthRequired))
}

func (service *Service) observeTransportFailure(ctx context.Context, err error) {
	if expectedAttachedFailure(err) {
		return
	}
	opssignal.Emit(ctx, service.opsSignals, opssignal.AttachedTransportFailure, opssignal.Transport, opssignal.Unexpected)
}

// observeTerminalCommit consumes only the transactional finalizer's returned
// authoritative attempt. An inbound terminal, broker acceptance and ACK are not
// canonical completion. This observer creates no additional read or effect.
func (service *Service) observeTerminalCommit(ctx context.Context, bearer ConnectionBearer, connection domain.AttachedWorkerConnection,
	frame attachedworkerprotocol.FrameV1, committed ports.AttachedWorkerAttemptResult, commitErr error,
) {
	if commitErr != nil {
		if expectedAttachedFailure(commitErr) {
			return
		}
		opssignal.Emit(ctx, service.opsSignals, opssignal.AttachedCanonicalUnknown, opssignal.CanonicalReadback, opssignal.Store)
		return
	}
	if committed.Status != ports.AttachedWorkerExecutionApplied && committed.Status != ports.AttachedWorkerExecutionReplayed {
		return
	}
	// Historical replay may deliberately omit the singleton head. Without a
	// qualified typed canonical readback, silence is a coverage gap, not success
	// or an assertion that canonical state has become unknown.
	if committed.Historical || frame.Terminal == nil || !attemptResultMatchesBinding(committed.Attempt, bearer, connection, frame.Terminal.Binding) ||
		committed.Attempt.EnrollmentGeneration != connection.EnrollmentGeneration ||
		committed.Attempt.State != domain.AttachedWorkerAttemptTerminalCommitted || committed.Attempt.TerminalSequence != frame.Terminal.TerminalSequence ||
		committed.Attempt.TerminalEvidenceDigest != domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(frame.Terminal.EvidenceDigest)) ||
		string(committed.Attempt.TerminalStatus) != string(frame.Terminal.Status) {
		return
	}
	switch committed.Attempt.TerminalStatus {
	case domain.AttachedWorkerTerminalSucceeded:
		opssignal.Emit(ctx, service.opsSignals, opssignal.AttachedCanonicalSuccess, opssignal.CanonicalReadback, opssignal.None)
	case domain.AttachedWorkerTerminalFailed:
		opssignal.Emit(ctx, service.opsSignals, opssignal.AttachedExecutionFailure, opssignal.Execution, opssignal.Unexpected)
	}
}
