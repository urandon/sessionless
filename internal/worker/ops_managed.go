package worker

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
)

func managedCanonicalJob(loaded ports.WorkerJobState) bool {
	return loaded.Job.Origin != nil && loaded.Job.ExecutionPlacementV2.Version == domain.ExecutionPlacementVersionV2 && loaded.Job.ExecutionPlacementV2.Kind == domain.ExecutionPlacementManaged
}

func managedExpectedDenial(err error) bool {
	if errors.Is(err, domain.ErrMembershipDenied) || errors.Is(err, context.Canceled) || errors.Is(err, providerloopsession.ErrBusy) {
		return true
	}
	var invalid domain.ValidationError
	var invalidPointer *domain.ValidationError
	if errors.As(err, &invalid) || errors.As(err, &invalidPointer) {
		return true
	}
	kind, classified := domain.ClassifyError(err)
	return classified && (kind == domain.ErrorQuotaBlocked || kind == domain.ErrorPolicyDenied || kind == domain.ErrorReauthRequired)
}

// The existing authority context is only observed. In particular no fresh
// background context or timeout is minted to preserve a telemetry record.
// A timeout may be reported through the still-live original parent: its record
// deadline_state describes that parent, while OriginalDeadline identifies the
// child authority's actually observed expiration. A canceled parent drops it.
func (manager *Manager) observeManagedFailure(parent, authority context.Context, signal opssignal.SignalID, stage opssignal.Stage, err error) {
	if err == nil {
		return
	}
	if manager.observeManagedDeadline(parent, authority, err) {
		return
	}
	if managedExpectedDenial(err) {
		return
	}
	if authority != nil && authority.Err() != nil {
		return
	}
	// Store/object-store details are deliberately collapsed by the original
	// local owners. Do not infer a cause from ErrSession or a raw error string.
	opssignal.Emit(authority, manager.config.OpsSignals, signal, stage, opssignal.Unexpected)
}

// This predicate permits deadline-only observations at prepublication stages
// where a collapsed owner refusal must not be called an execution/I/O outage.
func (manager *Manager) observeManagedDeadline(parent, authority context.Context, err error) bool {
	if err == nil {
		return false
	}
	// An original owner can intentionally collapse an expired call to a
	// membership refusal. The positively observed authority expiration takes
	// precedence; an active-context refusal still produces no outage event.
	// A raw nested timeout does not prove this authority expired: the manager
	// retries such errors while its original execution context is still live.
	if authority != nil && errors.Is(authority.Err(), context.DeadlineExceeded) {
		opssignal.Emit(parent, manager.config.OpsSignals, opssignal.ManagedDeadlineExceeded, opssignal.OriginalDeadline, opssignal.DeadlineExceeded)
		return true
	}
	return false
}

func (manager *Manager) observeManagedModelStep(parent, authority context.Context, result providerloopsession.ModelStepResultV1, err error) {
	if err == nil {
		return
	}
	if manager.observeManagedDeadline(parent, authority, err) {
		return
	}
	// A denied preparation/entry is not evidence that model execution failed.
	// These flags come from the original session's actual transport result.
	if result.Model.SendAttempted || (result.Compaction != nil && result.Compaction.SendAttempted) {
		manager.observeManagedFailure(parent, authority, opssignal.ManagedExecutionFailure, opssignal.Execution, err)
	}
}

func (manager *Manager) observeManagedUnknown(ctx context.Context, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	class := opssignal.None
	if err != nil && !managedExpectedDenial(err) {
		class = opssignal.Unexpected
	}
	opssignal.Emit(ctx, manager.config.OpsSignals, opssignal.ManagedCanonicalUnknown, opssignal.CanonicalReadback, class)
}

// Scope is joined against the already loaded result; this adds no store read.
// A successful prepared candidate, V1 Complete(nil), queue ACK or wake is not
// this readback. Local-development/deterministic observations do not qualify
// real managed-cloud execution, regardless of the canonical state they expose.
func (manager *Manager) observeManagedReadback(ctx context.Context, loaded ports.WorkerJobState, tenant domain.TenantID, run domain.RunID, attempt domain.AttemptID, found bool, err error) {
	if !found || err != nil {
		manager.observeManagedUnknown(ctx, err)
		return
	}
	if !managedCanonicalJob(loaded) {
		return
	}
	if tenant == "" || run == "" || attempt == "" || loaded.Run.TenantID != tenant || loaded.Run.ID != run ||
		loaded.Attempt.TenantID != tenant || loaded.Attempt.ID != attempt || loaded.Attempt.RunID != run ||
		loaded.Job.TenantID != tenant || loaded.Job.RunID != run || loaded.Job.AttemptID != attempt ||
		loaded.Job.SessionID != loaded.Run.SessionID || loaded.Job.TriggerEventID != loaded.Run.TriggerEventID {
		manager.observeManagedUnknown(ctx, nil)
		return
	}
	if loaded.Run.Status == domain.RunSucceeded && loaded.Attempt.Status == domain.AttemptSucceeded {
		opssignal.Emit(ctx, manager.config.OpsSignals, opssignal.ManagedCanonicalSuccess, opssignal.CanonicalReadback, opssignal.None)
		return
	}
	if loaded.Run.Status == domain.RunCancelled && loaded.Attempt.Status == domain.AttemptCancelled {
		return // A durable cancellation is not an assistant-answer success.
	}
	if loaded.Run.Status == domain.RunFailed && loaded.Attempt.Status == domain.AttemptFailed {
		return
	} // Known failure is not an unknown outcome.
	manager.observeManagedUnknown(ctx, nil)
}
