package worker

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// runLocalOwned joins the original Manager admission/lease/reservation to the
// concrete session's existing composer, cleanup owner and canonical finalizer.
// There is no V1 evidence conversion, caller-authored completion or replacement
// executor. RunModelStep is currently model-only: this route does not establish
// the separately required typed tool/result loop or a real model qualification.
func (manager *Manager) runLocalOwned(ctx context.Context, message ports.ReceivedMessage, loaded ports.WorkerJobState,
	lease domain.Lease, effect ports.ReserveAttemptEffectResultV1,
) (Outcome, error) {
	session := manager.config.LocalOwnedSession
	executionCtx, cancelExecution := context.WithTimeout(ctx, loaded.Job.Limits.MaxRuntime)
	defer cancelExecution()
	leaseState := &invocationLease{manager: manager, tenantID: loaded.Run.TenantID, lease: lease}
	watchdogCtx, cancelWatchdog := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWatchdog()
	watchdog := manager.startLeaseWatchdog(watchdogCtx, cancelExecution, loaded, leaseState)
	defer watchdog.Stop()
	// Close always joins this original keeper/allocation and owned buffers. It
	// does not stop the borrowed Source, database or user's inference server.
	defer session.Close()
	cleanupFinished, cleanupObserved := false, false
	cleanupObservationCtx := ctx
	observeClose := func(closeErr error) {
		if cleanupObserved {
			return
		}
		if closeErr != nil {
			manager.observeManagedFailure(cleanupObservationCtx, cleanupObservationCtx, opssignal.ManagedCleanupFailure, opssignal.Cleanup, closeErr)
			cleanupObserved = true
		} else if cleanupFinished {
			// The original typed physical cleanup plus its successful final
			// join is positive cleanup evidence even if canonical commit fails.
			opssignal.Emit(cleanupObservationCtx, manager.config.OpsSignals, opssignal.ManagedCleanupSuccess, opssignal.Cleanup, opssignal.None)
			cleanupObserved = true
		}
	}
	fail := func(cause error) (Outcome, error) {
		cancelExecution()
		cancelWatchdog()
		watchErr := watchdog.Stop()
		closeErr := session.Close()
		observeClose(closeErr)
		// Unknown/partial terminal work stays reconciliation-only. In particular
		// do not call the generic V1 failure/success writer with local DATA.
		return manager.retry(context.Background(), message, errors.Join(cause, watchErr, closeErr))
	}
	if _, err := session.EnterWorkerEffect(executionCtx, effect); err != nil {
		manager.observeManagedDeadline(ctx, executionCtx, err)
		return fail(err)
	}
	if modelResult, err := session.RunModelStep(executionCtx); err != nil {
		manager.observeManagedModelStep(ctx, executionCtx, modelResult, err)
		return fail(err)
	}
	terminal, err := session.PrepareTerminal(executionCtx)
	if err != nil {
		manager.observeManagedDeadline(ctx, executionCtx, err)
		return fail(err)
	}
	if _, err := terminal.PrepareOutput(executionCtx); err != nil {
		manager.observeManagedDeadline(ctx, executionCtx, err)
		return fail(err)
	}
	if err := terminal.PublishOutput(executionCtx); err != nil {
		manager.observeManagedFailure(ctx, executionCtx, opssignal.ManagedPublicationFailure, opssignal.Publication, err)
		return fail(err)
	}
	deadline, err := terminal.Deadline()
	if err != nil {
		return fail(err)
	}
	// Cleanup may outlive caller/Source cancellation, but never its original
	// absolute terminal deadline. The original concrete owner alone produces
	// stop, retirement, custody and erasure receipts before completion.
	completionCtx, cancelCompletion := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancelCompletion()
	cleanupObservationCtx = completionCtx
	cleanup, err := terminal.FinishCleanup(completionCtx)
	if err != nil {
		manager.observeManagedFailure(ctx, completionCtx, opssignal.ManagedCleanupFailure, opssignal.Cleanup, err)
		cleanupObserved = true
		return fail(err)
	}
	cleanupFinished = cleanup != nil
	cancelWatchdog()
	// Durable cancellation is an outcome for the SAME finalizer after actual
	// cleanup, not lease loss. It must still rejoin the current cancellation,
	// lease and fence before committing the fixed cancelled notice. Every other
	// watchdog error remains fatal; no caller or prepared DTO supplies success.
	if err := watchdog.Stop(); err != nil && !errors.Is(err, errWorkerCancellationObserved) {
		return fail(err)
	}
	lease = leaseState.Current()
	if _, err := manager.ensureLease(completionCtx, loaded.Run.TenantID, lease); err != nil {
		return fail(err)
	}
	if err := cleanup.Complete(completionCtx); err != nil {
		manager.observeManagedUnknown(completionCtx, err)
		return fail(err)
	}
	if err := session.Close(); err != nil {
		observeClose(err)
		return fail(err)
	}
	observeClose(nil)
	// The original transaction may have observed a late cancellation. Read its
	// actual canonical result; a prepared success candidate is not the outcome.
	completed, found, err := manager.state.LoadWorkerJob(completionCtx, loaded.Run.TenantID, loaded.Run.ID)
	manager.observeManagedReadback(completionCtx, completed, loaded.Run.TenantID, loaded.Run.ID, loaded.Attempt.ID, found, err)
	if err != nil || !found || completed.Run.ID != loaded.Run.ID || completed.Attempt.ID != loaded.Attempt.ID ||
		(completed.Run.Status != domain.RunSucceeded && completed.Run.Status != domain.RunCancelled) {
		return fail(errors.Join(errors.New("local canonical completion result unavailable"), err))
	}
	if err := manager.config.ProjectionWakePublisher.PublishFrontendProjectionWake(
		completionCtx, completed.Run.TenantID, completed.Run.ID, completed.Run.UpdatedAt,
	); err != nil {
		return fail(err)
	}
	if err := manager.queue.Ack(completionCtx, message.ReceiptHandle); err != nil {
		return "", err
	}
	if completed.Run.Status == domain.RunCancelled {
		return OutcomeCancelled, nil
	}
	return OutcomeCompleted, nil
}
