package worker

import (
	"context"
	"errors"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
)

func (m *Manager) finishLocalKnownMCPFailure(ctx context.Context, message ports.ReceivedMessage, loaded ports.WorkerJobState, lease *invocationLease, watchdog *leaseWatchdog, cancelExecution, cancelWatchdog context.CancelFunc, failure *providerloopsession.PreparedLocalFailureV1) (Outcome, error) {
	session := m.config.LocalOwnedSession
	fail := func(cause error) (Outcome, error) {
		cancelExecution()
		cancelWatchdog()
		watchErr := watchdog.Stop()
		closeErr := session.Close()
		return m.retry(context.Background(), message, errors.Join(cause, watchErr, closeErr))
	}
	deadline, err := failure.Deadline()
	if err != nil {
		return fail(err)
	}
	completion, cancel := context.WithDeadline(context.WithoutCancel(ctx), deadline)
	defer cancel()
	cancelWatchdog()
	if err := watchdog.Stop(); err != nil {
		return fail(err)
	}
	if _, err := m.ensureLease(completion, loaded.Run.TenantID, lease.Current()); err != nil {
		return fail(err)
	}
	if err := failure.PrepareOutput(completion); err != nil {
		return fail(fmt.Errorf("known MCP failure prepare: %w", err))
	}
	if err := failure.PublishOutput(completion); err != nil {
		return fail(fmt.Errorf("known MCP failure publish: %w", err))
	}
	if err := failure.FinishCleanup(completion); err != nil {
		return fail(fmt.Errorf("known MCP failure cleanup: %w", err))
	}
	if err := failure.Complete(completion); err != nil {
		m.observeManagedUnknown(completion, err)
		return fail(fmt.Errorf("known MCP failure complete: %w", err))
	}
	if err := session.Close(); err != nil {
		return fail(err)
	}
	cancelExecution() // consumed execution parent edge must detach before ACK
	// joined_retained deliberately emits no cleanup-success/erasure signal.
	got, found, err := m.state.LoadWorkerJob(completion, loaded.Run.TenantID, loaded.Run.ID)
	if err != nil || !found || got.Run.ID != loaded.Run.ID || got.Attempt.ID != loaded.Attempt.ID || got.Run.Status != domain.RunFailed || got.Attempt.Status != domain.AttemptFailed || got.Run.TerminalFailure == nil || got.Run.TerminalFailure.Stop != domain.KnownMCPServiceErrorV1 || got.Run.TerminalFailure.Cleanup != "joined_retained" {
		return fail(errors.Join(errors.New("known MCP failure canonical result unavailable"), err))
	}
	if err := m.config.ProjectionWakePublisher.PublishFrontendProjectionWake(completion, got.Run.TenantID, got.Run.ID, got.Run.UpdatedAt); err != nil {
		return fail(err)
	}
	if err := m.queue.Ack(completion, message.ReceiptHandle); err != nil {
		return "", err
	}
	return OutcomeFailed, nil
}
