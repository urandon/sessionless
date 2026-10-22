package ports

import (
	"context"
	"errors"
)

// Queue control-flow sentinels are shared by polling and HTTP-trigger adapters.
// They preserve the existing adapter outcomes; they do not grant ACK or retry
// authority to an observer.
var (
	ErrQueueEmpty          = errors.New("queue contains no visible message")
	ErrTriggerQueueEmpty   = errors.New("trigger batch contains no remaining messages")
	ErrQueueRetryRequested = errors.New("trigger invocation must be retried")
	ErrQueueRedrivePending = errors.New("message must be retried until YMQ redrive moves it to the DLQ")
)

// IsQueueControlFlow recognizes only finite, typed expected outcomes. In
// particular it never infers policy from error text or a remote response body.
func IsQueueControlFlow(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, ErrQueueEmpty) ||
		errors.Is(err, ErrTriggerQueueEmpty) || errors.Is(err, ErrQueueRetryRequested) ||
		errors.Is(err, ErrQueueRedrivePending)
}
