package domain

import (
	"errors"
	"fmt"
	"time"
)

type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

type ErrorKind string

const (
	ErrorRetryable      ErrorKind = "retryable"
	ErrorTerminal       ErrorKind = "terminal"
	ErrorQuotaBlocked   ErrorKind = "quota_blocked"
	ErrorReauthRequired ErrorKind = "reauthentication_required"
	ErrorPolicyDenied   ErrorKind = "policy_denied"
)

func (kind ErrorKind) Valid() bool {
	switch kind {
	case ErrorRetryable, ErrorTerminal, ErrorQuotaBlocked, ErrorReauthRequired, ErrorPolicyDenied:
		return true
	default:
		return false
	}
}

// ClassifiedError carries a stable machine-readable failure class across
// adapters without exposing provider-specific error objects to the core.
type ClassifiedError struct {
	Kind       ErrorKind
	Code       string
	Operation  string
	RetryAfter time.Duration
	Cause      error
}

func (e *ClassifiedError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := fmt.Sprintf("%s: %s", e.Kind, e.Code)
	if e.Operation != "" {
		message = e.Operation + ": " + message
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *ClassifiedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *ClassifiedError) Retryable() bool {
	return e != nil && e.Kind == ErrorRetryable
}

func ClassifyError(err error) (ErrorKind, bool) {
	var classified *ClassifiedError
	if !errors.As(err, &classified) || classified == nil || !classified.Kind.Valid() {
		return "", false
	}
	return classified.Kind, true
}
