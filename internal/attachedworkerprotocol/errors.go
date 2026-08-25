// Package attachedworkerprotocol defines the bounded, transport-neutral
// attached-worker wire contract. It performs no network or persistence work.
package attachedworkerprotocol

type ErrorCode string

const (
	ErrorMalformedFrame     ErrorCode = "malformed_frame"
	ErrorFrameTooLarge      ErrorCode = "frame_too_large"
	ErrorUnsupportedVersion ErrorCode = "unsupported_version"
	ErrorProtocolViolation  ErrorCode = "protocol_violation"
	ErrorUnauthorized       ErrorCode = "unauthorized"
	ErrorConflict           ErrorCode = "conflict"
	ErrorRetryLater         ErrorCode = "retry_later"

	ErrorInvalidFrame      = ErrorMalformedFrame
	ErrorInvalidSignature  = ErrorUnauthorized
	ErrorAuthentication    = ErrorUnauthorized
	ErrorInvalidTransition = ErrorProtocolViolation
	ErrorSequenceMismatch  = ErrorProtocolViolation
	ErrorBindingMismatch   = ErrorConflict
)

// ProtocolError deliberately carries no input-derived detail or wrapped error.
type ProtocolError struct {
	Code      ErrorCode
	Retryable bool
}

func (err *ProtocolError) Error() string {
	if err == nil {
		return string(ErrorInvalidFrame)
	}
	return string(err.Code)
}

func protocolError(code ErrorCode) error {
	return &ProtocolError{Code: code, Retryable: code == ErrorRetryLater}
}
