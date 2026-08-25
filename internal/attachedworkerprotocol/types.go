package attachedworkerprotocol

import (
	"crypto/ed25519"
	"crypto/sha256"
	"strings"
	"unicode/utf8"
)

const (
	MaxBatchFrames     = 32
	MaxBatchBytes      = 64 << 10
	MaxAttemptMessages = 64
	MaxProgressUpdates = 32
	maxOpaqueBytes     = 128
)

type MessageKind string

const (
	MessageHello             MessageKind = "hello"
	MessageChallenge         MessageKind = "challenge"
	MessageAttach            MessageKind = "attach"
	MessageAttachAccepted    MessageKind = "attach_accepted"
	MessageReconnect         MessageKind = "reconnect"
	MessageReconnectAccepted MessageKind = "reconnect_accepted"
	MessageManifest          MessageKind = "manifest"
	MessageHeartbeat         MessageKind = "heartbeat"
	MessageLeaseOffer        MessageKind = "lease_offer"
	MessageLeaseClaim        MessageKind = "lease_claim"
	MessageLeaseAccepted     MessageKind = "lease_accepted"
	MessageProgress          MessageKind = "progress"
	MessageCancel            MessageKind = "cancel"
	MessageCancelAck         MessageKind = "cancel_ack"
	MessageTerminal          MessageKind = "terminal"
	MessageTerminalAck       MessageKind = "terminal_ack"
	MessageDrain             MessageKind = "drain"
	MessageDrained           MessageKind = "drained"
	MessageRevoke            MessageKind = "revoke"
	MessageRevoked           MessageKind = "revoked"
	MessageError             MessageKind = "error"
)

type BatchV1 struct {
	Version ProtocolVersion `json:"version"`
	Frames  []FrameV1       `json:"frames"`
}

type FrameV1 struct {
	Version              ProtocolVersion `json:"version"`
	MessageID            string          `json:"message_id"`
	WorkerID             string          `json:"worker_id"`
	EnrollmentGeneration uint64          `json:"enrollment_generation"`
	ConnectionGeneration uint64          `json:"connection_generation"`
	Sequence             uint64          `json:"sequence"`
	Ack                  uint64          `json:"ack"`
	Kind                 MessageKind     `json:"kind"`

	Hello             *HelloV1             `json:"hello,omitempty"`
	Challenge         *ChallengeV1         `json:"challenge,omitempty"`
	Attach            *AttachV1            `json:"attach,omitempty"`
	AttachAccepted    *AttachAcceptedV1    `json:"attach_accepted,omitempty"`
	Reconnect         *ReconnectV1         `json:"reconnect,omitempty"`
	ReconnectAccepted *ReconnectAcceptedV1 `json:"reconnect_accepted,omitempty"`
	Manifest          *ManifestV1          `json:"manifest,omitempty"`
	Heartbeat         *HeartbeatV1         `json:"heartbeat,omitempty"`
	LeaseOffer        *LeaseOfferV1        `json:"lease_offer,omitempty"`
	LeaseClaim        *LeaseClaimV1        `json:"lease_claim,omitempty"`
	LeaseAccepted     *LeaseAcceptedV1     `json:"lease_accepted,omitempty"`
	Progress          *ProgressV1          `json:"progress,omitempty"`
	Cancel            *CancelV1            `json:"cancel,omitempty"`
	CancelAck         *CancelAckV1         `json:"cancel_ack,omitempty"`
	Terminal          *TerminalV1          `json:"terminal,omitempty"`
	TerminalAck       *TerminalAckV1       `json:"terminal_ack,omitempty"`
	Drain             *DrainV1             `json:"drain,omitempty"`
	Drained           *DrainedV1           `json:"drained,omitempty"`
	Revoke            *RevokeV1            `json:"revoke,omitempty"`
	Revoked           *RevokedV1           `json:"revoked,omitempty"`
	Error             *ErrorV1             `json:"error,omitempty"`
}

type HelloV1 struct {
	Offer       VersionOfferV1 `json:"offer"`
	WorkerNonce []byte         `json:"worker_nonce"`
}

type ChallengeV1 struct {
	WorkerOffer     VersionOfferV1  `json:"worker_offer"`
	PlatformOffer   VersionOfferV1  `json:"platform_offer"`
	SelectedVersion ProtocolVersion `json:"selected_version"`
	WorkerNonce     []byte          `json:"worker_nonce"`
	PlatformNonce   []byte          `json:"platform_nonce"`
}

type AttachV1 struct {
	WorkerOffer      VersionOfferV1  `json:"worker_offer"`
	PlatformOffer    VersionOfferV1  `json:"platform_offer"`
	SelectedVersion  ProtocolVersion `json:"selected_version"`
	WorkerNonce      []byte          `json:"worker_nonce"`
	PlatformNonce    []byte          `json:"platform_nonce"`
	CapabilityDigest []byte          `json:"capability_digest"`
	Signature        []byte          `json:"signature"`
}

type ReconnectV1 struct {
	WorkerOffer                  VersionOfferV1         `json:"worker_offer"`
	PlatformOffer                VersionOfferV1         `json:"platform_offer"`
	SelectedVersion              ProtocolVersion        `json:"selected_version"`
	PreviousConnectionGeneration uint64                 `json:"previous_connection_generation"`
	WorkerNonce                  []byte                 `json:"worker_nonce"`
	PlatformNonce                []byte                 `json:"platform_nonce"`
	CapabilityDigest             []byte                 `json:"capability_digest"`
	PreviousWatermarks           ConnectionWatermarksV1 `json:"previous_watermarks"`
	AttemptSummary               AttemptSummaryV1       `json:"attempt_summary"`
	Signature                    []byte                 `json:"signature"`
}

type AttachAcceptedV1 struct {
	WorkerOffer      VersionOfferV1  `json:"worker_offer"`
	PlatformOffer    VersionOfferV1  `json:"platform_offer"`
	SelectedVersion  ProtocolVersion `json:"selected_version"`
	WorkerNonce      []byte          `json:"worker_nonce"`
	PlatformNonce    []byte          `json:"platform_nonce"`
	CapabilityDigest []byte          `json:"capability_digest"`
}

type ReconnectAcceptedV1 struct {
	WorkerOffer             VersionOfferV1         `json:"worker_offer"`
	PlatformOffer           VersionOfferV1         `json:"platform_offer"`
	SelectedVersion         ProtocolVersion        `json:"selected_version"`
	WorkerNonce             []byte                 `json:"worker_nonce"`
	PlatformNonce           []byte                 `json:"platform_nonce"`
	CapabilityDigest        []byte                 `json:"capability_digest"`
	AuthoritativeWatermarks ConnectionWatermarksV1 `json:"authoritative_watermarks"`
	AuthoritativeAttempt    AttemptSummaryV1       `json:"authoritative_attempt"`
}

type ManifestV1 struct {
	Manifest  CapabilityManifestV1 `json:"manifest"`
	Digest    []byte               `json:"digest"`
	Signature []byte               `json:"signature"`
}

// HeartbeatV1 carries volatile presence and is excluded from the signed
// immutable capability manifest.
type HeartbeatV1 struct {
	ObservedAtUnixMicro int64  `json:"observed_at_unix_micro"`
	Available           bool   `json:"available"`
	ActiveAttempts      uint32 `json:"active_attempts"`
}

type AttemptBindingV1 struct {
	RunID              string `json:"run_id"`
	AttemptID          string `json:"attempt_id"`
	LeaseID            string `json:"lease_id"`
	LeaseGeneration    uint64 `json:"lease_generation"`
	FenceToken         string `json:"fence_token"`
	ExpiresAtUnixMicro int64  `json:"expires_at_unix_micro"`
	ContextDigest      []byte `json:"context_digest"`
	CapabilityDigest   []byte `json:"capability_digest"`
	PolicyDigest       []byte `json:"policy_digest"`
}

type LeaseOfferV1 struct {
	Binding         AttemptBindingV1 `json:"binding"`
	AttemptSequence uint64           `json:"attempt_sequence"`
}

type LeaseClaimV1 struct {
	Binding         AttemptBindingV1 `json:"binding"`
	AttemptSequence uint64           `json:"attempt_sequence"`
}

type LeaseAcceptedV1 struct {
	Binding         AttemptBindingV1 `json:"binding"`
	AttemptSequence uint64           `json:"attempt_sequence"`
}

type ProgressStage string

const (
	ProgressStarted ProgressStage = "started"
	ProgressActive  ProgressStage = "active"
)

type ProgressV1 struct {
	Binding          AttemptBindingV1 `json:"binding"`
	AttemptSequence  uint64           `json:"attempt_sequence"`
	ProgressSequence uint64           `json:"progress_sequence"`
	Stage            ProgressStage    `json:"stage"`
}

type CancelCode string

const (
	CancelRequested CancelCode = "requested"
	CancelFenced    CancelCode = "fenced"
)

type CancelV1 struct {
	Binding         AttemptBindingV1 `json:"binding"`
	AttemptSequence uint64           `json:"attempt_sequence"`
	CancelRevision  uint64           `json:"cancel_revision"`
	Code            CancelCode       `json:"code"`
}

type CancelAckV1 struct {
	Binding         AttemptBindingV1 `json:"binding"`
	AttemptSequence uint64           `json:"attempt_sequence"`
	CancelRevision  uint64           `json:"cancel_revision"`
}

type TerminalStatus string

const (
	TerminalSucceeded TerminalStatus = "succeeded"
	TerminalFailed    TerminalStatus = "failed"
	TerminalCancelled TerminalStatus = "cancelled"
)

type TerminalResult string

const (
	TerminalResultCompleted TerminalResult = "completed"
	TerminalResultFailed    TerminalResult = "failed"
	TerminalResultCancelled TerminalResult = "cancelled"
)

type TerminalV1 struct {
	Binding          AttemptBindingV1 `json:"binding"`
	AttemptSequence  uint64           `json:"attempt_sequence"`
	TerminalSequence uint64           `json:"terminal_sequence"`
	Status           TerminalStatus   `json:"status"`
	Result           TerminalResult   `json:"result"`
	EvidenceDigest   []byte           `json:"evidence_digest"`
}

type TerminalAckV1 struct {
	Binding          AttemptBindingV1 `json:"binding"`
	AttemptSequence  uint64           `json:"attempt_sequence"`
	TerminalSequence uint64           `json:"terminal_sequence"`
	Status           TerminalStatus   `json:"status"`
	Result           TerminalResult   `json:"result"`
	EvidenceDigest   []byte           `json:"evidence_digest"`
}

type DrainV1 struct {
	Revision uint64 `json:"revision"`
}

type DrainedV1 struct {
	Revision uint64 `json:"revision"`
}

type RevokeV1 struct {
	Revision                 uint64 `json:"revision"`
	NextEnrollmentGeneration uint64 `json:"next_enrollment_generation"`
	NextConnectionGeneration uint64 `json:"next_connection_generation"`
}

type RevokedV1 struct {
	Revision                 uint64 `json:"revision"`
	NextEnrollmentGeneration uint64 `json:"next_enrollment_generation"`
	NextConnectionGeneration uint64 `json:"next_connection_generation"`
}

type ErrorV1 struct {
	Code      ErrorCode `json:"code"`
	Retryable bool      `json:"retryable"`
}

func (batch BatchV1) Validate() error {
	if batch.Version != ProtocolVersionV1 || len(batch.Frames) == 0 || len(batch.Frames) > MaxBatchFrames {
		return protocolError(ErrorInvalidFrame)
	}
	for index := range batch.Frames {
		if err := batch.Frames[index].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (frame FrameV1) Validate() error {
	if validateFrameCommon(frame) != nil {
		return protocolError(ErrorInvalidFrame)
	}
	payloads := 0
	check := func(kind MessageKind, present bool, validate func() error) error {
		if !present {
			return nil
		}
		payloads++
		if frame.Kind != kind || validate() != nil {
			return protocolError(ErrorInvalidFrame)
		}
		return nil
	}
	checks := []error{
		check(MessageHello, frame.Hello != nil, func() error { return frame.Hello.Validate() }),
		check(MessageChallenge, frame.Challenge != nil, func() error { return frame.Challenge.Validate() }),
		check(MessageAttach, frame.Attach != nil, func() error { return frame.Attach.Validate() }),
		check(MessageAttachAccepted, frame.AttachAccepted != nil, func() error { return frame.AttachAccepted.Validate() }),
		check(MessageReconnect, frame.Reconnect != nil, func() error { return frame.Reconnect.Validate() }),
		check(MessageReconnectAccepted, frame.ReconnectAccepted != nil, func() error { return frame.ReconnectAccepted.Validate() }),
		check(MessageManifest, frame.Manifest != nil, func() error { return frame.Manifest.Validate() }),
		check(MessageHeartbeat, frame.Heartbeat != nil, func() error { return frame.Heartbeat.Validate() }),
		check(MessageLeaseOffer, frame.LeaseOffer != nil, func() error { return frame.LeaseOffer.Validate() }),
		check(MessageLeaseClaim, frame.LeaseClaim != nil, func() error { return frame.LeaseClaim.Validate() }),
		check(MessageLeaseAccepted, frame.LeaseAccepted != nil, func() error { return frame.LeaseAccepted.Validate() }),
		check(MessageProgress, frame.Progress != nil, func() error { return frame.Progress.Validate() }),
		check(MessageCancel, frame.Cancel != nil, func() error { return frame.Cancel.Validate() }),
		check(MessageCancelAck, frame.CancelAck != nil, func() error { return frame.CancelAck.Validate() }),
		check(MessageTerminal, frame.Terminal != nil, func() error { return frame.Terminal.Validate() }),
		check(MessageTerminalAck, frame.TerminalAck != nil, func() error { return frame.TerminalAck.Validate() }),
		check(MessageDrain, frame.Drain != nil, func() error { return frame.Drain.Validate() }),
		check(MessageDrained, frame.Drained != nil, func() error { return frame.Drained.Validate() }),
		check(MessageRevoke, frame.Revoke != nil, func() error { return frame.Revoke.Validate() }),
		check(MessageRevoked, frame.Revoked != nil, func() error { return frame.Revoked.Validate() }),
		check(MessageError, frame.Error != nil, func() error { return frame.Error.Validate() }),
	}
	for _, err := range checks {
		if err != nil {
			return err
		}
	}
	if payloads != 1 {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message HelloV1) Validate() error {
	if message.Offer.Validate() != nil || !validBytes(message.WorkerNonce, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message ChallengeV1) Validate() error {
	if message.WorkerOffer.Validate() != nil || message.PlatformOffer.Validate() != nil || message.SelectedVersion == 0 ||
		!validBytes(message.WorkerNonce, sha256.Size) || !validBytes(message.PlatformNonce, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message AttachV1) Validate() error {
	if validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
		message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest) != nil ||
		!validBytes(message.Signature, ed25519.SignatureSize) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message ReconnectV1) Validate() error {
	if message.PreviousConnectionGeneration == 0 ||
		validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
			message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest) != nil ||
		message.PreviousWatermarks.Validate() != nil || message.AttemptSummary.Validate() != nil ||
		!validBytes(message.Signature, ed25519.SignatureSize) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message AttachAcceptedV1) Validate() error {
	return validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
		message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest)
}

func (message ReconnectAcceptedV1) Validate() error {
	if validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
		message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest) != nil ||
		message.AuthoritativeWatermarks.Validate() != nil || message.AuthoritativeAttempt.Validate() != nil {
		return protocolError(ErrorMalformedFrame)
	}
	return nil
}

func (message ManifestV1) Validate() error {
	if message.Manifest.Validate() != nil || !validBytes(message.Digest, sha256.Size) ||
		!validBytes(message.Signature, ed25519.SignatureSize) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message HeartbeatV1) Validate() error {
	if message.ObservedAtUnixMicro <= 0 || message.ActiveAttempts > 1 || (message.Available && message.ActiveAttempts != 0) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (binding AttemptBindingV1) Validate() error {
	if !validOpaque(binding.RunID) || !validOpaque(binding.AttemptID) || !validOpaque(binding.LeaseID) ||
		binding.LeaseGeneration == 0 || !validOpaque(binding.FenceToken) || binding.ExpiresAtUnixMicro <= 0 ||
		!validBytes(binding.ContextDigest, sha256.Size) || !validBytes(binding.CapabilityDigest, sha256.Size) ||
		!validBytes(binding.PolicyDigest, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func validateAttempt(binding AttemptBindingV1, sequence uint64) error {
	if binding.Validate() != nil || sequence == 0 || sequence > MaxAttemptMessages {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message LeaseOfferV1) Validate() error {
	return validateAttempt(message.Binding, message.AttemptSequence)
}
func (message LeaseClaimV1) Validate() error {
	return validateAttempt(message.Binding, message.AttemptSequence)
}
func (message LeaseAcceptedV1) Validate() error {
	return validateAttempt(message.Binding, message.AttemptSequence)
}

func (message ProgressV1) Validate() error {
	if validateAttempt(message.Binding, message.AttemptSequence) != nil || message.ProgressSequence == 0 ||
		message.ProgressSequence > MaxProgressUpdates ||
		(message.Stage != ProgressStarted && message.Stage != ProgressActive) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message CancelV1) Validate() error {
	if validateAttempt(message.Binding, message.AttemptSequence) != nil || message.CancelRevision == 0 ||
		(message.Code != CancelRequested && message.Code != CancelFenced) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message CancelAckV1) Validate() error {
	if validateAttempt(message.Binding, message.AttemptSequence) != nil || message.CancelRevision == 0 {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message TerminalV1) Validate() error {
	if validateAttempt(message.Binding, message.AttemptSequence) != nil || message.TerminalSequence == 0 ||
		!validTerminalResult(message.Status, message.Result) || !validBytes(message.EvidenceDigest, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message TerminalAckV1) Validate() error {
	if validateAttempt(message.Binding, message.AttemptSequence) != nil || message.TerminalSequence == 0 ||
		!validTerminalResult(message.Status, message.Result) || !validBytes(message.EvidenceDigest, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message DrainV1) Validate() error {
	if message.Revision == 0 {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message DrainedV1) Validate() error { return DrainV1(message).Validate() }

func (message RevokeV1) Validate() error {
	if message.Revision == 0 || message.NextEnrollmentGeneration == 0 || message.NextConnectionGeneration == 0 {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func (message RevokedV1) Validate() error { return RevokeV1(message).Validate() }

func (message ErrorV1) Validate() error {
	switch message.Code {
	case ErrorMalformedFrame, ErrorFrameTooLarge, ErrorUnsupportedVersion, ErrorProtocolViolation,
		ErrorUnauthorized, ErrorConflict, ErrorRetryLater:
		if message.Retryable != (message.Code == ErrorRetryLater) {
			return protocolError(ErrorInvalidFrame)
		}
		return nil
	default:
		return protocolError(ErrorInvalidFrame)
	}
}

func validateNegotiationProof(
	workerOffer, platformOffer VersionOfferV1,
	selected ProtocolVersion,
	workerNonce, platformNonce, capabilityDigest []byte,
) error {
	if workerOffer.Validate() != nil || platformOffer.Validate() != nil || selected == 0 ||
		!validBytes(workerNonce, sha256.Size) || !validBytes(platformNonce, sha256.Size) ||
		!validBytes(capabilityDigest, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func validTerminal(status TerminalStatus) bool {
	return status == TerminalSucceeded || status == TerminalFailed || status == TerminalCancelled
}

func validTerminalResult(status TerminalStatus, result TerminalResult) bool {
	switch status {
	case TerminalSucceeded:
		return result == TerminalResultCompleted
	case TerminalFailed:
		return result == TerminalResultFailed
	case TerminalCancelled:
		return result == TerminalResultCancelled
	default:
		return false
	}
}

func validateFrameCommon(frame FrameV1) error {
	if frame.Version != ProtocolVersionV1 || !validOpaque(frame.MessageID) || !validOpaque(frame.WorkerID) ||
		frame.EnrollmentGeneration == 0 || frame.ConnectionGeneration == 0 || frame.Sequence == 0 {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}

func validOpaque(value string) bool {
	if value == "" || len(value) > maxOpaqueBytes || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e || character == '/' || character == '\\' {
			return false
		}
	}
	return true
}

func validBytes(value []byte, size int) bool { return len(value) == size }
