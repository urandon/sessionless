package attachedworkeroutput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Inline receipt stages describe the existing output-receipt custody owner,
// not an invocation scheduler, native process lifecycle or caller authority.
type AttachedWorkerInlineStageV2 string

const (
	InlinePreparedV2             AttachedWorkerInlineStageV2 = "prepared"
	InlineBodyClaimedV2          AttachedWorkerInlineStageV2 = "body_claimed"
	InlineCanonicalCopyingV2     AttachedWorkerInlineStageV2 = "canonical_copying"
	InlineReadyV2                AttachedWorkerInlineStageV2 = "ready"
	InlineRetiredWithoutWritesV2 AttachedWorkerInlineStageV2 = "retired_without_writes"
)

// These wire declarations are transport metadata, never producer provenance or
// permission to write. The server's existing receipt owner rechecks authority.
type AttachedWorkerInlinePrepareV2 struct {
	Version       uint32                                       `json:"version"`
	Authorization ports.AttachedWorkerSealedInputAuthorization `json:"authorization"`
	Nonce         domain.IdempotencyKey                        `json:"nonce"`
	Candidate     InlineCandidateV2                            `json:"candidate"`
	Producer      InlineProducerBindingV2                      `json:"producer"`
	Observation   ProcessObservationV1                         `json:"observation"`
	ExpiresAt     time.Time                                    `json:"expires_at"`
}

type AttachedWorkerInlineBodyV2 struct {
	Version       uint32                                       `json:"version"`
	Authorization ports.AttachedWorkerSealedInputAuthorization `json:"authorization"`
	Nonce         domain.IdempotencyKey                        `json:"nonce"`
	Fingerprint   string                                       `json:"fingerprint"`
	Capability    string                                       `json:"capability"`
	ExpiresAt     time.Time                                    `json:"expires_at"`
}

type AttachedWorkerInlineReconcileV2 struct {
	Version       uint32                                       `json:"version"`
	Authorization ports.AttachedWorkerSealedInputAuthorization `json:"authorization"`
	Nonce         domain.IdempotencyKey                        `json:"nonce"`
	Fingerprint   string                                       `json:"fingerprint"`
	// This bit is part of the immutable one-shot client commitment. Recovery
	// after a body send must not return body authority, even if it was lost.
	BodySendBurned bool `json:"body_send_burned"`
}

// A compact response never transports answer/body/materialization copies.
type AttachedWorkerInlineResultV2 struct {
	Version         uint32                                      `json:"version"`
	Stage           AttachedWorkerInlineStageV2                 `json:"stage"`
	Nonce           domain.IdempotencyKey                       `json:"nonce"`
	Fingerprint     string                                      `json:"fingerprint"`
	Capability      string                                      `json:"capability"`
	ExpiresAt       time.Time                                   `json:"expires_at"`
	CanonicalDigest domain.AttachedWorkerTerminalEvidenceDigest `json:"canonical_digest"`
}

// InlineOutputRequestFingerprintV2 preserves the receipt's original V2 byte
// contract. Bearer and mutable expected revision are deliberately omitted;
// hashing metadata proves neither provenance nor current write eligibility.
func InlineOutputRequestFingerprintV2(request AttachedWorkerInlinePrepareV2) (string, error) {
	observation, err := request.Observation.Digest()
	if err != nil {
		return "", err
	}
	binding := request.Authorization
	binding.PresentedSecretDigest = ""
	binding.ExpectedAttemptRevision = 0
	encoded, err := json.Marshal(struct {
		Binding            ports.AttachedWorkerSealedInputAuthorization
		Nonce              domain.IdempotencyKey
		Candidate          InlineCandidateV2
		Producer           InlineProducerBindingV2
		ObservationDigest  string
		CredentialRequired bool
		ExpiresAt          time.Time
	}{binding, request.Nonce, request.Candidate, request.Producer, observation, request.Observation.CredentialReleaseRequired, request.ExpiresAt})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-inline-plan:v2\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}
