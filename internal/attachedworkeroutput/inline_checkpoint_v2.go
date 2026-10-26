package attachedworkeroutput

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const MaxInlineCheckpointBytesV2 = 32 << 10

// InlineReceiptPlanV2 is immutable metadata, not producer provenance or write
// authority. It deliberately has no answer, original body, bearer or capability.
// Authorization retains its established ports JSON shape; new fields use
// explicit snake_case. The private native owner preserves exact original UTC
// instants without quantization.
type InlineReceiptPlanV2 struct {
	Authorization           ports.AttachedWorkerSealedInputAuthorization `json:"authorization"`
	Nonce                   domain.IdempotencyKey                        `json:"nonce"`
	Fingerprint             string                                       `json:"fingerprint"`
	Producer                InlineProducerBindingV2                      `json:"producer"`
	Artifacts               []InlineArtifactV2                           `json:"artifacts"`
	ObservationDigest       string                                       `json:"observation_digest"`
	ExpiresAt               time.Time                                    `json:"expires_at"`
	WorkerAttemptSequence   uint64                                       `json:"worker_attempt_sequence"`
	PlatformAttemptSequence uint64                                       `json:"platform_attempt_sequence"`
	TerminalSequence        uint64                                       `json:"terminal_sequence"`
}

type InlineReceiptProgressV2 struct {
	Revision        uint64                                      `json:"revision"`
	PrepareBurned   bool                                        `json:"prepare_burned"`
	BodyBurned      bool                                        `json:"body_burned"`
	ReconcileBurned bool                                        `json:"reconcile_burned"`
	CanonicalDigest domain.AttachedWorkerTerminalEvidenceDigest `json:"canonical_digest"`
	TerminalAcked   bool                                        `json:"terminal_acked"`
}

// A decoded checkpoint is only a bounded metadata commitment. Its owner must
// separately enforce an initial-only seal, durable CAS winner, provenance and
// immutable outer identity; decoding/AdvanceFrom never grants a fresh send.
type InlineReceiptCheckpointV2 struct {
	Version    uint32                  `json:"version"`
	Plan       InlineReceiptPlanV2     `json:"plan"`
	PlanSHA256 string                  `json:"plan_sha256"`
	Progress   InlineReceiptProgressV2 `json:"progress"`
}

func (p InlineReceiptPlanV2) Digest() (string, error) {
	if !validInlineReceiptPlanV2(p) {
		return "", ErrCandidateInvalid
	}
	raw, err := json.Marshal(p)
	defer clear(raw)
	if err != nil || len(raw) > MaxInlineCheckpointBytesV2 {
		return "", ErrCandidateInvalid
	}
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless:attached-worker-inline-checkpoint-plan:v2\x00"))
	_, _ = h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (c InlineReceiptCheckpointV2) Validate() error {
	digest, err := c.Plan.Digest()
	if c.Version != 2 || err != nil || !validInlineDigestV2(c.PlanSHA256) || digest != c.PlanSHA256 {
		return ErrCandidateInvalid
	}
	p := c.Progress
	if !p.PrepareBurned || p.Revision == 0 || p.Revision > 6 ||
		p.BodyBurned && len(c.Plan.Artifacts) == 0 || p.TerminalAcked && p.CanonicalDigest == "" {
		return ErrCandidateInvalid
	}
	if p.CanonicalDigest != "" && (!validInlineDigestV2(string(p.CanonicalDigest)) || len(c.Plan.Artifacts) > 0 && !p.BodyBurned) {
		return ErrCandidateInvalid
	}
	expected := uint64(1)
	for _, fact := range []bool{p.BodyBurned, p.ReconcileBurned, p.CanonicalDigest != "", p.TerminalAcked} {
		if fact {
			expected++
		}
	}
	if p.Revision != expected {
		return ErrCandidateInvalid
	}
	return nil
}

// AdvanceFrom allows an identical idempotent observation, or exactly one new
// monotonic fact at the next revision. An identical result is not a CAS win.
func (c InlineReceiptCheckpointV2) AdvanceFrom(previous InlineReceiptCheckpointV2) error {
	if c.Validate() != nil || previous.Validate() != nil || c.PlanSHA256 != previous.PlanSHA256 {
		return ErrCandidateInvalid
	}
	plan, err := json.Marshal(c.Plan)
	defer clear(plan)
	priorPlan, priorErr := json.Marshal(previous.Plan)
	defer clear(priorPlan)
	if err != nil || priorErr != nil || !bytes.Equal(plan, priorPlan) {
		return ErrCandidateInvalid
	}
	next, prior := c.Progress, previous.Progress
	if next == prior {
		return nil
	}
	if prior.TerminalAcked || next.Revision != prior.Revision+1 || prior.PrepareBurned != next.PrepareBurned ||
		prior.BodyBurned && !next.BodyBurned || prior.ReconcileBurned && !next.ReconcileBurned ||
		prior.CanonicalDigest != "" && next.CanonicalDigest != prior.CanonicalDigest || prior.TerminalAcked && !next.TerminalAcked {
		return ErrCandidateInvalid
	}
	changes := 0
	if next.BodyBurned != prior.BodyBurned {
		if prior.CanonicalDigest != "" || len(c.Plan.Artifacts) == 0 {
			return ErrCandidateInvalid
		}
		changes++
	}
	if next.ReconcileBurned != prior.ReconcileBurned {
		changes++
	}
	if next.CanonicalDigest != prior.CanonicalDigest {
		changes++
	}
	if next.TerminalAcked != prior.TerminalAcked {
		if prior.CanonicalDigest == "" {
			return ErrCandidateInvalid
		}
		changes++
	}
	if changes != 1 {
		return ErrCandidateInvalid
	}
	return nil
}

func EncodeInlineCheckpointV2(value InlineReceiptCheckpointV2) ([]byte, error) {
	if value.Validate() != nil {
		return nil, ErrCandidateInvalid
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > MaxInlineCheckpointBytesV2 {
		clear(raw)
		return nil, ErrCandidateInvalid
	}
	return raw, nil
}

func DecodeInlineCheckpointV2(raw []byte) (InlineReceiptCheckpointV2, error) {
	var value InlineReceiptCheckpointV2
	// The encoded bound precedes decoder allocation; lexical validation rejects
	// duplicate decoded keys, invalid Unicode, excessive depth/member counts.
	if strictjson.ValidateDocument(raw, MaxInlineCheckpointBytesV2, 12, 32) != nil {
		return value, ErrCandidateInvalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || d.Decode(new(any)) != io.EOF || value.Validate() != nil {
		return InlineReceiptCheckpointV2{}, ErrCandidateInvalid
	}
	canonical, err := json.Marshal(value)
	defer clear(canonical)
	// This fixed typed encoding also rejects missing fields, case aliases and
	// alternate JSON spellings. It is not general JSON reordering support.
	if err != nil || !bytes.Equal(raw, canonical) {
		return InlineReceiptCheckpointV2{}, ErrCandidateInvalid
	}
	return value, nil
}

func validInlineReceiptPlanV2(p InlineReceiptPlanV2) bool {
	a := p.Authorization
	for _, validate := range []func() error{a.TenantID.Validate, a.OwnerUserID.Validate, a.WorkerID.Validate, a.ConnectionID.Validate, a.RunID.Validate, a.AttemptID.Validate, a.LeaseID.Validate, a.FenceToken.Validate, a.ContextDigest.Validate, a.CapabilityDigest.Validate, a.PolicyDigest.Validate, p.Nonce.Validate, p.Producer.SessionID.Validate, p.Producer.ReservationID.Validate, p.Producer.HarnessBindingDigest.Validate} {
		if validate() != nil {
			return false
		}
	}
	if a.PresentedSecretDigest != "" || a.ExpectedAttemptRevision != 0 || a.AttemptSequence != 1 ||
		!inlineCheckpointCounterV2(a.EnrollmentGeneration) || !inlineCheckpointCounterV2(a.ConnectionGeneration) || !inlineCheckpointCounterV2(a.LeaseGeneration) ||
		!inlineCheckpointCounterV2(p.WorkerAttemptSequence) || !inlineCheckpointCounterV2(p.PlatformAttemptSequence) || p.TerminalSequence != 1 ||
		!inlineCheckpointTimeV2(p.Producer.AcceptedAt) || !inlineCheckpointTimeV2(p.ExpiresAt) || a.LeaseExpiresAtUnixMicro <= 0 {
		return false
	}
	lease := time.UnixMicro(a.LeaseExpiresAtUnixMicro).UTC()
	if !inlineCheckpointTimeV2(lease) || !p.ExpiresAt.After(p.Producer.AcceptedAt) || p.ExpiresAt.After(lease) {
		return false
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil || fence != a.FenceToken {
		return false
	}
	for _, digest := range []string{p.Fingerprint, p.ObservationDigest, p.Producer.SchemaDigest, p.Producer.FinalDigest, p.Producer.AnswerDigest} {
		if !validInlineDigestV2(digest) {
			return false
		}
	}
	if len(p.Artifacts) > MaxInlineArtifactsV2 {
		return false
	}
	var total uint64
	for i, artifact := range p.Artifacts {
		if !validInlineNameV2(artifact.Name) || artifact.MediaType != "text/plain" && artifact.MediaType != "application/json" ||
			artifact.Size == 0 || artifact.Size > MaxInlineArtifactBytesV2 || artifact.Size > MaxInlineTotalBytesV2-total || !validInlineDigestV2(artifact.SHA256) {
			return false
		}
		for _, previous := range p.Artifacts[:i] {
			if strings.EqualFold(previous.Name, artifact.Name) {
				return false
			}
		}
		total += artifact.Size
	}
	return true
}

func inlineCheckpointCounterV2(value uint64) bool { return value > 0 && value < math.MaxUint64 }
func inlineCheckpointTimeV2(value time.Time) bool {
	_, offset := value.Zone()
	return !value.IsZero() && offset == 0 && value.Year() >= 1 && value.Year() <= 9999 && value.After(time.Unix(0, 0))
}

func (InlineReceiptPlanV2) String() string     { return "InlineReceiptPlanV2{[redacted metadata]}" }
func (p InlineReceiptPlanV2) GoString() string { return p.String() }
func (p InlineReceiptPlanV2) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, p.String())
}
func (InlineReceiptProgressV2) String() string     { return "InlineReceiptProgressV2{[redacted metadata]}" }
func (p InlineReceiptProgressV2) GoString() string { return p.String() }
func (p InlineReceiptProgressV2) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, p.String())
}
func (InlineReceiptCheckpointV2) String() string {
	return "InlineReceiptCheckpointV2{[redacted metadata]}"
}
func (c InlineReceiptCheckpointV2) GoString() string { return c.String() }
func (c InlineReceiptCheckpointV2) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, c.String())
}
