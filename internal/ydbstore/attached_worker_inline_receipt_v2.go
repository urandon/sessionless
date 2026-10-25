package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const maxInlineReceiptRecordBytesV2 = 512 << 10

// AttachedWorkerOutputReceiptV2 has no Ready/CopyInProgress booleans and no
// pseudo source refs. CanonicalDigest is empty until the actual canonical plan
// exists. Its denormalized SQL column must carry the same empty/real value.
type AttachedWorkerOutputReceiptV2 struct {
	Version              uint32                                           `json:"version"`
	Stage                attachedworkeroutput.AttachedWorkerInlineStageV2 `json:"stage"`
	Binding              ports.AttachedWorkerSealedInputAuthorization     `json:"binding"`
	Nonce                domain.IdempotencyKey                            `json:"nonce"`
	CandidateFingerprint string                                           `json:"candidate_fingerprint"`
	Candidate            attachedworkeroutput.InlineCandidateV2           `json:"candidate"`
	Producer             attachedworkeroutput.InlineProducerBindingV2     `json:"producer"`
	Observation          attachedworkeroutput.ProcessObservationV1        `json:"observation"`
	ObservationDigest    string                                           `json:"observation_digest"`
	CredentialRequired   bool                                             `json:"credential_required"`
	BodyClaimID          string                                           `json:"body_claim_id"`
	CopyOwnerID          string                                           `json:"copy_owner_id"`
	CanonicalDigest      domain.AttachedWorkerTerminalEvidenceDigest      `json:"canonical_digest"`
	Materialization      ports.AttachedWorkerTerminalMaterialization      `json:"materialization"`
	CreatedAt            time.Time                                        `json:"created_at"`
	ExpiresAt            time.Time                                        `json:"expires_at"`
}

// Version dispatch is explicit before any old V1 reader or default field can
// interpret a V2 row. V1 mutation/replay remains restricted to actual V1 rows.
type attachedWorkerReceiptEnvelope struct {
	v1 *AttachedWorkerOutputReceiptV1
	v2 *AttachedWorkerOutputReceiptV2
}

func inlineReceiptPlanFingerprintV2(r AttachedWorkerOutputReceiptV2) (string, error) {
	encoded, err := json.Marshal(struct {
		Binding            ports.AttachedWorkerSealedInputAuthorization
		Nonce              domain.IdempotencyKey
		Candidate          attachedworkeroutput.InlineCandidateV2
		Producer           attachedworkeroutput.InlineProducerBindingV2
		ObservationDigest  string
		CredentialRequired bool
		ExpiresAt          time.Time
	}{r.Binding, r.Nonce, r.Candidate, r.Producer, r.ObservationDigest, r.CredentialRequired, r.ExpiresAt})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-inline-plan:v2\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func decodeAttachedWorkerReceiptEnvelope(payload []byte) (attachedWorkerReceiptEnvelope, error) {
	var zero attachedWorkerReceiptEnvelope
	if strictjson.ValidateDocument(payload, maxInlineReceiptRecordBytesV2, 64, maxInlineReceiptRecordBytesV2) != nil {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	version, err := attachedWorkerReceiptVersion(payload)
	if err != nil {
		return zero, ErrAttachedWorkerAttemptConflict
	}
	switch version {
	case 1:
		var record AttachedWorkerOutputReceiptV1
		if json.Unmarshal(payload, &record) != nil {
			return zero, ErrAttachedWorkerAttemptConflict
		}
		return attachedWorkerReceiptEnvelope{v1: &record}, nil
	case 2:
		var record AttachedWorkerOutputReceiptV2
		if decodeOwnedEffectCanonicalJSON(string(payload), &record) != nil {
			return zero, ErrAttachedWorkerAttemptConflict
		}
		return attachedWorkerReceiptEnvelope{v2: &record}, nil
	default:
		return zero, ErrAttachedWorkerAttemptConflict
	}
}

// The caller first validates the complete document's lexical limits and
// duplicates. Version dispatch is not canonical-shape or identity validation.
func attachedWorkerReceiptVersion(payload []byte) (uint32, error) {
	// Values consume only a tiny scalar probe, not a RawMessage copy of each
	// candidate/materialization. The complete typed/closed-shape decoder below
	// remains the authority; this map only selects its explicit version.
	var header map[string]attachedWorkerReceiptVersionProbe
	if json.Unmarshal(payload, &header) != nil {
		return 0, ErrAttachedWorkerAttemptConflict
	}
	for name := range header {
		if strings.EqualFold(name, "version") && name != "version" {
			return 0, ErrAttachedWorkerAttemptConflict
		}
	}
	version, exists := header["version"]
	if !exists || !version.valid {
		return 0, ErrAttachedWorkerAttemptConflict
	}
	return version.value, nil
}

type attachedWorkerReceiptVersionProbe struct {
	value uint32
	valid bool
}

func (p *attachedWorkerReceiptVersionProbe) UnmarshalJSON(raw []byte) error {
	*p = attachedWorkerReceiptVersionProbe{}
	raw = bytes.TrimSpace(raw)
	// Ignore non-numeric values without decoding/retaining their content.
	// Preserve uint32's old null->zero behavior; zero is still an unsupported
	// envelope version. No raw slice escapes this callback.
	if bytes.Equal(raw, []byte("null")) {
		p.valid = true
	} else if len(raw) > 0 && raw[0] >= '0' && raw[0] <= '9' {
		p.valid = json.Unmarshal(raw, &p.value) == nil
	}
	return nil
}

func (record attachedWorkerReceiptEnvelope) binding() ports.AttachedWorkerSealedInputAuthorization {
	if record.v1 != nil && record.v2 == nil {
		return record.v1.Binding
	}
	if record.v2 != nil && record.v1 == nil {
		return record.v2.Binding
	}
	return ports.AttachedWorkerSealedInputAuthorization{}
}

func (record attachedWorkerReceiptEnvelope) validateIdentity(tenant domain.TenantID, run domain.RunID,
	owner domain.UserID, worker domain.AttachedWorkerID, attempt domain.AttemptID, generation uint64,
) error {
	if record.v1 != nil && record.v2 == nil {
		return validateAttachedWorkerReceiptRecord(*record.v1, tenant, run, owner, worker, attempt, generation)
	}
	if record.v2 == nil || record.v1 != nil {
		return ErrAttachedWorkerAttemptConflict
	}
	r := record.v2
	structuralBinding := r.Binding
	// The persisted binding deliberately omits the bearer. Use a valid nonzero
	// digest only to validate its other structural fields; this grants no auth.
	structuralBinding.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("inline-v2-structural-validation-only"))
	if r.Version != 2 || r.Binding.TenantID != tenant || r.Binding.RunID != run || r.Binding.OwnerUserID != owner ||
		r.Binding.WorkerID != worker || r.Binding.AttemptID != attempt || r.Binding.LeaseGeneration != generation ||
		r.Binding.PresentedSecretDigest != "" || r.Binding.ExpectedAttemptRevision != 0 || validateAttachedWorkerSealedInputAuthorization(structuralBinding) != nil || r.Nonce.Validate() != nil ||
		r.Candidate.Validate() != nil || r.Producer.SessionID.Validate() != nil || r.Producer.HarnessBindingDigest.Validate() != nil || r.Producer.ReservationID.Validate() != nil ||
		r.Producer.AcceptedAt.IsZero() || r.CreatedAt.IsZero() || r.CreatedAt.Before(r.Producer.AcceptedAt) ||
		!r.ExpiresAt.After(r.CreatedAt) || r.ExpiresAt.Sub(r.CreatedAt) > 15*time.Second ||
		r.ExpiresAt.After(time.UnixMicro(r.Binding.LeaseExpiresAtUnixMicro)) ||
		r.Observation.ValidateFor(domain.AttachedWorkerTerminalSucceeded, r.CredentialRequired) != nil {
		return ErrAttachedWorkerAttemptConflict
	}
	for _, digest := range []string{r.CandidateFingerprint, r.Producer.SchemaDigest, r.Producer.FinalDigest, r.Producer.AnswerDigest} {
		if domain.HarnessBindingDigestV2(digest).Validate() != nil {
			return ErrAttachedWorkerAttemptConflict
		}
	}
	observation, err := r.Observation.Digest()
	if err != nil || observation != r.ObservationDigest {
		return ErrAttachedWorkerAttemptConflict
	}
	fingerprint, err := inlineReceiptPlanFingerprintV2(*r)
	answer := sha256.Sum256([]byte(r.Candidate.Summary))
	if err != nil || fingerprint != r.CandidateFingerprint || hex.EncodeToString(answer[:]) != r.Producer.AnswerDigest {
		return ErrAttachedWorkerAttemptConflict
	}
	switch r.Stage {
	case attachedworkeroutput.InlinePreparedV2:
		if r.BodyClaimID != "" || r.CopyOwnerID != "" || !inlineReceiptWithoutCanonicalPlanV2(*r) {
			return ErrAttachedWorkerAttemptConflict
		}
	case attachedworkeroutput.InlineBodyClaimedV2:
		if domain.HarnessBindingDigestV2(r.BodyClaimID).Validate() != nil || r.CopyOwnerID != "" || !inlineReceiptWithoutCanonicalPlanV2(*r) {
			return ErrAttachedWorkerAttemptConflict
		}
	case attachedworkeroutput.InlineRetiredWithoutWritesV2:
		if r.BodyClaimID != "" && domain.HarnessBindingDigestV2(r.BodyClaimID).Validate() != nil || r.CopyOwnerID != "" || !inlineReceiptWithoutCanonicalPlanV2(*r) {
			return ErrAttachedWorkerAttemptConflict
		}
	case attachedworkeroutput.InlineCanonicalCopyingV2, attachedworkeroutput.InlineReadyV2:
		if domain.HarnessBindingDigestV2(r.BodyClaimID).Validate() != nil || domain.HarnessBindingDigestV2(r.CopyOwnerID).Validate() != nil {
			return ErrAttachedWorkerAttemptConflict
		}
		digest, err := attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
		if err != nil || digest != r.CanonicalDigest || r.Materialization.EvidenceDigest != digest {
			return ErrAttachedWorkerAttemptConflict
		}
		if validateInlineCanonicalPlanV2(*r) != nil {
			return ErrAttachedWorkerAttemptConflict
		}
	default:
		return ErrAttachedWorkerAttemptConflict
	}
	return nil
}

func validateInlineCanonicalPlanV2(r AttachedWorkerOutputReceiptV2) error {
	c := r.Materialization.Completion
	a := r.Binding
	if c == nil || r.Materialization.Failure != nil || c.TenantID != a.TenantID || c.RunID != a.RunID || c.AttemptID != a.AttemptID ||
		c.ReservationID != r.Producer.ReservationID || c.LeaseID != a.LeaseID || c.Fence != a.LeaseGeneration || !c.At.Equal(r.CreatedAt) ||
		c.Manifest.TenantID != a.TenantID || c.Manifest.RunID != a.RunID || !c.Manifest.CreatedAt.Equal(r.CreatedAt) ||
		len(c.Manifest.Artifacts) != len(r.Candidate.Artifacts) || len(c.Events) != 1 || c.Events[0].Kind != domain.SessionEventAssistantMessage ||
		c.Events[0].DisplayText != strings.TrimSpace(r.Candidate.Summary) || !c.Events[0].CreatedAt.Equal(r.CreatedAt) {
		return ErrAttachedWorkerAttemptConflict
	}
	for i, artifact := range c.Manifest.Artifacts {
		decl := r.Candidate.Artifacts[i]
		key := domain.SessionRunObjectPrefix(a.TenantID, r.Producer.SessionID, a.RunID) + "artifacts/sha256/" + decl.SHA256
		if artifact.Name != decl.Name || artifact.MediaType != decl.MediaType || artifact.Blob.Validate() != nil || artifact.Blob.TenantID != a.TenantID ||
			artifact.Blob.Key != key || artifact.Blob.SHA256 != decl.SHA256 || artifact.Blob.Size != int64(decl.Size) {
			return ErrAttachedWorkerAttemptConflict
		}
	}
	event := c.Events[0]
	key := domain.SessionEventObjectPrefix(a.TenantID, r.Producer.SessionID, event.ID) + "payloads/sha256/" + event.Payload.SHA256 + ".json"
	if event.ID.Validate() != nil || event.IdempotencyKey.Validate() != nil || event.Payload.Validate() != nil || event.Payload.TenantID != a.TenantID || event.Payload.Key != key {
		return ErrAttachedWorkerAttemptConflict
	}
	return nil
}

func inlineReceiptWithoutCanonicalPlanV2(r AttachedWorkerOutputReceiptV2) bool {
	return r.CanonicalDigest == "" && r.Materialization.Completion == nil && r.Materialization.Failure == nil && r.Materialization.EvidenceDigest == ""
}

func (record attachedWorkerReceiptEnvelope) ready() bool {
	return record.v1 != nil && record.v2 == nil && record.v1.Ready || record.v2 != nil && record.v1 == nil && record.v2.Stage == attachedworkeroutput.InlineReadyV2
}

func (record attachedWorkerReceiptEnvelope) deletionSettled() bool {
	return record.ready() || record.v2 != nil && record.v1 == nil && record.v2.Stage == attachedworkeroutput.InlineRetiredWithoutWritesV2
}

func (record attachedWorkerReceiptEnvelope) objectRefs() []domain.BlobRef {
	if record.v1 != nil && record.v2 == nil {
		return attachedWorkerReceiptObjectRefs(*record.v1)
	}
	if record.v2 == nil || record.v1 != nil {
		return nil
	}
	return receiptMaterializationObjectRefs(record.v2.Materialization)
}

func receiptMaterializationObjectRefs(materialization ports.AttachedWorkerTerminalMaterialization) []domain.BlobRef {
	var refs []domain.BlobRef
	if materialization.Completion != nil {
		for _, artifact := range materialization.Completion.Manifest.Artifacts {
			refs = append(refs, artifact.Blob)
		}
		for _, event := range materialization.Completion.Events {
			refs = append(refs, event.Payload)
		}
	} else if materialization.Failure != nil {
		for _, event := range materialization.Failure.Events {
			refs = append(refs, event.Payload)
		}
	}
	return refs
}

// A version-neutral view exists only for an already validated READY receipt.
// It is not a V1 wire record and cannot make a pending V2 look like V1.
type attachedWorkerCanonicalReceipt struct {
	Binding              ports.AttachedWorkerSealedInputAuthorization
	Status               domain.AttachedWorkerTerminalStatus
	CanonicalDigest      domain.AttachedWorkerTerminalEvidenceDigest
	HarnessBindingDigest domain.HarnessBindingDigestV2
	CredentialRequired   bool
	Observation          attachedworkeroutput.ProcessObservationV1
	Materialization      ports.AttachedWorkerTerminalMaterialization
}

func (r attachedWorkerReceiptEnvelope) canonical() (attachedWorkerCanonicalReceipt, error) {
	if !r.ready() {
		return attachedWorkerCanonicalReceipt{}, ErrAttachedWorkerAttemptConflict
	}
	if r.v1 != nil {
		p := r.v1
		return attachedWorkerCanonicalReceipt{p.Binding, p.Status, p.CanonicalDigest, p.HarnessBindingDigest, p.CredentialRequired, p.Observation, p.Materialization}, nil
	}
	p := r.v2
	return attachedWorkerCanonicalReceipt{p.Binding, domain.AttachedWorkerTerminalSucceeded, p.CanonicalDigest, p.Producer.HarnessBindingDigest, p.CredentialRequired, p.Observation, p.Materialization}, nil
}
