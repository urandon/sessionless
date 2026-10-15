package attachedworkeroutput

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	AttemptOutputUploadVersionV1        uint32 = 1
	AttemptOutputUploadMaxArtifacts            = 2
	AttemptOutputUploadMaxArtifactBytes int64  = 32 << 10
	AttemptOutputUploadMaxTotalBytes    int64  = 64 << 10
	AttemptOutputUploadMaxDuration             = 15 * time.Second
)

type UploadPlanPhaseV1 string
type UploadOrdinalStateV1 string

const (
	UploadPlanActiveV1            UploadPlanPhaseV1    = "active"
	UploadPlanReceiptBoundV1      UploadPlanPhaseV1    = "receipt_bound"
	UploadPlanSettledAbandonedV1  UploadPlanPhaseV1    = "settled_abandoned"
	UploadOrdinalUnissuedV1       UploadOrdinalStateV1 = "unissued"
	UploadOrdinalWritePendingV1   UploadOrdinalStateV1 = "write_pending"
	UploadOrdinalNotDispatchedV1  UploadOrdinalStateV1 = "not_dispatched"
	UploadOrdinalSourceVerifiedV1 UploadOrdinalStateV1 = "source_verified"
)

// UploadPlanIdentityV1 is bounded persisted DATA, never producer evidence or
// current authorization. The admission transaction must derive/verify these
// fields against its actual head/job/Session and the original private exporter.
// In particular, accepting an arbitrary caller cutoff does not grant 15s.
type UploadPlanIdentityV1 struct {
	Binding              ports.AttachedWorkerSealedInputAuthorization `json:"binding"`
	SessionID            domain.SessionID                             `json:"session_id"`
	HarnessBindingDigest domain.HarnessBindingDigestV2                `json:"harness_binding_digest"`
	ProfileDigest        string                                       `json:"profile_digest"`
	SchemaDigest         string                                       `json:"schema_digest"`
	FinalDigest          string                                       `json:"final_digest"`
	AnswerDigest         string                                       `json:"answer_digest"`
	ProducerDigest       string                                       `json:"producer_digest"`
	Nonce                domain.IdempotencyKey                        `json:"nonce"`
	CreatedAt            time.Time                                    `json:"created_at"`
	ExpiresAt            time.Time                                    `json:"expires_at"`
}

// UploadArtifactMetadataV1 deliberately has no content or caller destination.
// Empty text is valid and still consumes one physical body attempt.
type UploadArtifactMetadataV1 struct {
	Ordinal    uint32 `json:"ordinal"`
	Name       string `json:"name"`
	MediaType  string `json:"media_type"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	ContentMD5 string `json:"content_md5"`
}

type UploadOrdinalV1 struct {
	Metadata UploadArtifactMetadataV1 `json:"metadata"`
	Key      string                   `json:"key"`
	State    UploadOrdinalStateV1     `json:"state"`
	// Nonsecret claim identity, not a signed URL, bearer or permission to retry.
	GrantDigest string          `json:"grant_digest,omitempty"`
	Source      *domain.BlobRef `json:"source,omitempty"`
}

// AttemptOutputUploadPlanV1 is the typed pre-Put subrecord intended for the
// existing per-attempt receipt row. Validation of this DATA does not establish
// request quiescence or authorize settlement/publication. No public witness
// constructor or generic "quiescent" flag is provided here.
type AttemptOutputUploadPlanV1 struct {
	Version    uint32               `json:"version"`
	Phase      UploadPlanPhaseV1    `json:"phase"`
	Identity   UploadPlanIdentityV1 `json:"identity"`
	PlanDigest string               `json:"plan_digest"`
	Ordinals   []UploadOrdinalV1    `json:"ordinals"`
}

// NewAttemptOutputUploadPlanV1 builds DATA only. Its caller must authorize and
// persist it before exposing any signer capability; construction does neither.
func NewAttemptOutputUploadPlanV1(identity UploadPlanIdentityV1, artifacts []UploadArtifactMetadataV1) (AttemptOutputUploadPlanV1, error) {
	if !validUploadIdentity(identity) || !validUploadMetadata(artifacts) {
		return AttemptOutputUploadPlanV1{}, ErrCandidateInvalid
	}
	digest, err := uploadPlanDigest(identity, artifacts)
	if err != nil {
		return AttemptOutputUploadPlanV1{}, ErrCandidateInvalid
	}
	plan := AttemptOutputUploadPlanV1{Version: AttemptOutputUploadVersionV1, Phase: UploadPlanActiveV1,
		Identity: identity, PlanDigest: digest, Ordinals: make([]UploadOrdinalV1, len(artifacts))}
	for i, artifact := range artifacts {
		plan.Ordinals[i] = UploadOrdinalV1{Metadata: artifact, Key: uploadSourceKey(identity, digest, artifact.Ordinal), State: UploadOrdinalUnissuedV1}
	}
	return plan, nil
}

func (plan AttemptOutputUploadPlanV1) Validate() error {
	if plan.Version != AttemptOutputUploadVersionV1 || !validUploadIdentity(plan.Identity) ||
		len(plan.Ordinals) == 0 || len(plan.Ordinals) > AttemptOutputUploadMaxArtifacts {
		return ErrCandidateInvalid
	}
	var metadata [AttemptOutputUploadMaxArtifacts]UploadArtifactMetadataV1
	for i, ordinal := range plan.Ordinals {
		metadata[i] = ordinal.Metadata
	}
	if !validUploadMetadata(metadata[:len(plan.Ordinals)]) {
		return ErrCandidateInvalid
	}
	digest, err := uploadPlanDigest(plan.Identity, metadata[:len(plan.Ordinals)])
	if err != nil || plan.PlanDigest != digest {
		return ErrCandidateInvalid
	}
	for _, ordinal := range plan.Ordinals {
		if ordinal.Key != uploadSourceKey(plan.Identity, plan.PlanDigest, ordinal.Metadata.Ordinal) {
			return ErrCandidateInvalid
		}
		switch ordinal.State {
		case UploadOrdinalUnissuedV1:
			if ordinal.GrantDigest != "" || ordinal.Source != nil {
				return ErrCandidateInvalid
			}
		case UploadOrdinalWritePendingV1, UploadOrdinalNotDispatchedV1:
			if !uploadDigest(ordinal.GrantDigest) || ordinal.Source != nil {
				return ErrCandidateInvalid
			}
		case UploadOrdinalSourceVerifiedV1:
			if !uploadDigest(ordinal.GrantDigest) || ordinal.Source == nil || ordinal.Source.Validate() != nil ||
				ordinal.Source.TenantID != plan.Identity.Binding.TenantID || ordinal.Source.Key != ordinal.Key ||
				ordinal.Source.Size != ordinal.Metadata.Size || ordinal.Source.SHA256 != ordinal.Metadata.SHA256 {
				return ErrCandidateInvalid
			}
		default:
			return ErrCandidateInvalid
		}
		switch plan.Phase {
		case UploadPlanActiveV1:
		case UploadPlanReceiptBoundV1:
			if ordinal.State != UploadOrdinalSourceVerifiedV1 {
				return ErrCandidateInvalid
			}
		case UploadPlanSettledAbandonedV1:
			if ordinal.State == UploadOrdinalWritePendingV1 {
				return ErrCandidateInvalid
			}
		default:
			return ErrCandidateInvalid
		}
	}
	return nil
}

// SpendOrdinal changes one DATA state without modifying the input or renewing
// its cutoff. Only a genuine same-transaction current-authority owner may
// persist the returned state, and must do so BEFORE invoking the signer.
// Repeating the operation, even with the same digest, never returns a new grant.
func (plan AttemptOutputUploadPlanV1) SpendOrdinal(ordinal uint32, grantDigest string, at time.Time) (AttemptOutputUploadPlanV1, error) {
	if plan.Validate() != nil || plan.Phase != UploadPlanActiveV1 || !uploadDigest(grantDigest) ||
		at.Before(plan.Identity.CreatedAt) || !at.Before(plan.Identity.ExpiresAt) ||
		ordinal >= uint32(len(plan.Ordinals)) || plan.Ordinals[ordinal].State != UploadOrdinalUnissuedV1 {
		return AttemptOutputUploadPlanV1{}, ErrCandidateInvalid
	}
	result := plan.Clone()
	result.Ordinals[ordinal].State = UploadOrdinalWritePendingV1
	result.Ordinals[ordinal].GrantDigest = grantDigest
	return result, nil
}

// Clone retains no writable slice/ref alias. It neither closes capabilities nor
// creates private producer/sink evidence.
func (plan AttemptOutputUploadPlanV1) Clone() AttemptOutputUploadPlanV1 {
	result := plan
	if plan.Ordinals != nil {
		result.Ordinals = make([]UploadOrdinalV1, len(plan.Ordinals))
		copy(result.Ordinals, plan.Ordinals)
		for i, ordinal := range plan.Ordinals {
			if ordinal.Source != nil {
				ref := *ordinal.Source
				result.Ordinals[i].Source = &ref
			}
		}
	}
	return result
}

func validUploadIdentity(value UploadPlanIdentityV1) bool {
	a := value.Binding
	return a.TenantID.Validate() == nil && a.OwnerUserID.Validate() == nil && a.WorkerID.Validate() == nil &&
		a.ConnectionID.Validate() == nil && a.RunID.Validate() == nil && a.AttemptID.Validate() == nil && a.LeaseID.Validate() == nil &&
		a.PresentedSecretDigest == "" && a.EnrollmentGeneration != 0 && a.ConnectionGeneration != 0 && a.AttemptSequence == 1 &&
		a.LeaseGeneration != 0 && a.FenceToken.Validate() == nil && a.ExpectedAttemptRevision != 0 &&
		a.ContextDigest.Validate() == nil && a.CapabilityDigest.Validate() == nil && a.PolicyDigest.Validate() == nil &&
		value.SessionID.Validate() == nil && value.Nonce.Validate() == nil && value.HarnessBindingDigest.Validate() == nil &&
		uploadDigest(value.ProfileDigest) && uploadDigest(value.SchemaDigest) && uploadDigest(value.FinalDigest) &&
		uploadDigest(value.AnswerDigest) && uploadDigest(value.ProducerDigest) && !value.CreatedAt.IsZero() &&
		value.ExpiresAt.After(value.CreatedAt) && value.ExpiresAt.Sub(value.CreatedAt) <= AttemptOutputUploadMaxDuration &&
		a.LeaseExpiresAtUnixMicro > 0 && !value.ExpiresAt.After(time.UnixMicro(a.LeaseExpiresAtUnixMicro)) &&
		canonicalUploadTime(value.CreatedAt) && canonicalUploadTime(value.ExpiresAt)
}

func canonicalUploadTime(value time.Time) bool {
	_, offset := value.Zone()
	return value.Year() >= 1 && value.Year() <= 9999 && offset == 0 && value.Nanosecond()%1000 == 0
}

func validUploadMetadata(values []UploadArtifactMetadataV1) bool {
	if len(values) == 0 || len(values) > AttemptOutputUploadMaxArtifacts {
		return false
	}
	var total int64
	for i, value := range values {
		if len(value.ContentMD5) != 24 {
			return false
		}
		decodedMD5, err := base64.StdEncoding.DecodeString(value.ContentMD5)
		if value.Ordinal != uint32(i) || !validArtifactName(value.Name) || !utf8.ValidString(value.Name) || !structuredArtifactName(value.Name) ||
			value.MediaType != "text/plain" && value.MediaType != "application/json" ||
			value.Size == 0 && value.MediaType != "text/plain" ||
			value.Size < 0 || value.Size > AttemptOutputUploadMaxArtifactBytes || value.Size > AttemptOutputUploadMaxTotalBytes-total ||
			!uploadDigest(value.SHA256) || err != nil || len(decodedMD5) != 16 || base64.StdEncoding.EncodeToString(decodedMD5) != value.ContentMD5 {
			return false
		}
		if value.Size == 0 {
			emptySHA, emptyMD5 := sha256.Sum256(nil), md5.Sum(nil)
			if value.SHA256 != hex.EncodeToString(emptySHA[:]) || value.ContentMD5 != base64.StdEncoding.EncodeToString(emptyMD5[:]) {
				return false
			}
		}
		for _, prior := range values[:i] {
			if strings.EqualFold(prior.Name, value.Name) {
				return false
			}
		}
		total += value.Size
	}
	return true
}

func uploadDigest(value string) bool {
	if len(value) != 2*sha256.Size {
		return false
	}
	for _, b := range []byte(value) {
		if !(b >= '0' && b <= '9' || b >= 'a' && b <= 'f') {
			return false
		}
	}
	return true
}

func uploadPlanDigest(identity UploadPlanIdentityV1, metadata []UploadArtifactMetadataV1) (string, error) {
	// Closed fixed struct, bounded strings, UTC microsecond timestamps: no map
	// order, mutable ordinal state, remote refs or capability secrets in identity.
	encoded, err := json.Marshal(struct {
		Version   uint32                     `json:"version"`
		Identity  UploadPlanIdentityV1       `json:"identity"`
		Artifacts []UploadArtifactMetadataV1 `json:"artifacts"`
	}{AttemptOutputUploadVersionV1, identity, metadata})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attempt-output-upload-plan:v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

func uploadSourceKey(identity UploadPlanIdentityV1, planDigest string, ordinal uint32) string {
	return domain.SessionRunObjectPrefix(identity.Binding.TenantID, identity.SessionID, identity.Binding.RunID) +
		"attached-worker/candidates/" + string(identity.Binding.AttemptID) + "/" + planDigest + "/" + strconv.FormatUint(uint64(ordinal), 10)
}
