package attachedworkerdaemon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const maxVerifiedAttachedInputBytes = 1 << 20

// VerifiedAttachedInputV1 preserves authenticated materialization data, not
// issuer authority. Only the authenticated sealed-input composition supplies
// its AcceptedAttempt. Executors must still use the accepted-attempt owner,
// credential owner and concrete driver's authority checks. It is never stdin
// or a wire DTO. All buffers are owned and borrowed only for RunPrepared.
type VerifiedAttachedInputV1 struct {
	AcceptedAttempt  domain.AttachedWorkerAttemptV1 `json:"-"`
	Job              domain.WorkerJob               `json:"-"`
	Manifest         domain.ArtifactManifest        `json:"-"`
	Context          []byte                         `json:"-"`
	Artifacts        []VerifiedAttachedArtifactV1   `json:"-"`
	CanonicalContext *sessioncontext.CanonicalProof `json:"-"`
}

type VerifiedAttachedArtifactV1 struct {
	Name string `json:"-"`
	Body []byte `json:"-"`
}

func (VerifiedAttachedInputV1) String() string         { return "VerifiedAttachedInputV1{input:[redacted]}" }
func (input VerifiedAttachedInputV1) GoString() string { return input.String() }
func (VerifiedAttachedArtifactV1) String() string {
	return "VerifiedAttachedArtifactV1{artifact:[redacted]}"
}
func (artifact VerifiedAttachedArtifactV1) GoString() string { return artifact.String() }

// This bounded internal copy shape is not exported or used as a wire input.
type verifiedAttachedCopy struct {
	AcceptedAttempt  domain.AttachedWorkerAttemptV1
	Job              domain.WorkerJob
	Manifest         domain.ArtifactManifest
	Context          []byte
	Artifacts        []verifiedArtifactCopy
	CanonicalContext *sessioncontext.CanonicalProof
}
type verifiedArtifactCopy struct {
	Name string
	Body []byte
}

func (input *VerifiedAttachedInputV1) copyShape() verifiedAttachedCopy {
	value := verifiedAttachedCopy{AcceptedAttempt: input.AcceptedAttempt, Job: input.Job,
		Manifest: input.Manifest, Context: input.Context, CanonicalContext: input.CanonicalContext}
	for _, artifact := range input.Artifacts {
		value.Artifacts = append(value.Artifacts, verifiedArtifactCopy{Name: artifact.Name, Body: artifact.Body})
	}
	return value
}

// Clone bounds and validates before any deep copy. Go immutable metadata
// strings are relinquished by Clear; no claim of string-byte zeroization is
// made. JSON is used only to copy finite typed metadata and byte buffers.
func (input *VerifiedAttachedInputV1) Clone() (*VerifiedAttachedInputV1, error) {
	if input == nil || input.ValidateFor(InvocationIdentity{TenantID: input.AcceptedAttempt.TenantID,
		OwnerUserID: input.AcceptedAttempt.OwnerUserID, WorkerID: input.AcceptedAttempt.WorkerID,
		RunID: input.AcceptedAttempt.RunID, AttemptID: input.AcceptedAttempt.AttemptID,
		LeaseID: input.AcceptedAttempt.LeaseID, FenceToken: input.AcceptedAttempt.LeaseGeneration}) != nil {
		return nil, ErrInvocationInvalid
	}
	encoded, err := json.Marshal(input.copyShape())
	defer clear(encoded)
	if err != nil || len(encoded) > maxVerifiedAttachedInputBytes || uint64(len(encoded)) > input.Job.Limits.MaxInputBytes {
		return nil, ErrInvocationInvalid
	}
	var value verifiedAttachedCopy
	if json.Unmarshal(encoded, &value) != nil {
		clearVerifiedCopy(&value)
		return nil, ErrInvocationInvalid
	}
	result := &VerifiedAttachedInputV1{AcceptedAttempt: value.AcceptedAttempt, Job: value.Job,
		Manifest: value.Manifest, Context: value.Context, CanonicalContext: value.CanonicalContext}
	for _, artifact := range value.Artifacts {
		result.Artifacts = append(result.Artifacts, VerifiedAttachedArtifactV1{Name: artifact.Name, Body: artifact.Body})
	}
	return result, nil
}

func clearVerifiedCopy(value *verifiedAttachedCopy) {
	clear(value.Context)
	for _, artifact := range value.Artifacts {
		clear(artifact.Body)
	}
	value.CanonicalContext.Clear()
	*value = verifiedAttachedCopy{}
}

func (input *VerifiedAttachedInputV1) Clear() {
	if input == nil {
		return
	}
	clear(input.Context)
	for _, artifact := range input.Artifacts {
		clear(artifact.Body)
	}
	input.CanonicalContext.Clear()
	*input = VerifiedAttachedInputV1{}
}

func (input *VerifiedAttachedInputV1) ValidateFor(identity InvocationIdentity) error {
	if input == nil || identity.Validate() != nil || !input.bounded() {
		return ErrInvocationInvalid
	}
	a, job := input.AcceptedAttempt, input.Job
	if a.Validate() != nil || a.State != domain.AttachedWorkerAttemptClaimed ||
		a.TenantID != identity.TenantID || a.OwnerUserID != identity.OwnerUserID || a.WorkerID != identity.WorkerID ||
		a.RunID != identity.RunID || a.AttemptID != identity.AttemptID || a.LeaseID != identity.LeaseID || a.LeaseGeneration != identity.FenceToken ||
		job.TenantID != a.TenantID || job.RunID != a.RunID || job.AttemptID != a.AttemptID || job.ReservationID != a.ReservationID ||
		job.CredentialOwnerUserID != a.OwnerUserID || job.ExecutionPlacementV2.OwnerUserID != a.OwnerUserID ||
		job.ExecutionPlacementV2.WorkerID != a.WorkerID || job.ExecutionPlacementV2.CapabilityDigest != a.CapabilityDigest ||
		job.ExecutionPlacementV2.PolicyDigest != a.PolicyDigest || job.HarnessBinding.ValidateForScope(job.TenantID,
		a.OwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2) != nil {
		return ErrInvocationInvalid
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil || digest != a.ContextDigest {
		return ErrInvocationInvalid
	}
	if job.ContextWindow == nil {
		if input.CanonicalContext != nil || !verifiedBlobMatches(job.ContextSnapshot, input.Context) {
			return ErrInvocationInvalid
		}
	} else {
		history, refs, err := sessioncontext.ProjectCanonical(job, input.CanonicalContext, maxVerifiedAttachedInputBytes)
		defer clear(history)
		if err != nil || !bytes.Equal(history, input.Context) ||
			sessioncontext.VerifyCanonicalAttachments(history, refs, input.CanonicalContext, job.Limits.MaxContextBytes) != nil {
			return ErrInvocationInvalid
		}
	}
	if len(input.Artifacts) != len(input.Manifest.Artifacts) {
		return ErrInvocationInvalid
	}
	seen := make(map[string]bool, len(input.Artifacts))
	for _, artifact := range input.Artifacts {
		matched := false
		for _, ref := range input.Manifest.Artifacts {
			if artifact.Name == ref.Name && verifiedBlobMatches(ref.Blob, artifact.Body) {
				matched = true
				break
			}
		}
		if !matched || seen[artifact.Name] {
			return ErrInvocationInvalid
		}
		seen[artifact.Name] = true
	}
	return nil
}

func (input *VerifiedAttachedInputV1) validateCredential(identity InvocationIdentity, credential *CredentialInvocation) error {
	if input.ValidateFor(identity) != nil || credential == nil || credential.ValidateFor(identity) != nil {
		return ErrInvocationInvalid
	}
	job, a, request := input.Job, input.AcceptedAttempt, credential.IssueRequest
	if request.ProviderResource != job.HarnessBinding.Resource ||
		credential.ExpectedBindingGeneration != job.HarnessBinding.Resource.CredentialGeneration ||
		request.Run.SessionID != job.SessionID || request.Run.TriggerEventID != job.TriggerEventID ||
		!request.Lease.ExpiresAt.Equal(a.LeaseExpiresAt) {
		return ErrInvocationInvalid
	}
	return nil
}

func verifiedBlobMatches(ref domain.BlobRef, body []byte) bool {
	digest := sha256.Sum256(body)
	return ref.Validate() == nil && ref.Size == int64(len(body)) && ref.SHA256 == hex.EncodeToString(digest[:])
}

func (input *VerifiedAttachedInputV1) bounded() bool {
	if !verifiedMetadataBound(reflect.ValueOf(input.AcceptedAttempt), 64) ||
		!verifiedMetadataBound(reflect.ValueOf(input.Job), 64) || !verifiedMetadataBound(reflect.ValueOf(input.Manifest), 64) ||
		len(input.Artifacts) > 64 || uint64(len(input.Artifacts)) > uint64(input.Job.Limits.MaxArtifacts) ||
		uint64(len(input.Context)) > input.Job.Limits.MaxContextBytes {
		return false
	}
	remaining := maxVerifiedAttachedInputBytes
	consume := func(body []byte) bool {
		if len(body) > remaining {
			return false
		}
		remaining -= len(body)
		return true
	}
	if !consume(input.Context) {
		return false
	}
	for _, artifact := range input.Artifacts {
		if len(artifact.Name) > 4096 || !consume(artifact.Body) {
			return false
		}
	}
	if proof := input.CanonicalContext; proof != nil {
		if !verifiedMetadataBound(reflect.ValueOf(proof.Input), 4096) || len(proof.EventBodies) > 4096 ||
			len(proof.Attachments) > 64 || uint64(len(proof.Attachments)+len(input.Artifacts)) > uint64(input.Job.Limits.MaxArtifacts) ||
			!consume(proof.SnapshotBytes) {
			return false
		}
		for _, body := range proof.EventBodies {
			if !consume(body) {
				return false
			}
		}
		for _, attachment := range proof.Attachments {
			if !verifiedMetadataBound(reflect.ValueOf(attachment.AttachmentRef), 64) || !consume(attachment.Body) {
				return false
			}
		}
	}
	return true
}

func verifiedMetadataBound(value reflect.Value, maxItems int) bool {
	remaining := maxVerifiedAttachedInputBytes
	var visit func(reflect.Value, int) bool
	visit = func(value reflect.Value, depth int) bool {
		if depth > 16 {
			return false
		}
		if value.Type() == reflect.TypeOf(time.Time{}) {
			return true
		}
		switch value.Kind() {
		case reflect.Interface, reflect.Pointer:
			return value.IsNil() || visit(value.Elem(), depth+1)
		case reflect.String:
			if value.Len() > 4096 || value.Len() > remaining {
				return false
			}
			remaining -= value.Len()
		case reflect.Slice, reflect.Array:
			if value.Len() > maxItems {
				return false
			}
			for i := 0; i < value.Len(); i++ {
				if !visit(value.Index(i), depth+1) {
					return false
				}
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if !visit(value.Field(i), depth+1) {
					return false
				}
			}
		case reflect.Map:
			return false
		}
		return true
	}
	return visit(value, 0)
}
