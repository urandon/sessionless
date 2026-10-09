package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const (
	defaultMaxSealedInputBytes = 1 << 20
	maxSealedCollectionItems   = 64
	maxSealedMetadataValue     = 4096
	// A resource safety ceiling, separate from the artifact collection cap.
	// Normal contexts use the admitted MaxContextEvents (currently 512), and
	// all proofs additionally remain inside the serialized byte budget.
	MaxSealedContextRecordsV1 = 4096
)

var (
	ErrSealedInputInvalid     = errors.New("attached worker sealed input is invalid")
	ErrSealedInputUnsupported = errors.New("attached worker sealed input mode is unsupported")
)

// SealedInputSource is a trusted, authenticated data port. Its implementation
// must authorize the exact request before returning the immutable job,
// manifest, and blob bytes, transferring ownership of those byte slices to
// the caller. This package still checks their content bindings; the port
// cannot confer authority merely by returning a matching ID.
type SealedInputSource interface {
	Load(context.Context, MaterializationRequestV1) (SealedInputV1, error)
}

type SealedArtifactV1 struct {
	Name string `json:"name"`
	Body []byte `json:"body"`
}

type SealedInputV1 struct {
	Job              domain.WorkerJob
	Manifest         domain.ArtifactManifest
	Context          []byte
	Artifacts        []SealedArtifactV1
	Credential       *attachedworkerdaemon.CredentialInvocation `json:"Credential,omitempty"`
	CanonicalContext *sessioncontext.CanonicalProof             `json:"CanonicalContext,omitempty"`
}

// BoundMaterializer is the credential-free, in-memory synthetic input path.
// It creates no host read root and cannot select a process or credential.
// Canonical windows use codec-verified immutable proofs. Workspace/skill
// bundles, provider credentials, and large artifact staging remain gated.
type BoundMaterializer struct {
	source   SealedInputSource
	maxBytes int
	now      func() time.Time
	// Only the ydbintegration-tagged test constructor can enable this path.
	allowTestProvider bool
}

func NewBoundMaterializer(source SealedInputSource, maxBytes int, now func() time.Time) (*BoundMaterializer, error) {
	if source == nil || maxBytes < 0 || maxBytes > defaultMaxSealedInputBytes {
		return nil, ErrInvalidConfiguration
	}
	if maxBytes == 0 {
		maxBytes = defaultMaxSealedInputBytes
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &BoundMaterializer{source: source, maxBytes: maxBytes, now: now}, nil
}

type sealedEnvelopeV1 struct {
	Version            uint32                      `json:"version"`
	Kind               string                      `json:"kind"`
	Context            []byte                      `json:"context"`
	Artifacts          []SealedArtifactV1          `json:"artifacts"`
	ContextAttachments []sealedContextAttachmentV1 `json:"context_attachments,omitempty"`
}

type sealedContextAttachmentV1 struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Body      []byte `json:"body"`
}

func (materializer *BoundMaterializer) Materialize(ctx context.Context, request MaterializationRequestV1) (MaterializedInputV1, error) {
	if materializer == nil || materializer.source == nil || ctx == nil || ctx.Err() != nil ||
		request.TenantID.Validate() != nil || request.OwnerUserID.Validate() != nil ||
		request.WorkerID.Validate() != nil || request.ConnectionID.Validate() != nil ||
		request.EnrollmentGeneration == 0 || request.ConnectionGeneration == 0 ||
		request.AttemptSequence != 1 || request.Attempt.Validate() != nil ||
		materializer.now().UTC().UnixMicro() >= request.Attempt.ExpiresAtUnixMicro {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	input, err := materializer.source.Load(ctx, cloneMaterializationRequest(request))
	if err != nil || ctx.Err() != nil {
		clearSealedInput(&input)
		return MaterializedInputV1{}, errors.Join(ErrMaterializationFailed, ctx.Err())
	}
	defer clearSealedInput(&input)
	if materializer.now().UTC().UnixMicro() >= request.Attempt.ExpiresAtUnixMicro {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	job := input.Job
	if job.TenantID != request.TenantID || job.RunID != domain.RunID(request.Attempt.RunID) ||
		job.AttemptID != domain.AttemptID(request.Attempt.AttemptID) ||
		job.CredentialOwnerUserID != request.OwnerUserID ||
		job.ExecutionPlacementV2.OwnerUserID != request.OwnerUserID ||
		job.ExecutionPlacementV2.WorkerID != request.WorkerID ||
		!digestEquals(request.Attempt.CapabilityDigest, string(job.ExecutionPlacementV2.CapabilityDigest)) ||
		!digestEquals(request.Attempt.PolicyDigest, string(job.ExecutionPlacementV2.PolicyDigest)) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	// Bound source-owned collections and strings before domain digest validation,
	// which copies/sorts manifest artifacts and allowed MCP server names.
	if !boundedSealedMetadata(reflect.ValueOf(job), materializer.maxBytes) ||
		!boundedSealedMetadata(reflect.ValueOf(input.Manifest), materializer.maxBytes) ||
		len(input.Artifacts) > maxSealedCollectionItems || len(input.Context) > materializer.maxBytes ||
		len(input.Artifacts) != len(input.Manifest.Artifacts) ||
		uint64(len(input.Artifacts)) > uint64(job.Limits.MaxArtifacts) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	remaining := materializer.maxBytes - len(input.Context)
	for _, artifact := range input.Artifacts {
		if len(artifact.Name) > maxSealedMetadataValue || len(artifact.Body) > remaining {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
		remaining -= len(artifact.Body)
	}
	if input.CanonicalContext != nil && (!boundedCanonicalProof(input.CanonicalContext, materializer.maxBytes) ||
		!BoundedSerializedCanonicalInputV1(input, materializer.maxBytes)) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	if job.HarnessBinding.ValidateForScope(job.TenantID, request.OwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2) != nil {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil || !digestEquals(request.Attempt.ContextDigest, string(digest)) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	provider := job.HarnessBinding.Backend.ProviderContractKind == domain.ProviderContractInvocationV1
	if job.WorkspaceSnapshot != nil || job.SkillBundle != nil ||
		provider && !materializer.allowTestProvider ||
		!provider && job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1 {
		return MaterializedInputV1{}, ErrSealedInputUnsupported
	}
	if provider {
		identity := attachedworkerdaemon.InvocationIdentity{
			TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
			RunID: domain.RunID(request.Attempt.RunID), AttemptID: domain.AttemptID(request.Attempt.AttemptID),
			LeaseID: domain.LeaseID(request.Attempt.LeaseID), FenceToken: request.Attempt.LeaseGeneration,
		}
		fence, fenceErr := domain.NewAttachedWorkerFenceTokenV1(identity.TenantID, identity.OwnerUserID,
			identity.WorkerID, identity.RunID, identity.AttemptID, identity.LeaseID, identity.FenceToken)
		if job.HarnessBinding.Backend.CredentialDeliveryKind != domain.ProviderCredentialDeliveryFileV1 ||
			job.HarnessBinding.Resource.Kind != domain.ProviderResourceSubscriptionV1 ||
			fenceErr != nil || string(fence) != request.Attempt.FenceToken ||
			input.Credential == nil || input.Credential.ValidateFor(identity) != nil ||
			input.Credential.HomeEnvironment != "SESSIONLESS_PROVIDER_HOME" ||
			input.Credential.ExpectedBindingGeneration != job.HarnessBinding.Resource.CredentialGeneration ||
			input.Credential.IssueRequest.ProviderResource != job.HarnessBinding.Resource ||
			input.Credential.IssueRequest.Run.ID != job.RunID ||
			input.Credential.IssueRequest.Attempt.ID != job.AttemptID ||
			input.Credential.IssueRequest.Lease.ID != domain.LeaseID(request.Attempt.LeaseID) ||
			input.Credential.IssueRequest.Lease.FenceToken != request.Attempt.LeaseGeneration ||
			input.Credential.IssueRequest.ValidateAt(materializer.now().UTC()) != nil {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
	} else if input.Credential != nil {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	var contextAttachments []sealedContextAttachmentV1
	kind := "sessionless.attached-worker.synthetic-input.v1"
	if job.ContextWindow != nil {
		if job.ContextWindow.ThroughSequence > MaxSealedContextRecordsV1 || input.CanonicalContext == nil || !boundedCanonicalProof(input.CanonicalContext, materializer.maxBytes) {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
		history, refs, projectionErr := sessioncontext.ProjectCanonical(job, input.CanonicalContext, uint64(materializer.maxBytes))
		defer clearBytes(history)
		contextLimit := job.Limits.MaxContextBytes
		if contextLimit > uint64(materializer.maxBytes) {
			contextLimit = uint64(materializer.maxBytes)
		}
		if projectionErr != nil || !bytes.Equal(history, input.Context) ||
			sessioncontext.VerifyCanonicalAttachments(history, refs, input.CanonicalContext, contextLimit) != nil {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
		for _, attachment := range input.CanonicalContext.Attachments {
			contextAttachments = append(contextAttachments, sealedContextAttachmentV1{Name: attachment.Name, MediaType: attachment.MediaType, Body: attachment.Body})
		}
		kind = "sessionless.attached-worker.canonical-input.v1"
	} else if input.CanonicalContext != nil || !blobMatches(job.ContextSnapshot, input.Context, materializer.maxBytes) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	if uint64(len(input.Context)) > job.Limits.MaxContextBytes ||
		len(input.Artifacts) != len(input.Manifest.Artifacts) ||
		uint64(len(input.Artifacts)+len(contextAttachments)) > uint64(job.Limits.MaxArtifacts) ||
		len(input.Artifacts)+len(contextAttachments) > maxSealedCollectionItems {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	byName := make(map[string]domain.Artifact, len(input.Manifest.Artifacts))
	for _, artifact := range input.Manifest.Artifacts {
		byName[artifact.Name] = artifact
	}
	seen := make(map[string]struct{}, len(input.Artifacts))
	remaining = materializer.maxBytes - len(input.Context)
	for _, attachment := range contextAttachments {
		if len(attachment.Body) > remaining {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
		remaining -= len(attachment.Body)
	}
	for _, artifact := range input.Artifacts {
		sealed, found := byName[artifact.Name]
		if _, duplicate := seen[artifact.Name]; !found || duplicate ||
			!blobMatches(sealed.Blob, artifact.Body, remaining) {
			return MaterializedInputV1{}, ErrSealedInputInvalid
		}
		seen[artifact.Name] = struct{}{}
		remaining -= len(artifact.Body)
	}
	sort.Slice(input.Artifacts, func(left, right int) bool {
		return input.Artifacts[left].Name < input.Artifacts[right].Name
	})
	// The fixed envelope is for synthetic harnesses only. No blob key, host
	// path, executable, environment, or credential is serialized into stdin.
	envelope, err := json.Marshal(sealedEnvelopeV1{
		Version: 1, Kind: kind,
		Context: input.Context, Artifacts: input.Artifacts,
		ContextAttachments: contextAttachments,
	})
	if err != nil || len(envelope) == 0 || len(envelope) > materializer.maxBytes ||
		uint64(len(envelope)) > job.Limits.MaxInputBytes {
		clearBytes(envelope)
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	result := MaterializedInputV1{Stdin: envelope}
	if provider {
		credential := *input.Credential
		result.Credential = &credential
	}
	return result, nil
}

// boundedSealedMetadata rejects oversized source-controlled typed metadata
// without copying it. No digest, sort, path normalization, or JSON encoding
// runs until this bounded walk succeeds.
func boundedSealedMetadata(value reflect.Value, budget int) bool {
	return boundedMetadataWithCollectionLimit(value, budget, maxSealedCollectionItems)
}

func boundedMetadataWithCollectionLimit(value reflect.Value, budget, maxItems int) bool {
	if budget <= 0 {
		return false
	}
	var visit func(reflect.Value, int) bool
	visit = func(current reflect.Value, depth int) bool {
		if depth > 16 {
			return false
		}
		// A timestamp is a scalar in the domain contract. Walking its
		// private Location transition table would make valid input depend on
		// the host's tzdata size rather than source-controlled metadata.
		if current.Type() == reflect.TypeOf(time.Time{}) {
			return true
		}
		switch current.Kind() {
		case reflect.Interface, reflect.Pointer:
			return current.IsNil() || visit(current.Elem(), depth+1)
		case reflect.String:
			size := current.Len()
			if size > maxSealedMetadataValue || size > budget {
				return false
			}
			budget -= size
			return true
		case reflect.Slice, reflect.Array:
			if current.Len() > maxItems {
				return false
			}
			for index := 0; index < current.Len(); index++ {
				if !visit(current.Index(index), depth+1) {
					return false
				}
			}
		case reflect.Struct:
			for index := 0; index < current.NumField(); index++ {
				if !visit(current.Field(index), depth+1) {
					return false
				}
			}
		case reflect.Map:
			// The sealed domain contracts contain no maps. A future map field
			// needs an explicit bounded and deterministic review.
			return false
		}
		return true
	}
	return visit(value, 0)
}

// BoundedSealedInputMetadataV1 allows a source to reject malformed durable
// metadata before the context digest copies or sorts any collections.
func BoundedSealedInputMetadataV1(job domain.WorkerJob, manifest domain.ArtifactManifest, maxBytes int) bool {
	return boundedSealedMetadata(reflect.ValueOf(job), maxBytes) &&
		boundedSealedMetadata(reflect.ValueOf(manifest), maxBytes)
}

func blobMatches(ref domain.BlobRef, body []byte, maxBytes int) bool {
	if ref.Validate() != nil || ref.Size > int64(maxBytes) ||
		len(body) != int(ref.Size) {
		return false
	}
	digest := sha256.Sum256(body)
	return digestEquals(digest[:], ref.SHA256)
}

func digestEquals(raw []byte, encoded string) bool {
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == sha256.Size && bytes.Equal(raw, decoded)
}

func clearSealedInput(input *SealedInputV1) {
	input.CanonicalContext.Clear()
	clearBytes(input.Context)
	for index := range input.Artifacts {
		clearBytes(input.Artifacts[index].Body)
	}
	*input = SealedInputV1{}
}

// Proof metadata and raw bytes are bounded before decoding, hashing or JSON
// serialization. Payload byte slices are content, not metadata collections.
func boundedCanonicalProof(proof *sessioncontext.CanonicalProof, budget int) bool {
	if proof == nil || !BoundedCanonicalInputMetadataV1(proof.Input, budget) ||
		len(proof.EventBodies) > MaxSealedContextRecordsV1 || len(proof.Attachments) > maxSealedCollectionItems {
		return false
	}
	remaining := budget
	consume := func(body []byte) bool {
		if len(body) > remaining {
			return false
		}
		remaining -= len(body)
		return true
	}
	if !consume(proof.SnapshotBytes) {
		return false
	}
	for _, body := range proof.EventBodies {
		if !consume(body) {
			return false
		}
	}
	for _, attachment := range proof.Attachments {
		if !boundedSealedMetadata(reflect.ValueOf(attachment.AttachmentRef), budget) || !consume(attachment.Body) {
			return false
		}
	}
	encoded, err := json.Marshal(proof)
	defer clearBytes(encoded)
	return err == nil && len(encoded) <= budget
}

func BoundedCanonicalProofV1(proof *sessioncontext.CanonicalProof, maxBytes int) bool {
	return boundedCanonicalProof(proof, maxBytes)
}

func BoundedCanonicalInputMetadataV1(input domain.SessionContextInput, maxBytes int) bool {
	return boundedMetadataWithCollectionLimit(reflect.ValueOf(input), maxBytes, MaxSealedContextRecordsV1)
}

// Include base64 expansion and the proof plus projected-history duplication
// in the admitted serialized input budget, not only raw blob sizes.
func BoundedSerializedCanonicalInputV1(input SealedInputV1, maxBytes int) bool {
	if maxBytes <= 0 || uint64(maxBytes) > input.Job.Limits.MaxInputBytes {
		maxBytes = int(input.Job.Limits.MaxInputBytes)
	}
	if maxBytes <= 0 {
		return false
	}
	encoded, err := json.Marshal(input)
	defer clearBytes(encoded)
	return err == nil && len(encoded) <= maxBytes
}

var _ Materializer = (*BoundMaterializer)(nil)
