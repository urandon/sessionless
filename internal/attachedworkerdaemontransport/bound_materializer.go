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

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	defaultMaxSealedInputBytes = 1 << 20
	maxSealedCollectionItems   = 64
	maxSealedMetadataValue     = 4096
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
	Job       domain.WorkerJob
	Manifest  domain.ArtifactManifest
	Context   []byte
	Artifacts []SealedArtifactV1
}

// BoundMaterializer is the credential-free, in-memory synthetic input path.
// It creates no host read root and cannot select a process or credential. A
// separate reviewed path is required for context windows, workspace/skill
// bundles, provider credentials, and large artifact staging.
type BoundMaterializer struct {
	source   SealedInputSource
	maxBytes int
	now      func() time.Time
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
	Version   uint32             `json:"version"`
	Kind      string             `json:"kind"`
	Context   []byte             `json:"context"`
	Artifacts []SealedArtifactV1 `json:"artifacts"`
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
	if job.HarnessBinding.ValidateForScope(job.TenantID, request.OwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2) != nil {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil || !digestEquals(request.Attempt.ContextDigest, string(digest)) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	if job.ContextWindow != nil || job.WorkspaceSnapshot != nil || job.SkillBundle != nil ||
		job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1 {
		return MaterializedInputV1{}, ErrSealedInputUnsupported
	}
	if !blobMatches(job.ContextSnapshot, input.Context, materializer.maxBytes) ||
		uint64(len(input.Context)) > job.Limits.MaxContextBytes ||
		len(input.Artifacts) != len(input.Manifest.Artifacts) ||
		uint64(len(input.Artifacts)) > uint64(job.Limits.MaxArtifacts) {
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	byName := make(map[string]domain.Artifact, len(input.Manifest.Artifacts))
	for _, artifact := range input.Manifest.Artifacts {
		byName[artifact.Name] = artifact
	}
	seen := make(map[string]struct{}, len(input.Artifacts))
	remaining = materializer.maxBytes - len(input.Context)
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
		Version: 1, Kind: "sessionless.attached-worker.synthetic-input.v1",
		Context: input.Context, Artifacts: input.Artifacts,
	})
	if err != nil || len(envelope) == 0 || len(envelope) > materializer.maxBytes ||
		uint64(len(envelope)) > job.Limits.MaxInputBytes {
		clearBytes(envelope)
		return MaterializedInputV1{}, ErrSealedInputInvalid
	}
	return MaterializedInputV1{Stdin: envelope}, nil
}

// boundedSealedMetadata rejects oversized source-controlled typed metadata
// without copying it. No digest, sort, path normalization, or JSON encoding
// runs until this bounded walk succeeds.
func boundedSealedMetadata(value reflect.Value, budget int) bool {
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
			if current.Len() > maxSealedCollectionItems {
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
	clearBytes(input.Context)
	for index := range input.Artifacts {
		clearBytes(input.Artifacts[index].Body)
	}
	*input = SealedInputV1{}
}

var _ Materializer = (*BoundMaterializer)(nil)
