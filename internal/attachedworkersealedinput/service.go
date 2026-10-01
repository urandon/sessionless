// Package attachedworkersealedinput serves only immutable, synthetic input for
// the exact claimed attached-worker attempt. It cannot select a process,
// credential, host path, or provider operation.
package attachedworkersealedinput

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxInputBytes = 1 << 20
const maxArtifacts = 64

var (
	ErrInvalid      = errors.New("attached worker sealed input request is invalid")
	ErrUnauthorized = errors.New("attached worker sealed input is unauthorized")
	ErrUnsupported  = errors.New("attached worker sealed input mode is unsupported")
	ErrUnavailable  = errors.New("attached worker sealed input is unavailable")
)

type Authorizer interface {
	AuthorizeSealedInputBearer(context.Context, []byte, ports.AttachedWorkerSealedInputAuthorization) (uint64, error)
}

type JobStore interface {
	LoadWorkerJob(context.Context, domain.TenantID, domain.RunID) (ports.WorkerJobState, bool, error)
}

type BlobStore interface {
	Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error)
}

type Service struct {
	authorizer Authorizer
	jobs       JobStore
	blobs      BlobStore
}

func NewService(authorizer Authorizer, jobs JobStore, blobs BlobStore) (*Service, error) {
	if authorizer == nil || jobs == nil || blobs == nil {
		return nil, ErrInvalid
	}
	return &Service{authorizer: authorizer, jobs: jobs, blobs: blobs}, nil
}

// Load holds no content authority between calls: it gates before any blob
// read and repeats that gate with the observed durable revision after every
// immutable byte has been read. Unknown authorization results release bytes.
func (service *Service) Load(ctx context.Context, bearer []byte, request attachedworkerdaemontransport.MaterializationRequestV1) (result attachedworkerdaemontransport.SealedInputV1, err error) {
	if service == nil || ctx == nil || ctx.Err() != nil || request.TenantID.Validate() != nil ||
		request.OwnerUserID.Validate() != nil || request.WorkerID.Validate() != nil ||
		request.ConnectionID.Validate() != nil || request.EnrollmentGeneration == 0 ||
		request.ConnectionGeneration == 0 || request.AttemptSequence != 1 ||
		request.Attempt.Validate() != nil || len(bearer) == 0 {
		return result, ErrInvalid
	}
	authorization := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		ConnectionID: request.ConnectionID, EnrollmentGeneration: request.EnrollmentGeneration,
		ConnectionGeneration: request.ConnectionGeneration, RunID: domain.RunID(request.Attempt.RunID),
		AttemptID: domain.AttemptID(request.Attempt.AttemptID), AttemptSequence: request.AttemptSequence,
		LeaseID: domain.LeaseID(request.Attempt.LeaseID), LeaseGeneration: request.Attempt.LeaseGeneration,
		FenceToken: domain.AttachedWorkerFenceToken(request.Attempt.FenceToken), LeaseExpiresAtUnixMicro: request.Attempt.ExpiresAtUnixMicro,
		ContextDigest:    domain.AttachedWorkerContextDigest(hex.EncodeToString(request.Attempt.ContextDigest)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(request.Attempt.CapabilityDigest)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(hex.EncodeToString(request.Attempt.PolicyDigest)),
	}
	revision, err := service.authorizer.AuthorizeSealedInputBearer(ctx, bearer, authorization)
	if err != nil || revision == 0 {
		return result, authorizationError(err)
	}
	state, found, err := service.jobs.LoadWorkerJob(ctx, request.TenantID, authorization.RunID)
	if err != nil {
		return result, ErrUnavailable
	}
	if !found || !sameJob(request, state) {
		return result, ErrUnauthorized
	}
	job, manifest := state.Job, state.InputManifest
	if job.ContextWindow != nil || job.WorkspaceSnapshot != nil || job.SkillBundle != nil ||
		job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1 {
		return result, ErrUnsupported
	}
	if len(manifest.Artifacts) > maxArtifacts || uint64(len(manifest.Artifacts)) > uint64(job.Limits.MaxArtifacts) ||
		job.ContextSnapshot.Size > maxInputBytes || job.ContextSnapshot.Size < 0 {
		return result, ErrInvalid
	}
	result.Job, result.Manifest = job, manifest
	defer func() {
		if err != nil {
			clearResult(&result)
		}
	}()
	remaining := int64(maxInputBytes)
	result.Context, err = service.readExact(ctx, request.TenantID, job.ContextSnapshot, remaining)
	if err != nil {
		return result, err
	}
	remaining -= int64(len(result.Context))
	for _, artifact := range manifest.Artifacts {
		if artifact.Blob.Size < 0 || artifact.Blob.Size > remaining {
			return result, ErrInvalid
		}
		body, readErr := service.readExact(ctx, request.TenantID, artifact.Blob, remaining)
		if readErr != nil {
			return result, readErr
		}
		result.Artifacts = append(result.Artifacts, attachedworkerdaemontransport.SealedArtifactV1{Name: artifact.Name, Body: body})
		remaining -= int64(len(body))
	}
	authorization.ExpectedAttemptRevision = revision
	observed, checkErr := service.authorizer.AuthorizeSealedInputBearer(ctx, bearer, authorization)
	if checkErr != nil || observed != revision {
		return result, authorizationError(checkErr)
	}
	return result, nil
}

func sameJob(request attachedworkerdaemontransport.MaterializationRequestV1, state ports.WorkerJobState) bool {
	job, manifest := state.Job, state.InputManifest
	if !attachedworkerdaemontransport.BoundedSealedInputMetadataV1(job, manifest, maxInputBytes) {
		return false
	}
	if job.TenantID != request.TenantID || job.RunID != domain.RunID(request.Attempt.RunID) ||
		job.AttemptID != domain.AttemptID(request.Attempt.AttemptID) ||
		job.CredentialOwnerUserID != request.OwnerUserID ||
		job.ExecutionPlacementV2.OwnerUserID != request.OwnerUserID ||
		job.ExecutionPlacementV2.WorkerID != request.WorkerID ||
		manifest.ID != job.InputManifestID || manifest.TenantID != request.TenantID || manifest.RunID != job.RunID {
		return false
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil || !sameDigest(string(digest), request.Attempt.ContextDigest) ||
		!sameDigest(string(job.ExecutionPlacementV2.CapabilityDigest), request.Attempt.CapabilityDigest) ||
		!sameDigest(string(job.ExecutionPlacementV2.PolicyDigest), request.Attempt.PolicyDigest) {
		return false
	}
	return true
}

func sameDigest(encoded string, raw []byte) bool {
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == sha256.Size && len(raw) == sha256.Size &&
		subtle.ConstantTimeCompare(decoded, raw) == 1
}

func (service *Service) readExact(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef, remaining int64) ([]byte, error) {
	if ref.Validate() != nil || ref.TenantID != tenant || ref.Size > remaining || ref.Size < 0 {
		return nil, ErrInvalid
	}
	reader, err := service.blobs.Open(ctx, tenant, ref)
	if err != nil || reader == nil {
		if reader != nil {
			_ = reader.Close()
		}
		return nil, ErrUnavailable
	}
	body, readErr := io.ReadAll(io.LimitReader(reader, ref.Size+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || ctx.Err() != nil {
		clearBytes(body)
		return nil, ErrUnavailable
	}
	digest := sha256.Sum256(body)
	if int64(len(body)) != ref.Size || !sameDigest(ref.SHA256, digest[:]) {
		clearBytes(body)
		return nil, ErrInvalid
	}
	return body, nil
}

func authorizationError(err error) error {
	if err == nil || errors.Is(err, ErrUnauthorized) || errors.Is(err, attachedworkertransport.ErrTransportUnauthorized) {
		return ErrUnauthorized
	}
	return ErrUnavailable
}

func clearResult(input *attachedworkerdaemontransport.SealedInputV1) {
	clearBytes(input.Context)
	for index := range input.Artifacts {
		clearBytes(input.Artifacts[index].Body)
	}
	*input = attachedworkerdaemontransport.SealedInputV1{}
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
