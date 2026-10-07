// Package attachedworkeroutput prepares canonical, server-owned output from a
// bounded attached-worker candidate. It does not authorize a connection or
// commit a terminal; those operations belong to the YDB receipt transaction.
package attachedworkeroutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxCandidateArtifactBytes uint64 = 64 << 20

var ErrCandidateInvalid = errors.New("attached-worker output candidate is invalid")

// CandidateFingerprint binds an exact submitted candidate to the immutable
// server receipt. Both the server and HTTPS client use this encoding.
func CandidateFingerprint(candidate Candidate) (string, error) {
	encoded, err := json.Marshal(candidate)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-output-candidate:v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

// Candidate has no canonical event IDs, manifest ID, idempotency keys or
// finalization digest. Those are allocated by the server. A source object is
// only a hint: its content is re-read, bounded, hashed and copied into a
// server-owned canonical key before it can become an artifact.
type Candidate struct {
	Status      domain.AttachedWorkerTerminalStatus
	Summary     string
	FailureCode string
	Artifacts   []ArtifactCandidate
}

type ArtifactCandidate struct {
	Name      string
	MediaType string
	Source    domain.BlobRef
}

// Materialize builds a canonical result without trusting candidate-selected
// IDs or a candidate-selected digest. The caller must first authorize the
// exact attempt; the receipt transaction must recheck that authority after
// these non-transactional Object Storage operations.
func Materialize(
	ctx context.Context,
	blobs ports.BlobStore,
	loaded ports.WorkerJobState,
	leaseID domain.LeaseID,
	leaseGeneration uint64,
	candidate Candidate,
	at time.Time,
) (ports.AttachedWorkerTerminalMaterialization, error) {
	if ctx == nil || blobs == nil || ctx.Err() != nil || at.IsZero() ||
		loaded.Job.ValidateForRun(loaded.Run) != nil ||
		loaded.Job.ExecutionPlacementV2.Kind != domain.ExecutionPlacementAttachedWorker ||
		loaded.Attempt.ValidateForRun(loaded.Run) != nil ||
		loaded.Reservation.ValidateForRun(loaded.Run) != nil ||
		loaded.Job.AttemptID != loaded.Attempt.ID ||
		loaded.Job.ReservationID != loaded.Reservation.ID ||
		leaseID.Validate() != nil || leaseGeneration == 0 ||
		at.Before(loaded.Run.CreatedAt) {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	if candidate.Status == domain.AttachedWorkerTerminalSucceeded {
		return materializeSuccess(ctx, blobs, loaded, leaseID, leaseGeneration, candidate, at)
	}
	if candidate.Status != domain.AttachedWorkerTerminalFailed &&
		candidate.Status != domain.AttachedWorkerTerminalCancelled {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	if candidate.Summary != "" || len(candidate.Artifacts) != 0 ||
		domain.ValidateOpaqueID("attached_worker_output.failure_code", candidate.FailureCode) != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	cancelled := candidate.Status == domain.AttachedWorkerTerminalCancelled
	payload, err := json.Marshal(struct {
		Schema    string `json:"schema"`
		Code      string `json:"code"`
		Cancelled bool   `json:"cancelled"`
	}{"sessionless.run-terminal-notice.v1", candidate.FailureCode, cancelled})
	if err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, err
	}
	eventID := domain.SessionEventID(stableID("evt", string(loaded.Run.ID), string(loaded.Attempt.ID), "terminal-notice"))
	event, err := putEvent(ctx, blobs, loaded.Run, eventID, domain.SessionEventSystemNotice, payload, "", at)
	if err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, err
	}
	return ports.AttachedWorkerTerminalMaterialization{Failure: &ports.WorkerFailure{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, AttemptID: loaded.Attempt.ID,
		ReservationID: loaded.Reservation.ID, LeaseID: leaseID, Fence: leaseGeneration,
		At: at, Cancelled: cancelled, Code: candidate.FailureCode,
		Events: []domain.SessionEventDraft{event},
	}}, nil
}

func materializeSuccess(
	ctx context.Context,
	blobs ports.BlobStore,
	loaded ports.WorkerJobState,
	leaseID domain.LeaseID,
	leaseGeneration uint64,
	candidate Candidate,
	at time.Time,
) (ports.AttachedWorkerTerminalMaterialization, error) {
	summary := strings.TrimSpace(candidate.Summary)
	if summary == "" || !utf8.ValidString(summary) || utf8.RuneCountInString(summary) > 32_000 ||
		candidate.FailureCode != "" || uint32(len(candidate.Artifacts)) > loaded.Job.Limits.MaxArtifacts {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	manifest := domain.ArtifactManifest{
		ID:       domain.ArtifactManifestID(stableID("art", string(loaded.Run.ID), string(loaded.Attempt.ID))),
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, CreatedAt: at,
	}
	maxBytes := loaded.Job.Limits.MaxInputBytes
	if maxBytes > maxCandidateArtifactBytes {
		maxBytes = maxCandidateArtifactBytes
	}
	var usedBytes uint64
	for _, artifact := range candidate.Artifacts {
		if !validArtifactName(artifact.Name) || strings.TrimSpace(artifact.MediaType) == "" ||
			artifact.Source.Validate() != nil || artifact.Source.TenantID != loaded.Run.TenantID ||
			!strings.HasPrefix(artifact.Source.Key, candidatePrefix(loaded)) ||
			artifact.Source.Size < 0 || uint64(artifact.Source.Size) > maxBytes-usedBytes {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
		}
		body, err := readVerified(ctx, blobs, loaded.Run.TenantID, artifact.Source, int64(maxBytes-usedBytes))
		if err != nil {
			return ports.AttachedWorkerTerminalMaterialization{}, err
		}
		usedBytes += uint64(len(body))
		digest := sha256.Sum256(body)
		key := domain.SessionRunObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, loaded.Run.ID) +
			"artifacts/sha256/" + hex.EncodeToString(digest[:])
		ref, err := putVerified(ctx, blobs, loaded.Run.TenantID, key, body)
		if err != nil {
			return ports.AttachedWorkerTerminalMaterialization{}, err
		}
		manifest.Artifacts = append(manifest.Artifacts, domain.Artifact{Name: artifact.Name, MediaType: artifact.MediaType, Blob: ref})
	}
	if err := manifest.ValidateForRun(loaded.Run); err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, err
	}
	payload, err := json.Marshal(struct {
		Schema             string                    `json:"schema"`
		Summary            string                    `json:"summary"`
		ArtifactManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
	}{"sessionless.assistant-message.v1", summary, manifest.ID})
	if err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, err
	}
	eventID := domain.SessionEventID(stableID("evt", string(loaded.Run.ID), string(loaded.Attempt.ID), "assistant"))
	event, err := putEvent(ctx, blobs, loaded.Run, eventID, domain.SessionEventAssistantMessage, payload, summary, at)
	if err != nil {
		return ports.AttachedWorkerTerminalMaterialization{}, err
	}
	return ports.AttachedWorkerTerminalMaterialization{Completion: &ports.WorkerCompletion{
		TenantID: loaded.Run.TenantID, RunID: loaded.Run.ID, AttemptID: loaded.Attempt.ID,
		ReservationID: loaded.Reservation.ID, LeaseID: leaseID, Fence: leaseGeneration,
		At: at, Manifest: manifest, Events: []domain.SessionEventDraft{event},
	}}, nil
}

func candidatePrefix(loaded ports.WorkerJobState) string {
	return domain.SessionRunObjectPrefix(loaded.Run.TenantID, loaded.Run.SessionID, loaded.Run.ID) +
		"attached-worker/candidates/" + string(loaded.Attempt.ID) + "/"
}

func validArtifactName(name string) bool {
	return name != "" && len(name) <= 255 && name != "." && name != ".." &&
		!strings.ContainsAny(name, "/\\\x00") && strings.TrimSpace(name) == name
}

func putEvent(ctx context.Context, blobs ports.BlobStore, run domain.Run, id domain.SessionEventID,
	kind domain.SessionEventKind, body []byte, display string, at time.Time,
) (domain.SessionEventDraft, error) {
	digest := sha256.Sum256(body)
	key := domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, id) +
		"payloads/sha256/" + hex.EncodeToString(digest[:]) + ".json"
	ref, err := putVerified(ctx, blobs, run.TenantID, key, body)
	if err != nil {
		return domain.SessionEventDraft{}, err
	}
	event := domain.SessionEventDraft{
		ID: id, Kind: kind,
		IdempotencyKey: domain.IdempotencyKey(stableID("evtkey", string(run.ID), string(id))),
		Payload:        ref, DisplayText: display, CreatedAt: at,
	}
	return event, event.ValidateForRun(run)
}

func putVerified(ctx context.Context, blobs ports.BlobStore, tenant domain.TenantID, key string, body []byte) (domain.BlobRef, error) {
	digest := sha256.Sum256(body)
	ref, err := blobs.Put(ctx, tenant, key, bytes.NewReader(body))
	if err != nil {
		return domain.BlobRef{}, err
	}
	if ref.Validate() != nil || ref.TenantID != tenant || ref.Key != key || ref.Size != int64(len(body)) ||
		ref.SHA256 != hex.EncodeToString(digest[:]) {
		return domain.BlobRef{}, ErrCandidateInvalid
	}
	if _, err := readVerified(ctx, blobs, tenant, ref, int64(len(body))); err != nil {
		return domain.BlobRef{}, err
	}
	return ref, nil
}

func readVerified(ctx context.Context, blobs ports.BlobStore, tenant domain.TenantID, ref domain.BlobRef, maxBytes int64) ([]byte, error) {
	if ref.Validate() != nil || ref.TenantID != tenant || maxBytes < 0 || ref.Size > maxBytes {
		return nil, ErrCandidateInvalid
	}
	reader, err := blobs.Open(ctx, tenant, ref)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(body)
	if int64(len(body)) != ref.Size || hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, ErrCandidateInvalid
	}
	return body, nil
}

func stableID(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		_, _ = io.WriteString(hash, value)
		_, _ = hash.Write([]byte{0})
	}
	return fmt.Sprintf("%s_%x", prefix, hash.Sum(nil)[:16])
}
