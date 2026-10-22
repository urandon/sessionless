package attachedworkeroutput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

const (
	MaxInlineAnswerBytesV2   = 32 << 10
	MaxInlineArtifactBytesV2 = 32 << 10
	MaxInlineTotalBytesV2    = 64 << 10
	MaxInlineArtifactsV2     = 2
)

// InlineCandidateV2 describes original semantic bytes, never storage objects.
// It is untrusted wire metadata, not producer or write authority.
type InlineCandidateV2 struct {
	Summary   string             `json:"summary"`
	Artifacts []InlineArtifactV2 `json:"artifacts"`
}

type InlineArtifactV2 struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	Size      uint64 `json:"size"`
	SHA256    string `json:"sha256"`
}

// InlineProducerBindingV2 is declaration metadata, not a producer witness.
// Only the original completed native owner can supply it to its protected
// transport composition after process, credential and staging cleanup.
type InlineProducerBindingV2 struct {
	SessionID            domain.SessionID              `json:"session_id"`
	HarnessBindingDigest domain.HarnessBindingDigestV2 `json:"harness_binding_digest"`
	ReservationID        domain.QuotaReservationID     `json:"reservation_id"`
	AcceptedAt           time.Time                     `json:"accepted_at"`
	SchemaDigest         string                        `json:"schema_digest"`
	FinalDigest          string                        `json:"final_digest"`
	AnswerDigest         string                        `json:"answer_digest"`
}

// VerifiedInlineV2 owns a private copy of a fully checked concatenated body.
// This proves content integrity only. The receipt owner must authenticate the
// original attempt and durably acquire canonical-copy custody before Put.
type VerifiedInlineV2 struct {
	owner *inlineOwnerV2
}

type inlineOwnerV2 struct {
	mu        sync.Mutex
	valid     bool
	candidate InlineCandidateV2
	bodies    [][]byte
}

// Clear retires this owner's content only after exact ACK/journal retirement
// or an authorized definitive disposition. All copied values share this same
// retirement decision, including answer-only values. It serializes with any
// materialization already using the original bytes.
func (verified *VerifiedInlineV2) Clear() {
	if verified == nil || verified.owner == nil {
		return
	}
	owner := verified.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	for _, body := range owner.bodies {
		clear(body)
	}
	owner.valid, owner.bodies, owner.candidate = false, nil, InlineCandidateV2{}
}

func (candidate InlineCandidateV2) Validate() error {
	if strings.TrimSpace(candidate.Summary) == "" || len(candidate.Summary) > MaxInlineAnswerBytesV2 ||
		utf8.RuneCountInString(candidate.Summary) > 32_000 || !validInlineTextV2([]byte(candidate.Summary)) ||
		len(candidate.Artifacts) > MaxInlineArtifactsV2 {
		return ErrCandidateInvalid
	}
	var total uint64
	for i, artifact := range candidate.Artifacts {
		if !validInlineNameV2(artifact.Name) || (artifact.MediaType != "text/plain" && artifact.MediaType != "application/json") ||
			artifact.Size == 0 || artifact.Size > MaxInlineArtifactBytesV2 || artifact.Size > MaxInlineTotalBytesV2-total ||
			!validInlineDigestV2(artifact.SHA256) {
			return ErrCandidateInvalid
		}
		for _, prior := range candidate.Artifacts[:i] {
			if strings.EqualFold(prior.Name, artifact.Name) {
				return ErrCandidateInvalid
			}
		}
		total += artifact.Size
	}
	return nil
}

func InlineFingerprintV2(candidate InlineCandidateV2) (string, error) {
	if err := candidate.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-inline-candidate:v2\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}

// VerifyInlineV2 checks the complete plan before exposing any materializable
// body. It neither reads nor writes storage and never reserializes JSON.
func VerifyInlineV2(candidate InlineCandidateV2, body []byte) (VerifiedInlineV2, error) {
	if candidate.Validate() != nil || len(body) > MaxInlineTotalBytesV2 {
		return VerifiedInlineV2{}, ErrCandidateInvalid
	}
	var total uint64
	for _, artifact := range candidate.Artifacts {
		total += artifact.Size
	}
	if uint64(len(body)) != total {
		return VerifiedInlineV2{}, ErrCandidateInvalid
	}
	owner := &inlineOwnerV2{candidate: candidate}
	owner.candidate.Artifacts = append([]InlineArtifactV2(nil), candidate.Artifacts...)
	owned := bytes.Clone(body)
	offset := 0
	for _, artifact := range candidate.Artifacts {
		end := offset + int(artifact.Size)
		content := owned[offset:end:end]
		digest := sha256.Sum256(content)
		if hex.EncodeToString(digest[:]) != artifact.SHA256 || !validInlineTextV2(content) ||
			artifact.MediaType == "application/json" && strictjson.ValidateDocument(content, MaxInlineArtifactBytesV2, 32, MaxInlineArtifactBytesV2) != nil {
			clear(owned)
			return VerifiedInlineV2{}, ErrCandidateInvalid
		}
		owner.bodies = append(owner.bodies, content)
		offset = end
	}
	owner.valid = true
	return VerifiedInlineV2{owner: owner}, nil
}

// MaterializeInlineV2 shares the V1 canonical ID/key/copy/reread core without
// pretending that inline bytes are a storage object. A planning BlobStore may
// stage writes in memory; its owner must commit their custody before dispatch.
func MaterializeInlineV2(ctx context.Context, blobs ports.BlobStore, loaded ports.WorkerJobState,
	leaseID domain.LeaseID, leaseGeneration uint64, verified VerifiedInlineV2, at time.Time,
) (ports.AttachedWorkerTerminalMaterialization, error) {
	if verified.owner == nil || !validMaterializationScope(ctx, blobs, loaded, leaseID, leaseGeneration, at) {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	owner := verified.owner
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if !owner.valid || owner.candidate.Validate() != nil || len(owner.bodies) != len(owner.candidate.Artifacts) {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	var used uint64
	for i, artifact := range owner.candidate.Artifacts {
		if artifact.Size > loaded.Job.Limits.MaxInputBytes-used {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
		}
		digest := sha256.Sum256(owner.bodies[i])
		if uint64(len(owner.bodies[i])) != artifact.Size || hex.EncodeToString(digest[:]) != artifact.SHA256 {
			return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
		}
		used += artifact.Size
	}
	if uint32(len(owner.bodies)) > loaded.Job.Limits.MaxArtifacts {
		return ports.AttachedWorkerTerminalMaterialization{}, ErrCandidateInvalid
	}
	artifacts := make([]materializationArtifact, len(owner.bodies))
	for i, body := range owner.bodies {
		declaration := owner.candidate.Artifacts[i]
		artifacts[i] = materializationArtifact{name: declaration.Name, mediaType: declaration.MediaType,
			read: func(ctx context.Context, maxBytes uint64) ([]byte, error) {
				if ctx.Err() != nil || uint64(len(body)) > maxBytes {
					return nil, ErrCandidateInvalid
				}
				return body, nil
			}}
	}
	return materializeSuccess(ctx, blobs, loaded, leaseID, leaseGeneration, owner.candidate.Summary, "", artifacts, at)
}

func validInlineDigestV2(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}

func validInlineNameV2(name string) bool {
	if !validArtifactName(name) || !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func validInlineTextV2(body []byte) bool {
	return utf8.Valid(body) && bytes.IndexByte(body, 0) < 0
}
