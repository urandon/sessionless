package domain

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"time"
)

type BlobRef struct {
	TenantID TenantID `json:"tenant_id"`
	Key      string   `json:"key"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
}

func TenantObjectPrefix(tenantID TenantID) string {
	return "tenants/" + string(tenantID) + "/"
}

func (ref BlobRef) Validate() error {
	if err := ref.TenantID.Validate(); err != nil {
		return err
	}
	if ref.Key == "" || strings.HasPrefix(ref.Key, "/") || path.Clean(ref.Key) != ref.Key {
		return ValidationError{Field: "blob.key", Reason: "must be a normalized relative object key"}
	}
	if !strings.HasPrefix(ref.Key, TenantObjectPrefix(ref.TenantID)) {
		return ValidationError{Field: "blob.key", Reason: fmt.Sprintf("must be under %q", TenantObjectPrefix(ref.TenantID))}
	}
	if ref.Size < 0 {
		return ValidationError{Field: "blob.size", Reason: "must not be negative"}
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(digest) != 32 {
		return ValidationError{Field: "blob.sha256", Reason: "must be a 64-character hexadecimal SHA-256 digest"}
	}
	return nil
}

type Artifact struct {
	Name      string  `json:"name"`
	MediaType string  `json:"media_type"`
	Blob      BlobRef `json:"blob"`
}

func (artifact Artifact) Validate() error {
	if strings.TrimSpace(artifact.Name) == "" {
		return ValidationError{Field: "artifact.name", Reason: "must not be empty"}
	}
	if strings.TrimSpace(artifact.MediaType) == "" {
		return ValidationError{Field: "artifact.media_type", Reason: "must not be empty"}
	}
	return artifact.Blob.Validate()
}

type ArtifactManifest struct {
	ID        ArtifactManifestID `json:"id"`
	TenantID  TenantID           `json:"tenant_id"`
	RunID     RunID              `json:"run_id"`
	Artifacts []Artifact         `json:"artifacts"`
	CreatedAt time.Time          `json:"created_at"`
}

func (manifest ArtifactManifest) ValidateForRun(run Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	if err := manifest.ID.Validate(); err != nil {
		return err
	}
	if err := EnsureSameTenant(run.TenantID, manifest.TenantID); err != nil {
		return err
	}
	if manifest.RunID != run.ID {
		return ValidationError{Field: "artifact_manifest.run_id", Reason: "must reference the owning run"}
	}
	if manifest.CreatedAt.IsZero() {
		return ValidationError{Field: "artifact_manifest.created_at", Reason: "must not be zero"}
	}
	seen := make(map[string]struct{}, len(manifest.Artifacts))
	for _, artifact := range manifest.Artifacts {
		if err := artifact.Validate(); err != nil {
			return err
		}
		if err := EnsureSameTenant(run.TenantID, artifact.Blob.TenantID); err != nil {
			return err
		}
		if _, exists := seen[artifact.Name]; exists {
			return ValidationError{Field: "artifact_manifest.artifacts", Reason: "artifact names must be unique"}
		}
		seen[artifact.Name] = struct{}{}
	}
	return nil
}
