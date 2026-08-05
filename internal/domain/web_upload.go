package domain

import (
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

var ErrUploadMismatch = errors.New("uploaded object does not match its upload intent")

type UploadIntentStatus string

const (
	UploadIntentPending   UploadIntentStatus = "pending"
	UploadIntentCommitted UploadIntentStatus = "committed"
)

type UploadIntent struct {
	ID             UploadIntentID     `json:"id"`
	TenantID       TenantID           `json:"tenant_id"`
	UserID         UserID             `json:"user_id"`
	SessionID      SessionID          `json:"session_id"`
	ObjectKey      string             `json:"object_key"`
	Name           string             `json:"name"`
	MediaType      string             `json:"media_type"`
	ExpectedSize   int64              `json:"expected_size"`
	ExpectedSHA256 string             `json:"expected_sha256"`
	Status         UploadIntentStatus `json:"status"`
	CreatedAt      time.Time          `json:"created_at"`
	ExpiresAt      time.Time          `json:"expires_at"`
	CommittedAt    *time.Time         `json:"committed_at,omitempty"`
}

func UploadIntentObjectPrefix(tenantID TenantID, intentID UploadIntentID) string {
	return TenantObjectPrefix(tenantID) + "uploads/" + string(intentID) + "/"
}

func (intent UploadIntent) Validate() error {
	if err := intent.ID.Validate(); err != nil {
		return err
	}
	if err := intent.TenantID.Validate(); err != nil {
		return err
	}
	if err := intent.UserID.Validate(); err != nil {
		return err
	}
	if err := intent.SessionID.Validate(); err != nil {
		return err
	}
	if path.Clean(intent.ObjectKey) != intent.ObjectKey || strings.HasPrefix(intent.ObjectKey, "/") ||
		!strings.HasPrefix(intent.ObjectKey, UploadIntentObjectPrefix(intent.TenantID, intent.ID)) {
		return ValidationError{
			Field:  "upload_intent.object_key",
			Reason: fmt.Sprintf("must be under %q", UploadIntentObjectPrefix(intent.TenantID, intent.ID)),
		}
	}
	if strings.TrimSpace(intent.Name) == "" || strings.TrimSpace(intent.MediaType) == "" {
		return ValidationError{Field: "upload_intent.metadata", Reason: "name and media_type are required"}
	}
	if intent.ExpectedSize <= 0 {
		return ValidationError{Field: "upload_intent.expected_size", Reason: "must be positive"}
	}
	if err := validateSHA256("upload_intent.expected_sha256", intent.ExpectedSHA256); err != nil {
		return err
	}
	if intent.Status != UploadIntentPending && intent.Status != UploadIntentCommitted {
		return ValidationError{Field: "upload_intent.status", Reason: "is unknown"}
	}
	if intent.CreatedAt.IsZero() || !intent.ExpiresAt.After(intent.CreatedAt) {
		return ValidationError{Field: "upload_intent.expires_at", Reason: "must be after a non-zero created_at"}
	}
	if intent.Status == UploadIntentCommitted && intent.CommittedAt == nil {
		return ValidationError{Field: "upload_intent.committed_at", Reason: "is required when committed"}
	}
	if intent.Status == UploadIntentPending && intent.CommittedAt != nil {
		return ValidationError{Field: "upload_intent.committed_at", Reason: "is allowed only when committed"}
	}
	return nil
}

// Commit validates storage metadata observed server-side. The browser never
// supplies an authoritative tenant, object key, size, or digest.
func (intent *UploadIntent) Commit(blob BlobRef, at time.Time) error {
	if intent == nil {
		return ValidationError{Field: "upload_intent", Reason: "must not be nil"}
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	if intent.Status == UploadIntentCommitted {
		return ErrUploadIntentCommitted
	}
	if !at.Before(intent.ExpiresAt) {
		return ErrUploadIntentExpired
	}
	if err := blob.Validate(); err != nil {
		return err
	}
	if blob.TenantID != intent.TenantID || blob.Key != intent.ObjectKey ||
		blob.Size != intent.ExpectedSize || blob.SHA256 != intent.ExpectedSHA256 {
		return ErrUploadMismatch
	}
	intent.Status, intent.CommittedAt = UploadIntentCommitted, &at
	return nil
}

func validateSHA256(field, value string) error {
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != 32 || value != strings.ToLower(value) {
		return ValidationError{Field: field, Reason: "must be a lowercase 64-character SHA-256 digest"}
	}
	return nil
}
