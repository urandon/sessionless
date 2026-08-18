package domain

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

var ErrUploadMismatch = errors.New("uploaded object does not match its upload intent")

var (
	ErrUploadIntentConflict     = errors.New("upload intent idempotency conflict")
	ErrUploadIntentNotCommitted = errors.New("upload intent has not been committed")
	ErrUploadIntentClaimed      = errors.New("upload intent is already claimed by another message")
)

type UploadIntentStatus string

const (
	UploadIntentPending   UploadIntentStatus = "pending"
	UploadIntentCommitted UploadIntentStatus = "committed"
)

type UploadIntent struct {
	ID                UploadIntentID     `json:"id"`
	TenantID          TenantID           `json:"tenant_id"`
	UserID            UserID             `json:"user_id"`
	SessionID         SessionID          `json:"session_id"`
	ObjectKey         string             `json:"object_key"`
	Name              string             `json:"name"`
	MediaType         string             `json:"media_type"`
	ExpectedSize      int64              `json:"expected_size"`
	ExpectedSHA256    string             `json:"expected_sha256"`
	ExpectedMD5       string             `json:"expected_md5"`
	Status            UploadIntentStatus `json:"status"`
	CreatedAt         time.Time          `json:"created_at"`
	ExpiresAt         time.Time          `json:"expires_at"`
	CommittedAt       *time.Time         `json:"committed_at,omitempty"`
	ObservedBlob      *BlobRef           `json:"observed_blob,omitempty"`
	ObservedMediaType string             `json:"observed_media_type,omitempty"`
	ObservedETag      string             `json:"observed_etag,omitempty"`
	ClaimedBy         *IdempotencyKey    `json:"claimed_by_message_idempotency_key,omitempty"`
	ClaimedAt         *time.Time         `json:"claimed_at,omitempty"`
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
	if err := validateMD5("upload_intent.expected_md5", intent.ExpectedMD5); err != nil {
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
	if intent.ObservedBlob != nil {
		if intent.Status != UploadIntentCommitted {
			return ValidationError{Field: "upload_intent.observed_blob", Reason: "is allowed only when committed"}
		}
		if err := intent.ObservedBlob.Validate(); err != nil {
			return err
		}
		if intent.ObservedBlob.TenantID != intent.TenantID || intent.ObservedBlob.Key != intent.ObjectKey ||
			intent.ObservedBlob.Size != intent.ExpectedSize || intent.ObservedBlob.SHA256 != intent.ExpectedSHA256 {
			return ErrUploadMismatch
		}
	}
	if intent.ObservedMediaType != "" && intent.ObservedMediaType != intent.MediaType {
		return ErrUploadMismatch
	}
	if intent.ClaimedBy != nil {
		if intent.Status != UploadIntentCommitted || intent.ClaimedAt == nil {
			return ValidationError{Field: "upload_intent.claim", Reason: "requires a committed intent and claimed_at"}
		}
		if err := intent.ClaimedBy.Validate(); err != nil {
			return err
		}
	} else if intent.ClaimedAt != nil {
		return ValidationError{Field: "upload_intent.claimed_at", Reason: "requires claimed_by"}
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
	observed := blob
	intent.ObservedBlob = &observed
	intent.ObservedMediaType = intent.MediaType
	return nil
}

// RecordObservedMetadata persists the complete server-observed object state.
// ETag is not trusted as a content digest; it is retained as an overwrite fence.
func (intent *UploadIntent) RecordObservedMetadata(blob BlobRef, mediaType, etag string, at time.Time) error {
	if strings.TrimSpace(mediaType) == "" {
		return ValidationError{Field: "upload_intent.observed_media_type", Reason: "must not be empty"}
	}
	if strings.TrimSpace(etag) == "" {
		return ValidationError{Field: "upload_intent.observed_etag", Reason: "must not be empty"}
	}
	if mediaType != intent.MediaType {
		return ErrUploadMismatch
	}
	if err := intent.Commit(blob, at); err != nil {
		return err
	}
	intent.ObservedMediaType = mediaType
	intent.ObservedETag = etag
	return nil
}

// Claim binds a committed upload to exactly one message idempotency key.
// Retrying that message is safe; a different message cannot reuse the object.
func (intent *UploadIntent) Claim(messageKey IdempotencyKey, at time.Time) error {
	if intent == nil {
		return ValidationError{Field: "upload_intent", Reason: "must not be nil"}
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	if err := messageKey.Validate(); err != nil {
		return err
	}
	if at.IsZero() {
		return ValidationError{Field: "upload_intent.claimed_at", Reason: "must not be zero"}
	}
	if intent.Status != UploadIntentCommitted {
		return ErrUploadIntentNotCommitted
	}
	if intent.ClaimedBy != nil {
		if *intent.ClaimedBy == messageKey {
			return nil
		}
		return ErrUploadIntentClaimed
	}
	key := messageKey
	intent.ClaimedBy, intent.ClaimedAt = &key, &at
	return nil
}

func validateSHA256(field, value string) error {
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != 32 || value != strings.ToLower(value) {
		return ValidationError{Field: field, Reason: "must be a lowercase 64-character SHA-256 digest"}
	}
	return nil
}

func validateMD5(field, value string) error {
	digest, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(digest) != 16 || base64.StdEncoding.EncodeToString(digest) != value {
		return ValidationError{Field: field, Reason: "must be a canonical standard-base64 MD5 digest"}
	}
	return nil
}
