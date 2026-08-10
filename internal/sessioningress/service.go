// Package sessioningress implements the frontend-neutral application service
// that turns authenticated frontend deliveries into canonical session events.
package sessioningress

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type Config struct {
	IdempotencyRetention  time.Duration
	IDKey                 []byte
	DispatchWakePublisher ports.DispatchWakePublisher
	WakePublishError      func(error)
}

type Service struct {
	store        ports.CanonicalIngressStore
	blobs        ports.BlobStore
	retention    time.Duration
	idKey        []byte
	dispatchWake ports.DispatchWakePublisher
	wakeError    func(error)
}

func New(config Config, store ports.CanonicalIngressStore, blobs ports.BlobStore) (*Service, error) {
	if store == nil || blobs == nil {
		return nil, errors.New("canonical ingress dependencies must not be nil")
	}
	if len(config.IDKey) < 32 {
		return nil, errors.New("canonical ingress ID HMAC key must contain at least 32 bytes")
	}
	if config.IdempotencyRetention <= 0 {
		config.IdempotencyRetention = 30 * 24 * time.Hour
	}
	return &Service{
		store: store, blobs: blobs, retention: config.IdempotencyRetention,
		idKey: append([]byte(nil), config.IDKey...), dispatchWake: config.DispatchWakePublisher,
		wakeError: config.WakePublishError,
	}, nil
}

type Actor struct {
	TenantID               domain.TenantID
	UserID                 domain.UserID
	Frontend               domain.Frontend
	ExternalConversationID string
}

func (actor Actor) validate() error {
	if err := actor.TenantID.Validate(); err != nil {
		return err
	}
	if err := actor.UserID.Validate(); err != nil {
		return err
	}
	if err := actor.Frontend.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(actor.ExternalConversationID) == "" {
		return domain.ValidationError{Field: "external_conversation_id", Reason: "must not be empty"}
	}
	return nil
}

type Attachment struct {
	Name      string
	MediaType string
	Body      io.Reader
}

type UserInput struct {
	Actor                    Actor
	ExternalEventID          string
	ReceivedAt               time.Time
	Text                     string
	Metadata                 map[string]string
	Attachments              []Attachment
	SubscriptionConnectionID domain.SubscriptionConnectionID
	AllowedMCPServers        []string
}

type eventEnvelope struct {
	Version     uint32                     `json:"version"`
	Origin      domain.FrontendEventOrigin `json:"origin"`
	Text        string                     `json:"text,omitempty"`
	Metadata    map[string]string          `json:"metadata,omitempty"`
	Attachments []eventAttachment          `json:"attachments,omitempty"`
}

type eventAttachment struct {
	Name      string         `json:"name"`
	MediaType string         `json:"media_type"`
	Blob      domain.BlobRef `json:"blob"`
}

func (service *Service) EnsureSession(ctx context.Context, actor Actor, at time.Time) (ports.FrontendSessionState, error) {
	if err := actor.validate(); err != nil {
		return ports.FrontendSessionState{}, err
	}
	if at.IsZero() {
		return ports.FrontendSessionState{}, domain.ValidationError{Field: "frontend_session.at", Reason: "must not be zero"}
	}
	bindingID := domain.FrontendBindingID(service.stableID("binding", actor.TenantID, actor.Frontend, actor.ExternalConversationID))
	return service.store.EnsureFrontendSession(ctx, ports.FrontendSessionRequest{
		TenantID: actor.TenantID, UserID: actor.UserID, Frontend: actor.Frontend,
		ExternalConversationID: actor.ExternalConversationID,
		BindingID:              bindingID,
		SessionID:              domain.SessionID(service.stableID("session", actor.TenantID, bindingID, "initial")),
		At:                     at.UTC(),
	})
}

func (service *Service) NewSession(
	ctx context.Context,
	actor Actor,
	expectedRevision uint64,
	externalRequestID string,
	at time.Time,
) (ports.FrontendSessionState, error) {
	if err := actor.validate(); err != nil {
		return ports.FrontendSessionState{}, err
	}
	if strings.TrimSpace(externalRequestID) == "" {
		return ports.FrontendSessionState{}, domain.ValidationError{Field: "external_request_id", Reason: "must not be empty"}
	}
	if at.IsZero() {
		return ports.FrontendSessionState{}, domain.ValidationError{Field: "canonical_session_switch.at", Reason: "must not be zero"}
	}
	bindingID := domain.FrontendBindingID(service.stableID("binding", actor.TenantID, actor.Frontend, actor.ExternalConversationID))
	sessionID := domain.SessionID(service.stableID("session", actor.TenantID, bindingID, "new", externalRequestID))
	return service.store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: actor.TenantID, UserID: actor.UserID, BindingID: bindingID,
		ExpectedRevision: expectedRevision, SessionID: sessionID, At: at.UTC(),
	})
}

func (service *Service) Ingest(ctx context.Context, input UserInput) (ports.CanonicalUserEventResult, error) {
	if err := validateInput(input); err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	bindingID := domain.FrontendBindingID(service.stableID(
		"binding", input.Actor.TenantID, input.Actor.Frontend,
		input.Actor.ExternalConversationID,
	))
	idempotencyKey := domain.IdempotencyKey(service.stableID(
		"ingress", input.Actor.TenantID, input.Actor.Frontend,
		input.Actor.ExternalConversationID, input.ExternalEventID,
	))
	eventID := domain.SessionEventID(service.stableID("event", input.Actor.TenantID, bindingID, idempotencyKey))
	runID := domain.RunID(service.stableID("run", input.Actor.TenantID, bindingID, idempotencyKey))
	dispatchID := domain.DispatchOutboxID(service.stableID("dispatch", input.Actor.TenantID, runID))
	existing, err := service.store.LookupCanonicalUserEvent(ctx, ports.CanonicalUserEventLookup{
		TenantID: input.Actor.TenantID, UserID: input.Actor.UserID,
		BindingID: bindingID, Frontend: input.Actor.Frontend,
		ExternalConversationID: input.Actor.ExternalConversationID,
		IdempotencyKey:         idempotencyKey, EventID: eventID, RunID: runID,
	})
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	if existing.Found {
		if service.dispatchWake != nil {
			if err := service.dispatchWake.PublishDispatchWake(
				ctx, input.Actor.TenantID, dispatchID, input.ReceivedAt.UTC(),
			); err != nil {
				service.reportWakePublishError(err)
			}
		}
		return existing.Result, nil
	}
	state, err := service.EnsureSession(ctx, input.Actor, input.ReceivedAt)
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	origin := domain.FrontendEventOrigin{
		BindingID: state.Binding.ID, BindingRevision: state.Binding.Revision,
		Frontend: input.Actor.Frontend, ExternalConversationID: input.Actor.ExternalConversationID,
		ExternalEventID: input.ExternalEventID,
	}
	prefix := domain.SessionEventObjectPrefix(input.Actor.TenantID, state.Session.ID, eventID)
	uploadToken, err := newUploadToken()
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	uploadPrefix := prefix + "uploads/" + uploadToken + "/"
	artifacts := make([]domain.Artifact, 0, len(input.Attachments)+1)
	descriptors := make([]eventAttachment, 0, len(input.Attachments))
	for index, attachment := range input.Attachments {
		name := safeName(attachment.Name, fmt.Sprintf("attachment-%02d.bin", index+1))
		ref, err := service.blobs.Put(
			ctx, input.Actor.TenantID,
			fmt.Sprintf("%sattachments/%02d-%s", uploadPrefix, index+1, name), attachment.Body,
		)
		if err != nil {
			return ports.CanonicalUserEventResult{}, fmt.Errorf("store canonical attachment: %w", err)
		}
		artifactName := fmt.Sprintf("attachment-%02d-%s", index+1, name)
		artifacts = append(artifacts, domain.Artifact{Name: artifactName, MediaType: attachment.MediaType, Blob: ref})
		descriptors = append(descriptors, eventAttachment{Name: name, MediaType: attachment.MediaType, Blob: ref})
	}
	payload, err := json.Marshal(eventEnvelope{
		Version: 1, Origin: origin, Text: input.Text,
		Metadata: cloneMetadata(input.Metadata), Attachments: descriptors,
	})
	if err != nil {
		return ports.CanonicalUserEventResult{}, fmt.Errorf("encode canonical event envelope: %w", err)
	}
	payloadRef, err := service.blobs.Put(
		ctx, input.Actor.TenantID, uploadPrefix+"message.json", bytes.NewReader(payload),
	)
	if err != nil {
		return ports.CanonicalUserEventResult{}, fmt.Errorf("store canonical event envelope: %w", err)
	}
	artifacts = append([]domain.Artifact{{
		Name: "message.json", MediaType: "application/json", Blob: payloadRef,
	}}, artifacts...)
	result, err := service.store.CommitCanonicalUserEvent(ctx, ports.CanonicalUserEventCommit{
		TenantID: input.Actor.TenantID, UserID: input.Actor.UserID,
		BindingID: state.Binding.ID, ExpectedBindingRevision: state.Binding.Revision,
		Origin: origin, IdempotencyKey: idempotencyKey,
		ExpireAt: input.ReceivedAt.UTC().Add(service.retention),
		EventID:  eventID, Payload: payloadRef,
		RunID:                    runID,
		AttemptID:                domain.AttemptID(service.stableID("attempt", input.Actor.TenantID, runID, "1")),
		SubscriptionConnectionID: input.SubscriptionConnectionID,
		ManifestID:               domain.ArtifactManifestID(service.stableID("manifest", input.Actor.TenantID, runID)),
		Artifacts:                artifacts,
		DispatchID:               dispatchID,
		AllowedMCPServers:        append([]string(nil), input.AllowedMCPServers...),
		CommittedAt:              input.ReceivedAt.UTC(),
	})
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	if service.dispatchWake != nil {
		if err := service.dispatchWake.PublishDispatchWake(
			ctx, input.Actor.TenantID, dispatchID, input.ReceivedAt.UTC(),
		); err != nil {
			service.reportWakePublishError(err)
		}
	}
	return result, nil
}

func (service *Service) reportWakePublishError(err error) {
	if service.wakeError != nil {
		service.wakeError(err)
	}
}

func newUploadToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate immutable upload namespace: %w", err)
	}
	return hex.EncodeToString(token[:]), nil
}

func validateInput(input UserInput) error {
	if err := input.Actor.validate(); err != nil {
		return err
	}
	if strings.TrimSpace(input.ExternalEventID) == "" {
		return domain.ValidationError{Field: "external_event_id", Reason: "must not be empty"}
	}
	if input.ReceivedAt.IsZero() {
		return domain.ValidationError{Field: "received_at", Reason: "must not be zero"}
	}
	if strings.TrimSpace(input.Text) == "" && len(input.Attachments) == 0 {
		return domain.ValidationError{Field: "user_input", Reason: "text or an attachment is required"}
	}
	if err := input.SubscriptionConnectionID.Validate(); err != nil {
		return err
	}
	for _, attachment := range input.Attachments {
		if strings.TrimSpace(attachment.MediaType) == "" {
			return domain.ValidationError{Field: "attachment.media_type", Reason: "must not be empty"}
		}
		if attachment.Body == nil {
			return domain.ValidationError{Field: "attachment.body", Reason: "must not be nil"}
		}
	}
	for key := range input.Metadata {
		if strings.TrimSpace(key) == "" {
			return domain.ValidationError{Field: "metadata", Reason: "must not contain an empty key"}
		}
	}
	return nil
}

func (service *Service) stableID(prefix string, values ...any) string {
	hash := hmac.New(sha256.New, service.idKey)
	_, _ = hash.Write([]byte("sessionless:canonical-ingress:v1:" + prefix + "\x00"))
	for _, value := range values {
		material := fmt.Sprint(value)
		_, _ = fmt.Fprintf(hash, "%d:", len(material))
		_, _ = hash.Write([]byte(material))
		_, _ = hash.Write([]byte{0})
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:20])
}

func safeName(value, fallback string) string {
	value = filepath.Base(strings.TrimSpace(value))
	if value == "" || value == "." || value == ".." {
		return fallback
	}
	var result strings.Builder
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '.' || char == '-' || char == '_' {
			result.WriteRune(char)
		} else {
			result.WriteByte('_')
		}
	}
	if result.Len() == 0 {
		return fallback
	}
	return result.String()
}

func cloneMetadata(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
