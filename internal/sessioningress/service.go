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

// StoredAttachment is an already-authorized immutable object promoted into
// the canonical event namespace. Frontend adapters must not pass staging
// object references here.
type StoredAttachment struct {
	Name      string
	MediaType string
	Blob      domain.BlobRef
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

// LookupBound returns an already committed bound mutation before callers
// resolve mutable compute state or touch upload objects. MutationDigest, when
// present, binds the idempotency key to the original request content.
func (service *Service) LookupBound(
	ctx context.Context,
	input BoundUserInput,
) (ports.CanonicalUserEventResult, bool, error) {
	plan, err := service.PlanBound(input)
	if err != nil {
		return ports.CanonicalUserEventResult{}, false, err
	}
	existing, err := service.store.LookupCanonicalUserEvent(ctx, ports.CanonicalUserEventLookup{
		TenantID: input.Actor.TenantID, UserID: input.Actor.UserID,
		BindingID: input.Binding.ID, Frontend: input.Actor.Frontend,
		ExternalConversationID: input.Actor.ExternalConversationID,
		IdempotencyKey:         plan.IdempotencyKey, MutationDigest: input.MutationDigest,
		EventID: plan.EventID, RunID: plan.RunID,
	})
	if err != nil || !existing.Found {
		return ports.CanonicalUserEventResult{}, false, err
	}
	service.publishWake(ctx, input.Actor.TenantID, plan.DispatchID, input.ReceivedAt)
	return existing.Result, true, nil
}

// BoundUserInput is used by authenticated product frontends whose target
// Session already exists. Binding authority must come from a server-side
// participant-authorized operation, never directly from a browser selector.
type BoundUserInput struct {
	Actor                    Actor
	Binding                  domain.FrontendBinding
	ExternalEventID          string
	MutationDigest           string
	ReceivedAt               time.Time
	Text                     string
	Metadata                 map[string]string
	Attachments              []StoredAttachment
	SubscriptionConnectionID domain.SubscriptionConnectionID
	AllowedMCPServers        []string
}

// BoundPlan exposes deterministic identities needed to conditionally promote
// staged browser uploads before the canonical transaction. IngestBound
// recomputes and verifies the same plan.
type BoundPlan struct {
	TenantID       domain.TenantID
	SessionID      domain.SessionID
	BindingID      domain.FrontendBindingID
	IdempotencyKey domain.IdempotencyKey
	EventID        domain.SessionEventID
	RunID          domain.RunID
	DispatchID     domain.DispatchOutboxID
}

func (plan BoundPlan) AttachmentObjectKey(uploadID domain.UploadIntentID, index int, name string) (string, error) {
	if err := uploadID.Validate(); err != nil {
		return "", err
	}
	if index < 0 {
		return "", domain.ValidationError{Field: "attachment.index", Reason: "must not be negative"}
	}
	name = safeName(name, fmt.Sprintf("attachment-%02d.bin", index+1))
	return fmt.Sprintf("%sattachments/%02d-%s-%s", domain.SessionEventObjectPrefix(
		plan.TenantID, plan.SessionID, plan.EventID,
	), index+1, uploadID, name), nil
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
	current, err := service.EnsureSession(ctx, actor, at)
	if err != nil {
		return ports.FrontendSessionState{}, err
	}
	bindingID := current.Binding.ID
	sessionID := domain.SessionID(service.stableID("session", actor.TenantID, bindingID, "new", externalRequestID))
	return service.store.CreateAndSwitchFrontendSession(ctx, ports.CanonicalSessionSwitchRequest{
		TenantID: actor.TenantID, UserID: actor.UserID, BindingID: bindingID,
		ExpectedRevision: expectedRevision, SessionID: sessionID, At: at.UTC(),
	})
}

func (service *Service) PlanBound(input BoundUserInput) (BoundPlan, error) {
	if err := validateBoundPlanInput(input); err != nil {
		return BoundPlan{}, err
	}
	idempotencyKey := domain.IdempotencyKey(service.stableID(
		"ingress", input.Actor.TenantID, input.Actor.UserID, input.Actor.Frontend,
		input.Actor.ExternalConversationID, input.ExternalEventID,
	))
	eventID := domain.SessionEventID(service.stableID(
		"event", input.Actor.TenantID, input.Binding.ID, idempotencyKey,
	))
	runID := domain.RunID(service.stableID(
		"run", input.Actor.TenantID, input.Binding.ID, idempotencyKey,
	))
	return BoundPlan{
		TenantID: input.Actor.TenantID, SessionID: input.Binding.SessionID,
		BindingID: input.Binding.ID, IdempotencyKey: idempotencyKey,
		EventID: eventID, RunID: runID,
		DispatchID: domain.DispatchOutboxID(service.stableID("dispatch", input.Actor.TenantID, runID)),
	}, nil
}

// IngestBound commits a canonical user event for an existing, authorized
// frontend binding. Stored attachments must already be promoted to the exact
// deterministic event prefix returned by PlanBound.
func (service *Service) IngestBound(
	ctx context.Context,
	input BoundUserInput,
) (ports.CanonicalUserEventResult, error) {
	if err := validateBoundInput(input); err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	plan, err := service.PlanBound(input)
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	existing, found, err := service.LookupBound(ctx, input)
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	if found {
		return existing, nil
	}

	origin := domain.FrontendEventOrigin{
		BindingID: input.Binding.ID, BindingRevision: input.Binding.Revision,
		Frontend: input.Actor.Frontend, ExternalConversationID: input.Actor.ExternalConversationID,
		ExternalEventID: input.ExternalEventID,
	}
	descriptors := make([]eventAttachment, 0, len(input.Attachments))
	artifacts := make([]domain.Artifact, 0, len(input.Attachments)+1)
	for index, attachment := range input.Attachments {
		if err := domain.ValidateSessionEventBlob(
			input.Actor.TenantID, input.Binding.SessionID, plan.EventID, attachment.Blob,
		); err != nil {
			return ports.CanonicalUserEventResult{}, err
		}
		name := safeName(attachment.Name, fmt.Sprintf("attachment-%02d.bin", index+1))
		artifacts = append(artifacts, domain.Artifact{
			Name:      fmt.Sprintf("attachment-%02d-%s", index+1, name),
			MediaType: attachment.MediaType, Blob: attachment.Blob,
		})
		descriptors = append(descriptors, eventAttachment{
			Name: name, MediaType: attachment.MediaType, Blob: attachment.Blob,
		})
	}
	payload, err := json.Marshal(eventEnvelope{
		Version: 1, Origin: origin, Text: input.Text,
		Metadata: cloneMetadata(input.Metadata), Attachments: descriptors,
	})
	if err != nil {
		return ports.CanonicalUserEventResult{}, fmt.Errorf("encode canonical event envelope: %w", err)
	}
	uploadToken, err := newUploadToken()
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	payloadRef, err := service.blobs.Put(
		ctx, input.Actor.TenantID,
		domain.SessionEventObjectPrefix(input.Actor.TenantID, input.Binding.SessionID, plan.EventID)+
			"uploads/"+uploadToken+"/message.json",
		bytes.NewReader(payload),
	)
	if err != nil {
		return ports.CanonicalUserEventResult{}, fmt.Errorf("store canonical event envelope: %w", err)
	}
	artifacts = append([]domain.Artifact{{
		Name: "message.json", MediaType: "application/json", Blob: payloadRef,
	}}, artifacts...)
	result, err := service.store.CommitCanonicalUserEvent(ctx, ports.CanonicalUserEventCommit{
		TenantID: input.Actor.TenantID, UserID: input.Actor.UserID,
		BindingID: input.Binding.ID, ExpectedBindingRevision: input.Binding.Revision,
		Origin: origin, IdempotencyKey: plan.IdempotencyKey, MutationDigest: input.MutationDigest,
		ExpireAt: input.ReceivedAt.UTC().Add(service.retention),
		EventID:  plan.EventID, Payload: payloadRef, DisplayText: input.Text,
		RunID:                    plan.RunID,
		AttemptID:                domain.AttemptID(service.stableID("attempt", input.Actor.TenantID, plan.RunID, "1")),
		SubscriptionConnectionID: input.SubscriptionConnectionID,
		ManifestID:               domain.ArtifactManifestID(service.stableID("manifest", input.Actor.TenantID, plan.RunID)),
		Artifacts:                artifacts, DispatchID: plan.DispatchID,
		AllowedMCPServers: append([]string(nil), input.AllowedMCPServers...),
		CommittedAt:       input.ReceivedAt.UTC(),
	})
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	service.publishWake(ctx, input.Actor.TenantID, plan.DispatchID, input.ReceivedAt)
	return result, nil
}

func (service *Service) Ingest(ctx context.Context, input UserInput) (ports.CanonicalUserEventResult, error) {
	if err := validateInput(input); err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	state, err := service.EnsureSession(ctx, input.Actor, input.ReceivedAt)
	if err != nil {
		return ports.CanonicalUserEventResult{}, err
	}
	bindingID := state.Binding.ID
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
		DisplayText:              input.Text,
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

func (service *Service) publishWake(
	ctx context.Context,
	tenantID domain.TenantID,
	dispatchID domain.DispatchOutboxID,
	at time.Time,
) {
	if service.dispatchWake == nil {
		return
	}
	if err := service.dispatchWake.PublishDispatchWake(ctx, tenantID, dispatchID, at.UTC()); err != nil {
		service.reportWakePublishError(err)
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

func validateBoundInput(input BoundUserInput) error {
	if err := validateBoundPlanInput(input); err != nil {
		return err
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
		if err := attachment.Blob.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// validateBoundPlanInput validates only the authenticated identity and stable
// mutation material needed to derive canonical object/event IDs. Attachments
// may not have been promoted yet; IngestBound validates the completed content.
func validateBoundPlanInput(input BoundUserInput) error {
	if err := input.Actor.validate(); err != nil {
		return err
	}
	if err := input.Binding.Validate(); err != nil {
		return err
	}
	if input.Binding.TenantID != input.Actor.TenantID ||
		input.Binding.Frontend != input.Actor.Frontend ||
		input.Binding.ExternalConversationID != input.Actor.ExternalConversationID {
		return domain.ValidationError{Field: "frontend_binding", Reason: "does not match the authenticated actor"}
	}
	if strings.TrimSpace(input.ExternalEventID) == "" {
		return domain.ValidationError{Field: "external_event_id", Reason: "must not be empty"}
	}
	if input.ReceivedAt.IsZero() {
		return domain.ValidationError{Field: "received_at", Reason: "must not be zero"}
	}
	if input.MutationDigest != "" {
		digest, err := hex.DecodeString(input.MutationDigest)
		if err != nil || len(digest) != sha256.Size || input.MutationDigest != strings.ToLower(input.MutationDigest) {
			return domain.ValidationError{Field: "mutation_digest", Reason: "must be a lowercase SHA-256 digest"}
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
