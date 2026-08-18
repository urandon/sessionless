// Package webapi implements authenticated Web application operations on top
// of canonical Session services and exact-object capabilities.
package webapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

const (
	defaultUploadTTL     = 10 * time.Minute
	defaultCapabilityTTL = 5 * time.Minute
	defaultDownloadTTL   = 2 * time.Minute
	defaultMaxUpload     = int64(32 << 20)
)

var (
	ErrResourceUnavailable = errors.New("web resource is unavailable")
	ErrComputeUnavailable  = errors.New("exactly one compute connection is required")
)

type Config struct {
	IDKey             []byte
	UploadTTL         time.Duration
	CapabilityTTL     time.Duration
	DownloadTTL       time.Duration
	MaxUploadBytes    int64
	AllowedMediaTypes map[string]struct{}
	AllowedMCPServers []string
}

type Service struct {
	sessions          *sessionapi.Service
	ingress           *sessioningress.Service
	uploads           ports.WebUploadStore
	resources         ports.WebResourceStore
	objects           ports.WebObjectStore
	clock             ports.Clock
	idKey             []byte
	uploadTTL         time.Duration
	capabilityTTL     time.Duration
	downloadTTL       time.Duration
	maxUploadBytes    int64
	allowedMediaTypes map[string]struct{}
	allowedMCPServers []string
}

func New(
	config Config,
	sessions *sessionapi.Service,
	ingress *sessioningress.Service,
	uploads ports.WebUploadStore,
	resources ports.WebResourceStore,
	objects ports.WebObjectStore,
	clock ports.Clock,
) (*Service, error) {
	if sessions == nil || ingress == nil || uploads == nil || resources == nil || objects == nil || clock == nil {
		return nil, errors.New("web API dependencies must not be nil")
	}
	if len(config.IDKey) < 32 {
		return nil, errors.New("web API ID HMAC key must contain at least 32 bytes")
	}
	if config.UploadTTL <= 0 {
		config.UploadTTL = defaultUploadTTL
	}
	if config.CapabilityTTL <= 0 {
		config.CapabilityTTL = defaultCapabilityTTL
	}
	if config.DownloadTTL <= 0 {
		config.DownloadTTL = defaultDownloadTTL
	}
	if config.UploadTTL > defaultUploadTTL || config.CapabilityTTL > defaultCapabilityTTL ||
		config.DownloadTTL > defaultDownloadTTL {
		return nil, errors.New("web object capability lifetimes may only be shortened")
	}
	if config.UploadTTL%time.Second != 0 || config.CapabilityTTL%time.Second != 0 ||
		config.DownloadTTL%time.Second != 0 {
		return nil, errors.New("web object capability lifetimes must use whole seconds")
	}
	if config.MaxUploadBytes <= 0 {
		config.MaxUploadBytes = defaultMaxUpload
	}
	if len(config.AllowedMediaTypes) == 0 {
		config.AllowedMediaTypes = defaultMediaTypes()
	}
	return &Service{
		sessions: sessions, ingress: ingress, uploads: uploads, resources: resources,
		objects: objects, clock: clock, idKey: append([]byte(nil), config.IDKey...),
		uploadTTL: config.UploadTTL, capabilityTTL: config.CapabilityTTL,
		downloadTTL: config.DownloadTTL, maxUploadBytes: config.MaxUploadBytes,
		allowedMediaTypes: cloneSet(config.AllowedMediaTypes),
		allowedMCPServers: append([]string(nil), config.AllowedMCPServers...),
	}, nil
}

func (service *Service) MaxUploadBytes() int64 { return service.maxUploadBytes }

func (service *Service) CreateUpload(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	request webcontract.CreateUploadIntentRequest,
) (webcontract.UploadIntentResponse, bool, error) {
	if err := request.Validate(service.maxUploadBytes); err != nil {
		return webcontract.UploadIntentResponse{}, false, err
	}
	mediaType := strings.ToLower(strings.TrimSpace(request.MediaType))
	if _, allowed := service.allowedMediaTypes[mediaType]; !allowed {
		return webcontract.UploadIntentResponse{}, false, domain.ValidationError{
			Field: "upload.media_type", Reason: "is not allowed",
		}
	}
	now := service.clock.Now().UTC()
	uploadID := domain.UploadIntentID(service.stableID(
		"upl_", "upload", tenantID, userID, request.IdempotencyKey,
	))
	intent := domain.UploadIntent{
		ID: uploadID, TenantID: tenantID, UserID: userID, SessionID: request.SessionID,
		ObjectKey: domain.UploadIntentObjectPrefix(tenantID, uploadID) + "object",
		Name:      strings.TrimSpace(request.Name), MediaType: mediaType,
		ExpectedSize: request.Size, ExpectedSHA256: request.SHA256, ExpectedMD5: request.ContentMD5,
		Status: domain.UploadIntentPending, CreatedAt: now, ExpiresAt: now.Add(service.uploadTTL),
	}
	stored, created, err := service.uploads.CreateWebUploadIntent(ctx, ports.WebUploadCreateRequest{
		Intent: intent, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return webcontract.UploadIntentResponse{}, false, err
	}
	remaining := stored.ExpiresAt.Sub(now)
	if remaining <= 0 {
		return webcontract.UploadIntentResponse{}, false, domain.ErrUploadIntentExpired
	}
	if remaining > service.capabilityTTL {
		remaining = service.capabilityTTL
	}
	remaining = remaining.Truncate(time.Second)
	if remaining < time.Second {
		return webcontract.UploadIntentResponse{}, false, domain.ErrUploadIntentExpired
	}
	capability, err := service.objects.PresignUpload(ctx, ports.UploadCapabilityRequest{
		TenantID: stored.TenantID, ObjectKey: stored.ObjectKey,
		MediaType: stored.MediaType, Size: stored.ExpectedSize, SHA256: stored.ExpectedSHA256,
		ContentMD5: stored.ExpectedMD5,
		ExpiresIn:  remaining,
	})
	if err != nil {
		return webcontract.UploadIntentResponse{}, false, err
	}
	return webcontract.UploadIntentResponse{
		UploadID: stored.ID, Method: capability.Method, URL: capability.URL,
		Headers: capability.Headers, ExpiresAt: capability.ExpiresAt,
	}, created, nil
}

func (service *Service) CommitUpload(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	uploadID domain.UploadIntentID,
) (domain.UploadIntent, error) {
	if err := uploadID.Validate(); err != nil {
		return domain.UploadIntent{}, err
	}
	metadata, err := service.objects.StatObject(
		ctx, tenantID, domain.UploadIntentObjectPrefix(tenantID, uploadID)+"object",
	)
	if err != nil {
		return domain.UploadIntent{}, err
	}
	return service.uploads.CommitWebUploadIntent(ctx, ports.WebUploadCommitRequest{
		TenantID: tenantID, UserID: userID, UploadID: uploadID,
		Observed: metadata, At: service.clock.Now().UTC(),
	})
}

func (service *Service) SubmitMessage(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	request webcontract.CreateMessageRequest,
) (webcontract.CreateMessageResponse, error) {
	if err := request.Validate(); err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	if err := sessionID.Validate(); err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	binding, err := service.sessions.BindFrontend(
		ctx, tenantID, userID, domain.FrontendWeb, string(sessionID), sessionID, 0,
	)
	if err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	now := service.clock.Now().UTC()
	mutationDigest, err := messageMutationDigest(request)
	if err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	input := sessioningress.BoundUserInput{
		Actor: sessioningress.Actor{
			TenantID: tenantID, UserID: userID, Frontend: domain.FrontendWeb,
			ExternalConversationID: string(sessionID),
		},
		Binding: binding, ExternalEventID: string(request.IdempotencyKey),
		MutationDigest: mutationDigest, ReceivedAt: now, Text: request.Text,
		AllowedMCPServers: append([]string(nil), service.allowedMCPServers...),
	}
	if existing, found, err := service.ingress.LookupBound(ctx, input); err != nil {
		return webcontract.CreateMessageResponse{}, err
	} else if found {
		run, runFound, err := service.resources.GetRunForUser(ctx, tenantID, userID, existing.RunID)
		if errors.Is(err, domain.ErrMembershipDenied) || !runFound {
			return webcontract.CreateMessageResponse{}, ErrResourceUnavailable
		}
		if err != nil {
			return webcontract.CreateMessageResponse{}, err
		}
		return messageResponse(existing, webcontract.ComputeConnection{
			Provider: run.Provider, Entitlement: domain.EntitlementUnknown,
			Quota: domain.ProviderQuotaUnknown, ObservedAt: run.Run.UpdatedAt,
		}), nil
	}
	connections, err := service.resources.ResolveComputeConnectionsForUser(
		ctx, ports.ComputeConnectionResolveRequest{TenantID: tenantID, UserID: userID, SessionID: sessionID},
	)
	if err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	if len(connections) != 1 {
		return webcontract.CreateMessageResponse{}, ErrComputeUnavailable
	}
	input.SubscriptionConnectionID = connections[0].ID
	var claimed []domain.UploadIntent
	if len(request.UploadIDs) != 0 {
		claimed, err = service.uploads.ClaimWebUploadIntents(ctx, ports.WebUploadClaimRequest{
			TenantID: tenantID, UserID: userID, SessionID: sessionID,
			UploadIDs: request.UploadIDs, MessageIdempotencyKey: request.IdempotencyKey, At: now,
		})
		if err != nil {
			return webcontract.CreateMessageResponse{}, err
		}
	}
	plan, err := service.ingress.PlanBound(input)
	if err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	for index, intent := range claimed {
		if intent.ObservedBlob == nil || intent.ObservedETag == "" {
			return webcontract.CreateMessageResponse{}, domain.ErrUploadIntentNotCommitted
		}
		current, err := service.objects.StatObject(ctx, tenantID, intent.ObjectKey)
		if err != nil {
			return webcontract.CreateMessageResponse{}, err
		}
		if current.Blob != *intent.ObservedBlob || current.MediaType != intent.ObservedMediaType ||
			current.ETag != intent.ObservedETag {
			return webcontract.CreateMessageResponse{}, domain.ErrUploadMismatch
		}
		finalKey, err := plan.AttachmentObjectKey(intent.ID, index, intent.Name)
		if err != nil {
			return webcontract.CreateMessageResponse{}, err
		}
		ref, err := service.objects.PromoteObject(ctx, ports.PromoteObjectRequest{
			TenantID: tenantID, Source: *intent.ObservedBlob, SourceETag: intent.ObservedETag,
			FinalKey: finalKey, MediaType: intent.MediaType,
		})
		if err != nil {
			return webcontract.CreateMessageResponse{}, err
		}
		if ref.Size != intent.ExpectedSize || ref.SHA256 != intent.ExpectedSHA256 {
			return webcontract.CreateMessageResponse{}, domain.ErrUploadMismatch
		}
		input.Attachments = append(input.Attachments, sessioningress.StoredAttachment{
			Name: intent.Name, MediaType: intent.MediaType, Blob: ref,
		})
	}
	result, err := service.ingress.IngestBound(ctx, input)
	if err != nil {
		return webcontract.CreateMessageResponse{}, err
	}
	return messageResponse(result, webcontract.ComputeConnection{
		Provider: connections[0].Provider, Entitlement: connections[0].Entitlement,
		Quota: connections[0].Quota, ObservedAt: connections[0].ObservedAt,
	}), nil
}

// ComputeStatus returns only the participant-authorized, credential-free
// compute projection used by message admission. The bounded resolver returns
// at most two connections, which is sufficient to distinguish a usable exact
// selection from missing or ambiguous configuration.
func (service *Service) ComputeStatus(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
) (webcontract.ComputeStatusResponse, error) {
	if err := sessionID.Validate(); err != nil {
		return webcontract.ComputeStatusResponse{}, err
	}
	connections, err := service.resources.ResolveComputeConnectionsForUser(
		ctx, ports.ComputeConnectionResolveRequest{
			TenantID: tenantID, UserID: userID, SessionID: sessionID,
		},
	)
	if errors.Is(err, domain.ErrMembershipDenied) {
		return webcontract.ComputeStatusResponse{}, ErrResourceUnavailable
	}
	if err != nil {
		return webcontract.ComputeStatusResponse{}, err
	}
	switch len(connections) {
	case 0:
		return webcontract.ComputeStatusResponse{Availability: webcontract.ComputeNotConfigured}, nil
	case 1:
		return webcontract.ComputeStatusResponse{
			Availability: webcontract.ComputeReady,
			Connection: &webcontract.ComputeConnection{
				Provider: connections[0].Provider, Entitlement: connections[0].Entitlement,
				Quota: connections[0].Quota, ObservedAt: connections[0].ObservedAt,
			},
		}, nil
	default:
		return webcontract.ComputeStatusResponse{Availability: webcontract.ComputeAmbiguous}, nil
	}
}

func messageMutationDigest(request webcontract.CreateMessageRequest) (string, error) {
	material, err := json.Marshal(struct {
		Version   uint8                   `json:"version"`
		Text      string                  `json:"text"`
		UploadIDs []domain.UploadIntentID `json:"upload_ids"`
	}{Version: 1, Text: request.Text, UploadIDs: request.UploadIDs})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(material)
	return hex.EncodeToString(digest[:]), nil
}

func messageResponse(
	result ports.CanonicalUserEventResult,
	compute webcontract.ComputeConnection,
) webcontract.CreateMessageResponse {
	return webcontract.CreateMessageResponse{
		SessionID: result.SessionID, EventID: result.EventID, Sequence: result.Sequence,
		RunID: result.RunID, Created: result.Created, Compute: compute,
	}
}

func (service *Service) GetRun(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	runID domain.RunID,
) (ports.RunRecord, error) {
	record, found, err := service.resources.GetRunForUser(ctx, tenantID, userID, runID)
	if errors.Is(err, domain.ErrMembershipDenied) || !found {
		return ports.RunRecord{}, ErrResourceUnavailable
	}
	return record, err
}

func (service *Service) DownloadAttachment(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	sequence uint64,
	index uint32,
) (ports.ObjectCapability, error) {
	if sequence == 0 || index >= webcontract.MaxMessageUploadCount {
		return ports.ObjectCapability{}, domain.ValidationError{Field: "attachment", Reason: "selector is outside the bounded range"}
	}
	page, err := service.sessions.HistoryAfter(ctx, tenantID, userID, sessionID, sequence-1, 1)
	if err != nil {
		return ports.ObjectCapability{}, err
	}
	if len(page.Items) != 1 || page.Items[0].Event.Sequence != sequence {
		return ports.ObjectCapability{}, ErrResourceUnavailable
	}
	var envelope struct {
		Attachments []struct {
			Blob domain.BlobRef `json:"blob"`
		} `json:"attachments"`
	}
	if err := json.Unmarshal(page.Items[0].Payload, &envelope); err != nil || int(index) >= len(envelope.Attachments) {
		return ports.ObjectCapability{}, ErrResourceUnavailable
	}
	ref := envelope.Attachments[index].Blob
	if err := domain.ValidateSessionEventBlob(tenantID, sessionID, page.Items[0].Event.ID, ref); err != nil {
		return ports.ObjectCapability{}, err
	}
	return service.objects.PresignDownload(ctx, tenantID, ref, service.downloadTTL)
}

func (service *Service) DownloadRunArtifact(
	ctx context.Context,
	tenantID domain.TenantID,
	userID domain.UserID,
	sessionID domain.SessionID,
	runID domain.RunID,
	manifestID domain.ArtifactManifestID,
	index uint32,
) (webcontract.ArtifactCapabilityResponse, error) {
	artifact, found, err := service.resources.GetRunArtifactForUser(ctx, ports.WebRunArtifactRequest{
		TenantID: tenantID, UserID: userID, SessionID: sessionID,
		RunID: runID, ManifestID: manifestID, Index: index,
	})
	if errors.Is(err, domain.ErrMembershipDenied) || !found {
		return webcontract.ArtifactCapabilityResponse{}, ErrResourceUnavailable
	}
	if err != nil {
		return webcontract.ArtifactCapabilityResponse{}, err
	}
	capability, err := service.objects.PresignDownload(ctx, tenantID, artifact.Blob, service.downloadTTL)
	if err != nil {
		return webcontract.ArtifactCapabilityResponse{}, err
	}
	return webcontract.ArtifactCapabilityResponse{
		Name: artifact.Name, MediaType: artifact.MediaType, Size: artifact.Blob.Size,
		Download: webcontract.DownloadCapabilityResponse{
			Method: capability.Method, URL: capability.URL,
			Headers: capability.Headers, ExpiresAt: capability.ExpiresAt,
		},
	}, nil
}

func (service *Service) stableID(prefix string, values ...any) string {
	hash := hmac.New(sha256.New, service.idKey)
	_, _ = hash.Write([]byte("sessionless:web-api:v1:" + prefix + "\x00"))
	for _, value := range values {
		material := fmt.Sprint(value)
		_, _ = fmt.Fprintf(hash, "%d:", len(material))
		_, _ = hash.Write([]byte(material))
		_, _ = hash.Write([]byte{0})
	}
	return prefix + hex.EncodeToString(hash.Sum(nil)[:20])
}

func defaultMediaTypes() map[string]struct{} {
	return map[string]struct{}{
		"application/json": {}, "application/pdf": {}, "application/zip": {},
		"image/gif": {}, "image/jpeg": {}, "image/png": {}, "image/webp": {},
		"text/csv": {}, "text/markdown": {}, "text/plain": {},
	}
}

func cloneSet(source map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(source))
	for key := range source {
		result[strings.ToLower(strings.TrimSpace(key))] = struct{}{}
	}
	return result
}
