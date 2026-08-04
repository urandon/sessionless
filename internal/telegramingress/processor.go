package telegramingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type File struct {
	Name      string
	MediaType string
	Body      io.ReadCloser
}

type FileFetcher interface {
	Fetch(ctx context.Context, fileID string) (File, error)
}

type ProcessorConfig struct {
	SourceID             string
	Provider             string
	IdempotencyRetention time.Duration
}

type Processor struct {
	config   ProcessorConfig
	identity *IdentityResolver
	ids      ports.IDGenerator
	clock    ports.Clock
	blobs    ports.BlobStore
	files    FileFetcher
	store    ports.TelegramIngressStore
}

func NewProcessor(
	config ProcessorConfig,
	identity *IdentityResolver,
	ids ports.IDGenerator,
	clock ports.Clock,
	blobs ports.BlobStore,
	files FileFetcher,
	store ports.TelegramIngressStore,
) (*Processor, error) {
	if err := domain.ValidateOpaqueID("telegram.source_id", config.SourceID); err != nil {
		return nil, err
	}
	if err := domain.ValidateOpaqueID("provider", config.Provider); err != nil {
		return nil, err
	}
	if config.IdempotencyRetention <= 0 {
		config.IdempotencyRetention = 30 * 24 * time.Hour
	}
	if identity == nil || ids == nil || clock == nil || blobs == nil || store == nil {
		return nil, errors.New("Telegram processor dependencies must not be nil")
	}
	return &Processor{
		config: config, identity: identity, ids: ids, clock: clock,
		blobs: blobs, files: files, store: store,
	}, nil
}

type normalizedMessage struct {
	Frontend  string                 `json:"frontend"`
	UpdateID  int64                  `json:"update_id"`
	MessageID int64                  `json:"message_id"`
	ChatID    int64                  `json:"chat_id"`
	UserID    int64                  `json:"user_id"`
	SentAt    int64                  `json:"sent_at"`
	Text      string                 `json:"text,omitempty"`
	Caption   string                 `json:"caption,omitempty"`
	Files     []normalizedAttachment `json:"files,omitempty"`
}

type normalizedAttachment struct {
	Name      string `json:"name"`
	MediaType string `json:"media_type"`
	FileID    string `json:"file_id"`
}

func (processor *Processor) Process(
	ctx context.Context,
	update Update,
) (ports.TelegramIngressResult, error) {
	if err := update.ValidatePrivate(); err != nil {
		return ports.TelegramIngressResult{}, fmt.Errorf("%w: %v", ErrUnsupportedUpdate, err)
	}
	message := *update.Message
	command, isCommand := parseCommand(message.Text)
	provider := processor.config.Provider
	if isCommand && (command == ports.TelegramCommandConnectCodex ||
		command == ports.TelegramCommandComputeStatus ||
		command == ports.TelegramCommandDisconnectCodex) {
		provider = "codex"
	}
	identity, err := processor.identity.ResolvePrivate(
		message.Chat.ID, message.From.ID, provider,
	)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	now := processor.clock.Now().UTC()
	state, err := processor.store.EnsureTelegramIdentity(ctx, ports.TelegramIdentityRequest{
		TenantID:                 identity.Tenant,
		Actor:                    identity.Actor,
		Conversation:             identity.Conversation,
		SubscriptionConnectionID: identity.SubscriptionConnection,
		Provider:                 provider,
		ObservedAt:               now,
	})
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	idempotencyKey := domain.IdempotencyKey(
		"tg:" + processor.config.SourceID + ":" + strconv.FormatInt(update.UpdateID, 10),
	)
	sessionID := state.SessionID
	triggerEventID := telegramTriggerEventID(processor.config.SourceID, update.UpdateID)
	if isCommand {
		if command == ports.TelegramCommandNewContext {
			sessionID, err = newTypedID[domain.SessionID](ctx, processor.ids, ports.IDSession)
			if err != nil {
				return ports.TelegramIngressResult{}, err
			}
		}
		runID, err := newTypedID[domain.RunID](ctx, processor.ids, ports.IDRun)
		if err != nil {
			return ports.TelegramIngressResult{}, err
		}
		deliveryID, err := newTypedID[domain.TelegramDeliveryID](
			ctx, processor.ids, ports.IDTelegramDelivery,
		)
		if err != nil {
			return ports.TelegramIngressResult{}, err
		}
		return processor.store.ExecuteTelegramCommand(ctx, ports.TelegramCommandRequest{
			TenantID: identity.Tenant, SourceID: processor.config.SourceID,
			UpdateID: update.UpdateID, ExpireAt: now.Add(processor.config.IdempotencyRetention),
			Kind: command, Provider: provider,
			Actor: identity.Actor, Conversation: identity.Conversation,
			SubscriptionConnectionID: identity.SubscriptionConnection,
			RunID:                    runID, DeliveryID: deliveryID,
			SessionID: sessionID, TriggerEventID: triggerEventID,
			Chat: domain.TelegramChatRef{
				TenantID: identity.Tenant,
				ChatID:   message.Chat.ID,
			},
			ReplyToMessageID: message.MessageID,
			IdempotencyKey:   idempotencyKey,
			RequestedAt:      now,
		})
	}

	runID, err := newTypedID[domain.RunID](ctx, processor.ids, ports.IDRun)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	attemptID, err := newTypedID[domain.AttemptID](ctx, processor.ids, ports.IDAttempt)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	manifestID, err := newTypedID[domain.ArtifactManifestID](ctx, processor.ids, ports.IDArtifactManifest)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	dispatchID, err := newTypedID[domain.DispatchOutboxID](ctx, processor.ids, ports.IDDispatchOutbox)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}

	normalized := normalizedMessage{
		Frontend: "telegram", UpdateID: update.UpdateID, MessageID: message.MessageID,
		ChatID: message.Chat.ID, UserID: message.From.ID, SentAt: message.Date,
		Text: message.Text, Caption: message.Caption,
	}
	attachments, descriptors, err := processor.storeAttachments(ctx, identity.Tenant, runID, message)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	normalized.Files = descriptors
	payload, err := json.Marshal(normalized)
	if err != nil {
		return ports.TelegramIngressResult{}, fmt.Errorf("encode normalized Telegram message: %w", err)
	}
	messageBlob, err := processor.blobs.Put(
		ctx, identity.Tenant, "inputs/"+string(runID)+"/message.json", bytes.NewReader(payload),
	)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	artifacts := []domain.Artifact{{
		Name: "message.json", MediaType: "application/json", Blob: messageBlob,
	}}
	artifacts = append(artifacts, attachments...)
	run := domain.Run{
		ID: runID, TenantID: identity.Tenant,
		SessionID: sessionID, TriggerEventID: triggerEventID,
		SubscriptionConnectionID: identity.SubscriptionConnection,
		Status:                   domain.RunCreated,
		IdempotencyKey:           idempotencyKey, CreatedAt: now, UpdatedAt: now,
	}
	attempt := domain.Attempt{
		ID: attemptID, TenantID: identity.Tenant, RunID: runID, Number: 1,
		Status: domain.AttemptCreated, CreatedAt: now, UpdatedAt: now,
	}
	manifest := domain.ArtifactManifest{
		ID: manifestID, TenantID: identity.Tenant, RunID: runID,
		Artifacts: artifacts, CreatedAt: now,
	}
	dispatch := domain.DispatchOutbox{
		ID: dispatchID, TenantID: identity.Tenant, RunID: runID, AttemptID: attemptID,
		InputManifestID: manifestID, ContextSnapshot: messageBlob,
		DeliveryChat: domain.TelegramChatRef{
			TenantID: identity.Tenant, ChatID: message.Chat.ID,
		},
		ReplyToMessageID: message.MessageID,
		Status:           domain.DispatchPending, IdempotencyKey: idempotencyKey,
		CreatedAt: now, UpdatedAt: now,
	}
	return processor.store.IngestTelegram(ctx, ports.TelegramIngress{
		TenantID: identity.Tenant, SourceID: processor.config.SourceID,
		UpdateID: update.UpdateID, ExpireAt: now.Add(processor.config.IdempotencyRetention),
		Run: run, Attempt: attempt, InputManifest: manifest, Dispatch: dispatch,
	})
}

// Telegram updates keep a stable trigger identity until SESSION-03 persists
// the corresponding canonical user event atomically with run creation.
func telegramTriggerEventID(sourceID string, updateID int64) domain.SessionEventID {
	sum := sha256.Sum256([]byte(fmt.Sprintf("telegram-event:%s:%d", sourceID, updateID)))
	return domain.SessionEventID(fmt.Sprintf("telegram-event:%x", sum[:16]))
}

func (processor *Processor) storeAttachments(
	ctx context.Context,
	tenantID domain.TenantID,
	runID domain.RunID,
	message Message,
) ([]domain.Artifact, []normalizedAttachment, error) {
	type pending struct {
		fileID, name, mediaType string
	}
	var files []pending
	if len(message.Photo) > 0 {
		photo := message.Photo[len(message.Photo)-1]
		files = append(files, pending{fileID: photo.FileID, name: "photo.jpg", mediaType: "image/jpeg"})
	}
	if message.Document != nil {
		files = append(files, pending{
			fileID:    message.Document.FileID,
			name:      safeFileName(message.Document.FileName, "document.bin"),
			mediaType: mediaType(message.Document.MIMEType, message.Document.FileName),
		})
	}
	if len(files) > 0 && processor.files == nil {
		return nil, nil, errors.New("Telegram file fetcher is required for attachment messages")
	}
	artifacts := make([]domain.Artifact, 0, len(files))
	descriptors := make([]normalizedAttachment, 0, len(files))
	for index, pendingFile := range files {
		if strings.TrimSpace(pendingFile.fileID) == "" {
			return nil, nil, errors.New("Telegram attachment file_id must not be empty")
		}
		fetched, err := processor.files.Fetch(ctx, pendingFile.fileID)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch Telegram attachment: %w", err)
		}
		if fetched.Body == nil {
			return nil, nil, errors.New("Telegram attachment body must not be nil")
		}
		name := safeFileName(fetched.Name, pendingFile.name)
		contentType := strings.TrimSpace(fetched.MediaType)
		if contentType == "" {
			contentType = pendingFile.mediaType
		}
		ref, putErr := processor.blobs.Put(
			ctx, tenantID,
			fmt.Sprintf("inputs/%s/attachments/%02d-%s", runID, index+1, name),
			fetched.Body,
		)
		closeErr := fetched.Body.Close()
		if putErr != nil {
			return nil, nil, putErr
		}
		if closeErr != nil {
			return nil, nil, closeErr
		}
		artifactName := fmt.Sprintf("attachment-%02d-%s", index+1, name)
		artifacts = append(artifacts, domain.Artifact{
			Name: artifactName, MediaType: contentType, Blob: ref,
		})
		descriptors = append(descriptors, normalizedAttachment{
			Name: artifactName, MediaType: contentType, FileID: pendingFile.fileID,
		})
	}
	return artifacts, descriptors, nil
}

func newTypedID[T ~string](
	ctx context.Context,
	generator ports.IDGenerator,
	kind ports.IDKind,
) (T, error) {
	value, err := generator.NewID(ctx, kind)
	return T(value), err
}

func safeFileName(candidate, fallback string) string {
	candidate = filepath.Base(strings.TrimSpace(candidate))
	if candidate == "" || candidate == "." || candidate == string(filepath.Separator) {
		return fallback
	}
	candidate = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, candidate)
	if candidate == "" {
		return fallback
	}
	return candidate
}

func mediaType(candidate, fileName string) string {
	if strings.TrimSpace(candidate) != "" {
		return candidate
	}
	if detected := mime.TypeByExtension(filepath.Ext(fileName)); detected != "" {
		return detected
	}
	return "application/octet-stream"
}

var _ UpdateProcessor = (*Processor)(nil)
