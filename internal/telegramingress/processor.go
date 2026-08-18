package telegramingress

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"mime"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
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
	SourceID              string
	Provider              string
	IdempotencyRetention  time.Duration
	DeliveryWakePublisher ports.TelegramDeliveryWakePublisher
	WakePublishError      func(error)
}

type CanonicalIngress interface {
	Ingest(context.Context, sessioningress.UserInput) (ports.CanonicalUserEventResult, error)
}

type Processor struct {
	config    ProcessorConfig
	identity  *IdentityResolver
	ids       ports.IDGenerator
	clock     ports.Clock
	files     FileFetcher
	canonical CanonicalIngress
	store     ports.TelegramControlStore
}

func NewProcessor(
	config ProcessorConfig,
	identity *IdentityResolver,
	ids ports.IDGenerator,
	clock ports.Clock,
	files FileFetcher,
	canonical CanonicalIngress,
	store ports.TelegramControlStore,
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
	if identity == nil || ids == nil || clock == nil || canonical == nil ||
		store == nil || config.DeliveryWakePublisher == nil {
		return nil, errors.New("Telegram processor dependencies must not be nil")
	}
	return &Processor{
		config: config, identity: identity, ids: ids, clock: clock,
		files: files, canonical: canonical, store: store,
	}, nil
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
		deliveryID := outboxwake.TelegramDeliveryID(runID)
		result, err := processor.store.ExecuteTelegramCommand(ctx, ports.TelegramCommandRequest{
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
		if err != nil {
			return ports.TelegramIngressResult{}, err
		}
		if err := processor.config.DeliveryWakePublisher.PublishTelegramDeliveryWake(
			ctx, identity.Tenant, outboxwake.TelegramDeliveryID(result.RunID), now,
		); err != nil {
			processor.reportWakePublishError(err)
		}
		return result, nil
	}

	attachments, fileMetadata, closeAttachments, err := processor.fetchAttachments(ctx, message)
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	defer closeAttachments()
	metadata := map[string]string{
		"telegram.source_id":  processor.config.SourceID,
		"telegram.update_id":  strconv.FormatInt(update.UpdateID, 10),
		"telegram.message_id": strconv.FormatInt(message.MessageID, 10),
		"telegram.chat_id":    strconv.FormatInt(message.Chat.ID, 10),
		"telegram.user_id":    strconv.FormatInt(message.From.ID, 10),
		"telegram.sent_at":    strconv.FormatInt(message.Date, 10),
	}
	if strings.TrimSpace(message.Caption) != "" {
		metadata["telegram.caption"] = message.Caption
	}
	for key, value := range fileMetadata {
		metadata[key] = value
	}
	text := message.Text
	if strings.TrimSpace(text) == "" {
		text = message.Caption
	}
	result, err := processor.canonical.Ingest(ctx, sessioningress.UserInput{
		Actor: sessioningress.Actor{
			TenantID: identity.Tenant, UserID: state.UserID,
			Frontend:               domain.FrontendTelegram,
			ExternalConversationID: identity.Conversation.ExternalID,
		},
		ExternalEventID: fmt.Sprintf("%s:%d", processor.config.SourceID, update.UpdateID),
		ReceivedAt:      now, Text: text, Metadata: metadata, Attachments: attachments,
		SubscriptionConnectionID: identity.SubscriptionConnection,
	})
	if err != nil {
		return ports.TelegramIngressResult{}, err
	}
	return ports.TelegramIngressResult{RunID: result.RunID, Created: result.Created}, nil
}

func (processor *Processor) reportWakePublishError(err error) {
	if processor.config.WakePublishError != nil {
		processor.config.WakePublishError(err)
	}
}

// Telegram control commands use a stable trigger identity for their terminal
// command run. Ordinary messages receive their event/run identities from the
// canonical ingress service instead.
func telegramTriggerEventID(sourceID string, updateID int64) domain.SessionEventID {
	sum := sha256.Sum256([]byte(fmt.Sprintf("telegram-event:%s:%d", sourceID, updateID)))
	return domain.SessionEventID(fmt.Sprintf("telegram-event:%x", sum[:16]))
}

func (processor *Processor) fetchAttachments(
	ctx context.Context,
	message Message,
) ([]sessioningress.Attachment, map[string]string, func(), error) {
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
		return nil, nil, nil, errors.New("Telegram file fetcher is required for attachment messages")
	}
	attachments := make([]sessioningress.Attachment, 0, len(files))
	metadata := make(map[string]string, len(files))
	closers := make([]io.Closer, 0, len(files))
	closeAll := func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}
	for index, pendingFile := range files {
		if strings.TrimSpace(pendingFile.fileID) == "" {
			closeAll()
			return nil, nil, nil, errors.New("Telegram attachment file_id must not be empty")
		}
		fetched, err := processor.files.Fetch(ctx, pendingFile.fileID)
		if err != nil {
			closeAll()
			return nil, nil, nil, fmt.Errorf("fetch Telegram attachment: %w", err)
		}
		if fetched.Body == nil {
			closeAll()
			return nil, nil, nil, errors.New("Telegram attachment body must not be nil")
		}
		closers = append(closers, fetched.Body)
		name := safeFileName(fetched.Name, pendingFile.name)
		contentType := strings.TrimSpace(fetched.MediaType)
		if contentType == "" {
			contentType = pendingFile.mediaType
		}
		attachments = append(attachments, sessioningress.Attachment{
			Name: name, MediaType: contentType, Body: fetched.Body,
		})
		metadata[fmt.Sprintf("telegram.attachment.%02d.file_id", index+1)] = pendingFile.fileID
	}
	return attachments, metadata, closeAll, nil
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
