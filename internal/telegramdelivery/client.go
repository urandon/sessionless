package telegramdelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxReplyBytes = 4 << 20

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	blobs      ports.BlobStore
}

func NewClient(
	baseURL, token string,
	httpClient *http.Client,
	blobs ports.BlobStore,
) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("Telegram API base URL must be an absolute HTTP(S) URL")
	}
	if token == "" || blobs == nil {
		return nil, errors.New("Telegram token and blob store are required")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		baseURL: parsed.String(), token: token, httpClient: httpClient, blobs: blobs,
	}, nil
}

func (client *Client) Send(
	ctx context.Context,
	request ports.TelegramSendRequest,
) (ports.TelegramSendResult, error) {
	if err := request.Chat.Validate(); err != nil {
		return ports.TelegramSendResult{}, err
	}
	if err := domain.EnsureSameTenant(request.TenantID, request.Chat.TenantID); err != nil {
		return ports.TelegramSendResult{}, err
	}
	payload, err := client.readBlob(ctx, request.TenantID, request.Payload)
	if err != nil {
		return ports.TelegramSendResult{}, err
	}
	text := decodeReplyText(payload)
	result, err := client.sendMessage(ctx, request.Chat.ChatID, request.ReplyToMessageID, text)
	if err != nil {
		return ports.TelegramSendResult{}, err
	}
	for _, artifact := range request.Artifacts {
		if err := domain.EnsureSameTenant(request.TenantID, artifact.Blob.TenantID); err != nil {
			return ports.TelegramSendResult{}, err
		}
		if err := client.sendDocument(ctx, request.Chat.ChatID, artifact); err != nil {
			return ports.TelegramSendResult{}, err
		}
	}
	return result, nil
}

func (client *Client) sendMessage(
	ctx context.Context,
	chatID, replyTo int64,
	text string,
) (ports.TelegramSendResult, error) {
	payload, err := json.Marshal(map[string]any{
		"chat_id": chatID, "reply_to_message_id": replyTo, "text": text,
	})
	if err != nil {
		return ports.TelegramSendResult{}, err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, client.methodURL("sendMessage"), bytes.NewReader(payload),
	)
	if err != nil {
		return ports.TelegramSendResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return ports.TelegramSendResult{}, err
	}
	defer response.Body.Close()
	var result struct {
		OK     bool `json:"ok"`
		Result struct {
			MessageID int64 `json:"message_id"`
			Date      int64 `json:"date"`
		} `json:"result"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		return ports.TelegramSendResult{}, err
	}
	if response.StatusCode != http.StatusOK || !result.OK {
		return ports.TelegramSendResult{}, fmt.Errorf(
			"Telegram sendMessage failed with status %d: %s",
			response.StatusCode, result.Description,
		)
	}
	return ports.TelegramSendResult{
		MessageID: result.Result.MessageID,
		SentAt:    time.Unix(result.Result.Date, 0).UTC(),
	}, nil
}

func (client *Client) sendDocument(
	ctx context.Context,
	chatID int64,
	artifact domain.Artifact,
) error {
	data, err := client.readBlob(ctx, artifact.Blob.TenantID, artifact.Blob)
	if err != nil {
		return err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("chat_id", strconv.FormatInt(chatID, 10)); err != nil {
		return err
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(
		`form-data; name="document"; filename=%q`, artifact.Name,
	))
	header.Set("Content-Type", artifact.MediaType)
	part, err := writer.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := part.Write(data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, client.methodURL("sendDocument"), &body,
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("Telegram sendDocument failed with status %d", response.StatusCode)
	}
	return nil
}

func (client *Client) readBlob(
	ctx context.Context,
	tenantID domain.TenantID,
	ref domain.BlobRef,
) ([]byte, error) {
	body, err := client.blobs.Open(ctx, tenantID, ref)
	if err != nil {
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, maxReplyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxReplyBytes {
		return nil, fmt.Errorf("Telegram reply payload exceeds %d bytes", maxReplyBytes)
	}
	return data, nil
}

func (client *Client) methodURL(method string) string {
	return client.baseURL + "/bot" + client.token + "/" + method
}

func decodeReplyText(payload []byte) string {
	var structured struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(payload, &structured) == nil && structured.Text != "" {
		return structured.Text
	}
	return string(payload)
}

var _ ports.TelegramClient = (*Client)(nil)
