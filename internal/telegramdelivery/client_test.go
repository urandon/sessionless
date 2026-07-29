package telegramdelivery

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type rejectingBlobStore struct{}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

func (rejectingBlobStore) Put(
	context.Context,
	domain.TenantID,
	string,
	io.Reader,
) (domain.BlobRef, error) {
	panic("unexpected blob put")
}

func (rejectingBlobStore) Open(
	context.Context,
	domain.TenantID,
	domain.BlobRef,
) (io.ReadCloser, error) {
	panic("inline reply must not read a blob")
}

func (rejectingBlobStore) Delete(
	context.Context,
	domain.TenantID,
	domain.BlobRef,
) error {
	panic("unexpected blob delete")
}

func TestClientSendsInlineCommandReplyWithoutBlobRead(t *testing.T) {
	t.Parallel()
	var received struct {
		ChatID int64  `json:"chat_id"`
		Text   string `json:"text"`
	}
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			t.Errorf("path = %q", request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(
				`{"ok":true,"result":{"message_id":44,"date":1785326400}}`,
			)),
		}, nil
	})}

	client, err := NewClient("https://telegram.invalid", "test-token", httpClient, rejectingBlobStore{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Send(context.Background(), ports.TelegramSendRequest{
		TenantID: "tenant-a", DeliveryID: "delivery-inline",
		Chat:             domain.TelegramChatRef{TenantID: "tenant-a", ChatID: 123},
		ReplyToMessageID: 78, Text: "command reply",
		IdempotencyKey: "delivery-inline",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != 44 || received.ChatID != 123 || received.Text != "command reply" {
		t.Fatalf("result = %#v, request = %#v", result, received)
	}
}

var _ ports.BlobStore = rejectingBlobStore{}
