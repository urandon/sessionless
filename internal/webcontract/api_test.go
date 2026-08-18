package webcontract_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func TestMutationRequestsDoNotCarryTenantAuthority(t *testing.T) {
	t.Parallel()
	requests := []any{
		webcontract.CreateSessionRequest{IdempotencyKey: "create-1"},
		webcontract.ArchiveSessionRequest{Archived: true, IdempotencyKey: "archive-1"},
		webcontract.CreateMessageRequest{IdempotencyKey: "message-1", Text: "hello"},
		webcontract.CreateUploadIntentRequest{SessionID: "session-1", Name: "a.txt", MediaType: "text/plain", Size: 1, SHA256: strings.Repeat("a", 64)},
		webcontract.CommitUploadRequest{UploadID: "upload-1"},
	}
	for _, request := range requests {
		encoded, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), "tenant_id") {
			t.Fatalf("request %T carries tenant authority: %s", request, encoded)
		}
	}
}

func TestPaginationContractsAreBounded(t *testing.T) {
	t.Parallel()
	if err := (webcontract.SessionListQuery{Limit: webcontract.MaxPageSize, Status: "active"}).Validate(); err != nil {
		t.Fatalf("valid session page rejected: %v", err)
	}
	if err := (webcontract.SessionListQuery{Limit: webcontract.MaxPageSize + 1}).Validate(); err == nil {
		t.Fatal("unbounded session page accepted")
	}
	if err := (webcontract.EventListQuery{Limit: 0}).Validate(); err == nil {
		t.Fatal("zero-sized event page accepted")
	}
}

func TestCookieContracts(t *testing.T) {
	t.Parallel()
	session := webcontract.SessionCookie("opaque", 12*time.Hour)
	if session.Name != "__Host-sessionless" || !session.Secure || !session.HttpOnly ||
		session.Path != "/" || session.Domain != "" || session.SameSite != http.SameSiteLaxMode {
		t.Fatalf("session cookie = %#v", session)
	}
	csrf := webcontract.CSRFCookie("csrf", 12*time.Hour)
	if csrf.Name != "__Host-sessionless-csrf" || !csrf.Secure || csrf.HttpOnly ||
		csrf.Path != "/" || csrf.Domain != "" || csrf.SameSite != http.SameSiteStrictMode {
		t.Fatalf("CSRF cookie = %#v", csrf)
	}
}

func TestStableErrorEnvelopeStatusMapping(t *testing.T) {
	t.Parallel()
	for _, code := range []webcontract.ErrorCode{
		webcontract.ErrorInvalidRequest, webcontract.ErrorUnauthenticated,
		webcontract.ErrorAccessDenied, webcontract.ErrorCSRFFailed,
		webcontract.ErrorNotFound, webcontract.ErrorConflict,
		webcontract.ErrorPayloadTooLarge, webcontract.ErrorRateLimited,
		webcontract.ErrorTemporarilyUnavailable,
	} {
		if code.HTTPStatus() == 0 {
			t.Fatalf("code %q has no HTTP status", code)
		}
	}
}

func TestOIDCCallbackContract(t *testing.T) {
	t.Parallel()
	if err := (webcontract.OIDCCallback{Code: "code", State: "state"}).Validate(); err != nil {
		t.Fatalf("valid callback rejected: %v", err)
	}
	for _, callback := range []webcontract.OIDCCallback{
		{Code: "code"},
		{State: "state"},
		{Code: "code", State: "state", Error: "access_denied"},
	} {
		if err := callback.Validate(); err == nil {
			t.Fatalf("invalid callback accepted: %#v", callback)
		}
	}
}

func TestBoundedMessageAndUploadContracts(t *testing.T) {
	t.Parallel()
	message := webcontract.CreateMessageRequest{IdempotencyKey: "message-1", Text: "hello"}
	if err := message.Validate(); err != nil {
		t.Fatalf("valid message rejected: %v", err)
	}
	upload := webcontract.CreateUploadIntentRequest{
		SessionID: "session-1", Name: "a.txt", MediaType: "text/plain", Size: 1, SHA256: strings.Repeat("a", 64),
	}
	if err := upload.Validate(1024); err != nil {
		t.Fatalf("valid upload rejected: %v", err)
	}
	upload.Size = 1025
	if err := upload.Validate(1024); err == nil {
		t.Fatal("oversized upload accepted")
	}
}
