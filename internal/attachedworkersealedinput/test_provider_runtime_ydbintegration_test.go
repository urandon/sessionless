//go:build ydbintegration

package attachedworkersealedinput

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

func TestTestProviderExchangeCloseRevokesBothBearers(t *testing.T) {
	server := httptest.NewTLSServer(http.NotFoundHandler())
	defer server.Close()
	base, err := NewSessionSourceFactory(server.URL+PathV1, server.Client(),
		&exchangeFixture{port: &exchangePortFixture{}})
	if err != nil {
		t.Fatal(err)
	}
	source := &testProviderSource{SessionSourceFactory: base,
		receiptEndpoint: server.URL + attachedworkerreceipt.PathV1}
	exchange, err := source.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-provider-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	if source.publisher == nil {
		t.Fatal("receipt publisher was not bound")
	}
	if err := exchange.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if source.publisher != nil {
		t.Fatal("receipt publisher retained after session close")
	}
	if _, err := source.Publish(context.Background(), attachedworkerdaemontransport.ReceiptSubmissionV1{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed publisher remained usable: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
}
