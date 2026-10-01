package attachedworkersealedinput

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

type exchangeFixture struct {
	port  *exchangePortFixture
	err   error
	calls int
}

func (fixture *exchangeFixture) Open(_ attachedworkersession.ConnectionBindingV1, _ []byte) (attachedworkersession.ExchangePort, error) {
	fixture.calls++
	return fixture.port, fixture.err
}

type exchangePortFixture struct {
	closes       int
	closeStarted chan struct{}
	closeRelease chan struct{}
}

func (*exchangePortFixture) Exchange(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	return &attachedworkerprotocol.BatchV1{}, nil
}

func (fixture *exchangePortFixture) Close() error {
	fixture.closes++
	if fixture.closeStarted != nil {
		close(fixture.closeStarted)
	}
	if fixture.closeRelease != nil {
		<-fixture.closeRelease
	}
	return nil
}

func TestSessionSourceBindsOnlyAfterOpenAndRetiresWithExchange(t *testing.T) {
	service, authorizer, _, blobs, request := fixtureInput(t)
	server := httptest.NewTLSServer(Handler(service))
	defer server.Close()
	delegate := &exchangeFixture{port: &exchangePortFixture{}}
	factory, err := NewSessionSourceFactory(server.URL+PathV1, server.Client(), delegate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Load(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("pre-connection read=%v", err)
	}
	materializer, err := attachedworkerdaemontransport.NewBoundMaterializer(factory, 4096, func() time.Time { return time.Unix(20, 0).UTC() })
	if err != nil {
		t.Fatal(err)
	}
	exchange, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	input, err := materializer.Materialize(context.Background(), request)
	if err != nil || !bytes.Contains(input.Stdin, []byte("sessionless.attached-worker.synthetic-input.v1")) ||
		authorizer.calls != 2 || blobs.calls != 2 || input.Credential != nil {
		t.Fatalf("bound materialization error=%v authorizations=%d blobs=%d", err, authorizer.calls, blobs.calls)
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("other-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("second connection replaced source: %v", err)
	}
	if err := exchange.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if err := exchange.(interface{ Close() error }).Close(); err != nil {
		t.Fatal(err)
	}
	if delegate.port.closes != 0 {
		t.Fatalf("session close blocked on delegate close: %d", delegate.port.closes)
	}
	if err := factory.Close(); err != nil {
		t.Fatal(err)
	}
	if delegate.calls != 1 || delegate.port.closes != 1 {
		t.Fatalf("exchange open=%d close=%d", delegate.calls, delegate.port.closes)
	}
	if _, err := factory.Load(context.Background(), request); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("retired connection still fetched input: %v", err)
	}
}

func TestSessionSourceOpenFailureAndPreOpenCloseFailClosed(t *testing.T) {
	service, authorizer, _, blobs, request := fixtureInput(t)
	server := httptest.NewTLSServer(Handler(service))
	defer server.Close()
	delegate := &exchangeFixture{port: &exchangePortFixture{}, err: errors.New("exchange failed")}
	factory, err := NewSessionSourceFactory(server.URL+PathV1, server.Client(), delegate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed exchange created source: %v", err)
	}
	if _, err := factory.Load(context.Background(), request); !errors.Is(err, ErrUnavailable) ||
		authorizer.calls != 0 || blobs.calls != 0 || delegate.port.closes != 1 {
		t.Fatalf("failed exchange crossed read boundary: err=%v auth=%d blobs=%d closes=%d", err, authorizer.calls, blobs.calls, delegate.port.closes)
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed factory reopened: %v", err)
	}
	preclosed, err := NewSessionSourceFactory(server.URL+PathV1, server.Client(), &exchangeFixture{port: &exchangePortFixture{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := preclosed.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := preclosed.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("preclosed factory opened: %v", err)
	}
}
