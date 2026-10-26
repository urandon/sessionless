package attachedworkersealedinput

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

func TestResourceGenerationRetirementCancelsSourcesBeforeExchangeCleanup(t *testing.T) {
	entered := make(chan string, 2)
	port := &exchangePortFixture{}
	factory := newReceiptFactory(t, receiptRoundTrip(func(request *http.Request) (*http.Response, error) {
		entered <- request.URL.Path
		<-request.Context().Done()
		return nil, request.Context().Err()
	}), &exchangeFixture{port: port})
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("synthetic-accepted-bearer")); err != nil {
		t.Fatal(err)
	}
	originalSource, originalPublisher := factory.source, factory.publisher
	ownedBearer := originalSource.bearer
	_, _, _, _, input := fixtureInput(t)
	submission := receiptSubmission(t)
	call, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 2)
	go func() { _, err := factory.Load(call, input); done <- err }()
	go func() { _, err := factory.Publish(call, submission); done <- err }()
	joined := 0
	t.Cleanup(func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer stop()
		for joined < 2 {
			select {
			case <-done:
				joined++
			case <-cleanup.Done():
				t.Error("source retirement test workers did not stop")
				return
			}
		}
	})
	seen := map[string]bool{}
	for range 2 {
		select {
		case path := <-entered:
			seen[path] = true
		case <-call.Done():
			t.Fatal("original source operations never reached fake transport", call.Err())
		}
	}
	if !seen[PathV1] || !seen[attachedworkerreceipt.PathV1] {
		t.Fatal("did not exercise BOTH original source channels", seen)
	}
	if err := factory.RetireConnectionSourcesV1(); err != nil {
		t.Fatal("nonblocking source retirement", err)
	}
	if port.closes != 0 || factory.port != port || factory.source != nil || factory.publisher != nil || factory.state != sourceClosed || factory.life.Err() == nil {
		t.Fatal("retirement lost deferred exchange ownership or retained source authority")
	}
	for _, value := range ownedBearer {
		if value != 0 {
			t.Fatal("original input bearer was not cleared")
		}
	}
	for joined < 2 {
		select {
		case err := <-done:
			joined++
			if err == nil {
				t.Error("cancelled original source returned usable result")
			}
		case <-call.Done():
			t.Fatal("retirement did not cancel BOTH source operations", call.Err())
		}
	}
	if _, err := originalSource.Load(t.Context(), input); err == nil {
		t.Fatal("retired input object still usable")
	}
	if _, err := originalPublisher.Publish(t.Context(), submission); err == nil {
		t.Fatal("retired receipt object still usable")
	}
	if err := factory.RetireConnectionSourcesV1(); err != nil || port.closes != 0 {
		t.Fatal("repeated retirement closed foreign exchange", err)
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("synthetic-new-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatal("retired factory reopened", err)
	}
	if err := factory.Close(); err != nil || port.closes != 1 {
		t.Fatal("outer cleanup lost original exchange", err)
	}
	if err := factory.Close(); err != nil || port.closes != 1 {
		t.Fatal("outer cleanup closed original exchange twice", err)
	}
	var absent *SessionSourceFactory
	if err := absent.RetireConnectionSourcesV1(); err != nil {
		t.Fatal("nil retirement not idempotent", err)
	}
}
