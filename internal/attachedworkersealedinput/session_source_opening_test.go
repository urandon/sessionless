package attachedworkersealedinput

import (
	"errors"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

type openingSourceDelegate struct {
	entered chan struct{}
	proceed chan struct{}
	port    *exchangePortFixture
}

func (v *openingSourceDelegate) Open(attachedworkersession.ConnectionBindingV1, []byte) (attachedworkersession.ExchangePort, error) {
	close(v.entered)
	<-v.proceed
	return v.port, nil
}

func TestSessionSourceOpeningClientsRetireBeforeForeignOpenReturns(t *testing.T) {
	synctest.Test(t, testSessionSourceOpeningClientsRetireBeforeForeignOpenReturns)
}

func testSessionSourceOpeningClientsRetireBeforeForeignOpenReturns(t *testing.T) {
	t.Helper()
	delegate := &openingSourceDelegate{entered: make(chan struct{}), proceed: make(chan struct{}), port: &exchangePortFixture{}}
	factory := newReceiptFactory(t, receiptRoundTrip(func(*http.Request) (*http.Response, error) {
		t.Error("opening/retired factory performed unexpected HTTP")
		return nil, errors.New("unexpected controlled HTTP")
	}), delegate)
	var once sync.Once
	release := func() { once.Do(func() { close(delegate.proceed) }) }
	done := make(chan error, 1)
	joined := false
	t.Cleanup(func() {
		release()
		if !joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("owned Open worker did not join")
			}
		}
		if err := factory.Close(); err != nil {
			t.Error(err)
		}
	})
	go func() {
		_, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("synthetic-opening-bearer"))
		done <- err
	}()
	select {
	case <-delegate.entered:
	case <-time.After(time.Second):
		t.Fatal("foreign Open never entered")
	}
	factory.mu.Lock()
	source, publisher := factory.source, factory.publisher
	opening := factory.state == sourceOpening
	factory.mu.Unlock()
	if !opening || source == nil || publisher == nil {
		t.Error("owned opening clients are not registered for immediate retirement")
	}
	if err := factory.RetireConnectionSourcesV1(); err != nil {
		t.Fatal(err)
	}
	if source != nil {
		source.mu.Lock()
		scrubbed := source.closed && len(source.bearer) == 0
		source.mu.Unlock()
		if !scrubbed {
			t.Error("opening input client retained its owned bearer before foreign return")
		}
	}
	if publisher != nil {
		if _, err := publisher.Publish(t.Context(), receiptSubmission(t)); err == nil {
			t.Error("opening V1 receipt client remained usable after retirement")
		}
	}
	if delegate.port.closes != 0 {
		t.Error("source retirement invoked foreign cleanup before Open returned")
	}
	release()
	select {
	case err := <-done:
		joined = true
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("late Open err=%v want unavailable", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late Open did not finish")
	}
	if delegate.port.closes != 1 || factory.state != sourceClosed || factory.source != nil || factory.publisher != nil {
		t.Fatal("late foreign Open resurrected clients or lost cleanup")
	}
	if _, err := factory.Open(attachedworkersession.ConnectionBindingV1{}, []byte("new-synthetic-bearer")); !errors.Is(err, ErrUnavailable) {
		t.Fatal("retired opening factory reopened", err)
	}
}
