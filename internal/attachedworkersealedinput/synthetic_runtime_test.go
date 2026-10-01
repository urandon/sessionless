package attachedworkersealedinput

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
)

type forbiddenBootstrap struct{}

func (forbiddenBootstrap) IssueChallenge(context.Context, attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error) {
	panic("synthetic test unexpectedly reached bootstrap")
}

func (forbiddenBootstrap) Activate(context.Context, attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error) {
	panic("synthetic test unexpectedly reached activation")
}

type syntheticRuntimeFixture struct {
	started      chan struct{}
	closeSession func() error
	leaseClosed  chan struct{}
	runs         int
	shutdown     int
}

func (fixture *syntheticRuntimeFixture) Run(ctx context.Context) error {
	fixture.runs++
	close(fixture.started)
	<-ctx.Done()
	if fixture.closeSession != nil {
		if err := fixture.closeSession(); err != nil {
			return errors.Join(ctx.Err(), err)
		}
		close(fixture.leaseClosed)
	}
	return ctx.Err()
}

func (fixture *syntheticRuntimeFixture) Drain(context.Context) error { return nil }
func (fixture *syntheticRuntimeFixture) Shutdown(context.Context) error {
	fixture.shutdown++
	return nil
}
func (fixture *syntheticRuntimeFixture) Wake() error { return nil }
func (fixture *syntheticRuntimeFixture) Status() attachedworkerdaemon.Status {
	return attachedworkerdaemon.Status{}
}

func syntheticConfig(t *testing.T, exchange attachedworkersession.ExchangeFactory) (SyntheticRuntimeConfig, attachedworkerdaemontransport.MaterializationRequestV1, func()) {
	t.Helper()
	service, _, _, _, request := fixtureInput(t)
	server := httptest.NewTLSServer(Handler(service))
	return SyntheticRuntimeConfig{
		Store: &attachedworkerlocal.Store{}, Bootstrap: forbiddenBootstrap{}, Exchange: exchange,
		SealedEndpoint: server.URL + PathV1, SealedClient: server.Client(),
		MaxInputBytes: 4096, Now: func() time.Time { return time.Unix(20, 0).UTC() },
	}, request, server.Close
}

func TestSyntheticOwnerClosesBoundSourceBeforeRun(t *testing.T) {
	exchange := &exchangeFixture{port: &exchangePortFixture{}}
	config, request, closeServer := syntheticConfig(t, exchange)
	defer closeServer()
	fixture := &syntheticRuntimeFixture{started: make(chan struct{}), leaseClosed: make(chan struct{})}
	var materializer *attachedworkerdaemontransport.BoundMaterializer
	owner, err := connectSyntheticPinnedRuntime(context.Background(), config,
		func(_ context.Context, source *SessionSourceFactory, input *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
			materializer = input
			sessionExchange, err := source.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer"))
			if err != nil {
				return nil, err
			}
			fixture.closeSession = sessionExchange.(interface{ Close() error }).Close
			return fixture, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fixture.runs != 1 || exchange.port.closes != 1 {
		t.Fatalf("closed-before-run calls=%d port closes=%d", fixture.runs, exchange.port.closes)
	}
	if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, attachedworkerdaemontransport.ErrMaterializationFailed) {
		t.Fatalf("retired source still materialized: %v", err)
	}
	if err := owner.Run(context.Background()); !errors.Is(err, attachedworkerdaemontransport.ErrRuntimeAlreadyUsed) {
		t.Fatalf("closed owner ran twice: %v", err)
	}
}

func TestSyntheticOwnerCancelsActiveRunAndClosesBoundSource(t *testing.T) {
	exchange := &exchangeFixture{port: &exchangePortFixture{}}
	config, request, closeServer := syntheticConfig(t, exchange)
	defer closeServer()
	fixture := &syntheticRuntimeFixture{started: make(chan struct{})}
	var materializer *attachedworkerdaemontransport.BoundMaterializer
	owner, err := connectSyntheticPinnedRuntime(context.Background(), config,
		func(_ context.Context, source *SessionSourceFactory, input *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
			materializer = input
			if _, err := source.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer")); err != nil {
				return nil, err
			}
			return fixture, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- owner.Run(context.Background()) }()
	select {
	case <-fixture.started:
	case <-time.After(time.Second):
		t.Fatal("synthetic runtime did not start")
	}
	if err := owner.Close(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("active close did not preserve cancellation: %v", err)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run result=%v", err)
	}
	if fixture.shutdown != 0 || exchange.port.closes != 1 {
		t.Fatalf("shutdown=%d port closes=%d", fixture.shutdown, exchange.port.closes)
	}
	if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, attachedworkerdaemontransport.ErrMaterializationFailed) {
		t.Fatalf("retired active source still materialized: %v", err)
	}
}

func TestSyntheticOwnerRetiresSourceBeforeStalledPreRunExchangeClose(t *testing.T) {
	port := &exchangePortFixture{closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
	exchange := &exchangeFixture{port: port}
	config, request, closeServer := syntheticConfig(t, exchange)
	defer closeServer()
	release := port.closeRelease
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	fixture := &syntheticRuntimeFixture{started: make(chan struct{}), leaseClosed: make(chan struct{})}
	var materializer *attachedworkerdaemontransport.BoundMaterializer
	owner, err := connectSyntheticPinnedRuntime(context.Background(), config,
		func(_ context.Context, source *SessionSourceFactory, input *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
			materializer = input
			sessionExchange, err := source.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer"))
			if err != nil {
				return nil, err
			}
			fixture.closeSession = sessionExchange.(interface{ Close() error }).Close
			return fixture, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan error, 1)
	go func() { closed <- owner.Close(ctx) }()
	select {
	case <-port.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("pre-run exchange close did not start")
	}
	select {
	case <-fixture.leaseClosed:
	default:
		t.Fatal("delegate close started before session lease was released")
	}
	if _, err := materializer.Materialize(context.Background(), request); !errors.Is(err, attachedworkerdaemontransport.ErrMaterializationFailed) {
		t.Fatalf("stalled exchange retained bearer: %v", err)
	}
	cancel()
	if err := <-closed; !errors.Is(err, context.Canceled) {
		t.Fatalf("bounded caller did not return: %v", err)
	}
	close(release)
	if err := owner.Close(context.Background()); err != nil {
		t.Fatalf("late finalizer did not complete: %v", err)
	}
	if fixture.runs != 1 || port.closes != 1 {
		t.Fatalf("runtime runs=%d exchange closes=%d", fixture.runs, port.closes)
	}
}

func TestSyntheticConstructionFailureClosesAcquiredExchange(t *testing.T) {
	exchange := &exchangeFixture{port: &exchangePortFixture{}}
	config, _, closeServer := syntheticConfig(t, exchange)
	defer closeServer()
	failure := errors.New("post-connect composition failed")
	var source *SessionSourceFactory
	owner, err := connectSyntheticPinnedRuntime(context.Background(), config,
		func(_ context.Context, bound *SessionSourceFactory, _ *attachedworkerdaemontransport.BoundMaterializer) (syntheticRuntimePort, error) {
			source = bound
			if _, err := bound.Open(attachedworkersession.ConnectionBindingV1{}, []byte("test-connection-bearer")); err != nil {
				return nil, err
			}
			return nil, failure
		})
	if owner != nil || !errors.Is(err, failure) || exchange.port.closes != 1 {
		t.Fatalf("failed constructor owner=%v err=%v closes=%d", owner, err, exchange.port.closes)
	}
	if _, err := source.Load(context.Background(), attachedworkerdaemontransport.MaterializationRequestV1{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed constructor retained source: %v", err)
	}
}

func TestSyntheticPinnedConstructorRejectsInvalidPreflightBeforeAnyEffect(t *testing.T) {
	exchange := &exchangeFixture{port: &exchangePortFixture{}}
	config, _, closeServer := syntheticConfig(t, exchange)
	defer closeServer()
	// No capability/profile agreement exists. The real pinned constructor must
	// reject this before it loads local state, opens an exchange, or reads input.
	owner, err := ConnectSyntheticPinnedRuntime(context.Background(), config)
	if owner != nil || !errors.Is(err, attachedworkerdaemontransport.ErrInvalidAuthority) || exchange.calls != 0 {
		t.Fatalf("invalid preflight owner=%v err=%v exchange opens=%d", owner, err, exchange.calls)
	}
}
