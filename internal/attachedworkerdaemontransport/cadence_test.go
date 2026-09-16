package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

type cadenceSourceFunc func(context.Context) (attachedworkerdaemon.Invocation, bool, error)

func (source cadenceSourceFunc) Next(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	return source(ctx)
}

func cadenceConfig(enabled bool) attachedworkertransport.Config {
	return attachedworkertransport.Config{
		Enabled: enabled, PollInterval: attachedworkertransport.MinimumHeartbeatInterval,
		InitialBackoff: time.Second, MaxBackoff: time.Minute,
	}
}

func TestCadencedSourceFeatureGatePreventsDaemonNetworkExchange(t *testing.T) {
	calls := 0
	source, err := NewCadencedSource(cadenceSourceFunc(func(context.Context) (attachedworkerdaemon.Invocation, bool, error) {
		calls++
		return attachedworkerdaemon.Invocation{}, false, nil
	}), cadenceConfig(false))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Next(context.Background()); !errors.Is(err, attachedworkertransport.ErrPollingDisabled) || calls != 0 {
		t.Fatalf("disabled Next err=%v calls=%d", err, calls)
	}
	if err := source.Wake(); !errors.Is(err, attachedworkertransport.ErrPollingDisabled) {
		t.Fatalf("disabled Wake err=%v", err)
	}
}

func TestCadencedSourceMapsOnePollToOneDaemonResult(t *testing.T) {
	calls := 0
	source, err := NewCadencedSource(cadenceSourceFunc(func(context.Context) (attachedworkerdaemon.Invocation, bool, error) {
		calls++
		return attachedworkerdaemon.Invocation{}, false, nil
	}), cadenceConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	invocation, available, err := source.Next(context.Background())
	if err != nil || available || invocation.Process.Executable != "" || invocation.Credential != nil || calls != 1 {
		t.Fatalf("first Next invocation=%+v available=%t err=%v calls=%d", invocation, available, err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := source.Next(ctx); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled cadence Next err=%v calls=%d", err, calls)
	}
}

func TestCadencedSourceStopsAmbiguousSessionErrorWithoutRetry(t *testing.T) {
	unsafe := errors.New("unavailable after send")
	calls := 0
	source, err := NewCadencedSource(cadenceSourceFunc(func(context.Context) (attachedworkerdaemon.Invocation, bool, error) {
		calls++
		return attachedworkerdaemon.Invocation{}, false, unsafe
	}), cadenceConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := source.Next(context.Background()); err != unsafe || calls != 1 {
		t.Fatalf("ambiguous Next err=%v calls=%d", err, calls)
	}
	if _, _, err := source.Next(context.Background()); !errors.Is(err, attachedworkertransport.ErrReconciliationRequired) || calls != 1 {
		t.Fatalf("ambiguous restart Next err=%v calls=%d", err, calls)
	}
}

func TestCadencedSourceRejectsMissingDelegate(t *testing.T) {
	if _, err := NewCadencedSource(nil, cadenceConfig(true)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing delegate err=%v", err)
	}
}

func TestCadencedSourceSerializesResultsAndCancelsWaitingCaller(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	source, err := NewCadencedSource(cadenceSourceFunc(func(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
		if calls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return attachedworkerdaemon.Invocation{}, false, ctx.Err()
			}
		}
		return attachedworkerdaemon.Invocation{}, false, nil
	}), cadenceConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, _, runErr := source.Next(context.Background()); first <- runErr }()
	<-entered
	secondCtx, cancel := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { _, _, runErr := source.Next(secondCtx); second <- runErr }()
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Fatalf("waiting caller err=%v calls=%d", err, calls.Load())
	}
	close(release)
	if err := <-first; err != nil || calls.Load() != 1 {
		t.Fatalf("first caller err=%v calls=%d", err, calls.Load())
	}
}

func TestCadencedSourceMovesInvocationWithoutRetainingSensitiveInput(t *testing.T) {
	stdin := []byte("bounded synthetic input")
	credential := &attachedworkerdaemon.CredentialInvocation{}
	source, err := NewCadencedSource(cadenceSourceFunc(func(context.Context) (attachedworkerdaemon.Invocation, bool, error) {
		return attachedworkerdaemon.Invocation{
			Process: attachedworkerdaemon.AttemptSpec{Stdin: stdin}, Credential: credential,
		}, true, nil
	}), cadenceConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	invocation, available, err := source.Next(context.Background())
	if err != nil || !available || !bytes.Equal(invocation.Process.Stdin, stdin) || invocation.Credential != credential {
		t.Fatalf("moved result available=%t err=%v input_size=%d credential=%t", available, err, len(invocation.Process.Stdin), invocation.Credential != nil)
	}
	if source.cycle.available || source.cycle.invocation.Process.Stdin != nil || source.cycle.invocation.Credential != nil {
		t.Fatal("cadence source retained invocation after transferring it to daemon")
	}
}
