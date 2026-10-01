package attachedworkerdaemontransport

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type pinnedReconnectPort struct {
	*fakeReconnectPort
	preflight     func(context.Context, attachedworkerlocal.ManifestV1) error
	manifest      attachedworkerlocal.ManifestV1
	networkCalls  int
	skipPreflight bool
}

func (port *pinnedReconnectPort) Reconnect(ctx context.Context, input attachedworkersession.ReconnectInputV1) (recoveredSession, error) {
	port.calls++
	if !port.skipPreflight {
		if err := port.preflight(ctx, port.manifest); err != nil {
			return nil, err
		}
	}
	port.networkCalls++
	return port.session, port.err
}

func pinnedReconnectFixture(t *testing.T) (*adapterFixture, *fakeRecoveredSession, *pinnedReconnectPort, attachedworkersession.ReconnectInputV1) {
	t.Helper()
	fixture, session, initial, input := pinnedInitialFixture(t)
	session.recovery = attachedworkersession.ReconnectRecoveryV1{
		ConnectionState:  attachedworkerprotocol.ConnectionReady,
		AttemptState:     attachedworkerprotocol.AttemptIdle,
		TerminalDecision: attachedworkerprotocol.ReconnectTerminalNone,
	}
	return fixture, session, &pinnedReconnectPort{
		fakeReconnectPort: &fakeReconnectPort{session: session},
		manifest:          initial.manifest,
	}, attachedworkersession.ReconnectInputV1(input)
}

func TestPinnedReconnectPreflightBeforeGenerationAndNetwork(t *testing.T) {
	for _, test := range []struct {
		name        string
		prepare     func(*adapterFixture, *pinnedReconnectPort, *attachedworkersession.ReconnectInputV1, *RuntimeConfig)
		stackErr    bool
		nilRunner   bool
		want        error
		wantNetwork int
		wantStack   int
		wantClose   int
	}{
		{name: "matching idle checkpoint", wantNetwork: 1, wantStack: 1, wantClose: 1},
		{name: "local executable changed", prepare: func(_ *adapterFixture, port *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			port.manifest.Harness.Executable = "/fixture/foreign-harness"
		}, want: ErrInvalidAuthority},
		{name: "local digest changed", prepare: func(_ *adapterFixture, port *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			port.manifest.Harness.SHA256 = hex.EncodeToString(make([]byte, 32))
		}, want: ErrInvalidAuthority},
		{name: "local argv changed", prepare: func(_ *adapterFixture, port *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			port.manifest.Harness.Arguments = []string{"--foreign"}
		}, want: ErrInvalidAuthority},
		{name: "stack reconciliation failed", stackErr: true, want: ErrInvalidConfiguration, wantStack: 1},
		{name: "stack returned no runner", nilRunner: true, want: ErrInvalidConfiguration, wantStack: 1},
		{name: "connector omitted preflight", prepare: func(_ *adapterFixture, port *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			port.skipPreflight = true
		}, want: ErrReconciliationRequired, wantNetwork: 1, wantClose: 1},
		{name: "environment outside allowlist", prepare: func(fixture *adapterFixture, _ *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			fixture.config.Profile.Environment[0].Name = "FOREIGN_MODE"
		}, want: ErrInvalidConfiguration},
		{name: "capability changed", prepare: func(_ *adapterFixture, _ *pinnedReconnectPort, input *attachedworkersession.ReconnectInputV1, _ *RuntimeConfig) {
			input.CapabilityManifest.BuildID = "foreign-build"
		}, want: ErrInvalidAuthority},
		{name: "runtime bound invalid", prepare: func(_ *adapterFixture, _ *pinnedReconnectPort, _ *attachedworkersession.ReconnectInputV1, config *RuntimeConfig) {
			config.CleanupTimeout = time.Minute + time.Second
		}, want: ErrInvalidConfiguration},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port, input := pinnedReconnectFixture(t)
			config := initialRuntimeConfig()
			if test.prepare != nil {
				test.prepare(fixture, port, &input, &config)
			}
			stackCalls := 0
			connectorCalls := 0
			process := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
			runtime, err := reconnectPinnedForegroundRuntime(context.Background(), nil, nil, nil,
				attachedworkersession.Config{}, input, fixture.materializer, fixture.config, cadenceConfig(true),
				attachedworkerstack.Config{AllowedEnvironmentNames: []string{"SESSIONLESS_MODE"}}, unusedInitialCredentials{}, config,
				func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, config attachedworkersession.Config) (reconnectPort, error) {
					connectorCalls++
					port.preflight = config.RuntimePreflight
					return port, nil
				},
				func(_ context.Context, _ attachedworkerlocal.ManifestV1, _ attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					stackCalls++
					if test.stackErr {
						return nil, ErrInvalidConfiguration
					}
					if test.nilRunner {
						return nil, nil
					}
					return process, nil
				})
			if test.want == nil {
				if err != nil || runtime == nil {
					t.Fatalf("runtime=%t error=%v", runtime != nil, err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := runtime.Run(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled run error=%v", err)
				}
			} else if runtime != nil || !errors.Is(err, test.want) {
				t.Fatalf("runtime=%t error=%v want=%v", runtime != nil, err, test.want)
			}
			if port.networkCalls != test.wantNetwork || stackCalls != test.wantStack || session.closeCalls != test.wantClose ||
				process.calls != 0 || len(session.actions) != 0 {
				t.Fatalf("network=%d stack=%d close=%d process=%d actions=%d", port.networkCalls, stackCalls, session.closeCalls, process.calls, len(session.actions))
			}
			if (test.name == "capability changed" || test.name == "environment outside allowlist" || test.name == "runtime bound invalid") && connectorCalls != 0 {
				t.Fatalf("connector constructed for invalid local configuration: %d", connectorCalls)
			}
		})
	}
}

func TestPinnedReconnectFencesNonIdleRecoveryWithoutProcess(t *testing.T) {
	for _, state := range []attachedworkerprotocol.AttemptState{attachedworkerprotocol.AttemptClaimed, attachedworkerprotocol.AttemptCancelRequested} {
		t.Run(string(state), func(t *testing.T) {
			fixture, session, port, input := pinnedReconnectFixture(t)
			session.recovery.AttemptState = state
			process := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
			runtime, err := reconnectPinnedForegroundRuntime(context.Background(), nil, nil, nil,
				attachedworkersession.Config{}, input, fixture.materializer, fixture.config, cadenceConfig(true),
				attachedworkerstack.Config{AllowedEnvironmentNames: []string{"SESSIONLESS_MODE"}}, unusedInitialCredentials{}, initialRuntimeConfig(),
				func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, config attachedworkersession.Config) (reconnectPort, error) {
					port.preflight = config.RuntimePreflight
					return port, nil
				},
				func(_ context.Context, _ attachedworkerlocal.ManifestV1, _ attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					return process, nil
				})
			if runtime != nil || !errors.Is(err, ErrReconciliationRequired) || port.networkCalls != 1 ||
				session.closeCalls != 1 || process.calls != 0 || len(session.actions) != 0 {
				t.Fatalf("state=%s runtime=%t error=%v network=%d close=%d process=%d actions=%d",
					state, runtime != nil, err, port.networkCalls, session.closeCalls, process.calls, len(session.actions))
			}
		})
	}
}
