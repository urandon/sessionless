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
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type fakeInitialConnectPort struct {
	session connectedSession
	err     error
	calls   int
	after   func()
}

type pinnedInitialConnectPort struct {
	*fakeInitialConnectPort
	preflight    func(context.Context, attachedworkerlocal.ManifestV1) error
	manifest     attachedworkerlocal.ManifestV1
	networkCalls int
}

func (port *pinnedInitialConnectPort) Connect(ctx context.Context, input attachedworkersession.ConnectInputV1) (connectedSession, error) {
	port.calls++
	if err := port.preflight(ctx, port.manifest); err != nil {
		return nil, err
	}
	port.networkCalls++
	return port.session, port.err
}

type unusedInitialCredentials struct{}

func (unusedInitialCredentials) Issue(context.Context, ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	return ports.CredentialHandle{}, ErrInvalidConfiguration
}
func (unusedInitialCredentials) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	return ports.CredentialMaterialization{}, ErrInvalidConfiguration
}
func (unusedInitialCredentials) WriteBack(context.Context, ports.CredentialHandle, ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	return ports.CredentialWriteBackResult{}, ErrInvalidConfiguration
}
func (unusedInitialCredentials) Release(context.Context, ports.CredentialHandle) error { return nil }
func (unusedInitialCredentials) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return nil
}

func pinnedInitialFixture(t *testing.T) (*adapterFixture, *fakeRecoveredSession, *pinnedInitialConnectPort, attachedworkersession.ConnectInputV1) {
	t.Helper()
	fixture, session, port, _ := initialRuntimeFixture(t)
	profile := fixture.config.Profile
	manifest := attachedworkerlocal.ManifestV1{Harness: attachedworkerlocal.HarnessConfigV1{
		Executable: profile.Executable, SHA256: hex.EncodeToString(profile.ExecutableDigest[:]),
		Arguments: append([]string(nil), profile.Arguments...),
	}}
	capability := attachedworkerprotocol.CapabilityManifestV1{
		WorkerID: "worker-001", EnrollmentGeneration: 2, Revision: 1,
		ProtocolOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: attachedworkerprotocol.ProtocolVersionV1, Maximum: attachedworkerprotocol.ProtocolVersionV1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		OperatingSystem: "linux", Architecture: "amd64", BuildID: "test-build",
		HarnessName: "fixture", HarnessVersion: "v1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn,
		HarnessExecutableDigest: append([]byte(nil), profile.ExecutableDigest[:]...),
		IsolationEvidence:       []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary},
		Features:                []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation},
		MaxConcurrentAttempts:   1,
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	fixture.config.Profile.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest))
	session.snapshot.CapabilityDigest = fixture.config.Profile.CapabilityDigest
	return fixture, session, &pinnedInitialConnectPort{fakeInitialConnectPort: port, manifest: manifest},
		attachedworkersession.ConnectInputV1{CapabilityManifest: capability}
}

func (port *fakeInitialConnectPort) Connect(context.Context, attachedworkersession.ConnectInputV1) (connectedSession, error) {
	port.calls++
	if port.after != nil {
		port.after()
	}
	return port.session, port.err
}

func initialRuntimeFixture(t *testing.T) (*adapterFixture, *fakeRecoveredSession, *fakeInitialConnectPort, *runtimeRunner) {
	t.Helper()
	fixture := newAdapterFixture(t)
	session := &fakeRecoveredSession{fakeSession: fixture.session}
	port := &fakeInitialConnectPort{session: session}
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	return fixture, session, port, runner
}

func initialRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Daemon:                attachedworkerdaemon.DaemonConfig{IdleBackoff: time.Second},
		ActiveControlInterval: time.Second, CleanupTimeout: time.Second,
	}
}

func TestPinnedInitialForegroundPreflightBeforeNetworkAndProcess(t *testing.T) {
	for _, test := range []struct {
		name         string
		prepare      func(*adapterFixture, *pinnedInitialConnectPort, *attachedworkersession.ConnectInputV1)
		stackFailure error
		nilRunner    bool
		want         error
		wantNetwork  int
		wantStack    int
		wantClose    int
	}{
		{name: "matching pinned manifest", wantNetwork: 1, wantStack: 1, wantClose: 1},
		{name: "swapped local executable", prepare: func(_ *adapterFixture, port *pinnedInitialConnectPort, _ *attachedworkersession.ConnectInputV1) {
			port.manifest.Harness.Executable = "/fixture/foreign-harness"
		}, want: ErrInvalidAuthority},
		{name: "swapped local digest", prepare: func(_ *adapterFixture, port *pinnedInitialConnectPort, _ *attachedworkersession.ConnectInputV1) {
			port.manifest.Harness.SHA256 = hex.EncodeToString(make([]byte, 32))
		}, want: ErrInvalidAuthority},
		{name: "swapped argv", prepare: func(_ *adapterFixture, port *pinnedInitialConnectPort, _ *attachedworkersession.ConnectInputV1) {
			port.manifest.Harness.Arguments = []string{"--foreign"}
		}, want: ErrInvalidAuthority},
		{name: "stack reconciliation fails", stackFailure: errors.New("private residue path"), want: ErrInvalidConfiguration, wantStack: 1},
		{name: "stack returns nil runner", nilRunner: true, want: ErrInvalidConfiguration, wantStack: 1},
		{name: "profile environment outside stack allowlist", prepare: func(fixture *adapterFixture, _ *pinnedInitialConnectPort, _ *attachedworkersession.ConnectInputV1) {
			fixture.config.Profile.Environment[0].Name = "FOREIGN_MODE"
		}, want: ErrInvalidConfiguration},
		{name: "capability changed before construction", prepare: func(_ *adapterFixture, _ *pinnedInitialConnectPort, input *attachedworkersession.ConnectInputV1) {
			input.CapabilityManifest.BuildID = "foreign-build"
		}, want: ErrInvalidAuthority},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port, input := pinnedInitialFixture(t)
			if test.prepare != nil {
				test.prepare(fixture, port, &input)
			}
			stackCalls := 0
			connectorCalls := 0
			process := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
			runtime, err := connectPinnedForegroundRuntime(context.Background(), nil, nil, nil,
				attachedworkersession.Config{}, input, fixture.materializer, fixture.config, cadenceConfig(true),
				attachedworkerstack.Config{AllowedEnvironmentNames: []string{"SESSIONLESS_MODE"}}, unusedInitialCredentials{}, initialRuntimeConfig(),
				func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, config attachedworkersession.Config) (initialConnectPort, error) {
					connectorCalls++
					port.preflight = config.RuntimePreflight
					return port, nil
				},
				func(_ context.Context, _ attachedworkerlocal.ManifestV1, _ attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
					stackCalls++
					if test.stackFailure != nil {
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
			if (test.name == "capability changed before construction" || test.name == "profile environment outside stack allowlist") && connectorCalls != 0 {
				t.Fatalf("connector construction crossed invalid local configuration: %d", connectorCalls)
			}
		})
	}
}

func TestPinnedInitialForegroundAcceptsZeroArgumentHarness(t *testing.T) {
	fixture, session, port, input := pinnedInitialFixture(t)
	fixture.config.Profile.Arguments = nil
	port.manifest.Harness.Arguments = nil
	runner := &runtimeRunner{started: make(chan struct{}, 1), release: make(chan struct{})}
	runtime, err := connectPinnedForegroundRuntime(context.Background(), nil, nil, nil,
		attachedworkersession.Config{}, input, fixture.materializer, fixture.config, cadenceConfig(true),
		attachedworkerstack.Config{AllowedEnvironmentNames: []string{"SESSIONLESS_MODE"}}, unusedInitialCredentials{}, initialRuntimeConfig(),
		func(_ *attachedworkerlocal.Store, _ attachedworkersession.BootstrapPort, _ attachedworkersession.ExchangeFactory, config attachedworkersession.Config) (initialConnectPort, error) {
			port.preflight = config.RuntimePreflight
			return port, nil
		},
		func(_ context.Context, manifest attachedworkerlocal.ManifestV1, _ attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			if len(manifest.Harness.Arguments) != 0 {
				t.Fatalf("stack received %d arguments", len(manifest.Harness.Arguments))
			}
			return runner, nil
		})
	if err != nil || runtime == nil || port.networkCalls != 1 {
		t.Fatalf("runtime=%t error=%v network=%d", runtime != nil, err, port.networkCalls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Run(ctx); !errors.Is(err, context.Canceled) || session.closeCalls != 1 || runner.calls != 0 {
		t.Fatalf("run error=%v close=%d process=%d", err, session.closeCalls, runner.calls)
	}
}

func TestInitialForegroundRuntimePreflightBeforeConnection(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*adapterFixture, *attachedworkertransport.Config, *RuntimeConfig)
		want    error
	}{
		{name: "polling disabled", prepare: func(_ *adapterFixture, poll *attachedworkertransport.Config, _ *RuntimeConfig) {
			poll.Enabled = false
		}, want: attachedworkertransport.ErrPollingDisabled},
		{name: "invalid adapter profile", prepare: func(fixture *adapterFixture, _ *attachedworkertransport.Config, _ *RuntimeConfig) {
			fixture.config.Profile.Name = ""
		}, want: ErrInvalidConfiguration},
		{name: "invalid poll clock", prepare: func(_ *adapterFixture, poll *attachedworkertransport.Config, _ *RuntimeConfig) {
			poll.Now = func() time.Time { return time.Time{} }
		}, want: attachedworkertransport.ErrInvalidConfig},
		{name: "invalid runtime cleanup bound", prepare: func(_ *adapterFixture, _ *attachedworkertransport.Config, runtime *RuntimeConfig) {
			runtime.CleanupTimeout = time.Minute + time.Second
		}, want: ErrInvalidConfiguration},
		{name: "invalid daemon report bound", prepare: func(_ *adapterFixture, _ *attachedworkertransport.Config, runtime *RuntimeConfig) {
			runtime.Daemon.ReportGrace = time.Minute + time.Second
		}, want: ErrInvalidConfiguration},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port, runner := initialRuntimeFixture(t)
			poll := cadenceConfig(true)
			config := initialRuntimeConfig()
			test.prepare(fixture, &poll, &config)
			runtime, err := connectForegroundRuntime(context.Background(), port, attachedworkersession.ConnectInputV1{},
				fixture.materializer, fixture.config, poll, runner, config)
			if runtime != nil || !errors.Is(err, test.want) || port.calls != 0 || session.closeCalls != 0 ||
				len(session.actions) != 0 || runner.calls != 0 {
				t.Fatalf("runtime=%t error=%v connect=%d close=%d actions=%d runs=%d",
					runtime != nil, err, port.calls, session.closeCalls, len(session.actions), runner.calls)
			}
		})
	}
}

func TestInitialForegroundRuntimeClosesSessionAfterPostConnectFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*fakeRecoveredSession, *fakeInitialConnectPort, context.CancelFunc)
		want    error
	}{
		{name: "fenced session", prepare: func(session *fakeRecoveredSession, _ *fakeInitialConnectPort, _ context.CancelFunc) {
			session.snapshot.State = attachedworkersession.StateFenced
		}, want: ErrReconciliationRequired},
		{name: "connect returns session and error", prepare: func(_ *fakeRecoveredSession, port *fakeInitialConnectPort, _ context.CancelFunc) {
			port.err = errors.New("ambiguous connect")
		}, want: ErrReconciliationRequired},
		{name: "cancelled during connect", prepare: func(_ *fakeRecoveredSession, port *fakeInitialConnectPort, cancel context.CancelFunc) {
			port.after = cancel
		}, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port, runner := initialRuntimeFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			test.prepare(session, port, cancel)
			runtime, err := connectForegroundRuntime(ctx, port, attachedworkersession.ConnectInputV1{},
				fixture.materializer, fixture.config, cadenceConfig(true), runner, initialRuntimeConfig())
			if runtime != nil || !errors.Is(err, test.want) || port.calls != 1 || session.closeCalls != 1 ||
				len(session.actions) != 0 || runner.calls != 0 {
				t.Fatalf("runtime=%t error=%v connect=%d close=%d actions=%d runs=%d",
					runtime != nil, err, port.calls, session.closeCalls, len(session.actions), runner.calls)
			}
		})
	}
}

func TestInitialForegroundRuntimeUsesOneOwnerAndPostManifestCooldown(t *testing.T) {
	fixture, session, port, runner := initialRuntimeFixture(t)
	poll := cadenceConfig(true)
	poll.StartWithCooldown = false
	clockReads := make(chan struct{}, 8)
	poll.Now = func() time.Time {
		clockReads <- struct{}{}
		return adapterTestTime
	}
	runtime, err := connectForegroundRuntime(context.Background(), port, attachedworkersession.ConnectInputV1{},
		fixture.materializer, fixture.config, poll, runner, initialRuntimeConfig())
	if err != nil || runtime == nil || port.calls != 1 || session.closeCalls != 0 {
		t.Fatalf("runtime=%t error=%v connect=%d close=%d", runtime != nil, err, port.calls, session.closeCalls)
	}
	for len(clockReads) > 0 {
		<-clockReads
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-clockReads: // The daemon reached the one cadence owner.
	case <-time.After(time.Second):
		cancel()
		t.Fatal("daemon never reached the post-Manifest cadence gate")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("daemon did not stop on cancellation")
	}
	if len(session.actions) != 0 || runner.calls != 0 || session.closeCalls != 1 {
		t.Fatalf("outbound actions=%d process runs=%d session closes=%d", len(session.actions), runner.calls, session.closeCalls)
	}
	if err := runtime.Run(context.Background()); !errors.Is(err, ErrRuntimeAlreadyUsed) {
		t.Fatalf("second run error=%v", err)
	}
}

func TestInitialForegroundRuntimeClosesLeaseWhenRunCannotStart(t *testing.T) {
	for _, test := range []struct {
		name    string
		context func() context.Context
		want    error
	}{
		{name: "pre-cancelled", context: func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, want: context.Canceled},
		{name: "nil context", context: func() context.Context { return nil }, want: ErrInvalidConfiguration},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture, session, port, runner := initialRuntimeFixture(t)
			runtime, err := connectForegroundRuntime(context.Background(), port, attachedworkersession.ConnectInputV1{},
				fixture.materializer, fixture.config, cadenceConfig(true), runner, initialRuntimeConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Run(test.context()); !errors.Is(err, test.want) {
				t.Fatalf("run error=%v, want %v", err, test.want)
			}
			if port.calls != 1 || session.closeCalls != 1 || len(session.actions) != 0 || runner.calls != 0 {
				t.Fatalf("connect=%d close=%d actions=%d runs=%d", port.calls, session.closeCalls, len(session.actions), runner.calls)
			}
			if err := runtime.Run(context.Background()); !errors.Is(err, ErrRuntimeAlreadyUsed) {
				t.Fatalf("second run error=%v", err)
			}
		})
	}
}
