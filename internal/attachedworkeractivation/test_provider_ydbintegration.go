//go:build ydbintegration

package attachedworkeractivation

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const joinedYDBOperationTimeout = 45 * time.Second

// ConnectTestSyntheticWithClock keeps the shipped 15-second transport budget
// unchanged while allowing the race-enabled, Docker-backed joined YDB fixture
// a bounded budget for two concurrent owners.
func ConnectTestSyntheticWithClock(ctx context.Context, store *attachedworkerlocal.Store,
	profile ProfileV1, now func() time.Time,
) (*attachedworkersealedinput.SyntheticRuntime, error) {
	return connectWithClock(ctx, store, profile, now, joinedYDBOperationTimeout,
		attachedworkersealedinput.ConnectSyntheticPinnedRuntime,
		attachedworkersealedinput.ReconnectSyntheticPinnedRuntime)
}

// ConnectTestProviderWithClock is visible only in the YDB integration test
// build. It does not authorize provider execution from an operator profile;
// the ordinary activation connector remains synthetic-denied.
func ConnectTestProviderWithClock(ctx context.Context, store *attachedworkerlocal.Store,
	profile ProfileV1, now func() time.Time, credentials ports.CredentialLifecycle,
	credentialRoot string,
	candidate attachedworkerdaemontransport.ReceiptCandidateBuilder,
) (*attachedworkersealedinput.SyntheticRuntime, error) {
	return connectWithClock(ctx, store, profile, now, joinedYDBOperationTimeout,
		func(ctx context.Context, config attachedworkersealedinput.SyntheticRuntimeConfig) (*attachedworkersealedinput.SyntheticRuntime, error) {
			config.Stack.AllowedEnvironmentNames = []string{"SESSIONLESS_PROVIDER_HOME"}
			config.Stack.AllowedReadRoots = []string{credentialRoot}
			return attachedworkersealedinput.ConnectTestProviderPinnedRuntime(ctx, config, credentials, candidate,
				profile.ControlPlaneOrigin+attachedworkerreceipt.PathV1)
		}, nil)
}

// ConnectTestPreparedCodexWithClock preserves the authentic source/client and
// normal pinned Stack composition. Only this integration binary can select
// the fixed fake executable; the Stack itself owns the genuine result witness
// and receipt candidate, so no caller result callback is installed.
func ConnectTestPreparedCodexWithClock(ctx context.Context, store *attachedworkerlocal.Store,
	profile ProfileV1, now func() time.Time, credentials ports.CredentialLifecycle,
	credentialRoot string, prepared codexexec.Config,
) (*attachedworkersealedinput.SyntheticRuntime, error) {
	return connectWithClock(ctx, store, profile, now, joinedYDBOperationTimeout,
		func(ctx context.Context, config attachedworkersealedinput.SyntheticRuntimeConfig) (*attachedworkersealedinput.SyntheticRuntime, error) {
			config.Stack.AllowedEnvironmentNames = []string{codexexec.CredentialHomeEnvironmentV1}
			config.Stack.AllowedReadRoots = []string{credentialRoot}
			prepared.Now = now
			config.Stack.PreparedCodex = &prepared
			return attachedworkersealedinput.ConnectTestProviderPinnedRuntime(ctx, config, credentials, nil,
				profile.ControlPlaneOrigin+attachedworkerreceipt.PathV1)
		}, nil)
}
