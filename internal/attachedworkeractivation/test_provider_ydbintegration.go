//go:build ydbintegration

package attachedworkeractivation

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// ConnectTestProviderWithClock is visible only in the YDB integration test
// build. It does not authorize provider execution from an operator profile;
// the ordinary activation connector remains synthetic-denied.
func ConnectTestProviderWithClock(ctx context.Context, store *attachedworkerlocal.Store,
	profile ProfileV1, now func() time.Time, credentials ports.CredentialLifecycle,
	credentialRoot string,
	candidate attachedworkerdaemontransport.ReceiptCandidateBuilder,
) (*attachedworkersealedinput.SyntheticRuntime, error) {
	return connectWithClock(ctx, store, profile, now,
		func(ctx context.Context, config attachedworkersealedinput.SyntheticRuntimeConfig) (*attachedworkersealedinput.SyntheticRuntime, error) {
			config.Stack.AllowedEnvironmentNames = []string{"SESSIONLESS_PROVIDER_HOME"}
			config.Stack.AllowedReadRoots = []string{credentialRoot}
			return attachedworkersealedinput.ConnectTestProviderPinnedRuntime(ctx, config, credentials, candidate,
				profile.ControlPlaneOrigin+attachedworkerreceipt.PathV1)
		}, nil)
}
