package attachedworkerdaemon

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedInvocationV1 is private, invocation-scoped input for a configured
// executor. The outer InvocationRunner has already validated, issued and
// materialized its exact credential. It retains WriteBack/Release ownership.
// Process is a cloned, credential-bound spec; its stdin is borrowed only until
// RunPrepared returns and must not be retained. This is not a wire DTO.
type PreparedInvocationV1 struct {
	Identity        InvocationIdentity                        `json:"-"`
	Process         AttemptSpec                               `json:"-"`
	Credential      ports.ProviderInvocationCredentialV1      `json:"-"`
	Materialization ports.ProviderCredentialMaterializationV1 `json:"-"`
}

func (PreparedInvocationV1) String() string {
	return "PreparedInvocationV1{scope:[redacted] process:[redacted] credential:[redacted] materialization:[redacted]}"
}

func (invocation PreparedInvocationV1) GoString() string { return invocation.String() }

// PreparedProcessRunner executes using the already prepared projection. It
// returns actual process observations only, never credential finalization
// flags. An executor must not wrap another InvocationRunner or repeat an
// invocation after an ambiguous result. Accepted-attempt authority, canonical
// input staging and semantic output collection remain its explicit contracts.
type PreparedProcessRunner interface {
	RunPrepared(context.Context, PreparedInvocationV1) (AttemptResult, error)
}

// NewPreparedInvocationRunner selects an explicit executor instead of the
// ordinary process runner, with exactly the same credential lifecycle owner.
// It rejects credentialless input; there is no fallback to the ordinary path.
// No shipped foreground/profile constructor uses this default-off library seam.
func NewPreparedInvocationRunner(config InvocationRunnerConfig, executor PreparedProcessRunner, credentials ports.CredentialLifecycle) (*InvocationRunner, error) {
	return newInvocationRunner(config, nil, executor, credentials)
}
