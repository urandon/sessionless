package codexexec

import (
	"context"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// AuthorityResolverV1 projects an already accepted attached-worker session,
// lease, and resource authority. The driver validates this projection against
// the canonical execution identity instead of manufacturing missing fields.
type AuthorityResolverV1 interface {
	ResolveExecution(context.Context, ports.ExecutionIdentity) (AuthorityV1, error)
	ResolveCancellation(context.Context, ports.ExecutionIdentity) (AuthorityV1, error)
}

// ProcessInvocationV1 carries no credential bytes. Its materialization is the
// invocation-private location already prepared by canonical orchestration.
type ProcessInvocationV1 struct {
	Authority                      AuthorityV1                               `json:"-"`
	Identity                       ports.ExecutionIdentity                   `json:"-"`
	Credential                     ports.ProviderInvocationCredentialV1      `json:"-"`
	CredentialMaterialization      ports.ProviderCredentialMaterializationV1 `json:"-"`
	WorkDir                        string                                    `json:"-"`
	Executable                     string
	ExecutableDigest               attachedworkerdaemon.ExecutableDigest
	Arguments                      []string
	CredentialHomeEnvironmentName  string
	RequirePrivateWorkingDirectory bool
	RequireSanitizedEnvironment    bool
	RequireNoAmbientHome           bool
	RequireNoAmbientAPIKey         bool
	RequireProviderEffectFence     bool
	MaxProviderEffects             uint64
	MaxStdoutBytes                 int
	MaxStderrBytes                 int
	Stdin                          []byte `json:"-"`
}

func (invocation ProcessInvocationV1) String() string {
	return fmt.Sprintf(
		"CodexExecProcessInvocation{tenant:%s owner:%s worker:%s run:%s attempt:%s executable:[redacted] args:%d stdin:[redacted:%d] credential:[redacted]}",
		invocation.Identity.TenantID, invocation.Identity.OwnerUserID,
		invocation.Authority.WorkerID, invocation.Identity.RunID,
		invocation.Identity.AttemptID, len(invocation.Arguments), len(invocation.Stdin),
	)
}

func (invocation ProcessInvocationV1) GoString() string { return invocation.String() }

type ProcessResultV1 struct {
	ExitCode                     int
	StdoutBytes                  int
	StderrBytes                  int
	OutputLimitExceeded          bool
	Cancelled                    bool
	Deadline                     bool
	ProcessStopped               bool
	DescendantsStopped           bool
	CleanupSucceeded             bool
	PrivateStateRemoved          bool
	CredentialStateQuiesced      bool
	ProviderEffectFenceSatisfied bool
	ProviderEffects              uint64
	FailureCode                  string
	Stdout                       []byte `json:"-"`
}

func (result ProcessResultV1) String() string {
	return fmt.Sprintf(
		"CodexExecProcessResult{exit:%d stdout_bytes:%d stderr_bytes:%d output_limit:%t cancelled:%t deadline:%t stopped:%t descendants_stopped:%t cleanup:%t private_state_removed:%t credential_quiesced:%t effect_fence:%t provider_effects:%d failure_present:%t stdout:[redacted:%d]}",
		result.ExitCode, result.StdoutBytes, result.StderrBytes, result.OutputLimitExceeded,
		result.Cancelled, result.Deadline, result.ProcessStopped, result.DescendantsStopped,
		result.CleanupSucceeded, result.PrivateStateRemoved, result.CredentialStateQuiesced,
		result.ProviderEffectFenceSatisfied, result.ProviderEffects, result.FailureCode != "", len(result.Stdout),
	)
}

func (result ProcessResultV1) GoString() string { return result.String() }

type ProcessBoundaryV1 interface {
	Run(context.Context, ProcessInvocationV1) (ProcessResultV1, error)
	Cancel(context.Context, AuthorityV1) error
}
