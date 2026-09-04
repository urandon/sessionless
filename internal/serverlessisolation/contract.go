// Package serverlessisolation composes the existing AW-05 process boundary
// with the Sessionless serverless authority chain. It is feature-disabled
// infrastructure: concrete provider/backend composition belongs to PR-03e.
package serverlessisolation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

var (
	ErrConfig             = errors.New("serverless isolation configuration is invalid")
	ErrAuthority          = errors.New("serverless isolation authority is invalid")
	ErrAttestation        = errors.New("serverless isolation attestation mismatch")
	ErrProcess            = errors.New("serverless isolation process failed")
	ErrOutputFinalization = errors.New("serverless output finalization failed")
	ErrCredentialFinalize = errors.New("serverless credential finalization failed")
	ErrCleanup            = errors.New("serverless isolation cleanup failed")
	ErrTaintNotConfirmed  = errors.New("serverless isolation taint was not confirmed")
)

// PreparedInvocationValidator is intentionally satisfied by the exact
// process-local CapabilityIssuer. A public-shaped authority/allocation tuple
// is insufficient to start a workload.
type PreparedInvocationValidator interface {
	Validate(serverlessharness.PreparedInvocation) error
}

// AttestedLauncher adds serverless allocation and residue evidence to the
// reviewed AW-05 launcher contract. Preflight must not create a workspace,
// read a credential, spawn a process, or start network activity.
type AttestedLauncher interface {
	attachedworkerdaemon.IsolationLauncher
	Preflight(context.Context, domain.ServerlessInvocationAuthorityV1) (domain.PreparedAllocationV1, error)
	VerifyResidue(context.Context, ResidueRequestV1) (ResidueProofV1, error)
	Taint(context.Context, TaintRequestV1) error
}

type RunSpecV1 struct {
	Prepared    serverlessharness.PreparedInvocation
	Executable  string
	Arguments   []string
	Environment []attachedworkerdaemon.EnvironmentVariable
	Stdin       []byte `json:"-"`
}

func (spec RunSpecV1) String() string {
	return fmt.Sprintf(
		"ServerlessRunSpec{executable:%s arguments:%d environment:%d stdin:[redacted:%d]}",
		spec.Executable, len(spec.Arguments), len(spec.Environment), len(spec.Stdin),
	)
}

func (spec RunSpecV1) GoString() string { return spec.String() }

type FinalizationRequestV1 struct {
	AuthorityDigest domain.ServerlessInvocationAuthorityDigestV1
	PreparedDigest  domain.PreparedInvocationDigestV1
	PhysicalClaimID string
	Process         attachedworkerdaemon.AttemptResult
}

// OutputFinalizer persists only bounded, allowlisted output candidates. It
// must be idempotent for the exact PreparedDigest and PhysicalClaimID.
type OutputFinalizer interface {
	FinalizeOutputs(context.Context, FinalizationRequestV1) (OutputFinalizationProofV1, error)
}

type OutputFinalizationProofV1 struct {
	Finalized        bool
	NativeEventCount uint64
	ArtifactBytes    uint64
	EvidenceBytes    uint64
}

// CredentialFinalizer observes write-back/release independently of provider
// and process completion. Credentialless profiles return not_required.
type CredentialFinalizer interface {
	FinalizeCredentials(context.Context, FinalizationRequestV1) (domain.CredentialFinalizationStateV1, error)
}

type ResidueRequestV1 struct {
	AuthorityDigest domain.ServerlessInvocationAuthorityDigestV1
	PreparedDigest  domain.PreparedInvocationDigestV1
	PhysicalClaimID string
}

type ResidueProofV1 struct {
	WorkspaceAbsent   bool
	ProcessesAbsent   bool
	SocketsAbsent     bool
	CredentialsAbsent bool
	VerifiedAt        time.Time
}

func (proof ResidueProofV1) Verified() bool {
	return proof.WorkspaceAbsent && proof.ProcessesAbsent && proof.SocketsAbsent &&
		proof.CredentialsAbsent && !proof.VerifiedAt.IsZero()
}

type StopProofV1 struct {
	DescendantsReaped bool
	BoundaryReleased  bool
	WorkspaceRemoved  bool
}

func (proof StopProofV1) Verified() bool {
	return proof.DescendantsReaped && proof.BoundaryReleased && proof.WorkspaceRemoved
}

type TaintReasonV1 string

const (
	TaintProcessStopUnknownV1       TaintReasonV1 = "process_stop_unknown"
	TaintCredentialFinalizeFailedV1 TaintReasonV1 = "credential_finalization_failed"
	TaintResidueUnknownV1           TaintReasonV1 = "residue_unknown"
)

type TaintRequestV1 struct {
	AuthorityDigest domain.ServerlessInvocationAuthorityDigestV1
	PreparedDigest  domain.PreparedInvocationDigestV1
	PhysicalClaimID string
	Reasons         []TaintReasonV1
}

type RunResultV1 struct {
	Process                attachedworkerdaemon.AttemptResult
	StopProof              StopProofV1
	Output                 OutputFinalizationProofV1
	CredentialFinalization domain.CredentialFinalizationStateV1
	Residue                ResidueProofV1
	Cleanup                domain.SubstrateCleanupStateV1
	TaintRequested         bool
	TaintConfirmed         bool
}

func (result RunResultV1) String() string {
	return fmt.Sprintf(
		"ServerlessRunResult{process:%v stop:%t output:%t credential:%s residue:%t cleanup:%s taint_requested:%t taint_confirmed:%t}",
		result.Process, result.StopProof.Verified(), result.Output.Finalized,
		result.CredentialFinalization, result.Residue.Verified(), result.Cleanup,
		result.TaintRequested, result.TaintConfirmed,
	)
}

func (result RunResultV1) GoString() string { return result.String() }
