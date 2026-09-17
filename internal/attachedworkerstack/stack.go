// Package attachedworkerstack assembles the manifest-pinned local execution
// stack for an already enrolled attached worker. It performs no enrollment,
// network exchange, provider call, or product activation.
package attachedworkerstack

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const evidenceVersionV1 = uint32(1)

var (
	ErrInvalidConfiguration   = errors.New("attached worker execution stack configuration is invalid")
	ErrArtifactMismatch       = errors.New("attached worker execution stack artifact digest mismatches manifest")
	ErrIsolationUnsupported   = errors.New("attached worker execution stack isolation is unsupported")
	ErrReconciliationRequired = errors.New("attached worker execution stack residue requires reconciliation")
	ErrInvocationMismatch     = errors.New("attached worker execution stack invocation mismatches manifest")
)

// Config contains only explicit operator-owned local bounds. No value is
// discovered from the process environment, HOME, PATH, or Docker contexts.
type Config struct {
	ScratchRoot             string
	Timeout                 time.Duration
	TerminationGrace        time.Duration
	MaxStdoutBytes          int
	MaxStderrBytes          int
	AllowedEnvironmentNames []string
	AllowedReadRoots        []string
	CredentialFinalizeGrace time.Duration
}

// EvidenceV1 is redacted startup evidence. It intentionally contains no
// executable path, Docker endpoint, engine identifier, argv, or host path.
type EvidenceV1 struct {
	Version           uint32                              `json:"version"`
	ManifestRevision  uint64                              `json:"manifest_revision"`
	Boundary          attachedworkerlocal.RuntimeBoundary `json:"boundary"`
	IsolationProfile  string                              `json:"isolation_profile"`
	ReconciledResidue int                                 `json:"reconciled_residue"`
}

func (evidence EvidenceV1) String() string {
	return fmt.Sprintf(
		"EvidenceV1{manifest_revision:%d boundary:%s isolation_profile:%s reconciled_residue:%d}",
		evidence.ManifestRevision, evidence.Boundary, evidence.IsolationProfile, evidence.ReconciledResidue,
	)
}

func (evidence EvidenceV1) GoString() string { return evidence.String() }

// Stack is the complete local process and credential runner. Its pinning
// wrapper rejects any invocation whose executable, digest, or argv differs
// from the exact installed manifest.
type Stack struct {
	runner   attachedworkerdaemon.Runner
	evidence EvidenceV1
}

func (stack *Stack) Run(ctx context.Context, invocation attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	if stack == nil || stack.runner == nil {
		return attachedworkerdaemon.InvocationResult{}, ErrInvalidConfiguration
	}
	return stack.runner.Run(ctx, invocation)
}

func (stack *Stack) Evidence() EvidenceV1 {
	if stack == nil {
		return EvidenceV1{}
	}
	return stack.evidence
}

type reconcilingLauncher interface {
	attachedworkerdaemon.IsolationLauncher
	Reconcile(context.Context) (int, error)
}

type dependencies struct {
	hostOS        string
	digest        func(string) (attachedworkerdaemon.ExecutableDigest, error)
	newLauncher   func(context.Context, attachedworkeroci.Config) (reconcilingLauncher, error)
	newSupervisor func(attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error)
	newRunner     func(attachedworkerdaemon.InvocationRunnerConfig, attachedworkerdaemon.ProcessRunner, ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error)
}

// New verifies the complete local authority before returning process
// authority. The shipped attached-worker command deliberately does not call it.
func New(
	ctx context.Context,
	manifest attachedworkerlocal.ManifestV1,
	config Config,
	credentials ports.CredentialLifecycle,
) (*Stack, error) {
	return newStack(ctx, manifest, config, credentials, defaultDependencies())
}

func newStack(
	ctx context.Context,
	manifest attachedworkerlocal.ManifestV1,
	config Config,
	credentials ports.CredentialLifecycle,
	deps dependencies,
) (*Stack, error) {
	manifest.Harness.Arguments = append([]string(nil), manifest.Harness.Arguments...)
	if ctx == nil || credentials == nil || manifest.Validate() != nil ||
		manifest.Lifecycle != attachedworkerlocal.LifecycleActive || deps.digest == nil ||
		deps.newLauncher == nil || deps.newSupervisor == nil || deps.newRunner == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !hostSupportsBoundary(deps.hostOS, manifest.OCI.Boundary) {
		return nil, ErrIsolationUnsupported
	}
	root, err := privateCanonicalDirectory(config.ScratchRoot)
	if err != nil || root != config.ScratchRoot {
		return nil, ErrInvalidConfiguration
	}
	dockerDigest, err := strictExecutableDigest(manifest.OCI.DockerPath, deps.digest)
	if err != nil || !digestMatches(dockerDigest, manifest.OCI.DockerSHA256) {
		return nil, ErrArtifactMismatch
	}
	harnessDigest, err := strictExecutableDigest(manifest.Harness.Executable, deps.digest)
	if err != nil || !digestMatches(harnessDigest, manifest.Harness.SHA256) {
		return nil, ErrArtifactMismatch
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	launcher, err := deps.newLauncher(ctx, launcherConfig(manifest.OCI))
	if err != nil {
		if errors.Is(err, attachedworkeroci.ErrUnsupported) {
			return nil, ErrIsolationUnsupported
		}
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	if launcher == nil || launcher.Profile().Validate() != nil {
		return nil, ErrIsolationUnsupported
	}
	residue, err := launcher.Reconcile(ctx)
	if err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	if residue < 0 {
		return nil, ErrReconciliationRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	supervisor, err := deps.newSupervisor(attachedworkerdaemon.SupervisorConfig{
		ScratchRoot: root, Launcher: launcher, Timeout: config.Timeout,
		TerminationGrace: config.TerminationGrace, MaxStdoutBytes: config.MaxStdoutBytes,
		MaxStderrBytes:          config.MaxStderrBytes,
		AllowedEnvironmentNames: append([]string(nil), config.AllowedEnvironmentNames...),
		AllowedReadRoots:        append([]string(nil), config.AllowedReadRoots...),
	})
	if err != nil {
		if errors.Is(err, attachedworkerdaemon.ErrIsolationUnsupported) {
			return nil, ErrIsolationUnsupported
		}
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	if supervisor == nil {
		return nil, ErrInvalidConfiguration
	}
	runner, err := deps.newRunner(
		attachedworkerdaemon.InvocationRunnerConfig{CredentialFinalizeGrace: config.CredentialFinalizeGrace},
		supervisor,
		credentials,
	)
	if err != nil {
		return nil, errors.Join(ErrInvalidConfiguration, err)
	}
	if runner == nil {
		return nil, ErrInvalidConfiguration
	}
	pinned := &pinnedRunner{
		delegate: runner, executable: manifest.Harness.Executable, digest: harnessDigest,
		arguments: append([]string(nil), manifest.Harness.Arguments...),
	}
	profile := launcher.Profile()
	return &Stack{runner: pinned, evidence: EvidenceV1{
		Version: evidenceVersionV1, ManifestRevision: manifest.Revision, Boundary: manifest.OCI.Boundary,
		IsolationProfile: profile.Name, ReconciledResidue: residue,
	}}, nil
}

type pinnedRunner struct {
	delegate   attachedworkerdaemon.Runner
	executable string
	digest     attachedworkerdaemon.ExecutableDigest
	arguments  []string
}

func (runner *pinnedRunner) Run(ctx context.Context, invocation attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	process := invocation.Process
	if runner == nil || runner.delegate == nil || process.Executable != runner.executable ||
		process.ExecutableDigest != runner.digest || !slices.Equal(process.Arguments, runner.arguments) {
		return attachedworkerdaemon.InvocationResult{}, ErrInvocationMismatch
	}
	return runner.delegate.Run(ctx, invocation)
}

func defaultDependencies() dependencies {
	return dependencies{
		hostOS: runtime.GOOS,
		digest: attachedworkerdaemon.DigestExecutable,
		newLauncher: func(ctx context.Context, config attachedworkeroci.Config) (reconcilingLauncher, error) {
			return attachedworkeroci.NewLauncher(ctx, config)
		},
		newSupervisor: func(config attachedworkerdaemon.SupervisorConfig) (attachedworkerdaemon.ProcessRunner, error) {
			return attachedworkerdaemon.NewSupervisor(config)
		},
		newRunner: func(config attachedworkerdaemon.InvocationRunnerConfig, process attachedworkerdaemon.ProcessRunner, credentials ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			return attachedworkerdaemon.NewInvocationRunner(config, process, credentials)
		},
	}
}

func launcherConfig(config attachedworkerlocal.OCIConfigV1) attachedworkeroci.Config {
	return attachedworkeroci.Config{
		DockerPath: config.DockerPath, DockerSHA256: config.DockerSHA256, CLIConfigDir: config.CLIConfigDir, Host: config.Host,
		EngineID: config.EngineID, InstallationID: config.InstallationID,
		Boundary: attachedworkeroci.BoundaryKind(config.Boundary), Image: config.Image,
		UserID: config.UserID, GroupID: config.GroupID, DiskBytes: config.DiskBytes,
		CredentialFileBytes: config.CredentialFileBytes, MemoryBytes: config.MemoryBytes,
		PIDsLimit: config.PIDsLimit, StopSeconds: config.StopSeconds,
	}
}

func digestMatches(observed attachedworkerdaemon.ExecutableDigest, encoded string) bool {
	expected, err := hex.DecodeString(encoded)
	return err == nil && len(expected) == len(observed) && slices.Equal(observed[:], expected)
}

func strictExecutableDigest(path string, digest func(string) (attachedworkerdaemon.ExecutableDigest, error)) (attachedworkerdaemon.ExecutableDigest, error) {
	if digest == nil || path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return attachedworkerdaemon.ExecutableDigest{}, ErrArtifactMismatch
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Size() <= 0 || info.Size() > 128<<20 || info.Mode().Perm()&0o022 != 0 || info.Mode().Perm()&0o111 == 0 {
		return attachedworkerdaemon.ExecutableDigest{}, ErrArtifactMismatch
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return attachedworkerdaemon.ExecutableDigest{}, ErrArtifactMismatch
	}
	return digest(path)
}

func privateCanonicalDirectory(path string) (string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", ErrInvalidConfiguration
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(info) {
		return "", ErrInvalidConfiguration
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return "", ErrInvalidConfiguration
	}
	return canonical, nil
}

func hostSupportsBoundary(hostOS string, boundary attachedworkerlocal.RuntimeBoundary) bool {
	return hostOS == "darwin" && boundary == attachedworkerlocal.BoundaryDarwinVM ||
		hostOS == "linux" && boundary == attachedworkerlocal.BoundaryLinuxRootless
}

var _ attachedworkerdaemon.Runner = (*Stack)(nil)
var _ attachedworkerdaemon.Runner = (*pinnedRunner)(nil)
