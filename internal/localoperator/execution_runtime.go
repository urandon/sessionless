package localoperator

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// LinkedConnectorIdentity identifies this worker's compiled-in connector, not
// an independently attested proxy or an observed network connection. It is a
// manifest-construction helper only; the runtime producer must still inspect
// the running worker and retain each actual connection separately.
func LinkedConnectorIdentity(workerExecutableDigest string) (string, error) {
	if domain.ValidateSHA256Digest("local_connector.worker", workerExecutableDigest) != nil {
		return "", ErrDenied
	}
	return runtimeIdentityDigest("sessionless.local-linked-connector.v1", struct{ WorkerExecutableDigest string }{workerExecutableDigest}), nil
}

// ValidateExecutionBinding is I/O-free. It correlates a concrete retained
// installation receipt with execution metadata; it grants no current DB,
// preparation, spending or send authority. Store still joins all original and
// current canonical state. Caller-authored Source values fail aliveLocked.
func (s *Source) ValidateExecutionBinding(a domain.ServerlessInvocationAuthorityV1, at time.Time) error {
	if s == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || a.ValidateAt(at) != nil || s.receipt.Profile.ValidateAt(at) != nil || s.config.ValidateForBinding(a.HarnessBinding, a.SubstrateBinding, at) != nil {
		return ErrDenied
	}
	p := s.receipt.Profile
	price, pe := p.CostCeiling.Digest()
	admission, ae := a.AdmissionCostCeiling.Digest()
	connector, ce := LinkedConnectorIdentity(s.config.WorkerExecutableDigest)
	if pe != nil || ae != nil || ce != nil || price != admission || p.Substrate != a.SubstrateBinding ||
		p.Resource != a.HarnessBinding.Resource || p.Locator.TenantID != a.Lease.TenantID || p.Locator.OwnerUserID != a.HarnessBinding.OwnerUserID ||
		p.Substrate.EgressProxyArtifactDigest != s.config.WorkerExecutableDigest || p.Substrate.EgressProxyIdentityDigest != connector {
		return ErrDenied
	}
	return nil
}

// InspectExecutionRuntime observes this trusted Go process's executable file,
// boot-scoped host and current network namespace, outside any SQL callback.
// An arbitrary installed file is not accepted as the executing worker. The
// owner host remains trusted: this is not a remote loaded-memory attestation,
// container/cgroup proof, whole-process RSS limit or model qualification.
func (s *Source) InspectExecutionRuntime(ctx context.Context, a domain.ServerlessInvocationAuthorityV1, at time.Time) (domain.LocalRuntimeAttestationV2, error) {
	if ctx == nil || ctx.Err() != nil || s.ValidateExecutionBinding(a, at) != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	c, err := s.Configuration()
	if err != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	executable, err := os.Executable()
	if err != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	// The OS may report /var rather than /private/var on Darwin. Resolve only
	// this kernel-selected path, not an ambient/caller executable selector;
	// InspectRuntime then retains its strict canonical no-symlink FD checks.
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	identity, err := InspectRuntime(ctx, executable)
	if err != nil || ctx.Err() != nil || s.ValidateExecutionBinding(a, at) != nil || identity.WorkerExecutableDigest != c.WorkerExecutableDigest ||
		identity.LocalHostIdentityDigest != c.LocalHostIdentityDigest || identity.LocalNetworkNamespaceIdentityDigest != c.LocalNetworkNamespaceIdentityDigest {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	digest, err := c.Digest()
	if err != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	v := domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: digest, InstallationID: c.LocalInstallationID, InstallationRevision: c.LocalInstallationRevision,
		WorkerExecutableDigest: identity.WorkerExecutableDigest, HostIdentityDigest: identity.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: identity.LocalNetworkNamespaceIdentityDigest, Limits: c.Limits}
	if v.ValidateForBinding(a.SubstrateBinding, a.HarnessBinding.Backend) != nil {
		return domain.LocalRuntimeAttestationV2{}, ErrDenied
	}
	return v, nil
}
