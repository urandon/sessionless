package localoperator

import (
	"context"
	"crypto/rand"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// PrepareInstallation creates fresh private manifest.json and source.key files
// for later, separately authorized Source intake. The parent must already be a
// canonical, owner-private directory; root must be an unused canonical absolute
// path. Existing or incomplete output is never repaired, removed or reused.
// Any failure after reservation leaves its private output for explicit operator
// reconciliation, not an automatic retry at another path.
//
// Input is the existing canonical ManifestV1 with the key fingerprint, observed
// runtime identities and matching substrate artifact/connector/egress/cost
// digests empty. All other declarations, evidence bytes and correlations must
// already be supplied by the operator. Installation-owned fields stay empty.
// The completed DATA passes ValidateManifestTemplate before any output exists.
// This observes the explicit executable but never executes it or probes a
// service. It fabricates no review, capability or price evidence, creates no
// Source, installation IDs, publication grant or receipt, and grants no SQL or
// runtime authority. Host/same-UID trust is unchanged from private file intake.
func PrepareInstallation(ctx context.Context, root, executable string, rawTemplate []byte) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	m, err := decodeManifestTemplate(rawTemplate)
	if err != nil {
		return ErrDenied
	}
	c, s := &m.Configuration, &m.Profile.Substrate
	if m.KeyFingerprint != "" || c.WorkerExecutableDigest != "" || c.LocalHostIdentityDigest != "" || c.LocalNetworkNamespaceIdentityDigest != "" ||
		s.LocalHostIdentityDigest != "" || s.LocalNetworkNamespaceIdentityDigest != "" || s.OuterHarnessArtifactDigest != "" ||
		s.EgressProxyArtifactDigest != "" || s.EgressProxyIdentityDigest != "" || s.EgressPolicyDigest != "" || s.AdmissionCostCeilingDigest != "" {
		return ErrDenied
	}
	identity, err := InspectRuntime(ctx, executable)
	if err != nil {
		return ErrDenied
	}
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	s.LocalHostIdentityDigest, s.LocalNetworkNamespaceIdentityDigest = identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	s.OuterHarnessArtifactDigest, s.EgressProxyArtifactDigest = identity.WorkerExecutableDigest, identity.WorkerExecutableDigest
	s.EgressProxyIdentityDigest, err = LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		return ErrDenied
	}
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(*c)
	if err != nil {
		return ErrDenied
	}
	s.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		return ErrDenied
	}
	s.AdmissionCostCeilingDigest, err = m.Profile.CostCeiling.Digest()
	if err != nil || ctx.Err() != nil {
		return ErrDenied
	}
	var key [32]byte
	defer clear(key[:])
	if _, err := rand.Read(key[:]); err != nil {
		return ErrDenied
	}
	m.KeyFingerprint = hash(key[:])
	raw, err := json.Marshal(m)
	defer clear(raw)
	if err != nil || ValidateManifestTemplate(ctx, raw, executable) != nil || ctx.Err() != nil {
		return ErrDenied
	}
	return writePreparedInstallation(ctx, root, raw, key[:])
}
