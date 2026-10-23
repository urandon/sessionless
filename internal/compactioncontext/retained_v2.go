package compactioncontext

import "gitcode.com/urandon/sessionless/internal/domain"

// RetainedCodecVersion selects only the exact DATA codec. It confers no
// current Source, upload, reuse-fit or execution authority.
func (p *GenerationV1) RetainedCodecVersion() uint32 {
	if p == nil || p.frozen == nil || (p.version != 1 && p.version != 2) {
		return 0
	}
	return p.version
}

// CodecVersion reports the already validated opaque container's wire format;
// callers must choose an explicit decoder before constructing that container.
func (v *RetainedV1) CodecVersion() uint32 {
	if v == nil || v.plan.Validate() != nil || v.summary.Core != v.manifest.Core {
		return 0
	}
	switch v.manifest.Core.Version {
	case 1:
		if v.summary.Schema == "sessionless.retained-compaction-summary.v1" && v.manifest.Schema == "sessionless.compaction-manifest.v1" {
			return 1
		}
	case 2:
		if v.summary.Schema == "sessionless.retained-compaction-summary.v2" && v.manifest.Schema == "sessionless.compaction-manifest.v2" {
			return 2
		}
	}
	return 0
}

// BuildRetainedV2 explicitly seals the V2 context grammar, including truthful
// unknown/full-reserved MCP DATA and canonical projection provenance. It never
// mints financial authority, cleanup evidence or an immutable-store receipt.
func (p *GenerationV1) BuildRetainedV2(e RetainedEvidenceV1) (*RetainedV1, error) {
	if p == nil || p.version != 2 {
		return nil, ErrCanonical
	}
	return p.buildRetained(e, 2)
}

// DecodeRetainedV2 has no fallback to the V1 schema or measured-money rules.
// The original reader separately authenticates the full stored closure.
func DecodeRetainedV2(profile domain.ConfiguredInferenceProfileV1, policy domain.AgentLoopPolicyV1, plan domain.DerivedContextPlanV1,
	summaryArtifact, manifest []byte, parent *RetainedV1) (*RetainedV1, error) {
	if profile.Validate() != nil {
		return nil, ErrCanonical
	}
	pd, _ := profile.Digest()
	return decodeRetainedV1(&profile, pd, policy, plan, summaryArtifact, manifest, parent, 2)
}

// DecodeSealedRetainedV2 validates retired-profile historical DATA only; it
// cannot restore schema qualification, token fit or execution permission.
func DecodeSealedRetainedV2(profileDigest string, policy domain.AgentLoopPolicyV1, plan domain.DerivedContextPlanV1, summaryArtifact, manifest []byte, parent *RetainedV1) (*RetainedV1, error) {
	if !retainedValidDigest(profileDigest) || policy.Backend.BackendProfileDigest != profileDigest {
		return nil, ErrCanonical
	}
	return decodeRetainedV1(nil, profileDigest, policy, plan, summaryArtifact, manifest, parent, 2)
}

// DependenciesV2 returns exact canonical/parent custody DATA for the explicit
// V2 reader. Deletion still requires original Store ownership and cleanup.
func (v *RetainedV1) DependenciesV2() ([]domain.SessionEvent, []domain.DerivedContextPlanV1) {
	if v == nil || v.manifest.Core.Version != 2 {
		return nil, nil
	}
	return v.dependencies()
}
