package ydbstore

import (
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Select a closed retained writer grammar, then validate that grammar exactly.
// The original reader separately validates object refs and financial/current
// authority; there is never a decode-V2 failure -> permissive V1 fallback.
func decodeLocalRetainedGeneration(profile *domain.ConfiguredInferenceProfileV1, profileDigest string, policy domain.AgentLoopPolicyV1, plan domain.DerivedContextPlanV1, summary, manifest []byte, parent *compactioncontext.RetainedV1) (*compactioncontext.RetainedV1, error) {
	switch terminalCapsuleSchema(manifest) {
	case "sessionless.compaction-manifest.v1":
		if profile != nil {
			return compactioncontext.DecodeRetainedV1(*profile, policy, plan, summary, manifest, parent)
		}
		return compactioncontext.DecodeSealedRetainedV1(profileDigest, policy, plan, summary, manifest, parent)
	case "sessionless.compaction-manifest.v2":
		if profile != nil {
			return compactioncontext.DecodeRetainedV2(*profile, policy, plan, summary, manifest, parent)
		}
		return compactioncontext.DecodeSealedRetainedV2(profileDigest, policy, plan, summary, manifest, parent)
	default:
		return nil, domain.ErrMembershipDenied
	}
}
