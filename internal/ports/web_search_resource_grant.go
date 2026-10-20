package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedWebSearchResourceGrantCustodyV2 is installed only with the ORIGINAL
// immutable profile/grant Source. It returns caller-owned nonsecret DATA for
// the exact accepted profile reference, within cap (at most 8KiB). The caller
// clears that owned buffer. It is not an operation-body store, historical
// archive, approval/source verifier, monetary ledger or send capability.
// Store verifies the profile's raw body identity and semantic grant digest,
// then rejoins the original current source/parent/spending/actor transaction.
// Nil must preserve genuine V2 denial; no fixture/legacy lookup is a fallback.
type OwnedWebSearchResourceGrantCustodyV2 interface {
	ReadWebSearchResourceGrant(context.Context, domain.OwnedAgentProfileReferenceV1, uint64) ([]byte, error)
	// ExecutionLifetime is the existing ORIGINAL Source lifetime, not a new
	// grant or context-Value carrier. Consumers retain only Done and its finite
	// unchanged Deadline, never cancel this owner or derive new authority.
	ExecutionLifetime() (context.Context, error)
}
