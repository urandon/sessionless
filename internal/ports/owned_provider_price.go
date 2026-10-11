package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedProviderPriceCustodyV1 is installed only with the ORIGINAL immutable
// profile-price producer. Its <=4-KiB canonical ProviderPriceObservationV1
// body must have been retained before authenticated profile publication;
// profile.ProviderPriceDigest seals the structured domain digest of that body.
// This read-only authority metadata is not a Session operation body, monetary
// ledger, caller price quote, send capability, or live provider observation.
// Store independently rejoins the original current profile/spending and parses
// the exact body. A caller-supplied success flag or verifier interface is not
// evidence. Serving installs no adapter by default.
type OwnedProviderPriceCustodyV1 interface {
	ReadOwnedProviderPrice(context.Context, domain.OwnedAgentProfileReferenceV1, uint64) ([]byte, error)
}
