package websearch

import (
	"bytes"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// FrozenRequestV1 owns copies of original arguments and the exact compiled wire.
// Its private state prevents accidental mutation, NOT fabrication of authority.
type FrozenRequestV1 struct {
	grant                                                 domain.WebSearchGrantV1
	binding                                               BindingV1
	arguments, wire                                       []byte
	query                                                 string
	maximum                                               int
	argumentsDigest, queryDigest, wireDigest, grantDigest string
	compiledAt, validUntil                                time.Time
	// validUntil is the original grant/price upper bound, even after binding.
	// Only BindOperationV1 fills operationDeadline; a caller-supplied digest
	// alone cannot establish the operation's narrower response window.
	operationDeadline time.Time
}

func (*FrozenRequestV1) String() string {
	return "WebSearchRequest{arguments:[redacted] query:[redacted] wire:[redacted]}"
}
func (f *FrozenRequestV1) GoString() string { return f.String() }
func (f *FrozenRequestV1) Arguments() []byte {
	if f == nil {
		return nil
	}
	return bytes.Clone(f.arguments)
}
func (f *FrozenRequestV1) Body() []byte {
	if f == nil {
		return nil
	}
	return bytes.Clone(f.wire)
}
func (f *FrozenRequestV1) Digests() (arguments, query, wire string) {
	if f == nil {
		return "", "", ""
	}
	return f.argumentsDigest, f.queryDigest, f.wireDigest
}

func CompileV1(g domain.WebSearchGrantV1, cost domain.ExternalServiceCostCeilingV1, b BindingV1, args []byte, at time.Time) (*FrozenRequestV1, error) {
	if g.ValidateCostAt(cost, at) != nil || b.validate(g) != nil {
		return nil, ErrContract
	}
	// Compilation does not begin a search operation or shorten consent waiting
	// to fifteen seconds. BindOperation later seals its already admitted deadline.
	until := g.ExpiresAt
	if cost.PriceExpiresAt.Before(until) {
		until = cost.PriceExpiresAt
	}
	gd, _ := g.Digest()
	facts := grantFacts{tenant: g.TenantID, owner: g.OwnerUserID, profileRevision: g.ProfileRevision, consentRevision: g.ConsentRevision, revocationGeneration: g.RevocationGeneration,
		artifactDigest: g.AdapterArtifactDigest, currency: g.Currency, priceEvidenceDigest: g.PriceEvidenceDigest, perCallReservation: g.PerCallReservationMicrounits}
	data, err := compileRequestData(facts, gd, b, args, at, until)
	if err != nil {
		return nil, err
	}
	return &FrozenRequestV1{grant: g, binding: b, arguments: data.arguments, wire: data.wire, query: data.query, maximum: data.maximum, compiledAt: at, validUntil: until,
		argumentsDigest: data.argumentsDigest, queryDigest: data.queryDigest, wireDigest: data.wireDigest, grantDigest: gd}, nil
}
