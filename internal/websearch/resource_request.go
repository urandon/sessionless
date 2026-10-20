package websearch

import (
	"bytes"
	"context"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// CompilerProfileV2 selects the closed reusable-resource-grant compiler. It is
// a profile identifier, not a computed code hash or proof of installed code.
const CompilerProfileV2 = "tavily.search.basic.resource-grant.v2"

// FrozenRequestV2 is request/result DATA, never Source, consent or send authority.
type FrozenRequestV2 struct {
	compilerProfile string
	data            frozenRequestData
}

func CompileV2(g domain.WebSearchResourceGrantV2, cost domain.ExternalServiceCostCeilingV1, b BindingV1, args []byte, at time.Time) (*FrozenRequestV2, error) {
	if g.ValidateCostAt(cost, at) != nil || b.validateIdentity(g.TenantID, g.OwnerUserID) != nil {
		return nil, ErrContract
	}
	gd, err := g.Digest()
	if err != nil {
		return nil, ErrContract
	}
	until := g.ExpiresAt
	if cost.PriceExpiresAt.Before(until) {
		until = cost.PriceExpiresAt
	}
	facts := grantFacts{tenant: g.TenantID, owner: g.OwnerUserID, profileRevision: g.ProfileRevision, consentRevision: g.ConsentRevision, revocationGeneration: g.RevocationGeneration,
		artifactDigest: g.AdapterArtifactDigest, currency: g.Currency, priceEvidenceDigest: g.PriceEvidenceDigest, perCallReservation: g.PerCallReservationMicrounits}
	data, err := compileRequestData(facts, gd, b, args, at, until)
	if err != nil {
		return nil, err
	}
	return &FrozenRequestV2{compilerProfile: CompilerProfileV2, data: data}, nil
}

func (*FrozenRequestV2) String() string {
	return "WebSearchRequestV2{arguments:[redacted] query:[redacted] wire:[redacted]}"
}
func (f *FrozenRequestV2) GoString() string { return f.String() }
func (f *FrozenRequestV2) valid() bool {
	return f != nil && f.compilerProfile == CompilerProfileV2 && f.data.binding.validateIdentity(f.data.facts.tenant, f.data.facts.owner) == nil
}
func (f *FrozenRequestV2) Arguments() []byte {
	if !f.valid() {
		return nil
	}
	return bytes.Clone(f.data.arguments)
}
func (f *FrozenRequestV2) Body() []byte {
	if !f.valid() {
		return nil
	}
	return bytes.Clone(f.data.wire)
}
func (f *FrozenRequestV2) Digests() (string, string, string) {
	if !f.valid() {
		return "", "", ""
	}
	return f.data.argumentsDigest, f.data.queryDigest, f.data.wireDigest
}
func (f *FrozenRequestV2) ValidateOperation(op domain.AttemptOperationV1) error {
	if !f.valid() {
		return ErrOperationCompatibility
	}
	return f.data.validateOperation(op)
}
func (f *FrozenRequestV2) BindOperationV2(op domain.AttemptOperationV1) (*FrozenRequestV2, error) {
	if f.ValidateOperation(op) != nil {
		return nil, ErrOperationCompatibility
	}
	copy := *f
	copy.data.arguments = f.Arguments()
	copy.data.wire = f.Body()
	copy.data.binding.OperationDigest, _ = op.Digest()
	copy.data.operationDeadline = op.Deadline
	return &copy, nil
}
func (f *FrozenRequestV2) ValidateConsentRequest(r domain.WebSearchConsentRequestV1) error {
	if !f.valid() {
		return ErrContract
	}
	return f.data.validateConsentRequest(r)
}

// DecodeResponseV2 uses the unchanged Basic wire/result V1 codec. Its Reader
// remains borrowed; the original owned transport must enforce cancellation and
// the response deadline. This DATA method does not start HTTP or close a body.
func (f *FrozenRequestV2) DecodeResponseV2(ctx context.Context, meta HTTPMetadataV1, r io.Reader, at time.Time) (domain.WebSearchResultV1, error) {
	if !f.valid() {
		return domain.WebSearchResultV1{}, ErrContract
	}
	return f.data.decodeResponse(ctx, meta, r, at)
}
