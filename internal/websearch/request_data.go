package websearch

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// grantFacts is a private DATA projection, not a grant, source, approval or
// version adapter. Each explicit compiler validates its original grant first.
type grantFacts struct {
	tenant                                                 domain.TenantID
	owner                                                  domain.UserID
	profileRevision, consentRevision, revocationGeneration uint64
	artifactDigest, currency, priceEvidenceDigest          string
	perCallReservation                                     uint64
}

// V1 exposes a borrowed view of its unchanged private fields; V2 owns this
// same representation. Neither path duplicates the Basic wire/result protocol.
type frozenRequestData struct {
	facts                                                 grantFacts
	binding                                               BindingV1
	arguments, wire                                       []byte
	query                                                 string
	maximum                                               int
	argumentsDigest, queryDigest, wireDigest, grantDigest string
	compiledAt, validUntil, operationDeadline             time.Time
}

func (f *FrozenRequestV1) dataView() frozenRequestData {
	return frozenRequestData{facts: grantFacts{tenant: f.grant.TenantID, owner: f.grant.OwnerUserID, profileRevision: f.grant.ProfileRevision,
		consentRevision: f.grant.ConsentRevision, revocationGeneration: f.grant.RevocationGeneration, artifactDigest: f.grant.AdapterArtifactDigest,
		currency: f.grant.Currency, priceEvidenceDigest: f.grant.PriceEvidenceDigest, perCallReservation: f.grant.PerCallReservationMicrounits},
		binding: f.binding, arguments: f.arguments, wire: f.wire, query: f.query, maximum: f.maximum, argumentsDigest: f.argumentsDigest,
		queryDigest: f.queryDigest, wireDigest: f.wireDigest, grantDigest: f.grantDigest, compiledAt: f.compiledAt, validUntil: f.validUntil, operationDeadline: f.operationDeadline}
}

func compileRequestData(facts grantFacts, grantDigest string, b BindingV1, args []byte, at, until time.Time) (frozenRequestData, error) {
	deny := func() (frozenRequestData, error) { return frozenRequestData{}, ErrContract }
	v, e := strictJSON(args, domain.WebSearchArgumentsBytesV1)
	m, ok := v.(map[string]any)
	if e != nil || !ok || !keys(m, "query", "max_results") {
		return deny()
	}
	query, ok := m["query"].(string)
	if !ok || strings.TrimSpace(query) == "" || len(query) > domain.WebSearchQueryBytesV1 || !utf8.ValidString(query) {
		return deny()
	}
	maximum := 5
	if value, exists := m["max_results"]; exists {
		n, ok := value.(json.Number)
		if !ok {
			return deny()
		}
		i, e := n.Int64()
		if e != nil || i < 1 || i > 5 {
			return deny()
		}
		maximum = int(i)
	}
	// Exact supported options, never SDK/vendor defaults or credential bytes.
	request := struct {
		Query                    string `json:"query"`
		MaxResults               int    `json:"max_results"`
		SearchDepth              string `json:"search_depth"`
		Topic                    string `json:"topic"`
		Language                 string `json:"language"`
		FilterByLanguage         bool   `json:"filter_by_language"`
		SafeSearch               bool   `json:"safe_search"`
		AutoParameters           bool   `json:"auto_parameters"`
		IncludeUsage             bool   `json:"include_usage"`
		IncludeAnswer            bool   `json:"include_answer"`
		IncludeRawContent        bool   `json:"include_raw_content"`
		IncludeImages            bool   `json:"include_images"`
		IncludeImageDescriptions bool   `json:"include_image_descriptions"`
		IncludeFavicon           bool   `json:"include_favicon"`
	}{query, maximum, "basic", "general", "en", true, true, false, true, false, false, false, false, false}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if encoder.Encode(request) != nil {
		return deny()
	}
	body := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	if len(body) > domain.WebSearchWireBytesV1 {
		return deny()
	}
	return frozenRequestData{facts: facts, binding: b, arguments: bytes.Clone(args), wire: bytes.Clone(body), query: query, maximum: maximum,
		argumentsDigest: digest(args), queryDigest: digest([]byte(query)), wireDigest: digest(body), grantDigest: grantDigest, compiledAt: at, validUntil: until}, nil
}

func (f frozenRequestData) validateOperation(op domain.AttemptOperationV1) error {
	if op.Validate() != nil || op.Kind != domain.AttemptOperationWebSearchV1 || op.Scope != f.binding.Scope || op.Sequence != f.binding.Sequence || op.CallID != f.binding.CallID ||
		op.SearchWire == nil || op.SearchWire.Version != 1 || op.SearchWire.APIProfile != domain.WebSearchAPIProfileV1 || op.SearchWire.ArgumentsBytes != uint64(len(f.arguments)) || op.SearchWire.CompilerArtifactDigest != f.facts.artifactDigest ||
		op.ModelResponseDigest != f.binding.ModelResponseDigest || op.ArgumentsDigest != f.argumentsDigest || op.QueryDigest != f.queryDigest || op.ToolGrantDigest != f.grantDigest || op.RequestDigest != f.wireDigest || op.RequestBytes != uint64(len(f.wire)) ||
		op.Debit.AmountMicrounits != f.facts.perCallReservation || op.Currency != f.facts.currency || op.PriceRevision != f.binding.ParentPriceRevision || op.AdmissionCostCeilingDigest != f.binding.ParentCostCeilingDigestV2 || op.ExternalChargeEvidenceDigest != f.facts.priceEvidenceDigest ||
		!op.Deadline.After(f.compiledAt) || op.Deadline.After(f.validUntil) || op.Debit.ActiveDuration > 15*time.Second {
		return ErrOperationCompatibility
	}
	d, e := op.Digest()
	if e != nil || (f.binding.OperationDigest != "" && d != f.binding.OperationDigest) {
		return ErrOperationCompatibility
	}
	return nil
}

func (f frozenRequestData) validateConsentRequest(r domain.WebSearchConsentRequestV1) error {
	if r.ValidateArguments(f.arguments) != nil || r.InitiatingUserID != f.binding.InitiatingUserID || r.ModelResponseDigest != f.binding.ModelResponseDigest || r.CallID != f.binding.CallID || r.GrantDigest != f.grantDigest || r.ArgumentsDigest != f.argumentsDigest ||
		r.QueryDigest != f.queryDigest || r.ConsentRevision != f.facts.consentRevision || r.RevocationGeneration != f.facts.revocationGeneration || r.ExpiresAt.After(f.validUntil) || (!f.operationDeadline.IsZero() && r.ExpiresAt.Before(f.operationDeadline)) {
		return ErrContract
	}
	s := f.binding.Scope
	expected := domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID,
		EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)}
	if r.Scope != expected {
		return ErrContract
	}
	return nil
}
