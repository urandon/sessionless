package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	WebSearchProfileV1            = "public_web_basic"
	WebSearchAPIProfileV1         = "tavily.search.basic.v1"
	WebSearchEndpointV1           = "https://api.tavily.com/search"
	WebSearchCredentialAudienceV1 = "tavily.search"
	WebSearchArgumentsBytesV1     = 4 << 10
	WebSearchQueryBytesV1         = 1024
	// Physical-wire cap for the explicitly versioned search-wire arm. Absent
	// that arm, the original operation retains its 4KiB tool request ceiling.
	WebSearchWireBytesV1     = 8 << 10
	WebSearchResponseBytesV1 = 256 << 10
	WebSearchResultBytesV1   = 24 << 10
)

// WebSearchGrantV1 is immutable scoped DATA, not current approval, funding,
// a credential, consent decision or an executable capability. Its digest is
// constructed before the enclosing loop policy/binding, without a digest cycle.
type WebSearchGrantV1 struct {
	Version                        uint32    `json:"version"`
	TenantID                       TenantID  `json:"tenant_id"`
	OwnerUserID                    UserID    `json:"owner_user_id"`
	RunID                          RunID     `json:"run_id"`
	AttemptID                      AttemptID `json:"attempt_id"`
	BillingOwnerUserID             UserID    `json:"billing_owner_user_id"`
	ResourceID                     string    `json:"resource_id"`
	ResourceRevision               uint64    `json:"resource_revision"`
	RevocationGeneration           uint64    `json:"revocation_generation"`
	ConsentRevision                uint64    `json:"consent_revision"`
	ProfileID                      string    `json:"profile_id"`
	ProfileRevision                uint64    `json:"profile_revision"`
	Provider                       string    `json:"provider"`
	APIProfile                     string    `json:"api_profile"`
	Endpoint                       string    `json:"endpoint"`
	Method                         string    `json:"method"`
	CredentialRef                  string    `json:"credential_ref"`
	CredentialAudience             string    `json:"credential_audience"`
	DataPolicyDigest               string    `json:"data_policy_digest"`
	APISchemaDigest                string    `json:"api_schema_digest"`
	AdapterArtifactDigest          string    `json:"adapter_artifact_digest"`
	Currency                       string    `json:"currency"`
	PriceRevision                  string    `json:"price_revision"`
	PriceEvidenceDigest            string    `json:"price_evidence_digest"`
	TermsEvidenceDigest            string    `json:"terms_evidence_digest"`
	PerCallReservationMicrounits   uint64    `json:"per_call_reservation_microunits"`
	AggregateReservationMicrounits uint64    `json:"aggregate_reservation_microunits"`
	IssuedAt                       time.Time `json:"issued_at"`
	ExpiresAt                      time.Time `json:"expires_at"`
}

func searchInvalid(field string) error {
	return ValidationError{Field: "web_search." + field, Reason: "invalid bounded search data"}
}

func (v WebSearchGrantV1) Validate() error {
	if v.Version != 1 || v.TenantID.Validate() != nil || v.OwnerUserID.Validate() != nil || v.RunID.Validate() != nil || v.AttemptID.Validate() != nil || v.BillingOwnerUserID.Validate() != nil {
		return searchInvalid("scope")
	}
	for _, id := range []string{v.ResourceID, v.CredentialRef, v.PriceRevision} {
		if ValidateOpaqueID("web_search.id", id) != nil {
			return searchInvalid("id")
		}
	}
	for _, d := range []string{v.DataPolicyDigest, v.APISchemaDigest, v.AdapterArtifactDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest} {
		if ValidateSHA256Digest("web_search.digest", d) != nil {
			return searchInvalid("digest")
		}
	}
	if v.ResourceRevision == 0 || v.RevocationGeneration == 0 || v.ConsentRevision == 0 || v.ProfileRevision == 0 ||
		v.ProfileID != WebSearchProfileV1 || v.Provider != "tavily" || v.APIProfile != WebSearchAPIProfileV1 || v.Endpoint != WebSearchEndpointV1 || v.Method != "POST" || v.CredentialAudience != WebSearchCredentialAudienceV1 ||
		v.Currency != "USD" || v.PerCallReservationMicrounits != 10000 || v.AggregateReservationMicrounits != 20000 {
		return searchInvalid("profile")
	}
	if !ownedCurrentTime(v.IssuedAt) || !ownedCurrentTime(v.ExpiresAt) || !v.ExpiresAt.After(v.IssuedAt) {
		return searchInvalid("window")
	}
	return nil
}

func searchJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'}), nil
}
func searchDigest(kind string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}
func (v WebSearchGrantV1) Digest() (string, error) {
	if err := v.Validate(); err != nil {
		return "", err
	}
	b, e := searchJSON(v)
	if e != nil {
		return "", e
	}
	return searchDigest("sessionless.web-search-grant.v1", b), nil
}

// ValidateCostAt checks structural correlation only. The original publisher,
// current Store transaction and custody must authenticate the actual evidence.
func (v WebSearchGrantV1) ValidateCostAt(cost ExternalServiceCostCeilingV1, at time.Time) error {
	d, e := v.Digest()
	if e != nil || cost.ValidateAt(at) != nil || at.Before(v.IssuedAt) || !at.Before(v.ExpiresAt) || cost.Kind != ExternalServiceWebSearchV1 ||
		cost.ValidateForOwnerGrantCurrency(v.TenantID, v.OwnerUserID, v.BillingOwnerUserID, d, v.Currency) != nil || cost.ResourceID != v.ResourceID || cost.ResourceRevision != v.ResourceRevision ||
		cost.RevocationGeneration != v.RevocationGeneration || cost.ProfileID != v.ProfileID || cost.ProfileRevision != v.ProfileRevision || cost.PriceRevision != v.PriceRevision ||
		cost.PriceEvidenceDigest != v.PriceEvidenceDigest || cost.TermsEvidenceDigest != v.TermsEvidenceDigest || cost.PriceState != ExternalServicePriceKnownPaidV1 ||
		cost.MaxAmountMicrounits == nil || *cost.MaxAmountMicrounits != v.AggregateReservationMicrounits {
		return searchInvalid("cost")
	}
	return nil
}

type WebSearchSourceV1 struct {
	SourceID         string     `json:"source_id"`
	Rank             uint32     `json:"rank"`
	URL              string     `json:"url"`
	Title            string     `json:"title"`
	Excerpt          string     `json:"excerpt"`
	TitleTruncated   bool       `json:"title_truncated"`
	ExcerptTruncated bool       `json:"excerpt_truncated"`
	PublishedAt      *time.Time `json:"published_at,omitempty"`
}
type WebSearchWarningV1 struct {
	Rank uint32 `json:"rank"`
	Code string `json:"code"`
}
type WebSearchUsageV1 struct {
	State   string  `json:"state"`
	Credits *uint64 `json:"credits,omitempty"`
}

// Result is normalized untrusted DATA. Raw digest is not transport provenance
// or persistence proof. No query, API key, provider error or monetary settlement
// enters this shape. Source links are not followed or DNS-validated here.
type WebSearchResultV1 struct {
	Version           uint32                   `json:"version"`
	Scope             AttemptOperationScopeV1  `json:"scope"`
	OperationDigest   AttemptOperationDigestV1 `json:"operation_digest"`
	Sequence          uint64                   `json:"sequence"`
	CallID            string                   `json:"call_id"`
	ProfileRevision   uint64                   `json:"profile_revision"`
	GrantDigest       string                   `json:"grant_digest"`
	QueryDigest       string                   `json:"query_digest"`
	WireRequestDigest string                   `json:"wire_request_digest"`
	RawResponseDigest string                   `json:"raw_response_digest"`
	Provider          string                   `json:"provider"`
	RetrievedAt       time.Time                `json:"retrieved_at"`
	Outcome           string                   `json:"outcome"`
	Usage             WebSearchUsageV1         `json:"usage"`
	Sources           []WebSearchSourceV1      `json:"sources"`
	Warnings          []WebSearchWarningV1     `json:"warnings"`
}

func WebSearchSourceIDV1(operation AttemptOperationDigestV1, rank uint32, uri string) string {
	return "source-" + searchDigest("sessionless.web-search-source.v1", []byte(string(operation)+"\x00"+strconv.FormatUint(uint64(rank), 10)+"\x00"+uri))
}
func (v WebSearchResultV1) Clone() WebSearchResultV1 {
	v.Sources = append([]WebSearchSourceV1{}, v.Sources...)
	for i := range v.Sources {
		if p := v.Sources[i].PublishedAt; p != nil {
			copy := *p
			v.Sources[i].PublishedAt = &copy
		}
	}
	v.Warnings = append([]WebSearchWarningV1{}, v.Warnings...)
	if v.Usage.Credits != nil {
		c := *v.Usage.Credits
		v.Usage.Credits = &c
	}
	return v
}
func (v WebSearchResultV1) Validate() error {
	if v.Version != 1 || v.Scope.Validate() != nil || v.OperationDigest.Validate() != nil || v.Sequence < 2 || v.Sequence > 12 || ValidateOpaqueID("web_search.call_id", v.CallID) != nil || v.ProfileRevision == 0 || v.Provider != "tavily" || !ownedCurrentTime(v.RetrievedAt) || v.Sources == nil || len(v.Sources) > 5 || v.Warnings == nil || len(v.Warnings) > 20 {
		return searchInvalid("result")
	}
	for _, d := range []string{v.GrantDigest, v.QueryDigest, v.WireRequestDigest, v.RawResponseDigest} {
		if ValidateSHA256Digest("web_search.result.digest", d) != nil {
			return searchInvalid("result.digest")
		}
	}
	if (v.Usage.State == "unknown" && v.Usage.Credits != nil) || (v.Usage.State == "provider_reported" && (v.Usage.Credits == nil || *v.Usage.Credits > 9007199254740991)) || (v.Usage.State != "unknown" && v.Usage.State != "provider_reported") {
		return searchInvalid("usage")
	}
	if (v.Outcome == "known_results" && len(v.Sources) == 0) || (v.Outcome != "known_results" && len(v.Sources) != 0) || (v.Outcome != "known_results" && v.Outcome != "known_empty" && v.Outcome != "known_service_error") {
		return searchInvalid("outcome")
	}
	seen := map[string]bool{}
	last := uint32(0)
	for _, s := range v.Sources {
		if s.Rank <= last || s.Rank > 5 || s.SourceID != WebSearchSourceIDV1(v.OperationDigest, s.Rank, s.URL) || ValidateWebSearchCitationURLV1(s.URL) != nil || seen[s.URL] || len(s.Title) == 0 || len(s.Title) > 512 || !utf8.ValidString(s.Title) || len(s.Excerpt) > 2048 || !utf8.ValidString(s.Excerpt) {
			return searchInvalid("source")
		}
		if s.PublishedAt != nil && !ownedCurrentTime(*s.PublishedAt) {
			return searchInvalid("publication")
		}
		seen[s.URL] = true
		last = s.Rank
	}
	for _, w := range v.Warnings {
		if w.Rank < 1 || w.Rank > 5 || (w.Code != "invalid_source" && w.Code != "duplicate_source" && w.Code != "unknown_publication_time") {
			return searchInvalid("warning")
		}
	}
	b, e := searchJSON(v)
	if e != nil || len(b) > WebSearchResultBytesV1 {
		return searchInvalid("result.bytes")
	}
	return nil
}
func EncodeWebSearchResultV1(v WebSearchResultV1) ([]byte, error) {
	if e := v.Validate(); e != nil {
		return nil, e
	}
	return searchJSON(v)
}
func (v WebSearchResultV1) ValidateForOperation(op AttemptOperationV1) error {
	if v.Validate() != nil || op.Validate() != nil || op.Kind != AttemptOperationWebSearchV1 || v.Scope != op.Scope || v.Sequence != op.Sequence || v.CallID != op.CallID ||
		v.GrantDigest != op.ToolGrantDigest || v.QueryDigest != op.QueryDigest || v.WireRequestDigest != op.RequestDigest {
		return searchInvalid("result.operation")
	}
	d, e := op.Digest()
	body, _ := searchJSON(v)
	if e != nil || d != v.OperationDigest || uint64(len(body)) > op.ResultBytes {
		return searchInvalid("result.operation")
	}
	return nil
}
func DecodeWebSearchResultV1(body []byte) (WebSearchResultV1, error) {
	var v WebSearchResultV1
	if len(body) == 0 || len(body) > WebSearchResultBytesV1 || !utf8.Valid(body) {
		return v, searchInvalid("result.bytes")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF || v.Validate() != nil {
		return WebSearchResultV1{}, searchInvalid("result.codec")
	}
	canonical, e := searchJSON(v)
	if e != nil || !bytes.Equal(canonical, body) {
		return WebSearchResultV1{}, searchInvalid("result.canonical")
	}
	return v, nil
}

// Syntax/hostname policy only: no DNS lookup, page fetching or safety claim.
func ValidateWebSearchCitationURLV1(raw string) error {
	if len(raw) == 0 || len(raw) > 2048 || !utf8.ValidString(raw) {
		return searchInvalid("url")
	}
	for _, r := range raw {
		if r <= ' ' || r == '\\' || r == 127 {
			return searchInvalid("url")
		}
	}
	u, e := url.ParseRequestURI(raw)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.Opaque != "" || u.User != nil {
		return searchInvalid("url")
	}
	h := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if h == "" || len(h) > 253 || net.ParseIP(h) != nil || strings.ContainsAny(h, "%:") {
		return searchInvalid("url.host")
	}
	if p := u.Port(); p != "" {
		n, e := strconv.ParseUint(p, 10, 16)
		if e != nil || n == 0 {
			return searchInvalid("url.port")
		}
	}
	labels := strings.Split(h, ".")
	if len(labels) < 2 {
		return searchInvalid("url.host")
	}
	for _, blocked := range []string{"localhost", "local", "internal", "localdomain", "lan", "home", "test", "invalid", "onion", "metadata.google.internal", "metadata.goog", "instance-data.ec2.internal"} {
		if h == blocked || strings.HasSuffix(h, "."+blocked) {
			return searchInvalid("url.host")
		}
	}
	allNumeric := true
	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return searchInvalid("url.host")
		}
		numeric := true
		number := strings.TrimPrefix(label, "0x")
		for _, c := range label {
			if !((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-') {
				return searchInvalid("url.host")
			}
		}
		for _, c := range number {
			if c < '0' || c > '9' {
				if !strings.HasPrefix(label, "0x") || c < 'a' || c > 'f' {
					numeric = false
				}
			}
		}
		allNumeric = allNumeric && numeric
	}
	if allNumeric {
		return searchInvalid("url.host")
	}
	return nil
}
