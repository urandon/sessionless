// Package localoperator authenticates owner-local installation declarations
// from explicitly selected private files. It does not qualify a model, attest
// cloud isolation, grant tenant membership or enable a serving profile.
package localoperator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxOperatorManifestBytes = 256 << 10

var ErrDenied = domain.ErrMembershipDenied

// EvidenceBodyV1 retains the actual nonsecret bytes selected by the operator,
// not a claim that their contents prove model behavior or independent review.
type EvidenceBodyV1 struct {
	Reference domain.OwnedAgentAuthorityBodyRefV1 `json:"reference"`
	Body      []byte                              `json:"body"`
}

// InvoiceTermsV1 is owner-local authorization, not an upstream tariff. These
// explicit zero invoice terms cannot fund external MCP/search or imply zero
// memory, energy, wall-time or other economic cost.
type InvoiceTermsV1 struct {
	Version                uint32          `json:"version"`
	TenantID               domain.TenantID `json:"tenant_id"`
	OwnerUserID            domain.UserID   `json:"owner_user_id"`
	BaseURL                string          `json:"base_url"`
	ModelVendorID          string          `json:"model_vendor_id"`
	ModelID                string          `json:"model_id"`
	Currency               string          `json:"currency"`
	Revision               string          `json:"revision"`
	OwnerSuppliesModel     bool            `json:"owner_supplies_model"`
	OwnerSuppliesHost      bool            `json:"owner_supplies_host"`
	ProviderInvoiceAmount  uint64          `json:"provider_invoice_amount_microunits"`
	SubstrateInvoiceAmount uint64          `json:"substrate_invoice_amount_microunits"`
	IssuedAt               time.Time       `json:"issued_at"`
	ExpiresAt              time.Time       `json:"expires_at"`
}

// ManifestV1 contains templates, never chosen resource/installation/mutation
// IDs or a key. The separate private source.key is authenticated by this public
// fingerprint, so resume cannot silently rotate it. OriginalSource and installed
// manifest references are derived from the actual retained manifest bytes.
type ManifestV1 struct {
	Version           uint32                              `json:"version"`
	KeyFingerprint    string                              `json:"key_fingerprint"`
	Configuration     domain.ConfiguredInferenceProfileV1 `json:"configuration"`
	Profile           domain.OwnedAgentProfileCurrentV1   `json:"profile"`
	InvoiceTerms      InvoiceTermsV1                      `json:"invoice_terms"`
	Evidence          []EvidenceBodyV1                    `json:"evidence"`
	SearchExpenditure *OwnerLocalSearchExpenditureV2      `json:"search_expenditure,omitempty"`
}

// Source has one immutable installed declaration, not a registry or per-Attempt
// approval map. Its private key binds carriers only; provenance is the actual
// private-file intake, exact retained bodies and runtime correlation below.
type Source struct {
	mu       sync.Mutex
	self     *Source
	ctx      context.Context
	cancel   context.CancelFunc
	key      *sourceKey
	closed   bool
	config   domain.ConfiguredInferenceProfileV1
	profile  domain.OwnedAgentProfileCurrentV1
	manifest []byte
	bodies   []EvidenceBodyV1
	grant    *ports.OwnedProfilePublicationGrantV1
	receipt  *ports.OwnedProfilePublicationReceiptV1
	search   *searchAcceptance
	parent   *Source // original authenticated keeper while replacement is pending
}

// Keep raw key bytes behind a private pointer even if trusted code mistakenly
// formats a dereferenced Source value; struct diagnostics must not print them.
type sourceKey struct{ bytes [32]byte }

func (*Source) String() string     { return "localoperator.Source{private source:[redacted]}" }
func (s *Source) GoString() string { return s.String() }

// OpenFresh is called only by trusted local command composition, never an HTTP,
// model, queue or tool path. Files supply declarations; all new IDs are minted
// here. No source files, keys, processes or models are created by this function.
func OpenFresh(ctx context.Context, root, executable string) (*Source, error) {
	return open(ctx, root, executable, nil, nil)
}

// OpenExisting can authenticate the exact original source in another local
// process. A receipt is a locator, not commit/current-state proof. This mode
// cannot issue a fresh publication or regenerate a missing source/key/receipt;
// spending still point-joins the actual original current head in the Store.
func OpenExisting(ctx context.Context, root, executable string, receipt ports.OwnedProfilePublicationReceiptV1) (*Source, error) {
	return open(ctx, root, executable, &receipt, nil)
}

// Replacement requires this original keeper and receipt, plus a separately
// authenticated new declaration. It preserves resource/installation identity;
// the existing publisher still CAS-rechecks actual rows and both audit intents.
func (s *Source) Replacement(ctx context.Context, root, executable string, receipt ports.OwnedProfilePublicationReceiptV1) (*Source, error) {
	if s == nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	if !s.aliveLocked() || !s.receiptMatchesLocked(receipt) {
		s.mu.Unlock()
		return nil, ErrDenied
	}
	s.mu.Unlock()
	return open(ctx, root, executable, &receipt, s)
}

func open(ctx context.Context, root, executable string, original *ports.OwnedProfilePublicationReceiptV1, parent *Source) (*Source, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrDenied
	}
	raw, key, err := readOperatorFiles(ctx, root)
	if err != nil {
		return nil, sourceDenial("private files")
	}
	defer clear(key[:])
	var manifest ManifestV1
	if strictJSON(raw, &manifest) != nil || manifest.Version != 1 || len(manifest.Evidence) == 0 || len(manifest.Evidence) > 16 {
		return nil, sourceDenial("manifest encoding")
	}
	fingerprint := sha256.Sum256(key[:])
	if manifest.KeyFingerprint != hex.EncodeToString(fingerprint[:]) || domain.ValidateSHA256Digest("local_operator.key", manifest.KeyFingerprint) != nil {
		return nil, sourceDenial("source key identity")
	}
	identity, err := InspectRuntime(ctx, executable)
	if err != nil {
		return nil, sourceDenial("installed executable")
	}
	c, p := manifest.Configuration.Clone(), manifest.Profile.Clone()
	if c.Resource.ResourceID != "" || c.Resource.Revision != 0 || c.ProfileRevision != 0 || c.LocalInstallationID != "" || c.LocalInstallationRevision != 0 ||
		p.Resource.ResourceID != "" || p.Resource.Revision != 0 || p.ProfileRevision != 0 || p.InstallationID != "" || p.InstallationRevision != 0 || p.RevocationGeneration != 0 || p.MutationID != "" || p.State != "" ||
		p.Locator.ResourceID != "" || p.Locator.BackendProfileDigest != "" || p.Backend.BackendProfileDigest != "" || p.Substrate.LocalInstallationID != "" || p.Substrate.LocalInstallationRevision != 0 ||
		c.OriginalSource != (domain.OwnedAgentAuthorityBodyRefV1{}) || p.InstalledManifest != (domain.OwnedAgentAuthorityBodyRefV1{}) {
		return nil, ErrDenied
	}
	if c.WorkerExecutableDigest != identity.WorkerExecutableDigest || c.LocalHostIdentityDigest != identity.LocalHostIdentityDigest || c.LocalNetworkNamespaceIdentityDigest != identity.LocalNetworkNamespaceIdentityDigest {
		return nil, ErrDenied
	}
	for i, body := range manifest.Evidence {
		if body.Reference.Validate() != nil || body.Reference.Bytes != uint64(len(body.Body)) || hash(body.Body) != body.Reference.Digest {
			return nil, ErrDenied
		}
		for j := 0; j < i; j++ {
			if manifest.Evidence[j].Reference.Ref == body.Reference.Ref {
				return nil, ErrDenied
			}
		}
	}
	if !hasBody(manifest.Evidence, c.CapabilityEvidence) || !hasBody(manifest.Evidence, p.ReviewReceipt) {
		return nil, ErrDenied
	}
	terms, err := json.Marshal(manifest.InvoiceTerms)
	if err != nil || !validTerms(manifest.InvoiceTerms, c, p) || c.OperatorTerms.Bytes != uint64(len(terms)) || c.OperatorTerms.Digest != hash(terms) || !hasExactBody(manifest.Evidence, c.OperatorTerms, terms) {
		return nil, ErrDenied
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	sourceRef := domain.OwnedAgentAuthorityBodyRefV1{Ref: "local-source-" + hash(raw), Digest: hash(raw), Bytes: uint64(len(raw))}
	if sourceRef.Validate() != nil {
		return nil, ErrDenied
	}
	var resource, installation, mutation string
	revision := uint64(1)
	if original == nil {
		generator := idgen.New()
		resource, err = generator.NewID(ctx, ports.IDProviderResource)
		if err != nil {
			return nil, ErrDenied
		}
		installation, err = generator.NewID(ctx, ports.IDOwnedInstallation)
		if err != nil {
			return nil, ErrDenied
		}
		mutation, err = generator.NewID(ctx, ports.IDOwnedPublication)
		if err != nil {
			return nil, ErrDenied
		}
	} else {
		if original.Profile.ValidateInitial() != nil {
			return nil, ErrDenied
		}
		resource, installation, mutation, revision = original.Profile.Resource.ResourceID, original.Profile.InstallationID, original.Profile.MutationID, original.Profile.InstallationRevision
		if parent == nil {
			now = original.Profile.UpdatedAt
		} else {
			if revision == ^uint64(0) {
				return nil, ErrDenied
			}
			revision++
			mutation, err = idgen.New().NewID(ctx, ports.IDOwnedPublication)
			if err != nil {
				return nil, ErrDenied
			}
		}
	}
	c.Resource.ResourceID, c.Resource.Revision, c.ProfileRevision, c.LocalInstallationID, c.LocalInstallationRevision = resource, revision, revision, installation, revision
	c.OriginalSource = sourceRef
	p.Resource = c.Resource
	p.ProfileRevision, p.RevocationGeneration, p.State = revision, 1, domain.OwnedAgentCurrentActiveV1
	p.InstallationID, p.InstallationRevision, p.MutationID, p.InstalledManifest = installation, revision, mutation, sourceRef
	p.Locator.ResourceID = resource
	p.Substrate.LocalInstallationID, p.Substrate.LocalInstallationRevision = installation, revision
	p.Backend.BackendProfileDigest, err = c.Digest()
	if err != nil {
		return nil, ErrDenied
	}
	p.Locator.BackendProfileDigest = p.Backend.BackendProfileDigest
	p.Window.PublishedAt, p.UpdatedAt = now, now
	if c.ValidateForOwnedProfile(p) != nil || c.ValidateAt(now) != nil || p.ValidateInitial() != nil {
		return nil, sourceDenial("profile correlation")
	}
	search, err := acceptSearchEvidence(manifest, p, now)
	if err != nil {
		return nil, sourceDenial("search expenditure")
	}
	lifetime, cancel := context.WithDeadline(ctx, c.ExpiresAt)
	s := &Source{ctx: lifetime, cancel: cancel, key: &sourceKey{bytes: key}, config: c, profile: p, manifest: append([]byte(nil), raw...), bodies: manifest.Evidence, search: search, parent: parent}
	s.self = s
	if original != nil && parent == nil {
		digest, err := p.Digest()
		if err != nil || digest != original.Digest {
			s.Close()
			return nil, ErrDenied
		}
		r := cloneReceipt(*original)
		s.receipt = &r
		return s, nil
	}
	expires := now.Add(10 * time.Second)
	if p.Window.ExpiresAt.Before(expires) {
		expires = p.Window.ExpiresAt
	}
	if search != nil && search.end.Before(expires) {
		expires = search.end
	}
	g := ports.OwnedProfilePublicationGrantV1{Profile: p.Clone(), IssuedAt: now, ExpiresAt: expires, Authenticator: make([]byte, 32), LocalInstallation: &ports.OwnedLocalInstallationIntentV1{Profile: c.Clone()}}
	if parent != nil {
		retirement, err := idgen.New().NewID(ctx, ports.IDOwnedPublication)
		if err != nil {
			s.Close()
			return nil, ErrDenied
		}
		r := cloneReceipt(*original)
		r.Replayed = false
		g.LocalInstallation.Predecessor = &r
		g.LocalInstallation.RetirementMutationID = retirement
	}
	intent, err := g.IntentBytes()
	if err != nil {
		s.Close()
		return nil, ErrDenied
	}
	g.Authenticator = s.mac("publication", intent)
	s.grant = &g
	return s, nil
}

func validTerms(t InvoiceTermsV1, c domain.ConfiguredInferenceProfileV1, p domain.OwnedAgentProfileCurrentV1) bool {
	return t.Version == 1 && t.TenantID == c.TenantID && t.OwnerUserID == c.OwnerUserID && t.BaseURL == c.BaseURL && t.ModelVendorID == c.ModelVendorID && t.ModelID == c.ModelID && t.Currency == p.CostCeiling.Currency && t.Revision == p.CostCeiling.PriceRevision && t.OwnerSuppliesModel && t.OwnerSuppliesHost && t.ProviderInvoiceAmount == 0 && t.SubstrateInvoiceAmount == 0 && t.IssuedAt.Equal(c.IssuedAt) && t.ExpiresAt.Equal(c.ExpiresAt) && p.ProviderTermsDigest == c.OperatorTerms.Digest && p.SubstrateTermsDigest == c.OperatorTerms.Digest && p.ProviderPriceDigest == c.OperatorTerms.Digest && p.SubstratePriceDigest == c.OperatorTerms.Digest
}

func (s *Source) aliveLocked() bool {
	if s == nil || s.self != s || s.closed || s.key == nil {
		return false
	}
	// Context cancellation may be dispatched after its retained deadline.
	// Historical DATA authentication must not extend this original lifetime,
	// including when its parent supplied the shorter bound.
	deadline, bounded := s.ctx.Deadline()
	if s.ctx.Err() != nil || (bounded && !time.Now().Before(deadline)) {
		clear(s.key.bytes[:])
		s.closed = true
		return false
	}
	return true
}

func (s *Source) Publication() (ports.OwnedProfilePublicationGrantV1, error) {
	if s == nil {
		return ports.OwnedProfilePublicationGrantV1{}, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.grant == nil || !time.Now().Before(s.grant.ExpiresAt) {
		return ports.OwnedProfilePublicationGrantV1{}, ErrDenied
	}
	return s.grant.Clone(), nil
}

func (s *Source) VerifyOwnedProfilePublication(g ports.OwnedProfilePublicationGrantV1, at time.Time) error {
	if s == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.grant == nil || at.Before(s.grant.IssuedAt) || !at.Before(s.grant.ExpiresAt) {
		return ErrDenied
	}
	if s.parent != nil {
		s.parent.mu.Lock()
		alive := s.parent.aliveLocked()
		s.parent.mu.Unlock()
		if !alive {
			return ErrDenied
		}
	}
	raw, err := g.IntentBytes()
	expected, expectedErr := s.grant.IntentBytes()
	if err != nil || expectedErr != nil || !bytes.Equal(raw, expected) || !hmac.Equal(g.Authenticator, s.mac("publication", raw)) {
		return ErrDenied
	}
	return nil
}

// AcceptReceipt only binds historical storage output to this original intent;
// it does not certify a commit or current row. Every later Store effect still
// rereads the original exact active profile/generation/digest transactionally.
func (s *Source) AcceptReceipt(receipt ports.OwnedProfilePublicationReceiptV1) error {
	if s == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || !s.receiptMatchesLocked(receipt) {
		return ErrDenied
	}
	r := cloneReceipt(receipt)
	s.receipt = &r
	s.profile = r.Profile.Clone()
	return nil
}

func (s *Source) receiptMatchesLocked(receipt ports.OwnedProfilePublicationReceiptV1) bool {
	if receipt.Profile.ValidateInitial() != nil {
		return false
	}
	expected := s.profile.Clone()
	expected.UpdatedAt = receipt.Profile.UpdatedAt
	expected.Window.PublishedAt = receipt.Profile.Window.PublishedAt
	digest, err := expected.Digest()
	actual, actualErr := receipt.Profile.Digest()
	if err != nil || actualErr != nil || digest != receipt.Digest || actual != receipt.Digest || receipt.Profile.UpdatedAt.Before(s.profile.UpdatedAt) || !receipt.Profile.UpdatedAt.Equal(receipt.Profile.Window.PublishedAt) {
		return false
	}
	return true
}

func (s *Source) Configuration() (domain.ConfiguredInferenceProfileV1, error) {
	if s == nil {
		return domain.ConfiguredInferenceProfileV1{}, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() {
		return domain.ConfiguredInferenceProfileV1{}, ErrDenied
	}
	return s.config.Clone(), nil
}

// ExecutionLifetime returns this original keeper's retained cancellation signal,
// bounded by its original installed window and caller-parent lifetime. It is
// read-only lifetime data, not current authority, a grant or send permission.
// Close revokes this owned child without cancelling its caller or a shared server.
func (s *Source) ExecutionLifetime() (context.Context, error) {
	if s == nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() {
		return nil, ErrDenied
	}
	return s.ctx, nil
}

func (s *Source) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != s {
		return ErrDenied
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.key != nil {
		clear(s.key.bytes[:])
	}
	s.closed = true
	s.grant = nil
	s.receipt = nil
	s.search = nil
	s.manifest = nil
	s.bodies = nil
	return nil
}

func (s *Source) mac(kind string, raw []byte) []byte {
	h := hmac.New(sha256.New, s.key.bytes[:])
	h.Write([]byte("sessionless.owner-local." + kind + ".v1\x00"))
	h.Write([]byte(s.config.OriginalSource.Digest))
	h.Write([]byte{0})
	h.Write(raw)
	return h.Sum(nil)
}
func hash(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func cloneReceipt(r ports.OwnedProfilePublicationReceiptV1) ports.OwnedProfilePublicationReceiptV1 {
	r.Profile = r.Profile.Clone()
	return r
}
func hasBody(bodies []EvidenceBodyV1, ref domain.OwnedAgentAuthorityBodyRefV1) bool {
	for _, b := range bodies {
		if b.Reference == ref {
			return true
		}
	}
	return false
}
func hasExactBody(bodies []EvidenceBodyV1, ref domain.OwnedAgentAuthorityBodyRefV1, raw []byte) bool {
	for _, b := range bodies {
		if b.Reference == ref && bytes.Equal(b.Body, raw) {
			return true
		}
	}
	return false
}
func strictJSON(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(dst) != nil {
		return ErrDenied
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		return ErrDenied
	}
	encoded, err := json.Marshal(dst)
	if err != nil || !bytes.Equal(raw, encoded) {
		return ErrDenied
	}
	return nil
}

// Constant classifications only: never include source paths, input, key bytes,
// terms or provider details in diagnostic errors.
func sourceDenial(stage string) error {
	return fmt.Errorf("local operator: invalid %s: %w", stage, ErrDenied)
}
