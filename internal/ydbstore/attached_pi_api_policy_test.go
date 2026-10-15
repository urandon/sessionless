package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Synthetic catalog/SQL snapshots ONLY. This tests actual Store/browser/CAS
// code, not native YDB isolation or genuinely installed native/tariff evidence.
type piPolicyTestCatalog struct {
	selection ports.AttachedPiAPIPolicySelectionV1
	denied    bool
}

func (c *piPolicyTestCatalog) SelectAttachedPiAPIPolicy(ports.AttachedPiAPIPolicyLocatorV1, time.Time) (ports.AttachedPiAPIPolicySelectionV1, error) {
	if c.denied {
		return ports.AttachedPiAPIPolicySelectionV1{}, domain.ErrMembershipDenied
	}
	s := c.selection
	s.Evidence = s.Evidence.Clone()
	return s, nil
}

const piPolicyWebSessionSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2`

type piPolicySQLFixture struct {
	rows             map[string][]driver.Value
	expected         map[string][]driver.Value
	records, pending map[string][]driver.Value
	writes, commits  int
	unknownCommit    bool
	queries          []string
	extra            func(string, []driver.NamedValue) (driver.Rows, error)
}

func (f *piPolicySQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *piPolicySQLFixture) Driver() driver.Driver                        { return explanationFixtureDriver{} }
func (f *piPolicySQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("policy fixture forbids Prepare")
}
func (f *piPolicySQLFixture) Close() error { return nil }
func (f *piPolicySQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("policy fixture requires BeginTx")
}
func (f *piPolicySQLFixture) BeginTx(_ context.Context, o driver.TxOptions) (driver.Tx, error) {
	if o.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("requires serializable")
	}
	f.pending = make(map[string][]driver.Value)
	for k, v := range f.records {
		f.pending[k] = append([]driver.Value(nil), v...)
	}
	return piPolicyTestTx{f}, nil
}

type piPolicyTestTx struct{ f *piPolicySQLFixture }

func (tx piPolicyTestTx) Commit() error {
	tx.f.commits++
	tx.f.records = tx.f.pending
	tx.f.pending = nil
	if tx.f.unknownCommit {
		return errors.New("synthetic unknown commit")
	}
	return nil
}
func (tx piPolicyTestTx) Rollback() error { tx.f.pending = nil; return nil }
func (f *piPolicySQLFixture) QueryContext(_ context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	f.queries = append(f.queries, s)
	if want, ok := f.expected[s]; ok {
		if len(args) != len(want) {
			return nil, errors.New("foreign policy query arity")
		}
		for i, v := range want {
			if !reflect.DeepEqual(args[i].Value, v) {
				return nil, errors.New("foreign policy query selector")
			}
		}
	}
	if s == piPolicyReadSQL {
		if len(args) != 4 {
			return nil, errors.New("policy key arity")
		}
		row := f.records[args[3].Value.(string)]
		if f.pending != nil {
			row = f.pending[args[3].Value.(string)]
		}
		return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
	}
	if row, ok := f.rows[s]; ok {
		return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
	}
	if f.extra != nil {
		return f.extra(s, args)
	}
	return nil, errors.New("unexpected policy query")
}
func (f *piPolicySQLFixture) ExecContext(_ context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if !strings.Contains(s, " INTO attached_pi_api_policy_v1 ") || len(args) != 9 || f.pending == nil {
		return nil, errors.New("unexpected policy write")
	}
	key := args[3].Value.(string)
	if strings.HasPrefix(s, "INSERT") && f.pending[key] != nil {
		return nil, errors.New("immutable decision rewrite")
	}
	f.pending[key] = []driver.Value{args[4].Value, args[5].Value, args[6].Value, args[7].Value, args[8].Value}
	f.writes++
	return driver.RowsAffected(1), nil
}

func piPolicyTestSelection(t *testing.T, at time.Time) ports.AttachedPiAPIPolicySelectionV1 {
	t.Helper()
	d := strings.Repeat("a", 64)
	p := domain.AttachedPiSDKNativeProfileV1{Version: 1, ProfileID: domain.AttachedPiSDKProfileIDV1, NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1,
		Sources:  domain.AttachedPiSDKNativeSourcesV1{PiSourceRevision: "6c4f360264397c59801f6da2bdac13e3b1fcbe91", OpenAISDKVersion: "6.40.0", OpenAISDKIntegrity: "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==", NodeVersion: "synthetic-runtime"},
		Model:    domain.AttachedPiSDKNativeModelV1{ModelVendorID: "synthetic-vendor", ModelID: "synthetic-model", Endpoint: "https://synthetic.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024},
		Platform: domain.AttachedPiSDKNativePlatformV1{OS: "synthetic-os", Architecture: "synthetic-architecture"},
		Limits:   domain.AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 512 << 10, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 8 << 20, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10, SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 4096, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20, ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 512 << 10, ProcessStdoutBytes: 8 << 20, ProcessEvents: 2048, JSONDepth: 16, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10, ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(target).Elem()
		for i := 0; i < v.NumField(); i++ {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				v.Field(i).SetString(d)
			}
		}
	}
	backend, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.ProviderEvidenceScopeV1{TenantID: "pi-policy-tenant", OwnerUserID: "pi-policy-owner", Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "pi-policy-api", OwnerUserID: "pi-policy-owner", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}, Backend: backend}
	start, end := at.Add(-time.Minute), at.Add(time.Hour)
	e := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "synthetic-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, CatalogDigest: d}}, ObservedAt: start, ExpiresAt: end},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, PolicyID: "synthetic-route", Revision: 1, FallbackPolicy: domain.ProviderFallbackDenyV1, Routes: []domain.ProviderRouteV1{{BackendKind: domain.HarnessBackendPiV1, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, TransportKind: domain.ProviderTransportDirectAPIV1, TransportProvider: "synthetic-direct", UpstreamProviderID: "synthetic-upstream", EndpointID: "synthetic-endpoint", BillingKind: domain.ProviderBillingAPIAccountV1, BillingAuthority: scope.Resource.ResourceID}}, ObservedAt: start, ExpiresAt: end},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: p.Model.ModelContextTokens}, ToolCalling: domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceSupportedV1, ObservedAt: start, ExpiresAt: end},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceFalseV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceSupportedV1, Value: 1}, PolicyRevision: "synthetic-privacy", ObservedAt: start, ExpiresAt: end},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceSupportedV1, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, Currency: "USD", InputMicrounitsPerMTokens: 1000001, OutputMicrounitsPerMTokens: 2000001, ObservedAt: start, ExpiresAt: end},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "synthetic-policy", Revision: 1, DecisionOwner: "synthetic-only", EvidenceSource: "not-real-tariff", Verdict: domain.ProviderPolicyGoV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, ObservedAt: start, ExpiresAt: end}}
	cd, _ := e.Capability.Digest()
	pd, _ := e.Privacy.Digest()
	rd, _ := e.Route.Digest()
	price, _ := e.Price.Digest()
	e.Policy.CapabilityEvidenceDigest = string(cd)
	e.Policy.PrivacyEvidenceDigest = string(pd)
	e.Policy.RoutePolicyDigest = string(rd)
	e.Policy.PriceObservationDigest = string(price)
	policy, _ := e.Policy.Digest()
	terms := domain.AttachedPiAPIInvocationPolicyV1{Version: 1, Scope: scope, ModelVendorID: p.Model.ModelVendorID, ModelID: p.Model.ModelID, InputDataClass: domain.ProviderDataPrivateV1, EffectivePolicyDigest: string(policy), PriceObservationDigest: string(price), Currency: "USD", MaxContextTokens: 8192, MaxOutputTokens: 1024, MaxChargeMicrounits: 4098, NotBefore: start, ExpiresAt: end}
	if _, err := domain.ValidateAttachedPiAPITermsV1(p, e, terms, at); err != nil {
		t.Fatal("synthetic P/E/T setup:", err)
	}
	return ports.AttachedPiAPIPolicySelectionV1{SourceID: "synthetic-not-source-qualified", Profile: p, Evidence: e, Terms: terms}
}

// Shared package-test seam: root phase tests can add exact query rows/expected
// args or f.extra; no private production test exports or native service needed.
func newPiPolicyFixture(t *testing.T) (*Store, *piPolicySQLFixture, ports.AttachedPiAPIPolicySelectionV1, ports.ConfirmAttachedPiAPIPolicyV1, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	s := piPolicyTestSelection(t, at)
	l := ports.AttachedPiAPIPolicyLocatorV1{TenantID: s.Terms.Scope.TenantID, OwnerUserID: s.Terms.Scope.OwnerUserID, ResourceID: s.Terms.Scope.Resource.ResourceID, ModelVendorID: s.Terms.ModelVendorID, ModelID: s.Terms.ModelID, InputDataClass: s.Terms.InputDataClass}
	session := domain.WebSession{SessionDigest: domain.DigestSecret("synthetic-session"), CSRFTokenDigest: domain.DigestSecret("synthetic-csrf"), UserID: l.OwnerUserID, ActiveTenantID: l.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "synthetic-owner"}, MembershipSecurityVersion: 3, IssuedAt: at.Add(-time.Minute), LastSeenAt: at.Add(-time.Minute), IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(2 * time.Hour)}
	member := domain.TenantMembership{TenantID: l.TenantID, UserID: l.OwnerUserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 3, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	sb, _ := json.Marshal(session)
	mb, _ := json.Marshal(member)
	ref, err := domain.NewCredentialSecretRef("synthetic-never-read-reference")
	if err != nil {
		t.Fatal(err)
	}
	credential := domain.ProviderCredentialBindingV1{Version: 1, TenantID: l.TenantID, OwnerUserID: l.OwnerUserID, ResourceKind: domain.ProviderResourceAPIAccountV1, ResourceID: l.ResourceID, ResourceRevision: 1, CredentialGeneration: 1, CandidateMutationID: "synthetic-ingestion-not-consent", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.CredentialFingerprint(strings.Repeat("b", 64)), UpdatedAt: at.Add(-time.Minute)}
	cb, _ := json.Marshal(credential)
	f := &piPolicySQLFixture{rows: map[string][]driver.Value{`SELECT CurrentUtcTimestamp()`: {at}, piPolicyWebSessionSQL: {string(sb)}, computeChoiceMembershipSQL: {string(mb), string(member.Role), string(member.Status), int64(3)}, providerCredentialReadSQL: {int64(1), int64(1), credential.CandidateMutationID, string(credential.State), ref.StorageValue(), string(credential.SecretFingerprint), credential.UpdatedAt, string(cb)}}, expected: map[string][]driver.Value{}, records: map[string][]driver.Value{}}
	db := sql.OpenDB(f)
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	store := &Store{db: db, attachedPiAPIPolicyCatalog: &piPolicyTestCatalog{selection: s}}
	td, _ := s.Terms.Digest()
	request := ports.ConfirmAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: ports.ReadAttachedPiAPIPolicyV1{Locator: l, WebSessionDigest: session.SessionDigest}, CSRFTokenDigest: session.CSRFTokenDigest, TermsDigest: td, MutationID: "synthetic-confirmation", ExpectedGeneration: 0}
	return store, f, s, request, at
}

func TestAttachedPiAPIPolicyActualOwnerDecisionAndNonrevivingReplay(t *testing.T) {
	s, f, _, r, _ := newPiPolicyFixture(t)
	offer, err := s.ReadAttachedPiAPIPolicy(t.Context(), r.ReadAttachedPiAPIPolicyV1)
	if err != nil || offer.TermsDigest != r.TermsDigest || offer.CurrentGeneration != 0 {
		t.Fatal("server-selected offer:", err)
	}
	first, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err != nil || first.Generation != 1 || first.Replayed || first.Receipt.IssuerID != ports.AttachedPiAPIOwnerPolicyIssuerV1 || first.Receipt.MembershipSecurityVersion != 3 || first.Receipt.DecisionID == string(r.MutationID) {
		t.Fatal("actual owner confirmation:", err)
	}
	if f.writes != 2 || len(f.records) != 2 {
		t.Fatalf("decision/head writes=%d rows=%d want2,2", f.writes, len(f.records))
	}
	revoke := ports.RevokeAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: r.ReadAttachedPiAPIPolicyV1, CSRFTokenDigest: r.CSRFTokenDigest, ExpectedGeneration: 1, ExpectedMapping: first.MappingDigest}
	if err := s.RevokeAttachedPiAPIPolicy(t.Context(), revoke); err != nil {
		t.Fatal(err)
	}
	if err := s.RevokeAttachedPiAPIPolicy(t.Context(), revoke); err != nil {
		t.Fatal("exact revoke replay:", err)
	}
	before := f.writes
	replayed, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err != nil || !replayed.Replayed || replayed.Receipt != first.Receipt || f.writes != before {
		t.Fatal("historical confirmation rewrote/revived head:", err)
	}
	var head piPolicyHead
	if piPolicyDecode(f.records["head"][0].(string), &head) != nil || head.State != "revoked" || head.Generation != 2 {
		t.Fatal("tombstone lost")
	}
	r.MutationID = "synthetic-explicit-reauthorize"
	r.ExpectedGeneration = 2
	next, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err != nil || next.Generation != 3 || next.MappingDigest == first.MappingDigest {
		t.Fatal("new explicit owner decision did not replace M:", err)
	}
}

func TestAttachedPiAPIPolicyAuthorityFailuresBeforeWrites(t *testing.T) {
	for _, mode := range []string{"missing_catalog", "denied_catalog", "csrf", "foreign_owner", "terms", "stale_cas", "membership_version", "rotated_resource", "future_resource", "revoked_session"} {
		t.Run(mode, func(t *testing.T) {
			s, f, _, r, at := newPiPolicyFixture(t)
			switch mode {
			case "missing_catalog":
				s.attachedPiAPIPolicyCatalog = nil
			case "denied_catalog":
				s.attachedPiAPIPolicyCatalog.(*piPolicyTestCatalog).denied = true
			case "csrf":
				r.CSRFTokenDigest = domain.DigestSecret("foreign")
			case "foreign_owner":
				r.Locator.OwnerUserID = "foreign-owner"
			case "terms":
				r.TermsDigest = domain.ProviderEvidenceDigestV1(strings.Repeat("c", 64))
			case "stale_cas":
				r.ExpectedGeneration = 2
			case "membership_version":
				f.rows[computeChoiceMembershipSQL][3] = uint64(4)
			case "rotated_resource":
				f.rows[providerCredentialReadSQL][1] = uint64(2)
			case "future_resource":
				f.rows[providerCredentialReadSQL][6] = at.Add(time.Second)
			case "revoked_session":
				var session domain.WebSession
				_ = json.Unmarshal([]byte(f.rows[piPolicyWebSessionSQL][0].(string)), &session)
				session.RevokedAt = &at
				b, _ := json.Marshal(session)
				f.rows[piPolicyWebSessionSQL][0] = string(b)
			}
			got, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
			if err == nil || got.Receipt.DecisionID != "" || f.writes != 0 {
				t.Fatalf("mode=%s unauthorized decision writes=%d err=%v", mode, f.writes, err)
			}
		})
	}
}

func TestAttachedPiAPIPolicyUnknownCommitReturnsNoReceiptAndRecoversHistory(t *testing.T) {
	s, f, _, r, _ := newPiPolicyFixture(t)
	f.unknownCommit = true
	got, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err == nil || got.Receipt.DecisionID != "" {
		t.Fatal("speculative receipt escaped unknown commit")
	}
	f.unknownCommit = false
	before := f.writes
	got, err = s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err != nil || !got.Replayed || f.writes != before {
		t.Fatal("unknown commit recovery reissued decision:", err)
	}
}

func TestAttachedPiAPIPolicyStrictImmutableRecordRejectsForgedReceipt(t *testing.T) {
	s, f, _, r, at := newPiPolicyFixture(t)
	first, err := s.ConfirmAttachedPiAPIPolicy(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := piDecisionKey(r.MutationID)
	var d piPolicyDecision
	if piPolicyDecode(f.records[key][0].(string), &d) != nil {
		t.Fatal("fixture record")
	}
	d.Decision.Receipt.IssuerID = "forged-issuer"
	b, _ := json.Marshal(d)
	f.records[key][0] = string(b)
	if _, err := readPiPolicyDecision(t.Context(), s.db, r.Locator, key, at); err == nil {
		t.Fatal("forged issuer/hash-only receipt accepted")
	}
	if first.MappingDigest == "" {
		t.Fatal("missing original control")
	}
}
