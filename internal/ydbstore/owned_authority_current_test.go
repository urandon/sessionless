package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

// This connector exercises the actual adapter and SDK retry callbacks, NOT
// native YDB isolation, source publication, live funding or query plans.
type ownedCurrentSQLFixture struct {
	*ratedSQLFixture
	profileArgs  []driver.Value
	profileRow   []driver.Value
	spendingArgs [4][]driver.Value
	spendingRows [4][]driver.Value
	at           time.Time
	execCalls    int
}

func (f *ownedCurrentSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *ownedCurrentSQLFixture) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.statement(ctx, statement, args); err != nil {
		return nil, err
	}
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	var row []driver.Value
	switch statement {
	case `SELECT CurrentUtcTimestamp()`:
		if len(args) != 0 {
			return nil, errors.New("clock arguments forbidden")
		}
		row = []driver.Value{f.at}
	case ownedProfileCurrentSQL:
		if !reflect.DeepEqual(values, f.profileArgs) {
			return nil, errors.New("foreign profile lookup")
		}
		row = f.profileRow
	case ownedSpendingCurrentSQL:
		for i := range f.spendingArgs {
			if reflect.DeepEqual(values, f.spendingArgs[i]) {
				row = f.spendingRows[i]
				break
			}
		}
		if row == nil {
			known := false
			for i := range f.spendingArgs {
				known = known || reflect.DeepEqual(values, f.spendingArgs[i])
			}
			if !known {
				return nil, errors.New("foreign spending lookup")
			}
		}
	default:
		return nil, errors.New("non-manifest query forbidden")
	}
	row = append([]driver.Value(nil), row...)
	// Emulate the actual cap-plus-one projections, rather than expecting a full
	// oversized record to be transferred by the production SQL.
	for i, value := range row {
		if text, ok := value.(string); ok {
			cap := 161
			if i == 0 {
				cap = ownedAuthorityHeadBytes + 1
			}
			if i == 3 && statement == ownedSpendingCurrentSQL || i == 4 && statement == ownedProfileCurrentSQL {
				cap = 17
			}
			if i == 4 && statement == ownedSpendingCurrentSQL || i == 5 && statement == ownedProfileCurrentSQL {
				cap = 65
			}
			if len(text) > cap {
				row[i] = text[:cap]
			}
		}
	}
	return &explanationFixtureRows{values: row}, nil
}
func (f *ownedCurrentSQLFixture) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	f.execCalls++
	return nil, errors.New("reader cannot publish, debit or activate")
}

type ownedReaderFixture struct {
	store    *Store
	driver   *ownedCurrentSQLFixture
	profile  domain.OwnedAgentProfileCurrentV1
	spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	ref      domain.OwnedAgentProfileReferenceV1
	refs     [4]ownedSpendingReference
	scope    ownedCurrentScope
}

func newOwnedReaderFixture(t *testing.T) *ownedReaderFixture {
	t.Helper()
	at := time.Date(2026, 10, 11, 3, 0, 0, 1000, time.UTC)
	return newOwnedReaderFixtureAt(t, at, "")
}

func newOwnedReaderFixtureAt(t *testing.T, at time.Time, nonce string) *ownedReaderFixture {
	t.Helper()
	f := &ownedReaderFixture{driver: &ownedCurrentSQLFixture{ratedSQLFixture: &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}, at: at}}
	f.scope = ownedCurrentScope{tenant: domain.TenantID("tenant-189" + nonce), owner: domain.UserID("owner-189" + nonce), run: domain.RunID("run-189" + nonce), session: domain.SessionID("session-189" + nonce), attempt: domain.AttemptID("attempt-189" + nonce)}
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2(f.scope.tenant, f.scope.owner, f.scope.run, f.scope.attempt, "fixture-connection", at)
	if err != nil {
		t.Fatal(err)
	}
	cost := a.AdmissionCostCeiling.Clone()
	mcpGrant, searchGrant := strings.Repeat("3", 64), strings.Repeat("4", 64)
	for _, v := range []struct {
		kind   domain.ExternalServiceKindV1
		slot   **domain.ExternalServiceCostCeilingV1
		grant  string
		amount uint64
	}{
		{domain.ExternalServiceMCPToolV1, &cost.MCPToolCost, mcpGrant, 30}, {domain.ExternalServiceWebSearchV1, &cost.WebSearchCost, searchGrant, 40},
	} {
		component := &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: v.kind, TenantID: f.scope.tenant, OwnerUserID: f.scope.owner, BillingOwnerUserID: "fixture-payer", ResourceID: string(v.kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(v.kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: v.grant, Currency: cost.Currency, PriceRevision: "scripted-price.v1", PriceObservedAt: cost.PriceObservedAt, PriceExpiresAt: cost.PriceExpiresAt, PriceEvidenceDigest: strings.Repeat("8", 64), TermsEvidenceDigest: strings.Repeat("9", 64), PriceState: domain.ExternalServicePriceKnownPaidV1, MaxAmountMicrounits: new(uint64)}
		*component.MaxAmountMicrounits = v.amount
		*v.slot = component
		*cost.MaxTotalAmountMicrounits += v.amount
	}
	substrate := a.SubstrateBinding
	substrate.AdmissionCostCeilingDigest, err = cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	digest := strings.Repeat("a", 64)
	f.profile = domain.OwnedAgentProfileCurrentV1{
		Version: 1, Locator: domain.OwnedAgentProfileLocatorV1{TenantID: f.scope.tenant, OwnerUserID: f.scope.owner, Profile: domain.AgentLoopOwnedPublicV1, BackendProfileDigest: a.HarnessBinding.Backend.BackendProfileDigest, ResourceKind: a.HarnessBinding.Resource.Kind, ResourceID: a.HarnessBinding.Resource.ResourceID, ModelVendorID: "fixture-vendor", ModelID: a.HarnessBinding.ModelID, InputDataClass: domain.ProviderDataPublicV1},
		ProfileRevision: 1, RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Backend: a.HarnessBinding.Backend, Resource: a.HarnessBinding.Resource, Substrate: substrate, InstallationID: "fixture-installation", InstallationRevision: 1,
		InstalledManifest: domain.OwnedAgentAuthorityBodyRefV1{Ref: "scripted-manifest", Digest: digest, Bytes: 123}, ReviewReceipt: domain.OwnedAgentAuthorityBodyRefV1{Ref: "scripted-review", Digest: strings.Repeat("b", 64), Bytes: 456}, ProviderCatalogDigest: digest, ProviderRouteDigest: digest, CapabilityEvidenceDigest: digest, PrivacyPolicyDigest: digest, EffectivePolicyDigest: digest, ProviderPriceDigest: digest, ProviderTermsDigest: digest, SubstratePriceDigest: strings.Repeat("c", 64), SubstrateTermsDigest: strings.Repeat("d", 64), SubstratePriceState: domain.ExternalServicePriceKnownFreeV1, BillingOwnerUserID: "fixture-payer", InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "fixture-estimator.v1", CalibrationDigest: digest, InstructionVersion: "fixture-instructions.v1", CompactionPolicyDigest: digest, MCPToolGrantDigest: mcpGrant, WebSearchGrantDigest: searchGrant,
		Limits:      domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2, MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384, MaxActiveDuration: 120 * time.Second, MaxNetworkOperationDuration: 30 * time.Second, MaxCleanupDuration: 30 * time.Second, MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10, MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20},
		CostCeiling: cost, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: at.Add(time.Hour)}, UpdatedAt: at, MutationID: "scripted-installation",
	}
	if err := f.profile.ValidateAt(at); err != nil {
		t.Fatalf("scripted row shape (not approval): %v", err)
	}
	profileDigest, err := f.profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.ref = domain.OwnedAgentProfileReferenceV1{Locator: f.profile.Locator, RevocationGeneration: 1, Digest: profileDigest}
	p := f.profile
	for i, kind := range ownedSpendingKinds {
		v := domain.AttemptSpendingAuthorizationCurrentV1{Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: f.scope.tenant, AttemptID: f.scope.attempt, ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID}, RunID: f.scope.run, SessionID: f.scope.session, OwnerUserID: f.scope.owner, SourceKind: domain.AttemptSpendingFixtureSourceV1, SourceInstallationID: p.InstallationID, SourceInstallationRevision: p.InstallationRevision, SourceProfile: f.ref, ResourceID: p.Resource.ResourceID, ResourceRevision: p.Resource.Revision, ResourceGeneration: p.Resource.CredentialGeneration, ProfileID: string(p.Locator.Profile), ProfileRevision: p.ProfileRevision, ScopedGrantDigest: p.EffectivePolicyDigest, Currency: p.CostCeiling.Currency, PriceRevision: p.CostCeiling.PriceRevision, PriceEvidenceDigest: p.ProviderPriceDigest, TermsEvidenceDigest: p.ProviderTermsDigest, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: new(uint64), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Window: p.Window, UpdatedAt: at, MutationID: "scripted-original-attempt"}
		if kind == domain.AttemptSpendingSubstrateV1 {
			v.ResourceID, v.ResourceRevision, v.ResourceGeneration = p.Substrate.ProfileID, p.Substrate.ProfileRevision, p.RevocationGeneration
			v.ProfileID, v.ProfileRevision = p.Substrate.ProfileID, p.Substrate.ProfileRevision
			v.ScopedGrantDigest, v.PriceEvidenceDigest, v.TermsEvidenceDigest = p.Substrate.ProfileDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest
		} else if kind == domain.AttemptSpendingMCPToolV1 || kind == domain.AttemptSpendingWebSearchV1 {
			service := p.CostCeiling.MCPToolCost
			if kind == domain.AttemptSpendingWebSearchV1 {
				service = p.CostCeiling.WebSearchCost
			}
			v.ResourceID, v.ResourceRevision, v.ResourceGeneration, v.ProfileID, v.ProfileRevision = service.ResourceID, service.ResourceRevision, service.RevocationGeneration, service.ProfileID, service.ProfileRevision
			v.ScopedGrantDigest, v.PriceRevision, v.PriceEvidenceDigest, v.TermsEvidenceDigest, v.PriceState = service.ScopedGrantDigest, service.PriceRevision, service.PriceEvidenceDigest, service.TermsEvidenceDigest, service.PriceState
			*v.MaxAmountMicrounits = *service.MaxAmountMicrounits
		}
		bindReaderSpendingSource(t, &v)
		if err := v.ValidateAgainstProfile(p, at); err != nil {
			t.Fatal(err)
		}
		f.spending[i] = v
		digest, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		f.refs[i] = ownedSpendingReference{locator: v.Locator, generation: v.RevocationGeneration, digest: digest}
	}
	f.sync(t)
	db := sql.OpenDB(f.driver)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store = &Store{db: db}
	return f
}

func bindReaderSpendingSource(t *testing.T, v *domain.AttemptSpendingAuthorizationCurrentV1) {
	t.Helper()
	raw, err := v.SourceCanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	v.SourceAuthorization = domain.OwnedAgentAuthorityBodyRefV1{Ref: "scripted-source", Digest: hex.EncodeToString(hash[:]), Bytes: uint64(len(raw))}
}

func (f *ownedReaderFixture) sync(t *testing.T) {
	t.Helper()
	p := f.profile
	l := f.ref.Locator
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	f.driver.profileArgs = []driver.Value{string(l.TenantID), string(l.OwnerUserID), string(l.Profile), l.BackendProfileDigest, string(l.ResourceKind), l.ResourceID, l.ModelVendorID, l.ModelID, string(l.InputDataClass)}
	f.driver.profileRow = []driver.Value{string(raw), int64(p.Version), int64(p.ProfileRevision), int64(p.RevocationGeneration), string(p.State), digest, p.InstallationID, int64(p.InstallationRevision), p.Window.PublishedAt, p.Window.ExpiresAt, p.UpdatedAt}
	for i, v := range f.spending {
		raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		l := f.refs[i].locator
		f.driver.spendingArgs[i] = []driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID)}
		f.driver.spendingRows[i] = []driver.Value{string(raw), int64(v.Version), int64(v.RevocationGeneration), string(v.State), digest, string(v.RunID), string(v.SessionID), string(v.OwnerUserID), v.Window.PublishedAt, v.Window.ExpiresAt, v.UpdatedAt}
	}
}

func (f *ownedReaderFixture) read() (ownedCurrentSnapshot, error) {
	var pending ownedCurrentSnapshot
	err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		pending = ownedCurrentSnapshot{}
		current, err := readOwnedCurrentSnapshot(ctx, q, f.scope, f.ref, f.refs)
		if err == nil {
			pending = current
		}
		return err
	})
	if err != nil {
		return ownedCurrentSnapshot{}, err
	}
	return pending, nil
}

func TestOwnedCurrentReaderCorrelatesAllComponentsInOneTransaction(t *testing.T) {
	f := newOwnedReaderFixture(t)
	got, err := f.read()
	if err != nil || !reflect.DeepEqual(got.profile, f.profile) || !reflect.DeepEqual(got.spending, f.spending) {
		t.Fatalf("scripted row correlation=%v, error=%v", got.profile.Locator, err)
	}
	if len(f.driver.statements) != 6 || f.driver.commits != 1 || f.driver.writes != 0 || f.driver.deadlineMissing || !reflect.DeepEqual(f.driver.isolations, []driver.IsolationLevel{driver.IsolationLevel(sql.LevelSerializable)}) {
		t.Fatalf("statements=%d commits=%d writes=%d deadlineMissing=%t isolation=%v", len(f.driver.statements), f.driver.commits, f.driver.writes, f.driver.deadlineMissing, f.driver.isolations)
	}
}

func TestOwnedCurrentReaderDeniesWithoutPartialValues(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *ownedReaderFixture)
	}{
		{"missing_profile", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow = nil }},
		{"missing_search", func(_ *testing.T, f *ownedReaderFixture) { f.driver.spendingRows[3] = nil }},
		{"malformed_profile", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[0] = "{" }},
		{"duplicate_profile", func(_ *testing.T, f *ownedReaderFixture) {
			f.driver.profileRow[0] = strings.Replace(f.driver.profileRow[0].(string), `"version":1`, `"version":1,"version":1`, 1)
		}},
		{"oversized_profile", func(_ *testing.T, f *ownedReaderFixture) {
			f.driver.profileRow[0] = strings.Repeat("x", ownedAuthorityHeadBytes+100)
		}},
		{"oversized_spending", func(_ *testing.T, f *ownedReaderFixture) {
			f.driver.spendingRows[2][0] = strings.Repeat("x", ownedAuthorityHeadBytes+100)
		}},
		{"oversized_scalar", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[6] = strings.Repeat("x", 161) }},
		{"profile_scalar_generation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[3] = int64(2) }},
		{"integral_float_generation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[3] = float64(1) }},
		{"string_generation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[3] = "1" }},
		{"negative_generation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[3] = int64(-1) }},
		{"profile_scalar_state", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[4] = "revoked" }},
		{"profile_scalar_digest", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[5] = strings.Repeat("f", 64) }},
		{"profile_scalar_installation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[6] = "different-installation" }},
		{"profile_scalar_time", func(_ *testing.T, f *ownedReaderFixture) { f.driver.profileRow[9] = f.driver.at.Add(time.Minute) }},
		{"spending_scalar_owner", func(_ *testing.T, f *ownedReaderFixture) { f.driver.spendingRows[0][7] = "another-owner" }},
		{"spending_scalar_generation", func(_ *testing.T, f *ownedReaderFixture) { f.driver.spendingRows[1][2] = int64(2) }},
		{"spending_scalar_digest", func(_ *testing.T, f *ownedReaderFixture) { f.driver.spendingRows[2][4] = strings.Repeat("f", 64) }},
		{"foreign_scope", func(_ *testing.T, f *ownedReaderFixture) { f.scope.session = "other-session" }},
		{"foreign_locator", func(_ *testing.T, f *ownedReaderFixture) { f.ref.Locator.ModelID = "other-model" }},
		{"stale_profile_reference", func(_ *testing.T, f *ownedReaderFixture) { f.ref.RevocationGeneration++ }},
		{"stale_spending_reference", func(_ *testing.T, f *ownedReaderFixture) { f.refs[0].generation++ }},
		{"changed_original_digest", func(_ *testing.T, f *ownedReaderFixture) { f.refs[3].digest = strings.Repeat("f", 64) }},
		{"duplicate_component", func(_ *testing.T, f *ownedReaderFixture) { f.refs[3] = f.refs[2] }},
		{"wrong_payer", func(_ *testing.T, f *ownedReaderFixture) { f.refs[3].locator.BillingOwnerUserID = "another-payer" }},
		{"exclusive_expiry", func(_ *testing.T, f *ownedReaderFixture) { f.driver.at = f.profile.Window.ExpiresAt }},
		{"future_publication", func(_ *testing.T, f *ownedReaderFixture) {
			f.driver.at = f.profile.Window.PublishedAt.Add(-time.Microsecond)
		}},
		{"invalid_db_precision", func(_ *testing.T, f *ownedReaderFixture) { f.driver.at = f.driver.at.Add(time.Nanosecond) }},
		{"coherent_profile_revoke", func(t *testing.T, f *ownedReaderFixture) {
			f.profile.State = domain.OwnedAgentCurrentRevokedV1
			f.profile.RevocationGeneration++
			f.sync(t)
		}},
		{"coherent_spending_revoke", func(t *testing.T, f *ownedReaderFixture) {
			f.spending[3].State = domain.OwnedAgentCurrentRevokedV1
			f.spending[3].RevocationGeneration++
			f.sync(t)
		}},
		{"changed_source_generation", func(t *testing.T, f *ownedReaderFixture) {
			f.spending[1].SourceProfile.RevocationGeneration++
			bindReaderSpendingSource(t, &f.spending[1])
			f.sync(t)
			f.refs[1].digest, _ = f.spending[1].Digest()
		}},
		{"unknown_search_price", func(t *testing.T, f *ownedReaderFixture) {
			f.spending[3].PriceState = domain.ExternalServicePriceUnknownV1
			f.spending[3].MaxAmountMicrounits = nil
			bindReaderSpendingSource(t, &f.spending[3])
			f.sync(t)
			f.refs[3].digest, _ = f.spending[3].Digest()
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newOwnedReaderFixture(t)
			c.change(t, f)
			got, err := f.read()
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || f.driver.commits != 0 || f.driver.writes != 0 {
				t.Fatalf("case=%s err=%v partial=%t commits=%d writes=%d", c.name, err, !reflect.DeepEqual(got, ownedCurrentSnapshot{}), f.driver.commits, f.driver.writes)
			}
		})
	}
}

func TestOwnedCurrentReaderRetryDiscardsRevokedAndAmbiguousSnapshots(t *testing.T) {
	for _, mode := range []string{"rollback", "ambiguous", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedReaderFixture(t)
			f.driver.commitModes = []string{mode}
			f.driver.afterCommit = func() {
				f.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.profile.RevocationGeneration++
				f.sync(t)
			}
			got, err := f.read()
			wantStatements := 8
			if mode == "unknown" {
				wantStatements = 6
			}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || len(f.driver.statements) != wantStatements || f.driver.commits != 1 {
				t.Fatalf("mode=%s err=%v statements=%d commits=%d partial=%t", mode, err, len(f.driver.statements), f.driver.commits, !reflect.DeepEqual(got, ownedCurrentSnapshot{}))
			}
		})
	}
}

func TestOwnedCurrentReaderEveryPhysicalScalarMustCorrelate(t *testing.T) {
	for _, table := range []string{"profile", "spending"} {
		for column := 1; column <= 10; column++ {
			t.Run(table+"/"+[]string{"record", "version", "revision_or_generation", "generation_or_state", "state_or_digest", "digest_or_run", "installation_or_session", "installation_revision_or_owner", "published", "expires", "updated"}[column], func(t *testing.T) {
				f := newOwnedReaderFixture(t)
				row := f.driver.profileRow
				if table == "spending" {
					row = f.driver.spendingRows[2]
				}
				switch v := row[column].(type) {
				case string:
					row[column] = v + "x"
				case int64:
					row[column] = v + 1
				case time.Time:
					row[column] = v.Add(time.Microsecond)
				default:
					t.Fatalf("uncovered physical column=%d type=%T", column, v)
				}
				got, err := f.read()
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || f.driver.commits != 0 {
					t.Fatalf("table=%s column=%d err=%v commits=%d", table, column, err, f.driver.commits)
				}
			})
		}
	}
}

func TestOwnedCurrentReaderAccountsColumnsFollowingMalformedScalar(t *testing.T) {
	f := newOwnedReaderFixture(t)
	f.driver.profileRow[3] = nil
	f.driver.profileRow[6] = strings.Repeat("x", 160)
	want := uint64(8 + len(f.driver.profileRow[0].(string)) + 8 + 8 + len(f.driver.profileRow[4].(string)) + len(f.driver.profileRow[5].(string)) + 160 + 8 + 3*8)
	var returned uint64
	err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, err := readOwnedCurrentSnapshot(ctx, q, f.scope, f.ref, f.refs)
		returned = q.budget.bytes
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) || returned != want || f.driver.commits != 0 {
		t.Fatalf("malformed scalar err=%v returned=%d want=%d commits=%d", err, returned, want, f.driver.commits)
	}
}

func TestOwnedCurrentReaderOriginalScopeAndSourceAxes(t *testing.T) {
	for _, axis := range []string{"tenant", "owner", "run", "session", "attempt", "installation", "installation_revision", "source_digest", "resource", "profile", "grant", "currency", "price", "terms", "window"} {
		t.Run(axis, func(t *testing.T) {
			f := newOwnedReaderFixture(t)
			v := &f.spending[3]
			switch axis {
			case "tenant":
				f.scope.tenant = "another-tenant"
			case "owner":
				f.scope.owner = "another-owner"
			case "run":
				v.RunID = "another-run"
			case "session":
				v.SessionID = "another-session"
			case "attempt":
				f.scope.attempt = "another-attempt"
			case "installation":
				v.SourceInstallationID = "another-installation"
			case "installation_revision":
				v.SourceInstallationRevision++
			case "source_digest":
				v.SourceProfile.Digest = strings.Repeat("f", 64)
			case "resource":
				v.ResourceID = "another-resource"
			case "profile":
				v.ProfileID = "another-profile"
			case "grant":
				v.ScopedGrantDigest = strings.Repeat("f", 64)
			case "currency":
				v.Currency = "EUR"
			case "price":
				v.PriceEvidenceDigest = strings.Repeat("f", 64)
			case "terms":
				v.TermsEvidenceDigest = strings.Repeat("f", 64)
			case "window":
				v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Microsecond)
			}
			bindReaderSpendingSource(t, v)
			f.sync(t)
			f.refs[3].digest, _ = v.Digest()
			got, err := f.read()
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || f.driver.commits != 0 {
				t.Fatalf("axis=%s err=%v commits=%d", axis, err, f.driver.commits)
			}
		})
	}
}

func TestOwnedAuthorityBudgetLimitsAndStickyFailure(t *testing.T) {
	t.Run("exact_96_statements_across_three_callbacks", func(t *testing.T) {
		b := &ownedAuthorityBudget{}
		for i := 0; i < 3; i++ {
			id, err := b.begin()
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 32; j++ {
				if err := b.statement(context.Background(), id); err != nil {
					t.Fatal(err)
				}
			}
			b.finish(id)
		}
		if b.statements != 96 {
			t.Fatal(b.statements)
		}
		if _, err := b.begin(); err == nil {
			t.Fatal("fourth callback accepted")
		}
	})
	t.Run("limit_plus_one_prevents_driver_statement", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			for i := 0; i < 32; i++ {
				var at time.Time
				_ = q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at)
			}
			return nil
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || len(f.driver.statements) != 32 || f.driver.commits != 0 {
			t.Fatalf("err=%v statements=%d commits=%d", err, len(f.driver.statements), f.driver.commits)
		}
	})
	t.Run("metadata_exact_and_plus_one", func(t *testing.T) {
		b := &ownedAuthorityBudget{}
		id, _ := b.begin()
		if b.materialized(id, ownedAuthorityMaxBytes) != nil || b.materialized(id, 1) == nil || b.materialized(id, 0) == nil {
			t.Fatal("metadata boundary/sticky failure")
		}
	})
	t.Run("exec_limit_before_driver_and_ignored_error", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			for i := 0; i < 31; i++ {
				var at time.Time
				if err := q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
					return err
				}
			}
			_, _ = q.ExecContext(ctx, "forbidden_write")
			return nil
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || f.driver.execCalls != 0 || f.driver.commits != 0 || len(f.driver.statements) != 32 {
			t.Fatalf("err=%v driverExec=%d commits=%d statements=%d", err, f.driver.execCalls, f.driver.commits, len(f.driver.statements))
		}
	})
	t.Run("metadata_overflow", func(t *testing.T) {
		b := &ownedAuthorityBudget{}
		id, _ := b.begin()
		if b.materialized(id, math.MaxUint64) == nil || b.bytes != 0 {
			t.Fatal("overflow accepted")
		}
	})
	t.Run("stale_callback", func(t *testing.T) {
		b := &ownedAuthorityBudget{}
		old, _ := b.begin()
		b.finish(old)
		_, _ = b.begin()
		if b.statement(context.Background(), old) == nil {
			t.Fatal("stale callback accepted")
		}
	})
	t.Run("retry_cap_on_actual_sdk", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		f.driver.commitModes = []string{"rollback", "rollback", "rollback", "rollback"}
		got, err := f.read()
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ownedCurrentSnapshot{}) || f.driver.commits != 3 || len(f.driver.isolations) != 3 || len(f.driver.statements) != 18 {
			t.Fatalf("err=%v commits=%d callbacks=%d statements=%d", err, f.driver.commits, len(f.driver.isolations), len(f.driver.statements))
		}
	})
	t.Run("exact_statement_boundary_across_actual_retries", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		f.driver.commitModes = []string{"rollback", "rollback"}
		var b *ownedAuthorityBudget
		err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			b = q.budget
			for i := 0; i < 31; i++ {
				var at time.Time
				if err := q.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
					return err
				}
				if err := q.budget.materialized(q.callback, 8); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil || b.statements != 96 || b.callbacks != 3 || b.bytes != 96*8 || len(f.driver.statements) != 96 || f.driver.commits != 3 {
			t.Fatalf("err=%v total=%d callbacks=%d bytes=%d driver=%d commits=%d", err, b.statements, b.callbacks, b.bytes, len(f.driver.statements), f.driver.commits)
		}
	})
	t.Run("bytes_accumulate_across_retries", func(t *testing.T) {
		f := newOwnedReaderFixture(t)
		f.driver.commitModes = []string{"rollback"}
		var budget *ownedAuthorityBudget
		err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			budget = q.budget
			return q.budget.materialized(q.callback, ownedAuthorityMaxBytes/2)
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || budget.bytes <= ownedAuthorityMaxBytes/2 || f.driver.commits != 1 || len(f.driver.statements) != 2 {
			t.Fatalf("err=%v bytes=%d commits=%d statements=%d", err, budget.bytes, f.driver.commits, len(f.driver.statements))
		}
	})
	t.Run("whole_call_deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newOwnedReaderFixture(t)
			err := f.store.ownedAuthorityTx(context.Background(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				deadline, ok := ctx.Deadline()
				if !ok || deadline.Sub(time.Now()) != ownedAuthorityDeadline {
					t.Fatalf("deadline=%v ok=%t", deadline, ok)
				}
				time.Sleep(ownedAuthorityDeadline)
				<-ctx.Done()
				return nil
			})
			if !errors.Is(err, domain.ErrMembershipDenied) || f.driver.commits != 0 {
				t.Fatalf("expired callback err=%v commits=%d", err, f.driver.commits)
			}
		})
	})
}
