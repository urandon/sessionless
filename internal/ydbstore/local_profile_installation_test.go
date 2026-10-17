package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// Exact locator-keyed transactional model. Unlike the older one-head fixture,
// this can detect writing one successor without retaining its predecessor or
// losing one of the two audits. It is not native YDB concurrency/source proof.
type localInstallationSQLFixture struct {
	*ownedCurrentSQLFixture
	heads, pendingHeads  map[string][]driver.Value
	audits, pendingAudit map[string][]driver.Value
	failAuditNumber      int
	auditWritesThisTx    int
}

func localInstallationSQLKey(values []driver.Value) string {
	raw, _ := json.Marshal(values)
	return string(raw)
}

func localInstallationNamedValues(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	return values
}

func cloneLocalInstallationRows(rows map[string][]driver.Value) map[string][]driver.Value {
	cloned := make(map[string][]driver.Value, len(rows))
	for key, row := range rows {
		cloned[key] = append([]driver.Value(nil), row...)
	}
	return cloned
}

func (f *localInstallationSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localInstallationSQLFixture) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.isolations = append(f.isolations, options.Isolation)
	if options.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("local installation requires serializable")
	}
	f.pendingHeads, f.pendingAudit = cloneLocalInstallationRows(f.heads), cloneLocalInstallationRows(f.audits)
	f.auditWritesThisTx = 0
	return localInstallationFixtureTx{f}, nil
}

type localInstallationFixtureTx struct{ f *localInstallationSQLFixture }

func (tx localInstallationFixtureTx) Commit() error {
	f := tx.f
	f.commits++
	mode := ""
	if len(f.commitModes) > 0 {
		mode, f.commitModes = f.commitModes[0], f.commitModes[1:]
	}
	if mode != "rollback" {
		f.heads, f.audits = cloneLocalInstallationRows(f.pendingHeads), cloneLocalInstallationRows(f.pendingAudit)
	}
	f.pendingHeads, f.pendingAudit = nil, nil
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if mode == "rollback" || mode == "ambiguous" {
		return retry.RetryableError(errors.New("local installation lost ACK"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	if mode == "unknown" {
		return errors.New("local installation unknown commit")
	}
	return nil
}
func (tx localInstallationFixtureTx) Rollback() error {
	tx.f.rollbacks++
	tx.f.pendingHeads, tx.f.pendingAudit = nil, nil
	return nil
}

func (f *localInstallationSQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != ownedProfileCurrentSQL && query != ownedProfileAuditSQL {
		return f.ownedCurrentSQLFixture.QueryContext(ctx, query, args)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.statements = append(f.statements, query)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	values := localInstallationNamedValues(args)
	var row []driver.Value
	if query == ownedProfileCurrentSQL && len(values) == 9 {
		row = f.pendingHeads[localInstallationSQLKey(values)]
	} else if query == ownedProfileAuditSQL && len(values) == 3 {
		row = f.pendingAudit[localInstallationSQLKey(values)]
	} else {
		return nil, errors.New("wrong local installation query arity")
	}
	row = append([]driver.Value(nil), row...)
	for i, value := range row {
		if text, ok := value.(string); ok {
			limit := 161
			if query == ownedProfileAuditSQL {
				limit = 65
			}
			if query == ownedProfileCurrentSQL && i == 0 || query == ownedProfileAuditSQL && i == 1 {
				limit = ownedAuthorityHeadBytes + 1
			}
			if len(text) > limit {
				row[i] = text[:limit]
			}
		}
	}
	return &explanationFixtureRows{values: row}, nil
}

func (f *localInstallationSQLFixture) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.execCalls++
	f.statements = append(f.statements, query)
	values := localInstallationNamedValues(args)
	switch query {
	case ownedProfileWriteSQL:
		if len(values) != 20 {
			return nil, errors.New("wrong local installation head arity")
		}
		f.pendingHeads[localInstallationSQLKey(values[:9])] = append([]driver.Value(nil), values[9:]...)
	case ownedProfileAuditWriteSQL:
		f.auditWritesThisTx++
		if f.auditWritesThisTx == f.failAuditNumber {
			return nil, errors.New("local installation injected audit failure")
		}
		if len(values) != 8 {
			return nil, errors.New("wrong local installation audit arity")
		}
		key := localInstallationSQLKey(values[:3])
		if _, exists := f.pendingAudit[key]; exists {
			return nil, errors.New("immutable local installation audit already exists")
		}
		f.pendingAudit[key] = append([]driver.Value(nil), values[3:]...)
	default:
		return nil, errors.New("non-manifest local installation write")
	}
	return driver.RowsAffected(1), nil
}

func syntheticLocalInstallationGrant(t *testing.T, original domain.OwnedAgentProfileCurrentV1) ports.OwnedProfilePublicationGrantV1 {
	t.Helper()
	p := original.Clone()
	p.Backend.BackendKind, p.Backend.ProviderContractKind, p.Backend.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	p.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryNoneV1
	p.Resource.Kind, p.Resource.CredentialMode, p.Resource.CredentialGeneration = domain.ProviderResourceLocalEndpointV1, domain.ProviderCredentialNoneV1, 0
	p.Locator.ResourceKind = p.Resource.Kind
	p.Substrate.Kind, p.Substrate.Region, p.Substrate.ImageDigest, p.Substrate.WorkloadMode = domain.SubstrateLocalDevelopmentV1, "", "", domain.SubstrateWorkloadInProcessDirectV1
	p.Substrate.LocalInstallationID, p.Substrate.LocalInstallationRevision = p.InstallationID, p.InstallationRevision
	p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = strings.Repeat("a", 64), strings.Repeat("b", 64)
	p.BillingOwnerUserID = p.Locator.OwnerUserID
	p.CostCeiling.MCPToolCost.BillingOwnerUserID = p.BillingOwnerUserID
	p.CostCeiling.WebSearchCost.BillingOwnerUserID = p.BillingOwnerUserID
	p.SubstrateTermsDigest = p.ProviderTermsDigest
	p.Substrate.ProfileEvidenceExpiresAt = p.Window.ExpiresAt
	costDigest, costErr := p.CostCeiling.Digest()
	if costErr != nil {
		t.Fatal(costErr)
	}
	p.Substrate.AdmissionCostCeilingDigest = costDigest
	hash := strings.Repeat("a", 64)
	cfg := domain.ConfiguredInferenceProfileV1{
		Version: 1, TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, ProfileRevision: p.ProfileRevision, Resource: p.Resource,
		BaseURL: "http://127.0.0.1:11434/v1", EndpointPolicy: domain.ConfiguredInferenceLoopbackPolicyV1, NativeProtocolVersion: domain.ConfiguredInferenceProtocolChatCompletionsV1, Auth: domain.ConfiguredInferenceAuthNoneV1,
		ModelVendorID: p.Locator.ModelVendorID, ModelID: p.Locator.ModelID, ResponseModelIDs: []string{p.Locator.ModelID}, ServerID: "synthetic-server", ServerVersion: "synthetic-version", ChatTemplateDigest: hash,
		Capabilities:       domain.ConfiguredInferenceCapabilitiesV1{Text: domain.ProviderEvidenceSupportedV1, Image: domain.ProviderEvidenceUnsupportedV1, File: domain.ProviderEvidenceUnsupportedV1, ToolCalls: domain.ProviderEvidenceSupportedV1, JSONSchema: domain.ProviderEvidenceUnknownV1},
		CapabilityEvidence: domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-capability", Digest: p.CapabilityEvidenceDigest, Bytes: 32}, ProviderCatalogDigest: p.ProviderCatalogDigest, ProviderRouteDigest: p.ProviderRouteDigest, PrivacyPolicyDigest: p.PrivacyPolicyDigest, EffectivePolicyDigest: p.EffectivePolicyDigest,
		AllowedDataClasses: []domain.ProviderDataClassV1{p.Locator.InputDataClass}, ModelWindowTokens: p.ModelWindowTokens, TokenEstimatorVersion: p.TokenEstimatorVersion, CalibrationDigest: p.CalibrationDigest,
		Limits:              domain.ConfiguredInferenceLimitsV1{RequestBytes: 1 << 20, ResponseBytes: 1 << 20, ResponseHeaderBytes: 16 << 10, JSONDepth: 32, JSONMembers: 256, Choices: 1, ToolCalls: 1, ToolNameBytes: 128, ToolCallIDBytes: 128, ToolArgumentsBytes: 4 << 10, FinalTextBytes: 256 << 10, Messages: 256, Tools: 16, MaxInputTokens: p.Limits.MaxInputTokensPerCall, MaxOutputTokens: p.Limits.MaxOutputTokensPerCall, OperationTimeout: p.Limits.MaxNetworkOperationDuration, CleanupTimeout: p.Limits.MaxCleanupDuration},
		LocalInstallationID: p.InstallationID, LocalInstallationRevision: p.InstallationRevision, LocalHostIdentityDigest: p.Substrate.LocalHostIdentityDigest, LocalNetworkNamespaceIdentityDigest: p.Substrate.LocalNetworkNamespaceIdentityDigest, WorkerExecutableDigest: p.Substrate.OuterHarnessArtifactDigest,
		OperatorTerms: domain.OwnedAgentAuthorityBodyRefV1{Ref: "synthetic-local-terms", Digest: p.ProviderTermsDigest, Bytes: 32}, OriginalSource: p.InstalledManifest, IssuedAt: p.Window.ObservedAt, ExpiresAt: p.Window.ExpiresAt,
	}
	digest, err := cfg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p.Locator.BackendProfileDigest, p.Backend.BackendProfileDigest = digest, digest
	if err := cfg.ValidateForOwnedProfile(p); err != nil {
		t.Fatalf("synthetic local installation association: %v", err)
	}
	return ports.OwnedProfilePublicationGrantV1{Profile: p, IssuedAt: p.Window.ObservedAt, ExpiresAt: p.Window.ObservedAt.Add(10 * time.Second), LocalInstallation: &ports.OwnedLocalInstallationIntentV1{Profile: cfg}}
}

func newLocalInstallationFixture(t *testing.T) (*Store, *localInstallationSQLFixture, *fixtureProfileVerifier, ports.OwnedProfilePublicationGrantV1) {
	t.Helper()
	r := newOwnedReaderFixture(t)
	g := syntheticLocalInstallationGrant(t, r.profile)
	f := &localInstallationSQLFixture{ownedCurrentSQLFixture: r.driver, heads: make(map[string][]driver.Value), audits: make(map[string][]driver.Value)}
	v := &fixtureProfileVerifier{key: []byte("only-scripted-local-source-not-real-auth"), allowed: make(map[string]bool)}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := New(db, Options{OwnedProfilePublicationVerifier: v})
	if err != nil {
		t.Fatal(err)
	}
	g = v.install(t, g)
	return s, f, v, g
}

func replacementLocalGrant(t *testing.T, verifier *fixtureProfileVerifier, original ports.OwnedProfilePublicationGrantV1, predecessor ports.OwnedProfilePublicationReceiptV1, suffix string) ports.OwnedProfilePublicationGrantV1 {
	t.Helper()
	g := original.Clone()
	g.Profile = predecessor.Profile.Clone()
	g.Profile.ProfileRevision++
	g.Profile.InstallationRevision++
	g.Profile.Resource.Revision++
	g.Profile.Substrate.LocalInstallationRevision++
	g.Profile.MutationID = "local-replace-" + suffix
	g.Profile.RevocationGeneration = 1
	review := sha256.Sum256([]byte("synthetic-local-review-" + suffix))
	g.Profile.ReviewReceipt.Digest = hex.EncodeToString(review[:])
	cfg := g.LocalInstallation.Profile.Clone()
	cfg.BaseURL = "http://127.0.0.1:11435/v1"
	cfg.Resource = g.Profile.Resource
	cfg.ProfileRevision, cfg.LocalInstallationRevision = g.Profile.ProfileRevision, g.Profile.InstallationRevision
	digest, err := cfg.Digest()
	if err != nil {
		t.Fatal(err)
	}
	g.Profile.Locator.BackendProfileDigest, g.Profile.Backend.BackendProfileDigest = digest, digest
	g.LocalInstallation = &ports.OwnedLocalInstallationIntentV1{Profile: cfg, Predecessor: &predecessor, RetirementMutationID: "local-retire-" + suffix}
	return verifier.install(t, g)
}

func localInstallationHeadKey(p domain.OwnedAgentProfileCurrentV1) string {
	l := p.Locator
	return localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.OwnerUserID), string(l.Profile), l.BackendProfileDigest, string(l.ResourceKind), l.ResourceID, l.ModelVendorID, l.ModelID, string(l.InputDataClass)})
}

func TestLocalInstallationAtomicReplacementAndHistoricalReadback(t *testing.T) {
	s, f, verifier, g := newLocalInstallationFixture(t)
	first, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil || first.Replayed {
		t.Fatalf("first installation err=%v replay=%t writes=%d commits=%d statements=%v", err, first.Replayed, f.execCalls, f.commits, f.statements)
	}
	r := replacementLocalGrant(t, verifier, g, first, "b")
	second, err := s.PublishOwnedProfile(t.Context(), r)
	if err != nil || second.Replayed || len(f.heads) != 2 || len(f.audits) != 3 {
		t.Fatalf("replace err=%v replay=%t heads=%d audits=%d", err, second.Replayed, len(f.heads), len(f.audits))
	}
	beforeHeads, beforeAudits, writes := cloneLocalInstallationRows(f.heads), cloneLocalInstallationRows(f.audits), f.execCalls
	for _, historical := range []ports.OwnedProfilePublicationGrantV1{g, r} {
		recovered, err := s.PublishOwnedProfile(t.Context(), historical)
		if err != nil || !recovered.Replayed {
			t.Fatalf("historical recovery err=%v replay=%t", err, recovered.Replayed)
		}
	}
	if f.execCalls != writes || !reflect.DeepEqual(beforeHeads, f.heads) || !reflect.DeepEqual(beforeAudits, f.audits) {
		t.Fatal("historical receipt recovery mutated storage")
	}
	err = s.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, err := readOwnedProfileCurrent(ctx, q, domain.OwnedAgentProfileReferenceV1{Locator: first.Profile.Locator, Digest: first.Digest, RevocationGeneration: 1})
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("old current source remains eligible: %v", err)
	}
	thirdGrant := replacementLocalGrant(t, verifier, r, second, "c")
	if _, err := s.PublishOwnedProfile(t.Context(), thirdGrant); err != nil {
		t.Fatalf("later successor: %v", err)
	}
	writes = f.execCalls
	recovered, err := s.PublishOwnedProfile(t.Context(), r)
	if err != nil || !recovered.Replayed || recovered.Digest != second.Digest || f.execCalls != writes {
		t.Fatalf("replay after later retirement err=%v replay=%t writes=%d want=%d", err, recovered.Replayed, f.execCalls, writes)
	}
}

func TestLocalInstallationReplacementRollsBackAndRecoversExactPair(t *testing.T) {
	for _, mode := range []string{"first_audit_failure", "second_audit_failure", "rollback", "ambiguous", "unknown", "retry_expired", "retry_revoked"} {
		t.Run(mode, func(t *testing.T) {
			s, f, v, g := newLocalInstallationFixture(t)
			first, err := s.PublishOwnedProfile(t.Context(), g)
			if err != nil {
				t.Fatal(err)
			}
			r := replacementLocalGrant(t, v, g, first, "b")
			beforeHeads, beforeAudits := cloneLocalInstallationRows(f.heads), cloneLocalInstallationRows(f.audits)
			switch mode {
			case "first_audit_failure":
				f.failAuditNumber = 1
			case "second_audit_failure":
				f.failAuditNumber = 2
			case "rollback", "ambiguous", "unknown":
				f.commitModes = []string{mode}
			case "retry_expired":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { f.at = r.ExpiresAt }
			case "retry_revoked":
				f.commitModes = []string{"rollback"}
				f.afterCommit = func() { v.fail = true }
			}
			got, err := s.PublishOwnedProfile(t.Context(), r)
			if mode == "rollback" || mode == "ambiguous" {
				if err != nil || got.Digest == "" || got.Replayed != (mode == "ambiguous") || len(f.heads) != 2 || len(f.audits) != 3 {
					t.Fatalf("mode=%s err=%v replay=%t heads=%d audits=%d", mode, err, got.Replayed, len(f.heads), len(f.audits))
				}
				return
			}
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) {
				t.Fatalf("mode=%s ambiguous/denied receipt err=%v nonzero=%t", mode, err, got.Digest != "")
			}
			if mode == "unknown" {
				// Unknown commit returns no receipt. A new exact readback can recover
				// both original intents without issuing a second transition.
				writes := f.execCalls
				recovered, recoveryErr := s.PublishOwnedProfile(t.Context(), r)
				if recoveryErr != nil || !recovered.Replayed || f.execCalls != writes {
					t.Fatalf("exact reconciliation err=%v replay=%t", recoveryErr, recovered.Replayed)
				}
			} else if !reflect.DeepEqual(beforeHeads, f.heads) || !reflect.DeepEqual(beforeAudits, f.audits) {
				t.Fatalf("mode=%s leaked one half of rolled-back publication", mode)
			}
		})
	}
}

func TestLocalInstallationDeniesPartialAuditOrMissingTombstone(t *testing.T) {
	for _, corruption := range []string{"missing_old_audit", "missing_new_audit", "wrong_intent", "missing_old_head", "missing_new_head", "revived_old_head", "changed_new_package"} {
		t.Run(corruption, func(t *testing.T) {
			s, f, v, g := newLocalInstallationFixture(t)
			first, err := s.PublishOwnedProfile(t.Context(), g)
			if err != nil {
				t.Fatal(err)
			}
			r := replacementLocalGrant(t, v, g, first, "b")
			second, err := s.PublishOwnedProfile(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			oldLocator, _ := first.Profile.Locator.Digest()
			newLocator, _ := second.Profile.Locator.Digest()
			oldAudit := localInstallationSQLKey([]driver.Value{string(first.Profile.Locator.TenantID), oldLocator, r.LocalInstallation.RetirementMutationID})
			newAudit := localInstallationSQLKey([]driver.Value{string(second.Profile.Locator.TenantID), newLocator, r.Profile.MutationID})
			switch corruption {
			case "missing_old_audit":
				delete(f.audits, oldAudit)
			case "missing_new_audit":
				delete(f.audits, newAudit)
			case "wrong_intent":
				f.audits[oldAudit][0] = strings.Repeat("f", 64)
			case "missing_old_head":
				delete(f.heads, localInstallationHeadKey(first.Profile))
			case "missing_new_head":
				delete(f.heads, localInstallationHeadKey(second.Profile))
			case "revived_old_head":
				row := f.heads[localInstallationHeadKey(first.Profile)]
				row[4] = string(domain.OwnedAgentCurrentActiveV1)
			case "changed_new_package":
				row := f.heads[localInstallationHeadKey(second.Profile)]
				row[5] = strings.Repeat("f", 64)
			}
			writes := f.execCalls
			got, err := s.PublishOwnedProfile(t.Context(), r)
			if !errors.Is(err, domain.ErrMembershipDenied) || got.Digest != "" || f.execCalls != writes {
				t.Fatalf("corruption=%s err=%v digest=%q extraWrites=%d", corruption, err, got.Digest, f.execCalls-writes)
			}
		})
	}
}

func TestLocalInstallationCompetingSuccessorCannotBypassPredecessor(t *testing.T) {
	s, f, v, g := newLocalInstallationFixture(t)
	first, err := s.PublishOwnedProfile(t.Context(), g)
	if err != nil {
		t.Fatal(err)
	}
	one, two := replacementLocalGrant(t, v, g, first, "b"), replacementLocalGrant(t, v, g, first, "c")
	two.LocalInstallation.Profile.BaseURL = "http://127.0.0.1:11436/v1"
	two.Profile.Backend.BackendProfileDigest, err = two.LocalInstallation.Profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	two.Profile.Locator.BackendProfileDigest = two.Profile.Backend.BackendProfileDigest
	two = v.install(t, two)
	if one.Profile.Locator == two.Profile.Locator {
		t.Fatal("causal predecessor CAS test requires distinct successor locators")
	}
	if _, err := s.PublishOwnedProfile(t.Context(), one); err != nil {
		t.Fatal(err)
	}
	writes := f.execCalls
	got, err := s.PublishOwnedProfile(t.Context(), two)
	if !errors.Is(err, domain.ErrMembershipDenied) || got.Digest != "" || f.execCalls != writes || len(f.heads) != 2 {
		t.Fatalf("stale competitor err=%v writes=%d heads=%d", err, f.execCalls-writes, len(f.heads))
	}
	loserLocator, err := two.Profile.Locator.Digest()
	if err != nil {
		t.Fatal(err)
	}
	loserAudit := localInstallationSQLKey([]driver.Value{string(two.Profile.Locator.TenantID), loserLocator, two.Profile.MutationID})
	if _, exists := f.heads[localInstallationHeadKey(two.Profile)]; exists || f.audits[loserAudit] != nil || len(f.audits) != 3 {
		t.Fatal("losing distinct successor leaked a head or immutable audit")
	}
	// Even a separately authenticated shape cannot overwrite that retained key
	// through a fresh-publication grant; actual minting remains producer-owned.
	revival := g.Clone()
	revival.Profile.MutationID = "try-fresh-revival"
	revival = v.install(t, revival)
	if _, err := s.PublishOwnedProfile(t.Context(), revival); !errors.Is(err, domain.ErrMembershipDenied) || f.execCalls != writes {
		t.Fatalf("fresh overwrite err=%v writes=%d", err, f.execCalls-writes)
	}
}
