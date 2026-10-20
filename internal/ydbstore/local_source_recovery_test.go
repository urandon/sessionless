package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Test-side contract, deliberately without a production stub. The operation
// authenticates/rejoins original installed funding and materializes only truly
// missing initial components. Its result is a candidate package, not canonical
// admission, capacity, a physical claim or permission to send. Existing complete
// history must remain read-only, including after the short publication grants
// expire. This is not the future full Web/binder/PUT/commit budget proof.
// The original full profile-grant preimage/authenticator is not persisted in
// its audit and is absent after OpenExisting. These tests do NOT establish
// cryptographic correspondence of that opaque intent hash to a recreated
// permission grant. Recovery instead requires the SAME concrete Source already
// installed in both publishers, trusted private installation custody, its exact
// retained receipt and current head/audit postimages. Audit DATA cannot grant
// issuance. Audit DATA alone does not authenticate a private key. Actual Source
// intake independently checks KeyFingerprint against the private key and the
// exact manifest anchored by the receipt; this test adds no audit key commitment.
type originalLocalSourceRecovery interface {
	RecoverLocalOwnedExecutionPackage(context.Context, *localoperator.Source, ports.OwnedProfilePublicationReceiptV1, domain.AttemptExecutionSourceScopeV1) (domain.OwnedAgentExecutionPackageV1, error)
}

func requireOriginalLocalSourceRecovery(t *testing.T, store *Store) originalLocalSourceRecovery {
	t.Helper()
	recovery, ok := any(store).(originalLocalSourceRecovery)
	if !ok {
		t.Fatal("original Store lacks coherent original Source publication recovery/materialization")
	}
	return recovery
}

type originalLocalRecoveryFixture struct {
	root    string
	exe     string
	source  *localoperator.Source
	store   *Store
	sql     *localInstallationSQLFixture
	receipt ports.OwnedProfilePublicationReceiptV1
	scope   domain.AttemptExecutionSourceScopeV1
	grants  [4]ports.AttemptSpendingPublicationGrantV1
	rows    [4]domain.AttemptSpendingAuthorizationCurrentV1
}

func newOriginalLocalRecoveryFixture(t *testing.T, published int) *originalLocalRecoveryFixture {
	t.Helper()
	if published < 0 || published > 4 {
		t.Fatal("recovery fixture requires zero to four actual publications")
	}
	root, exe, manifest := localOperatorFiles(t)
	localSearchDeclaration(t, &manifest)
	localSourceMCPDeclaration(t, &manifest)
	writeLocalOperatorManifest(t, root, manifest)
	source, err := localoperator.OpenFresh(t.Context(), root, exe)
	if err != nil {
		t.Fatalf("actual original recovery Source intake: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	publication, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	store, f, _, _ := newLocalInstallationFixture(t)
	f.at = publication.IssuedAt
	store.ownedProfilePublicationVerifier, store.attemptSpendingPublicationVerifier = source, source
	receipt, err := store.PublishOwnedProfile(t.Context(), publication)
	if err != nil || source.AcceptReceipt(receipt) != nil {
		t.Fatalf("actual original recovery profile publication/receipt: %v", err)
	}
	r := &originalLocalRecoveryFixture{root: root, exe: exe, source: source, store: store, sql: f, receipt: receipt, scope: localSearchScope(receipt.Profile, "original-recovery")}
	store.ownedAttemptEffectSourceVerifier = source
	// The SQL fixture admits only these four exact locator keys. Registering an
	// empty key is not a spending publication or source/authentication decision.
	f.fundingKeys = make(map[string]bool)
	for i, kind := range ownedSpendingKinds {
		key := localInstallationSQLKey([]driver.Value{string(r.scope.TenantID), string(r.scope.AttemptID), string(kind), string(receipt.Profile.BillingOwnerUserID)})
		f.fundingKeys[key] = true
		if i >= published {
			continue
		}
		g, err := source.SpendingPublication(t.Context(), r.scope, kind)
		if err != nil {
			t.Fatalf("original Source initial %s grant: %v", kind, err)
		}
		f.at = g.IssuedAt
		post, err := store.PublishAttemptSpending(t.Context(), g)
		if err != nil || post.Replayed {
			t.Fatalf("actual original %s publication: replay=%t err=%v", kind, post.Replayed, err)
		}
		r.grants[i], r.rows[i] = g, post.Authorization
	}
	return r
}

func (f *originalLocalRecoveryFixture) packageFromOriginalRows(t *testing.T) domain.OwnedAgentExecutionPackageV1 {
	t.Helper()
	issued := f.receipt.Profile.Window.PublishedAt
	for _, row := range f.rows {
		if row.Window.PublishedAt.After(issued) {
			issued = row.Window.PublishedAt
		}
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(f.scope, f.receipt.Profile, f.rows, issued)
	if err != nil || f.source.VerifyOwnedAttemptEffectSource(pkg.Source) != nil {
		t.Fatalf("actual four-publication source/package control: %v", err)
	}
	return pkg
}

func assertOriginalRecoveryNoWrites(t *testing.T, f *originalLocalRecoveryFixture, writes int, heads, audits map[string][]driver.Value) {
	t.Helper()
	if f.sql.execCalls != writes || !reflect.DeepEqual(heads, f.sql.fundingHeads) || !reflect.DeepEqual(audits, f.sql.fundingAudits) {
		t.Fatalf("original recovery changed retained funding: exec=%d/%d headsEqual=%t auditsEqual=%t", f.sql.execCalls, writes, reflect.DeepEqual(heads, f.sql.fundingHeads), reflect.DeepEqual(audits, f.sql.fundingAudits))
	}
}

func TestLocalSourceRecoveryExpiredGrantOriginalHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		want := f.packageFromOriginalRows(t)
		deadline := f.grants[3].ExpiresAt
		time.Sleep(time.Until(deadline) + time.Microsecond)
		f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
		life, err := f.source.ExecutionLifetime()
		if err != nil || life.Err() != nil || !f.sql.at.Before(want.Source.ExpiresAt) {
			t.Fatal("short-grant recovery control expired the original installation/Attempt")
		}
		if f.source.VerifyAttemptSpendingPublication(f.grants[3], f.sql.at) == nil {
			t.Fatal("causal setup failed: original short publication grant remains executable")
		}
		// Same DB/original Source, fresh Store; no remembered per-Attempt grants,
		// Boolean verifier, fixture approval or re-issued grant is supplied.
		fresh, err := New(f.store.db, Options{OwnedProfilePublicationVerifier: f.source, AttemptSpendingPublicationVerifier: f.source, OwnedAttemptEffectSourceVerifier: f.source})
		if err != nil {
			t.Fatal(err)
		}
		recovery := requireOriginalLocalSourceRecovery(t, fresh)
		heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
		got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("expired short grant lost exact original package: equal=%t err=%v", reflect.DeepEqual(got, want), err)
		}
		assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
	})
}

func TestLocalSourceRecoveryOpenExistingWithoutPublicationGrant(t *testing.T) {
	for _, count := range []int{0, 2, 4} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, count)
				profileHeads, profileAudits := cloneLocalInstallationRows(f.sql.heads), cloneLocalInstallationRows(f.sql.audits)
				fundingHeads, fundingAudits := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits)
				if err := f.source.Close(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(11 * time.Second) // fake time: original short grants expired
				f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
				resumed, err := localoperator.OpenExisting(t.Context(), f.root, f.exe, f.receipt)
				if err != nil {
					t.Fatalf("actual private-file OpenExisting: %v", err)
				}
				t.Cleanup(func() { _ = resumed.Close() })
				if _, err := resumed.Publication(); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatal("OpenExisting re-created original profile publication grant")
				}
				if _, err := f.source.Configuration(); !errors.Is(err, domain.ErrMembershipDenied) {
					t.Fatal("independent startup revived the closed original Source")
				}
				fresh, err := New(f.store.db, Options{OwnedProfilePublicationVerifier: resumed, AttemptSpendingPublicationVerifier: resumed, OwnedAttemptEffectSourceVerifier: resumed})
				if err != nil {
					t.Fatal(err)
				}
				recovery := requireOriginalLocalSourceRecovery(t, fresh)
				writes := f.sql.execCalls
				got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), resumed, f.receipt, f.scope)
				if err != nil || resumed.VerifyOwnedAttemptEffectSource(got.Source) != nil || f.sql.execCalls-writes != (4-count)*2 || len(f.sql.fundingHeads) != 4 || len(f.sql.fundingAudits) != 4 || !reflect.DeepEqual(profileHeads, f.sql.heads) || !reflect.DeepEqual(profileAudits, f.sql.audits) {
					t.Fatalf("actual resumed missing-only recovery count%d: writes=%d heads=%d audits=%d profileUnchanged=%t err=%v", count, f.sql.execCalls-writes, len(f.sql.fundingHeads), len(f.sql.fundingAudits), reflect.DeepEqual(profileHeads, f.sql.heads) && reflect.DeepEqual(profileAudits, f.sql.audits), err)
				}
				for key, row := range fundingHeads {
					if !reflect.DeepEqual(row, f.sql.fundingHeads[key]) {
						t.Fatal("resumed issuer replaced an original existing head")
					}
				}
				for key, row := range fundingAudits {
					if !reflect.DeepEqual(row, f.sql.fundingAudits[key]) {
						t.Fatal("resumed issuer replaced an immutable original audit")
					}
				}
				completeHeads, completeAudits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
				replay, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), resumed, f.receipt, f.scope)
				if err != nil || !reflect.DeepEqual(replay, got) || f.sql.execCalls != writes || !reflect.DeepEqual(completeHeads, f.sql.fundingHeads) || !reflect.DeepEqual(completeAudits, f.sql.fundingAudits) {
					t.Fatalf("actual OpenExisting full replay reissued/mutated funding: %v", err)
				}
			})
		})
	}
}

func TestLocalSourceRecoveryReceiptMaterializationBoundary(t *testing.T) {
	for _, fault := range []string{"four-modalities", "nested-price-revision", "exhausted-bytes", "sticky-bytes", "valid-owned-clone"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 4)
				ctx, cancel := ownedAuthorityCallContext(t.Context())
				defer cancel()
				b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				input := f.receipt
				switch fault {
				case "four-modalities":
					input.Profile.InputModalities = []domain.AgentInputModalityV1{domain.AgentInputFileV1, domain.AgentInputImageV1, domain.AgentInputTextV1, domain.AgentInputTextV1}
				case "nested-price-revision":
					input.Profile = input.Profile.Clone()
					input.Profile.CostCeiling.MCPToolCost.PriceRevision = strings.Repeat("x", 129)
				case "exhausted-bytes":
					if err := chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes); err != nil {
						t.Fatal(err)
					}
				case "sticky-bytes":
					if chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes+1) == nil {
						t.Fatal("causal byte exhaustion did not fail")
					}
				}
				beforeBytes, statements := b.bytes, len(f.sql.statements)
				got, err := materializeLocalSourceRecoveryReceipt(ctx, input)
				if fault == "valid-owned-clone" {
					if err != nil || b.bytes != beforeBytes+ownedAuthorityHeadBytes || !reflect.DeepEqual(got, input) {
						t.Fatalf("valid bounded clone not charged to original call: bytes=%d err=%v", b.bytes, err)
					}
					originalModality := got.Profile.InputModalities[0]
					input.Profile.InputModalities[0] = "changed-after-copy"
					originalAmount := *got.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits
					*input.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits = originalAmount + 1
					if got.Profile.InputModalities[0] != originalModality || *got.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits != originalAmount {
						t.Fatal("validated clone aliases caller collection or nested cost")
					}
				} else if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}) || b.bytes != beforeBytes {
					t.Fatalf("%s crossed receipt validation/budget boundary: zero=%t bytes=%d/%d err=%v", fault, reflect.DeepEqual(got, ports.OwnedProfilePublicationReceiptV1{}), b.bytes, beforeBytes, err)
				}
				if b.callbacks != 0 || len(f.sql.statements) != statements {
					t.Fatal("receipt materialization entered SQL/current authority")
				}
				// Counters/clone independence test validation and accounting, not
				// observation of an allocation the old public method already made.
				if fault == "four-modalities" {
					recovery := requireOriginalLocalSourceRecovery(t, f.store)
					candidate, err := recovery.RecoverLocalOwnedExecutionPackage(ctx, f.source, input, f.scope)
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(candidate, domain.OwnedAgentExecutionPackageV1{}) || b.bytes != beforeBytes || b.callbacks != 0 || len(f.sql.statements) != statements {
						t.Fatal("public receipt route bypassed pre-copy validation")
					}
				}
			})
		})
	}
}

func TestLocalSourceRecoveryPartialMaterializationOriginalEnvelope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 2)
		originalHeads, originalAudits := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits)
		earliest := f.rows[0].Window.ExpiresAt
		if f.rows[1].Window.ExpiresAt.Before(earliest) {
			earliest = f.rows[1].Window.ExpiresAt
		}
		time.Sleep(11 * time.Second) // controlled short-grant expiry, not readiness
		f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
		recovery := requireOriginalLocalSourceRecovery(t, f.store)
		writes, commits := f.sql.execCalls, f.sql.commits
		got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if err != nil || f.source.VerifyOwnedAttemptEffectSource(got.Source) != nil {
			t.Fatalf("two-of-four original materialization: %v", err)
		}
		if len(f.sql.fundingHeads) != 4 || len(f.sql.fundingAudits) != 4 || f.sql.execCalls-writes != 4 || f.sql.commits-commits > 2 {
			t.Fatalf("missing-only materialization lost atomic/bounded pair: heads=%d audits=%d writes=%d callbacks=%d", len(f.sql.fundingHeads), len(f.sql.fundingAudits), f.sql.execCalls-writes, f.sql.commits-commits)
		}
		for key, row := range originalHeads {
			if !reflect.DeepEqual(f.sql.fundingHeads[key], row) {
				t.Fatal("materialization rewrote an existing original component")
			}
		}
		for key, row := range originalAudits {
			if !reflect.DeepEqual(f.sql.fundingAudits[key], row) {
				t.Fatal("materialization replaced an immutable original audit")
			}
		}
		for i, row := range got.Source.Spending {
			if row.Locator.ComponentKind != ownedSpendingKinds[i] || row.Window.ExpiresAt.After(earliest) {
				t.Fatalf("component%d escaped original ordered/earliest envelope: %s expiry=%v original=%v", i, row.Locator.ComponentKind, row.Window.ExpiresAt, earliest)
			}
			if i < 2 && !reflect.DeepEqual(row, f.rows[i]) {
				t.Fatalf("component%d was reissued/top-upped instead of recovered", i)
			}
		}
		if got.Source.ExpiresAt.After(earliest) || !got.Source.IssuedAt.Equal(f.sql.at) {
			t.Fatalf("package renewed/backdated original window: issue=%v expectedPublication=%v expiry=%v original=%v", got.Source.IssuedAt, f.sql.at, got.Source.ExpiresAt, earliest)
		}
		heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
		again, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if err != nil || !reflect.DeepEqual(again, got) {
			t.Fatalf("completed original package replay changed bytes: %v", err)
		}
		assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
	})
}

func TestLocalSourceRecoveryUnknownCommitReadbackNoReissue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 2)
		recovery := requireOriginalLocalSourceRecovery(t, f.store)
		// First read commits; the atomic missing-component write commits but its
		// ACK is unknown. The operation must not return a speculative package.
		f.sql.commitModes = []string{"", "unknown"}
		got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || len(f.sql.fundingHeads) != 4 || len(f.sql.fundingAudits) != 4 {
			t.Fatalf("unknown commit escaped or lost durable original publication: resultZero=%t heads=%d audits=%d err=%v", reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), len(f.sql.fundingHeads), len(f.sql.fundingAudits), err)
		}
		heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
		got, err = recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if err != nil || f.source.VerifyOwnedAttemptEffectSource(got.Source) != nil {
			t.Fatalf("durable missing-ACK publication did not recover exactly: %v", err)
		}
		assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
	})
}

func TestLocalSourceRecoveryCorruptOrRetiredHistoryDenies(t *testing.T) {
	for _, fault := range []string{"head-scalar", "audit-digest", "audit-missing", "audit-scope", "profile-audit-missing", "profile-audit-digest", "current-component-revoked", "current-profile-revoked", "source-closed", "foreign-source", "copied-source", "receipt-digest", "foreign-scope"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 4)
				_ = f.packageFromOriginalRows(t)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				source, receipt, scope := f.source, f.receipt, f.scope
				l := f.rows[2].Locator
				headKey := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID)})
				auditKey := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID), f.rows[2].MutationID})
				switch fault {
				case "head-scalar":
					f.sql.fundingHeads[headKey][7] = "foreign-owner"
				case "audit-digest":
					f.sql.fundingAudits[auditKey][2] = strings.Repeat("f", 64)
				case "audit-missing":
					delete(f.sql.fundingAudits, auditKey)
				case "audit-scope":
					f.sql.fundingAudits[auditKey][6] = "foreign-session"
				case "profile-audit-missing", "profile-audit-digest":
					locatorDigest, err := receipt.Profile.Locator.Digest()
					if err != nil {
						t.Fatal(err)
					}
					key := localInstallationSQLKey([]driver.Value{string(receipt.Profile.Locator.TenantID), locatorDigest, receipt.Profile.MutationID})
					if _, exists := f.sql.audits[key]; !exists {
						t.Fatal("genuine profile publication audit missing before fault")
					}
					if fault == "profile-audit-missing" {
						delete(f.sql.audits, key)
					} else {
						f.sql.audits[key][2] = strings.Repeat("f", 64)
					}
				case "current-component-revoked":
					v := f.rows[2].Clone()
					v.State, v.RevocationGeneration, v.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, "original-component-revocation"
					raw, encodeErr := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
					digest, digestErr := v.Digest()
					if encodeErr != nil || digestErr != nil {
						t.Fatalf("coherent revoked component control: %v / %v", encodeErr, digestErr)
					}
					row := f.sql.fundingHeads[headKey]
					row[0], row[2], row[3], row[4] = string(raw), int64(v.RevocationGeneration), string(v.State), digest
				case "current-profile-revoked":
					v := receipt.Profile.Clone()
					v.State, v.RevocationGeneration, v.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, "original-profile-revocation"
					raw, encodeErr := domain.EncodeOwnedAgentProfileCurrentV1(v)
					digest, digestErr := v.Digest()
					if encodeErr != nil || digestErr != nil {
						t.Fatalf("coherent revoked profile control: %v / %v", encodeErr, digestErr)
					}
					row := f.sql.heads[localInstallationHeadKey(receipt.Profile)]
					row[0], row[3], row[4], row[5] = string(raw), int64(v.RevocationGeneration), string(v.State), digest
				case "source-closed":
					_ = source.Close()
				case "foreign-source":
					other := newOriginalLocalRecoveryFixture(t, 0)
					source = other.source
				case "copied-source":
					source = new(localoperator.Source)
					reflect.ValueOf(source).Elem().Set(reflect.ValueOf(f.source).Elem())
				case "receipt-digest":
					receipt.Digest = strings.Repeat("f", 64)
				case "foreign-scope":
					scope.SessionID = "foreign-session"
				}
				heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
				got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), source, receipt, scope)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) {
					t.Fatalf("%s authenticated/materialized substituted history: resultZero=%t err=%v", fault, reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
				}
				assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
			})
		})
	}
}

func TestLocalSourceRecoverySourceCloseDuringReadDiscardsCandidate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		recovery := requireOriginalLocalSourceRecovery(t, f.store)
		heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
		f.sql.afterCommit = func() { _ = f.source.Close() }
		got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) {
			t.Fatalf("Source closure at read commit released candidate: resultZero=%t err=%v", reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
		}
		assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
	})
}

func TestLocalSourceRecoverySharesOriginalBudget(t *testing.T) {
	for _, scenario := range []string{"two-callbacks-consumed", "sticky-byte-exhaustion", "statement-exhaustion", "original-deadline-expired", "caller-cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 4)
				want := f.packageFromOriginalRows(t)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				ctx, cancel := ownedAuthorityCallContext(t.Context())
				defer cancel()
				budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				deadline, _ := ctx.Deadline()
				switch scenario {
				case "two-callbacks-consumed":
					for phase := 0; phase < 2; phase++ {
						if err := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { return nil }); err != nil {
							t.Fatalf("original budget prephase%d: %v", phase, err)
						}
					}
				case "sticky-byte-exhaustion":
					if !errors.Is(chargeOwnedAuthorityBody(ctx, ownedAuthorityMaxBytes+1), domain.ErrMembershipDenied) {
						t.Fatal("causal budget setup did not exhaust original allowance")
					}
				case "statement-exhaustion":
					budget.statements = ownedAuthorityMaxTotalStatements
				case "original-deadline-expired":
					time.Sleep(time.Until(deadline))
					synctest.Wait()
				case "caller-cancelled":
					cancel()
				}
				f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
				heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
				statements := len(f.sql.statements)
				got, err := recovery.RecoverLocalOwnedExecutionPackage(ctx, f.source, f.receipt, f.scope)
				if scenario == "two-callbacks-consumed" {
					if err != nil || !reflect.DeepEqual(got, want) || budget.callbacks != 3 {
						t.Fatalf("recovery reset/failed inherited original callback budget: callbacks=%d equal=%t err=%v", budget.callbacks, reflect.DeepEqual(got, want), err)
					}
					statements = len(f.sql.statements)
					got, err = recovery.RecoverLocalOwnedExecutionPackage(ctx, f.source, f.receipt, f.scope)
				}
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || len(f.sql.statements) != statements {
					t.Fatalf("%s reset exhausted/dead original call: callbacks=%d statements=%d/%d resultZero=%t err=%v", scenario, budget.callbacks, len(f.sql.statements), statements, reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
				}
				assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
			})
		})
	}
}

// Record bytes are inspectable DATA in tests, never a substitute source/grant.
// Assert the immutable audit already retains exact original postimages and
// original mutation keys. A well-formed opaque intent hash here is DATA only;
// this assertion does not prove its original grant preimage/authentication.
func TestLocalSourceRecoveryOriginalAuditContainsExactPostimage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		for _, v := range f.rows {
			l := v.Locator
			key := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID), v.MutationID})
			row := f.sql.fundingAudits[key]
			var decoded domain.AttemptSpendingAuthorizationCurrentV1
			if len(row) != 7 || json.Unmarshal([]byte(row[1].(string)), &decoded) != nil || !reflect.DeepEqual(decoded, v) || domain.ValidateSHA256Digest("original.audit.intent", row[0].(string)) != nil {
				t.Fatal("original immutable audit lost exact component/mutation/postimage/intent commitment")
			}
		}
	})
}

type wrappedOriginalLocalRecoverySource struct{ *localoperator.Source }

func TestLocalSourceRecoveryRequiresInstalledOriginalPublishers(t *testing.T) {
	for _, fault := range []string{"profile-nil", "spending-nil", "profile-wrapper", "spending-wrapper", "profile-copy", "spending-copy", "profile-foreign", "spending-foreign"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 4)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				if _, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope); err != nil {
					t.Fatalf("same-original publisher positive control: %v", err)
				}
				var profile ports.OwnedProfilePublicationVerifierV1 = f.source
				var spending ports.AttemptSpendingPublicationVerifierV1 = f.source
				if strings.HasSuffix(fault, "wrapper") {
					wrapper := &wrappedOriginalLocalRecoverySource{f.source}
					profile, spending = wrapper, wrapper
				} else if strings.HasSuffix(fault, "copy") {
					copy := new(localoperator.Source)
					reflect.ValueOf(copy).Elem().Set(reflect.ValueOf(f.source).Elem())
					profile, spending = copy, copy
				} else if strings.HasSuffix(fault, "foreign") {
					other := newOriginalLocalRecoveryFixture(t, 0)
					profile, spending = other.source, other.source
				} else {
					profile, spending = nil, nil
				}
				if strings.HasPrefix(fault, "profile-") {
					f.store.ownedProfilePublicationVerifier = profile
				} else {
					f.store.attemptSpendingPublicationVerifier = spending
				}
				heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
				queries := len(f.sql.statements)
				got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || len(f.sql.statements) != queries {
					t.Fatalf("%s substituted original installed publisher: resultZero=%t queries=%d/%d err=%v", fault, reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), len(f.sql.statements), queries, err)
				}
				assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
			})
		})
	}
}

func TestLocalSourceRecoveryAllMissingMeasuredBoundedGraph(t *testing.T) {
	for _, mode := range []string{"normal", "persisted-retry", "rollback-retry"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 0)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
				before := len(f.sql.statements)
				writesBefore := f.sql.execCalls
				var statementsPerCallback []int
				f.sql.afterCommit = func() {
					statementsPerCallback = append(statementsPerCallback, len(f.sql.statements)-before)
					before = len(f.sql.statements)
				}
				if mode == "persisted-retry" {
					f.sql.commitModes = []string{"", "ambiguous", ""}
				} else if mode == "rollback-retry" {
					f.sql.commitModes = []string{"", "rollback", ""}
				}
				call, cancel := ownedAuthorityCallContext(t.Context())
				defer cancel()
				budget := call.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				got, err := recovery.RecoverLocalOwnedExecutionPackage(call, f.source, f.receipt, f.scope)
				if err != nil || f.source.VerifyOwnedAttemptEffectSource(got.Source) != nil || len(f.sql.fundingHeads) != 4 || len(f.sql.fundingAudits) != 4 {
					t.Fatalf("all-missing %s failed actual Source/Store graph: heads=%d audits=%d callbacks=%v err=%v", mode, len(f.sql.fundingHeads), len(f.sql.fundingAudits), statementsPerCallback, err)
				}
				want := []int{7, 23}
				if mode == "persisted-retry" {
					want = []int{7, 23, 15}
				} else if mode == "rollback-retry" {
					want = []int{7, 23, 23}
				}
				if !reflect.DeepEqual(statementsPerCallback, want) || budget.callbacks != len(want) || budget.statements > ownedAuthorityMaxTotalStatements || budget.bytes > ownedAuthorityMaxBytes || budget.failed || f.sql.deadlineMissing {
					t.Fatalf("actual recovery reset/exceeded original budget: perCallback=%v want=%v callbacks=%d statements=%d bytes=%d failed=%t missingDeadline=%t", statementsPerCallback, want, budget.callbacks, budget.statements, budget.bytes, budget.failed, f.sql.deadlineMissing)
				}
				for _, n := range statementsPerCallback {
					if n > ownedAuthorityMaxStatements {
						t.Fatalf("callback exceeded original statement bound: %d", n)
					}
				}
				if mode != "rollback-retry" && f.sql.execCalls-writesBefore != 8 || mode == "rollback-retry" && f.sql.execCalls-writesBefore != 16 {
					t.Fatalf("%s repeated a persisted initial publication or lost rollback accounting: writes=%d", mode, f.sql.execCalls-writesBefore)
				}
			})
		})
	}
}

func TestLocalSourceRecoveryOriginalCapAndLaterDBPublication(t *testing.T) {
	for _, mode := range []string{"later-publication", "original-cap-expired"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 2)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				earliest := f.rows[0].Window.ExpiresAt
				if f.rows[1].Window.ExpiresAt.Before(earliest) {
					earliest = f.rows[1].Window.ExpiresAt
				}
				if mode == "original-cap-expired" {
					time.Sleep(time.Until(earliest) + time.Microsecond)
					f.sql.at = time.Now().UTC().Truncate(time.Microsecond)
				} else {
					time.Sleep(11 * time.Second)
					// The scripted authoritative DB observation deliberately follows
					// the real issuer observation. They are NOT the same timestamp.
					f.sql.at = time.Now().UTC().Truncate(time.Microsecond).Add(2 * time.Microsecond)
				}
				if _, err := f.source.Configuration(); err != nil {
					t.Fatalf("finite Attempt control expired the installation: %v", err)
				}
				heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
				got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
				if mode == "original-cap-expired" {
					if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) {
						t.Fatalf("elapsed original envelope renewed/materialized: %v", err)
					}
					assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
					return
				}
				if err != nil || f.source.VerifyOwnedAttemptEffectSource(got.Source) != nil || !got.Source.IssuedAt.Equal(f.sql.at) || got.Source.ExpiresAt.After(earliest) {
					t.Fatalf("later DB publication changed producer/window semantics: issue=%v db=%v expiry=%v original=%v err=%v", got.Source.IssuedAt, f.sql.at, got.Source.ExpiresAt, earliest, err)
				}
				for _, row := range got.Source.Spending[2:] {
					if !row.Window.ObservedAt.Before(row.Window.PublishedAt) || row.Window.ExpiresAt.After(earliest) {
						t.Fatalf("original signing clock backdated/renewed: observed=%v published=%v end=%v original=%v", row.Window.ObservedAt, row.Window.PublishedAt, row.Window.ExpiresAt, earliest)
					}
				}
			})
		})
	}
}

func TestLocalSourceRecoveryPrivateHelperNeverCreatesBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		life, err := f.source.ExecutionLifetime()
		if err != nil {
			t.Fatal(err)
		}
		before := len(f.sql.statements)
		got, err := f.store.recoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope, life)
		if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || len(f.sql.statements) != before {
			t.Fatalf("private helper created/entered an unbounded logical call: queries=%d/%d err=%v", len(f.sql.statements), before, err)
		}
	})
}

func TestLocalSourceRecoveryRejoinsBetweenPhases(t *testing.T) {
	for _, fault := range []string{"source-close", "current-revoke", "scope-MAC-substitution"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 2)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				writes := f.sql.execCalls
				fired := false
				f.sql.afterCommit = func() {
					if fired {
						return
					}
					fired = true
					switch fault {
					case "source-close":
						_ = f.source.Close()
					case "current-revoke", "scope-MAC-substitution":
						v := f.rows[0].Clone()
						if fault == "current-revoke" {
							v.State, v.RevocationGeneration, v.MutationID = domain.OwnedAgentCurrentRevokedV1, 2, "recovery-current-revocation"
						} else {
							last := byte('0')
							if v.SourceAuthorization.Ref[len(v.SourceAuthorization.Ref)-1] == last {
								last = '1'
							}
							v.SourceAuthorization.Ref = v.SourceAuthorization.Ref[:len(v.SourceAuthorization.Ref)-1] + string(last)
						}
						raw, encodeErr := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
						digest, digestErr := v.Digest()
						if encodeErr != nil || digestErr != nil {
							t.Fatalf("coherent between-phase control: %v / %v", encodeErr, digestErr)
						}
						l := v.Locator
						key := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID)})
						row := f.sql.fundingHeads[key]
						row[0], row[2], row[3], row[4] = string(raw), int64(v.RevocationGeneration), string(v.State), digest
						if fault == "scope-MAC-substitution" {
							auditKey := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID), v.MutationID})
							f.sql.fundingAudits[auditKey][1], f.sql.fundingAudits[auditKey][2] = string(raw), digest
						}
					}
				}
				got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
				if !fired || !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || f.sql.execCalls != writes || len(f.sql.fundingHeads) != 2 || len(f.sql.fundingAudits) != 2 {
					t.Fatalf("%s reused first snapshot as write permission: fired=%t writes=%d/%d heads=%d audits=%d resultZero=%t err=%v", fault, fired, f.sql.execCalls, writes, len(f.sql.fundingHeads), len(f.sql.fundingAudits), reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
				}
			})
		})
	}
}

func TestLocalSourceRecoveryWriteCommitCancellationDiscardsCandidate(t *testing.T) {
	for _, fault := range []string{"source-close", "caller-cancelled"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 2)
				recovery := requireOriginalLocalSourceRecovery(t, f.store)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				commits := 0
				f.sql.afterCommit = func() {
					commits++
					if commits != 2 {
						return
					}
					if fault == "source-close" {
						_ = f.source.Close()
					} else {
						cancel()
					}
				}
				got, err := recovery.RecoverLocalOwnedExecutionPackage(ctx, f.source, f.receipt, f.scope)
				// SQL may have committed before cancellation is observed. That is
				// not rollback evidence and must not release a speculative package.
				if commits != 2 || len(f.sql.fundingHeads) != 4 || len(f.sql.fundingAudits) != 4 || !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) {
					t.Fatalf("%s at materialization commit leaked candidate or assumed rollback: commits=%d heads=%d audits=%d zero=%t err=%v", fault, commits, len(f.sql.fundingHeads), len(f.sql.fundingAudits), reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
				}
			})
		})
	}
}

func TestLocalSourceRecoveryConcurrentOriginalPublicationDeniesReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 3)
		recovery := requireOriginalLocalSourceRecovery(t, f.store)
		v := f.rows[2]
		l := v.Locator
		key := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID)})
		auditKey := localInstallationSQLKey([]driver.Value{string(l.TenantID), string(l.AttemptID), string(l.ComponentKind), string(l.BillingOwnerUserID), v.MutationID})
		// Capture the exact postimage from a genuine original Source/Store
		// publication. This single-connection SQL model makes that competing
		// postimage visible only after TX1, without nested SQL in driver.Commit.
		head, audit := f.sql.fundingHeads[key], f.sql.fundingAudits[auditKey]
		delete(f.sql.fundingHeads, key)
		delete(f.sql.fundingAudits, auditKey)
		writes := f.sql.execCalls
		published := false
		f.sql.afterCommit = func() {
			if published {
				return
			}
			published = true
			f.sql.fundingHeads[key], f.sql.fundingAudits[auditKey] = head, audit
		}
		got, err := recovery.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
		if !published || !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) || f.sql.execCalls != writes || len(f.sql.fundingHeads) != 3 || len(f.sql.fundingAudits) != 3 || !reflect.DeepEqual(head, f.sql.fundingHeads[key]) || !reflect.DeepEqual(audit, f.sql.fundingAudits[auditKey]) {
			t.Fatalf("concurrent publication adopted as original prepared retry: published=%t writes=%d/%d heads=%d audits=%d zero=%t err=%v", published, f.sql.execCalls, writes, len(f.sql.fundingHeads), len(f.sql.fundingAudits), reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}), err)
		}
	})
}
