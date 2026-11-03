package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

func TestLocalSourceTextOnlyActualPublicationRecovery(t *testing.T) {
	synctest.Test(t, testLocalSourceTextOnlyActualPublicationRecovery)
}

func testLocalSourceTextOnlyActualPublicationRecovery(t *testing.T) {
	f := newLocalTextInstallation(t, "http://127.0.0.1:11434/v1")
	f.scope = localSearchScope(f.receipt.Profile, "original-text-only")
	textOnlyRegisterAttemptKeys(f, f.scope)
	pkg, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
	if err != nil {
		t.Fatalf("actual original two-head recovery: %v", err)
	}
	if len(pkg.Source.Spending) != 2 || len(f.sql.fundingHeads) != 2 || len(f.sql.fundingAudits) != 2 || pkg.Source.Profile.Locator.Profile != domain.AgentLoopOwnedLocalTextV1 {
		t.Fatal("text recovery invented service funding or lost profile")
	}
	if err := f.source.VerifyOwnedAttemptEffectSource(pkg.Source); err != nil {
		t.Fatalf("private Source authenticates original actual two-head package: %v", err)
	}
	again, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
	if err != nil || !reflect.DeepEqual(pkg, again) || len(f.sql.fundingHeads) != 2 || len(f.sql.fundingAudits) != 2 {
		t.Fatalf("original exact recovery changed package/top-up err=%v", err)
	}
	reopened, err := localoperator.OpenExisting(t.Context(), f.root, f.exe, f.receipt)
	if err != nil {
		t.Fatalf("text Source reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	f.store.ownedProfilePublicationVerifier, f.store.attemptSpendingPublicationVerifier, f.store.ownedAttemptEffectSourceVerifier = reopened, reopened, reopened
	resumed, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), reopened, f.receipt, f.scope)
	if err != nil || !reflect.DeepEqual(pkg, resumed) {
		t.Fatalf("same original receipt/open recovery: %v", err)
	}
	for _, kind := range ownedSpendingKinds[2:] {
		if _, err := reopened.SpendingPublication(t.Context(), f.scope, kind); err == nil {
			t.Fatalf("text Source issued disabled %s", kind)
		}
	}
}

func TestLocalSourceTextOnlyCurrentHeadsAndServiceAbsence(t *testing.T) {
	for _, name := range []string{"current", "missing_provider", "changed_provider_scalar", "foreign_mcp_head", "foreign_search_head", "same_key_mcp_head", "same_key_search_head", "partial_presence", "data_without_presence", "invalid_utf8_last_column_charged", "oversize_body_last_column_charged", "wrong_type_last_column_charged"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newLocalTextInstallation(t, "http://127.0.0.1:11434/v1")
				f.scope = localSearchScope(f.receipt.Profile, "text-current")
				textOnlyRegisterAttemptKeys(f, f.scope)
				pkg, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
				if err != nil {
					t.Fatalf("real two-head original baseline: %v", err)
				}
				peer := &localTextSourcesSQL{localInstallationSQLFixture: f.sql}
				db := sql.OpenDB(peer)
				db.SetMaxOpenConns(1)
				t.Cleanup(func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				})
				f.store.db = db
				read := func() (int, uint64, error) {
					var statements int
					var materialized uint64
					err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
						current, err := readOwnedAdmissionSources(ctx, q, pkg.Source)
						statements, materialized = q.budget.callbackStatements, q.budget.bytes
						if err == nil && (len(current.spending) != 2 || current.profile.Locator.Profile != domain.AgentLoopOwnedLocalTextV1) {
							t.Fatal("current reader invented service head")
						}
						return err
					})
					return statements, materialized, err
				}
				statements, materialized, err := read()
				if err != nil || statements != 2 || materialized == 0 {
					t.Fatalf("actual current/absence baseline statements=%d bytes=%d err=%v", statements, materialized, err)
				}
				if name == "current" {
					return
				}
				var extraBytes uint64
				key := func(kind domain.AttemptSpendingComponentKindV1) string {
					return localInstallationSQLKey([]driver.Value{string(f.scope.TenantID), string(f.scope.AttemptID), string(kind), string(f.receipt.Profile.BillingOwnerUserID)})
				}
				switch name {
				case "partial_presence", "data_without_presence", "invalid_utf8_last_column_charged", "oversize_body_last_column_charged", "wrong_type_last_column_charged":
					peer.absenceOverride = make([]driver.Value, 24)
					peer.absenceOverride[23] = strings.Repeat("z", 65)
					extraBytes = 65
					switch name {
					case "partial_presence":
						peer.absenceOverride[0] = "mcp_tool"
						extraBytes += 8
					case "data_without_presence":
					case "invalid_utf8_last_column_charged":
						peer.absenceOverride[0] = []byte{0xff}
						extraBytes++
					case "oversize_body_last_column_charged":
						peer.absenceOverride[1] = strings.Repeat("x", 32769)
						extraBytes += 32769
					case "wrong_type_last_column_charged":
						peer.absenceOverride[2] = uint64(1)
						extraBytes += 8
					}
				case "missing_provider":
					delete(f.sql.fundingHeads, key(domain.AttemptSpendingProviderV1))
				case "changed_provider_scalar":
					row := append([]driver.Value(nil), f.sql.fundingHeads[key(domain.AttemptSpendingProviderV1)]...)
					row[7] = "foreign-owner"
					f.sql.fundingHeads[key(domain.AttemptSpendingProviderV1)] = row
				case "foreign_mcp_head":
					f.sql.fundingHeads[key(domain.AttemptSpendingMCPToolV1)] = append([]driver.Value(nil), f.sql.spendingRows[2]...)
				case "foreign_search_head":
					f.sql.fundingHeads[key(domain.AttemptSpendingWebSearchV1)] = append([]driver.Value(nil), f.sql.spendingRows[3]...)
				case "same_key_mcp_head", "same_key_search_head":
					index := 2
					if name == "same_key_search_head" {
						index = 3
					}
					kind := ownedSpendingKinds[index]
					v, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(f.sql.spendingRows[index][0].(string)))
					if err != nil {
						t.Fatal(err)
					}
					// Valid foreign-profile DATA under the exact expected physical
					// key. This is an adversarial row, never Source issuance or an
					// added text funding reference. Recompute every commitment.
					v.Locator.TenantID, v.Locator.AttemptID, v.Locator.BillingOwnerUserID = f.scope.TenantID, f.scope.AttemptID, f.receipt.Profile.BillingOwnerUserID
					v.RunID, v.SessionID, v.OwnerUserID = f.scope.RunID, f.scope.SessionID, f.scope.OwnerUserID
					v.SourceProfile.Locator.TenantID, v.SourceProfile.Locator.OwnerUserID = f.scope.TenantID, f.scope.OwnerUserID
					v.Window, v.UpdatedAt = f.receipt.Profile.Window, f.receipt.Profile.UpdatedAt
					bindReaderSpendingSource(t, &v)
					raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
					if err != nil || v.ValidateAt(f.sql.at) != nil {
						t.Fatalf("canonical same-key adversarial DATA: %v", err)
					}
					digest, err := v.Digest()
					if err != nil {
						t.Fatal(err)
					}
					f.sql.fundingHeads[key(kind)] = []driver.Value{string(raw), int64(v.Version), int64(v.RevocationGeneration), string(v.State), digest, string(v.RunID), string(v.SessionID), string(v.OwnerUserID), v.Window.PublishedAt, v.Window.ExpiresAt, v.UpdatedAt}
					if err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
						loaded, found, err := loadOwnedSpendingProjection(ctx, q, v.Locator)
						if err != nil || !found || !reflect.DeepEqual(loaded, v) {
							t.Fatalf("same-key physical DATA must pass projection before absence decision: found=%t err=%v", found, err)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				_, charged, err := read()
				if err == nil {
					t.Fatalf("current-source reader accepted %s", name)
				}
				if extraBytes != 0 && charged < materialized+extraBytes {
					t.Fatalf("all57 columns must be charged even after first invalid scalar: baseline=%d got=%d extra=%d", materialized, charged, extraBytes)
				}
			})
		})
	}
}

func TestLocalSourceTextOnlyPartialPublicationAndLostACK(t *testing.T) {
	for _, published := range []int{0, 1, 2} {
		for _, lost := range []bool{false, true} {
			t.Run(fmt.Sprintf("published%d/lost%t", published, lost), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := newLocalTextInstallation(t, "http://127.0.0.1:11434/v1")
					f.scope = localSearchScope(f.receipt.Profile, "partial-text")
					textOnlyRegisterAttemptKeys(f, f.scope)
					for _, kind := range ownedSpendingKinds[:published] {
						g, err := f.source.SpendingPublication(t.Context(), f.scope, kind)
						if err != nil {
							t.Fatal(err)
						}
						f.sql.at = g.IssuedAt
						if _, err := f.store.PublishAttemptSpending(t.Context(), g); err != nil {
							t.Fatal(err)
						}
					}
					heads, audits := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits)
					if lost {
						if published == 2 {
							f.sql.commitModes = []string{"ambiguous"}
						} else {
							f.sql.commitModes = []string{"", "ambiguous"}
						}
					}
					pkg, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
					if err != nil || len(pkg.Source.Spending) != 2 || len(f.sql.fundingHeads) != 2 || len(f.sql.fundingAudits) != 2 || f.source.VerifyOwnedAttemptEffectSource(pkg.Source) != nil {
						t.Fatalf("original missing-only text recovery published=%d lost=%t err=%v", published, lost, err)
					}
					for key, row := range heads {
						if !reflect.DeepEqual(row, f.sql.fundingHeads[key]) {
							t.Fatal("text recovery renewed original head")
						}
					}
					for key, row := range audits {
						if !reflect.DeepEqual(row, f.sql.fundingAudits[key]) {
							t.Fatal("text recovery rewrote original audit")
						}
					}
					if len(f.sql.commitModes) != 0 {
						t.Fatal("lost-ACK fixture did not exercise original commit boundary")
					}
					again, err := f.store.RecoverLocalOwnedExecutionPackage(t.Context(), f.source, f.receipt, f.scope)
					if err != nil || !reflect.DeepEqual(pkg, again) {
						t.Fatalf("lost-ACK result was not exact original replay: %v", err)
					}
				})
			})
		}
	}
}
