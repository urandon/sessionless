package ydbstore

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func localBinderRequest(scope domain.AttemptExecutionSourceScopeV1, at time.Time) ports.HarnessBindingRequest {
	return ports.HarnessBindingRequest{TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, SessionID: scope.SessionID,
		RunID: scope.RunID, AttemptID: scope.AttemptID, At: at}
}

func TestLocalHarnessBinderOriginalCompleteSourceIsReadOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		binder, err := NewLocalHarnessBinder(f.store, f.source, f.receipt)
		if err != nil {
			t.Fatal("construct binder from original Source/Store/receipt", err)
		}
		// Event time remains real event DATA. It predates the DB-observed
		// publication; candidate validity uses original Source issuance instead.
		request := localBinderRequest(f.scope, f.receipt.Profile.Window.PublishedAt.Add(-time.Minute))
		heads, audits, writes := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits), f.sql.execCalls
		profileHeads, profileAudits := cloneLocalInstallationRows(f.sql.heads), cloneLocalInstallationRows(f.sql.audits)
		got, err := binder.BindHarness(t.Context(), request)
		if err != nil || request.SubscriptionConnectionID != "" || got.LocalCapacityAssociation == nil || got.ValidateForScope(request) != nil {
			t.Fatalf("original explicit zero-subscription binding: denied=%t local=%t", err != nil, got.LocalCapacityAssociation != nil)
		}
		source, err := domain.DecodeAttemptExecutionSourceV1([]byte(got.OwnedExecutionSourceRecord))
		if err != nil || source.Scope != f.scope || !request.At.Before(source.IssuedAt) || f.source.VerifyOwnedAttemptEffectSource(source) != nil ||
			got.LocalCapacityAssociation.ValidateForOwnedSource(got.OwnedExecutionSourceRecord, got.ExecutionPlacementV2, got.HarnessBinding, got.SubstrateBinding, got.AdmissionCostCeiling) != nil {
			t.Fatalf("candidate lost exact strict original Source/event-time distinction: denied=%t eventPredatesIssue=%t", err != nil, request.At.Before(source.IssuedAt))
		}
		want := got.Clone()
		again, err := binder.BindHarness(t.Context(), request)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("original candidate changed on repeat read: equal=%t denied=%t", reflect.DeepEqual(again, want), err != nil)
		}
		assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
		if !reflect.DeepEqual(profileHeads, f.sql.heads) || !reflect.DeepEqual(profileAudits, f.sql.audits) {
			t.Fatal("binder rewrote original installed profile/audit")
		}
	})
}

func TestLocalHarnessBinderMaterializesTwoOriginalScopes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 0)
		binder, err := NewLocalHarnessBinder(f.store, f.source, f.receipt)
		if err != nil {
			t.Fatal("construct original local binder", err)
		}
		profileHeads, profileAudits := cloneLocalInstallationRows(f.sql.heads), cloneLocalInstallationRows(f.sql.audits)
		var results [2]ports.ExecutionAuthorityV2
		for i, suffix := range []string{"binder-one", "binder-two"} {
			scope := localSearchScope(f.receipt.Profile, suffix)
			// The controlled SQL driver accepts ONLY explicitly registered exact
			// locator keys. This adds an empty lookup key, not a published grant.
			for _, kind := range ownedSpendingKinds {
				key := localInstallationSQLKey([]driver.Value{string(scope.TenantID), string(scope.AttemptID), string(kind), string(f.receipt.Profile.BillingOwnerUserID)})
				f.sql.fundingKeys[key] = true
			}
			request := localBinderRequest(scope, f.sql.at)
			writes := f.sql.execCalls
			got, err := binder.BindHarness(t.Context(), request)
			if err != nil || got.ValidateForScope(request) != nil || got.LocalCapacityAssociation == nil || f.sql.execCalls-writes != 8 {
				t.Fatalf("new original scope %d missing-only publication: denied=%t local=%t writes=%d want8", i+1, err != nil, got.LocalCapacityAssociation != nil, f.sql.execCalls-writes)
			}
			source, err := domain.DecodeAttemptExecutionSourceV1([]byte(got.OwnedExecutionSourceRecord))
			if err != nil || source.Scope != scope || f.source.VerifyOwnedAttemptEffectSource(source) != nil {
				t.Fatalf("new scope %d lost original authenticated Source: denied=%t", i+1, err != nil)
			}
			results[i] = got.Clone()
			heads, audits := cloneLocalInstallationRows(f.sql.fundingHeads), cloneLocalInstallationRows(f.sql.fundingAudits)
			writes = f.sql.execCalls
			again, err := binder.BindHarness(t.Context(), request)
			if err != nil || !reflect.DeepEqual(again, got) {
				t.Fatalf("scope %d replay changed original candidate: denied=%t equal=%t", i+1, err != nil, reflect.DeepEqual(again, got))
			}
			assertOriginalRecoveryNoWrites(t, f, writes, heads, audits)
		}
		if len(f.sql.fundingHeads) != 8 || len(f.sql.fundingAudits) != 8 ||
			results[0].HarnessBinding.RunID == results[1].HarnessBinding.RunID || results[0].HarnessBinding.AttemptID == results[1].HarnessBinding.AttemptID ||
			*results[0].LocalCapacityAssociation != *results[1].LocalCapacityAssociation ||
			!reflect.DeepEqual(profileHeads, f.sql.heads) || !reflect.DeepEqual(profileAudits, f.sql.audits) {
			t.Fatal("two original scopes replaced funding/profile or invented separate local resource capacity")
		}
	})
}

func TestLocalHarnessBinderRejectsForeignOrClosedAuthority(t *testing.T) {
	for _, fault := range []string{"tenant", "owner", "subscription", "session", "run", "attempt", "zero event time", "nil context", "cancelled context", "closed source", "different verifier"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newOriginalLocalRecoveryFixture(t, 4)
				binder, err := NewLocalHarnessBinder(f.store, f.source, f.receipt)
				if err != nil {
					t.Fatal("original binder control", err)
				}
				request, ctx := localBinderRequest(f.scope, f.sql.at), t.Context()
				switch fault {
				case "tenant":
					request.TenantID = "foreign"
				case "owner":
					request.OwnerUserID = "foreign"
				case "subscription":
					request.SubscriptionConnectionID = "invented-subscription"
				case "session":
					request.SessionID = "invalid session"
				case "run":
					request.RunID = ""
				case "attempt":
					request.AttemptID = ""
				case "zero event time":
					request.At = time.Time{}
				case "nil context":
					ctx = nil
				case "cancelled context":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(t.Context())
					cancel()
				case "closed source":
					if err := f.source.Close(); err != nil {
						t.Fatal("close original source", err)
					}
				case "different verifier":
					other := newOriginalLocalRecoveryFixture(t, 0)
					f.store.attemptSpendingPublicationVerifier = other.source
				}
				statements, writes := len(f.sql.statements), f.sql.execCalls
				got, err := binder.BindHarness(ctx, request)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.ExecutionAuthorityV2{}) || f.sql.execCalls != writes || len(f.sql.statements) != statements {
					t.Fatalf("invalid binder input crossed pre-SQL gate: fault=%s denied=%t zero=%t writes=%d queries=%d", fault, errors.Is(err, domain.ErrMembershipDenied), reflect.DeepEqual(got, ports.ExecutionAuthorityV2{}), f.sql.execCalls-writes, len(f.sql.statements)-statements)
				}
				if fault == "closed source" || fault == "different verifier" {
					fresh, err := NewLocalHarnessBinder(f.store, f.source, f.receipt)
					if !errors.Is(err, domain.ErrMembershipDenied) || fresh != nil || len(f.sql.statements) != statements {
						t.Fatalf("constructor accepted unavailable original authority: fault=%s denied=%t nil=%t queries=%d", fault, errors.Is(err, domain.ErrMembershipDenied), fresh == nil, len(f.sql.statements)-statements)
					}
				}
			})
		})
	}
}

func TestLocalHarnessBinderReceiptAndResultAreOwnedCopies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newOriginalLocalRecoveryFixture(t, 4)
		receipt := f.receipt
		receipt.Profile = receipt.Profile.Clone()
		binder, err := NewLocalHarnessBinder(f.store, f.source, receipt)
		if err != nil {
			t.Fatal("construct original receipt holder", err)
		}
		receipt.Profile.InputModalities[0] = "caller-mutation"
		*receipt.Profile.CostCeiling.MCPToolCost.MaxAmountMicrounits++
		request := localBinderRequest(f.scope, f.sql.at)
		got, err := binder.BindHarness(t.Context(), request)
		if err != nil {
			t.Fatal("caller mutation rewrote constructor-owned receipt", err)
		}
		want := got.Clone()
		got.LocalCapacityAssociation.ResourceID = "caller-mutation"
		got.HarnessBinding.AgentLoopPolicy.InputModalities[0] = "caller-mutation"
		*got.AdmissionCostCeiling.MCPToolCost.MaxAmountMicrounits++
		again, err := binder.BindHarness(t.Context(), request)
		if err != nil || !reflect.DeepEqual(again, want) {
			t.Fatalf("caller mutation rewrote binder/source authority: denied=%t equal=%t", err != nil, reflect.DeepEqual(again, want))
		}
		badReceipt := f.receipt
		badReceipt.Digest = strings.Repeat("f", 64)
		before := len(f.sql.statements)
		if denied, err := NewLocalHarnessBinder(f.store, f.source, badReceipt); !errors.Is(err, domain.ErrMembershipDenied) || denied != nil || len(f.sql.statements) != before {
			t.Fatalf("foreign receipt accepted or queried: denied=%t nil=%t queries=%d", errors.Is(err, domain.ErrMembershipDenied), denied == nil, len(f.sql.statements)-before)
		}
	})
}
