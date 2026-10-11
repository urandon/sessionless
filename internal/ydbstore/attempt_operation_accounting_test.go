package ydbstore

import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

func installOperationIssuer(t *testing.T, f *ownedAttemptEffectParentFixture) {
	t.Helper()
	issuer, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return f.graph.at }, strings.NewReader(strings.Repeat("k", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	f.store.attemptOperationIssuer = issuer
}

func TestOwnedOperationSetupAccountingCommitsWithOriginalPreparationBurn(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	installOperationIssuer(t, f)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("authentic outer ownership grant absent")
	}
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || !fresh || f.sql.accountingRow == nil {
		t.Fatalf("confirmed setup/burn: fresh=%t err=%v accounting=%v", fresh, err, f.sql.accountingRow)
	}
	var accounting ownedAttemptAccounting
	if err := json.Unmarshal([]byte(f.sql.accountingRow[2].(string)), &accounting); err != nil {
		t.Fatal(err)
	}
	if accounting.validate(f.parent, f.source, f.at) != nil || !accounting.SetupReservedAt.Equal(f.sql.burnRow[2].(time.Time)) || accounting.Amounts[1] != *f.source.Spending[1].MaxAmountMicrounits || accounting.Aggregate.Operations != 0 {
		t.Fatal("setup must be exact original substrate envelope at the SAME burn transaction time")
	}
	original := append([]driver.Value(nil), f.sql.accountingRow...)
	fresh, err = beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || fresh || !reflect.DeepEqual(original, f.sql.accountingRow) {
		t.Fatalf("historical preparation must not repair/redebit setup: fresh=%t err=%v", fresh, err)
	}
}

func TestOwnedOperationSetupAccountingRollbackLeavesNoBurnOrDebit(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	installOperationIssuer(t, f)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("authentic outer ownership grant absent")
	}
	f.graph.commitModes = []string{"rollback", "rollback", "rollback"}
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err == nil || fresh || f.sql.accountingRow != nil || f.sql.burnRow[0] != uint64(0) || f.sql.activeTransactions != 0 {
		t.Fatalf("rollback retained setup/burn or returned permission: fresh=%t err=%v accounting=%v burn=%v active=%d", fresh, err, f.sql.accountingRow, f.sql.burnRow, f.sql.activeTransactions)
	}
}

func TestOwnedOperationSetupAccountingLostAcknowledgementNeverReprepares(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	installOperationIssuer(t, f)
	created := f.reserve(t, f.request)
	if created.Grant == nil {
		t.Fatal("authentic original grant absent")
	}
	f.graph.commitModes = []string{"ambiguous", ""}
	fresh, err := beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || fresh || f.sql.accountingRow == nil || f.sql.burnUpdates != 1 {
		t.Fatalf("committed lost ACK must return history without preparing: fresh=%t err=%v updates=%d", fresh, err, f.sql.burnUpdates)
	}
	original := append([]driver.Value(nil), f.sql.accountingRow...)
	fresh, err = beginOwnedPreparation(t, f, *created.Grant)
	if err != nil || fresh || f.sql.burnUpdates != 1 || !reflect.DeepEqual(original, f.sql.accountingRow) {
		t.Fatal("lost ACK recovery repaired or renewed setup")
	}
}

func TestOwnedOperationComponentDebitRequiresActualKnownPriceAndCannotRefund(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	pd, err := f.parent.Digest()
	if err != nil {
		t.Fatal(err)
	}
	initial := ownedAttemptAccounting{Version: 1, ParentDigest: pd, SetupReservedAt: f.at, UpdatedAt: f.at}
	initial.Amounts[1] = *f.source.Spending[1].MaxAmountMicrounits
	if initial.validate(f.parent, f.source, f.at) != nil {
		t.Fatal("valid original setup accounting")
	}
	op := domain.AttemptOperationV1{Kind: domain.AttemptOperationModelV1, Sequence: 1, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, AmountMicrounits: *f.source.Spending[0].MaxAmountMicrounits}}
	next, err := debitOwnedOperation(initial, op, f.parent, f.source, f.at)
	if err != nil || next.Aggregate.Operations != 1 || next.Aggregate.ModelCalls != 1 || next.Amounts[0] != *f.source.Spending[0].MaxAmountMicrounits {
		t.Fatalf("actual admitted known-free provider reservation denied: next=%+v err=%v", next, err)
	}
	for _, name := range []string{"unknown_price", "underquote", "duplicate_sequence", "component_overflow", "future_update", "foreign_parent"} {
		t.Run(name, func(t *testing.T) {
			source, value, call := f.source.Clone(), next, op
			call.Sequence = 2
			switch name {
			case "unknown_price":
				source.Spending[0].MaxAmountMicrounits = nil
			case "underquote":
				call.Kind = domain.AttemptOperationMCPToolV1
				call.Debit.AmountMicrounits = 0
			case "duplicate_sequence":
				call.Sequence = 1
			case "component_overflow":
				value.Amounts[0] = math.MaxUint64
				call.Debit.AmountMicrounits = 1
			case "future_update":
				value.SetupReservedAt = f.at.Add(time.Microsecond)
			case "foreign_parent":
				value.ParentDigest = strings.Repeat("a", 64)
			}
			if _, err := debitOwnedOperation(value, call, f.parent, source, f.at); err == nil {
				t.Fatal("invalid current component accounting accepted")
			}
			if next.Aggregate.Operations != 1 || initial.Aggregate.Operations != 0 {
				t.Fatal("denial mutated/refunded original immutable value")
			}
		})
	}
}
