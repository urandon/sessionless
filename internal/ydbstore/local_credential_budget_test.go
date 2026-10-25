package ydbstore

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Independent size oracle for the fixture's physical projection, not JSON size
// and not a call to the production scanner/accounting under test. These tests
// exercise scripted SQL only, not native YDB or actual credential material.
func localCredentialProjectionBytes(t *testing.T, row []driver.Value) uint64 {
	t.Helper()
	var total uint64
	for i, value := range row {
		switch v := value.(type) {
		case string:
			total += uint64(len(v))
		case []byte:
			total += uint64(len(v))
		case int64, uint64, time.Time:
			total += 8
		case nil:
		default:
			t.Fatalf("unsupported fixture projection column=%d type=%T", i, value)
		}
	}
	return total
}

func TestLocalCredentialBudgetPhysicalProjectionSharesOriginalCall(t *testing.T) {
	f, fixture, binding, credential := localCurrentProfileFixture(t, true)
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	const bodyBytes uint64 = 19
	if err := chargeOwnedAuthorityBody(ctx, bodyBytes); err != nil {
		t.Fatalf("original authority body charge: %v", err)
	}
	original := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	projection := localCredentialProjectionBytes(t, fixture.credential)
	err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if q.budget != original {
			t.Fatal("credential read replaced original logical-call budget")
		}
		got, found, err := readProviderCredential(ctx, q, ports.ProviderCredentialLocatorV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: binding.Resource.Kind, ResourceID: binding.Resource.ResourceID})
		if err != nil || !found || got.CredentialGeneration != credential.CredentialGeneration || got.SecretRef.StorageValue() != credential.SecretRef.StorageValue() {
			t.Fatalf("credential projection read found=%t generation=%d err=%v", found, got.CredentialGeneration, err)
		}
		return err
	})
	want := bodyBytes + 8 + projection // same Tx's authoritative clock is 8 bytes
	if err != nil || original.bytes != want || original.statements != 2 || fixture.credentialReads != 1 || fixture.commits != 1 || fixture.execCalls != 0 {
		t.Fatalf("projection accounting err=%v bytes=%d want=%d statements=%d credentialReads=%d commits=%d writes=%d", err, original.bytes, want, original.statements, fixture.credentialReads, fixture.commits, fixture.execCalls)
	}
}

func TestLocalCredentialBudgetMalformedPhysicalRowsStillCharged(t *testing.T) {
	for _, name := range []string{"invalid_first_scalar", "invalid_record", "null_timestamp"} {
		t.Run(name, func(t *testing.T) {
			f, fixture, binding, _ := localCurrentProfileFixture(t, true)
			switch name {
			case "invalid_first_scalar":
				fixture.credential[0] = strings.Repeat("r", 161)
			case "invalid_record":
				fixture.credential[7] = "{"
			case "null_timestamp":
				fixture.credential[6] = nil
			}
			projection := localCredentialProjectionBytes(t, fixture.credential)
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			const previousBody uint64 = 23
			if err := chargeOwnedAuthorityBody(ctx, previousBody); err != nil {
				t.Fatal(err)
			}
			original := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
				if q.budget != original {
					t.Fatal("malformed row escaped original budget")
				}
				_, _, err := readProviderCredential(ctx, q, ports.ProviderCredentialLocatorV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: binding.Resource.Kind, ResourceID: binding.Resource.ResourceID})
				if err == nil {
					t.Fatalf("malformed credential row accepted case=%s", name)
				}
				return err
			})
			if !errors.Is(err, domain.ErrMembershipDenied) || original.bytes != previousBody+8+projection || original.statements != 2 || fixture.credentialReads != 1 || fixture.commits != 0 || fixture.rollbacks != 1 || fixture.execCalls != 0 {
				t.Fatalf("malformed=%s err=%v bytes=%d want=%d statements=%d reads=%d commits=%d rollbacks=%d writes=%d", name, err, original.bytes, previousBody+8+projection, original.statements, fixture.credentialReads, fixture.commits, fixture.rollbacks, fixture.execCalls)
			}
		})
	}
}

func TestLocalCredentialBudgetExhaustionCannotResetOrCommitIgnoredError(t *testing.T) {
	f, fixture, binding, _ := localCurrentProfileFixture(t, true)
	projection := localCredentialProjectionBytes(t, fixture.credential)
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	// One byte less than the actual credential projection remains after the
	// authoritative clock. A separate/restarted allowance would wrongly admit it.
	previous := ownedAuthorityMaxBytes - 8 - projection + 1
	if err := chargeOwnedAuthorityBody(ctx, previous); err != nil {
		t.Fatal(err)
	}
	original := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	err := f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		if q.budget != original {
			t.Fatal("credential read replaced exhausted original budget")
		}
		_, _, readErr := readProviderCredential(ctx, q, ports.ProviderCredentialLocatorV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: binding.Resource.Kind, ResourceID: binding.Resource.ResourceID})
		if !errors.Is(readErr, domain.ErrMembershipDenied) {
			t.Fatalf("over-budget physical projection accepted: %v", readErr)
		}
		return nil // Ignoring the charge error must never permit a commit.
	})
	calledAgain := false
	nextErr := f.store.ownedAuthorityTx(ctx, func(context.Context, *ownedAuthorityQuery) error { calledAgain = true; return nil })
	if !errors.Is(err, domain.ErrMembershipDenied) || !errors.Is(nextErr, domain.ErrMembershipDenied) || calledAgain || !original.failed || original.bytes != previous+8 || original.callbacks != 1 || fixture.credentialReads != 1 || fixture.commits != 0 || fixture.execCalls != 0 {
		t.Fatalf("shared exhaustion err=%v next=%v callbackAgain=%t failed=%t bytes=%d want=%d callbacks=%d reads=%d commits=%d writes=%d", err, nextErr, calledAgain, original.failed, original.bytes, previous+8, original.callbacks, fixture.credentialReads, fixture.commits, fixture.execCalls)
	}
}

func TestLocalCredentialBudgetOriginalCurrentPrerequisiteNoneHasNoCredentialRead(t *testing.T) {
	for _, bearer := range []bool{false, true} {
		t.Run(map[bool]string{false: "none", true: "bearer"}[bearer], func(t *testing.T) {
			f, fixture, binding, _ := localCurrentProfileFixture(t, bearer)
			want := uint64(8) + localCredentialProjectionBytes(t, fixture.profileRow)
			wantReads := 0
			if bearer {
				want += localCredentialProjectionBytes(t, fixture.credential)
				wantReads = 1
			} else {
				// Poison an unreachable credential row: noauth must not even query it.
				fixture.credential = []driver.Value{"credential row forbidden for none"}
			}
			var original *ownedAuthorityBudget
			err := f.store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
				original = q.budget
				return currentServerlessProviderResourceTx(ctx, q, binding, &f.ref, q.at)
			})
			if original == nil {
				t.Fatalf("current prerequisite callback not entered bearer=%t err=%v", bearer, err)
			}
			if err != nil || original.bytes != want || fixture.credentialReads != wantReads || original.statements != 2+wantReads || fixture.commits != 1 || fixture.execCalls != 0 {
				t.Fatalf("current prerequisite bearer=%t err=%v bytes=%d want=%d credentialReads=%d wantReads=%d statements=%d commits=%d writes=%d", bearer, err, original.bytes, want, fixture.credentialReads, wantReads, original.statements, fixture.commits, fixture.execCalls)
			}
		})
	}
}
