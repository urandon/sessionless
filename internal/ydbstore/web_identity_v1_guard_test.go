package ydbstore

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestIdentityV1GuardPreservesOnlyExplicitV1Ownership(t *testing.T) {
	state, identity, _, _ := identityRowsV2Data(t)
	legacy := domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}
	state.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, legacy)
	fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
	got, created, err := resolveOrCreateExternalIdentityTx(ctx, tx, identity.Subject, "different-new-candidate", identityRowsV2At())
	if err != nil || created || !reflect.DeepEqual(got, legacy) || fixture.queries != 1 || fixture.writes != 0 {
		t.Fatalf("explicit V1 immutable mapping changed: got=%+v created=%v err=%v queries=%d writes=%d", got, created, err, fixture.queries, fixture.writes)
	}
}

func TestIdentityV1GuardDeniesV2ActiveAndTombstoneBeforeAnyWrite(t *testing.T) {
	for _, status := range []domain.ExternalIdentityStateV2{domain.ExternalIdentityActiveV2, domain.ExternalIdentityUnlinkedV2} {
		t.Run(string(status), func(t *testing.T) {
			state, identity, _, _ := identityRowsV2Data(t)
			identity.State = status
			state.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, state)
			got, created, err := resolveOrCreateExternalIdentityTx(ctx, tx, identity.Subject, identity.UserID, identityRowsV2At())
			if !errors.Is(err, domain.ErrMembershipDenied) || created || !reflect.DeepEqual(got, domain.ExternalIdentity{}) || fixture.queries != 1 || fixture.writes != 0 {
				t.Errorf("V1 resolver adopted V2 %s ownership: got=%+v created=%v err=%v writes=%d", status, got, created, err, fixture.writes)
			}
		})
	}
}

func TestIdentityV1GuardRejectsAmbiguousOrMismatchedCanonicalRows(t *testing.T) {
	state, identity, _, _ := identityRowsV2Data(t)
	legacy := domain.ExternalIdentity{Subject: identity.Subject, UserID: identity.UserID, CreatedAt: identity.CreatedAt, UpdatedAt: identity.UpdatedAt}
	raw := identityRowsV2JSON(t, legacy)
	for name, altered := range map[string]string{
		"unknown_state":     strings.TrimSuffix(raw, "}") + `,"state":"unlinked"}`,
		"null_version":      strings.TrimSuffix(raw, "}") + `,"version":null}`,
		"wrong_case":        strings.Replace(raw, `"user_id"`, `"User_ID"`, 1),
		"duplicate_owner":   strings.Replace(raw, `"user_id":`, `"user_id":"foreign","user_id":`, 1),
		"duplicate_subject": strings.Replace(raw, `"provider":`, `"provider":"telegram","provider":`, 1),
		"key_mismatch":      strings.Replace(raw, `"subject":"123"`, `"subject":"different"`, 1),
		"trailing_value":    raw + `{}`,
		"oversized":         raw + strings.Repeat(" ", domain.IdentityAuthMaxRecordBytesV2),
	} {
		t.Run(name, func(t *testing.T) {
			data := state.clone()
			data.forward[identityRowsV2SubjectKey(identity.Subject)] = altered
			fixture, ctx, tx := newIdentityRowsV2Fixture(t, data)
			got, found, err := readExternalIdentityTx(ctx, tx, identity.Subject)
			if !errors.Is(err, domain.ErrMembershipDenied) || found || !reflect.DeepEqual(got, domain.ExternalIdentity{}) || fixture.writes != 0 {
				t.Errorf("ambiguous legacy row %s yielded authority: got=%+v found=%v err=%v", name, got, found, err)
			}
		})
	}
}
