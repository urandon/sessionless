package ydbstore

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalCapacityStorageExactArms(t *testing.T) {
	tenant := domain.TenantID("tenant-local-capacity")
	local := domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: tenant, OwnerUserID: "owner-local", ResourceID: "resource-original"}
	stored, err := encodeCapacityStorageV1(tenant, "", &local)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.key) != 73 || !strings.HasPrefix(stored.key, "local-v1/") || !stored.association.Valid || !stored.digest.Valid {
		t.Fatalf("physical local shape: key length=%d association=%t digest=%t", len(stored.key), stored.association.Valid, stored.digest.Valid)
	}
	subscription, decoded, err := decodeCapacityStorageV1(tenant, stored)
	if err != nil || subscription != "" || decoded == nil || *decoded != local {
		t.Fatalf("local roundtrip: subscription=%q association=%v err=%v", subscription, decoded, err)
	}
	legacy, err := encodeCapacityStorageV1(tenant, "subscription-original", nil)
	if err != nil || legacy.key != "subscription-original" || legacy.association.Valid || legacy.digest.Valid {
		t.Fatalf("legacy bytes/NULL changed: %+v err=%v", legacy, err)
	}
	subscription, decoded, err = decodeCapacityStorageV1(tenant, legacy)
	if err != nil || subscription != "subscription-original" || decoded != nil {
		t.Fatalf("legacy roundtrip: subscription=%q association=%v err=%v", subscription, decoded, err)
	}
	for _, tc := range []struct {
		name string
		edit func(*capacityStorageV1)
	}{
		{"association-null", func(s *capacityStorageV1) { s.association = sql.NullString{} }},
		{"digest-null", func(s *capacityStorageV1) { s.digest = sql.NullString{} }},
		{"both-null-local-key", func(s *capacityStorageV1) { s.association = sql.NullString{}; s.digest = sql.NullString{} }},
		{"empty-association", func(s *capacityStorageV1) { s.association.String = "" }},
		{"noncanonical", func(s *capacityStorageV1) { s.association.String = " " + s.association.String }},
		{"wrong-key", func(s *capacityStorageV1) { s.key = "subscription-original" }},
		{"wrong-digest", func(s *capacityStorageV1) { s.digest.String = strings.Repeat("0", 64) }},
		{"upper-digest", func(s *capacityStorageV1) { s.digest.String = strings.ToUpper(s.digest.String) }},
		{"foreign-owner", func(s *capacityStorageV1) {
			s.association.String = strings.Replace(s.association.String, "owner-local", "owner-other", 1)
		}},
		{"foreign-tenant", func(s *capacityStorageV1) {
			s.association.String = strings.Replace(s.association.String, string(tenant), "tenant-other", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := stored
			tc.edit(&changed)
			if _, _, err := decodeCapacityStorageV1(tenant, changed); !errors.Is(err, domain.ErrMembershipDenied) {
				t.Fatalf("invalid physical arm accepted: %v", err)
			}
		})
	}
	if _, err := encodeCapacityStorageV1(tenant, "subscription-original", &local); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("mixed arm accepted: %v", err)
	}
	if _, err := encodeCapacityStorageV1("tenant-other", "", &local); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("foreign tenant accepted: %v", err)
	}
}
