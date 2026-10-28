package ydbstore

import "testing"

func TestAttachedResourceGenerationRouteConfigurationIsExplicit(t *testing.T) {
	var absent *Store
	if absent.AttachedResourceGenerationEnabledV1() || new(Store).AttachedResourceGenerationEnabledV1() {
		t.Fatal("missing installed registry enabled publication routes")
	}
	f := newGenerationStoreFixture(t)
	if !f.store.AttachedResourceGenerationEnabledV1() || f.sql.begins != 0 || len(f.sql.queries) != 0 {
		t.Fatal("route configuration did not use installed registry or read runtime state")
	}
}
