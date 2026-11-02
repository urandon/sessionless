package ydbstore

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Scripted projection DATA only; no installation, model, effect or real DB.
func explanationLocalRun(t *testing.T, run domain.Run) (domain.Run, string) {
	t.Helper()
	run.SubscriptionConnectionID = ""
	run.LocalCapacityAssociation = &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: run.TenantID, OwnerUserID: "local-owner-original", ResourceID: "local-resource-original"}
	if err := run.Validate(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	return run, string(raw)
}

func TestLocalCapacityRunExplanationResource(t *testing.T) {
	for _, size := range []int{10 << 10, 28 << 10} {
		t.Run(map[int]string{10 << 10: "10KiB", 28 << 10: "28KiB"}[size], func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			_, raw := explanationLocalRun(t, run)
			f.rows["runs"] = []driver.Value{explanationRunWithExcludedMetadata(t, raw, size)}
			got, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if err != nil || got.RunID != run.ID || len(f.queries) != 7 {
				t.Fatalf("local Run scalar projection: id=%s statements=%d want=7 err=%v", got.RunID, len(f.queries), err)
			}
			response, err := json.Marshal(got)
			if err != nil || bytes.Contains(response, []byte("private-derived-source-")) || bytes.Contains(response, []byte("local-resource-original")) || len(response) > runexplanation.MaxResponseBytes {
				t.Fatal("private Run metadata or local capacity identity entered the explanation response")
			}
		})
	}
}

func TestLocalCapacityRunExplanationProjectionPreservesExactArms(t *testing.T) {
	for _, arm := range []string{"subscription", "local", "normalized-local"} {
		t.Run(arm, func(t *testing.T) {
			store, f, _, run := explanationReadFixture(t)
			raw := f.rows["runs"][0].(string)
			if arm != "subscription" {
				run, raw = explanationLocalRun(t, run)
			}
			if arm == "normalized-local" {
				var document map[string]json.RawMessage
				if err := json.Unmarshal([]byte(raw), &document); err != nil {
					t.Fatal(err)
				}
				var members map[string]json.RawMessage
				if err := json.Unmarshal(document["local_capacity_association"], &members); err != nil {
					t.Fatal(err)
				}
				ordered, err := json.Marshal(members)
				if err != nil {
					t.Fatal(err)
				}
				var formatted bytes.Buffer
				if err := json.Indent(&formatted, ordered, "", "  "); err != nil {
					t.Fatal(err)
				}
				if _, err := domain.DecodeLocalCapacityAssociationV1(formatted.Bytes()); err == nil {
					t.Fatal("standalone String decoder accepted reordered/formatted identity")
				}
				raw = strings.Replace(raw, string(document["local_capacity_association"]), formatted.String(), 1)
			}
			f.rows["runs"] = []driver.Value{raw}
			got, found, err := readExplanationRun(t.Context(), store.db, run.TenantID, run.ID)
			if err != nil || !found || !reflect.DeepEqual(got, run) || len(f.queries) != 1 {
				t.Fatalf("projected arm changed original base data: arm=%s found=%t statements=%d err=%v", arm, found, len(f.queries), err)
			}
			if arm == "subscription" {
				before, err := json.Marshal(run)
				if err != nil {
					t.Fatal(err)
				}
				after, err := json.Marshal(got)
				if err != nil || !bytes.Equal(before, after) {
					t.Fatal("legacy Run bytes changed")
				}
			}
		})
	}
}

func TestLocalCapacityRunExplanationProjectionBounds(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		t.Run(map[bool]string{false: "maximum-producer", true: "sentinel-before-parse"}[oversized], func(t *testing.T) {
			store, f, _, run := explanationReadFixture(t)
			run, _ = explanationLocalRun(t, run)
			run.TenantID = domain.TenantID(strings.Repeat("t", 160))
			run.LocalCapacityAssociation.TenantID = run.TenantID
			run.LocalCapacityAssociation.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
			run.LocalCapacityAssociation.ResourceID = strings.Repeat("r", 160)
			if err := run.Validate(); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			if oversized {
				raw = []byte(strings.Replace(string(raw), strings.Repeat("r", 160), strings.Repeat("r", 2048), 1))
			}
			f.rows["runs"] = []driver.Value{explanationRunWithExcludedMetadata(t, string(raw), 28<<10)}
			args := []driver.NamedValue{{Ordinal: 1, Value: string(run.TenantID)}, {Ordinal: 2, Value: string(run.ID)}}
			rows, err := explanationRunProjectionRows(explanationRunProjectionSQL, args, f.rows["runs"])
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			values := make([]driver.Value, 13)
			if err := rows.Next(values); err != nil {
				t.Fatal(err)
			}
			var transferred int
			for _, value := range values {
				if text, ok := value.(string); ok {
					transferred += len(text)
					if strings.Contains(text, "private-derived-source-") {
						t.Fatal("private metadata entered SQL projection")
					}
				}
			}
			if transferred > 2204 || transferred > runexplanation.MaxProjectionBytes {
				t.Fatalf("projection exceeded original source allowance: bytes=%d", transferred)
			}
			got, found, err := readExplanationRun(t.Context(), store.db, run.TenantID, run.ID)
			if oversized {
				if len(values[12].(string)) != 1025 || !errors.Is(err, runexplanation.ErrCorruptProjection) || found || !reflect.DeepEqual(got, domain.Run{}) {
					t.Fatalf("overflow sentinel accepted: found=%t bytes=%d err=%v", found, len(values[12].(string)), err)
				}
			} else if err != nil || !found || len(values[12].(string)) != 563 || !reflect.DeepEqual(got, run) {
				t.Fatalf("valid maximum local projection: found=%t bytes=%d err=%v", found, len(values[12].(string)), err)
			}
		})
	}
}

func TestLocalCapacityRunExplanationRejectsMalformedAssociation(t *testing.T) {
	for _, tc := range []struct {
		name, member, value string
		missing             bool
	}{
		{name: "missing-version", member: "version", missing: true},
		{name: "null-version", member: "version", value: "null"},
		{name: "string-version", member: "version", value: `"1"`},
		{name: "wrong-version", member: "version", value: "2"},
		{name: "fraction-version", member: "version", value: "1.5"},
		{name: "missing-kind", member: "kind", missing: true},
		{name: "null-kind", member: "kind", value: "null"},
		{name: "wrong-kind", member: "kind", value: `"subscription"`},
		{name: "array-kind", member: "kind", value: `["local_endpoint"]`},
		{name: "missing-tenant", member: "tenant_id", missing: true},
		{name: "null-tenant", member: "tenant_id", value: "null"},
		{name: "foreign-tenant", member: "tenant_id", value: `"tenant-other"`},
		{name: "numeric-tenant", member: "tenant_id", value: "1"},
		{name: "overlong-tenant", member: "tenant_id", value: `"` + strings.Repeat("t", 161) + `"`},
		{name: "missing-owner", member: "owner_id", missing: true},
		{name: "null-owner", member: "owner_id", value: "null"},
		{name: "object-owner", member: "owner_id", value: `{"id":"owner"}`},
		{name: "invalid-owner", member: "owner_id", value: `"owner/foreign"`},
		{name: "overlong-owner", member: "owner_id", value: `"` + strings.Repeat("o", 161) + `"`},
		{name: "missing-resource", member: "resource_id", missing: true},
		{name: "null-resource", member: "resource_id", value: "null"},
		{name: "bool-resource", member: "resource_id", value: "true"},
		{name: "empty-resource", member: "resource_id", value: `""`},
		{name: "overlong-resource", member: "resource_id", value: `"` + strings.Repeat("r", 161) + `"`},
		{name: "invalid-resource", member: "resource_id", value: `"local-v1/foreign"`},
		{name: "unknown-member", member: "unknown", value: "1"},
		{name: "case-alias", member: "OWNER_ID", value: `"owner-case-alias"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			_, raw := explanationLocalRun(t, run)
			var document map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			var association map[string]json.RawMessage
			if err := json.Unmarshal(document["local_capacity_association"], &association); err != nil {
				t.Fatal(err)
			}
			if tc.missing {
				delete(association, tc.member)
			} else {
				association[tc.member] = json.RawMessage(tc.value)
			}
			if tc.name == "case-alias" {
				delete(association, "owner_id")
			}
			object, err := json.Marshal(association)
			if err != nil {
				t.Fatal(err)
			}
			document["local_capacity_association"] = object
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			f.rows["runs"] = []driver.Value{string(encoded)}
			if _, err := store.ReadRunExplanationV1(t.Context(), auth, run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) || len(f.queries) != 4 {
				t.Fatalf("invalid local member accepted: fault=%s statements=%d err=%v", tc.name, len(f.queries), err)
			}
		})
	}
}

func TestLocalCapacityRunExplanationRejectsMissingMixedAndInvalidShapes(t *testing.T) {
	for _, fault := range []string{"neither", "local-null", "local-scalar", "local-string", "local-array", "mixed", "mixed-empty", "mixed-null", "mixed-array", "legacy-null-local", "legacy-scalar-local", "legacy-array-local", "legacy-malformed-local", "duplicate", "array-root", "oversize"} {
		t.Run(fault, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			_, raw := explanationLocalRun(t, run)
			var document map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			original := document["local_capacity_association"]
			switch fault {
			case "neither":
				delete(document, "local_capacity_association")
			case "local-null":
				document["local_capacity_association"] = json.RawMessage("null")
			case "local-scalar":
				document["local_capacity_association"] = json.RawMessage("1")
			case "local-string":
				document["local_capacity_association"] = json.RawMessage(`"local_endpoint"`)
			case "local-array":
				document["local_capacity_association"] = append(append([]byte("["), original...), ']')
			case "mixed":
				document["subscription_connection_id"] = json.RawMessage(`"subscription-test"`)
			case "mixed-empty":
				document["subscription_connection_id"] = json.RawMessage(`""`)
			case "mixed-null":
				document["subscription_connection_id"] = json.RawMessage("null")
			case "mixed-array":
				document["subscription_connection_id"] = json.RawMessage(`["subscription-test"]`)
			case "legacy-null-local", "legacy-scalar-local", "legacy-array-local", "legacy-malformed-local":
				document["subscription_connection_id"] = json.RawMessage(`"subscription-test"`)
				switch fault {
				case "legacy-null-local":
					document["local_capacity_association"] = json.RawMessage("null")
				case "legacy-scalar-local":
					document["local_capacity_association"] = json.RawMessage("1")
				case "legacy-array-local":
					document["local_capacity_association"] = append(append([]byte("["), original...), ']')
				case "legacy-malformed-local":
					document["local_capacity_association"] = json.RawMessage(`{"unknown":1}`)
				}
			case "duplicate":
				document["local_capacity_association"] = json.RawMessage(strings.Replace(string(original), `"version":1`, `"version":1,"version":1`, 1))
			case "oversize":
				value, err := json.Marshal(strings.Repeat("r", 2048))
				if err != nil {
					t.Fatal(err)
				}
				document["local_capacity_association"] = json.RawMessage(strings.Replace(string(original), `"local-resource-original"`, string(value), 1))
			}
			encoded, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "array-root" {
				encoded = append(append([]byte("["), encoded...), ']')
			}
			f.rows["runs"] = []driver.Value{string(encoded)}
			if _, err := store.ReadRunExplanationV1(t.Context(), auth, run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) || len(f.queries) != 4 {
				t.Fatalf("invalid local arm accepted: fault=%s statements=%d err=%v", fault, len(f.queries), err)
			}
		})
	}
}

func TestLocalCapacityRunExplanationRatedBudget(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	_, raw := explanationLocalRun(t, run)
	f.rows["runs"] = []driver.Value{explanationRunWithExcludedMetadata(t, raw, 28<<10)}
	got, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "local-projected-run", run.ID)
	assertRatedOutcome(t, got, err, ports.RunExplanationReadSuccessV1)
	if len(f.statements) != 12 || f.writes != 1 {
		t.Fatalf("local Run changed rated point/debit budget: statements=%d writes=%d", len(f.statements), f.writes)
	}
}
