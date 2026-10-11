package webcontract_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

func computeChoicesFixture() webcontract.ComputeChoicesPageV1 {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	observed, expires := at.Add(-time.Minute), at.Add(time.Minute)
	window := webcontract.ComputeChoiceWindowV1{ObservedAt: &observed, ValidUntil: &expires, Freshness: "current"}
	return webcontract.ComputeChoicesPageV1{Version: 1, SessionID: "session-1", ReadAt: at,
		InputKind: computechoice.InputText, RegistryRevision: strings.Repeat("1", 64), Coverage: "complete",
		Choices: []webcontract.ComputeChoiceV1{{ChoiceID: "choice-1", ChoiceRevision: strings.Repeat("2", 64),
			ModelLabel: "Reviewed model", ProviderLabel: "Reviewed provider", Placement: "attached", State: "eligible",
			ComputeChoiceWindowV1: window, DisclosureRevision: strings.Repeat("3", 64),
			Disclosure: webcontract.ComputeChoiceDisclosureV1{DataClass: domain.ProviderDataPrivateV1, InputKinds: []computechoice.InputKind{computechoice.InputText},
				ProviderProcessing: true, HostOperatorAccess: true, Fallback: "deny", Text: webcontract.ComputeChoicePrivateDisclosure},
			Capacity: webcontract.ComputeChoiceCapacityV1{State: "available", ComputeChoiceWindowV1: window}}}}
}

func TestComputeChoicesClosedPublicManifest(t *testing.T) {
	t.Parallel()
	value := computeChoicesFixture()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var page map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &page); err != nil {
		t.Fatal(err)
	}
	want := []string{"version", "session_id", "read_at", "input_kind", "registry_revision", "coverage", "choices"}
	assertJSONFields(t, page, want)
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(page["choices"], &entries); err != nil {
		t.Fatal(err)
	}
	assertJSONFields(t, entries[0], []string{"choice_id", "choice_revision", "model_label", "provider_label", "placement", "state", "observed_at", "valid_until", "freshness", "disclosure_revision", "disclosure", "capacity"})
	var disclosure, capacity map[string]json.RawMessage
	if err := json.Unmarshal(entries[0]["disclosure"], &disclosure); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(entries[0]["capacity"], &capacity); err != nil {
		t.Fatal(err)
	}
	assertJSONFields(t, disclosure, []string{"data_class", "input_kinds", "provider_processing", "host_operator_access", "fallback", "text"})
	assertJSONFields(t, capacity, []string{"state", "observed_at", "valid_until", "freshness"})
	for _, private := range []string{"tenant_id", "owner_user_id", "subscription_connection_id", "credential", "fingerprint", "worker_id", "connection_id", "endpoint", "route_id", "manifest_digest", "quota_remaining", "price", "budget", "queue_depth", "active_runs"} {
		if strings.Contains(string(encoded), private) {
			t.Errorf("public manifest contains private field %q", private)
		}
	}
	if len(encoded) > webcontract.MaxComputeChoicesResponseBytes {
		t.Errorf("bytes=%d ceiling=%d", len(encoded), webcontract.MaxComputeChoicesResponseBytes)
	}
}

func assertJSONFields(t *testing.T, fields map[string]json.RawMessage, want []string) {
	t.Helper()
	if len(fields) != len(want) {
		t.Errorf("field count=%d want=%d fields=%v", len(fields), len(want), fields)
	}
	for _, name := range want {
		if _, ok := fields[name]; !ok {
			t.Errorf("missing field %q", name)
		}
	}
}

func TestComputeChoicesUnknownAndExpiryBoundary(t *testing.T) {
	t.Parallel()
	value := computeChoicesFixture()
	entry := &value.Choices[0]
	entry.State, entry.ReasonCode = "unknown", "unknown"
	entry.ComputeChoiceWindowV1 = webcontract.ComputeChoiceWindowV1{}
	entry.Capacity = webcontract.ComputeChoiceCapacityV1{State: "unknown"}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "observed_at") || strings.Contains(string(encoded), "valid_until") || strings.Contains(string(encoded), "freshness") {
		t.Errorf("missing source fabricated a window: %s", encoded)
	}
	// Real current evidence can be semantically unknown (for example a measured
	// capability explicitly says unknown). Do not discard its truthful window.
	known := computeChoicesFixture().Choices[0].ComputeChoiceWindowV1
	entry.ComputeChoiceWindowV1 = known
	if err := value.Validate(); err != nil {
		t.Fatalf("unknown semantic fact with real current window: %v", err)
	}
	value.ReadAt = *known.ValidUntil
	entry.State, entry.ReasonCode, entry.Freshness = "unavailable", "evidence_stale", "expired"
	if err := value.Validate(); err != nil {
		t.Fatalf("expiry equality: %v", err)
	}
	value.ReadAt = value.ReadAt.Add(-time.Nanosecond)
	if err := value.Validate(); err == nil {
		t.Fatal("expired before authoritative expiry")
	}
	entry.Freshness = "current"
	entry.State, entry.ReasonCode = "unknown", "unknown"
	if err := value.Validate(); err != nil {
		t.Fatalf("one ns before expiry: %v", err)
	}
}

func TestComputeChoicesRejectContradictoryShape(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*webcontract.ComputeChoicesPageV1)
	}{
		{"version", func(v *webcontract.ComputeChoicesPageV1) { v.Version = 2 }},
		{"session", func(v *webcontract.ComputeChoicesPageV1) { v.SessionID = "" }},
		{"zero_time", func(v *webcontract.ComputeChoicesPageV1) { v.ReadAt = time.Time{} }},
		{"offset", func(v *webcontract.ComputeChoicesPageV1) { v.ReadAt = v.ReadAt.In(time.FixedZone("offset", 3600)) }},
		{"input", func(v *webcontract.ComputeChoicesPageV1) { v.InputKind = "public" }},
		{"revision", func(v *webcontract.ComputeChoicesPageV1) { v.RegistryRevision = strings.Repeat("A", 64) }},
		{"null_choices", func(v *webcontract.ComputeChoicesPageV1) { v.Choices = nil }},
		{"five", func(v *webcontract.ComputeChoicesPageV1) {
			for len(v.Choices) < 5 {
				v.Choices = append(v.Choices, v.Choices[0])
			}
		}},
		{"duplicate", func(v *webcontract.ComputeChoicesPageV1) { v.Choices = append(v.Choices, v.Choices[0]) }},
		{"coverage", func(v *webcontract.ComputeChoicesPageV1) { v.Coverage = "inventory" }},
		{"page_without_cursor", func(v *webcontract.ComputeChoicesPageV1) { v.Coverage = "page" }},
		{"raw_cursor", func(v *webcontract.ComputeChoicesPageV1) { v.NextCursor = "private-owner" }},
		{"choice_id", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ChoiceID = strings.Repeat("a", 129) }},
		{"choice_revision", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ChoiceRevision = "" }},
		{"disclosure_revision", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].DisclosureRevision = strings.Repeat("F", 64) }},
		{"placement", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Placement = "fallback" }},
		{"state", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].State = "running" }},
		{"eligible_reason", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ReasonCode = "unknown" }},
		{"eligible_no_window", func(v *webcontract.ComputeChoicesPageV1) {
			v.Choices[0].ComputeChoiceWindowV1 = webcontract.ComputeChoiceWindowV1{}
		}},
		{"partial_window", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ObservedAt = nil }},
		{"future_observation", func(v *webcontract.ComputeChoicesPageV1) {
			at := v.ReadAt.Add(time.Nanosecond)
			v.Choices[0].ObservedAt = &at
		}},
		{"reversed_window", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ValidUntil = v.Choices[0].ObservedAt }},
		{"raw_freshness", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Freshness = "durable" }},
		{"eligible_expired", func(v *webcontract.ComputeChoicesPageV1) {
			v.ReadAt = *v.Choices[0].ValidUntil
			v.Choices[0].Freshness = "expired"
		}},
		{"eligible_unknown_capacity", func(v *webcontract.ComputeChoicesPageV1) {
			v.Choices[0].Capacity = webcontract.ComputeChoiceCapacityV1{State: "unknown"}
		}},
		{"raw_capacity", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Capacity.State = "free" }},
		{"capacity_future", func(v *webcontract.ComputeChoicesPageV1) {
			at := v.ReadAt.Add(time.Nanosecond)
			v.Choices[0].Capacity.ObservedAt = &at
		}},
		{"eligible_unsupported_input", func(v *webcontract.ComputeChoicesPageV1) { v.InputKind = computechoice.InputFile }},
		{"public_class", func(v *webcontract.ComputeChoicesPageV1) {
			v.Choices[0].Disclosure.DataClass = domain.ProviderDataPublicV1
		}},
		{"concealed_provider", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Disclosure.ProviderProcessing = false }},
		{"concealed_operator", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Disclosure.HostOperatorAccess = false }},
		{"fallback", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Disclosure.Fallback = "allow" }},
		{"raw_disclosure", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Disclosure.Text = "provider secret error" }},
		{"no_kinds", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].Disclosure.InputKinds = nil }},
		{"duplicate_kinds", func(v *webcontract.ComputeChoicesPageV1) {
			v.Choices[0].Disclosure.InputKinds = []computechoice.InputKind{computechoice.InputText, computechoice.InputText}
		}},
		{"oversize_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = strings.Repeat("a", 129) }},
		{"invalid_utf8", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = string([]byte{0xff}) }},
		{"control_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ProviderLabel = "provider\nsecret" }},
		{"bidi_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ProviderLabel = "provider\u202esecret" }},
		{"leading_space_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = " model" }},
		{"trailing_space_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model " }},
		{"tab_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model\tname" }},
		{"format_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model\u200bname" }},
		{"leading_nbsp_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "\u00a0model" }},
		{"trailing_nbsp_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model\u00a0" }},
		{"control_nel_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model\u0085name" }},
		{"control_del_label", func(v *webcontract.ComputeChoicesPageV1) { v.Choices[0].ModelLabel = "model\u007fname" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := computeChoicesFixture()
			tc.mutate(&v)
			if err := v.Validate(); err == nil {
				t.Error("invalid shape validated")
			}
			if _, err := json.Marshal(v); err == nil {
				t.Error("invalid shape serialized")
			}
		})
	}
}

func TestComputeChoicesReasonsCapacityAndBoundedEscaping(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"profile_disabled", "evidence_stale", "capability_unavailable", "consent_required", "capacity_unavailable", "policy_unavailable", "unknown"} {
		v := computeChoicesFixture()
		v.Choices[0].State, v.Choices[0].ReasonCode = "unavailable", webcontract.ComputeChoiceReasonCode(reason)
		if reason == "evidence_stale" {
			v.ReadAt = *v.Choices[0].ValidUntil
			v.Choices[0].Freshness, v.Choices[0].Capacity.Freshness, v.Choices[0].Capacity.State = "expired", "expired", "unavailable"
		}
		if err := v.Validate(); err != nil {
			t.Errorf("safe reason=%s: %v", reason, err)
		}
		v.Choices[0].ReasonCode = "raw private reason"
		if err := v.Validate(); err == nil {
			t.Errorf("raw reason accepted for %s", reason)
		}
	}
	v := computeChoicesFixture()
	for len(v.Choices) < 4 {
		v.Choices = append(v.Choices, computeChoicesFixture().Choices[0])
	}
	for i := range v.Choices {
		v.Choices[i].ChoiceID = string(rune('a'+i)) + strings.Repeat("a", 127)
		v.Choices[i].ModelLabel = strings.Repeat("<", 128)   // JSON HTML escaping uses six bytes per source byte.
		v.Choices[i].ProviderLabel = strings.Repeat("😀", 32) // Exact 128-byte UTF-8 boundary.
	}
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 16*1024 || webcontract.MaxComputeChoicesResponseBytes != 16*1024 {
		t.Errorf("encoded bytes=%d ceiling=%d", len(encoded), webcontract.MaxComputeChoicesResponseBytes)
	}
	var roundtrip webcontract.ComputeChoicesPageV1
	if err := json.Unmarshal(encoded, &roundtrip); err != nil || !reflect.DeepEqual(roundtrip, v) {
		t.Fatalf("roundtrip err=%v equal=%v", err, reflect.DeepEqual(roundtrip, v))
	}
	v.Choices = []webcontract.ComputeChoiceV1{}
	if _, err := json.Marshal(v); err != nil {
		t.Fatalf("empty complete page: %v", err)
	}
}

func TestComputeChoicesEmptyContinuationIsNotInventory(t *testing.T) {
	t.Parallel()
	v := computeChoicesFixture()
	scope := computechoice.Scope{TenantID: "tenant-1", UserID: "user-1", SessionID: v.SessionID, MembershipSecurityVersion: 1}
	registry, err := computechoice.NewRegistry([]computechoice.Registration{
		{TenantID: scope.TenantID, OwnerUserID: scope.UserID, ChoiceID: "a", RegistrationRevision: 1, DisclosureRevision: strings.Repeat("3", 64)},
		{TenantID: scope.TenantID, OwnerUserID: scope.UserID, ChoiceID: "b", RegistrationRevision: 1, DisclosureRevision: strings.Repeat("3", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := registry.Scoped(scope)
	if err != nil {
		t.Fatal(err)
	}
	codec, err := computechoice.NewCursorCodec([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	v.RegistryRevision = scoped.Revision()
	v.NextCursor, err = codec.Encode(scope, scoped, v.InputKind, 1, v.ReadAt, v.ReadAt.Add(time.Second), v.ReadAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	v.Coverage, v.Choices = "page", []webcontract.ComputeChoiceV1{}
	if _, err := json.Marshal(v); err != nil {
		t.Fatalf("empty page with examined-position cursor: %v", err)
	}
	v.Coverage = "complete"
	if err := v.Validate(); err == nil {
		t.Fatal("complete page with continuation accepted")
	}
}

func TestComputeChoicesAggregateWindowCannotOutliveCapacity(t *testing.T) {
	t.Parallel()
	v := computeChoicesFixture()
	expiry := v.ReadAt.Add(time.Second)
	v.Choices[0].Capacity.ValidUntil = &expiry
	if err := v.Validate(); err == nil {
		t.Fatal("aggregate required-source expiry outlives known capacity expiry")
	}
	if _, err := json.Marshal(v); err == nil {
		t.Fatal("contradictory aggregate window serialized")
	}
	v.Choices[0].ValidUntil = &expiry
	if err := v.Validate(); err != nil {
		t.Fatalf("aggregate at shortest known source expiry: %v", err)
	}
}

func TestComputeChoicesOrdinaryUnicodeLabels(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, label string }{
		{"multilingual_emoji", "模型 😀"},
		{"combining_marks", "café e\u0301"},
		{"internal_nbsp", "model\u00a0name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := computeChoicesFixture()
			v.Choices[0].ModelLabel, v.Choices[0].ProviderLabel = tc.label, tc.label
			if _, err := json.Marshal(v); err != nil {
				t.Fatalf("ordinary reviewed label %q rejected: %v", tc.label, err)
			}
		})
	}
}
