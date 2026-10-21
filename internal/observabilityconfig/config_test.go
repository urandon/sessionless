package observabilityconfig

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func fixtureConfig() *Config {
	q := Query{ID: "queue_messages", Source: "native", Service: "message-queue", Metric: "queue.messages.stored_count", MetricType: "DGAUGE", Unit: "messages", Aggregation: "max", SeriesAggregation: "none", Evidence: "VERIFIED", Qualifications: []string{}, LabelKeys: []string{"service", "name", "queue"}, MaxSeries: 1, Sparse: false, Freshness: "5m"}
	a := Alert{ID: "queue_occupied", QueryRef: q.ID, Class: "queue", Condition: Condition{Aggregation: "max", Operator: "gt", Value: 0, Unit: "messages", Window: "10m"}, EvaluationDelay: "5m", Demand: "nonzero_occurrence", Freshness: "5m", NoData: NoData{NoMatchingMetrics: "no_data", NoPoints: "no_data"}, Notifications: Notifications{ChannelRefs: []string{"operator_primary"}, States: []string{"alarm"}, Repeat: false}, Recovery: "manual_positive_evidence", Owner: "deployment_operator", RunbookRef: "queue_dlq"}
	sections := []Section{{ID: "deployment", Title: "Deployment", Panels: []Panel{{ID: "deployment_gap", Title: "Dated profile", Gap: "Unknown is not healthy", RunbookRef: "queue_dlq"}}}, {ID: "web", Title: "Web", Panels: []Panel{{ID: "web_gap", Title: "Web outcome", Gap: "Not qualified", RunbookRef: "queue_dlq"}}}, {ID: "canonical", Title: "Canonical", Panels: []Panel{{ID: "canonical_gap", Title: "Canonical outcome", Gap: "Not qualified", RunbookRef: "queue_dlq"}}}, {ID: "routes", Title: "Separate managed and attached", Panels: []Panel{{ID: "routes_gap", Title: "Routes", Gap: "Not qualified", RunbookRef: "queue_dlq"}}}, {ID: "dependencies", Title: "Dependencies", Panels: []Panel{{ID: "queue_panel", Title: "Queue messages", QueryRef: q.ID, RunbookRef: "queue_dlq"}}}}
	return &Config{SchemaVersion: 1, Queries: []Query{q}, Dashboards: []Dashboard{{ID: "overview", Name: "sessionless-fixture-overview", Title: "Operator overview", Sections: sections}}, Alerts: []Alert{a}, Profile: Profile{ID: "fixture", DashboardRef: "overview", EnabledQueries: []string{q.ID}, EnabledAlerts: []string{}, ChannelRefs: []string{"operator_primary"}, RunbookRefs: []string{"queue_dlq"}, MaxLeafRules: 12, MaxSemanticSeries: 24, MaxReadValues: 2000000, MaxSeriesPerLoad: 30, MaxPointsPerSeries: 60, AutoRefresh: false}}
}
func cloneConfig(t *testing.T, c *Config) *Config {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	var clone Config
	if err = json.Unmarshal(b, &clone); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return &clone
}
func fixturePrivate(c *Config) (*BindingsDocument, *OwnershipDocument, time.Time) {
	now := time.Date(2026, 10, 21, 17, 0, 0, 0, time.UTC)
	b := &BindingsDocument{SchemaVersion: 1, Bindings: Bindings{Revision: "fixture-revision", FolderID: "fixture-folder", Queries: []QueryBinding{{ID: c.Queries[0].ID, ObservedAt: now.Format(time.RFC3339), MetricType: "DGAUGE", Unit: "messages", Labels: map[string]string{"service": "message-queue", "name": "queue.messages.stored_count", "queue": "fixture-queue"}, Qualifications: []string{}}}}}
	o := &OwnershipDocument{SchemaVersion: 1, Ownership: Ownership{FolderID: b.Bindings.FolderID, Name: c.Dashboards[0].Name, ObservedAt: now.Format(time.RFC3339), Complete: true, SoleOwner: true, Existing: []ExistingDashboard{}, ImportID: "", SnapshotSHA256: ""}}
	return b, o, now
}

func TestDecodeClosedBoundedYAML(t *testing.T) {
	t.Parallel()
	type sample struct {
		SchemaVersion int    `yaml:"schema_version"`
		Name          string `yaml:"name"`
	}
	valid := "schema_version: 1\nname: fixture\n"
	for _, tc := range []struct{ name, input string }{{"valid", valid}, {"duplicate", "schema_version: 1\nname: fixture\nname: second\n"}, {"unknown", valid + "unexpected: true\n"}, {"alias", "schema_version: 1\nname: &anchor fixture\n"}, {"merge", "schema_version: 1\nname: fixture\n<<: {}\n"}, {"custom_tag", "schema_version: 1\nname: !secret fixture\n"}, {"number_to_string", "schema_version: 1\nname: 123\n"}, {"ambiguous_yes", "schema_version: 1\nname: yes\n"}, {"null", "schema_version: 1\nname: null\n"}, {"missing", "schema_version: 1\n"}, {"multiple", valid + "---\n" + valid}, {"hex_integer", "schema_version: 0x1\nname: fixture\n"}, {"limit", strings.Repeat(" ", MaxDocumentBytes+1)}} {
		t.Run(tc.name, func(t *testing.T) {
			var got sample
			err := Decode([]byte(tc.input), &got)
			if tc.name == "valid" {
				if err != nil || got.Name != "fixture" {
					t.Fatalf("Decode valid: got=%+v err=%v", got, err)
				}
			} else if err == nil {
				t.Fatalf("Decode %s: unsafe input accepted", tc.name)
			}
		})
	}
	var got sample
	if err := Decode([]byte("schema_version: 1\nname: 'yes'\n"), &got); err != nil {
		t.Fatalf("explicit quoted ambiguous string rejected: %v", err)
	}
}
func TestValidateSafetyPolicies(t *testing.T) {
	t.Parallel()
	base := fixtureConfig()
	if err := base.Validate(); err != nil {
		t.Fatalf("fixture Validate: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"no_matching_metrics", func(c *Config) { c.Alerts[0].NoData.NoMatchingMetrics = "ok" }},
		{"no_points", func(c *Config) { c.Alerts[0].NoData.NoPoints = "zero" }},
		{"idle_notifications", func(c *Config) {
			c.Alerts[0].Notifications.States = append(c.Alerts[0].Notifications.States, "no_data")
		}},
		{"semantic_ok", func(c *Config) { c.Alerts[0].Notifications.States = append(c.Alerts[0].Notifications.States, "ok") }},
		{"periodic_repeat", func(c *Config) { c.Alerts[0].Notifications.Repeat = true }},
		{"wrong_units", func(c *Config) { c.Alerts[0].Condition.Unit = "bytes" }},
		{"wrong_query_units", func(c *Config) { c.Queries[0].Unit = "bytes"; c.Alerts[0].Condition.Unit = "bytes" }},
		{"missing_reference", func(c *Config) { c.Alerts[0].QueryRef = "missing" }},
		{"unqualified_enabled", func(c *Config) { c.Queries[0].Evidence = "CATALOG_ONLY" }},
		{"sparse_not_heartbeat", func(c *Config) { c.Queries[0].Sparse = true }},
		{"ambiguous_deadline", func(c *Config) { c.Alerts[0].Freshness = "300" }},
		{"zero_deadline", func(c *Config) { c.Alerts[0].EvaluationDelay = "0m" }},
		{"series_budget", func(c *Config) { c.Queries[0].MaxSeries = 31 }},
		{"rule_cap", func(c *Config) { c.Profile.MaxLeafRules = 13 }},
		{"semantic_cap", func(c *Config) { c.Profile.MaxSemanticSeries = 25 }},
		{"read_cap", func(c *Config) { c.Profile.MaxReadValues = 2000001 }},
		{"auto_refresh", func(c *Config) { c.Profile.AutoRefresh = true }},
		{"queue_age_unit", func(c *Config) { c.Queries[0].Metric = "queue.messages.oldest_age_milliseconds" }},
		{"histogram_count", func(c *Config) { c.Queries[0].Aggregation = "p95" }},
		{"raw_path_label", func(c *Config) { c.Queries[0].LabelKeys = append(c.Queries[0].LabelKeys, "path") }},
		{"secret_title", func(c *Config) { c.Dashboards[0].Title = "Bearer private-sentinel" }},
		{"terraform_template", func(c *Config) { c.Dashboards[0].Title = "${file(private)}" }},
		{"control_title", func(c *Config) { c.Dashboards[0].Title = "line\nline" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cloneConfig(t, base)
			tc.change(c)
			if err := c.Validate(); err == nil {
				t.Fatalf("Validate %s: unsafe policy accepted", tc.name)
			}
		})
	}
}

func TestRepositoryCatalog(t *testing.T) {
	t.Parallel()
	c, err := Load("../../observability", "cloud-dev")
	if err != nil {
		t.Fatalf("Load repository YAML catalog: %v", err)
	}
	if len(c.Dashboards) != 1 || len(c.Dashboards[0].Sections) != 5 || len(c.Profile.EnabledAlerts) != 0 {
		t.Fatalf("repository overview or disabled-rule safety contract changed: dashboards=%d selected_alerts=%d", len(c.Dashboards), len(c.Profile.EnabledAlerts))
	}
	bundle, err := Render(c, nil, nil, time.Time{})
	if err != nil || bundle.Terraform != nil {
		t.Fatalf("repository preview: deployable=%v err=%v", bundle != nil && bundle.Terraform != nil, err)
	}
}
func TestRenderPreviewNeverDeploys(t *testing.T) {
	t.Parallel()
	c := fixtureConfig()
	b, err := Render(c, nil, nil, time.Time{})
	if err != nil {
		t.Fatalf("Render preview: %v", err)
	}
	if b.Terraform != nil || b.Envelope.Metadata.Mode != "preview" {
		t.Fatalf("preview deployable: mode=%s terraform=%v", b.Envelope.Metadata.Mode, b.Terraform)
	}
	p := b.Envelope.Panels[len(b.Envelope.Panels)-1]
	if p.State != "unbound_not_deployable" {
		t.Errorf("sparse/unbound state: got=%s", p.State)
	}
	first, _ := Encode(b.Envelope)
	secondBundle, err := Render(c, nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	second, _ := Encode(secondBundle.Envelope)
	if !bytes.Equal(first, second) {
		t.Error("repeated render differs for equal explicit inputs")
	}
}
func TestRenderBoundGuardsAndTerraformShape(t *testing.T) {
	t.Parallel()
	c := fixtureConfig()
	bindings, ownership, now := fixturePrivate(c)
	bundle, err := Render(c, bindings, ownership, now)
	if err != nil {
		t.Fatalf("Render bound: %v", err)
	}
	encoded, err := Encode(bundle.Terraform)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"yandex_monitoring_dashboard", "prevent_destroy", "GAP_FILLING_NONE", "VISUALIZATION_TYPE_POINTS", "= 0.220.0", "folder_id"} {
		if !bytes.Contains(encoded, []byte(want)) {
			t.Errorf("Terraform missing supported field %s", want)
		}
	}
	if bytes.Contains(encoded, []byte("INTERPOLATE_NONE")) || bytes.Contains(encoded, []byte("alert")) {
		t.Error("Terraform invented an alert/interpolation surface")
	}
	for _, tc := range []struct {
		name   string
		change func(*BindingsDocument, *OwnershipDocument)
	}{
		{"stale_query", func(b *BindingsDocument, _ *OwnershipDocument) {
			b.Bindings.Queries[0].ObservedAt = now.Add(-25 * time.Hour).Format(time.RFC3339)
		}},
		{"future_query", func(b *BindingsDocument, _ *OwnershipDocument) {
			b.Bindings.Queries[0].ObservedAt = now.Add(time.Second).Format(time.RFC3339)
		}},
		{"private_override", func(b *BindingsDocument, _ *OwnershipDocument) { b.Bindings.Queries[0].Labels["service"] = "storage" }},
		{"wildcard", func(b *BindingsDocument, _ *OwnershipDocument) { b.Bindings.Queries[0].Labels["queue"] = "*" }},
		{"template", func(b *BindingsDocument, _ *OwnershipDocument) {
			b.Bindings.Queries[0].Labels["queue"] = "${file(private-sentinel)}"
		}},
		{"missing_binding", func(b *BindingsDocument, _ *OwnershipDocument) { b.Bindings.Queries = []QueryBinding{} }},
		{"missing_label", func(b *BindingsDocument, _ *OwnershipDocument) { delete(b.Bindings.Queries[0].Labels, "queue") }},
		{"stale_ownership", func(_ *BindingsDocument, o *OwnershipDocument) {
			o.Ownership.ObservedAt = now.Add(-2 * time.Hour).Format(time.RFC3339)
		}},
		{"partial_list", func(_ *BindingsDocument, o *OwnershipDocument) { o.Ownership.Complete = false }},
		{"wrong_folder", func(_ *BindingsDocument, o *OwnershipDocument) { o.Ownership.FolderID = "other-fixture-folder" }},
		{"collision", func(_ *BindingsDocument, o *OwnershipDocument) {
			o.Ownership.Existing = []ExistingDashboard{{ID: "fixture-id", Name: c.Dashboards[0].Name, Owner: "unknown"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, o, _ := fixturePrivate(c)
			tc.change(b, o)
			_, err := Render(c, b, o, now)
			if err == nil {
				t.Fatalf("Render %s: unsafe private assertion accepted", tc.name)
			}
			if strings.Contains(err.Error(), "private-sentinel") {
				t.Fatalf("private source leaked in error: %v", err)
			}
		})
	}
}
func TestOwnedImportAndDrift(t *testing.T) {
	t.Parallel()
	c := fixtureConfig()
	b, o, now := fixturePrivate(c)
	o.Ownership.Existing = []ExistingDashboard{{ID: "fixture-dashboard", Name: c.Dashboards[0].Name, Owner: "sessionless"}}
	o.Ownership.ImportID = "fixture-dashboard"
	o.Ownership.SnapshotSHA256 = strings.Repeat("a", 64)
	bundle, err := Render(c, b, o, now)
	if err != nil {
		t.Fatalf("owned explicit import: %v", err)
	}
	expected, _ := Encode(bundle.Envelope)
	drift, err := Compare(expected, expected)
	if err != nil || !drift.Equal {
		t.Fatalf("unchanged comparator: got=%+v err=%v", drift, err)
	}
	changed := cloneConfig(t, c)
	changed.Alerts[0].Condition.Value = 9
	actualBundle, err := Render(changed, b, o, now)
	if err != nil {
		t.Fatalf("render internally consistent changed readback: %v", err)
	}
	actual, _ := Encode(actualBundle.Envelope)
	drift, err = Compare(expected, actual)
	if err != nil || drift.Equal || !contains(drift.ChangedSections, "console") {
		t.Fatalf("condition drift: got=%+v err=%v", drift, err)
	}
	b.Bindings.FolderID = "different-fixture-folder"
	o.Ownership.FolderID = b.Bindings.FolderID
	actualBundle, err = Render(changed, b, o, now)
	if err != nil {
		t.Fatalf("render different explicit folder: %v", err)
	}
	actual, _ = Encode(actualBundle.Envelope)
	drift, err = Compare(expected, actual)
	if err != nil || !contains(drift.ChangedSections, "ownership") {
		t.Fatalf("replacement drift: got=%+v err=%v", drift, err)
	}
}

func TestCompareRejectsIncompleteAndDuplicateReadbacks(t *testing.T) {
	t.Parallel()
	c := fixtureConfig()
	bundle, err := Render(c, nil, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	full, err := Encode(bundle.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(full, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"policy", "panels", "console", "query_bindings", "ownership"} {
		t.Run(field, func(t *testing.T) {
			copyFields := map[string]json.RawMessage{}
			for key, value := range fields {
				copyFields[key] = value
			}
			delete(copyFields, field)
			incomplete, _ := json.Marshal(copyFields)
			if _, err := Compare(incomplete, incomplete); err == nil {
				t.Fatalf("both snapshots missing %s accepted as MATCH", field)
			}
		})
	}
	duplicate := append([]byte(`{"metadata":{},`), full[1:]...)
	if _, err := Compare(duplicate, duplicate); err == nil {
		t.Error("duplicate readback key accepted")
	}
	bundle.Envelope.Console[0].Policy.Condition.Value = 9
	contradictory, _ := Encode(bundle.Envelope)
	if _, err := Compare(contradictory, contradictory); err == nil {
		t.Error("inconsistent policy and generated console accepted")
	}
}
func FuzzDecodeYAML(f *testing.F) {
	for _, seed := range []string{"schema_version: 1\nname: fixture\n", "schema_version: 1\nname: yes\n", "schema_version: 1\nname: &a fixture\n", "schema_version: 1\nname: !bad private-sentinel\n", "schema_version: 1\nname: fixture\nname: duplicate\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > MaxDocumentBytes+1 {
			return
		}
		var got struct {
			SchemaVersion int    `yaml:"schema_version"`
			Name          string `yaml:"name"`
		}
		err := Decode([]byte(input), &got)
		if err != nil && strings.Contains(err.Error(), "private-sentinel") {
			t.Fatalf("source leaked in parser diagnostic: %v", err)
		}
	})
}
