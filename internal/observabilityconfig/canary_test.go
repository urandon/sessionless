package observabilityconfig

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testCanary(t *testing.T) *CanaryDocument {
	t.Helper()
	c, err := LoadCanary("../../observability/canary.yaml")
	if err != nil {
		t.Fatalf("load repository canary policy: %v", err)
	}
	return c
}
func testCanaryBindings() *CanaryBindingsDocument {
	return &CanaryBindingsDocument{SchemaVersion: 1, Bindings: CanaryBindings{Revision: "fixture-v1", FolderID: "fixturefolder", ProjectID: "folder__fixturefolder", ChannelID: "folder__fixturefolder_fixture-push", Instance: "0123456789abcdef0123456789abcdef", Preflight: CanaryPreflight{ObservedAt: "2026-01-01T12:00:00Z", SoleOwner: true, CompleteRuleList: true, CompleteSeriesList: true, CloudPush: true, ChannelScopeConfirmed: true, OneConfirmedRecipient: true, NoDefaultSeveritySubscription: true}}}
}
func canaryNow(t *testing.T) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, "2026-01-01T12:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func TestCanaryClosedPolicyRejections(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		change func(*CanaryDocument)
	}{
		{"schema", func(c *CanaryDocument) { c.SchemaVersion = 2 }},
		{"kind", func(c *CanaryDocument) { c.Kind = "production" }},
		{"id", func(c *CanaryDocument) { c.ID = "other" }},
		{"fake_verified", func(c *CanaryDocument) { c.Evidence = "VERIFIED" }},
		{"service", func(c *CanaryDocument) { c.Metric.Service = "storage" }},
		{"metric_name", func(c *CanaryDocument) { c.Metric.Name = "other" }},
		{"type", func(c *CanaryDocument) { c.Metric.MetricType = "RATE" }},
		{"unit", func(c *CanaryDocument) { c.Metric.Unit = "events" }},
		{"label", func(c *CanaryDocument) { c.Metric.InstanceLabel = "tenant" }},
		{"series", func(c *CanaryDocument) { c.Metric.MaxSeries = 2 }},
		{"points", func(c *CanaryDocument) { c.Metric.MaxPointsPerSeries = 10 }},
		{"fill", func(c *CanaryDocument) { c.Metric.GapFilling = "zero" }},
		{"aggregation", func(c *CanaryDocument) { c.Rule.Aggregation = "avg" }},
		{"operator", func(c *CanaryDocument) { c.Rule.Operator = "lt" }},
		{"threshold", func(c *CanaryDocument) { c.Rule.Threshold = 0 }},
		{"threshold_nan", func(c *CanaryDocument) { c.Rule.Threshold = math.NaN() }},
		{"window", func(c *CanaryDocument) { c.Rule.Window = "10m" }},
		{"delay", func(c *CanaryDocument) { c.Rule.EvaluationDelay = "0s" }},
		{"freshness", func(c *CanaryDocument) { c.Rule.Freshness = "1h" }},
		{"noise_suppression", func(c *CanaryDocument) { c.Rule.NoiseSuppression = true }},
		{"missing_series_zero", func(c *CanaryDocument) { c.Rule.NoData.NoMatchingMetrics = "ok" }},
		{"missing_points_zero", func(c *CanaryDocument) { c.Rule.NoData.NoPoints = "ok" }},
		{"repeat", func(c *CanaryDocument) { c.Rule.Notifications.Repeat = true }},
		{"channel", func(c *CanaryDocument) { c.Rule.Notifications.ChannelRefs = []string{"other"} }},
		{"duplicate_states", func(c *CanaryDocument) { c.Rule.Notifications.States = []string{"alarm", "alarm"} }},
		{"idle_page", func(c *CanaryDocument) { c.Rule.Notifications.States = []string{"alarm", "ok", "no_data"} }},
		{"semantic_recovery", func(c *CanaryDocument) { c.Rule.Recovery = "canonical_success" }},
		{"writes_ceiling", func(c *CanaryDocument) { c.Budget.MetricWrites = 13 }},
		{"reads_ceiling", func(c *CanaryDocument) { c.Budget.ReadRequests = 41 }},
		{"values_ceiling", func(c *CanaryDocument) { c.Budget.ReturnedValues = 10001 }},
		{"notifications_ceiling", func(c *CanaryDocument) { c.Budget.PlannedNotifications = 5 }},
		{"cost_ceiling", func(c *CanaryDocument) { c.Budget.EstimatedIncrementalRUB = 6 }},
		{"cost_nan", func(c *CanaryDocument) { c.Budget.EstimatedIncrementalRUB = math.NaN() }},
		{"cost_inf", func(c *CanaryDocument) { c.Budget.EstimatedIncrementalRUB = math.Inf(1) }},
		{"cost_zero", func(c *CanaryDocument) { c.Budget.EstimatedIncrementalRUB = 0 }},
		{"wall_time", func(c *CanaryDocument) { c.Budget.WallTime = "91m" }},
		{"phase_skip", func(c *CanaryDocument) { c.Phases = c.Phases[1:] }},
		{"phase_order", func(c *CanaryDocument) { c.Phases[0], c.Phases[1] = c.Phases[1], c.Phases[0] }},
		{"idle_duration", func(c *CanaryDocument) { c.Phases[5].Duration = "19m" }},
		{"write_in_idle", func(c *CanaryDocument) { c.Phases[5].MaxSamples = 1 }},
		{"wrong_breach", func(c *CanaryDocument) { c.Phases[3].SampleValue = 0 }},
		{"stale_clear", func(c *CanaryDocument) { c.Phases[4].SampleValue = 1 }},
		{"sample_count", func(c *CanaryDocument) { c.Phases[1].MaxSamples = 5 }},
		{"no_baseline", func(c *CanaryDocument) { c.Phases[1].MaxSamples = 0 }},
		{"too_fast", func(c *CanaryDocument) { c.Phases[1].MinSampleSpacing = "1s" }},
		{"stale_single_sample", func(c *CanaryDocument) { c.Phases[1].MaxSamples = 1; c.Phases[1].MinSampleSpacing = "4m" }},
		{"phase_negative_read", func(c *CanaryDocument) { c.Phases[0].ReadRequests = -1 }},
		{"phase_negative_values", func(c *CanaryDocument) { c.Phases[0].ReturnedValues = -1 }},
		{"phase_no_query_values", func(c *CanaryDocument) { c.Phases[1].ReturnedValues = 0 }},
		{"phase_idle_notification", func(c *CanaryDocument) { c.Phases[5].PlannedNotifications = 1 }},
		{"initial_ok_omitted", func(c *CanaryDocument) { c.Phases[2].PlannedNotifications = 0 }},
		{"aggregate_writes", func(c *CanaryDocument) { c.Budget.MetricWrites = 11 }},
		{"aggregate_reads", func(c *CanaryDocument) { c.Phases[0].ReadRequests++ }},
		{"aggregate_values", func(c *CanaryDocument) { c.Phases[0].ReturnedValues++ }},
		{"aggregate_notifications", func(c *CanaryDocument) { c.Budget.PlannedNotifications = 2 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := testCanary(t)
			tc.change(c)
			if err := c.Validate(); err == nil {
				t.Errorf("mutated policy %s accepted", tc.name)
			}
		})
	}
}

func TestCanaryParserClosedAndBounded(t *testing.T) {
	t.Parallel()
	seed, err := os.ReadFile("../../observability/canary.yaml")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"duplicate": append([]byte("schema_version: 1\n"), seed...),
		"unknown":   append(append([]byte{}, seed...), []byte("recipient: private-sentinel\n")...),
		"missing":   bytes.Replace(seed, []byte("id: obs228\n"), nil, 1),
		"coercion":  bytes.Replace(seed, []byte("schema_version: 1"), []byte("schema_version: \"1\""), 1),
		"alias":     bytes.Replace(seed, []byte("service: custom"), []byte("service: &ref custom"), 1),
		"tag":       bytes.Replace(seed, []byte("service: custom"), []byte("service: !!str custom"), 1),
		"multi":     append(append([]byte{}, seed...), []byte("\n---\n{}\n")...),
		"over_size": bytes.Repeat([]byte("x"), MaxDocumentBytes+1),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			var c CanaryDocument
			err := Decode(data, &c)
			if err == nil {
				t.Error("unsafe canary document accepted")
			} else if strings.Contains(err.Error(), "private-sentinel") {
				t.Error("parser leaked input value")
			}
		})
	}
}

func TestCanaryPrivateBindingAdmission(t *testing.T) {
	t.Parallel()
	c := testCanary(t)
	now := canaryNow(t)
	cases := []struct {
		name   string
		change func(*CanaryBindingsDocument)
	}{
		{"schema", func(b *CanaryBindingsDocument) { b.SchemaVersion = 2 }},
		{"revision", func(b *CanaryBindingsDocument) { b.Bindings.Revision = "secret=private-sentinel" }},
		{"folder", func(b *CanaryBindingsDocument) { b.Bindings.FolderID = "folder*" }},
		{"project", func(b *CanaryBindingsDocument) { b.Bindings.ProjectID = "folder__other" }},
		{"channel_scope_unconfirmed", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.ChannelScopeConfirmed = false }},
		{"channel_suffix", func(b *CanaryBindingsDocument) { b.Bindings.ChannelID += "*" }},
		{"channel_empty", func(b *CanaryBindingsDocument) { b.Bindings.ChannelID = "" }},
		{"channel_oversized", func(b *CanaryBindingsDocument) { b.Bindings.ChannelID = strings.Repeat("x", 257) }},
		{"channel_whitespace", func(b *CanaryBindingsDocument) { b.Bindings.ChannelID = "opaque channel" }},
		{"nonce_upper", func(b *CanaryBindingsDocument) { b.Bindings.Instance = strings.Repeat("A", 32) }},
		{"nonce_short", func(b *CanaryBindingsDocument) { b.Bindings.Instance = "aabb" }},
		{"nonce_injection", func(b *CanaryBindingsDocument) { b.Bindings.Instance = "\"}*" }},
		{"stale", func(b *CanaryBindingsDocument) {
			b.Bindings.Preflight.ObservedAt = now.Add(-time.Hour - time.Second).Format(time.RFC3339)
		}},
		{"future", func(b *CanaryBindingsDocument) {
			b.Bindings.Preflight.ObservedAt = now.Add(time.Second).Format(time.RFC3339)
		}},
		{"not_sole_owner", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.SoleOwner = false }},
		{"rule_list_incomplete", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.CompleteRuleList = false }},
		{"series_list_incomplete", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.CompleteSeriesList = false }},
		{"rule_collision", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.RuleCollision = true }},
		{"series_collision", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.SeriesCollision = true }},
		{"wrong_method", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.CloudPush = false }},
		{"recipient_unconfirmed", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.OneConfirmedRecipient = false }},
		{"default_subscription", func(b *CanaryBindingsDocument) { b.Bindings.Preflight.NoDefaultSeveritySubscription = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testCanaryBindings()
			tc.change(b)
			if _, err := RenderCanary(c, b, now); err == nil {
				t.Errorf("invalid private binding %s accepted", tc.name)
			} else if strings.Contains(err.Error(), "private-sentinel") {
				t.Error("private value leaked")
			}
		})
	}
	if _, err := RenderCanary(c, testCanaryBindings(), time.Time{}); err == nil {
		t.Error("bound render without explicit time accepted")
	}
	if _, err := RenderCanary(c, nil, now); err == nil {
		t.Error("time without private binding accepted")
	}
}

func TestCanaryDeterministicPlanAndPrivateIdentity(t *testing.T) {
	t.Parallel()
	c := testCanary(t)
	u, err := RenderCanary(c, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if u.Console.Identity != nil || u.Plan.Identity != nil || u.Envelope.Bindings != nil || u.Envelope.Metadata.EvaluatedAt != "" {
		t.Fatal("unbound plan invented a private target")
	}
	b := testCanaryBindings()
	now := canaryNow(t)
	one, err := RenderCanary(c, b, now)
	if err != nil {
		t.Fatal(err)
	}
	c.Rule.Notifications.States = []string{"ok", "alarm"}
	two, err := RenderCanary(c, b, now)
	if err != nil {
		t.Fatal(err)
	}
	x, err := Encode(one)
	if err != nil {
		t.Fatal(err)
	}
	y, err := Encode(two)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(x, y) {
		t.Error("equivalent canary policy rendering not deterministic")
	}
	if one.Envelope.Metadata.Authority != "PLAN_ONLY_NO_LIVE_AUTHORITY" || one.Envelope.Metadata.Evidence != "planned_not_observed" || one.Envelope.Metadata.GeneratorVersion == GeneratorVersion {
		t.Error("canary became a production or live qualification")
	}
	if one.Envelope.Metadata.ActualRuleNotificationControls != "PENDING_RULE_READBACK" || one.Console.Metadata.ActualRuleNotificationControls != "PENDING_RULE_READBACK" || one.Plan.Metadata.ActualRuleNotificationControls != "PENDING_RULE_READBACK" || u.Envelope.Metadata.ActualRuleNotificationControls != "PENDING_RULE_READBACK" {
		t.Error("honestly pending actual rule controls not preserved in both plan modes")
	}
	id := one.Console.Identity
	if id == nil || id.Instance != b.Bindings.Instance || !strings.HasSuffix(id.RuleName, b.Bindings.Instance) || !strings.Contains(id.Selector, b.Bindings.Instance) || one.Plan.Identity.Selector != id.Selector {
		t.Fatal("rule/read/sample identity drift")
	}
	if one.Plan.Phases[5].Allocation.Duration != "20m" || one.Plan.Phases[6].CumulativeDurationBudget != "1h30m0s" {
		t.Error("idle/cleanup reservation lost")
	}
	if one.Plan.Phases[2].Allocation.PlannedNotifications != 1 || !strings.Contains(one.Console.ClearMessage, "not application") {
		t.Error("initial OK or recovery limitation missing")
	}
	raw, err := Encode(one.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compare(raw, raw); err == nil {
		t.Error("normal compare accepted canary desired as actual")
	}
	if bytes.Contains(x, []byte("terraform")) || bytes.Contains(x, []byte("Bearer ")) {
		t.Error("unexpected executable resource or credential")
	}
}

func TestCanaryObservationDrivenTimingAndOpaqueChannel(t *testing.T) {
	t.Parallel()
	b := testCanaryBindings()
	b.Bindings.ChannelID = "push-method-v2:ABC.DEF"
	plan, err := RenderCanary(testCanary(t), b, canaryNow(t))
	if err != nil {
		t.Fatalf("opaque safe channel with explicit scope assertion: %v", err)
	}
	if plan.Console.Identity.ChannelID != b.Bindings.ChannelID {
		t.Error("opaque channel identity changed")
	}
	if !strings.Contains(plan.Plan.TimingSemantics, "not scheduled starts") {
		t.Error("duration budget presented as scheduled waits")
	}
	for _, p := range plan.Plan.Phases {
		want := "immediately_on_required_observation_within_upper_budget"
		if p.Allocation.ID == "idle" {
			want = "only_after_full_twenty_minute_idle_observation"
		}
		if p.AdvancePolicy != want {
			t.Errorf("phase %s advance=%q want=%q", p.Allocation.ID, p.AdvancePolicy, want)
		}
	}
	arming, breach := plan.Plan.Phases[2], plan.Plan.Phases[3]
	if !strings.Contains(arming.RequiredObservation, "fresh zero/native normal immediately before arming") || !strings.Contains(breach.RequiredObservation, "fresh zero/native normal immediately precedes breach") {
		t.Error("fresh baseline/native normal gates lost across configuration")
	}
	if !strings.Contains(arming.Action, "unsaved exact test-alert form") || !strings.Contains(arming.Action, "Check result") || !strings.Contains(arming.Action, "persisted rule and attachment settings") || !strings.Contains(breach.Action, "Before the first breach write, require exact persisted attachment readback") {
		t.Error("supported unsaved baseline check and post-save attachment readback gate missing")
	}
	if strings.Contains(arming.Action, "notifications unarmed") {
		t.Error("unsupported saved-unarmed workflow retained")
	}
	joined := strings.Join(plan.Plan.StopConditions, " ")
	if !strings.Contains(joined, "configuration or waiting consumes baseline freshness") || !strings.Contains(joined, "do not silently add refresh samples or restart the budget") {
		t.Error("configuration freshness exhaustion not fail-closed within original caps")
	}
	raw, err := Encode(plan.Plan)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(`"start_offset"`)) || bytes.Contains(raw, []byte(`"deadline_offset"`)) {
		t.Error("generated plan retains scheduled-looking starts")
	}
	if plan.Plan.Phases[5].Allocation.Duration != "20m" || plan.Plan.Phases[6].CumulativeDurationBudget != "1h30m0s" || plan.Plan.Budget.MetricWrites != 12 {
		t.Error("timing fix widened original idle/time/write limits")
	}
}

func TestCanaryPrivateFilesAndExampleNotAdmitted(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "binding.yaml")
	data, err := os.ReadFile("../../observability/canary-bindings.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := ReadCanaryBindings(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RenderCanary(testCanary(t), b, canaryNow(t)); err == nil {
		t.Error("deliberately incomplete example admitted")
	}
	for _, field := range []string{"notification_states_checked", "repeat_checked"} {
		var obsolete CanaryBindingsDocument
		changed := append(append([]byte{}, data...), []byte("\n    "+field+": true\n")...)
		if err := Decode(changed, &obsolete); err == nil {
			t.Errorf("obsolete fake channel preflight %s accepted", field)
		}
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCanaryBindings(path); err == nil {
		t.Error("permissive binding accepted")
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCanaryBindings(link); err == nil {
		t.Error("symlinked binding accepted")
	}
}

func TestCanaryDoesNotWeakenProduction(t *testing.T) {
	t.Parallel()
	c, err := Load("../../observability", "cloud-dev")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Profile.EnabledAlerts) != 0 {
		t.Fatal("production rules enabled")
	}
	c.Queries[0].Service = "custom"
	if err := c.Validate(); err == nil {
		t.Error("production accepted synthetic service")
	}
	for name, schema := range CanarySchemas() {
		data, err := json.Marshal(schema)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(data, []byte(`"additionalProperties":false`)) || !bytes.Contains(data, []byte(`"required"`)) {
			t.Errorf("schema %s not closed/required", name)
		}
	}
}

func FuzzCanaryDecode(f *testing.F) {
	seed, err := os.ReadFile("../../observability/canary.yaml")
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte("schema_version: 1\nkind: &x synthetic_metric_canary\n"))
	f.Add([]byte("schema_version: 1\nschema_version: 1\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxDocumentBytes+1 {
			return
		}
		var c CanaryDocument
		if err := Decode(data, &c); err != nil {
			return
		}
		if err := c.Validate(); err != nil {
			return
		}
		if _, err := RenderCanary(&c, nil, time.Time{}); err != nil {
			t.Errorf("valid decoded canary failed render: %v", err)
		}
	})
}
