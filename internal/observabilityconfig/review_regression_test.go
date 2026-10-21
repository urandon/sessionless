package observabilityconfig

import (
	"strings"
	"testing"
)

func gatewayFixture() *Config {
	c := fixtureConfig()
	q := &c.Queries[0]
	q.Service = "serverless-apigateway"
	q.Metric = "api_gateway.requests_count_per_second"
	q.Unit = "requests_per_second"
	q.LabelKeys = []string{"service", "name", "gateway"}
	q.SeriesAggregation = "sum"
	q.Sparse = true
	q.Freshness = "last_observed"
	a := &c.Alerts[0]
	a.Class = "platform"
	a.Condition.Unit = q.Unit
	a.Demand = "positive_activity"
	a.Recovery = "metric_clear_only"
	return c
}

func TestQueryGuardsRejectTheirIntendedMutation(t *testing.T) {
	t.Parallel()
	storage := fixtureConfig()
	storage.Alerts = []Alert{}
	q := &storage.Queries[0]
	q.Service = "storage"
	q.Metric = "space_usage"
	q.Unit = "bytes"
	q.LabelKeys = []string{"service", "name", "resource_type", "resource_id", "storage_type"}
	q.FixedLabels = map[string]string{"resource_type": "bucket", "storage_type": "All"}

	ratio := cloneConfig(t, storage)
	ratio.Queries[0].Metric = "proposed_compatible_capacity"
	ratio.Queries[0].Unit = "percent"
	ratio.Queries[0].Qualifications = []string{"compatible_limit"}
	ratio.Queries[0].Evidence = "PROPOSED"
	ratio.Profile.EnabledQueries = []string{} // This adapter cannot deploy a ratio.

	for _, tc := range []struct {
		name   string
		base   *Config
		change func(*Config)
		guard  string
	}{
		{"rate_samples_not_event_count", gatewayFixture(), func(c *Config) { c.Queries[0].Aggregation = "sum" }, "rate samples cannot be summed"},
		{"storage_component_not_all_total", storage, func(c *Config) { c.Queries[0].FixedLabels["storage_type"] = "Simple" }, "non-overlapping All total"},
		{"ratio_needs_compatible_positive_limit", ratio, func(c *Config) { c.Queries[0].Qualifications = []string{} }, "ratio requires compatible used and positive limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.base.Validate(); err != nil {
				t.Fatalf("coherent positive baseline rejected: %v", err)
			}
			changed := cloneConfig(t, tc.base)
			tc.change(changed)
			err := changed.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.guard) {
				t.Fatalf("single mutation: got %v, want guard %q", err, tc.guard)
			}
		})
	}
}

func TestRecoveryCannotBeRelabeledByCallerClass(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		change func(*Config)
		guard  string
	}{
		{"joint_queue_relabel", func(c *Config) {
			a := &c.Alerts[0]
			a.Class = "platform"
			a.Recovery = "metric_clear_only"
			a.Demand = "positive_activity"
			a.Notifications.States = []string{"alarm", "ok"}
		}, "cannot relabel queue or semantic"},
		{"native_logging_aggregate", func(c *Config) { c.Queries[0].Service = "logging_aggregates" }, "source and service must agree"},
		{"semantic_native_service", func(c *Config) { c.Queries[0].Source = "semantic" }, "source and service must agree"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fixtureConfig()
			if err := c.Validate(); err != nil {
				t.Fatalf("positive baseline: %v", err)
			}
			tc.change(c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.guard) {
				t.Fatalf("joint relabel: got %v, want guard %q", err, tc.guard)
			}
		})
	}
}

func TestGatewayQueryPreservesCrossSeriesAggregate(t *testing.T) {
	t.Parallel()
	c := gatewayFixture()
	c.Profile.EnabledAlerts = []string{c.Alerts[0].ID}
	b, o, now := fixturePrivate(c)
	b.Bindings.Queries[0].Unit = "requests_per_second"
	b.Bindings.Queries[0].Labels = map[string]string{"service": "serverless-apigateway", "name": "api_gateway.requests_count_per_second", "gateway": "fixture-gateway"}
	bundle, err := Render(c, b, o, now)
	if err != nil {
		t.Fatalf("render coherent gateway: %v", err)
	}
	// No grouping key: all matching path/code/release/operation series collapse
	// at each instant. Grid MAX is separate; this is not sum(samples) -> count.
	// Vendor function semantics were measured by #223, not simulated by this test.
	want := `series_sum("api_gateway.requests_count_per_second"{folderId="fixture-folder",gateway="fixture-gateway",service="serverless-apigateway"})`
	if got := bundle.Envelope.Console[0].Selector; got != want {
		t.Fatalf("console query = %q, want aggregate %q", got, want)
	}
	resource := bundle.Terraform["resource"].(map[string]any)["yandex_monitoring_dashboard"].(map[string]any)["overview"].(map[string]any)
	widgets := resource["widgets"].([]any)
	charts := 0
	for _, widget := range widgets {
		chart, ok := widget.(map[string]any)["chart"]
		if !ok {
			continue
		}
		charts++
		queries := chart.([]any)[0].(map[string]any)["queries"].([]any)[0].(map[string]any)
		got := queries["target"].([]any)[0].(map[string]any)["query"]
		if got != want {
			t.Fatalf("dashboard query = %q, want %q", got, want)
		}
		if got := queries["downsampling"].([]any)[0].(map[string]any)["grid_aggregation"]; got != "GRID_AGGREGATION_MAX" {
			t.Fatalf("grid aggregation = %v; cross-series sum must not replace time-grid MAX", got)
		}
	}
	if charts != 1 {
		t.Fatalf("charts = %d, want one aggregate chart", charts)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Queries[0].SeriesAggregation = "none" }, func(c *Config) { c.Queries[0].MaxSeries = 2 }} {
		bad := cloneConfig(t, c)
		change(bad)
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "single series_sum aggregate") {
			t.Fatalf("unbounded raw gateway query: got %v", err)
		}
	}
}
