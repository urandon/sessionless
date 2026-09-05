package yandexsubstrate

import (
	"strings"
	"testing"
	"time"
)

var reportTestTime = time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)

func TestFinalizeFailsClosedAcrossFailureAndUnknownEvidence(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*PublicReport)
		want    Recommendation
		blocker string
	}{
		{name: "complete evidence", want: RecommendationGo},
		{name: "failed safety gate", edit: func(report *PublicReport) {
			report.Gates[gateIndex(report.Gates, "redelivery_single_effect_owner")].State = EvidenceFail
		}, want: RecommendationNoGo, blocker: "redelivery_single_effect_owner"},
		{name: "unknown price", edit: func(report *PublicReport) {
			quantity := &report.Quantities[quantityIndex(report.Quantities, "estimated_cost_microunits")]
			quantity.State, quantity.Value, quantity.Unit, quantity.ObservedAt = QuantityUnknown, nil, "", nil
		}, want: RecommendationConditional, blocker: "estimated_cost_microunits"},
		{name: "unknown latency", edit: func(report *PublicReport) {
			metric := &report.Metrics[metricIndex(report.Metrics, "cold_start_ms")]
			metric.State, metric.Distribution = QuantityUnknown, nil
		}, want: RecommendationConditional, blocker: "cold_start_ms"},
		{name: "insufficient lifecycle cohort", edit: func(report *PublicReport) {
			report.Samples.Warm = MinimumLifecycleSamples - 1
		}, want: RecommendationConditional, blocker: "lifecycle_sample_floor"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := validReport()
			if test.edit != nil {
				test.edit(&report)
			}
			finalized, err := report.Finalize()
			if err != nil {
				t.Fatal(err)
			}
			if finalized.Recommendation != test.want {
				t.Fatalf("recommendation = %q, want %q", finalized.Recommendation, test.want)
			}
			if test.blocker != "" && !contains(finalized.Blockers, test.blocker) {
				t.Fatalf("blockers = %v, want %q", finalized.Blockers, test.blocker)
			}
		})
	}
}

func TestUnknownQuantityCannotMasqueradeAsFree(t *testing.T) {
	report := validReport()
	quantity := &report.Quantities[quantityIndex(report.Quantities, "estimated_cost_microunits")]
	zero := int64(0)
	quantity.State, quantity.Value, quantity.Unit, quantity.ObservedAt = QuantityUnknown, &zero, "currency_microunit", &reportTestTime
	if _, err := report.Finalize(); err == nil {
		t.Fatal("unknown price with a zero value was accepted")
	}
}

func TestUnknownMetricCannotCarryADistribution(t *testing.T) {
	report := validReport()
	metric := &report.Metrics[metricIndex(report.Metrics, "cold_start_ms")]
	metric.State = QuantityUnknown
	if _, err := report.Finalize(); err == nil {
		t.Fatal("unknown latency with a distribution was accepted")
	}
}

func TestValidateRejectsCallerSelectedRecommendation(t *testing.T) {
	report, err := validReport().Finalize()
	if err != nil {
		t.Fatal(err)
	}
	report.Recommendation = RecommendationNoGo
	if err := report.Validate(); err == nil {
		t.Fatal("caller-selected recommendation was accepted")
	}
}

func TestReportRejectsMissingSafetyGateAndNonOfficialProvenance(t *testing.T) {
	tests := []struct {
		name string
		edit func(*PublicReport)
	}{
		{name: "missing gate", edit: func(report *PublicReport) { report.Gates = report.Gates[1:] }},
		{name: "mutable image tag", edit: func(report *PublicReport) { report.Candidate.ImageDigest = "latest" }},
		{name: "untrusted price source", edit: func(report *PublicReport) {
			report.Quantities[0].SourceURL = "https://example.com/price"
		}},
		{name: "identity bearing source", edit: func(report *PublicReport) {
			report.Quantities[0].SourceURL = "https://account@yandex.cloud/en/docs/serverless-containers/pricing"
		}},
		{name: "known cost without currency", edit: func(report *PublicReport) {
			report.Candidate.BillingCurrency = "unknown"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := validReport()
			test.edit(&report)
			if _, err := report.Finalize(); err == nil {
				t.Fatal("invalid report was accepted")
			}
		})
	}
}

func TestMarshalProducesAggregatePublicSafeDecision(t *testing.T) {
	report := validReport()
	encoded, err := report.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"tenant_id", "user_id", "queue_body", "prompt", "provider_output", "credential", "hostname", "raw_log"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public report contains forbidden field %q", forbidden)
		}
	}
}

func validReport() PublicReport {
	gates := make([]Gate, len(mandatoryGates))
	for index, name := range mandatoryGates {
		gates[index] = Gate{Name: name, State: EvidencePass}
	}
	value := int64(1)
	quantities := make([]Quantity, len(mandatoryQuantities))
	for index, name := range mandatoryQuantities {
		quantities[index] = Quantity{
			Name: name, State: QuantityKnown, Unit: expectedQuantityUnit(name), Value: &value,
			SourceURL: "https://yandex.cloud/en/docs/serverless-containers/pricing", ObservedAt: &reportTestTime,
		}
	}
	return PublicReport{
		SchemaVersion: ReportSchemaVersion,
		Candidate: Candidate{
			Region: "ru-central1", BillingCurrency: "RUB", ImageDigest: strings.Repeat("a", 64),
			ProfileDigest: strings.Repeat("b", 64), RuntimeRevision: "worker-v1", ObservedAt: reportTestTime,
		},
		Samples: SampleCounts{Cold: 30, Warm: 30, LongCall: 1, Redelivery: 1, LostResponse: 1, Cancellation: 1, FenceLoss: 1, WarmReuse: 1},
		Metrics: validMetrics(),
		Gates:   gates, Quantities: quantities,
	}
}

func validMetrics() []Metric {
	metrics := make([]Metric, len(mandatoryMetrics))
	for index, name := range mandatoryMetrics {
		unit := "ms"
		if strings.HasSuffix(name, "_bytes") {
			unit = "bytes"
		}
		metrics[index] = Metric{Name: name, State: QuantityKnown, Distribution: &Distribution{Unit: unit, Count: 60, Min: 1, P50: 2, P95: 3, P99: 4, Max: 5}}
	}
	return metrics
}

func metricIndex(metrics []Metric, name string) int {
	for index := range metrics {
		if metrics[index].Name == name {
			return index
		}
	}
	panic("metric not found")
}

func gateIndex(gates []Gate, name string) int {
	for index := range gates {
		if gates[index].Name == name {
			return index
		}
	}
	panic("gate not found")
}

func quantityIndex(quantities []Quantity, name string) int {
	for index := range quantities {
		if quantities[index].Name == name {
			return index
		}
	}
	panic("quantity not found")
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
