package codexsurface

import (
	"strings"
	"testing"
	"time"
)

func TestDistributionUsesDeterministicNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for index := range samples {
		samples[index] = time.Duration(100-index) * time.Millisecond
	}
	distribution, err := MillisecondDistribution("turn_wall", samples)
	if err != nil {
		t.Fatal(err)
	}
	if distribution.Min != 1 || distribution.P50 != 50 || distribution.P95 != 95 || distribution.P99 != 99 || distribution.Max != 100 {
		t.Fatalf("distribution = %#v", distribution)
	}
}

func TestPublicMeasurementContractRejectsSmallOrIdentityBearingEvidence(t *testing.T) {
	report := PublicMeasurementReport{
		SchemaVersion: AuthenticatedMeasurementSchemaVersion,
		Phase:         "explicitly-consented-local", Surface: SurfaceExec, Status: "pass",
		Version: "0.148.0-alpha.15", Runtime: "0.148.0-alpha.15", OS: "darwin", Arch: "arm64",
		SampleCount: 29,
	}
	if err := report.Validate(); err == nil {
		t.Fatal("sub-30 sample report was accepted")
	}
	report.SampleCount = 30
	report.Metrics = []Distribution{{Name: "turn_wall", Unit: "ms", Count: 30, Min: 1, P50: 2, P95: 3, P99: 4, Max: 5}}
	report.Checks = []Check{{Name: "billing_route", Pass: true}}
	report.FailureCounts = map[string]int{"account@example.com": 1}
	if err := report.Validate(); err == nil {
		t.Fatal("identity-bearing failure code was accepted")
	}
}

func TestPublicMeasurementContractRequiresEveryMetricToCoverAllSamples(t *testing.T) {
	report := PublicMeasurementReport{
		SchemaVersion: AuthenticatedMeasurementSchemaVersion,
		Phase:         "explicitly-consented-local", Surface: SurfaceSDK, Status: "no_go",
		Version: "0.147.0", Runtime: "0.147.0", OS: "darwin", Arch: "arm64", SampleCount: 30,
		Metrics:       []Distribution{{Name: "spawn", Unit: "ms", Count: 29, Min: 1, P50: 2, P95: 3, P99: 4, Max: 5}},
		Checks:        []Check{{Name: "billing_route", Pass: false}},
		FailureCounts: map[string]int{"harness_failure": 1},
	}
	if err := report.Validate(); err == nil {
		t.Fatal("partial metric coverage was accepted")
	}
}

func TestPublicMeasurementContractRejectsOverlappingFailureCounts(t *testing.T) {
	report := PublicMeasurementReport{
		SchemaVersion: AuthenticatedMeasurementSchemaVersion,
		Phase:         "explicitly-consented-local", Surface: SurfaceExec, Status: "no_go",
		Version: "0.148.0-alpha.15", Runtime: "0.148.0-alpha.15", OS: "darwin", Arch: "arm64", SampleCount: 30,
		Metrics: []Distribution{{Name: "turn_wall", Unit: "ms", Count: 30, Min: 1, P50: 2, P95: 3, P99: 4, Max: 5}},
		Checks:  []Check{{Name: "billing_route", Pass: false}},
		FailureCounts: map[string]int{
			"provider_outage": 20,
			"harness_failure": 11,
		},
	}
	if err := report.Validate(); err == nil {
		t.Fatal("failure counts exceeding the sample population were accepted")
	}
}

func TestPublicMeasurementReportMarshalsAggregatesOnly(t *testing.T) {
	report := PublicMeasurementReport{
		SchemaVersion: AuthenticatedMeasurementSchemaVersion,
		Phase:         "explicitly-consented-local", Surface: SurfaceExec, Status: "pass",
		Version: "0.148.0-alpha.15", Runtime: "0.148.0-alpha.15", OS: "darwin", Arch: "arm64", SampleCount: 30,
		Metrics:       []Distribution{{Name: "turn_wall", Unit: "ms", Count: 30, Min: 1, P50: 2, P95: 3, P99: 4, Max: 5}},
		Checks:        []Check{{Name: "billing_route", Pass: true}},
		FailureCounts: map[string]int{"provider_outage": 0},
	}
	encoded, err := report.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"prompt", "output", "email", "auth_url", "device_code", "protocol_frame"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("public measurement contains forbidden field %q", forbidden)
		}
	}
}
