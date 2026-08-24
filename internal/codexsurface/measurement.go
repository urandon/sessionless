package codexsurface

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

const AuthenticatedMeasurementSchemaVersion = 1

type Distribution struct {
	Name  string `json:"name"`
	Unit  string `json:"unit"`
	Count int    `json:"count"`
	Min   int64  `json:"min"`
	P50   int64  `json:"p50"`
	P95   int64  `json:"p95"`
	P99   int64  `json:"p99"`
	Max   int64  `json:"max"`
}

// PublicMeasurementReport is intentionally aggregate-only. Raw observations,
// prompts, output, credentials, account identity and protocol frames are not
// representable in this contract.
type PublicMeasurementReport struct {
	SchemaVersion int            `json:"schema_version"`
	Phase         string         `json:"phase"`
	Surface       Surface        `json:"surface"`
	Status        string         `json:"status"`
	Version       string         `json:"version"`
	Runtime       string         `json:"runtime"`
	OS            string         `json:"os"`
	Arch          string         `json:"arch"`
	SampleCount   int            `json:"sample_count"`
	Metrics       []Distribution `json:"metrics"`
	Checks        []Check        `json:"checks"`
	FailureCounts map[string]int `json:"failure_counts"`
	FindingCodes  []string       `json:"finding_codes,omitempty"`
}

func MillisecondDistribution(name string, samples []time.Duration) (Distribution, error) {
	values := make([]int64, len(samples))
	for index, sample := range samples {
		if sample < 0 {
			return Distribution{}, errors.New("negative measurement sample")
		}
		values[index] = sample.Milliseconds()
	}
	return integerDistribution(name, "ms", values)
}

func ByteDistribution(name string, samples []int64) (Distribution, error) {
	return integerDistribution(name, "bytes", samples)
}

func integerDistribution(name, unit string, samples []int64) (Distribution, error) {
	if !validCode(name) || (unit != "ms" && unit != "bytes" && unit != "count") || len(samples) == 0 {
		return Distribution{}, errors.New("invalid measurement distribution")
	}
	values := append([]int64(nil), samples...)
	for _, value := range values {
		if value < 0 {
			return Distribution{}, errors.New("negative measurement sample")
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return Distribution{
		Name: name, Unit: unit, Count: len(values), Min: values[0],
		P50: nearestRank(values, 50), P95: nearestRank(values, 95),
		P99: nearestRank(values, 99), Max: values[len(values)-1],
	}, nil
}

func nearestRank(sorted []int64, percentile int) int64 {
	index := (percentile*len(sorted) + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func (report PublicMeasurementReport) Validate() error {
	if report.SchemaVersion != AuthenticatedMeasurementSchemaVersion || report.Phase != "explicitly-consented-local" {
		return errors.New("unsupported authenticated measurement contract")
	}
	if report.Surface != SurfaceAppServer && report.Surface != SurfaceExec && report.Surface != SurfaceSDK {
		return errors.New("unsupported authenticated measurement surface")
	}
	if report.Status != "pass" && report.Status != "no_go" && report.Status != "unsupported" {
		return errors.New("unsupported authenticated measurement status")
	}
	if report.SampleCount < 30 || report.Version == "" || report.Runtime == "" || report.OS == "" || report.Arch == "" {
		return errors.New("incomplete authenticated measurement provenance")
	}
	if len(report.Metrics) == 0 || len(report.Checks) == 0 {
		return errors.New("authenticated measurement evidence is empty")
	}
	seenMetrics := make(map[string]struct{}, len(report.Metrics))
	for _, metric := range report.Metrics {
		if !validCode(metric.Name) || metric.Count != report.SampleCount || metric.Min > metric.P50 || metric.P50 > metric.P95 ||
			metric.P95 > metric.P99 || metric.P99 > metric.Max {
			return errors.New("invalid authenticated measurement distribution")
		}
		if _, exists := seenMetrics[metric.Name]; exists {
			return errors.New("duplicate authenticated measurement distribution")
		}
		seenMetrics[metric.Name] = struct{}{}
	}
	totalFailures := 0
	for code, count := range report.FailureCounts {
		if !validCode(code) || count < 0 || count > report.SampleCount {
			return errors.New("invalid authenticated failure count")
		}
		totalFailures += count
	}
	if totalFailures > report.SampleCount {
		return errors.New("authenticated failure counts exceed sample count")
	}
	seenChecks := make(map[string]struct{}, len(report.Checks))
	hasFailedCheck := false
	for _, check := range report.Checks {
		if !validCode(check.Name) {
			return errors.New("invalid authenticated check code")
		}
		if _, exists := seenChecks[check.Name]; exists {
			return errors.New("duplicate authenticated check code")
		}
		seenChecks[check.Name] = struct{}{}
		if !check.Pass {
			hasFailedCheck = true
		}
	}
	if report.Status == "pass" && hasFailedCheck {
		return errors.New("passing authenticated report contains a failed check")
	}
	if report.Status == "no_go" && !hasFailedCheck {
		return errors.New("no-go authenticated report has no failed check")
	}
	seenFindings := make(map[string]struct{}, len(report.FindingCodes))
	for _, finding := range report.FindingCodes {
		if !validCode(finding) {
			return errors.New("invalid authenticated finding code")
		}
		if _, exists := seenFindings[finding]; exists {
			return errors.New("duplicate authenticated finding code")
		}
		seenFindings[finding] = struct{}{}
	}
	return nil
}

func (report PublicMeasurementReport) Marshal() ([]byte, error) {
	if err := report.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode authenticated measurement report: %w", err)
	}
	return append(encoded, '\n'), nil
}
