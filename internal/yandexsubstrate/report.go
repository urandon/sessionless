// Package yandexsubstrate defines the public-safe evidence contract used to
// decide whether an immutable Yandex Serverless Containers candidate may be
// promoted as a Sessionless execution substrate.
package yandexsubstrate

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ReportSchemaVersion     = 1
	MinimumLifecycleSamples = 30
)

type Recommendation string

const (
	RecommendationGo          Recommendation = "go"
	RecommendationConditional Recommendation = "conditional"
	RecommendationNoGo        Recommendation = "no_go"
)

type EvidenceState string

const (
	EvidencePass    EvidenceState = "pass"
	EvidenceFail    EvidenceState = "fail"
	EvidenceUnknown EvidenceState = "unknown"
)

type QuantityState string

const (
	QuantityKnown   QuantityState = "known"
	QuantityUnknown QuantityState = "unknown"
)

type Candidate struct {
	Region          string    `json:"region"`
	BillingCurrency string    `json:"billing_currency"`
	ImageDigest     string    `json:"image_digest"`
	ProfileDigest   string    `json:"profile_digest"`
	RuntimeRevision string    `json:"runtime_revision"`
	ObservedAt      time.Time `json:"observed_at"`
}

type SampleCounts struct {
	Cold         int `json:"cold"`
	Warm         int `json:"warm"`
	LongCall     int `json:"long_call"`
	Redelivery   int `json:"redelivery"`
	LostResponse int `json:"lost_response"`
	Cancellation int `json:"cancellation"`
	FenceLoss    int `json:"fence_loss"`
	WarmReuse    int `json:"warm_reuse"`
}

type Distribution struct {
	Unit  string `json:"unit"`
	Count int    `json:"count"`
	Min   int64  `json:"min"`
	P50   int64  `json:"p50"`
	P95   int64  `json:"p95"`
	P99   int64  `json:"p99"`
	Max   int64  `json:"max"`
}

type Metric struct {
	Name         string        `json:"name"`
	State        QuantityState `json:"state"`
	Distribution *Distribution `json:"distribution,omitempty"`
}

type Gate struct {
	Name  string        `json:"name"`
	State EvidenceState `json:"state"`
}

// Quantity deliberately represents unknown with a nil value. A known zero is
// retained as zero and can never be confused with free or unavailable data.
type Quantity struct {
	Name       string        `json:"name"`
	State      QuantityState `json:"state"`
	Unit       string        `json:"unit,omitempty"`
	Value      *int64        `json:"value,omitempty"`
	SourceURL  string        `json:"source_url"`
	ObservedAt *time.Time    `json:"observed_at,omitempty"`
}

// PublicReport is aggregate-only. It cannot carry queue bodies, tenant or user
// identifiers, prompts, provider output, credentials, hostnames, or raw logs.
type PublicReport struct {
	SchemaVersion  int            `json:"schema_version"`
	Candidate      Candidate      `json:"candidate"`
	Samples        SampleCounts   `json:"samples"`
	Metrics        []Metric       `json:"metrics"`
	Gates          []Gate         `json:"gates"`
	Quantities     []Quantity     `json:"quantities"`
	Recommendation Recommendation `json:"recommendation"`
	Blockers       []string       `json:"blockers,omitempty"`
}

var codePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,95}$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

var mandatoryGates = []string{
	"container_concurrency_one",
	"fence_loss_blocks_success",
	"iam_authenticated_trigger",
	"lost_response_reconcile_only",
	"one_hour_guard_budget",
	"provider_processing_residency",
	"proxy_only_egress",
	"redelivery_single_effect_owner",
	"renewal_loss_blocks_success",
	"rollback_disables_profile",
	"silent_call_lease_watchdog",
	"taint_terminates_instance",
	"trigger_batch_one",
	"warm_descendants_stopped",
	"warm_sockets_closed",
	"warm_workspace_erased",
}

var mandatoryMetrics = []string{
	"active_duration_ms",
	"billed_duration_ms",
	"cleanup_duration_ms",
	"cold_start_ms",
	"egress_bytes",
	"output_bytes",
	"peak_memory_bytes",
	"queue_to_start_ms",
	"scratch_bytes",
	"warm_start_ms",
}

var mandatoryQuantities = []string{
	"capacity_max_memory_bytes",
	"capacity_max_request_duration_ms",
	"estimated_cost_microunits",
}

func (report PublicReport) Finalize() (PublicReport, error) {
	if err := report.validateEvidence(); err != nil {
		return PublicReport{}, err
	}
	blockers := make([]string, 0)
	hasFailure := false
	hasUnknown := false
	for _, gate := range report.Gates {
		switch gate.State {
		case EvidenceFail:
			hasFailure = true
			blockers = append(blockers, gate.Name)
		case EvidenceUnknown:
			hasUnknown = true
			blockers = append(blockers, gate.Name)
		}
	}
	for _, quantity := range report.Quantities {
		if quantity.State == QuantityUnknown {
			hasUnknown = true
			blockers = append(blockers, quantity.Name)
		}
	}
	for _, metric := range report.Metrics {
		if metric.State == QuantityUnknown {
			hasUnknown = true
			blockers = append(blockers, metric.Name)
		}
	}
	if report.Samples.Cold < MinimumLifecycleSamples || report.Samples.Warm < MinimumLifecycleSamples {
		hasUnknown = true
		blockers = append(blockers, "lifecycle_sample_floor")
	}
	for name, count := range map[string]int{
		"cancellation_sample":  report.Samples.Cancellation,
		"fence_loss_sample":    report.Samples.FenceLoss,
		"long_call_sample":     report.Samples.LongCall,
		"lost_response_sample": report.Samples.LostResponse,
		"redelivery_sample":    report.Samples.Redelivery,
		"warm_reuse_sample":    report.Samples.WarmReuse,
	} {
		if count == 0 {
			hasUnknown = true
			blockers = append(blockers, name)
		}
	}
	sort.Strings(blockers)
	report.Blockers = blockers
	switch {
	case hasFailure:
		report.Recommendation = RecommendationNoGo
	case hasUnknown:
		report.Recommendation = RecommendationConditional
	default:
		report.Recommendation = RecommendationGo
	}
	return report, nil
}

func (report PublicReport) Validate() error {
	finalized, err := report.Finalize()
	if err != nil {
		return err
	}
	if report.Recommendation != finalized.Recommendation || !equalStrings(report.Blockers, finalized.Blockers) {
		return errors.New("substrate recommendation does not match evidence")
	}
	return nil
}

func (report PublicReport) Marshal() ([]byte, error) {
	finalized, err := report.Finalize()
	if err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(finalized, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func (report PublicReport) validateEvidence() error {
	if report.SchemaVersion != ReportSchemaVersion {
		return errors.New("unsupported Yandex substrate report schema")
	}
	if report.Candidate.Region != "ru-central1" || report.Candidate.ObservedAt.IsZero() ||
		(report.Candidate.BillingCurrency != "unknown" && !currencyPattern.MatchString(report.Candidate.BillingCurrency)) ||
		!codePattern.MatchString(report.Candidate.RuntimeRevision) ||
		!validDigest(report.Candidate.ImageDigest) || !validDigest(report.Candidate.ProfileDigest) {
		return errors.New("invalid immutable Yandex substrate candidate")
	}
	counts := []int{report.Samples.Cold, report.Samples.Warm, report.Samples.LongCall,
		report.Samples.Redelivery, report.Samples.LostResponse, report.Samples.Cancellation,
		report.Samples.FenceLoss, report.Samples.WarmReuse}
	for _, count := range counts {
		if count < 0 || count > 10000 {
			return errors.New("invalid substrate sample count")
		}
	}
	if err := validateMetrics(report.Metrics); err != nil {
		return err
	}
	if err := validateGates(report.Gates); err != nil {
		return err
	}
	if err := validateQuantities(report.Quantities); err != nil {
		return err
	}
	if report.Candidate.BillingCurrency == "unknown" {
		for _, quantity := range report.Quantities {
			if quantity.Name == "estimated_cost_microunits" && quantity.State == QuantityKnown {
				return errors.New("known substrate cost requires a known billing currency")
			}
		}
	}
	return nil
}

func validateMetrics(metrics []Metric) error {
	seen := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		if !codePattern.MatchString(metric.Name) {
			return errors.New("invalid substrate metric")
		}
		if _, found := seen[metric.Name]; found {
			return errors.New("duplicate substrate metric")
		}
		seen[metric.Name] = struct{}{}
		switch metric.State {
		case QuantityKnown:
			if metric.Distribution == nil || metric.Distribution.Unit != expectedMetricUnit(metric.Name) ||
				metric.Distribution.Count <= 0 || metric.Distribution.Min < 0 ||
				metric.Distribution.Min > metric.Distribution.P50 ||
				metric.Distribution.P50 > metric.Distribution.P95 ||
				metric.Distribution.P95 > metric.Distribution.P99 ||
				metric.Distribution.P99 > metric.Distribution.Max {
				return errors.New("invalid substrate metric distribution")
			}
		case QuantityUnknown:
			if metric.Distribution != nil {
				return errors.New("unknown substrate metric fabricated a distribution")
			}
		default:
			return errors.New("invalid substrate metric state")
		}
	}
	for _, required := range mandatoryMetrics {
		if _, found := seen[required]; !found {
			return errors.New("missing mandatory substrate metric: " + required)
		}
	}
	if len(seen) != len(mandatoryMetrics) {
		return errors.New("unsupported public substrate metric")
	}
	return nil
}

func validateGates(gates []Gate) error {
	seen := make(map[string]EvidenceState, len(gates))
	for _, gate := range gates {
		if !codePattern.MatchString(gate.Name) ||
			(gate.State != EvidencePass && gate.State != EvidenceFail && gate.State != EvidenceUnknown) {
			return errors.New("invalid substrate gate")
		}
		if _, found := seen[gate.Name]; found {
			return errors.New("duplicate substrate gate")
		}
		seen[gate.Name] = gate.State
	}
	for _, required := range mandatoryGates {
		if _, found := seen[required]; !found {
			return errors.New("missing mandatory substrate gate: " + required)
		}
	}
	if len(seen) != len(mandatoryGates) {
		return errors.New("unsupported public substrate gate")
	}
	return nil
}

func validateQuantities(quantities []Quantity) error {
	seen := make(map[string]struct{}, len(quantities))
	for _, quantity := range quantities {
		if !codePattern.MatchString(quantity.Name) || !officialSourceURL(quantity.SourceURL) {
			return errors.New("invalid substrate quantity provenance")
		}
		if _, found := seen[quantity.Name]; found {
			return errors.New("duplicate substrate quantity")
		}
		seen[quantity.Name] = struct{}{}
		switch quantity.State {
		case QuantityKnown:
			if quantity.Value == nil || quantity.ObservedAt == nil || quantity.ObservedAt.IsZero() ||
				quantity.Unit != expectedQuantityUnit(quantity.Name) || *quantity.Value < 0 {
				return errors.New("known substrate quantity lacks a value or provenance")
			}
		case QuantityUnknown:
			if quantity.Value != nil || quantity.ObservedAt != nil || quantity.Unit != "" {
				return errors.New("unknown substrate quantity fabricated a value")
			}
		default:
			return errors.New("invalid substrate quantity state")
		}
	}
	for _, required := range mandatoryQuantities {
		if _, found := seen[required]; !found {
			return errors.New("missing mandatory substrate quantity: " + required)
		}
	}
	if len(seen) != len(mandatoryQuantities) {
		return errors.New("unsupported public substrate quantity")
	}
	return nil
}

func officialSourceURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host == "yandex.cloud" && parsed.User == nil &&
		strings.HasPrefix(parsed.Path, "/en/docs/") && parsed.RawQuery == "" && parsed.Fragment == ""
}

func expectedMetricUnit(name string) string {
	if strings.HasSuffix(name, "_bytes") {
		return "bytes"
	}
	return "ms"
}

func expectedQuantityUnit(name string) string {
	switch name {
	case "capacity_max_memory_bytes":
		return "bytes"
	case "capacity_max_request_duration_ms":
		return "ms"
	case "estimated_cost_microunits":
		return "currency_microunit"
	default:
		return ""
	}
}

func validDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
