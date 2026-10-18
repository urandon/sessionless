package configuredinference

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func usageCount(v uint64) *uint64 { return &v }

func TestConfiguredUsageProjectionTruthfulReportedUnknownAndOwnedCopies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		usage UsageV1
		want  domain.AttemptOperationUsageV1
	}{
		{"unknown", UsageV1{Provenance: UsageUnknownV1}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}},
		{"reported_input", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(9)}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(9)}},
		{"reported_output_zero", UsageV1{Provenance: UsageProviderReportedV1, OutputTokens: usageCount(0)}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: usageCount(0)}},
		{"reported_full", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(9), OutputTokens: usageCount(2), TotalTokens: usageCount(11)}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(9), OutputTokens: usageCount(2)}},
		{"reported_without_total", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(9), OutputTokens: usageCount(2)}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(9), OutputTokens: usageCount(2)}},
		{"uint64_without_inference", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(math.MaxUint64)}, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: usageCount(math.MaxUint64)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.usage.OperationUsage()
			if err != nil || !reflect.DeepEqual(got, tc.want) || got.Validate() != nil || got.AmountMicrounits != nil {
				t.Fatalf("truthful usage projection: err=%v gotState=%s wantState=%s", err, got.State, tc.want.State)
			}
			if tc.usage.InputTokens != nil {
				old := *tc.usage.InputTokens
				*got.InputTokens = 0
				if *tc.usage.InputTokens != old {
					t.Fatal("operation usage aliases provider input count")
				}
				*tc.usage.InputTokens = 17
				if *got.InputTokens != 0 {
					t.Fatal("later provider mutation changed projected input")
				}
			}
			if tc.usage.OutputTokens != nil {
				old := *tc.usage.OutputTokens
				*got.OutputTokens = 7
				if *tc.usage.OutputTokens != old {
					t.Fatal("operation usage aliases provider output count")
				}
			}
		})
	}
}

func TestConfiguredUsageProjectionRejectsForgedProvenanceAndTotals(t *testing.T) {
	for _, tc := range []struct {
		name  string
		usage UsageV1
	}{
		{"empty_provenance", UsageV1{}},
		{"invented_measured", UsageV1{Provenance: "observed", InputTokens: usageCount(1)}},
		{"unknown_input", UsageV1{Provenance: UsageUnknownV1, InputTokens: usageCount(0)}},
		{"unknown_output", UsageV1{Provenance: UsageUnknownV1, OutputTokens: usageCount(0)}},
		{"unknown_total", UsageV1{Provenance: UsageUnknownV1, TotalTokens: usageCount(0)}},
		{"empty_report", UsageV1{Provenance: UsageProviderReportedV1}},
		{"total_only", UsageV1{Provenance: UsageProviderReportedV1, TotalTokens: usageCount(1)}},
		{"total_without_output", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(1), TotalTokens: usageCount(1)}},
		{"total_without_input", UsageV1{Provenance: UsageProviderReportedV1, OutputTokens: usageCount(1), TotalTokens: usageCount(1)}},
		{"inconsistent_total", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(2), OutputTokens: usageCount(3), TotalTokens: usageCount(4)}},
		{"overflow_total", UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(math.MaxUint64), OutputTokens: usageCount(1), TotalTokens: usageCount(0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.usage.OperationUsage()
			if err == nil || got.State != "" || got.InputTokens != nil || got.OutputTokens != nil || got.AmountMicrounits != nil {
				t.Fatalf("invalid usage returned usable projected evidence: err=%v state=%s", err, got.State)
			}
		})
	}
}

func TestConfiguredUsageDecodedWireNeverBecomesMeasuredMoney(t *testing.T) {
	f := freeze(t)
	base := string(responseWire("model-local", "stop", "complete known response", nil))
	for _, tc := range []struct {
		name, rawUsage string
		want           UsageV1
		overrun        bool
	}{
		{"complete_report", `{"prompt_tokens":9,"completion_tokens":2,"total_tokens":11}`, UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(9), OutputTokens: usageCount(2), TotalTokens: usageCount(11)}, false},
		{"missing_usage", "", UsageV1{Provenance: UsageUnknownV1}, false},
		{"no_valid_component", `{"prompt_tokens":-1,"completion_tokens":"bad","total_tokens":1}`, UsageV1{Provenance: UsageUnknownV1}, false},
		{"invalid_input_retains_output", `{"prompt_tokens":-1,"completion_tokens":6,"total_tokens":5}`, UsageV1{Provenance: UsageProviderReportedV1, OutputTokens: usageCount(6)}, true},
		{"mismatched_total_retains_counts", `{"prompt_tokens":100,"completion_tokens":2,"total_tokens":10}`, UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(100), OutputTokens: usageCount(2)}, true},
		{"missing_total_retains_overrun", `{"prompt_tokens":100,"completion_tokens":2}`, UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(100), OutputTokens: usageCount(2)}, true},
		{"missing_output_retains_overrun", `{"prompt_tokens":100}`, UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(100)}, true},
		{"fractional_output_retains_input", `{"prompt_tokens":100,"completion_tokens":1.5}`, UsageV1{Provenance: UsageProviderReportedV1, InputTokens: usageCount(100)}, true},
		{"total_only_not_a_component", `{"total_tokens":100}`, UsageV1{Provenance: UsageUnknownV1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var obj map[string]json.RawMessage
			if json.Unmarshal([]byte(base), &obj) != nil {
				t.Fatal("independent wire oracle")
			}
			if tc.rawUsage == "" {
				delete(obj, "usage")
			} else {
				obj["usage"] = json.RawMessage(tc.rawUsage)
			}
			wire, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			response, err := f.DecodeResponse(wire)
			if err != nil || response.Stop != StopFinalV1 || response.Text != "complete known response" || !reflect.DeepEqual(response.Usage, tc.want) {
				t.Fatalf("complete response lost independent usage component: err=%v stop=%s provenance=%s", err, response.Stop, response.Usage.Provenance)
			}
			usage, err := response.Usage.OperationUsage()
			if err != nil || usage.AmountMicrounits != nil {
				t.Fatalf("wire projection inferred measured money: err=%v state=%s", err, usage.State)
			}
			if got := usage.ExceedsReservation(domain.AttemptOperationDebitV1{InputTokens: 10, OutputTokens: 5}); got != tc.overrun {
				t.Fatalf("valid known wire overrun hidden by absent/invalid metadata: got=%t want=%t", got, tc.overrun)
			}
			if tc.want.Provenance == UsageUnknownV1 && (usage.State != domain.AttemptOperationUsageUnknownV1 || usage.InputTokens != nil || usage.OutputTokens != nil) {
				t.Fatal("unknown projection fabricated token zero")
			}
		})
	}
	// Projection does not bypass the existing full lexical JSON validator.
	var decoded map[string]any
	if json.Unmarshal([]byte(base), &decoded) != nil {
		t.Fatal("independent valid response oracle")
	}
	if _, err := f.DecodeResponse([]byte(strings.Replace(base, `"prompt_tokens":9`, `"prompt_tokens":1e9999`, 1))); err == nil {
		t.Fatal("invalid numeric grammar/range produced usage evidence")
	}
}
