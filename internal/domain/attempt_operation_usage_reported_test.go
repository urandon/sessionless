package domain_test

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func reportedUsageMetric(v uint64) *uint64 { return &v }

func TestAttemptOperationReportedUsageClosedProvenanceAndOldShapes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		usage domain.AttemptOperationUsageV1
		valid bool
	}{
		{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, true},
		{"reported_input_zero", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(0)}, true},
		{"reported_output", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: reportedUsageMetric(3)}, true},
		{"reported_both", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(9), OutputTokens: reportedUsageMetric(2)}, true},
		{"observed_amount_only", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: reportedUsageMetric(0)}, true},
		{"observed_input_only", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: reportedUsageMetric(1)}, true},
		{"unknown_input_zero", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1, InputTokens: reportedUsageMetric(0)}, false},
		{"unknown_amount_zero", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1, AmountMicrounits: reportedUsageMetric(0)}, false},
		{"empty_report", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1}, false},
		{"reported_money", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(1), AmountMicrounits: reportedUsageMetric(0)}, false},
		{"reported_money_only", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, AmountMicrounits: reportedUsageMetric(0)}, false},
		{"empty_observed", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1}, false},
		{"invented_state", domain.AttemptOperationUsageV1{State: "estimated", InputTokens: reportedUsageMetric(1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.usage.Validate(); (err == nil) != tc.valid {
				t.Fatalf("usage validity got err=%v want valid=%t", err, tc.valid)
			}
		})
	}
	for _, tc := range []struct {
		usage domain.AttemptOperationUsageV1
		want  string
	}{
		{domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, `{"state":"unknown"}`},
		{domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: reportedUsageMetric(0)}, `{"state":"observed","amount_microunits":0}`},
	} {
		raw, err := json.Marshal(tc.usage)
		if err != nil || string(raw) != tc.want {
			t.Fatalf("old canonical usage shape changed: got=%s want=%s err=%v", raw, tc.want, err)
		}
	}
}

func TestAttemptOperationReportedUsageOverrunIsPresentMetricComparison(t *testing.T) {
	d := domain.AttemptOperationDebitV1{InputTokens: 10, OutputTokens: 5, AmountMicrounits: 2}
	for _, tc := range []struct {
		name  string
		usage domain.AttemptOperationUsageV1
		want  bool
	}{
		{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, false},
		{"reported_equal_input", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(10)}, false},
		{"reported_input_over", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(11)}, true},
		{"reported_output_over", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: reportedUsageMetric(6)}, true},
		{"observed_money_over", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: reportedUsageMetric(3)}, true},
		{"observed_money_equal", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: reportedUsageMetric(2)}, false},
		{"uint64_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(math.MaxUint64)}, true},
		// This deliberately invalid provenance still compares its present metric:
		// ExceedsReservation is not a validator or an authorization substitute.
		{"invalid_unknown_still_structural", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1, OutputTokens: reportedUsageMetric(6)}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.usage.ExceedsReservation(d); got != tc.want {
				t.Fatalf("present-metric overrun=%t want=%t", got, tc.want)
			}
		})
	}
}

func TestAttemptOperationReportedObservationIsRecordableOwnedAndSealed(t *testing.T) {
	r := operationReservationFixture(t, operationFixture())
	o := completedOperationFixture(t, r)
	o.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(r.Operation.Debit.InputTokens + 1), OutputTokens: reportedUsageMetric(0)}
	if err := o.ValidateForReservation(r); err != nil {
		t.Fatalf("known reported overrun must remain recordable evidence: %v", err)
	}
	digest, err := o.Digest()
	if err != nil {
		t.Fatal(err)
	}
	measured := o.Clone()
	measured.Usage.State = domain.AttemptOperationUsageObservedV1
	measuredDigest, err := measured.Digest()
	if err != nil || measuredDigest == digest {
		t.Fatal("provenance was not sealed independently of identical numeric metrics")
	}
	clone := o.Clone()
	*clone.Usage.InputTokens = 1
	if *o.Usage.InputTokens != r.Operation.Debit.InputTokens+1 {
		t.Fatal("observation clone aliases original reported count")
	}
	changedDigest, _ := clone.Digest()
	if changedDigest == digest {
		t.Fatal("reported count mutation did not change observation digest")
	}
	wrong := o.Clone()
	wrong.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("c", 64))
	if wrong.ValidateForReservation(r) == nil {
		t.Fatal("reported provenance bypassed exact original operation correlation")
	}
	for _, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationMCPToolV1, domain.AttemptOperationWebSearchV1} {
		op := toolOperationFixture()
		op.Kind = kind
		if kind == domain.AttemptOperationMCPToolV1 {
			op.Debit.WebSearches = 0
			op.QueryDigest = ""
			op.ConsentRequestDigest = ""
			op.ConsentDecisionDigest = ""
			op.ConsentDecisionRef = ""
			op.Debit.MCPOutboundMessages = 1
			op.Debit.MCPInboundFrames = 1
			op.Debit.MCPInboundBytes = 1
		}
		tool := operationReservationFixture(t, op)
		observation := completedOperationFixture(t, tool)
		observation.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(0)}
		if observation.ValidateForReservation(tool) == nil {
			t.Fatalf("provider-reported state accepted for %s predecessor", kind)
		}
	}
}

func TestAttemptOperationReportedModelCanPrecedeExactToolButNotOverrun(t *testing.T) {
	r := operationReservationFixture(t, operationFixture())
	for _, tc := range []struct {
		name  string
		usage domain.AttemptOperationUsageV1
		want  bool
	}{
		{"unknown", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}, true},
		{"partial_measured", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, InputTokens: reportedUsageMetric(1)}, true},
		{"reported", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: reportedUsageMetric(1)}, true},
		{"reported_overrun", domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, OutputTokens: reportedUsageMetric(r.Operation.Debit.OutputTokens + 1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := completedOperationFixture(t, r)
			o.Usage = tc.usage
			d, err := o.Digest()
			if err != nil {
				t.Fatal(err)
			}
			next := toolOperationFixture()
			next.PredecessorObservationDigest = d
			next.ModelResponseDigest = o.ResponseDigest
			if err := next.ValidatePredecessor(r, o); (err == nil) != tc.want {
				t.Fatalf("model→tool exact predecessor err=%v want eligibility=%t", err, tc.want)
			}
		})
	}
}
