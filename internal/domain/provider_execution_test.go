package domain_test

import (
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func TestProviderExecutionEvidenceBindsAuthorityAndPreservesUnknownVsZero(t *testing.T) {
	t.Parallel()
	binding, err := sessionlessharness.NewDeterministicFixtureBindingV1(
		"tenant-1", "user-1", "run-1", "attempt-1", "subscription-1",
		domain.ManagedExecutionPlacementV1(), time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	base, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass:     domain.ProviderAcceptanceAcceptedV1,
		FinishClass:         domain.ProviderFinishCompletedV1,
		RouteState:          domain.ProviderEvidenceSupportedV1,
		ActualModelVendorID: "sessionless", ActualModelID: binding.ModelID, TransportProvider: "sessionless", UpstreamProviderID: "local", EndpointID: "fixture",
		TransportKind: domain.ProviderTransportLocalCLIV1,
		PolicyVerdict: domain.ProviderPolicyGoV1, UsageProvenance: domain.ProviderUsageHarnessMeasuredV1,
		InputTokens: &zero, OutputTokens: &zero,
	}).SealForBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := base.ValidateForBinding(binding); err != nil {
		t.Fatal(err)
	}
	unknown, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceUnknownV1, FinishClass: domain.ProviderFinishUnknownV1,
		RouteState: domain.ProviderEvidenceUnknownV1, PolicyVerdict: domain.ProviderPolicyUnknownV1,
		UsageProvenance: domain.ProviderUsageUnknownV1, FailureCode: domain.ProviderExecutionFailureAcceptedUnknownV1,
	}).SealForBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	if unknown.InputTokens != nil || unknown.OutputTokens != nil || unknown.EvidenceDigest == base.EvidenceDigest {
		t.Fatal("unknown usage collapsed into known zero")
	}

	for name, mutate := range map[string]func(*domain.ProviderExecutionEvidenceV1){
		"binding": func(value *domain.ProviderExecutionEvidenceV1) {
			value.BindingDigest = domain.HarnessBindingDigestV1(strings.Repeat("a", 64))
		},
		"vendor": func(value *domain.ProviderExecutionEvidenceV1) { value.ActualModelVendorID = "other-vendor" },
		"model":  func(value *domain.ProviderExecutionEvidenceV1) { value.ActualModelID = "other-model" },
		"route":  func(value *domain.ProviderExecutionEvidenceV1) { value.EndpointID = "other-endpoint" },
		"transport": func(value *domain.ProviderExecutionEvidenceV1) {
			value.TransportKind = domain.ProviderTransportDirectAPIV1
		},
		"data class": func(value *domain.ProviderExecutionEvidenceV1) { value.InputDataClass = domain.ProviderDataPublicV1 },
		"policy":     func(value *domain.ProviderExecutionEvidenceV1) { value.EffectivePolicyDigest = strings.Repeat("b", 64) },
		"usage":      func(value *domain.ProviderExecutionEvidenceV1) { value.InputTokens = nil },
		"digest": func(value *domain.ProviderExecutionEvidenceV1) {
			value.EvidenceDigest = domain.ProviderEvidenceDigestV1(strings.Repeat("c", 64))
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base.Clone()
			mutate(&candidate)
			if candidate.ValidateForBinding(binding) == nil {
				t.Fatal("authority mutation accepted")
			}
		})
	}
}

func TestProviderExecutionEvidenceLifecycleCompatibility(t *testing.T) {
	t.Parallel()
	binding, err := sessionlessharness.NewDeterministicFixtureBindingV1(
		"tenant-1", "user-1", "run-1", "attempt-1", "subscription-1",
		domain.ManagedExecutionPlacementV1(), time.Unix(10, 0).UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name       string
		acceptance domain.ProviderAcceptanceClassV1
		finish     domain.ProviderFinishClassV1
		failure    domain.ProviderExecutionFailureCodeV1
		valid      bool
	}{
		{name: "completed", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishCompletedV1, valid: true},
		{name: "pre acceptance failure", acceptance: domain.ProviderAcceptancePreAcceptanceV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailurePreAcceptanceV1, valid: true},
		{name: "accepted provider failure", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureProviderFailedV1, valid: true},
		{name: "accepted unknown", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishUnknownV1, failure: domain.ProviderExecutionFailureAcceptedUnknownV1, valid: true},
		{name: "cancelled", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishCancelledV1, failure: domain.ProviderExecutionFailureCancelledV1, valid: true},
		{name: "accepted with pre acceptance code", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailurePreAcceptanceV1},
		{name: "pre acceptance accepted unknown", acceptance: domain.ProviderAcceptancePreAcceptanceV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureAcceptedUnknownV1},
		{name: "unknown provider failure", acceptance: domain.ProviderAcceptanceUnknownV1, finish: domain.ProviderFinishFailedV1, failure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "unknown cancelled with provider code", acceptance: domain.ProviderAcceptanceUnknownV1, finish: domain.ProviderFinishCancelledV1, failure: domain.ProviderExecutionFailureProviderFailedV1},
		{name: "completed with failure", acceptance: domain.ProviderAcceptanceAcceptedV1, finish: domain.ProviderFinishCompletedV1, failure: domain.ProviderExecutionFailureBackendV1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verdict := domain.ProviderPolicyUnknownV1
			if test.finish == domain.ProviderFinishCompletedV1 {
				verdict = domain.ProviderPolicyGoV1
			}
			value := domain.ProviderExecutionEvidenceV1{
				AcceptanceClass: test.acceptance, FinishClass: test.finish, FailureCode: test.failure,
				RouteState: domain.ProviderEvidenceUnknownV1, PolicyVerdict: verdict,
				UsageProvenance: domain.ProviderUsageUnknownV1,
			}
			_, err := value.SealForBinding(binding)
			if (err == nil) != test.valid {
				t.Fatalf("SealForBinding() error = %v, valid=%v", err, test.valid)
			}
		})
	}
}
