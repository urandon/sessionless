package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type configOnlyAttachedStore struct{}

func (configOnlyAttachedStore) LoadAttachedWorker(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	panic("configuration must not read worker authority")
}

func (configOnlyAttachedStore) LoadAttachedWorkerConnection(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorkerConnection, bool, error) {
	panic("configuration must not read connection authority")
}

func TestWebAttachedExecutionAcceptsExplicitExactPins(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant", "owner", "run", "attempt", "resource", now)
	if err != nil {
		t.Fatal(err)
	}
	binding := managed.HarnessBinding.Clone()
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = "", "", ""
	binding.Backend.BackendKind = domain.HarnessBackendCodexExecV1
	binding.Backend.ArtifactKind = domain.HarnessArtifactExecutableV1
	binding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
	binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
	binding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "resource", OwnerUserID: "owner",
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	expires := now.Add(time.Hour)
	binding.EvidenceExpiresAt = &expires
	pins, err := json.Marshal([]sessionlessharness.AttachedResourcePin{{
		TenantID: "tenant", OwnerUserID: "owner", SubscriptionConnectionID: "resource", WorkerID: "worker", EnrollmentGeneration: 1,
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("a", 64)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest), Binding: binding,
	}})
	if err != nil {
		t.Fatal(err)
	}
	binder, err := webHarnessBinderFromEnv(func(name string) string {
		switch name {
		case "WEB_ATTACHED_EXECUTION_ENABLED":
			return "true"
		case "WEB_ATTACHED_RESOURCE_PINS":
			return string(pins)
		}
		return ""
	}, configOnlyAttachedStore{})
	if err != nil {
		t.Fatal(err)
	}
	if _, attached := binder.(*sessionlessharness.AttachedResourceBinder); !attached {
		t.Fatalf("enabled exact pins produced %T", binder)
	}
}

func TestWebAttachedExecutionDefaultOff(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"", "false"} {
		t.Run("flag_"+flag, func(t *testing.T) {
			binder, err := webHarnessBinderFromEnv(func(name string) string {
				if name == "WEB_ATTACHED_EXECUTION_ENABLED" {
					return flag
				}
				return ""
			}, nil)
			if err != nil || binder == nil {
				t.Fatalf("disabled config: binder=%v error=%v", binder, err)
			}
			if _, attached := binder.(*sessionlessharness.AttachedResourceBinder); attached {
				t.Fatal("default-off configuration enabled attached execution")
			}
		})
	}
}

func TestWebAttachedExecutionRejectsInvalidConfigWithoutFallback(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, flag, pins string
	}{
		{name: "typo", flag: "TRUE"},
		{name: "padded", flag: "true "},
		{name: "implicit_pin", pins: "[]"},
		{name: "disabled_pin", flag: "false", pins: "[]"},
		{name: "missing_pin", flag: "true"},
		{name: "empty", flag: "true", pins: "[]"},
		{name: "malformed", flag: "true", pins: "{"},
		{name: "unknown", flag: "true", pins: `[{"secret_from_browser":"must-not-be-logged"}]`},
		{name: "trailing", flag: "true", pins: "[] []"},
		{name: "oversized", flag: "true", pins: strings.Repeat("x", maxWebAttachedPinConfigBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binder, err := webHarnessBinderFromEnv(func(name string) string {
				if name == "WEB_ATTACHED_EXECUTION_ENABLED" {
					return tc.flag
				}
				if name == "WEB_ATTACHED_RESOURCE_PINS" {
					return tc.pins
				}
				return ""
			}, nil)
			if err == nil || binder != nil {
				t.Fatalf("invalid config %s fell back: binder=%v error=%v", tc.name, binder, err)
			}
			if strings.Contains(err.Error(), "must-not-be-logged") {
				t.Fatal("configuration error disclosed raw input")
			}
		})
	}
}
