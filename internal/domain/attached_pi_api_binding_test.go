package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// Structural local fixtures never supply a real decision, tariff or runtime.
func TestAttachedPiAPIBindingCarrierSealsOriginalContext(t *testing.T) {
	t.Parallel()
	p := piSDKFixtureProfile(t)
	b, pl := piSDKFixtureBinding(t, p)
	pBytes, err := p.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	bDigest, err := b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	job := WorkerJob{TenantID: b.TenantID, RunID: b.RunID, SessionID: "local-fixture-session", TriggerEventID: "local-fixture-event", AttemptID: b.AttemptID,
		ReservationID: "local-fixture-reservation", InputManifestID: "local-fixture-manifest", CredentialOwnerUserID: b.OwnerUserID, ExecutionPlacementV2: pl, HarnessBinding: b,
		ContextSnapshot: BlobRef{TenantID: b.TenantID, Key: "tenants/" + string(b.TenantID) + "/context", Size: 1, SHA256: strings.Repeat("a", 64)},
		Limits: ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10, MaxInputBytes: 1 << 20,
			MaxContextBytes: 1 << 20, MaxContextEvents: 100, MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}}
	manifest := ArtifactManifest{ID: job.InputManifestID, TenantID: job.TenantID, RunID: job.RunID, CreatedAt: time.Unix(10, 0).UTC()}
	contextDigest, err := AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	changed := b.Clone()
	changed.AttachedPiAPIInvocationPolicyDigest = AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("c", 64))
	changedDigest, err := changed.Digest()
	if err != nil || changedDigest == bDigest {
		t.Fatalf("M mutation must change original binding digest: err=%v equal=%v", err, changedDigest == bDigest)
	}
	job.HarnessBinding = changed
	changedContext, err := AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil || changedContext == contextDigest {
		t.Fatalf("M mutation must change original context seal: err=%v equal=%v", err, changedContext == contextDigest)
	}
	if changed.Backend != b.Backend || changed.EffectivePolicyDigest != b.EffectivePolicyDigest {
		t.Fatal("M must not feed back into P or E")
	}
	pAfter, err := p.CanonicalBytes()
	if err != nil || !bytes.Equal(pBytes, pAfter) {
		t.Fatalf("native P representation changed: %v", err)
	}
	wire, err := json.Marshal(changed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire, []byte(`"attached_pi_api_invocation_policy_digest":"`+string(changed.AttachedPiAPIInvocationPolicyDigest)+`"`)) {
		t.Fatal("original canonical binding omitted M")
	}
	var loaded HarnessBindingV2
	if err := json.Unmarshal(wire, &loaded); err != nil || loaded.ValidateForScope(b.TenantID, b.OwnerUserID, b.RunID, b.AttemptID, pl) != nil ||
		loaded.AttachedPiAPIInvocationPolicyDigest != changed.AttachedPiAPIInvocationPolicyDigest {
		t.Fatalf("original binding round-trip lost M: %v", err)
	}
}

func TestAttachedPiAPIBindingCarrierRejectsWrongArms(t *testing.T) {
	t.Parallel()
	p := piSDKFixtureProfile(t)
	b, pl := piSDKFixtureBinding(t, p)
	for _, tc := range []struct {
		name   string
		change func(*HarnessBindingV2)
	}{
		{"missing", func(v *HarnessBindingV2) { v.AttachedPiAPIInvocationPolicyDigest = "" }},
		{"zero", func(v *HarnessBindingV2) {
			v.AttachedPiAPIInvocationPolicyDigest = AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("0", 64))
		}},
		{"uppercase", func(v *HarnessBindingV2) {
			v.AttachedPiAPIInvocationPolicyDigest = AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("A", 64))
		}},
		{"short", func(v *HarnessBindingV2) { v.AttachedPiAPIInvocationPolicyDigest = "abc" }},
		{"subscription", func(v *HarnessBindingV2) { v.Resource.Kind = ProviderResourceSubscriptionV1 }},
		{"router", func(v *HarnessBindingV2) { v.Resource.Kind = ProviderResourceRouterAccountV1 }},
		{"old_rpc", func(v *HarnessBindingV2) { v.Backend.NativeProtocolVersion = "pi-rpc-jsonl.v1" }},
		{"unknown_sdk", func(v *HarnessBindingV2) { v.Backend.NativeProtocolVersion = "pi-sdk-unreviewed.v1" }},
		{"environment", func(v *HarnessBindingV2) { v.Backend.CredentialDeliveryKind = ProviderCredentialDeliveryEnvironmentV1 }},
		{"direct", func(v *HarnessBindingV2) { v.Backend.CredentialDeliveryKind = ProviderCredentialDeliveryDirectV1 }},
		{"non_pi", func(v *HarnessBindingV2) { v.Backend.BackendKind = HarnessBackendCodexExecV1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := b.Clone()
			tc.change(&v)
			if v.Validate() == nil {
				t.Fatal("forbidden carrier arm accepted")
			}
			if digest, err := v.Digest(); err == nil || digest != "" {
				t.Fatalf("invalid arm produced digest: %q %v", digest, err)
			}
			if err := b.ValidateForScope(b.TenantID, b.OwnerUserID, b.RunID, b.AttemptID, pl); err != nil {
				t.Fatalf("restored valid arm denied: %v", err)
			}
		})
	}
	for _, placement := range []ExecutionPlacementV2{
		{Version: ExecutionPlacementVersionV2, Kind: ExecutionPlacementManaged, FallbackPolicy: ExecutionFallbackDenied, SubstrateBindingDigest: strings.Repeat("d", 64)},
		{Version: pl.Version, Kind: pl.Kind, FallbackPolicy: pl.FallbackPolicy, OwnerUserID: "foreign-owner", WorkerID: pl.WorkerID, CapabilityDigest: pl.CapabilityDigest, PolicyDigest: pl.PolicyDigest},
	} {
		v := b.Clone()
		digest, err := ExecutionPlacementDigest(placement)
		if err != nil {
			t.Fatal(err)
		}
		v.ExecutionPlacementDigest = string(digest)
		if v.ValidateForScope(v.TenantID, v.OwnerUserID, v.RunID, v.AttemptID, placement) == nil {
			t.Fatal("M accepted outside the same owner's attached placement")
		}
	}
}

func TestAttachedPiAPIBindingCarrierConstituentAndFixtureFences(t *testing.T) {
	t.Parallel()
	p := piSDKFixtureProfile(t)
	b, _ := piSDKFixtureBinding(t, p)
	// The public owned policy validator precedes this constituent. Exercise
	// this fence directly so a missing policy cannot mask a removed M guard.
	owned := b.Clone()
	owned.Profile = HarnessExecutionOwnedAgentV2
	var problem ValidationError
	if err := owned.validateAttachedPiAPIPolicyCarrier(); !errors.As(err, &problem) ||
		problem.Field != "harness_binding.attached_pi_api_invocation_policy_digest" {
		t.Fatalf("owned carrier constituent got=%v, want carrier-specific denial", err)
	}
	owned.AttachedPiAPIInvocationPolicyDigest = ""
	if err := owned.validateAttachedPiAPIPolicyCarrier(); err != nil {
		t.Fatalf("absent nonselected constituent must retain its old policy rules: %v", err)
	}
	fixture := b.Clone()
	fixture.Backend = HarnessBackendDescriptorV1{HarnessKind: HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: HarnessBackendDeterministicFixtureV1,
		ArtifactKind: HarnessArtifactEmbeddedProfileV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "fixture.v1", BackendProfileDigest: strings.Repeat("b", 64),
		ProviderContractKind: ProviderContractCredentiallessFixtureV1, CredentialDeliveryKind: ProviderCredentialDeliveryNoneV1}
	fixture.Resource = ProviderResourceBindingV1{Kind: ProviderResourceCredentiallessFixtureV1, ResourceID: "local-fixture-resource", OwnerUserID: b.OwnerUserID, Revision: 1, CredentialMode: ProviderCredentialNoneV1}
	fixture.EvidenceExpiresAt = nil
	fixture.AttachedPiAPIInvocationPolicyDigest = ""
	if err := fixture.Validate(); err != nil {
		t.Fatalf("genuinely valid absent fixture control: %v", err)
	}
	fixture.AttachedPiAPIInvocationPolicyDigest = b.AttachedPiAPIInvocationPolicyDigest
	if err := fixture.Validate(); !errors.As(err, &problem) || problem.Field != "harness_binding.attached_pi_api_invocation_policy_digest" {
		t.Fatalf("valid fixture plus M got=%v, want carrier-specific denial", err)
	}
}

func TestAttachedPiAPIBindingCarrierAbsentNonselectedSDKResources(t *testing.T) {
	t.Parallel()
	p := piSDKFixtureProfile(t)
	b, pl := piSDKFixtureBinding(t, p)
	for _, kind := range []ProviderResourceKindV1{ProviderResourceSubscriptionV1, ProviderResourceRouterAccountV1} {
		t.Run(string(kind), func(t *testing.T) {
			v := b.Clone()
			v.Resource.Kind = kind
			v.AttachedPiAPIInvocationPolicyDigest = ""
			if err := v.ValidateForScope(v.TenantID, v.OwnerUserID, v.RunID, v.AttemptID, pl); err != nil {
				t.Fatalf("predecessor-compatible absent arm denied: %v", err)
			}
			if digest, err := v.Digest(); err != nil || digest == "" {
				t.Fatalf("predecessor-compatible absent arm digest: %q %v", digest, err)
			}
			// Structural compatibility never makes this a selected native resource.
			if p.ValidateBindingIdentity(v, pl) == nil {
				t.Fatal("unselected resource passed genuine expected-P API identity")
			}
			v.AttachedPiAPIInvocationPolicyDigest = b.AttachedPiAPIInvocationPolicyDigest
			var problem ValidationError
			if err := v.Validate(); !errors.As(err, &problem) || problem.Field != "harness_binding.attached_pi_api_invocation_policy_digest" {
				t.Fatalf("nonselected resource plus M got=%v, want carrier-specific denial", err)
			}
		})
	}
}

func TestAttachedPiAPIBindingAbsentRouteLiteralGolden(t *testing.T) {
	t.Parallel()
	p := piSDKFixtureProfile(t)
	b, pl := piSDKFixtureBinding(t, p)
	b.Backend.NativeProtocolVersion = "pi-rpc-jsonl.v1"
	b.Backend.ArtifactDigest = strings.Repeat("a", 64)
	b.Backend.BackendProfileDigest = strings.Repeat("b", 64)
	b.AttachedPiAPIInvocationPolicyDigest = ""
	// Independently computed from a literal primitive predecessor transcript:
	// 1083 bytes, original domain-NUL + big-endian lengths/uints. Not obtained
	// from this producer or a new reference-encoder helper.
	const expected = "b60f493963f2264b5e43bae72e46600be11fdaa6b2007b27864fff9dfcc802f4"
	digest, err := b.Digest()
	if err != nil || string(digest) != expected {
		t.Fatalf("absent-route predecessor digest got=%q err=%v want=%s", digest, err, expected)
	}
	if err := b.ValidateForScope(b.TenantID, b.OwnerUserID, b.RunID, b.AttemptID, pl); err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(b)
	if err != nil || bytes.Contains(wire, []byte("attached_pi_api_invocation_policy_digest")) {
		t.Fatalf("absent ordinary wire changed: %v", err)
	}
}
