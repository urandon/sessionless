//go:build darwin || linux

package codexexec

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is ONLY immutable metadata/proof data. No Session admission, ACK,
// credential/native call or execution grant is established by this fixture.
func nativeGenerationProjectionFixtureV3(t *testing.T, generation uint64) (*nativeFrozenProfileV3, domain.AttachedResourceGenerationBaselineV1, *attachedworkerdaemon.VerifiedAttachedInputV1, time.Time) {
	t.Helper()
	p, evidence, template, now := nativePolicyFixtureV3(t)
	request, authority := validDriverRequestAt(t, now)
	input := preparedOwnerTypedInput(t, request, authority)
	a := &input.AcceptedAttempt
	a.CreatedAt, a.UpdatedAt = now, now
	template.TenantID, template.OwnerUserID = a.TenantID, a.OwnerUserID
	template.Resource.ResourceID, template.Resource.OwnerUserID = input.Job.HarnessBinding.Resource.ResourceID, a.OwnerUserID
	for _, scope := range []*domain.ProviderEvidenceScopeV1{&evidence.Catalog.Scope, &evidence.Route.Scope, &evidence.Capability.Scope, &evidence.Privacy.Scope, &evidence.Price.Scope, &evidence.Policy.Scope} {
		scope.TenantID, scope.OwnerUserID, scope.Resource = a.TenantID, a.OwnerUserID, template.Resource
	}
	nativePolicyRebindV2(t, &evidence, &template)
	template.RunID, template.AttemptID, template.ExecutionPlacementDigest = "", "", ""
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: a.TenantID, OwnerUserID: a.OwnerUserID,
		SubscriptionConnectionID: domain.SubscriptionConnectionID(template.Resource.ResourceID), WorkerID: a.WorkerID,
		EnrollmentGeneration: a.EnrollmentGeneration, BaselineRevision: template.Resource.Revision,
		CapabilityDigest: a.CapabilityDigest, GenerationOnlyDelegation: true, Binding: template, Evidence: evidence}
	if err := baseline.Validate(); err != nil {
		t.Fatal(err)
	}
	binding, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, generation)
	if err != nil {
		t.Fatal(err)
	}
	input.Job.ExecutionPlacementV2.PolicyDigest = domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)
	digest, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = a.RunID, a.AttemptID, string(digest)
	input.Job.HarnessBinding, a.PolicyDigest = binding, input.Job.ExecutionPlacementV2.PolicyDigest
	a.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	identity := attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
		RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
	if err := input.ValidateFor(identity); err != nil {
		t.Fatalf("self-consistent metadata input: %v", err)
	}
	return p, baseline, input, now
}

func TestNativeGenerationProjectionV3PreservesOriginalAndSuccessorMetadata(t *testing.T) {
	for _, generation := range []uint64{1, 2, 1<<53 + 1} {
		t.Run(fmt.Sprint(generation), func(t *testing.T) {
			p, baseline, input, now := nativeGenerationProjectionFixtureV3(t, generation)
			originalBaseline := baseline.Clone()
			originalInput, err := input.Clone()
			if err != nil {
				t.Fatal(err)
			}
			defer originalInput.Clear()
			bundle, err := projectNativeGenerationEvidenceV3(p, baseline, input, now)
			if err != nil || bundle.ValidateForBinding(input.Job.HarnessBinding) != nil || bundle.Policy.Scope.Resource.CredentialGeneration != generation {
				t.Fatalf("generation=%d full projection err=%v", generation, err)
			}
			if !reflect.DeepEqual(baseline, originalBaseline) || !reflect.DeepEqual(input, originalInput) ||
				bundle.Policy.ObservedAt != baseline.Evidence.Policy.ObservedAt || bundle.Policy.ExpiresAt != baseline.Evidence.Policy.ExpiresAt ||
				bundle.Route.State != domain.ProviderEvidenceUnknownV1 || bundle.Capability.ToolCalling != domain.ProviderEvidenceUnsupportedV1 {
				t.Fatal("projection mutated source input or renewed/changed evidence semantics")
			}
			bundle.Catalog.Models[0].ModelID = "caller-mutated"
			bundle.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
			again, err := projectNativeGenerationEvidenceV3(p, baseline, input, now)
			if err != nil || again.Catalog.Models[0].ModelID == "caller-mutated" || !reflect.DeepEqual(baseline, originalBaseline) {
				t.Fatalf("output aliases installed baseline or another attempt: %v", err)
			}
		})
	}
}

func TestNativeGenerationProjectionV3PreservesExplicitEmptyEvidenceCollections(t *testing.T) {
	p, baseline, input, now := nativeGenerationProjectionFixtureV3(t, 2)
	baseline.Evidence.Route.Routes = []domain.ProviderRouteV1{}
	nativePolicyRebindV2(t, &baseline.Evidence, &baseline.Binding)
	binding, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, 2)
	if err != nil {
		t.Fatal(err)
	}
	input.Job.ExecutionPlacementV2.PolicyDigest = domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)
	input.AcceptedAttempt.PolicyDigest = input.Job.ExecutionPlacementV2.PolicyDigest
	digest, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = input.Job.RunID, input.Job.AttemptID, string(digest)
	input.Job.HarnessBinding = binding
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := projectNativeGenerationEvidenceV3(p, baseline, input, now)
	if err != nil || bundle.Route.Routes == nil || len(bundle.Route.Routes) != 0 || bundle.ValidateForBinding(binding) != nil {
		t.Fatalf("explicit empty route collection normalized or digest changed: %v", err)
	}
}

func TestNativeGenerationProjectionV3DeniesBaselineScopeAndValiditySubstitution(t *testing.T) {
	for _, mode := range []string{"nil input", "nil profile", "worker", "enrollment", "capability", "delegation", "rehashed body", "expired evidence", "future observation", "expired lease", "expired original runtime", "zero now"} {
		t.Run(mode, func(t *testing.T) {
			p, baseline, input, now := nativeGenerationProjectionFixtureV3(t, 2)
			if _, err := projectNativeGenerationEvidenceV3(p, baseline, input, now); err != nil {
				t.Fatalf("positive control: %v", err)
			}
			switch mode {
			case "nil input":
				input = nil
			case "nil profile":
				p = nil
			case "worker":
				baseline.WorkerID = "foreign-worker"
			case "enrollment":
				baseline.EnrollmentGeneration++
			case "capability":
				baseline.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("f", 64))
			case "delegation":
				baseline.GenerationOnlyDelegation = false
			case "rehashed body":
				baseline.Evidence.Catalog.CatalogRevision = "other-approved-looking-body"
				nativePolicyRebindV2(t, &baseline.Evidence, &baseline.Binding)
				if err := baseline.Validate(); err != nil {
					t.Fatalf("rehash must stay structurally valid: %v", err)
				}
			case "expired evidence":
				now = *baseline.Binding.EvidenceExpiresAt
				input.AcceptedAttempt.LeaseExpiresAt = now.Add(time.Minute)
				input.Job.Limits.MaxRuntime = 2 * time.Hour
			case "future observation":
				baseline.Evidence.Policy.ObservedAt = now.Add(time.Microsecond)
				nativePolicyRebindV2(t, &baseline.Evidence, &baseline.Binding)
				binding, _, err := domain.ProjectAttachedResourceGenerationV1(baseline, input.Job.HarnessBinding.Resource.CredentialGeneration)
				if err != nil {
					t.Fatal(err)
				}
				input.Job.ExecutionPlacementV2.PolicyDigest = domain.AttachedWorkerPolicyDigest(binding.EffectivePolicyDigest)
				input.AcceptedAttempt.PolicyDigest = input.Job.ExecutionPlacementV2.PolicyDigest
				digest, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
				if err != nil {
					t.Fatal(err)
				}
				binding.RunID, binding.AttemptID, binding.ExecutionPlacementDigest = input.Job.RunID, input.Job.AttemptID, string(digest)
				input.Job.HarnessBinding = binding
			case "expired lease":
				// Isolate this fence from the later runtime/evidence deadlines.
				input.AcceptedAttempt.LeaseExpiresAt = input.AcceptedAttempt.CreatedAt.Add(30 * time.Second)
				if _, err := projectNativeGenerationEvidenceV3(p, baseline, input, now); err != nil {
					t.Fatalf("shorter-lease positive control: %v", err)
				}
				now = input.AcceptedAttempt.LeaseExpiresAt
			case "expired original runtime":
				now = input.AcceptedAttempt.CreatedAt.Add(input.Job.Limits.MaxRuntime)
			case "zero now":
				now = time.Time{}
			}
			if input != nil {
				var err error
				input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
				if err != nil {
					t.Fatal(err)
				}
			}
			bundle, err := projectNativeGenerationEvidenceV3(p, baseline, input, now)
			if err == nil || !reflect.DeepEqual(bundle, domain.ProviderEvidenceBundleV1{}) {
				t.Fatalf("mode=%s err=%v nonzero=%t", mode, err, !reflect.DeepEqual(bundle, domain.ProviderEvidenceBundleV1{}))
			}
		})
	}
}

func TestNativeGenerationProjectionV3RejectsSelfConsistentSealedPolicyRoleOrExpiryChanges(t *testing.T) {
	for _, mode := range []string{"static as dynamic", "foreign catalog digest", "renewed expiry", "older expiry", "other revision"} {
		t.Run(mode, func(t *testing.T) {
			p, baseline, input, now := nativeGenerationProjectionFixtureV3(t, 2)
			if _, err := projectNativeGenerationEvidenceV3(p, baseline, input, now); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "static as dynamic":
				static, err := p.profile.InstallationPolicyDigestV3()
				if err != nil {
					t.Fatal(err)
				}
				input.Job.HarnessBinding.EffectivePolicyDigest = string(static)
				input.Job.ExecutionPlacementV2.PolicyDigest, input.AcceptedAttempt.PolicyDigest = static, static
				placementDigest, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
				if err != nil {
					t.Fatal(err)
				}
				input.Job.HarnessBinding.ExecutionPlacementDigest = string(placementDigest)
			case "foreign catalog digest":
				input.Job.HarnessBinding.ProviderCatalogDigest = strings.Repeat("a", 64)
			case "renewed expiry":
				expiry := input.Job.HarnessBinding.EvidenceExpiresAt.Add(time.Microsecond)
				input.Job.HarnessBinding.EvidenceExpiresAt = &expiry
			case "older expiry":
				expiry := input.Job.HarnessBinding.EvidenceExpiresAt.Add(-time.Microsecond)
				input.Job.HarnessBinding.EvidenceExpiresAt = &expiry
			case "other revision":
				input.Job.HarnessBinding.Resource.Revision++
			}
			var err error
			input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			a := input.AcceptedAttempt
			identity := attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID,
				RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}
			if err := input.ValidateFor(identity); err != nil {
				t.Fatalf("negative must be self-consistent sealed metadata before projection: %v", err)
			}
			bundle, err := projectNativeGenerationEvidenceV3(p, baseline, input, now)
			if err == nil || !reflect.DeepEqual(bundle, domain.ProviderEvidenceBundleV1{}) {
				t.Fatalf("mode=%s admitted full binding substitution: %v", mode, err)
			}
		})
	}
}
