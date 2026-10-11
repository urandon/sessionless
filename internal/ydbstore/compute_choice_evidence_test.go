package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func computeChoiceEvidenceFixture(t *testing.T, at time.Time, pin computeChoiceAttachedPin) (domain.HarnessBindingV2, domain.ProviderEvidenceBundleV1) {
	t.Helper()
	backend := domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("a", 64), NativeProtocolVersion: "1", BackendProfileDigest: strings.Repeat("b", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: string(pin.subscriptionID), OwnerUserID: pin.ownerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	scope := domain.ProviderEvidenceScopeV1{TenantID: pin.tenantID, OwnerUserID: pin.ownerUserID, Resource: resource, Backend: backend}
	observed, expires := at.Add(-time.Minute), at.Add(time.Second)
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "catalog-v1", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: "openai", ModelID: "reviewed-model", CatalogDigest: strings.Repeat("c", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "reviewed-model", MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnknownV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: "openai", ModelID: "reviewed-model", AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "privacy-v1", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: "openai", ModelID: "reviewed-model", ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: "policy-v1", Revision: 1, DecisionOwner: "operator-review", EvidenceSource: "reviewed-source", Verdict: domain.ProviderPolicyConditionalV1, AllowedDataClasses: []domain.ProviderDataClassV1{domain.ProviderDataPrivateV1}, ObservedAt: observed, ExpiresAt: expires},
	}
	template := domain.HarnessBindingV2{Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: pin.tenantID, OwnerUserID: pin.ownerUserID, Backend: backend, Resource: resource, ModelVendorID: "openai", ModelID: "reviewed-model", InputDataClass: domain.ProviderDataPrivateV1}
	sealComputeChoiceEvidence(t, &template, &bundle)
	return template, bundle
}

func sealComputeChoiceEvidence(t *testing.T, template *domain.HarnessBindingV2, bundle *domain.ProviderEvidenceBundleV1) {
	t.Helper()
	catalog, err := bundle.Catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	route, err := bundle.Route.Digest()
	if err != nil {
		t.Fatal(err)
	}
	capability, err := bundle.Capability.Digest()
	if err != nil {
		t.Fatal(err)
	}
	privacy, err := bundle.Privacy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	price, err := bundle.Price.Digest()
	if err != nil {
		t.Fatal(err)
	}
	bundle.Policy.CapabilityEvidenceDigest, bundle.Policy.PrivacyEvidenceDigest = string(capability), string(privacy)
	bundle.Policy.PriceObservationDigest, bundle.Policy.RoutePolicyDigest = string(price), string(route)
	policy, err := bundle.Policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	template.ProviderCatalogDigest, template.ProviderRouteDigest = string(catalog), string(route)
	template.CapabilityEvidenceDigest, template.PrivacyPolicyDigest, template.EffectivePolicyDigest = string(capability), string(privacy), string(policy)
	expires := bundle.Catalog.ExpiresAt
	for _, at := range []time.Time{bundle.Route.ExpiresAt, bundle.Capability.ExpiresAt, bundle.Privacy.ExpiresAt, bundle.Price.ExpiresAt, bundle.Policy.ExpiresAt} {
		if at.Before(expires) {
			expires = at
		}
	}
	template.EvidenceExpiresAt = &expires
}

func TestComputeChoiceProviderEvidenceUnknownAndCurrentWindows(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	template, bundle := computeChoiceEvidenceFixture(t, f.source.at, f.pin)
	for _, mode := range []string{"unknown", "go private", "privacy public", "conditional", "future", "expiry boundary", "two model routes"} {
		t.Run(mode, func(t *testing.T) {
			input, evidence := template.Clone(), bundle.Clone()
			if mode != "unknown" {
				evidence.Policy.Verdict = domain.ProviderPolicyGoV1
				evidence.Route.State, evidence.Route.PolicyID, evidence.Route.Revision = domain.ProviderEvidenceSupportedV1, "route-v1", 1
				evidence.Route.Routes = []domain.ProviderRouteV1{{BackendKind: input.Backend.BackendKind, ModelVendorID: input.ModelVendorID, ModelID: input.ModelID, TransportKind: domain.ProviderTransportLocalCLIV1, TransportProvider: "openai", UpstreamProviderID: "openai", EndpointID: "exact-local-cli", BillingKind: domain.ProviderBillingSubscriptionV1, BillingAuthority: input.Resource.ResourceID}}
			}
			switch mode {
			case "privacy public":
				evidence.Privacy.AllowedDataClasses = []domain.ProviderDataClassV1{domain.ProviderDataPublicV1}
			case "conditional":
				evidence.Policy.Verdict = domain.ProviderPolicyConditionalV1
			case "future":
				evidence.Privacy.ObservedAt = f.source.at.Add(time.Nanosecond)
			case "expiry boundary":
				evidence.Privacy.ExpiresAt = f.source.at
			case "two model routes":
				second := evidence.Route.Routes[0]
				second.EndpointID = "different-local-cli"
				evidence.Route.Routes = append(evidence.Route.Routes, second)
			}
			sealComputeChoiceEvidence(t, &input, &evidence)
			pin, err := newComputeChoiceProviderEvidence(input, evidence)
			if err != nil {
				t.Fatal(err)
			}
			auth := computeChoiceAuth{scope: computechoice.Scope{TenantID: f.pin.tenantID, UserID: f.pin.ownerUserID, SessionID: f.source.session.ID, MembershipSecurityVersion: 1}, at: f.source.at}
			budget := computechoice.NewResourceBudget()
			attempt, err := budget.NewAttempt()
			if err != nil {
				t.Fatal(err)
			}
			defer attempt.Finish()
			got, err := observeComputeChoiceProviderEvidence(&computeChoiceQuery{budget: attempt}, auth, f.pin, pin)
			if err != nil {
				t.Fatal(err)
			}
			wantWindow := computeChoiceEvidenceCurrent
			if mode == "future" {
				wantWindow = computeChoiceEvidenceFuture
			}
			if mode == "expiry boundary" {
				wantWindow = computeChoiceEvidenceExpired
			}
			if got.window != wantWindow || got.privatePolicyGo != (mode == "go private" || mode == "two model routes") || got.routeObserved != (mode != "unknown" && mode != "two model routes") || got.priceState != domain.ProviderEvidenceUnknownV1 || got.contextTokens.State != domain.ProviderEvidenceUnknownV1 || got.toolCalling != domain.ProviderEvidenceUnknownV1 || budget.Snapshot().TotalBytes != uint64(len(pin.material)) {
				t.Fatalf("%s projection invented policy/route/price/context or lost byte accounting: %+v budget=%+v", mode, got, budget.Snapshot())
			}
		})
	}
}

func TestComputeChoiceProviderEvidenceConstructorAndPrivacyBounds(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	template, bundle := computeChoiceEvidenceFixture(t, f.source.at, f.pin)
	evidence, err := newComputeChoiceProviderEvidence(template, bundle)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Catalog.Models[0].ModelID = "caller-mutated"
	bundle.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	*template.EvidenceExpiresAt = f.source.at.Add(24 * time.Hour)
	if evidence.bundle.Catalog.Models[0].ModelID != "reviewed-model" || evidence.bundle.Privacy.AllowedDataClasses[0] != domain.ProviderDataPrivateV1 || !evidence.template.EvidenceExpiresAt.Equal(f.source.at.Add(time.Second)) {
		t.Fatal("constructor retained caller-owned slices/pointers")
	}
	for _, value := range []any{evidence, computeChoiceProviderSnapshot{}} {
		if _, err := json.Marshal(value); !errors.Is(err, computechoice.ErrPrivateRegistrationNotSerializable) {
			t.Fatalf("private provider material serialized: %v", err)
		}
	}
	for _, mode := range []string{"run", "placement", "public context", "mixed digest", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			input, bundle := evidence.template.Clone(), evidence.bundle.Clone()
			switch mode {
			case "run":
				input.RunID = "not-a-preview"
			case "placement":
				input.ExecutionPlacementDigest = strings.Repeat("c", 64)
			case "public context":
				input.InputDataClass = domain.ProviderDataPublicV1
			case "mixed digest":
				input.ProviderRouteDigest = strings.Repeat("d", 64)
			case "oversize":
				bundle.Catalog.Models = nil
				for i := 0; i < 100; i++ {
					bundle.Catalog.Models = append(bundle.Catalog.Models, domain.ProviderCatalogModelV1{ModelVendorID: "openai", ModelID: fmt.Sprintf("model-%03d", i), CatalogDigest: strings.Repeat("a", 64)})
				}
				bundle.Catalog.Models = append(bundle.Catalog.Models, domain.ProviderCatalogModelV1{ModelVendorID: "openai", ModelID: "reviewed-model", CatalogDigest: strings.Repeat("b", 64)})
				sealComputeChoiceEvidence(t, &input, &bundle)
			}
			if got, err := newComputeChoiceProviderEvidence(input, bundle); got != nil || !errors.Is(err, errComputeChoiceSource) {
				t.Fatalf("%s accepted malformed/unbounded preview: %v", mode, err)
			}
		})
	}
}

func TestComputeChoiceProviderEvidenceJoinedSameRatedPage(t *testing.T) {
	f, fixture, catalog := computeChoiceListFixture(t, 1)
	key := computeChoiceCatalogKey{f.cookie.ActiveTenantID, f.cookie.UserID, "choice-a"}
	template, bundle := computeChoiceEvidenceFixture(t, f.at, catalog.bindings[key])
	evidence, err := newComputeChoiceProviderEvidence(template, bundle)
	if err != nil {
		t.Fatal(err)
	}
	registration := computechoice.Registration{TenantID: key.tenant, OwnerUserID: key.owner, ChoiceID: key.choice, RegistrationRevision: 1, DisclosureRevision: strings.Repeat("d", 64)}
	catalog, err = newComputeChoiceAttachedCatalog(f.pins, catalog.cursor, []computeChoiceAttachedRegistration{{enumeration: registration, pin: catalog.bindings[key], evidence: evidence}})
	if err != nil {
		t.Fatal(err)
	}
	// Caller mutation after installation cannot alter the exact immutable input.
	evidence.bundle.Privacy.AllowedDataClasses[0] = domain.ProviderDataPublicV1
	fixture.commitModes = []string{"ambiguous"}
	fixture.afterCommit = func() {
		// Only the authoritative clock changes. The auth fixture's syncRows
		// replaces its manifest and would erase the six candidate query keys.
		f.at = f.at.Add(time.Second)
		f.driver.rows[`SELECT CurrentUtcTimestamp()`][0] = f.at
	}
	page, err := f.store.readRatedComputeChoiceAttachedPage(context.Background(), f.cookie.SessionDigest, f.session.ID, computeChoiceRateInput(), "joined-evidence", catalog)
	if err != nil || len(page.candidates) != 1 || page.candidates[0].provider.window != computeChoiceEvidenceExpired || page.candidates[0].provider.priceState != domain.ProviderEvidenceUnknownV1 || page.candidates[0].sources.state != computeChoiceAttachedMissing || fixture.writes != 1 || len(fixture.statements) != 33 {
		t.Fatalf("lostACK did not reobserve currentDB evidence independently of missing live authority: page=%+v SQL=%d writes=%d err=%v", page, len(fixture.statements), fixture.writes, err)
	}
}

func TestComputeChoiceProviderEvidenceScopeAndRetryByteCeiling(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	template, bundle := computeChoiceEvidenceFixture(t, f.source.at, f.pin)
	evidence, err := newComputeChoiceProviderEvidence(template, bundle)
	if err != nil {
		t.Fatal(err)
	}
	auth := computeChoiceAuth{scope: computechoice.Scope{TenantID: f.pin.tenantID, UserID: f.pin.ownerUserID, SessionID: f.source.session.ID, MembershipSecurityVersion: 1}, at: f.source.at}
	budget := computechoice.NewResourceBudget()
	attempt, err := budget.NewAttempt()
	if err != nil {
		t.Fatal(err)
	}
	foreign := auth
	foreign.scope.UserID = "another-writer"
	if got, err := observeComputeChoiceProviderEvidence(&computeChoiceQuery{budget: attempt}, foreign, f.pin, evidence); !errors.Is(err, errComputeChoiceSource) || got != (computeChoiceProviderSnapshot{}) || budget.Snapshot().TotalBytes != 0 {
		t.Fatalf("foreign scope reached materialized evidence: got=%+v budget=%+v err=%v", got, budget.Snapshot(), err)
	}
	charge := func(attempt *computechoice.AttemptBudget, amount uint64) {
		t.Helper()
		for amount > 0 {
			chunk := min(amount, uint64(computechoice.MaxConnectionRecordBytes))
			if err := attempt.RecordMaterialized(computechoice.SourceConnection, int64(chunk)); err != nil {
				t.Fatal(err)
			}
			amount -= chunk
		}
	}
	charge(attempt, computechoice.MaxMaterializedBytesTotal/2)
	if err := attempt.Finish(); err != nil {
		t.Fatal(err)
	}
	attempt, err = budget.NewAttempt()
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.Finish()
	charge(attempt, computechoice.MaxMaterializedBytesTotal/2-uint64(len(evidence.material))+1)
	if got, err := observeComputeChoiceProviderEvidence(&computeChoiceQuery{budget: attempt}, auth, f.pin, evidence); !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || got != (computeChoiceProviderSnapshot{}) || budget.Snapshot().TotalBytes != computechoice.MaxMaterializedBytesTotal+1 {
		t.Fatalf("static work bypassed aggregate retry byte bound: got=%+v budget=%+v err=%v", got, budget.Snapshot(), err)
	}
}
