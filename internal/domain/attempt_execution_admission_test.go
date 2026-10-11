package domain_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func admissionSourcesFixture(t testing.TB) (domain.AttemptExecutionSourceScopeV1, domain.OwnedAgentProfileCurrentV1, [4]domain.AttemptSpendingAuthorizationCurrentV1, time.Time) {
	t.Helper()
	p, at := ownedCurrentProfileFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: "tenant-189", SessionID: "session-189", OwnerUserID: "owner-189", RunID: "run-189", AttemptID: "attempt-189"}
	kinds := [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1}
	var sources [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range kinds {
		sources[i] = ownedSpendingFixture(t, p, kind)
	}
	return scope, p, sources, at
}

func cloneAdmissionSources(s [4]domain.AttemptSpendingAuthorizationCurrentV1) [4]domain.AttemptSpendingAuthorizationCurrentV1 {
	for i := range s {
		s[i] = s[i].Clone()
	}
	return s
}

func mustAdmissionPackage(t testing.TB, scope domain.AttemptExecutionSourceScopeV1, p domain.OwnedAgentProfileCurrentV1, s [4]domain.AttemptSpendingAuthorizationCurrentV1, at time.Time) domain.OwnedAgentExecutionPackageV1 {
	t.Helper()
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, s, at)
	if err != nil {
		t.Fatalf("compose original admission: %v", err)
	}
	return pkg
}

func TestOwnedAdmissionComposesOriginalFourSourcesAndWindow(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	s[3].Window.ExpiresAt = at.Add(5 * time.Minute)
	bindOwnedSpendingSource(t, &s[3])
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	if pkg.Source.Scope != scope || pkg.Binding.Profile != domain.HarnessExecutionOwnedAgentV2 || pkg.Binding.EvidenceExpiresAt != nil ||
		pkg.Source.ExpiresAt != s[3].Window.ExpiresAt || pkg.Binding.AgentLoopPolicy.ExpiresAt != s[3].Window.ExpiresAt || pkg.Source.MaxTotalAmountMicrounits != 70 {
		t.Fatalf("original package lost scope/window/known allocation: %+v", pkg.Source)
	}
	if err := pkg.Source.ValidateForExecution(pkg.Placement, pkg.Binding, pkg.Substrate, pkg.Cost); err != nil {
		t.Fatal(err)
	}
	for _, now := range []time.Time{at, pkg.Source.ExpiresAt.Add(-time.Microsecond)} {
		if err := pkg.Source.ValidateAgainstCurrentSources(p, s, now); err != nil {
			t.Fatalf("legitimate current join at %s: %v", now, err)
		}
	}
	if err := pkg.Source.ValidateAgainstCurrentSources(p, s, pkg.Source.ExpiresAt); err == nil {
		t.Fatal("exclusive original expiry admitted current use")
	}
	// Returned policy, cost and all spending snapshots isolate caller storage.
	*p.CostCeiling.MCPToolCost.MaxAmountMicrounits = 999
	*s[2].MaxAmountMicrounits = 999
	p.InputModalities[0] = domain.AgentInputFileV1
	if pkg.Binding.AgentLoopPolicy.InputModalities[0] != domain.AgentInputTextV1 || *pkg.Cost.MCPToolCost.MaxAmountMicrounits != 30 || *pkg.Source.Spending[2].MaxAmountMicrounits != 30 {
		t.Fatal("source mutation changed sealed original package")
	}
	clone := pkg.Source.Clone()
	*clone.Spending[2].MaxAmountMicrounits = 999
	if *pkg.Source.Spending[2].MaxAmountMicrounits != 30 {
		t.Fatal("attachment clone aliases its funding snapshot")
	}
}

func TestOwnedAdmissionRejectsCoherentScopeOrSourceSubstitution(t *testing.T) {
	t.Parallel()
	for _, axis := range []string{"tenant", "session", "principal", "run", "attempt", "payer", "duplicate_component", "missing_component", "order", "profile_generation", "source_digest", "resource", "price", "terms", "grant", "unknown", "expired", "future", "nanos", "total_overflow"} {
		t.Run(axis, func(t *testing.T) {
			scope, p, s, at := admissionSourcesFixture(t)
			v := &s[3]
			switch axis {
			case "tenant":
				v.Locator.TenantID = "other-tenant"
				v.SourceProfile.Locator.TenantID = "other-tenant"
			case "session":
				v.SessionID = "other-session"
			case "principal":
				v.OwnerUserID = "other-owner"
				v.SourceProfile.Locator.OwnerUserID = "other-owner"
			case "run":
				v.RunID = "other-run"
			case "attempt":
				v.Locator.AttemptID = "other-attempt"
			case "payer":
				v.Locator.BillingOwnerUserID = "another-payer"
			case "duplicate_component":
				s[3] = s[2].Clone()
			case "missing_component":
				s[3] = domain.AttemptSpendingAuthorizationCurrentV1{}
			case "order":
				s[0], s[1] = s[1], s[0]
			case "profile_generation":
				v.SourceProfile.RevocationGeneration++
			case "source_digest":
				v.SourceProfile.Digest = strings.Repeat("f", 64)
			case "resource":
				v.ResourceID = "changed-resource"
			case "price":
				v.PriceRevision = "changed-price"
			case "terms":
				v.TermsEvidenceDigest = strings.Repeat("f", 64)
			case "grant":
				v.ScopedGrantDigest = strings.Repeat("f", 64)
			case "unknown":
				v.PriceState, v.MaxAmountMicrounits = domain.ExternalServicePriceUnknownV1, nil
			case "expired":
				at = v.Window.ExpiresAt
			case "future":
				at = v.Window.PublishedAt.Add(-time.Microsecond)
			case "nanos":
				at = at.Add(time.Nanosecond)
			case "total_overflow":
				*v.MaxAmountMicrounits = ^uint64(0)
			}
			if axis != "missing_component" {
				bindOwnedSpendingSource(t, v)
				if err := v.Validate(); err != nil {
					t.Fatalf("counterexample must remain structurally coherent: %v", err)
				}
			}
			got, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, s, at)
			if err == nil || !reflect.DeepEqual(got, domain.OwnedAgentExecutionPackageV1{}) {
				t.Fatalf("%s substitution obtained package: err=%v", axis, err)
			}
		})
	}
}

func TestOwnedAdmissionSmallerKnownAllocationNeverInheritsLargerCeiling(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	*s[3].MaxAmountMicrounits = 5 // source authorizes5; profile still prices40
	bindOwnedSpendingSource(t, &s[3])
	if err := s[3].ValidateAgainstProfile(p, at); err != nil {
		t.Fatalf("independently valid smaller finite source: %v", err)
	}
	if _, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, s, at); err == nil {
		t.Fatal("funding5 silently inherited profile ceiling40")
	}
	// A genuinely smaller installed package, with new matching source refs,
	// is legitimate; an existing Attempt's frozen attachment is not replaced.
	*p.CostCeiling.WebSearchCost.MaxAmountMicrounits = 5
	*p.CostCeiling.MaxTotalAmountMicrounits = 35
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	for i := range s {
		s[i] = ownedSpendingFixture(t, p, s[i].Locator.ComponentKind)
	}
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	if *pkg.Cost.WebSearchCost.MaxAmountMicrounits != 5 || pkg.Source.MaxTotalAmountMicrounits != 35 {
		t.Fatal("legitimate smaller source package lost its exact ceilings")
	}
}

func TestOwnedAdmissionHistoricalBindingIsNotCurrentPermission(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	for _, kind := range []string{"profile_revoked", "profile_changed", "spending_revoked", "spending_absent", "spending_payer_changed", "spending_window_extended"} {
		t.Run(kind, func(t *testing.T) {
			cp, cs := p.Clone(), cloneAdmissionSources(s)
			switch kind {
			case "profile_revoked":
				cp.State, cp.RevocationGeneration = domain.OwnedAgentCurrentRevokedV1, 2
			case "profile_changed":
				cp.InstructionVersion = "new-instructions.v2"
			case "spending_revoked":
				cs[3].State, cs[3].RevocationGeneration = domain.OwnedAgentCurrentRevokedV1, 2
			case "spending_absent":
				cs[3] = domain.AttemptSpendingAuthorizationCurrentV1{}
			case "spending_payer_changed":
				cs[3].Locator.BillingOwnerUserID = "new-payer"
				bindOwnedSpendingSource(t, &cs[3])
			case "spending_window_extended":
				cs[3].Window.ExpiresAt = cs[3].Window.ExpiresAt.Add(time.Minute)
				bindOwnedSpendingSource(t, &cs[3])
			}
			if pkg.Source.ValidateAgainstCurrentSources(cp, cs, at) == nil {
				t.Fatalf("%s granted current permission from original historical attachment", kind)
			}
			if pkg.Source.ValidateForExecution(pkg.Placement, pkg.Binding, pkg.Substrate, pkg.Cost) != nil {
				t.Fatal("source revocation rewrote immutable historical binding")
			}
		})
	}
}

func TestOwnedAdmissionCodecAndIndependentDigestFraming(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	a := mustAdmissionPackage(t, scope, p, s, at).Source
	raw, err := domain.EncodeAttemptExecutionSourceV1(a)
	if err != nil || len(raw) > domain.OwnedAgentCurrentMaxRecordBytesV1 {
		t.Fatalf("bounded original attachment: bytes=%d err=%v", len(raw), err)
	}
	got, err := domain.DecodeAttemptExecutionSourceV1(raw)
	if err != nil || !reflect.DeepEqual(a, got) {
		t.Fatalf("exact roundtrip err=%v", err)
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
	h := sha256.New()
	h.Write([]byte("sessionless.attempt-execution-source.v1\x00"))
	h.Write(length[:])
	h.Write(raw)
	want := hex.EncodeToString(h.Sum(nil))
	digest, err := a.Digest()
	if err != nil || digest != want {
		t.Fatalf("independent digest frame=%s got=%s err=%v", want, digest, err)
	}
	const golden = "c5987c6b235e5f1246ff1d051e0ef1c05894211a3194c748ae344568c643b4e4"
	if len(raw) != 8390 || digest != golden {
		t.Fatalf("canonical source V1 producer golden changed: bytes=%d digest=%s want8390/%s", len(raw), digest, golden)
	}
	for name, changed := range map[string][]byte{
		"duplicate_version": bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"unknown_field":     append([]byte(`{"unexpected":1,`), raw[1:]...),
		"spaces":            append([]byte(" "), raw...),
		"trailing":          append(append([]byte(nil), raw...), []byte("{}")...),
		"fifth_component":   bytes.Replace(raw, []byte(`"spending":[`), []byte(`"spending":[{},`), 1),
		"null_component":    bytes.Replace(raw, []byte(`"spending":[{`), []byte(`"spending":[null,{`), 1),
		"missing_total":     bytes.Replace(raw, []byte(`"max_total_amount_microunits":70,`), nil, 1),
		"oversized":         bytes.Repeat([]byte("x"), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
	} {
		t.Run(name, func(t *testing.T) {
			v, err := domain.DecodeAttemptExecutionSourceV1(changed)
			if err == nil || !reflect.DeepEqual(v, domain.AttemptExecutionSourceV1{}) {
				t.Fatalf("%s noncanonical bytes accepted: err=%v", name, err)
			}
		})
	}
	// All exported attachment fields participate in its canonical transcript.
	value := reflect.ValueOf(a)
	for i := 0; i < value.NumField(); i++ {
		name := value.Type().Field(i).Tag.Get("json")
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || len(fields[name]) == 0 {
			t.Fatalf("field %s missing from producer bytes: %v", name, err)
		}
	}
}

func TestOwnedAdmissionEveryCompletedProjectionRemainsOriginal(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	base := mustAdmissionPackage(t, scope, p, s, at)
	for _, field := range []string{"policy", "binding", "placement", "substrate", "cost", "profile_model", "aggregate", "scope", "expiry", "currency"} {
		t.Run(field, func(t *testing.T) {
			a, b, l, substrate, cost := base.Source.Clone(), base.Binding.Clone(), base.Placement, base.Substrate, base.Cost.Clone()
			switch field {
			case "policy":
				a.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("f", 64))
			case "binding":
				a.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("f", 64))
			case "placement":
				a.ExecutionPlacementDigest = strings.Repeat("f", 64)
			case "substrate":
				a.SubstrateBindingDigest = domain.SubstrateBindingDigestV1(strings.Repeat("f", 64))
			case "cost":
				a.AdmissionCostCeilingDigest = domain.AdmissionCostCeilingDigestV2(strings.Repeat("f", 64))
			case "profile_model":
				a.Profile.Locator.ModelID = "another-model"
			case "aggregate":
				a.MaxTotalAmountMicrounits++
			case "scope":
				a.Scope.RunID = "another-run"
			case "expiry":
				a.ExpiresAt = a.ExpiresAt.Add(time.Microsecond)
			case "currency":
				a.Currency = "EUR"
			}
			if err := a.ValidateForExecution(l, b, substrate, cost); err == nil {
				t.Fatalf("changed completed %s projection accepted", field)
			}
		})
	}
}

func TestOwnedAdmissionCurrentSourcesRejectCoherentlyReboundPolicy(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	// Both binding and policy remain structurally valid and their digests are
	// coherently replaced, but the original installed instruction is unchanged.
	b := pkg.Binding.Clone()
	b.AgentLoopPolicy.InstructionVersion = "uninstalled-instructions.v2"
	a := pkg.Source.Clone()
	a.AgentLoopPolicyDigest, _ = b.AgentLoopPolicy.Digest()
	a.HarnessBindingDigest, _ = b.Digest()
	if err := a.ValidateForExecution(pkg.Placement, b, pkg.Substrate, pkg.Cost); err != nil {
		t.Fatalf("coherent but not source-derived projection: %v", err)
	}
	if a.ValidateAgainstCurrentSources(p, s, at) == nil {
		t.Fatal("current source accepted separately valid caller-authored policy")
	}
}

func TestOwnedAdmissionStructuralSumRejectsExactUint64Overflow(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	a := mustAdmissionPackage(t, scope, p, s, at).Source.Clone()
	*a.Spending[2].MaxAmountMicrounits = ^uint64(0)
	bindOwnedSpendingSource(t, &a.Spending[2])
	if err := a.Spending[2].ValidateAt(at); err != nil {
		t.Fatalf("exact max Uint64 structural source: %v", err)
	}
	a.MaxTotalAmountMicrounits = 39 // wrapped max+40, NEVER an authorized sum
	if a.Validate() == nil {
		t.Fatal("known component sum wrapped into a finite total")
	}
}

func TestOwnedAdmissionPreservesExactMaxUint64KnownAllocation(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	*p.CostCeiling.MCPToolCost.MaxAmountMicrounits = ^uint64(0)
	p.CostCeiling.WebSearchCost.PriceState = domain.ExternalServicePriceKnownFreeV1
	*p.CostCeiling.WebSearchCost.MaxAmountMicrounits = 0
	*p.CostCeiling.MaxTotalAmountMicrounits = ^uint64(0)
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	for i := range s {
		s[i] = ownedSpendingFixture(t, p, s[i].Locator.ComponentKind)
	}
	a := mustAdmissionPackage(t, scope, p, s, at).Source
	raw, err := domain.EncodeAttemptExecutionSourceV1(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := domain.DecodeAttemptExecutionSourceV1(raw)
	if err != nil || got.MaxTotalAmountMicrounits != ^uint64(0) || *got.Spending[2].MaxAmountMicrounits != ^uint64(0) {
		t.Fatalf("exact Uint64 allocation lost: err=%v", err)
	}
}

func TestOwnedAdmissionExplicitAllFreeIsNotUnknown(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	for _, c := range []*domain.ExternalServiceCostCeilingV1{p.CostCeiling.MCPToolCost, p.CostCeiling.WebSearchCost} {
		c.PriceState = domain.ExternalServicePriceKnownFreeV1
		*c.MaxAmountMicrounits = 0
	}
	*p.CostCeiling.MaxTotalAmountMicrounits = 0
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	for i := range s {
		s[i] = ownedSpendingFixture(t, p, s[i].Locator.ComponentKind)
	}
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	if pkg.Source.MaxTotalAmountMicrounits != 0 {
		t.Fatal("explicit all-free package is not zero")
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	missingZero := bytes.Replace(raw, []byte(`"max_total_amount_microunits":0,`), nil, 1)
	if _, err := domain.DecodeAttemptExecutionSourceV1(missingZero); err == nil {
		t.Fatal("absent amount was reconstructed as explicit free")
	}
}

type admissionOneShotSpy struct{ calls int }

func (s *admissionOneShotSpy) Preflight(context.Context, ports.ExecutionIdentity) error {
	s.calls++
	return nil
}
func (s *admissionOneShotSpy) Cancel(context.Context, ports.ExecutionIdentity) error {
	s.calls++
	return nil
}
func (s *admissionOneShotSpy) Execute(context.Context, ports.ExecutionRequest, ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	s.calls++
	return ports.ExecutionResult{}, nil
}

func TestOwnedAdmissionValidClosedFixtureCannotEnterOneShotRegistry(t *testing.T) {
	t.Parallel()
	scope, p, s, at := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, p, s, at)
	identity := ports.ExecutionIdentity{TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID,
		ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost}
	if err := identity.Validate(); err != nil {
		t.Fatalf("denial fixture must pass canonical shape validation: %v", err)
	}
	driver := &admissionOneShotSpy{}
	validatorCalls := 0
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return at }, sessionlessharness.Registration{
		Descriptor: p.Backend, Enabled: true, Driver: driver,
		ValidateBinding: func(domain.HarnessBindingV2) sessionlessharness.FailureCode { validatorCalls++; return "" },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, invoke := range []func() error{
		func() error { return registry.Preflight(context.Background(), identity) },
		func() error { return registry.Cancel(context.Background(), identity) },
	} {
		if err := invoke(); err == nil || !strings.Contains(err.Error(), string(sessionlessharness.FailureHarnessBackendUnsupported)) {
			t.Fatalf("valid owned fixture reached old registry: %v", err)
		}
	}
	if driver.calls != 0 || validatorCalls != 0 {
		t.Fatal("structural fixture approval reached one-shot registration")
	}
	for _, axis := range []string{"evidence", "generation", "credentials", "backend", "private"} {
		t.Run(axis, func(t *testing.T) {
			b := pkg.Binding.Clone()
			switch axis {
			case "evidence":
				b.EvidenceExpiresAt = &at
			case "generation":
				b.Resource.CredentialGeneration = 1
				b.AgentLoopPolicy.Resource = b.Resource
			case "credentials":
				b.Resource.CredentialMode = domain.ProviderCredentialInvocationV1
				b.AgentLoopPolicy.Resource = b.Resource
			case "backend":
				b.Backend.BackendKind = domain.HarnessBackendOpenCodeV1
				b.AgentLoopPolicy.Backend = b.Backend
			case "private":
				b.InputDataClass = domain.ProviderDataPrivateV1
				b.AgentLoopPolicy.InputDataClass = b.InputDataClass
			}
			if b.Validate() == nil {
				t.Fatalf("mixed fixture axes %s accepted", axis)
			}
		})
	}
}

func TestOwnedAdmissionFinalPhasePreservesOriginalIssuedPackage(t *testing.T) {
	t.Parallel()
	scope, profile, spending, issued := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, profile, spending, issued)
	original, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	admitted := issued.Add(time.Minute)
	a, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "capacity-189", admitted)
	if err != nil || a.AdmittedAt != admitted || a.Source.IssuedAt != issued ||
		pkg.Binding.AgentLoopPolicy.IssuedAt != issued || a.Source.ExpiresAt != pkg.Source.ExpiresAt {
		t.Fatalf("dispatch changed original issuance: err=%v admission=%+v", err, a)
	}
	retained, err := domain.EncodeAttemptExecutionSourceV1(a.Source)
	if err != nil || !bytes.Equal(original, retained) {
		t.Fatal("dispatch rebuilt source bytes")
	}
	if a.ValidateForExecution(pkg.Placement, pkg.Binding, pkg.Substrate, pkg.Cost) != nil ||
		a.ValidateAgainstCurrentSources(profile, spending, admitted) != nil {
		t.Fatal("legitimate later dispatch lost the original issued policy")
	}
	if a.ValidateAgainstCurrentSources(profile, spending, admitted.Add(-time.Microsecond)) == nil {
		t.Fatal("current permission predates canonical admission")
	}
	*a.Source.Spending[2].MaxAmountMicrounits = 99
	if *pkg.Source.Spending[2].MaxAmountMicrounits != 30 {
		t.Fatal("final attachment aliases original source")
	}
	for _, at := range []time.Time{issued.Add(-time.Microsecond), pkg.Source.ExpiresAt, admitted.Add(time.Nanosecond)} {
		got, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "capacity-189", at)
		if err == nil || !reflect.DeepEqual(got, domain.AttemptExecutionAdmissionV1{}) {
			t.Fatalf("invalid dispatch timestamp %v yielded attachment err=%v", at, err)
		}
	}
	if _, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "", admitted); err == nil {
		t.Fatal("missing actual reservation identity accepted")
	}
}

func TestOwnedAdmissionFinalCodecIsDistinctAndExact(t *testing.T) {
	t.Parallel()
	scope, profile, spending, issued := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, profile, spending, issued)
	a, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "capacity-189", issued.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := domain.EncodeAttemptExecutionAdmissionV1(a)
	if err != nil {
		t.Fatal(err)
	}
	got, err := domain.DecodeAttemptExecutionAdmissionV1(raw)
	if err != nil || !reflect.DeepEqual(a, got) {
		t.Fatalf("final attachment roundtrip: %v", err)
	}
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
	h := sha256.New()
	h.Write([]byte("sessionless.attempt-execution-admission.v1\x00"))
	h.Write(length[:])
	h.Write(raw)
	want := hex.EncodeToString(h.Sum(nil))
	digest, err := a.Digest()
	if err != nil || digest != want {
		t.Fatalf("final framed digest=%s want=%s err=%v", digest, want, err)
	}
	const golden = "0edc45a6c47d07497712eb6fdeaa2787dd8b4cc5e10308332293d5e99ce9b5e9"
	if len(raw) != 8489 || digest != golden {
		t.Fatalf("final V1 golden bytes=%d digest=%s", len(raw), digest)
	}
	sourceRaw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	for name, invalid := range map[string][]byte{
		"source_is_not_admission": sourceRaw,
		"missing_reservation":     bytes.Replace(raw, []byte(`"reservation_id":"capacity-189",`), nil, 1),
		"null_source":             bytes.Replace(raw, sourceRaw, []byte("null"), 1),
		"duplicate_reservation":   bytes.Replace(raw, []byte(`"reservation_id":"capacity-189"`), []byte(`"reservation_id":"capacity-189","reservation_id":"capacity-189"`), 1),
		"unknown":                 append([]byte(`{"unknown":1,`), raw[1:]...),
		"spaces":                  append([]byte(" "), raw...),
		"trailing":                append(append([]byte(nil), raw...), []byte("{}")...),
		"oversize":                bytes.Repeat([]byte("x"), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
	} {
		t.Run(name, func(t *testing.T) {
			v, err := domain.DecodeAttemptExecutionAdmissionV1(invalid)
			if err == nil || !reflect.DeepEqual(v, domain.AttemptExecutionAdmissionV1{}) {
				t.Fatalf("invalid final %s err=%v", name, err)
			}
		})
	}
	if _, err := domain.DecodeAttemptExecutionSourceV1(raw); err == nil {
		t.Fatal("final admission reinterpreted as ingress source")
	}
}

func TestOwnedAdmissionSourceMandatoryAtWriterBoundaries(t *testing.T) {
	t.Parallel()
	scope, profile, spending, issued := admissionSourcesFixture(t)
	pkg := mustAdmissionPackage(t, scope, profile, spending, issued)
	raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.HarnessBindingRequest{TenantID: scope.TenantID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID,
		RunID: scope.RunID, AttemptID: scope.AttemptID, SubscriptionConnectionID: "capacity-connection", At: issued}
	authority := ports.ManagedExecutionAuthorityV2{ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding,
		SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost, OwnedExecutionSourceRecord: string(raw)}
	if err := authority.ValidateForScope(request); err != nil {
		t.Fatalf("original source authority: %v", err)
	}
	if err := authority.ExecutionAuthority().ValidateForScope(request); err != nil {
		t.Fatal(err)
	}
	run, attempt := validRun(), validAttempt()
	run.TenantID, run.SessionID, run.ID = scope.TenantID, scope.SessionID, scope.RunID
	attempt.TenantID, attempt.RunID, attempt.ID = scope.TenantID, scope.RunID, scope.AttemptID
	blob := validBlob()
	blob.TenantID, blob.Key = scope.TenantID, "tenants/"+string(scope.TenantID)+"/context"
	dispatch := domain.DispatchOutbox{ID: "source-outbox", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID,
		InputManifestID: "input-manifest", ContextSnapshot: blob, CredentialOwnerUserID: scope.OwnerUserID,
		ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost,
		OwnedExecutionSourceRecord: string(raw), DeliveryChat: domain.TelegramChatRef{TenantID: scope.TenantID, ChatID: 11}, ReplyToMessageID: 1,
		Status: domain.DispatchPending, IdempotencyKey: "source-outbox", CreatedAt: issued, UpdatedAt: issued}
	job := domain.WorkerJob{TenantID: scope.TenantID, SessionID: scope.SessionID, RunID: scope.RunID, TriggerEventID: run.TriggerEventID,
		AttemptID: scope.AttemptID, ReservationID: "capacity-189", InputManifestID: dispatch.InputManifestID, ContextSnapshot: blob,
		CredentialOwnerUserID: scope.OwnerUserID, ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate,
		AdmissionCostCeiling: &pkg.Cost, OwnedExecutionSourceRecord: string(raw), DeliveryChat: dispatch.DeliveryChat, ReplyToMessageID: 1,
		CreatedAt: issued, Limits: attachedContextJob(t).Limits}
	if err := dispatch.ValidateForAttempt(run, attempt); err != nil {
		t.Fatalf("original dispatch: %v", err)
	}
	if err := job.ValidateForRun(run); err != nil {
		t.Fatalf("original job: %v", err)
	}
	for _, invalid := range []string{"", "null", " " + string(raw)} {
		badAuthority, badDispatch, badJob := authority, dispatch, job
		badAuthority.OwnedExecutionSourceRecord, badDispatch.OwnedExecutionSourceRecord, badJob.OwnedExecutionSourceRecord = invalid, invalid, invalid
		if badAuthority.ValidateForScope(request) == nil || badAuthority.ExecutionAuthority().ValidateForScope(request) == nil ||
			badDispatch.ValidateForAttempt(run, attempt) == nil || badJob.ValidateForRun(run) == nil {
			t.Fatalf("invalid owned source %q accepted", invalid)
		}
	}
	for _, axis := range []string{"tenant", "session", "owner", "run", "attempt"} {
		r := request
		switch axis {
		case "tenant":
			r.TenantID = "foreign-tenant"
		case "session":
			r.SessionID = "foreign-session"
		case "owner":
			r.OwnerUserID = "foreign-owner"
		case "run":
			r.RunID = "foreign-run"
		case "attempt":
			r.AttemptID = "foreign-attempt"
		}
		if authority.ValidateForScope(r) == nil {
			t.Fatalf("foreign %s source accepted", axis)
		}
	}
	foreignRun := run
	foreignRun.SessionID = "foreign-session"
	foreignJob := job
	foreignJob.SessionID = foreignRun.SessionID // coherent owning Run+Job, not a trivial pre-existing session mismatch
	if dispatch.ValidateForAttempt(foreignRun, attempt) == nil || foreignJob.ValidateForRun(foreignRun) == nil {
		t.Fatal("coherent Run/Job Session change preserved another Session's source")
	}
	legacy, err := sessionlessharness.NewDeterministicFixtureBinderV1().BindHarness(context.Background(), request)
	if err != nil || legacy.ValidateForScope(request) != nil {
		t.Fatalf("legitimate single-turn: %v", err)
	}
	legacy.OwnedExecutionSourceRecord = string(raw)
	if legacy.ValidateForScope(request) == nil {
		t.Fatal("single-turn accepted owned source bytes")
	}
}
