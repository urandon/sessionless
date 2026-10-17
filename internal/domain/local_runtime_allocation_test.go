package domain_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Canonical synthetic DATA only: no source authentication, OS observation,
// connector, capability issuer, provider, or model is exercised by this fixture.
func localRuntimeAllocationFixture(t testing.TB, bearer bool) registeredFixture {
	t.Helper()
	g := localInstallationGrantFixture(t)
	p := localSpendingProfileFixture(t, bearer)
	kinds := [4]domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range kinds {
		spending[i] = localSpendingFixture(t, p, kind)
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, SessionID: "local-session", RunID: "local-run", AttemptID: "local-attempt"}
	at := p.Window.PublishedAt
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal(err)
	}
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease:                 domain.Lease{ID: "local-runtime-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "local-runtime-worker", FenceToken: 3, AcquiredAt: at, ExpiresAt: at.Add(5 * time.Minute)},
		ContextManifestDigest: strings.Repeat("1", 64), InputManifestDigest: strings.Repeat("2", 64), InvocationDeadline: at.Add(2 * time.Minute)}
	parent, err := domain.BuildAttemptEffectReservationV1(a, "local-runtime-claim", nil, at)
	if err != nil {
		t.Fatal(err)
	}
	sd, _ := pkg.Substrate.Digest()
	prepared := domain.PreparedAllocationV2{Version: domain.PreparedAllocationVersionV2, SubstrateBindingDigest: sd,
		ObservedOuterHarnessDigest: p.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: p.Substrate.EgressProxyArtifactDigest,
		ObservedProxyIdentityDigest: p.Substrate.EgressProxyIdentityDigest, WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1,
		LocalRuntime: &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, InstallationID: p.Substrate.LocalInstallationID,
			InstallationRevision: p.Substrate.LocalInstallationRevision, WorkerExecutableDigest: p.Substrate.OuterHarnessArtifactDigest,
			HostIdentityDigest: p.Substrate.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: p.Substrate.LocalNetworkNamespaceIdentityDigest, Limits: g.LocalInstallation.Profile.Limits}}
	if err := prepared.ValidateForBinding(pkg.Substrate, pkg.Binding.Backend); err != nil {
		t.Fatalf("synthetic local preparation: %v", err)
	}
	// A structural digest token is NOT a real prepared capability or producer.
	pd := domain.PreparedInvocationDigestV1(strings.Repeat("8", 64))
	expires := at.Add(time.Minute)
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at.Add(time.Microsecond), ExpiresAt: at.Add(30 * time.Second)}
	allocation, err := domain.BuildRegisteredAllocationV2(a, parent, prepared, pd, expires, pkg.Source, "local-runtime-allocation", 1, window)
	if err != nil {
		t.Fatalf("synthetic local registered allocation: %v", err)
	}
	return registeredFixture{authority: a, parent: parent, prepared: prepared, preparedDigest: pd, preparedExpiresAt: expires, source: pkg.Source, allocation: allocation, at: at}
}

func TestLocalRuntimeAllocationCanonicalCorrelationAndCodec(t *testing.T) {
	t.Parallel()
	for _, bearer := range []bool{false, true} {
		name := "none"
		if bearer {
			name = "bearer"
		}
		t.Run(name, func(t *testing.T) {
			f := localRuntimeAllocationFixture(t, bearer)
			if err := f.allocation.ValidateForAuthority(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source); err != nil {
				t.Fatalf("exact local original authority: %v", err)
			}
			if err := f.allocation.ValidateAt(f.allocation.Window.PublishedAt); err != nil {
				t.Fatal(err)
			}
			if f.allocation.ValidateAt(f.allocation.Window.ExpiresAt) == nil {
				t.Error("exclusive original expiry accepted")
			}
			raw, err := domain.EncodeRegisteredAllocationV2(f.allocation)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := domain.DecodeRegisteredAllocationV2(raw)
			if err != nil || !reflect.DeepEqual(decoded, f.allocation) {
				t.Fatalf("canonical local allocation round trip: %v", err)
			}
			old := bytes.Replace(raw, []byte(`"version":2`), []byte(`"version":1`), 1)
			if _, err := domain.DecodeRegisteredAllocationV2(old); err == nil {
				t.Error("old allocation version accepted by V2 decoder")
			}
			unknown := bytes.Replace(raw, []byte(`"local_runtime":{`), []byte(`"local_runtime":{"legacy_identity":"x",`), 1)
			if _, err := domain.DecodeRegisteredAllocationV2(unknown); err == nil {
				t.Error("unknown local runtime field accepted")
			}
			clone := f.allocation.Clone()
			clone.LocalRuntime.Limits.JSONMembers--
			if f.allocation.LocalRuntime.Limits.JSONMembers == clone.LocalRuntime.Limits.JSONMembers {
				t.Error("registered clone aliases original runtime")
			}
			if clone.Validate() == nil {
				t.Error("changed retained runtime did not invalidate its original prepared digest")
			}
			retired := f.allocation.Clone()
			retired.State, retired.RevocationGeneration, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, retired.UpdatedAt.Add(time.Microsecond)
			if err := retired.ValidateTransitionFrom(f.allocation); err != nil {
				t.Fatalf("exact original local retirement: %v", err)
			}
			retired.Window.ExpiresAt = retired.Window.ExpiresAt.Add(time.Microsecond)
			if retired.ValidateTransitionFrom(f.allocation) == nil {
				t.Error("local retirement rewrote original authority window")
			}
			preparedClone := f.prepared.Clone()
			preparedClone.LocalRuntime.InstallationRevision++
			if f.prepared.LocalRuntime.InstallationRevision == preparedClone.LocalRuntime.InstallationRevision {
				t.Error("prepared clone aliases original runtime")
			}
			for _, service := range []domain.RegisteredProxyServiceKindV1{domain.RegisteredProxyProviderV1, domain.RegisteredProxyMCPToolV1, domain.RegisteredProxyWebSearchV1} {
				hash := strings.Repeat("a", 64)
				body := domain.OwnedAgentAuthorityBodyRefV1{Ref: "structural-body", Digest: hash, Bytes: 1}
				if _, err := domain.BuildRegisteredPinnedHTTPSProxyAttestationV2(f.allocation, f.source, service, hash, hash, hash, body, body, f.allocation.Window, 1, domain.AttemptOperationDigestV1(hash)); err == nil {
					t.Errorf("local allocation fabricated an HTTPS %s observation", service)
				}
			}
		})
	}
}

func TestLocalRuntimeAllocationRejectsMissingMixedAndMismatchedFacts(t *testing.T) {
	t.Parallel()
	f := localRuntimeAllocationFixture(t, false)
	for name, mutate := range map[string]func(*domain.PreparedAllocationV2){
		"old-version":     func(v *domain.PreparedAllocationV2) { v.Version = 1 },
		"unknown-version": func(v *domain.PreparedAllocationV2) { v.Version = 3 },
		"missing-local":   func(v *domain.PreparedAllocationV2) { v.LocalRuntime = nil },
		"mixed-image":     func(v *domain.PreparedAllocationV2) { v.ObservedImageDigest = strings.Repeat("a", 64) },
		"mixed-child":     func(v *domain.PreparedAllocationV2) { v.ChildProcess = &domain.ChildProcessAttestationV1{} },
		"mixed-in-process": func(v *domain.PreparedAllocationV2) {
			v.InProcess = &domain.InProcessAttestationV1{LinkedBackendProfileDigest: f.authority.HarnessBinding.Backend.BackendProfileDigest}
		},
		"profile":      func(v *domain.PreparedAllocationV2) { v.LocalRuntime.ConfiguredProfileDigest = strings.Repeat("e", 64) },
		"installation": func(v *domain.PreparedAllocationV2) { v.LocalRuntime.InstallationID = "other-installation" },
		"revision":     func(v *domain.PreparedAllocationV2) { v.LocalRuntime.InstallationRevision++ },
		"worker":       func(v *domain.PreparedAllocationV2) { v.LocalRuntime.WorkerExecutableDigest = strings.Repeat("e", 64) },
		"host":         func(v *domain.PreparedAllocationV2) { v.LocalRuntime.HostIdentityDigest = strings.Repeat("e", 64) },
		"network": func(v *domain.PreparedAllocationV2) {
			v.LocalRuntime.NetworkNamespaceIdentityDigest = strings.Repeat("e", 64)
		},
		"outer":          func(v *domain.PreparedAllocationV2) { v.ObservedOuterHarnessDigest = strings.Repeat("e", 64) },
		"proxy-artifact": func(v *domain.PreparedAllocationV2) { v.ObservedProxyArtifactDigest = strings.Repeat("e", 64) },
		"proxy-identity": func(v *domain.PreparedAllocationV2) { v.ObservedProxyIdentityDigest = strings.Repeat("e", 64) },
		"workload":       func(v *domain.PreparedAllocationV2) { v.WorkloadMode = domain.SubstrateWorkloadChildProcessV1 },
		"invalid-limits": func(v *domain.PreparedAllocationV2) { v.LocalRuntime.Limits.ResponseHeaderBytes = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			v := f.prepared.Clone()
			mutate(&v)
			if v.ValidateForBinding(f.authority.SubstrateBinding, f.authority.HarnessBinding.Backend) == nil {
				t.Error("missing/mixed/mismatched local preparation accepted")
			}
			if _, err := v.DigestForBinding(f.authority.SubstrateBinding, f.authority.HarnessBinding.Backend); err == nil {
				t.Error("invalid local preparation received a digest")
			}
		})
	}
	for _, child := range []bool{false, true} {
		image := registeredAllocationFixture(t, child)
		if f.prepared.ValidateForBinding(f.authority.SubstrateBinding, image.authority.HarnessBinding.Backend) == nil {
			t.Errorf("local runtime accepted a valid non-configured backend: child=%t", child)
		}
		mixed := image.prepared.Clone()
		mixed.LocalRuntime = f.prepared.LocalRuntime
		if mixed.ValidateForBinding(image.authority.SubstrateBinding, image.authority.HarnessBinding.Backend) == nil {
			t.Errorf("image allocation accepted local runtime: child=%t", child)
		}
		registered := image.allocation.Clone()
		registered.LocalRuntime = f.prepared.LocalRuntime
		if registered.Validate() == nil {
			t.Errorf("registered image allocation accepted local runtime: child=%t", child)
		}
	}
}

func TestLocalRuntimeAllocationConfiguredLimitsFitOwnedRawIOAndTime(t *testing.T) {
	t.Parallel()
	f := localRuntimeAllocationFixture(t, false)
	local, backend := *f.prepared.LocalRuntime, f.authority.HarnessBinding.Backend
	for _, name := range []string{"memory", "execution", "cleanup"} {
		t.Run(name, func(t *testing.T) {
			binding := f.authority.SubstrateBinding
			switch name {
			case "memory":
				binding.Limits.MemoryBytes = local.Limits.RequestBytes + local.Limits.ResponseBytes + local.Limits.ResponseHeaderBytes - 1
			case "execution":
				binding.Limits.ExecutionTimeout = local.Limits.OperationTimeout - time.Nanosecond
			case "cleanup":
				binding.Limits.CleanupTimeout = local.Limits.CleanupTimeout - time.Nanosecond
			}
			if err := binding.Validate(); err != nil {
				t.Fatalf("limit intersection control must remain valid: %v", err)
			}
			if local.ValidateForBinding(binding, backend) == nil {
				t.Error("configured local limits exceeded exact substrate limits")
			}
		})
	}
	// Exact raw IO fit is allowed; it does not attest total process/model RSS.
	binding := f.authority.SubstrateBinding
	binding.Limits.MemoryBytes = local.Limits.RequestBytes + local.Limits.ResponseBytes + local.Limits.ResponseHeaderBytes
	if err := local.ValidateForBinding(binding, backend); err != nil {
		t.Fatalf("exact raw IO fit: %v", err)
	}
}

func TestLocalRuntimeAllocationActiveWindowDoesNotRenewSetup(t *testing.T) {
	t.Parallel()
	f := localRuntimeAllocationFixture(t, false)
	deadline, err := domain.LocalAllocationActiveDeadlineV2(f.authority, f.parent, f.source)
	if err != nil {
		t.Fatal(err)
	}
	want := f.parent.ReservedAt.Add(f.authority.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration)
	if !deadline.Equal(want) || !deadline.After(f.preparedExpiresAt) {
		t.Fatalf("active deadline got=%s want original parent+active=%s after setup=%s", deadline, want, f.preparedExpiresAt)
	}
	w := f.allocation.Window
	w.ExpiresAt = deadline
	allocation, err := domain.BuildRegisteredAllocationV2(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source, f.allocation.AllocationID, f.allocation.AllocationGeneration, w)
	if err != nil {
		t.Fatalf("local lifetime beyond setup within original active deadline: %v", err)
	}
	if !allocation.PreparedInvocationExpiresAt.Equal(f.preparedExpiresAt) || allocation.PreparedInvocationDigest != f.preparedDigest {
		t.Fatal("longer owned allocation rewrote original prepared capability/first-entry expiry")
	}
	if err := allocation.ValidateAt(f.preparedExpiresAt); err != nil {
		t.Fatalf("retained allocation was truncated at setup expiry: %v", err)
	}
	if err := allocation.ValidateForAuthority(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source); err != nil {
		t.Fatalf("original full-authority reproduction after lifetime separation: %v", err)
	}
	for _, name := range []string{"extended-active", "late-first-publication", "at-setup-first-publication"} {
		t.Run(name, func(t *testing.T) {
			changed := w
			if name == "extended-active" {
				changed.ExpiresAt = deadline.Add(time.Microsecond)
			} else {
				changed.PublishedAt = f.preparedExpiresAt
				if name == "late-first-publication" {
					changed.PublishedAt = changed.PublishedAt.Add(time.Microsecond)
				}
			}
			if _, err := domain.BuildRegisteredAllocationV2(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source, allocation.AllocationID, allocation.AllocationGeneration, changed); err == nil {
				t.Error("late registration or widened active lifetime accepted")
			}
		})
	}
	// Delayed registration cannot obtain an extra active interval: only the
	// original parent timestamp participates in the derived absolute deadline.
	later := allocation.Clone()
	later.Window.PublishedAt = f.preparedExpiresAt.Add(-time.Microsecond)
	later.UpdatedAt = later.Window.PublishedAt
	if err := later.ValidateForAuthority(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source); err != nil || !later.Window.ExpiresAt.Equal(deadline) {
		t.Fatalf("contained delayed publication altered original absolute deadline: %v", err)
	}
	for _, child := range []bool{false, true} {
		image := registeredAllocationFixture(t, child)
		if _, err := domain.LocalAllocationActiveDeadlineV2(image.authority, image.parent, image.source); err == nil {
			t.Error("local deadline helper accepted an image allocation")
		}
		imageWindow := image.allocation.Window
		imageWindow.ExpiresAt = image.preparedExpiresAt.Add(time.Microsecond)
		if _, err := domain.BuildRegisteredAllocationV2(image.authority, image.parent, image.prepared, image.preparedDigest, image.preparedExpiresAt, image.source, image.allocation.AllocationID, image.allocation.AllocationGeneration, imageWindow); err == nil {
			t.Errorf("image allocation lost its original setup cap: child=%t", child)
		}
	}
}

func TestLocalRuntimeAllocationActiveDeadlineRespectsEveryOriginalComponent(t *testing.T) {
	t.Parallel()
	f := localRuntimeAllocationFixture(t, false)
	for i := range f.source.Spending {
		t.Run(string(f.source.Spending[i].Locator.ComponentKind), func(t *testing.T) {
			source := f.source.Clone()
			shorter := f.at.Add(90 * time.Second)
			source.ExpiresAt = shorter
			source.Spending[i].Window.ExpiresAt = shorter
			// Rebind synthetic DATA only; this does not authenticate or amend an
			// actual invoice/source. All other original execution digests remain.
			bindOwnedSpendingSource(t, &source.Spending[i])
			if err := source.ValidateForExecution(f.authority.ExecutionPlacementV2, f.authority.HarnessBinding, f.authority.SubstrateBinding, f.authority.AdmissionCostCeiling); err != nil {
				t.Fatalf("shortened component control must remain valid: %v", err)
			}
			deadline, err := domain.LocalAllocationActiveDeadlineV2(f.authority, f.parent, source)
			if err != nil || !deadline.Equal(shorter) {
				t.Fatalf("component original expiry ignored: got=%s want=%s err=%v", deadline, shorter, err)
			}
		})
	}
	parent := f.parent.Clone()
	parent.InvocationAuthorityDigest = domain.ServerlessInvocationAuthorityDigestV1(strings.Repeat("e", 64))
	if _, err := domain.LocalAllocationActiveDeadlineV2(f.authority, parent, f.source); err == nil {
		t.Error("foreign original invocation authority accepted")
	}
}

func TestLocalRuntimeAllocationDigestFramesEveryConfiguredLimit(t *testing.T) {
	t.Parallel()
	f := localRuntimeAllocationFixture(t, false)
	v := f.prepared
	got, err := v.DigestForBinding(f.authority.SubstrateBinding, f.authority.HarnessBinding.Backend)
	if err != nil {
		t.Fatal(err)
	}
	var framed bytes.Buffer
	write := func(value []byte) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(value)))
		framed.Write(n[:])
		framed.Write(value)
	}
	number := func(value uint64) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], value)
		write(n[:])
	}
	framed.WriteString("sessionless.prepared-allocation.v2")
	framed.WriteByte(0)
	number(2)
	for _, field := range []string{string(v.SubstrateBindingDigest), "", v.ObservedOuterHarnessDigest, v.ObservedProxyArtifactDigest, v.ObservedProxyIdentityDigest, string(v.WorkloadMode)} {
		write([]byte(field))
	}
	number(0) // no child process
	number(0) // no image in-process attestation
	number(1) // explicit local runtime variant
	local := v.LocalRuntime
	for _, field := range []string{local.ConfiguredProfileDigest, local.InstallationID, local.WorkerExecutableDigest, local.HostIdentityDigest, local.NetworkNamespaceIdentityDigest} {
		write([]byte(field))
	}
	number(local.InstallationRevision)
	limits, err := json.Marshal(local.Limits)
	if err != nil {
		t.Fatal(err)
	}
	write(limits)
	want := sha256.Sum256(framed.Bytes())
	if string(got) != hex.EncodeToString(want[:]) {
		t.Fatalf("independent complete local frame got=%s want=%x", got, want)
	}
	// Every independently mutable configured limit is either sealed into a new
	// digest or rejected if its closed enum/bound has no other valid value.
	for i := 0; i < reflect.TypeOf(local.Limits).NumField(); i++ {
		name := reflect.TypeOf(local.Limits).Field(i).Name
		t.Run(name, func(t *testing.T) {
			changed := v.Clone()
			field := reflect.ValueOf(&changed.LocalRuntime.Limits).Elem().Field(i)
			if field.Kind() == reflect.Uint64 {
				field.SetUint(field.Uint() - 1)
			} else {
				field.SetInt(field.Int() - 1)
			}
			d, err := changed.DigestForBinding(f.authority.SubstrateBinding, f.authority.HarnessBinding.Backend)
			if err == nil && d == got {
				t.Error("changed configured limit was omitted from digest")
			}
		})
	}
}
