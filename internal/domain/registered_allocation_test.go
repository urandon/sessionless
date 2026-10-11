package domain_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type registeredFixture struct {
	authority         domain.ServerlessInvocationAuthorityV1
	parent            domain.AttemptEffectReservationV1
	prepared          domain.PreparedAllocationV1
	preparedDigest    domain.PreparedInvocationDigestV1
	preparedExpiresAt time.Time
	source            domain.AttemptExecutionSourceV1
	allocation        domain.RegisteredAllocationV1
	proxy             domain.RegisteredProxyAttestationV1
	at                time.Time
}

func registeredAllocationFixture(t testing.TB, child bool) registeredFixture {
	t.Helper()
	scope, p, spending, at := admissionSourcesFixture(t)
	if child {
		p.Substrate.WorkloadMode = domain.SubstrateWorkloadChildProcessV1
		// Rebind all four independent fixture sources after the profile changes.
		for i, s := range spending {
			spending[i] = ownedSpendingFixture(t, p, s.Locator.ComponentKind)
		}
	}
	pkg := mustAdmissionPackage(t, scope, p, spending, at)
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement,
		SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease:                 domain.Lease{ID: "registered-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "registered-worker", FenceToken: 7, AcquiredAt: at, ExpiresAt: at.Add(10 * time.Minute)},
		ContextManifestDigest: strings.Repeat("1", 64), InputManifestDigest: strings.Repeat("2", 64), InvocationDeadline: at.Add(5 * time.Minute)}
	upstream := strings.Repeat("3", 64)
	parent, err := domain.BuildAttemptEffectReservationV1(a, "registered-sole-claim", &upstream, at)
	if err != nil {
		t.Fatal(err)
	}
	sd, _ := pkg.Substrate.Digest()
	prepared := domain.PreparedAllocationV1{Version: 1, SubstrateBindingDigest: sd, ObservedImageDigest: pkg.Substrate.ImageDigest,
		ObservedOuterHarnessDigest: pkg.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: pkg.Substrate.EgressProxyArtifactDigest,
		ObservedProxyIdentityDigest: pkg.Substrate.EgressProxyIdentityDigest, WorkloadMode: pkg.Substrate.WorkloadMode}
	if child {
		prepared.ChildProcess = &domain.ChildProcessAttestationV1{ExecutableDigest: pkg.Binding.Backend.ArtifactDigest,
			ExactArgv: make([]string, 64), NativeProtocol: pkg.Binding.Backend.NativeProtocolVersion, BackendProfileDigest: pkg.Binding.Backend.BackendProfileDigest}
		for i := range prepared.ChildProcess.ExactArgv {
			prepared.ChildProcess.ExactArgv[i] = strings.Repeat("a", 4096)
		}
	} else {
		prepared.InProcess = &domain.InProcessAttestationV1{LinkedBackendProfileDigest: pkg.Binding.Backend.BackendProfileDigest}
	}
	issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return at }, bytes.NewReader(bytes.Repeat([]byte{9}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	grant, err := issuer.MintAttemptEffectOwnershipGrant(a, parent, at.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	capability, err := issuer.Issue(grant, prepared)
	if err != nil {
		t.Fatal(err)
	}
	window := domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at.Add(time.Microsecond), ExpiresAt: at.Add(time.Minute)}
	allocation, err := domain.BuildRegisteredAllocationV1(a, parent, prepared, capability.Digest(), capability.ExecuteDeadline(), pkg.Source, "actual-scripted-allocation", 11, window)
	if err != nil {
		t.Fatal(err)
	}
	policyBody := domain.OwnedAgentAuthorityBodyRefV1{Ref: "fixed-proxy-policy", Digest: strings.Repeat("4", 64), Bytes: 123}
	attestationBody := domain.OwnedAgentAuthorityBodyRefV1{Ref: "actual-scripted-proxy-observation", Digest: strings.Repeat("5", 64), Bytes: 456}
	proxy, err := domain.BuildRegisteredProxyAttestationV1(allocation, pkg.Source, domain.RegisteredProxyMCPToolV1,
		strings.Repeat("6", 64), strings.Repeat("7", 64), strings.Repeat("8", 64), policyBody, attestationBody, window)
	if err != nil {
		t.Fatal(err)
	}
	return registeredFixture{a, parent, prepared, capability.Digest(), capability.ExecuteDeadline(), pkg.Source, allocation, proxy, at}
}

func TestRegisteredPreparationExactOriginalAndFiniteWindow(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	if f.allocation.ValidateInitial() != nil || f.proxy.ValidateInitial() != nil || f.allocation.ValidateForAuthority(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source) != nil || f.proxy.ValidateForAllocation(f.allocation, f.source) != nil {
		t.Fatal("original fixture correlation rejected")
	}
	for _, now := range []time.Time{f.allocation.Window.PublishedAt, f.allocation.Window.ExpiresAt.Add(-time.Microsecond)} {
		if f.allocation.ValidateAt(now) != nil || f.proxy.ValidateAt(now) != nil {
			t.Fatalf("contained window rejected at %s", now)
		}
	}
	for _, now := range []time.Time{time.Time{}, f.at, f.allocation.Window.ExpiresAt, f.at.Add(time.Nanosecond)} {
		if f.allocation.ValidateAt(now) == nil || f.proxy.ValidateAt(now) == nil {
			t.Fatalf("invalid/future/exclusive time accepted at %s", now)
		}
	}
	clone := f.allocation.Clone()
	*clone.ParentReservation.UpstreamIdempotencyKeyDigest = strings.Repeat("f", 64)
	if *f.allocation.ParentReservation.UpstreamIdempotencyKeyDigest != strings.Repeat("3", 64) {
		t.Fatal("allocation clone aliases parent optional digest")
	}
	// Structural post-claim facts do not activate the existing one-shot registry.
	driver := &admissionOneShotSpy{}
	registry, err := sessionlessharness.NewRegistry(func() time.Time { return f.at }, sessionlessharness.Registration{Descriptor: f.authority.HarnessBinding.Backend, Enabled: true, Driver: driver, ValidateBinding: func(domain.HarnessBindingV2) sessionlessharness.FailureCode { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	identity := ports.ExecutionIdentity{TenantID: f.source.Scope.TenantID, OwnerUserID: f.source.Scope.OwnerUserID, RunID: f.source.Scope.RunID, AttemptID: f.source.Scope.AttemptID,
		ExecutionPlacementV2: f.authority.ExecutionPlacementV2, HarnessBinding: f.authority.HarnessBinding, SubstrateBinding: &f.authority.SubstrateBinding, AdmissionCostCeiling: &f.authority.AdmissionCostCeiling}
	if err := registry.Preflight(context.Background(), identity); err == nil || !strings.Contains(err.Error(), string(sessionlessharness.FailureHarnessBackendUnsupported)) {
		t.Fatalf("structural registered allocation activated one-shot preflight: %v", err)
	}
	if driver.calls != 0 {
		t.Fatal("constructor invoked serving driver")
	}
}

func TestRegisteredPreparationRejectsOriginalAuthoritySubstitution(t *testing.T) {
	t.Parallel()
	for _, axis := range []string{"claim", "fence", "worker", "invocation", "prepared", "source", "allocation_zero", "allocation_id", "before_parent", "deadline", "prepared_deadline", "source_expiry", "nanoseconds", "nonowned"} {
		t.Run(axis, func(t *testing.T) {
			f := registeredAllocationFixture(t, false)
			a, parent, prepared, source, w := f.authority.Clone(), f.parent.Clone(), f.prepared.Clone(), f.source.Clone(), f.allocation.Window
			pd, id, gen := f.preparedDigest, f.allocation.AllocationID, f.allocation.AllocationGeneration
			preparedExpires := f.preparedExpiresAt
			switch axis {
			case "claim":
				parent.PhysicalInvocationClaimID = "foreign-claim"
			case "fence":
				a.Lease.FenceToken++
			case "worker":
				a.Lease.WorkerID = "foreign-worker"
			case "invocation":
				a.ContextManifestDigest = strings.Repeat("f", 64)
			case "prepared":
				prepared.ObservedProxyIdentityDigest = strings.Repeat("f", 64)
			case "source":
				source.Scope.SessionID = "foreign-session"
			case "allocation_zero":
				gen = 0
			case "allocation_id":
				id = ""
			case "before_parent":
				w.ObservedAt = f.at.Add(-time.Microsecond)
			case "deadline":
				w.ExpiresAt = a.InvocationDeadline.Add(time.Microsecond)
			case "prepared_deadline":
				preparedExpires = w.ExpiresAt.Add(-time.Microsecond)
			case "source_expiry":
				w.ExpiresAt = source.ExpiresAt.Add(time.Microsecond)
			case "nanoseconds":
				w.PublishedAt = w.PublishedAt.Add(time.Nanosecond)
			case "nonowned":
				a.HarnessBinding.Profile = domain.HarnessExecutionSingleTurnV2
				a.HarnessBinding.AgentLoopPolicy = nil
			}
			got, err := domain.BuildRegisteredAllocationV1(a, parent, prepared, pd, preparedExpires, source, id, gen, w)
			// A different but domain-valid claim can only construct its OWN
			// observation; it cannot validate the original registered record.
			if axis == "claim" {
				if err != nil {
					t.Fatal("coherent different claim should remain structurally expressible", err)
				}
				if reflect.DeepEqual(got, f.allocation) || f.allocation.ValidateForAuthority(a, parent, prepared, pd, preparedExpires, source) == nil {
					t.Fatal("foreign claim substituted original registration")
				}
				return
			}
			if err == nil || !reflect.DeepEqual(got, domain.RegisteredAllocationV1{}) {
				t.Fatalf("axis=%s accepted partial/foreign allocation err=%v", axis, err)
			}
		})
	}
	for _, axis := range []string{"claim", "allocation_generation", "allocation_digest", "source", "grant", "proxy_identity", "service", "expired_allocation", "widen_window", "body_overflow"} {
		t.Run("proxy_"+axis, func(t *testing.T) {
			f := registeredAllocationFixture(t, false)
			v := f.proxy.Clone()
			switch axis {
			case "claim":
				v.PhysicalInvocationClaimID = "foreign-claim"
			case "allocation_generation":
				v.AllocationGeneration++
			case "allocation_digest":
				v.RegisteredAllocationDigest = domain.RegisteredAllocationDigestV1(strings.Repeat("f", 64))
			case "source":
				v.SourceDigest = strings.Repeat("f", 64)
			case "grant":
				v.GrantDigest = strings.Repeat("f", 64)
			case "proxy_identity":
				v.ProxyIdentityDigest = strings.Repeat("f", 64)
			case "service":
				v.ServiceKind = "unknown"
			case "expired_allocation":
				v.Window.PublishedAt = f.allocation.Window.ExpiresAt
				v.Window.ExpiresAt = v.Window.PublishedAt.Add(time.Microsecond)
			case "widen_window":
				v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Microsecond)
			case "body_overflow":
				v.AttestationBody.Bytes = (256 << 10) + 1
			}
			if v.ValidateForAllocation(f.allocation, f.source) == nil {
				t.Fatalf("axis=%s foreign proxy accepted", axis)
			}
		})
	}
}

func TestRegisteredPreparationMonotonicNoReplacementOrRenewal(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	retired := f.allocation.Clone()
	retired.State = domain.RegisteredPreparationRetiredV1
	retired.RevocationGeneration = 2
	retired.UpdatedAt = f.at.Add(2 * time.Minute)
	if retired.ValidateTransitionFrom(f.allocation) != nil || retired.Validate() != nil || retired.ValidateAt(f.at.Add(3*time.Microsecond)) == nil {
		t.Fatal("retirement/history semantics wrong")
	}
	revoked := retired.Clone()
	revoked.State = domain.RegisteredPreparationRevokedV1
	revoked.RevocationGeneration = 3
	if revoked.ValidateTransitionFrom(retired) != nil {
		t.Fatal("monotonic retired-to-revoked rejected")
	}
	for _, axis := range []string{"resurrect", "refresh", "replacement", "skip_generation", "overflow", "changed_prepared"} {
		t.Run(axis, func(t *testing.T) {
			old, v := f.allocation.Clone(), retired.Clone()
			switch axis {
			case "resurrect":
				old = retired
				v = f.allocation.Clone()
			case "refresh":
				v.Window.ExpiresAt = v.Window.ExpiresAt.Add(time.Microsecond)
			case "replacement":
				v.AllocationGeneration++
			case "skip_generation":
				v.RevocationGeneration = 3
			case "overflow":
				old = retired
				old.RevocationGeneration = math.MaxUint64
				v.RevocationGeneration = 0
			case "changed_prepared":
				v.PreparedInvocationDigest = domain.PreparedInvocationDigestV1(strings.Repeat("f", 64))
			}
			if v.ValidateTransitionFrom(old) == nil {
				t.Fatal("permission-expanding transition accepted")
			}
		})
	}
	p := f.proxy.Clone()
	p.State = domain.RegisteredPreparationRevokedV1
	p.RevocationGeneration = 2
	p.UpdatedAt = f.at.Add(2 * time.Minute)
	if p.ValidateTransitionFrom(f.proxy) != nil || p.ValidateAt(p.UpdatedAt) == nil {
		t.Fatal("proxy revoke/history semantics wrong")
	}
	changed := p.Clone()
	changed.AttestationBody.Digest = strings.Repeat("f", 64)
	if changed.ValidateTransitionFrom(f.proxy) == nil {
		t.Fatal("proxy revoke replaced attestation body")
	}
}

func TestRegisteredPreparationBoundedHeadNeverTruncatesPreparedArgv(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, true)
	body, err := json.Marshal(f.prepared)
	if err != nil || len(body) <= 32<<10 {
		t.Fatal("must exercise legitimate prepared body larger than head", err)
	}
	raw, err := domain.EncodeRegisteredAllocationV1(f.allocation)
	if err != nil || len(raw) >= 32<<10 || bytes.Contains(raw, []byte("exact_argv")) {
		t.Fatal("head embeds/truncates large private argv", err)
	}
	changed := f.prepared.Clone()
	changed.ChildProcess.ExactArgv[63] = strings.Repeat("b", 4096)
	if f.allocation.ValidateForAuthority(f.authority, f.parent, changed, f.preparedDigest, f.preparedExpiresAt, f.source) == nil {
		t.Fatal("last argv field not sealed by prepared digest")
	}
	decoded, err := domain.DecodeRegisteredAllocationV1(raw)
	if err != nil || decoded.PreparedAllocationDigest != f.allocation.PreparedAllocationDigest {
		t.Fatal("bounded head lost original allocation digest", err)
	}
}

func TestRegisteredPreparationStrictCanonicalCodecAndGoldens(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	for _, spec := range []struct {
		name, kind, want, codecSHA string
		codecBytes                 int
		raw                        func() ([]byte, error)
		digest                     func() (string, error)
		decode                     func([]byte) bool
	}{
		{"allocation", "sessionless.registered-allocation.v1", "99f817d3dd802eeea0903f2c4353ff1347cddf481378ba7ae621555f743a580c", "7b721adc3b2d2bc32388f35a5e887cad368ca3fcbb67febb68abd081df29795e", 6303, func() ([]byte, error) { return domain.EncodeRegisteredAllocationV1(f.allocation) }, func() (string, error) { d, e := f.allocation.Digest(); return string(d), e }, func(b []byte) bool {
			v, e := domain.DecodeRegisteredAllocationV1(b)
			if e != nil && !reflect.DeepEqual(v, domain.RegisteredAllocationV1{}) {
				t.Error("decoder returned partial allocation on error")
			}
			return e == nil && reflect.DeepEqual(v, f.allocation)
		}},
		{"proxy", "sessionless.registered-proxy-attestation.v1", "57f93d9a5839202e4c6dcb2e2901c966763daff0e5a3200261d35f3ff5363e95", "a9289e9cba1ae59c539b9ca97112081136ae46bce6dcbc4be39e6e74c3eb70f9", 1518, func() ([]byte, error) { return domain.EncodeRegisteredProxyAttestationV1(f.proxy) }, func() (string, error) { d, e := f.proxy.Digest(); return string(d), e }, func(b []byte) bool {
			v, e := domain.DecodeRegisteredProxyAttestationV1(b)
			if e != nil && !reflect.DeepEqual(v, domain.RegisteredProxyAttestationV1{}) {
				t.Error("decoder returned partial proxy on error")
			}
			return e == nil && reflect.DeepEqual(v, f.proxy)
		}},
	} {
		t.Run(spec.name, func(t *testing.T) {
			raw, err := spec.raw()
			if err != nil {
				t.Fatal(err)
			}
			if !spec.decode(raw) {
				t.Fatal("producer failed exact round trip")
			}
			got, err := spec.digest()
			if err != nil {
				t.Fatal(err)
			}
			// Independent fixed V1 frame: domain+NUL, uint64BE body length,
			// body, then SHA256. No production digest helper is called.
			h := sha256.New()
			h.Write([]byte(spec.kind))
			h.Write([]byte{0})
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
			h.Write(n[:])
			h.Write(raw)
			want := hex.EncodeToString(h.Sum(nil))
			if got != want {
				t.Fatalf("independent digest got=%s want=%s", got, want)
			}
			if got != spec.want {
				t.Errorf("literal V1 digest got=%s want=%s codec_sha256=%x bytes=%d", got, spec.want, sha256.Sum256(raw), len(raw))
			}
			codecHash := sha256.Sum256(raw)
			if hex.EncodeToString(codecHash[:]) != spec.codecSHA || len(raw) != spec.codecBytes {
				t.Errorf("literal codec bytes/hash changed: bytes=%d sha256=%x", len(raw), codecHash)
			}
			for _, mutant := range [][]byte{[]byte("null"), []byte("{}"), append([]byte(" "), raw...), append(append([]byte(nil), raw...), raw...), bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1), bytes.Replace(raw, []byte(`"version":1`), []byte(`"Version":1`), 1), bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1.0`), 1), bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"unknown":0`), 1), bytes.Repeat([]byte("x"), (32<<10)+1), bytes.Replace(raw, []byte(`"scope":`), []byte(`"scope_alias":`), 1)} {
				if spec.decode(mutant) {
					t.Fatal("malformed/noncanonical record accepted")
				}
			}
		})
	}
}

func TestRegisteredPreparationEveryFieldSealed(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	for _, spec := range []struct {
		name   string
		copy   func() any
		digest func(any) (string, error)
	}{
		{"allocation", func() any { v := f.allocation.Clone(); return &v }, func(v any) (string, error) { d, e := v.(*domain.RegisteredAllocationV1).Digest(); return string(d), e }},
		{"proxy", func() any { v := f.proxy.Clone(); return &v }, func(v any) (string, error) {
			d, e := v.(*domain.RegisteredProxyAttestationV1).Digest()
			return string(d), e
		}},
	} {
		t.Run(spec.name, func(t *testing.T) {
			base, err := spec.digest(spec.copy())
			if err != nil {
				t.Fatal(err)
			}
			var walk func(reflect.Value, []int)
			walk = func(v reflect.Value, path []int) {
				if v.Kind() == reflect.Struct && v.Type() != reflect.TypeOf(time.Time{}) {
					for i := 0; i < v.NumField(); i++ {
						walk(v.Field(i), append(append([]int(nil), path...), i))
					}
					return
				}
				if v.Kind() == reflect.Array {
					for i := 0; i < v.Len(); i++ {
						walk(v.Index(i), append(append([]int(nil), path...), i))
					}
					return
				}
				copy := spec.copy()
				leaf := reflect.ValueOf(copy).Elem()
				for _, i := range path {
					if leaf.Kind() == reflect.Array {
						leaf = leaf.Index(i)
					} else {
						leaf = leaf.Field(i)
					}
				}
				if leaf.Type() == reflect.TypeOf(time.Time{}) {
					leaf.Set(reflect.ValueOf(leaf.Interface().(time.Time).Add(time.Microsecond)))
				} else {
					switch leaf.Kind() {
					case reflect.String:
						old := leaf.String()
						if len(old) == 64 {
							c := "e"
							if old[0] == 'e' {
								c = "f"
							}
							leaf.SetString(c + old[1:])
						} else {
							leaf.SetString(old + "x")
						}
					case reflect.Uint32, reflect.Uint64:
						leaf.SetUint(leaf.Uint() + 1)
					case reflect.Int64:
						leaf.SetInt(leaf.Int() + 1)
					case reflect.Pointer:
						leaf.SetZero()
					default:
						t.Fatalf("uncovered leaf kind=%s path=%v", leaf.Kind(), path)
					}
				}
				if d, e := spec.digest(copy); e == nil && d == base {
					t.Fatalf("field path=%v was not sealed", path)
				}
			}
			walk(reflect.ValueOf(spec.copy()).Elem(), nil)
		})
	}
}

func TestRegisteredPreparationExactIntegersZonesBodiesAndProviderPolicy(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	v := f.allocation.Clone()
	v.AllocationGeneration = math.MaxUint64
	raw, err := domain.EncodeRegisteredAllocationV1(v)
	decoded, decodeErr := domain.DecodeRegisteredAllocationV1(raw)
	if err != nil || decodeErr != nil || decoded.AllocationGeneration != math.MaxUint64 || !bytes.Contains(raw, []byte(`"allocation_generation":18446744073709551615`)) {
		t.Fatal("canonical String lost exact uint64", err, decodeErr)
	}
	zone := time.FixedZone("same-instant", 3*60*60)
	z := f.allocation.Clone()
	z.Lease.AcquiredAt = z.Lease.AcquiredAt.In(zone)
	z.Lease.ExpiresAt = z.Lease.ExpiresAt.In(zone)
	z.ParentReservation.ReservedAt = z.ParentReservation.ReservedAt.In(zone)
	z.PreparedInvocationExpiresAt = z.PreparedInvocationExpiresAt.In(zone)
	z.InvocationDeadline = z.InvocationDeadline.In(zone)
	z.Substrate.ProfileEvidenceExpiresAt = z.Substrate.ProfileEvidenceExpiresAt.In(zone)
	z.SourceIssuedAt = z.SourceIssuedAt.In(zone)
	z.SourceExpiresAt = z.SourceExpiresAt.In(zone)
	z.Window.ObservedAt = z.Window.ObservedAt.In(zone)
	z.Window.PublishedAt = z.Window.PublishedAt.In(zone)
	z.Window.ExpiresAt = z.Window.ExpiresAt.In(zone)
	z.UpdatedAt = z.UpdatedAt.In(zone)
	for i := range z.Spending {
		z.Spending[i].Window.ObservedAt = z.Spending[i].Window.ObservedAt.In(zone)
		z.Spending[i].Window.PublishedAt = z.Spending[i].Window.PublishedAt.In(zone)
		z.Spending[i].Window.ExpiresAt = z.Spending[i].Window.ExpiresAt.In(zone)
	}
	original, _ := f.allocation.Digest()
	equivalent, err := z.Digest()
	if err != nil || original != equivalent {
		t.Fatal("equivalent-zone instants changed canonical digest", err)
	}
	for _, kind := range []domain.RegisteredProxyServiceKindV1{domain.RegisteredProxyProviderV1, domain.RegisteredProxyMCPToolV1, domain.RegisteredProxyWebSearchV1} {
		policyDigest := f.proxy.PolicyDigest
		if kind == domain.RegisteredProxyProviderV1 {
			policyDigest = f.allocation.Substrate.EgressPolicyDigest
		}
		body := f.proxy.PolicyBody
		body.Bytes = 256 << 10
		proxy, err := domain.BuildRegisteredProxyAttestationV1(f.allocation, f.source, kind, f.proxy.TransportDigest, policyDigest, f.proxy.ResolutionSetDigest, body, f.proxy.AttestationBody, f.proxy.Window)
		if err != nil || proxy.ValidateForAllocation(f.allocation, f.source) != nil {
			t.Fatalf("kind=%s exact grant/body bound denied: %v", kind, err)
		}
		body.Bytes++
		if _, err := domain.BuildRegisteredProxyAttestationV1(f.allocation, f.source, kind, f.proxy.TransportDigest, policyDigest, f.proxy.ResolutionSetDigest, body, f.proxy.AttestationBody, f.proxy.Window); err == nil {
			t.Fatal("oversized immutable policy body accepted")
		}
	}
	if _, err := domain.BuildRegisteredProxyAttestationV1(f.allocation, f.source, domain.RegisteredProxyProviderV1, f.proxy.TransportDigest, strings.Repeat("f", 64), f.proxy.ResolutionSetDigest, f.proxy.PolicyBody, f.proxy.AttestationBody, f.proxy.Window); err == nil {
		t.Fatal("provider proxy accepted policy different from sealed substrate")
	}
}

func TestRegisteredProxyObservationCannotPredateActualAllocation(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	allocationWindow := f.allocation.Window
	allocationWindow.ObservedAt = f.at.Add(3 * time.Microsecond)
	allocationWindow.PublishedAt = f.at.Add(4 * time.Microsecond)
	allocation, err := domain.BuildRegisteredAllocationV1(f.authority, f.parent, f.prepared, f.preparedDigest, f.preparedExpiresAt, f.source, f.allocation.AllocationID, f.allocation.AllocationGeneration, allocationWindow)
	if err != nil {
		t.Fatal("coherent later actual allocation observation", err)
	}
	for _, delta := range []time.Duration{-time.Microsecond, 0, time.Microsecond} {
		window := f.proxy.Window
		window.ObservedAt = allocation.Window.ObservedAt.Add(delta)
		window.PublishedAt = allocation.Window.PublishedAt.Add(time.Microsecond)
		if window.ObservedAt.Before(f.parent.ReservedAt) || window.Validate() != nil || allocation.ValidateAt(window.PublishedAt) != nil {
			t.Fatal("test must isolate allocation observation, not parent/publication/expiry")
		}
		proxy, err := domain.BuildRegisteredProxyAttestationV1(allocation, f.source, f.proxy.ServiceKind, f.proxy.TransportDigest, f.proxy.PolicyDigest, f.proxy.ResolutionSetDigest, f.proxy.PolicyBody, f.proxy.AttestationBody, window)
		if delta < 0 {
			if err == nil || !reflect.DeepEqual(proxy, domain.RegisteredProxyAttestationV1{}) {
				t.Fatal("proxy observation predating actual allocation was attached to it")
			}
		} else if err != nil || proxy.ValidateForAllocation(allocation, f.source) != nil {
			t.Fatalf("proxy observation at/after allocation rejected: delta=%s err=%v", delta, err)
		}
	}
}

func TestRegisteredProxyHistoricalCorrelationRequiresOriginalAllocationSnapshot(t *testing.T) {
	t.Parallel()
	f := registeredAllocationFixture(t, false)
	retired := f.allocation.Clone()
	retired.State, retired.RevocationGeneration = domain.RegisteredPreparationRetiredV1, 2
	retired.UpdatedAt = f.at.Add(2 * time.Minute)
	if err := retired.ValidateTransitionFrom(f.allocation); err != nil {
		t.Fatal(err)
	}
	proxy := f.proxy.Clone()
	proxy.State, proxy.RevocationGeneration = domain.RegisteredPreparationRevokedV1, 2
	proxy.UpdatedAt = retired.UpdatedAt
	if proxy.ValidateTransitionFrom(f.proxy) != nil || proxy.ValidateForAllocation(f.allocation, f.source) != nil {
		t.Fatal("historical correlation lost exact original allocation")
	}
	if proxy.ValidateForAllocation(retired, f.source) == nil || proxy.ValidateAt(proxy.UpdatedAt) == nil || retired.ValidateAt(retired.UpdatedAt) == nil {
		t.Fatal("current retired/revoked facts were treated as original active or send permission")
	}
	raw, err := domain.EncodeRegisteredProxyAttestationV1(proxy)
	decoded, decodeErr := domain.DecodeRegisteredProxyAttestationV1(raw)
	if err != nil || decodeErr != nil || !reflect.DeepEqual(decoded, proxy) {
		t.Fatal("expired/revoked historical observation became unreadable", err, decodeErr)
	}
}
