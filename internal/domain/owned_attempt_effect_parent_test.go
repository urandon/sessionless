package domain_test

import (
	"bytes"
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
)

func ownedParentFixture(t testing.TB, maximal bool) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionAdmissionV1, time.Time) {
	t.Helper()
	scope, profile, spending, at := admissionSourcesFixture(t)
	if maximal {
		scope, profile, spending, at = maximalAdmissionProducerSourcesFixture(t)
	}
	pkg := mustAdmissionPackage(t, scope, profile, spending, at)
	final, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "owned-parent-quota", at.Add(time.Microsecond))
	if err != nil {
		t.Fatalf("original Final: %v", err)
	}
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement,
		SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost,
		Lease: domain.Lease{ID: "owned-parent-lease", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID,
			WorkerID: "owned-parent-worker", FenceToken: math.MaxUint64, AcquiredAt: final.AdmittedAt, ExpiresAt: at.Add(10 * time.Minute)},
		ContextManifestDigest: strings.Repeat("1", 64), InputManifestDigest: strings.Repeat("2", 64), InvocationDeadline: at.Add(2 * time.Minute)}
	claim := "owned-parent-sole-claim"
	if maximal {
		a.Lease.ID, a.Lease.WorkerID, claim = domain.LeaseID(strings.Repeat("l", 160)), strings.Repeat("w", 160), strings.Repeat("c", 160)
	}
	upstream := strings.Repeat("3", 64)
	parent, err := domain.BuildOwnedAttemptEffectParentV1(final, a, claim, &upstream, at.Add(2*time.Microsecond))
	if err != nil {
		t.Fatalf("owned parent builder (maximal=%v): %v", maximal, err)
	}
	return parent, final, at
}

func TestOwnedAttemptEffectParentPreservesOriginalFinalAndPreparationWindow(t *testing.T) {
	t.Parallel()
	v, final, at := ownedParentFixture(t, false)
	if err := v.ValidateForAdmission(final); err != nil {
		t.Fatalf("original historical correlation: %v", err)
	}
	want := v.Reservation.ReservedAt.Add(v.Authority.AdmissionCostCeiling.MaxPreEffectDurationPerDelivery)
	if !v.PreparationExpiresAt.Equal(want) {
		t.Errorf("preparation deadline: got=%s want original reserve+pre-effect=%s", v.PreparationExpiresAt, want)
	}
	if err := v.Authority.ValidateAt(at.Add(24 * time.Hour)); err == nil {
		t.Fatal("expired authority must not be current permission")
	}
	if err := v.ValidateForAdmission(final); err != nil {
		t.Fatalf("historical bytes must remain correlatable after expiry: %v", err)
	}
	cloned := v.Clone()
	*cloned.Reservation.UpstreamIdempotencyKeyDigest = strings.Repeat("4", 64)
	*cloned.Authority.AdmissionCostCeiling.MaxProviderAmountMicrounits++
	cloned.Authority.HarnessBinding.AgentLoopPolicy.InputModalities[0] = domain.AgentInputImageV1
	if *v.Reservation.UpstreamIdempotencyKeyDigest != strings.Repeat("3", 64) ||
		*v.Authority.AdmissionCostCeiling.MaxProviderAmountMicrounits == *cloned.Authority.AdmissionCostCeiling.MaxProviderAmountMicrounits ||
		v.Authority.HarnessBinding.AgentLoopPolicy.InputModalities[0] == domain.AgentInputImageV1 {
		t.Fatal("Clone retained mutable reservation, amount or policy storage")
	}
}

func TestOwnedAttemptEffectParentRejectsSubstitutionAndWindowRenewal(t *testing.T) {
	t.Parallel()
	cases := map[string]func(*domain.OwnedAttemptEffectParentV1, *domain.AttemptExecutionAdmissionV1){
		"version": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) { v.Version++ },
		"scope session": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Scope.SessionID = "another-session"
		},
		"scope owner": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Scope.OwnerUserID = "another-owner"
		},
		"Final digest": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.AdmissionDigest = strings.Repeat("8", 64)
		},
		"different reservation identity": func(_ *domain.OwnedAttemptEffectParentV1, f *domain.AttemptExecutionAdmissionV1) {
			f.ReservationID = "another-held-capacity"
		},
		"admitted after reserve": func(v *domain.OwnedAttemptEffectParentV1, f *domain.AttemptExecutionAdmissionV1) {
			f.AdmittedAt = v.Reservation.ReservedAt.Add(time.Microsecond)
		},
		"deadline renewal": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.PreparationExpiresAt = v.PreparationExpiresAt.Add(time.Microsecond)
		},
		"shortened nonproducer deadline": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.PreparationExpiresAt = v.PreparationExpiresAt.Add(-time.Microsecond)
		},
		"submicro reserve": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Reservation.ReservedAt = v.Reservation.ReservedAt.Add(time.Nanosecond)
		},
		"submicro expiry": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.PreparationExpiresAt = v.PreparationExpiresAt.Add(time.Nanosecond)
		},
		"physical claim empty": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Reservation.PhysicalInvocationClaimID = ""
		},
		"zero fence": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Authority.Lease.FenceToken = 0
		},
		"unknown price": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Authority.AdmissionCostCeiling.ProviderPriceState = domain.ProviderPriceUnknownV1
		},
		"zero allocation": func(v *domain.OwnedAttemptEffectParentV1, _ *domain.AttemptExecutionAdmissionV1) {
			v.Authority.AdmissionCostCeiling.MaxTotalAmountMicrounits = nil
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			v, f, _ := ownedParentFixture(t, false)
			mutate(&v, &f)
			if err := v.ValidateForAdmission(f); err == nil {
				t.Fatal("substituted original parent/Final accepted")
			}
		})
	}
}

func TestOwnedAttemptEffectParentBuilderDeniesInvalidInputsAndSealsOptionalPresence(t *testing.T) {
	t.Parallel()
	base, final, _ := ownedParentFixture(t, false)
	for name, mutate := range map[string]func(*domain.AttemptExecutionAdmissionV1, *domain.ServerlessInvocationAuthorityV1, *string, **string, *time.Time){
		"empty claim": func(_ *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, c *string, _ **string, _ *time.Time) {
			*c = ""
		},
		"oversize claim": func(_ *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, c *string, _ **string, _ *time.Time) {
			*c = strings.Repeat("c", 161)
		},
		"zero time": func(_ *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, at *time.Time) {
			*at = time.Time{}
		},
		"submicro time": func(_ *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, at *time.Time) {
			*at = at.Add(time.Nanosecond)
		},
		"reserve before Final": func(f *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, at *time.Time) {
			f.AdmittedAt = at.Add(time.Microsecond)
		},
		"expired invocation": func(_ *domain.AttemptExecutionAdmissionV1, a *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, at *time.Time) {
			*at = a.InvocationDeadline
		},
		"deadline beyond source": func(f *domain.AttemptExecutionAdmissionV1, a *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, _ *time.Time) {
			a.Lease.ExpiresAt = f.Source.ExpiresAt.Add(time.Minute)
			a.InvocationDeadline = f.Source.ExpiresAt.Add(time.Microsecond)
		},
		"invalid upstream": func(_ *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, _ *string, u **string, _ *time.Time) {
			bad := "not-a-digest"
			*u = &bad
		},
		"zero Final": func(f *domain.AttemptExecutionAdmissionV1, _ *domain.ServerlessInvocationAuthorityV1, _ *string, _ **string, _ *time.Time) {
			*f = domain.AttemptExecutionAdmissionV1{}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f, a, claim, upstream, at := final.Clone(), base.Authority.Clone(), base.Reservation.PhysicalInvocationClaimID, base.Reservation.Clone().UpstreamIdempotencyKeyDigest, base.Reservation.ReservedAt
			mutate(&f, &a, &claim, &upstream, &at)
			got, err := domain.BuildOwnedAttemptEffectParentV1(f, a, claim, upstream, at)
			if err == nil || !reflect.DeepEqual(got, domain.OwnedAttemptEffectParentV1{}) {
				t.Fatalf("invalid producer returned usable parent: err=%v", err)
			}
		})
	}
	noUpstream, err := domain.BuildOwnedAttemptEffectParentV1(final, base.Authority, base.Reservation.PhysicalInvocationClaimID, nil, base.Reservation.ReservedAt)
	if err != nil || noUpstream.Reservation.UpstreamIdempotencyKeyDigest != nil {
		t.Fatalf("explicit absent upstream must remain absent: err=%v", err)
	}
	absentDigest, _ := noUpstream.Digest()
	presentDigest, _ := base.Digest()
	if absentDigest == presentDigest {
		t.Fatal("optional upstream presence disappeared from original parent digest")
	}
	short := base.Authority.Clone()
	short.InvocationDeadline = base.Reservation.ReservedAt.Add(5 * time.Second)
	bounded, err := domain.BuildOwnedAttemptEffectParentV1(final, short, base.Reservation.PhysicalInvocationClaimID, nil, base.Reservation.ReservedAt)
	if err != nil || !bounded.PreparationExpiresAt.Equal(short.InvocationDeadline) {
		t.Fatalf("short original invocation bound: got=%s want=%s err=%v", bounded.PreparationExpiresAt, short.InvocationDeadline, err)
	}
}

func TestOwnedAttemptEffectParentIndependentDigestAndEveryPersistedLeaf(t *testing.T) {
	t.Parallel()
	v, _, _ := ownedParentFixture(t, false)
	raw, err := domain.EncodeOwnedAttemptEffectParentV1(v)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	h.Write([]byte("sessionless.owned-attempt-effect-parent.v1\x00"))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(raw)))
	h.Write(length[:])
	h.Write(raw)
	want := hex.EncodeToString(h.Sum(nil))
	got, err := v.Digest()
	if err != nil || got != want {
		t.Fatalf("independent complete-record framing: got=%s want=%s err=%v", got, want, err)
	}
	const literal = "58e66c270a768edf149966977282388bae37a2a57c010e79fbf38f6b8a5ee225"
	if got != literal {
		t.Errorf("literal original-parent digest: got=%s want=%s", got, literal)
	}
	var obj map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&obj); err != nil {
		t.Fatal(err)
	}
	var paths [][]string
	var walk func(map[string]any, []string)
	walk = func(m map[string]any, prefix []string) {
		for key, value := range m {
			path := append(append([]string(nil), prefix...), key)
			if nested, ok := value.(map[string]any); ok {
				walk(nested, path)
			} else {
				paths = append(paths, path)
			}
		}
	}
	walk(obj, nil)
	for _, path := range paths {
		t.Run(strings.Join(path, "/"), func(t *testing.T) {
			var fresh map[string]any
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&fresh); err != nil {
				t.Fatal(err)
			}
			m := fresh
			for _, part := range path[:len(path)-1] {
				m = m[part].(map[string]any)
			}
			key := path[len(path)-1]
			switch value := m[key].(type) {
			case string:
				m[key] = value + "x"
			case json.Number:
				m[key] = json.Number("0")
				if value == "0" {
					m[key] = json.Number("1")
				}
			case []any:
				m[key] = []any{"file"}
			default:
				t.Fatalf("uncovered persisted leaf %s (%T)", key, value)
			}
			encoded, err := json.Marshal(fresh)
			if err != nil {
				t.Fatal(err)
			}
			// Unmarshal just constructs a typed candidate: the producer codec is
			// intentionally stricter than generic JSON and must not bless it.
			var candidate domain.OwnedAttemptEffectParentV1
			if json.Unmarshal(encoded, &candidate) == nil {
				digest, err := candidate.Digest()
				if err == nil && digest == got {
					t.Fatalf("persisted field %s omitted from digest", strings.Join(path, "/"))
				}
			}
		})
	}
}

func TestOwnedAttemptEffectParentStrictCodecAndMaximalProducer(t *testing.T) {
	t.Parallel()
	v, _, _ := ownedParentFixture(t, false)
	raw, err := domain.EncodeOwnedAttemptEffectParentV1(v)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeOwnedAttemptEffectParentV1(raw)
	if err != nil || !reflect.DeepEqual(decoded, v.Clone()) {
		t.Fatalf("canonical roundtrip: err=%v equal=%v", err, reflect.DeepEqual(decoded, v.Clone()))
	}
	for name, bad := range map[string][]byte{
		"duplicate":              bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"unknown":                append([]byte(`{"unexpected":0,`), raw[1:]...),
		"case alias":             bytes.Replace(raw, []byte(`"admission_digest"`), []byte(`"Admission_Digest"`), 1),
		"null optional":          bytes.Replace(raw, []byte(`"upstream_idempotency_key_digest":"`+strings.Repeat("3", 64)+`"`), []byte(`"upstream_idempotency_key_digest":null`), 1),
		"trailing":               append(append([]byte(nil), raw...), []byte(` {}`)...),
		"whitespace":             append([]byte(" "), raw...),
		"exact 32KiB whitespace": append(append([]byte(nil), raw...), bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1-len(raw))...),
		"above 32KiB":            bytes.Repeat([]byte("a"), domain.OwnedAgentCurrentMaxRecordBytesV1+1),
		"lossy max integer":      bytes.Replace(raw, []byte(`"fence_token":18446744073709551615`), []byte(`"fence_token":18446744073709551616`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			value, err := domain.DecodeOwnedAttemptEffectParentV1(bad)
			if err == nil || !reflect.DeepEqual(value, domain.OwnedAttemptEffectParentV1{}) {
				t.Fatalf("invalid codec returned usable partial parent: err=%v", err)
			}
		})
	}
	maximal, final, _ := ownedParentFixture(t, true)
	maxRaw, err := domain.EncodeOwnedAttemptEffectParentV1(maximal)
	if err != nil || len(maxRaw) > 32<<10 || domain.OwnedAgentCurrentMaxRecordBytesV1 != 32<<10 {
		t.Fatalf("maximal complete parent: bytes=%d err=%v", len(maxRaw), err)
	}
	if err := maximal.ValidateForAdmission(final); err != nil {
		t.Fatal(err)
	}
	if maximal.Scope != final.Source.Scope || len(maximal.Reservation.PhysicalInvocationClaimID) != 160 || maximal.Authority.Lease.FenceToken != math.MaxUint64 || maximal.Authority.HarnessBinding.ModelID != final.Source.Profile.Locator.ModelID {
		t.Fatal("maximal producer truncated original IDs, exact integers or model")
	}
	t.Logf("maximal original parent head: %d / %d bytes", len(maxRaw), domain.OwnedAgentCurrentMaxRecordBytesV1)
}

func FuzzOwnedAttemptEffectParentCodec(f *testing.F) {
	v, _, _ := ownedParentFixture(f, false)
	raw, err := domain.EncodeOwnedAttemptEffectParentV1(v)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add([]byte(`{"version":1,"version":1}`))
	f.Add(append([]byte(" "), raw...))
	f.Add(bytes.Repeat([]byte("x"), (32<<10)+1))
	f.Fuzz(func(t *testing.T, candidate []byte) {
		decoded, err := domain.DecodeOwnedAttemptEffectParentV1(candidate)
		if err != nil {
			if !reflect.DeepEqual(decoded, domain.OwnedAttemptEffectParentV1{}) {
				t.Fatal("decode failure returned partial authority")
			}
			return
		}
		canonical, err := domain.EncodeOwnedAttemptEffectParentV1(decoded)
		if err != nil || !bytes.Equal(canonical, candidate) {
			t.Fatalf("accepted noncanonical producer bytes: err=%v", err)
		}
	})
}
