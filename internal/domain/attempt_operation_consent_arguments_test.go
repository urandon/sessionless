package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestAttemptOperationConsentArgumentsClosedReference(t *testing.T) {
	scope := operationFixture().Scope
	digest := strings.Repeat("a", 64)
	raw, err := json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	for _, size := range []uint64{1, 4096} {
		ref, err := domain.AttemptOperationConsentArgumentsRefV1(scope, size, digest)
		want := domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent-arguments-v1/" + hex.EncodeToString(hash[:]) + "/consent-args-v1." + strconv.FormatUint(size, 10) + "." + digest, Size: int64(size), SHA256: digest}
		if err != nil || ref != want {
			t.Fatalf("size=%d closed reference got=%+v err=%v want=%+v", size, ref, err, want)
		}
		parsed, err := domain.ParseAttemptOperationConsentArgumentsRefV1(scope, ref.Key, size)
		if err != nil || parsed != want {
			t.Fatalf("size=%d exact parse got=%+v err=%v", size, parsed, err)
		}
		if _, err := domain.ParseAttemptOperationConsentArgumentsRefV1(scope, ref.Key, size-1); err == nil {
			t.Fatalf("size=%d parsed without original read capacity", size)
		}
	}
	for _, size := range []uint64{0, 4097, ^uint64(0)} {
		if ref, err := domain.AttemptOperationConsentArgumentsRefV1(scope, size, digest); err == nil || ref != (domain.BlobRef{}) {
			t.Fatalf("invalid size=%d returned ref=%+v err=%v", size, ref, err)
		}
	}
}

func TestAttemptOperationConsentArgumentsRejectForeignAndAlias(t *testing.T) {
	scope := operationFixture().Scope
	ref, err := domain.AttemptOperationConsentArgumentsRefV1(scope, 12, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		key  string
	}{
		{"leading_zero", strings.Replace(ref.Key, ".12.", ".012.", 1)},
		{"plus", strings.Replace(ref.Key, ".12.", ".+12.", 1)},
		{"upper_digest", strings.TrimSuffix(ref.Key, ref.SHA256) + strings.ToUpper(ref.SHA256)},
		{"future_arm", strings.Replace(ref.Key, "/consent-arguments-v1/", "/consent-arguments-v2/", 1)},
		{"ordinary_suffix", strings.Replace(ref.Key, "consent-args-v1.", "opbody-v1.", 1)},
		{"path_alias", strings.Replace(ref.Key, "/attempts/", "/./attempts/", 1)},
		{"trailing", ref.Key + "/extra"},
		{"overlong", strings.Repeat("x", 2049)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := domain.ParseAttemptOperationConsentArgumentsRefV1(scope, tc.key, 4096); err == nil || got != (domain.BlobRef{}) {
				t.Fatalf("alias accepted got=%+v err=%v", got, err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.AttemptOperationScopeV1)
	}{
		{"tenant", func(v *domain.AttemptOperationScopeV1) { v.TenantID = "foreign-tenant" }},
		{"owner", func(v *domain.AttemptOperationScopeV1) { v.OwnerUserID = "foreign-owner" }},
		{"session", func(v *domain.AttemptOperationScopeV1) { v.SessionID = "foreign-session" }},
		{"run", func(v *domain.AttemptOperationScopeV1) { v.RunID = "foreign-run" }},
		{"attempt", func(v *domain.AttemptOperationScopeV1) { v.AttemptID = "foreign-attempt" }},
		{"lease", func(v *domain.AttemptOperationScopeV1) { v.LeaseID = "foreign-lease" }},
		{"fence", func(v *domain.AttemptOperationScopeV1) { v.FenceToken++ }},
		{"claim", func(v *domain.AttemptOperationScopeV1) { v.PhysicalInvocationClaimID = "foreign-claim" }},
		{"effect", func(v *domain.AttemptOperationScopeV1) {
			v.EffectReservationDigest = domain.AttemptEffectReservationDigestV1(strings.Repeat("b", 64))
		}},
		{"authority", func(v *domain.AttemptOperationScopeV1) {
			v.InvocationAuthorityDigest = domain.ServerlessInvocationAuthorityDigestV1(strings.Repeat("b", 64))
		}},
		{"binding", func(v *domain.AttemptOperationScopeV1) {
			v.HarnessBindingDigest = domain.HarnessBindingDigestV2(strings.Repeat("b", 64))
		}},
		{"policy", func(v *domain.AttemptOperationScopeV1) {
			v.AgentLoopPolicyDigest = domain.AgentLoopPolicyDigestV1(strings.Repeat("b", 64))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			other := scope
			tc.mutate(&other)
			if got, err := domain.ParseAttemptOperationConsentArgumentsRefV1(other, ref.Key, 4096); err == nil || got != (domain.BlobRef{}) {
				t.Fatalf("foreign scope accepted got=%+v err=%v", got, err)
			}
		})
	}
}
