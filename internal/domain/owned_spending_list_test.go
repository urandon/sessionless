package domain_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func textExecutionPackage(t testing.TB) domain.OwnedAgentExecutionPackageV1 {
	t.Helper()
	p := localTextInstallationGrantFixture(t).Profile
	spending := []domain.AttemptSpendingAuthorizationCurrentV1{
		localSpendingFixture(t, p, domain.AttemptSpendingProviderV1),
		localSpendingFixture(t, p, domain.AttemptSpendingSubstrateV1),
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, RunID: "local-run", AttemptID: "local-attempt", SessionID: "local-session"}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, p.Window.PublishedAt)
	if err != nil {
		t.Fatalf("actual two-component DATA package: %v", err)
	}
	return pkg
}

func TestOwnedSpendingTextCardinalityCanonicalAndClone(t *testing.T) {
	t.Parallel()
	kinds := domain.OwnedSpendingComponentsV1(domain.AgentLoopOwnedLocalTextV1)
	if len(kinds) != 2 {
		t.Fatal("text component shape")
	}
	kinds[0] = domain.AttemptSpendingWebSearchV1
	if domain.OwnedSpendingComponentsV1(domain.AgentLoopOwnedLocalTextV1)[0] != domain.AttemptSpendingProviderV1 || domain.OwnedSpendingComponentsV1("unknown") != nil {
		t.Fatal("component shape aliases or infers unknown authority")
	}
	pkg := textExecutionPackage(t)
	s := pkg.Source
	if len(s.Spending) != 2 || s.MaxTotalAmountMicrounits != 0 {
		t.Fatal("text source contains phantom service allocation")
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(s)
	if err != nil {
		t.Fatal(err)
	}
	round, err := domain.DecodeAttemptExecutionSourceV1(raw)
	if err != nil {
		t.Fatalf("canonical text source: %v", err)
	}
	again, err := domain.EncodeAttemptExecutionSourceV1(round)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("text source roundtrip changed original bytes")
	}
	clone := s.Clone()
	clone.Spending[0].MutationID = "independent-copy"
	*clone.Spending[0].MaxAmountMicrounits = 1
	if s.Spending[0].MutationID == clone.Spending[0].MutationID || *s.Spending[0].MaxAmountMicrounits != 0 {
		t.Fatal("source clone aliases list or nested component")
	}
	for name, mutate := range map[string]func(*domain.AttemptExecutionSourceV1){
		"missing":         func(v *domain.AttemptExecutionSourceV1) { v.Spending = nil },
		"short":           func(v *domain.AttemptExecutionSourceV1) { v.Spending = v.Spending[:1] },
		"duplicate":       func(v *domain.AttemptExecutionSourceV1) { v.Spending[1] = v.Spending[0] },
		"reordered":       func(v *domain.AttemptExecutionSourceV1) { v.Spending[0], v.Spending[1] = v.Spending[1], v.Spending[0] },
		"extra":           func(v *domain.AttemptExecutionSourceV1) { v.Spending = append(v.Spending, v.Spending[0]) },
		"crossed_profile": func(v *domain.AttemptExecutionSourceV1) { v.Profile.Locator.Profile = domain.AgentLoopOwnedPublicV1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := s.Clone()
			mutate(&candidate)
			if candidate.Validate() == nil {
				t.Fatal("noncanonical actual-allocation list accepted")
			}
		})
	}
}

func TestOwnedSpendingListDecoderBoundsEveryOccurrence(t *testing.T) {
	t.Parallel()
	var nilRecords *domain.OwnedSpendingListV1
	var nilRefs *domain.RegisteredSpendingListV1
	if nilRecords.UnmarshalJSON([]byte(`[{}]`)) == nil || nilRefs.UnmarshalJSON([]byte(`[{}]`)) == nil {
		t.Fatal("nil list receiver accepted")
	}
	for _, raw := range [][]byte{nil, bytes.Repeat([]byte(" "), domain.OwnedAgentCurrentMaxRecordBytesV1+1), {'[', '"', 0xff, '"', ']'}} {
		var records domain.OwnedSpendingListV1
		var refs domain.RegisteredSpendingListV1
		if records.UnmarshalJSON(raw) == nil || refs.UnmarshalJSON(raw) == nil {
			t.Fatal("invalid raw list boundary accepted")
		}
	}
	for _, raw := range []string{"null", "[]", "[null]", "[{},null]", "[{},{},{},{},{}]", "[{}] null"} {
		t.Run(raw, func(t *testing.T) {
			var records domain.OwnedSpendingListV1
			if json.Unmarshal([]byte(raw), &records) == nil {
				t.Fatal("unbounded/null source list accepted")
			}
			var refs domain.RegisteredSpendingListV1
			if json.Unmarshal([]byte(raw), &refs) == nil {
				t.Fatal("unbounded/null reference list accepted")
			}
		})
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(textExecutionPackage(t).Source)
	if err != nil {
		t.Fatal(err)
	}
	// Even when a later duplicate is valid, the first occurrence must stop at
	// the fifth element before growing a typed list.
	mutated := strings.Replace(string(raw), `"spending":`, `"spending":[{},{},{},{},{}],"spending":`, 1)
	if _, err := domain.DecodeAttemptExecutionSourceV1([]byte(mutated)); err == nil {
		t.Fatal("oversized first duplicate list hidden by later valid list")
	}
}

func FuzzOwnedSpendingListBound(f *testing.F) {
	for _, seed := range []string{"null", "[]", "[null]", "[{},{},{},{},{}]", "[{}]"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 32<<10 {
			t.Skip()
		}
		var records domain.OwnedSpendingListV1
		if json.Unmarshal([]byte(raw), &records) == nil && (len(records) == 0 || len(records) > 4) {
			t.Fatalf("decoded list length %d", len(records))
		}
	})
}
