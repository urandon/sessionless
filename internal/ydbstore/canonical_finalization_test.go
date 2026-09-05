package ydbstore

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestRunFinalizationDigestSealsSubstrateEvidenceDigest(t *testing.T) {
	t.Parallel()
	withoutEvidence, err := runFinalizationDigest(domain.RunSucceeded, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence := &domain.SubstrateExecutionEvidenceV1{
		EvidenceDigest: domain.SubstrateExecutionEvidenceDigestV1(strings.Repeat("a", 64)),
	}
	withEvidence, err := runFinalizationDigest(domain.RunSucceeded, nil, nil, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if withEvidence == withoutEvidence {
		t.Fatal("substrate evidence digest did not change canonical finalization identity")
	}
	mutated := evidence.Clone()
	mutated.EvidenceDigest = domain.SubstrateExecutionEvidenceDigestV1(strings.Repeat("b", 64))
	withMutatedEvidence, err := runFinalizationDigest(domain.RunSucceeded, nil, nil, &mutated)
	if err != nil {
		t.Fatal(err)
	}
	if withMutatedEvidence == withEvidence {
		t.Fatal("different substrate evidence retained canonical finalization identity")
	}
}
