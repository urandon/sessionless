//go:build darwin || linux

package codexexec

import (
	"fmt"
	"sort"
	"testing"
)

// BoundMaterializer validates the name/blob closure and then sorts the body
// carrier by name, independently of the immutable ingress manifest order.
// This reproduces that carrier transformation, not authenticated admission or
// actual materialization; the joined exact-head CI proves that composition.
func TestPreparedIngressManifestAcceptsNameBoundBodyOrder(t *testing.T) {
	for _, counts := range []struct{ older, current int }{{current: 4}, {older: 2, current: 2}} {
		for _, order := range []string{"materializer sorted", "reverse"} {
			t.Run(fmt.Sprintf("older%d-current%d/%s", counts.older, counts.current, order), func(t *testing.T) {
				input := preparedIngressManifestFixture(t, counts.older, counts.current)
				sort.Slice(input.Artifacts, func(i, j int) bool {
					if order == "reverse" {
						return input.Artifacts[i].Name > input.Artifacts[j].Name
					}
					return input.Artifacts[i].Name < input.Artifacts[j].Name
				})
				if err := input.ValidateFor(preparedIngressManifestIdentity(input)); err != nil {
					t.Fatalf("name-bound carrier remains verified: %v", err)
				}
				if stage := preparedIngressContractStage(input); stage != 0 {
					t.Fatalf("verified name-bound carrier rejected at %s", PreparedContractStageV1(preparedContractFailure(stage)))
				}
			})
		}
	}
}

func TestPreparedIngressManifestNameBoundBodyDenials(t *testing.T) {
	for _, fault := range []string{"duplicate", "foreign", "mismatched body"} {
		t.Run(fault, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, 2, 2)
			sort.Slice(input.Artifacts, func(i, j int) bool { return input.Artifacts[i].Name < input.Artifacts[j].Name })
			switch fault {
			case "duplicate":
				input.Artifacts[1].Name = input.Artifacts[0].Name
			case "foreign":
				input.Artifacts[1].Name = "undeclared-body"
			case "mismatched body":
				input.Artifacts[0].Body[0] ^= 1
			}
			if preparedIngressEnvelope(input) {
				t.Fatal("invalid name/body carrier accepted")
			}
		})
	}
}
