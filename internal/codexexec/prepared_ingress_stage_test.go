//go:build darwin || linux

package codexexec

import (
	"bytes"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

func TestPreparedIngressContractStageMatchesExistingManifestFixtures(t *testing.T) {
	for _, tc := range []struct {
		name           string
		older, current int
	}{
		{name: "normal five-entry manifest", current: 4},
		{name: "current subset after older history", older: 2, current: 2},
		{name: "text successor retains four older attachments", older: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, tc.older, tc.current)
			stage := preparedIngressContractStage(input)
			if (stage == 0) != preparedIngressEnvelope(input) {
				t.Fatal("legacy bool wrapper diverged from the closed stage predicate")
			}
			if stage != 0 {
				t.Fatalf("valid existing manifest fixture classified as %s", PreparedContractStageV1(preparedContractFailure(stage)))
			}
		})
	}
	for _, tc := range []struct {
		fault string
		stage preparedContractStage
	}{
		{fault: "missing", stage: preparedContractStageIngressSource},
		{fault: "extra", stage: preparedContractStageIngressSource},
		{fault: "duplicate", stage: preparedContractStageIngressSource},
		{fault: "foreign blob", stage: preparedContractStageIngressSource},
		{fault: "name", stage: preparedContractStageIngressSource},
		{fault: "media", stage: preparedContractStageIngressSource},
		{fault: "body", stage: preparedContractStageIngressUploads},
		{fault: "proof body", stage: preparedContractStageIngressUploads},
		{fault: "message body", stage: preparedContractStageIngressTrigger},
		{fault: "message first", stage: preparedContractStageIngressManifest},
		{fault: "older as current", stage: preparedContractStageIngressSource},
	} {
		t.Run(tc.fault, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, 2, 2)
			switch tc.fault {
			case "missing":
				input.Manifest.Artifacts = input.Manifest.Artifacts[:2]
				input.Artifacts = input.Artifacts[:2]
			case "extra", "duplicate":
				ref, body := input.Manifest.Artifacts[1], input.Artifacts[1]
				if tc.fault == "extra" {
					ref.Name, body.Name = "unrequested.bin", "unrequested.bin"
				}
				input.Manifest.Artifacts = append(input.Manifest.Artifacts, ref)
				input.Artifacts = append(input.Artifacts, body)
			case "foreign blob":
				input.Manifest.Artifacts[1].Blob.Key = "tenants/" + string(input.Job.TenantID) + "/foreign-object"
			case "name":
				input.Manifest.Artifacts[1].Name, input.Artifacts[1].Name = "attachment-01-foreign.png", "attachment-01-foreign.png"
			case "media":
				input.Manifest.Artifacts[1].MediaType = "text/plain"
			case "body":
				input.Artifacts[1].Body[0] ^= 1
			case "proof body":
				input.CanonicalContext.Attachments[2].Body[0] ^= 1
			case "message body":
				input.Artifacts[0].Body[0] ^= 1
			case "message first":
				input.Artifacts[0], input.Artifacts[1] = input.Artifacts[1], input.Artifacts[0]
				input.Manifest.Artifacts[0], input.Manifest.Artifacts[1] = input.Manifest.Artifacts[1], input.Manifest.Artifacts[0]
			case "older as current":
				input.Manifest.Artifacts[1].Blob = input.CanonicalContext.Attachments[0].Blob
				input.Artifacts[1].Body = bytes.Clone(input.CanonicalContext.Attachments[0].Body)
			}
			if tc.fault != "duplicate" { // The canonical job digest already rejects duplicate artifact names.
				preparedIngressManifestDigest(t, input)
			}
			stage := preparedIngressContractStage(input)
			if preparedIngressEnvelope(input) || stage != tc.stage {
				t.Fatalf("existing manifest denial classification got=%s want=%s",
					PreparedContractStageV1(preparedContractFailure(stage)), PreparedContractStageV1(preparedContractFailure(tc.stage)))
			}
		})
	}
}

func TestPreparedIngressContractStageNamesEachClosedDenialBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stage  preparedContractStage
		mutate func(*attachedworkerdaemon.VerifiedAttachedInputV1)
	}{
		{name: "shape", stage: preparedContractStageIngressShape, mutate: func(input *attachedworkerdaemon.VerifiedAttachedInputV1) {
			input.CanonicalContext = nil
		}},
		{name: "records", stage: preparedContractStageIngressRecords, mutate: func(input *attachedworkerdaemon.VerifiedAttachedInputV1) {
			clear(input.Context)
			input.Context = nil
		}},
		{name: "history", stage: preparedContractStageIngressHistory, mutate: func(input *attachedworkerdaemon.VerifiedAttachedInputV1) {
			input.CanonicalContext.Attachments = nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := preparedIngressManifestFixture(t, 0, 2)
			tc.mutate(input)
			stage := preparedIngressContractStage(input)
			if stage != tc.stage || preparedIngressEnvelope(input) {
				t.Fatalf("closed ingress denial got=%s want=%s", PreparedContractStageV1(preparedContractFailure(stage)),
					PreparedContractStageV1(preparedContractFailure(tc.stage)))
			}
		})
	}
}
