package codexexec

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestPreparedContractStageV1IsClosedAndRedacted(t *testing.T) {
	cases := []struct {
		stage preparedContractStage
		want  string
	}{
		{preparedContractStageIdentity, "identity"},
		{preparedContractStageVerifiedInput, "verified_input"},
		{preparedContractStageNativeGeneration, "native_generation"},
		{preparedContractStageNativeFixed, "native_fixed"},
		{preparedContractStageDescriptor, "descriptor"},
		{preparedContractStageIngress, "ingress"},
		{preparedContractStageUnsupportedInput, "unsupported_input"},
		{preparedContractStageDriver, "driver"},
		{preparedContractStagePreflight, "preflight"},
		{preparedContractStageIngressShape, "ingress_shape"},
		{preparedContractStageIngressManifest, "ingress_manifest"},
		{preparedContractStageIngressRecords, "ingress_records"},
		{preparedContractStageIngressTrigger, "ingress_trigger"},
		{preparedContractStageIngressSource, "ingress_source"},
		{preparedContractStageIngressHistory, "ingress_history"},
		{preparedContractStageIngressUploads, "ingress_uploads"},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			err := preparedContractFailure(tc.stage)
			if !errors.Is(err, ErrContract) || PreparedContractStageV1(err) != tc.want {
				t.Fatalf("stage=%q preserves contract classification", PreparedContractStageV1(err))
			}
			var classified *preparedContractStageError
			if !errors.As(err, &classified) || err.Error() != "prepared invocation contract failure" {
				t.Fatal("classified error must be discoverable with fixed content-free text")
			}
		})
	}
	if got := PreparedContractStageV1(preparedContractFailure(preparedContractStage(255))); got != "" {
		t.Fatalf("unknown internal stage must be closed, got %q", got)
	}
	var typedNil *preparedContractStageError
	if PreparedContractStageV1(nil) != "" || PreparedContractStageV1(typedNil) != "" {
		t.Fatal("nil errors must not produce a diagnostic stage")
	}
}

func TestPreparedContractStageV1HandlesJoinedAndUnknownErrorsWithoutDisclosure(t *testing.T) {
	const marker = "private-input-path-and-reference-marker"
	joined := errors.Join(fmt.Errorf("%s: %w", marker, preparedContractFailure(preparedContractStageIngress)), errors.New(marker))
	if !errors.Is(joined, ErrContract) || PreparedContractStageV1(joined) != "ingress" {
		t.Fatal("joined classified contract must preserve errors.Is and closed stage")
	}
	unknown := errors.New(marker)
	if got := PreparedContractStageV1(unknown); got != "" || strings.Contains(got, marker) {
		t.Fatal("unknown errors must produce no stage and classifier output must not disclose source text")
	}
	if got := PreparedContractStageV1(joined); strings.Contains(got, marker) {
		t.Fatal("joined error classifier disclosed source text")
	}
}
