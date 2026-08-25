package ports_test

import (
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestAttachedWorkerExecutionMutationAuthorityBoundary(t *testing.T) {
	t.Parallel()
	offer := reflect.TypeOf(ports.AttachedWorkerAttemptOffer{})
	for _, forbidden := range []string{"Placement", "ContextDigest", "ConnectionID", "ExpectedWorkerRevision", "ExpectedConnectionRevision", "EnrollmentGeneration", "ConnectionGeneration", "ProtocolSnapshot", "ProtocolConfig"} {
		if _, found := offer.FieldByName(forbidden); found {
			t.Fatalf("offer exposes caller-derived field %q", forbidden)
		}
	}
	exchange := reflect.TypeOf(ports.AttachedWorkerAttemptExchange{})
	inbound, found := exchange.FieldByName("InboundFrame")
	if !found || inbound.Type != reflect.TypeOf(attachedworkerprotocol.FrameV1{}) {
		t.Fatal("exchange must expose only the untrusted typed inbound protocol frame")
	}
	for _, forbidden := range []string{"Inbound", "Outbound", "ProtocolSnapshot", "ProtocolConfig", "ExpectedAttemptRevision", "ExpectedConnectionRevision"} {
		if _, found := exchange.FieldByName(forbidden); found {
			t.Fatalf("exchange exposes caller-derived field %q", forbidden)
		}
	}
	for name, mutation := range map[string]struct {
		typeOf    reflect.Type
		forbidden []string
	}{
		"terminal": {reflect.TypeOf(ports.AttachedWorkerTerminalCommit{}), []string{"TerminalSequence"}},
		"cancel":   {reflect.TypeOf(ports.AttachedWorkerCancellationRequest{}), []string{"CancelRevision"}},
	} {
		for _, forbidden := range append([]string{"CommitMessage", "CancelMessage", "Outbound", "ProtocolSnapshot", "ProtocolConfig", "ExpectedAttemptRevision", "ExpectedConnectionRevision"}, mutation.forbidden...) {
			if _, found := mutation.typeOf.FieldByName(forbidden); found {
				t.Fatalf("%s mutation exposes caller-derived field %q", name, forbidden)
			}
		}
	}
	fence := reflect.TypeOf(ports.AttachedWorkerAttemptFence{})
	if _, found := fence.FieldByName("ExpectedAttemptRevision"); found {
		t.Fatal("deadline fence exposes an authority-like expected revision")
	}
	if _, found := fence.FieldByName("CandidateAttemptRevision"); !found {
		t.Fatal("deadline fence must retain an explicitly candidate-scoped stale-work guard")
	}
	result := reflect.TypeOf(ports.AttachedWorkerAttemptResult{})
	if _, found := result.FieldByName("Outbound"); !found {
		t.Fatal("store result must retain the authoritative durable outbound record")
	}
}
