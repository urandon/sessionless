package attachedworkerdaemontransport

import (
	"context"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestPreparedReceiptGlueRequiresFeatureAndOriginalConcreteStack(t *testing.T) {
	for _, name := range []string{"missing feature", "missing publisher", "caller builder", "fixed bridge"} {
		t.Run(name, func(t *testing.T) {
			config := Config{ReceiptPublisher: &receiptPublisherFixture{}}
			capability := attachedworkerprotocol.CapabilityManifestV1{Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureOutputReceipt}}
			var original attachedworkerdaemon.Runner = &runtimeRunner{}
			runner := &preflightRunner{current: &original}
			switch name {
			case "missing feature":
				capability.Features = nil
			case "missing publisher":
				config.ReceiptPublisher = nil
			case "caller builder":
				config.ReceiptCandidate = func(attachedworkerdaemon.InvocationIdentity, attachedworkerdaemon.InvocationResult, error, domain.AttachedWorkerTerminalStatus) (attachedworkeroutput.Candidate, error) {
					return attachedworkeroutput.Candidate{}, nil
				}
			}
			err := installPreparedReceipt(&config, capability, runner)
			if name != "fixed bridge" {
				if err == nil {
					t.Fatal("missing original receipt contract accepted")
				}
				return
			}
			if err != nil || config.preparedReceipt != runner || config.ReceiptCandidate == nil {
				t.Fatalf("fixed private glue: err=%v installed=%t", err, config.preparedReceipt == runner)
			}
			if _, err := config.ReceiptCandidate(attachedworkerdaemon.InvocationIdentity{}, attachedworkerdaemon.InvocationResult{}, nil, domain.AttachedWorkerTerminalSucceeded); err == nil {
				t.Fatal("arbitrary Runner forged concrete Stack semantic origin")
			}
		})
	}
}

func TestPinnedPreflightFreezesPreparedSelectionBeforeConnection(t *testing.T) {
	fixture, _, port, input := pinnedInitialFixture(t)
	prepared := &codexexec.Config{Enabled: true, Model: "original-model"}
	config := attachedworkerstack.Config{PreparedCodex: prepared, AllowedEnvironmentNames: []string{"SESSIONLESS_MODE"}}
	var selected string
	preflight, _, err := pinnedRuntimePreflight(input.CapabilityManifest, fixture.config.Profile, config, unusedInitialCredentials{},
		func(_ context.Context, _ attachedworkerlocal.ManifestV1, c attachedworkerstack.Config, _ ports.CredentialLifecycle) (attachedworkerdaemon.Runner, error) {
			selected = c.PreparedCodex.Model
			return &runtimeRunner{}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	prepared.Model = "mutated-model"
	if err := preflight(context.Background(), port.manifest); err != nil {
		t.Fatal(err)
	}
	if selected != "original-model" {
		t.Fatalf("prepared selection changed across preflight: got=%q", selected)
	}
}
