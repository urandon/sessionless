//go:build darwin || linux

package attachedworkerstack_test

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	transport "gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
)

// This materializer must not run during configuration or original-owner bind.
type uncalledPreparedMaterializer struct{ t *testing.T }

func (m uncalledPreparedMaterializer) Materialize(context.Context, transport.MaterializationRequestV1) (transport.MaterializedInputV1, error) {
	m.t.Error("prepared adapter construction materialized an attempt")
	return transport.MaterializedInputV1{}, transport.ErrInvalidConfiguration
}

func TestPreparedAdapterUsesOriginalSessionAndOneShotStack(t *testing.T) {
	for _, invalid := range []string{"none", "executable", "digest", "arguments", "environment", "capability", "publisher", "caller-builder"} {
		t.Run(invalid, func(t *testing.T) {
			stack, session, invocation, capability := attachedworkerstack.PreparedAdapterFixtureForTest(t)
			before := session.Snapshot()
			original := transport.Config{Profile: transport.LocalProfileV1{Name: "prepared-adapter-fixture", CapabilityDigest: before.CapabilityDigest,
				Executable: invocation.Process.Executable, ExecutableDigest: invocation.Process.ExecutableDigest, Arguments: append([]string(nil), invocation.Process.Arguments...)},
				CapabilityManifest: &capability, ReceiptPublisher: &preparedReceiptPublisher{}, MaterializationRoot: t.TempDir(),
				Now: func() time.Time { return invocation.VerifiedInput.AcceptedAttempt.CreatedAt }}
			config := original
			switch invalid {
			case "executable":
				config.Profile.Executable += ".foreign"
			case "digest":
				config.Profile.ExecutableDigest[0]++
			case "arguments":
				config.Profile.Arguments = append([]string(nil), config.Profile.Arguments...)
				config.Profile.Arguments[0] = "foreign"
			case "capability":
				config.CapabilityManifest = nil
			case "environment":
				config.Profile.Environment = []attachedworkerdaemon.EnvironmentVariable{{Name: "FOREIGN_MODE", Value: "cannot-bypass-stack-allowlist"}}
			case "publisher":
				config.ReceiptPublisher = nil
			case "caller-builder":
				config.ReceiptCandidate = stack.ReceiptCandidate
			}
			adapter, err := transport.NewPreparedAdapter(session, uncalledPreparedMaterializer{t}, config, stack)
			if invalid != "none" {
				if err == nil || adapter != nil {
					t.Fatalf("%s accepted foreign configuration: adapter=%t err=%v", invalid, adapter != nil, err)
				}
				// A rejected configuration must not consume the original bind.
				adapter, err = transport.NewPreparedAdapter(session, uncalledPreparedMaterializer{t}, original, stack)
			}
			if err != nil || adapter == nil {
				t.Fatalf("original prepared assembly after %s: adapter=%t err=%v", invalid, adapter != nil, err)
			}
			after := session.Snapshot()
			if after.ConnectionID != before.ConnectionID || after.ConnectionGeneration != before.ConnectionGeneration || after.State != before.State {
				t.Fatal("post-connect assembly replaced or advanced the original Session")
			}
			if second, err := transport.NewPreparedAdapter(session, uncalledPreparedMaterializer{t}, original, stack); err == nil || second != nil {
				t.Fatalf("second owner acquired the same prepared Stack: adapter=%t err=%v", second != nil, err)
			}
		})
	}
}

func TestPreparedAdapterRejectsMissingOriginalOwners(t *testing.T) {
	if a, err := transport.NewPreparedAdapter(nil, uncalledPreparedMaterializer{t}, transport.Config{}, &attachedworkerstack.Stack{}); err == nil || a != nil {
		t.Fatalf("missing Session accepted: adapter=%t err=%v", a != nil, err)
	}
	stack, session, _, _ := attachedworkerstack.PreparedAdapterFixtureForTest(t)
	if a, err := transport.NewPreparedAdapter(session, uncalledPreparedMaterializer{t}, transport.Config{}, nil); err == nil || a != nil {
		t.Fatalf("missing Stack accepted: adapter=%t err=%v", a != nil, err)
	}
	if _, err := stack.PreparedInlineOutputV2("", [32]byte{}, nil, nil, attachedworkerprotocol.CapabilityManifestV1{}); err == nil {
		t.Fatal("foreign profile returned original output selection")
	}
}
