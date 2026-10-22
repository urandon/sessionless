//go:build darwin || linux

package codexexec

import (
	"context"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestPreparedCompositionPublishesOnlyAuthenticClosedFailure(t *testing.T) {
	const start = "{\"type\":\"thread.started\"}\n{\"type\":\"turn.started\"}\n"
	const notice = "{\"type\":\"error\",\"message\":\"synthetic\"}\n"
	const failed = "{\"type\":\"turn.failed\",\"error\":{\"message\":\"synthetic\"}}\n"
	for _, tc := range []struct {
		name, protocol string
		closed         bool
	}{
		{name: "failed terminal", protocol: start + failed, closed: true},
		{name: "error then failed terminal", protocol: start + notice + failed, closed: true},
		{name: "partial final discarded", protocol: start + "{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"partial\"}}\n" + failed, closed: true},
		{name: "notification is not terminal", protocol: start + notice},
		{name: "lost reply is not terminal", protocol: start},
		{name: "frame after failure is drift", protocol: start + failed + notice},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner, invocation, owner, launcher := preparedCompositionFixture(t, tc.protocol)
			result, runErr := runner.Run(t.Context(), invocation)
			if runErr == nil || !result.CleanupComplete() || result.Succeeded(runErr) || launcher.prepareCount() != 1 {
				t.Fatalf("joined failed invocation: result=%+v err=%v preparations=%d", result, runErr, launcher.prepareCount())
			}
			if len(owner.order) != 4 || owner.order[2] != "writeback" || owner.order[3] != "release" {
				t.Fatalf("original credential finalization missing: %v", owner.order)
			}
			candidate, err := runner.ReceiptCandidate(invocation.Identity, result, runErr, domain.AttachedWorkerTerminalFailed)
			if tc.closed {
				if err != nil || candidate.FailureCode != "codex_exec_provider_failed" || candidate.Summary != "" || len(candidate.Artifacts) != 0 {
					t.Fatalf("closed-failure candidate=%+v err=%v", candidate, err)
				}
				if _, err := runner.ReceiptCandidate(invocation.Identity, result, runErr, domain.AttachedWorkerTerminalSucceeded); err == nil {
					t.Fatal("known native failure became a success candidate")
				}
				forged := result
				forged.Process.ExitCode++
				if _, err := runner.ReceiptCandidate(invocation.Identity, forged, runErr, domain.AttachedWorkerTerminalFailed); err == nil {
					t.Fatal("public process mutation replaced original outer result")
				}
				if err := runner.RetireReceipt(invocation.Identity); err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatalf("unclosed/drifted native result minted failed receipt: %+v", candidate)
			}
			if _, err := runner.Run(context.Background(), invocation); err == nil || launcher.prepareCount() != 1 {
				t.Fatalf("failed/unknown invocation replayed: err=%v preparations=%d", err, launcher.prepareCount())
			}
		})
	}
}
