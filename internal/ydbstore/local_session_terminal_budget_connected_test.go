package ydbstore_test

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
)

// Original factory/Source/Store/issuer/body adapter and actual owned socket;
// SQL/storage/funding remain fixtures. This proves the generic first-MODEL
// Store budget boundary, not prospective encoded capsule-format admission.
func TestLocalSessionTerminalBudgetGenericFirstModelBoundary(t *testing.T) {
	for _, over := range []bool{false, true} {
		name := "exact_effective_body_limit"
		if over {
			name = "one_over_zero_send"
		}
		t.Run(name, func(t *testing.T) {
			peer := newLocalLoopbackPeer(t, !over)
			session, seed, _ := newFactorySession(t, peer)
			parent, err := session.Start(t.Context(), seed.Request)
			if err != nil {
				t.Fatal("genuine original entry", err)
			}
			ref, err := session.WriteModelRequest(t.Context(), seed.Profile.Limits, sessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			op := firstSessionOperation(t, seed, parent, ref)
			bodyLimit := parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxAggregateBodyBytes - operationbody.LocalTerminalCapsuleBytesV1
			// Existing setup accounting prepays original MCP/search grant
			// custody (4KiB each); it is not part of this MODEL's debit.
			setup := uint64(0)
			if parent.Authority.AdmissionCostCeiling.MCPToolCost != nil {
				setup += 4 << 10
			}
			if parent.Authority.AdmissionCostCeiling.WebSearchCost != nil {
				setup += 4 << 10
			}
			op.ResultBytes = bodyLimit - setup - op.RequestBytes
			if over {
				op.ResultBytes++
			}
			op.Debit.BodyBytes, op.Debit.ResponseBytes = op.RequestBytes+op.ResultBytes, op.ResultBytes
			if err := op.ValidateForParent(parent.Authority, parent.Reservation); err != nil {
				t.Fatalf("causal control requires valid original-domain debit: %v", err)
			}
			result, err := session.InvokeFirstModel(t.Context(), op, seed.Profile.Limits, sessionRequest())
			if over {
				if err == nil || result.SendAttempted || seed.Stats().Operation != 0 || seed.Stats().Call != 0 {
					t.Fatalf("first-over shared body budget sent/reserved: err=%v attempted=%v stats=%+v", err, result.SendAttempted, seed.Stats())
				}
				observed := peer.waitResult(t)
				if observed.err != nil || len(observed.body) != 0 || len(observed.extra) != 0 {
					t.Fatalf("first-over emitted application bytes before real EOF: bytes=%d err=%v", len(observed.body), observed.err)
				}
			} else if err != nil || !result.SendAttempted || result.Observation.Observation.Outcome != domain.AttemptOperationCompletedV1 || seed.Stats().Operation != 1 {
				t.Fatalf("exact original budget failed actual send: err=%v attempted=%v stats=%+v", err, result.SendAttempted, seed.Stats())
			}
			if err := session.Close(); err != nil {
				t.Fatal("bounded original cleanup", err)
			}
		})
	}
}
