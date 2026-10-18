package operationbody_test

import (
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Structural DATA plus original physical ownership only. No registered Store
// plan, current permission, completed cleanup or canonical commit is attested.
func canonicalTerminalData(t *testing.T) (domain.Run, domain.SessionEventDraft, []byte, domain.TerminalCustodyPlanV1, []byte) {
	t.Helper()
	capsule := []byte("physical capsule DATA")
	plan := terminalCapsulePlan(t, capsule)
	scope := bodyScope()
	at := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	run := domain.Run{ID: scope.RunID, TenantID: scope.TenantID, SessionID: scope.SessionID, TriggerEventID: "trigger", SubscriptionConnectionID: "connection", Status: domain.RunRunning, IdempotencyKey: "request", CreatedAt: at, UpdatedAt: at, TerminalCustody: &domain.RunTerminalCustodyV1{Version: 1, Plan: plan}}
	body := []byte(`{"schema":"sessionless.assistant-message.v1","summary":"physical answer"}`)
	draft, err := canonicaloutput.PlanEventDraftV1(run, "assistant", domain.SessionEventAssistantMessage, body, at)
	if err != nil || run.Validate() != nil {
		t.Fatal("valid structural test DATA", err, run.Validate())
	}
	return run, draft, body, plan, capsule
}

func TestTerminalCanonicalPhysicalOneUseAndExactReadback(t *testing.T) {
	for _, mode := range []string{"positive", "no_capsule", "bad_owned_bytes", "ambiguous_put", "bad_readback", "close_failure", "foreign_run"} {
		t.Run(mode, func(t *testing.T) {
			b := newBodyBlobs()
			store, owner := terminalOutputStore(t, b, 1, 1<<20)
			run, draft, body, plan, capsule := canonicalTerminalData(t)
			if mode != "no_capsule" {
				if err := owner.WriteCapsule(t.Context(), plan, capsule); err != nil {
					t.Fatal(err)
				}
			}
			before, _, _, _, _ := b.counts()
			call := t.Context()
			switch mode {
			case "bad_owned_bytes":
				call = &terminalMutatingContext{Context: call, mutate: func() { body[0] ^= 1 }}
			case "ambiguous_put":
				b.putFault = "lost_ack"
			case "bad_readback":
				b.openFault = "corrupt"
			case "close_failure":
				b.closeFault = true
			case "foreign_run":
				run.ID = "foreign-run"
			}
			err := owner.WriteCanonicalAssistant(call, run, draft, body)
			puts, opens, closes, _, _ := b.counts()
			if mode == "positive" {
				if err != nil || puts != 2 || opens != 2 || closes != 2 {
					t.Fatal("same uploader+readback positive", err, puts, opens, closes)
				}
				copyOwner := *owner
				if copyOwner.WriteCanonicalAssistant(t.Context(), run, draft, body) == nil {
					t.Fatal("copy retried original canonical slot")
				}
				if _, err := store.Write(t.Context(), []byte("canonical bytes do not debit ordinary body count")); err != nil {
					t.Fatal("independent canonical allowance changed ordinary body capacity", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid/uncertain canonical write accepted", mode)
			}
			if mode == "no_capsule" || mode == "bad_owned_bytes" || mode == "foreign_run" {
				if puts != before {
					t.Fatal("pre-PUT denial performed storage effects", mode, puts-before)
				}
			}
			if mode == "bad_owned_bytes" || mode == "ambiguous_put" || mode == "bad_readback" || mode == "close_failure" {
				b.putFault = ""
				b.openFault = ""
				b.closeFault = false
				if mode == "bad_owned_bytes" {
					body[0] ^= 1
				}
				copyOwner := *owner
				if copyOwner.WriteCanonicalAssistant(t.Context(), run, draft, body) == nil {
					t.Error("uncertain canonical retry", mode)
				}
				if _, err := store.Write(t.Context(), []byte("retry")); err == nil {
					t.Error("uncertain canonical write did not stop original ordinary writer", mode)
				}
				if after, _, _, _, _ := b.counts(); after != puts {
					t.Error("uncertainty retried storage", mode)
				}
			}
		})
	}
}
