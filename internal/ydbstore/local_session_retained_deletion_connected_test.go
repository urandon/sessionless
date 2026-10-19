package ydbstore_test

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Actual original MODEL/compaction/Publish/cleanup/Complete, followed by the
// historical body reader with the producer Source and session closed. This is
// NOT Start/Delete/resume coverage: destructive lifecycle guards stay closed.
func TestLocalSessionRetainedDeletionHistoricalBodiesConnected(t *testing.T) {
	for _, mode := range []string{"A0", "A2"} {
		t.Run(mode, func(t *testing.T) {
			session, seed, completed, _, blobs := newCompletionSession(t, mode == "A2")
			steps := 1
			if mode == "A2" {
				steps = 3
			}
			for i := 0; i < steps; i++ {
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
					t.Fatalf("original producing model step=%d err=%v", i, err)
				}
			}
			h, _ := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal("original retained output publication", err)
			}
			cleanup, err := h.FinishCleanup(t.Context())
			if err != nil || cleanup == nil {
				t.Fatal("original stopped/erased cleanup", err)
			}
			completed.ObserveCompletion()
			if err := cleanup.Complete(t.Context()); err != nil {
				t.Fatal("original canonical Complete", err, completed.Trace())
			}
			state := completed.Snapshot(t)
			if err := session.Close(); err != nil {
				t.Fatal("close original producer", err)
			}
			if err := seed.Source.Close(); err != nil {
				t.Fatal("close original Source", err)
			}
			mutations := []string{"positive", "capture_positive", "start_positive", "start_changed_metadata", "start_lost_membership", "start_new_hold", "start_foreign_admission", "resume_changed_metadata", "complete_changed_index", "complete_extra_child", "missing_anchor_owner", "mismatched_anchor_owner", "changed_seal", "writer_positive", "writer_extra_counts", "writer_extra_call", "writer_changed_call", "writer_query_error", "writer_close_error", "writer_capsule_mismatch"}
			mutations = append(mutations, "service_positive", "service_lost_ack", "service_no_reader")
			if mode == "A2" {
				mutations = append(mutations, "missing_retained_owner")
			}
			for _, mutation := range mutations {
				t.Run(mutation, func(t *testing.T) {
					refs, err := ydbstore.InspectRetainedDeletionBodiesV1(t, seed, completed, blobs, mutation)
					if mutation != "positive" && mutation != "writer_positive" && mutation != "capture_positive" && mutation != "start_positive" && mutation != "service_positive" && mutation != "service_lost_ack" {
						if err == nil || len(refs) != 0 {
							t.Fatal("changed original DATA accepted", mutation, err)
						}
						return
					}
					if err != nil {
						t.Fatal("genuine closed-producer body verification", err)
					}
					want := []domain.BlobRef{state.Run.TerminalCustody.Plan.Capsule}
					if state.Run.DerivedContext != nil {
						for _, plan := range state.Run.DerivedContext.Inventory {
							want = append(want, plan.Summary, plan.Manifest)
						}
					}
					notice, err := canonicaloutput.EncodeFailureV1(state.Run.ID, true, "cancelled")
					if err != nil {
						t.Fatal(err)
					}
					draft, err := canonicaloutput.PlanEventDraftV1(state.Run, notice.ID, notice.Kind, notice.Payload, state.Run.UpdatedAt)
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, draft.Payload)
					if len(refs) != len(want) {
						t.Fatalf("ALL registered Run refs: got=%d want=%d", len(refs), len(want))
					}
					for i := range want {
						if refs[i] != want[i] {
							t.Error("original exact capsule/pair/notice mismatch", i)
						}
					}
				})
			}
		})
	}
}
