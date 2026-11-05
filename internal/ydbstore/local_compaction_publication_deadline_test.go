package ydbstore

import (
	"bytes"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is a causal in-process window experiment, not a reproduction of an
// unrecorded CI timestamp or a live-model test. The original Source, Store,
// issuer, reservation, completed observation, retained uploads and head commit
// execute normally over controlled SQL/blob DATA. Virtual time advances only
// to inject a known retained-object latency AFTER the actual head commit.
func TestLocalCompactionPublicationOriginalDeadlineAfterHead(t *testing.T) {
	for _, tc := range []struct {
		name          string
		window, delay time.Duration
		wantNext      bool
	}{
		{name: "five_second_live_control", window: 5 * time.Second, wantNext: true},
		{name: "five_second_post_head_expiry", window: 5 * time.Second, delay: 6 * time.Second},
		{name: "twenty_second_live_control", window: 20 * time.Second, wantNext: true},
		{name: "twenty_second_same_post_head_delay", window: 20 * time.Second, delay: 6 * time.Second, wantNext: true},
		{name: "twenty_second_post_head_expiry", window: 20 * time.Second, delay: 21 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f, g, engine := compactionPublicationFixture(t, localRuntimeActualPrebuild{networkOperation: tc.window})
				// Sample the SAME virtual wall clock for every later scripted DB
				// transaction, as the original issuer/Source already do. Do not
				// freeze/backdate current authority while advancing the blob clock.
				f.queries.sampleWallClock = true
				original := g.operation.Clone()
				reservation := g.reservation.Clone()
				observation := g.observation.Clone()
				if original.Kind != domain.AttemptOperationCompactionV1 || original.Debit.ActiveDuration != tc.window || !original.Deadline.Equal(reservation.ReservedAt.Add(tc.window)) || f.actual.config.Limits.OperationTimeout != tc.window || f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxNetworkOperationDuration != tc.window || f.entry.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxActiveDuration != 120*time.Second {
					t.Fatal("pre-intake window was not sealed into the original operation/debit")
				}
				if observation.Observation.Outcome != domain.AttemptOperationCompletedV1 || observation.Observation.ValidateForReservation(reservation) != nil || len(engine.operations) != 1 || engine.observationWrites != 0 {
					// The publication SQL wrapper starts after the real observation
					// writer. Its durable inherited row must already be complete.
					// No hypothetical summary/head is seeded by this experiment.
					t.Fatal("actual recorded compaction prerequisite missing")
				}
				beforeRun := publicationRun(t, f)
				beforeEvents := cloneGenerationRows(f.queries.events)
				beforeOperations := cloneGenerationRows(engine.operations)
				entered := 0
				var atHead, afterRead time.Time
				var barrier func()
				barrier = func() {
					if engine.headWrites == 0 {
						f.canonical.beforeOpen = barrier
						return
					}
					entered++
					run := publicationRun(t, f)
					if entered != 1 || engine.headWrites != 1 || g.publication == nil || run.DerivedContext == nil || run.DerivedContext.Head == nil || !domain.EqualRunDerivedContextV1(run.DerivedContext, &g.publication.postimage) || !time.Now().Before(original.Deadline) {
						t.Fatal("delay did not start at the genuine live committed-head read")
					}
					atHead = time.Now()
					time.Sleep(tc.delay) // explicit virtual I/O latency, never readiness
					afterRead = time.Now()
				}
				f.canonical.beforeOpen = barrier
				next, err := publishLocalCompaction(t.Context(), g)
				if entered != 1 || !afterRead.Equal(atHead.Add(tc.delay)) || !reflect.DeepEqual(g.operation, original) || !reflect.DeepEqual(g.reservation, reservation) || !reflect.DeepEqual(g.observation, observation) {
					t.Fatal("publication changed a sealed deadline/fact or did not reach the post-head delay")
				}
				if tc.wantNext {
					if err != nil || next == nil || !afterRead.Before(original.Deadline) {
						t.Fatalf("live original window denied next request: window=%s delay=%s remaining=%s err=%v", tc.window, tc.delay, original.Deadline.Sub(afterRead), err)
					}
					again, readErr := readPublishedLocalCompaction(t.Context(), g)
					if readErr != nil || again == nil || again.Digest() != next.Digest() || !bytes.Equal(again.Body(), next.Body()) {
						t.Fatalf("live exact original retained readback differs: %v", readErr)
					}
				} else {
					if err == nil || next != nil || afterRead.Before(original.Deadline) {
						t.Fatalf("expired original window produced next request: window=%s delay=%s remaining=%s err=%v", tc.window, tc.delay, original.Deadline.Sub(afterRead), err)
					}
					if repeated, readErr := readPublishedLocalCompaction(t.Context(), g); readErr == nil || repeated != nil {
						t.Fatal("expired original operation gained permission through a fresh reader")
					}
				}
				after := publicationRun(t, f)
				if engine.headWrites != 1 || f.sql.registrationWrites != 2 || f.canonical.retainedPuts != 2 || f.canonical.premature || after.DerivedContext == nil || len(after.DerivedContext.Inventory) != 1 || after.DerivedContext.Head == nil || !domain.EqualRunDerivedContextV1(after.DerivedContext, &g.publication.postimage) || !reflect.DeepEqual(beforeOperations, engine.operations) || !reflect.DeepEqual(beforeEvents, f.queries.events) {
					t.Fatal("post-head outcome hid retained facts or repeated writes/observations")
				}
				base := after.Clone()
				base.DerivedContext = beforeRun.DerivedContext
				if !reflect.DeepEqual(base, beforeRun) {
					t.Fatal("post-head experiment changed the canonical Run outside its head/inventory")
				}
				if repeated, repeatErr := publishLocalCompaction(t.Context(), g); repeatErr == nil || repeated != nil || engine.headWrites != 1 || f.canonical.retainedPuts != 2 {
					t.Fatal("a retry regained physical publication ownership")
				}
				t.Logf("original window=%s retained-read delay=%s deadline remaining=%s completed=1 head=1 uploads=2 next=%t", tc.window, tc.delay, original.Deadline.Sub(afterRead), tc.wantNext)
			})
		})
	}
}
