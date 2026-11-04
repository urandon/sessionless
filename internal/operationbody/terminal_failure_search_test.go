package operationbody_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Structural notices/capsule/prefix DATA plus the original physical owner.
// These are not authenticated operation tails, SQL/current permission or an
// actual terminal producer. Existing producer/capsule checks remain mandatory.
func searchFailurePhysicalData(t *testing.T, code string, prefix bool, rejectedModel bool) (domain.Run, []domain.SessionEventDraft, [][]byte, domain.TerminalCustodyPlanV1, []byte) {
	t.Helper()
	run, drafts, bodies, custody, capsule := canonicalChainData(t)
	notice := domain.SearchFailureNoticeV4{Schema: "sessionless.run-terminal-notice.v4", Code: code, Operation: domain.AttemptOperationDigestV1(strings.Repeat("a", 64)), Reservation: domain.AttemptOperationReservationDigestV1(strings.Repeat("b", 64)), Observation: domain.AttemptOperationObservationDigestV1(strings.Repeat("c", 64))}
	switch code {
	case domain.SearchConsentStoppedV4:
		notice.Execution, notice.Kind, notice.Outcome, notice.Reason = "not_executed", domain.AttemptOperationModelV1, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1
	case domain.SearchRejectedBeforeSendV4:
		notice.Execution, notice.Kind, notice.Outcome, notice.Reason = "not_sent", domain.AttemptOperationWebSearchV1, domain.AttemptOperationRejectedBeforeSendV1, domain.AttemptOperationReasonAuthorityDeniedV1
	case domain.SearchServiceErrorV4:
		notice.Execution, notice.Kind, notice.Outcome, notice.Reason = "known_service_error", domain.AttemptOperationWebSearchV1, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonServiceErrorV1
	case domain.SearchOutcomeUnknownV4:
		notice.Execution, notice.Kind, notice.Outcome, notice.Reason = "unknown", domain.AttemptOperationWebSearchV1, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1
	case domain.ModelAfterSearchFailureV4:
		notice.Execution, notice.Kind, notice.Outcome, notice.Reason = "unknown", domain.AttemptOperationModelV1, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonWorkerLostV1
		if rejectedModel {
			notice.Execution, notice.Outcome, notice.Reason = "not_sent", domain.AttemptOperationRejectedBeforeSendV1, domain.AttemptOperationReasonTransportUnknownV1
		}
	default:
		t.Fatal("unknown test notice code")
	}
	body, err := json.Marshal(notice)
	if err != nil || notice.Validate() != nil {
		t.Fatal("closed V4 notice DATA prerequisite", err)
	}
	if _, err := domain.DecodeSearchFailureNoticeV4(body); err != nil {
		t.Fatal("strict V4 codec prerequisite", err)
	}
	identity, err := canonicaloutput.EncodeFailureV1(run.ID, false, code)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(identity.Payload)
	draft, err := canonicaloutput.PlanEventDraftV1(run, identity.ID, domain.SessionEventSystemNotice, body, run.CreatedAt)
	if err != nil {
		t.Fatal("canonical notice draft prerequisite", err)
	}
	prepared := ports.PrepareCanonicalDraftV1(run, draft, body)
	if prepared == nil {
		t.Fatal("V4 notice material prerequisite")
	}
	prepared.Release()
	if !prefix {
		drafts = nil
		bodies = nil
	}
	if prefix {
		drafts = drafts[:len(drafts)-1]
		bodies = bodies[:len(bodies)-1]
	}
	drafts = append(drafts, draft)
	bodies = append(bodies, body)
	t.Cleanup(func() { clear(body) })
	return run, drafts, bodies, custody, capsule
}

func replaceSearchPhysicalNotice(t *testing.T, run domain.Run, drafts []domain.SessionEventDraft, bodies [][]byte, body []byte) {
	t.Helper()
	i := len(drafts) - 1
	draft, err := canonicaloutput.PlanEventDraftV1(run, drafts[i].ID, domain.SessionEventSystemNotice, body, run.CreatedAt)
	if err != nil {
		t.Fatal("coherent negative notice DATA", err)
	}
	drafts[i], bodies[i] = draft, body
	t.Cleanup(func() { clear(body) })
}

func TestTerminalSearchFailureV4RejectsClosedPlanBeforeEventIO(t *testing.T) {
	for _, fault := range []string{"singleton-other-tail", "singleton-legacy", "duplicate-code", "aliased-schema", "unknown-schema", "unknown-code", "crossed-execution", "private-display", "foreign-id", "wrong-kind", "body-mismatch", "missing-capsule", "foreign-scope", "cancelled-run", "oversize-noncanonical"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				s, owner := terminalOutputStore(t, b, 1, 1<<20)
				if _, err := s.Write(t.Context(), []byte("original retained body")); err != nil {
					t.Fatal(err)
				}
				refs, err := owner.SnapshotRetainedBodies(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
				if fault != "missing-capsule" {
					if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
						t.Fatal(err)
					}
				}
				admitted := true
				switch fault {
				case "singleton-other-tail":
					run, drafts, bodies, _, _ = searchFailurePhysicalData(t, domain.SearchOutcomeUnknownV4, false, false)
				case "singleton-legacy":
					notice, e := canonicaloutput.EncodeFailureV1(run.ID, false, domain.KnownMCPServiceErrorV1)
					if e != nil {
						t.Fatal(e)
					}
					replaceSearchPhysicalNotice(t, run, drafts, bodies, notice.Payload)
				case "duplicate-code":
					replaceSearchPhysicalNotice(t, run, drafts, bodies, bytes.Replace(bodies[0], []byte(`"code":`), []byte(`"code":"search_consent_stopped","code":`), 1))
				case "aliased-schema":
					replaceSearchPhysicalNotice(t, run, drafts, bodies, bytes.Replace(bodies[0], []byte(`"schema":`), []byte(`"Schema":`), 1))
				case "unknown-schema":
					replaceSearchPhysicalNotice(t, run, drafts, bodies, bytes.Replace(bodies[0], []byte(`.v4"`), []byte(`.v5"`), 1))
				case "unknown-code":
					replaceSearchPhysicalNotice(t, run, drafts, bodies, bytes.Replace(bodies[0], []byte(domain.SearchConsentStoppedV4), []byte("mcp_service_error"), 1))
				case "crossed-execution":
					replaceSearchPhysicalNotice(t, run, drafts, bodies, bytes.Replace(bodies[0], []byte(`"not_executed"`), []byte(`"unknown"`), 1))
				case "private-display":
					drafts[0].DisplayText = "private query must not enter a notice"
				case "foreign-id":
					drafts[0], err = canonicaloutput.PlanEventDraftV1(run, "another-event", domain.SessionEventSystemNotice, bodies[0], run.CreatedAt)
					if err != nil {
						t.Fatal(err)
					}
				case "wrong-kind":
					drafts[0].Kind = domain.SessionEventAssistantMessage
				case "body-mismatch":
					bodies[0][0] ^= 1
				case "foreign-scope":
					run.TerminalCustody.Plan.Scope.AttemptID = "foreign-attempt"
					admitted = false
				case "cancelled-run":
					at := run.CreatedAt
					run.CancellationRequestedAt = &at
					admitted = false
				case "missing-capsule":
					admitted = false
				case "oversize-noncanonical":
					body := bytes.Clone(bodies[0])
					body = append(body, bytes.Repeat([]byte(" "), int(operationbody.LocalTerminalFailureNoticeBytesV1)+1-len(body))...)
					replaceSearchPhysicalNotice(t, run, drafts, bodies, body)
				}
				before, _, _, _, _ := b.counts()
				if err := owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err == nil {
					t.Fatal("invalid V4 whole plan accepted")
				}
				if after, _, _, deletes, _ := b.counts(); after != before || deletes != 0 {
					t.Fatal("invalid plan performed event I/O", before, after, deletes)
				}
				if admitted {
					validRun, validDrafts, validBodies, _, _ := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
					alias := *owner
					if alias.WriteFailureEventsV1(t.Context(), validRun, validDrafts, validBodies) == nil || alias.WriteFailureNotice(t.Context(), validDrafts[0], validBodies[0]) == nil {
						t.Error("admitted invalid batch reset its protected slots")
					}
					if receipt, e := owner.FenceRetainedBodies(t.Context(), refs); e == nil || receipt != nil {
						t.Error("failed batch fabricated retained fence")
					}
				}
			})
		})
	}
}

func TestTerminalSearchFailureV4KeepsOriginalPrefixBounds(t *testing.T) {
	for _, mode := range []string{"exact", "encoded-first-over", "event-first-over", "mismatched-pair"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchServiceErrorV4, true, false)
				product, substrate := terminalOutputLimits(1, 1<<20)
				product.MaxToolEvents = 2
				product.MaxToolEventBytes = uint64(len(bodies[0]) + len(bodies[1]))
				switch mode {
				case "encoded-first-over":
					product.MaxToolEventBytes--
				case "event-first-over":
					product.MaxToolEvents = 1
				case "mismatched-pair":
					body := bytes.Replace(bodies[1], []byte(`"call-a"`), []byte(`"call-b"`), 1)
					var err error
					drafts[1], err = canonicaloutput.PlanEventDraftV1(run, drafts[1].ID, drafts[1].Kind, body, run.CreatedAt)
					if err != nil {
						t.Fatal(err)
					}
					bodies[1] = body
				}
				b := newBodyBlobs()
				s := retainedStore(t, b, retainedLimits())
				owner, err := s.AdmitTerminalOutput(t.Context(), product, substrate)
				if err != nil {
					t.Fatal("valid finite original product prerequisite", err)
				}
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
				before, _, _, _, _ := b.counts()
				err = owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies)
				if mode == "exact" {
					if err != nil {
						t.Fatal("exact original prefix bound rejected", err)
					}
				} else {
					if err == nil {
						t.Fatal("first-over or mismatched prefix accepted")
					}
					if after, _, _, _, _ := b.counts(); after != before {
						t.Fatal("invalid prefix partially published events")
					}
				}
			})
		})
	}
}

func TestTerminalSearchFailureV4UncertaintyCannotRetryOrFence(t *testing.T) {
	for _, mode := range []string{"lost-ack", "corrupt-readback", "close-failure"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				s, owner := terminalOutputStore(t, b, 1, 1<<20)
				if _, err := s.Write(t.Context(), []byte("retained original private body")); err != nil {
					t.Fatal(err)
				}
				refs, err := owner.SnapshotRetainedBodies(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "lost-ack":
					b.putFault = "lost_ack"
				case "corrupt-readback":
					b.openFault = "corrupt"
				case "close-failure":
					b.closeFault = true
				}
				before, _, _, _, _ := b.counts()
				if err := owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err == nil {
					t.Fatal("uncertain notice publication became success")
				}
				b.putFault, b.openFault, b.closeFault = "", "", false
				alias := *owner
				if alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies) == nil || alias.WriteFailureNotice(t.Context(), drafts[0], bodies[0]) == nil {
					t.Fatal("uncertain shared slots permitted retry")
				}
				if receipt, err := owner.FenceRetainedBodies(t.Context(), refs); err == nil || receipt != nil {
					t.Error("uncertain notice produced a retained-fence receipt")
				}
				if puts, _, _, deletes, _ := b.counts(); puts != before+1 || deletes != 0 {
					t.Error("uncertainty caused another effect or erasure", puts, deletes)
				}
			})
		})
	}
}

func TestTerminalSearchFailureV4SingletonSharesOriginalGate(t *testing.T) {
	for _, failureActive := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary-active", true: "failure-active"}[failureActive], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				s, owner := terminalOutputStore(t, b, 1, 1<<20)
				run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal(err)
				}
				release, done, joined := make(chan struct{}), make(chan error, 1), make(chan struct{})
				b.blockPut = release
				ctx, cancel := context.WithCancel(t.Context())
				var once sync.Once
				t.Cleanup(func() {
					cancel()
					once.Do(func() { close(release) })
					synctest.Wait()
					select {
					case <-joined:
					default:
						t.Error("original publication not joined")
					}
				})
				go func() {
					defer close(joined)
					if failureActive {
						done <- owner.WriteFailureEventsV1(ctx, run, drafts, bodies)
					} else {
						_, err := s.Write(ctx, []byte("ordinary gate holder"))
						done <- err
					}
				}()
				synctest.Wait()
				alias := *owner
				before, _, _, _, _ := b.counts()
				if err := alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies); !errors.Is(err, operationbody.ErrBusy) {
					t.Error("singleton bypassed original active gate", err)
				}
				if after, _, _, _, _ := b.counts(); after != before {
					t.Error("busy alias performed I/O")
				}
				once.Do(func() { close(release) })
				synctest.Wait()
				if err := <-done; err != nil {
					t.Fatal("original gate holder", err)
				}
				if !failureActive {
					if err := alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err != nil {
						t.Fatal("busy loser burned still-unused singleton", err)
					}
				}
				if err := owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err == nil {
					t.Error("singleton alias refilled publication slots")
				}
			})
		})
	}
}

func TestTerminalSearchFailureV4CancellationBurnsOriginalSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		s, owner := terminalOutputStore(t, b, 1, 1<<20)
		if _, err := s.Write(t.Context(), []byte("retained cancellation DATA")); err != nil {
			t.Fatal(err)
		}
		refs, err := owner.SnapshotRetainedBodies(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
		if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
			t.Fatal(err)
		}
		release, done, joined := make(chan struct{}), make(chan error, 1), make(chan struct{})
		b.blockPut = release
		ctx, cancel := context.WithCancel(t.Context())
		var once sync.Once
		t.Cleanup(func() {
			cancel()
			once.Do(func() { close(release) })
			synctest.Wait()
			select {
			case <-joined:
			default:
				t.Error("cancelled publication not joined")
			}
		})
		before, _, _, _, _ := b.counts()
		go func() { defer close(joined); done <- owner.WriteFailureEventsV1(ctx, run, drafts, bodies) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		if err := <-done; err == nil {
			t.Fatal("cancelled physical write became success")
		}
		alias := *owner
		if alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies) == nil || alias.WriteFailureNotice(t.Context(), drafts[0], bodies[0]) == nil {
			t.Error("cancelled original slots reopened")
		}
		if receipt, err := owner.FenceRetainedBodies(t.Context(), refs); err == nil || receipt != nil {
			t.Error("cancelled publication produced a retained fence")
		}
		if puts, _, _, deletes, _ := b.counts(); puts != before+1 || deletes != 0 {
			t.Error("cancellation repeated an event or erased retained material", puts, deletes)
		}
	})
}

func TestTerminalSearchFailureV4SingletonDoesNotWidenSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBodyBlobs()
		_, owner := terminalOutputStore(t, b, 1, 1<<20)
		run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, domain.SearchConsentStoppedV4, false, false)
		if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
			t.Fatal(err)
		}
		before, _, _, _, _ := b.counts()
		if owner.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies) == nil {
			t.Fatal("singleton failure widened success batch language")
		}
		if after, _, _, _, _ := b.counts(); after != before {
			t.Fatal("denied singleton success performed I/O")
		}
		if err := owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err != nil {
			t.Fatal("pre-admission shape denial consumed original failure slot", err)
		}
	})
}

func TestTerminalSearchFailureV4PhysicalPublication(t *testing.T) {
	for _, tc := range []struct {
		name, code            string
		prefix, rejectedModel bool
	}{
		{"first-consent-stop", domain.SearchConsentStoppedV4, false, false},
		{"later-consent-stop", domain.SearchConsentStoppedV4, true, false},
		{"rejected-before-send", domain.SearchRejectedBeforeSendV4, true, false},
		{"known-service-error", domain.SearchServiceErrorV4, true, false},
		{"outcome-unknown", domain.SearchOutcomeUnknownV4, true, false},
		{"model-after-search-unknown", domain.ModelAfterSearchFailureV4, true, false},
		{"model-after-search-rejected", domain.ModelAfterSearchFailureV4, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := newBodyBlobs()
				s, owner := terminalOutputStore(t, b, 1, 1<<20)
				if _, err := s.Write(t.Context(), []byte("original retained request DATA")); err != nil {
					t.Fatal(err)
				}
				refs, err := owner.SnapshotRetainedBodies(t.Context())
				if err != nil || len(refs) != 1 {
					t.Fatal("original retained inventory prerequisite", err)
				}
				run, drafts, bodies, custody, capsule := searchFailurePhysicalData(t, tc.code, tc.prefix, tc.rejectedModel)
				if err := owner.WriteCapsule(t.Context(), custody, capsule); err != nil {
					t.Fatal("original capsule physical prerequisite", err)
				}
				before, _, _, _, _ := b.counts()
				if err := owner.WriteFailureEventsV1(t.Context(), run, drafts, bodies); err != nil {
					t.Fatalf("valid strict V4 physical batch rejected (%d events): %v", len(drafts), err)
				}
				for i, draft := range drafts {
					if !bytes.Equal(b.objects[draft.Payload.Key], bodies[i]) {
						t.Errorf("stored event%d differs from exact notice/prefix DATA", i)
					}
				}
				if puts, _, _, deletes, _ := b.counts(); puts != before+len(drafts) || deletes != 0 {
					t.Fatal("physical publication count/retention changed", puts, deletes)
				}
				alias := *owner
				last := len(drafts) - 1
				if alias.WriteFailureEventsV1(t.Context(), run, drafts, bodies) == nil || alias.WriteFailureNotice(t.Context(), drafts[last], bodies[last]) == nil || alias.WriteCanonicalEventsV2(t.Context(), run, drafts, bodies) == nil {
					t.Error("shared batch/notice slots reopened")
				}
				receipt, err := owner.FenceRetainedBodies(t.Context(), refs)
				if err != nil || owner.VerifyRetainedBodies(receipt) != nil {
					t.Fatal("V4 failure did not consume both original slots for retained fencing", err)
				}
				if _, err := s.Write(t.Context(), []byte("after original fence")); err == nil {
					t.Error("fenced ordinary writer was reopened")
				}
			})
		})
	}
}
