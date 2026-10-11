package runexplanation_test

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

func fixture(t *testing.T, attached bool) runexplanation.Snapshot {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	run := domain.Run{ID: "run-fixture", TenantID: "tenant-fixture", SessionID: "session-fixture", TriggerEventID: "event-trigger", SubscriptionConnectionID: "subscription-fixture", Status: domain.RunCreated, IdempotencyKey: "request-fixture", CreatedAt: at, UpdatedAt: at}
	attempt := domain.Attempt{ID: "attempt-fixture", TenantID: run.TenantID, RunID: run.ID, Number: 1, Status: domain.AttemptCreated, CreatedAt: at, UpdatedAt: at}
	placement := domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementManaged, FallbackPolicy: domain.ExecutionFallbackDenied, SubstrateBindingDigest: strings.Repeat("1", 64)}
	if attached {
		placement = domain.ExecutionPlacementV2{Version: domain.ExecutionPlacementVersionV2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: "owner-private", WorkerID: "worker-private", CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy")))}
	}
	head, changed, err := runexplanation.Select(nil, run, attempt, placement, at)
	if err != nil || !changed {
		t.Fatalf("create locator: changed=%v err=%v", changed, err)
	}
	return runexplanation.Snapshot{Run: run, ReadAt: at, Head: &head, Attempt: &attempt}
}

func transition(t *testing.T, s *runexplanation.Snapshot, status domain.RunStatus, delta time.Duration) {
	t.Helper()
	s.ReadAt = s.ReadAt.Add(delta)
	if err := s.Run.Transition(status, s.ReadAt); err != nil {
		t.Fatalf("transition to %s: %v", status, err)
	}
}

func recordAdmission(t *testing.T, s *runexplanation.Snapshot, code string) {
	t.Helper()
	next, changed, err := runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, code, s.ReadAt)
	if err != nil || !changed {
		t.Fatalf("record %s: changed=%v err=%v", code, changed, err)
	}
	s.Head = &next
}

func projection(t *testing.T, s runexplanation.Snapshot) runexplanation.Projection {
	t.Helper()
	got, err := runexplanation.Project(s)
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	return got
}

func TestAdmissionMappingClosed(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code string
		want runexplanation.AdmissionReason
	}{
		{"admitted", runexplanation.ReasonAdmitted}, {"already_admitted", runexplanation.ReasonAdmitted},
		{"subscription_reauthentication_required", runexplanation.SubscriptionAttentionRequired}, {"subscription_draining", runexplanation.CapacityDraining},
		{"provider_quota_blocked", runexplanation.QuotaResetPending}, {"provider_quota_exhausted_reset_unknown", runexplanation.QuotaExhaustedResetUnknown},
		{"runtime_limit_exceeded", runexplanation.RuntimeLimitExceeded}, {"turn_limit_exceeded", runexplanation.TurnLimitExceeded}, {"input_limit_exceeded", runexplanation.InputLimitExceeded},
		{"context_limit_exceeded", runexplanation.ContextLimitExceeded}, {"artifact_limit_exceeded", runexplanation.ArtifactLimitExceeded}, {"subscription_slot_busy", runexplanation.CapacityBusy},
		{"tenant_queue_depth_exhausted", runexplanation.WorkspaceQueueLimit}, {"tenant_active_run_limit", runexplanation.WorkspaceActiveRunLimit}, {"quota_blocked", ""}, {"provider-secret", ""}, {"dispatch_not_pending", ""},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			outcome, got := runexplanation.MapAdmission(tc.code)
			if got != tc.want {
				t.Errorf("code=%q got=%q want=%q", tc.code, got, tc.want)
			}
			if tc.want == "" && outcome != "" {
				t.Errorf("unsupported code has outcome=%s", outcome)
			}
		})
	}
}

func TestComputeChoiceAdmissionDenialsRecorded(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code string
		want runexplanation.AdmissionReason
	}{
		{code: "compute_choice_unavailable", want: "compute_unavailable"},
		{code: "compute_choice_stale", want: "choice_stale"},
		{code: "compute_choice_consent_required", want: "consent_required"},
		{code: "compute_choice_policy_denied", want: "compute_policy_denied"},
	}
	for _, tc := range cases {
		for _, attached := range []bool{false, true} {
			placement := "managed"
			if attached {
				placement = "attached"
			}
			t.Run(tc.code+"/"+placement, func(t *testing.T) {
				s := fixture(t, attached)
				transition(t, &s, domain.RunQuotaBlocked, time.Second)
				recordAdmission(t, &s, tc.code)
				got := projection(t, s).Admission
				if got.Availability != runexplanation.Recorded || got.Outcome != runexplanation.Denied || got.ReasonCode != tc.want || !got.ReasonCode.Valid() {
					t.Errorf("code=%q placement=%s admission=%+v want recorded denied %q", tc.code, placement, got, tc.want)
				}
				next, changed, err := runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, tc.code, s.ReadAt)
				if err != nil || changed || !reflect.DeepEqual(next, *s.Head) {
					t.Errorf("exact denial replay code=%q changed=%v err=%v", tc.code, changed, err)
				}
				// Public reasons are not aliases for canonical admission inputs.
				if outcome, reason := runexplanation.MapAdmission(string(tc.want)); outcome != "" || reason != "" {
					t.Errorf("public reason %q accepted as canonical decision: %q/%q", tc.want, outcome, reason)
				}
			})
		}
	}
}

func TestAdmissionReplayReasonChangeReadmission(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunQuotaBlocked, time.Second)
	recordAdmission(t, &s, "subscription_slot_busy")
	old := *s.Head
	oldRevision := s.Head.Revision
	next, changed, err := runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, "subscription_slot_busy", s.ReadAt)
	if err != nil || changed || !reflect.DeepEqual(next, *s.Head) {
		t.Fatalf("exact replay changed=%v err=%v got=%+v", changed, err, next)
	}
	recordAdmission(t, &s, "tenant_queue_depth_exhausted") // equal time and canonical status
	if got := projection(t, s).Admission; got.ReasonCode != runexplanation.WorkspaceQueueLimit || got.DecisionRevision != oldRevision+1 {
		t.Errorf("changed reason got=%+v", got)
	}
	if !reflect.DeepEqual(old.Admission.Reason, runexplanation.CapacityBusy) {
		t.Errorf("pure reducer mutated prior head: %+v", old)
	}
	next, changed, err = runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, "dispatch_not_pending", s.ReadAt)
	if err != nil || changed || !reflect.DeepEqual(next, *s.Head) {
		t.Errorf("dispatch observation overwrote admission: changed=%v err=%v", changed, err)
	}
	recordAdmission(t, &s, "future_code")
	if got := projection(t, s).Admission; got.Availability != runexplanation.Unknown || got.ObservedAt != nil || got.ReasonCode != "" {
		t.Errorf("unsupported supersession got=%+v", got)
	}
	transition(t, &s, domain.RunAdmitted, time.Second)
	recordAdmission(t, &s, "admitted")
	transition(t, &s, domain.RunFailed, time.Second)
	got := projection(t, s)
	if got.Admission.ReasonCode != runexplanation.ReasonAdmitted || got.Terminal.Availability != runexplanation.Unknown || got.Attempt.Status != domain.AttemptCreated {
		t.Errorf("independent admission/terminal/attempt got=%+v", got)
	}
}

func TestSelectAttemptResetsEvidence(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunQuotaBlocked, time.Second)
	recordAdmission(t, &s, "provider_quota_blocked")
	placement, _ := domain.ManagedExecutionPlacementV2(strings.Repeat("1", 64))
	attempt := *s.Attempt
	attempt.ID, attempt.Number = "attempt-new", 2
	next, changed, err := runexplanation.Select(s.Head, s.Run, attempt, placement, s.ReadAt)
	if err != nil || !changed || next.Admission != nil || next.Terminal != nil || next.SelectedAttemptID != attempt.ID {
		t.Fatalf("select reset got=%+v changed=%v err=%v", next, changed, err)
	}
	s.Head = &next
	if got := projection(t, s); got.Attempt.Availability != runexplanation.Unknown || got.Attempt.AttemptID != "" {
		t.Errorf("replaced exact Attempt got=%+v", got)
	}
	s.Attempt = &attempt
	if got := projection(t, s); got.Attempt.Number != 2 || got.Admission.Availability != runexplanation.Unknown {
		t.Errorf("new selected Attempt got=%+v", got)
	}
}

func TestTerminalNoticeValidation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		status       domain.RunStatus
		schema, code string
		cancelled    bool
		want         runexplanation.TerminalReason
		invalid      bool
	}{
		{"harness", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "harness_failed", false, runexplanation.ExecutionFailed, false},
		{"artifact", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "artifact_upload_failed", false, runexplanation.ResultPersistenceFailed, false},
		{"canonical_result", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "canonical_result_upload_failed", false, runexplanation.ResultPersistenceFailed, false},
		{"opaque_unclassified", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "new_failure_code", false, runexplanation.UnclassifiedFailure, false},
		{"cancelled", domain.RunCancelled, runexplanation.TerminalNoticeSchemaV1, "cancelled", true, runexplanation.CanonicalCancelled, false},
		{"before_start", domain.RunCancelled, runexplanation.TerminalNoticeSchemaV1, "cancelled_before_start", true, runexplanation.CanonicalCancelled, false},
		{"future_schema", domain.RunFailed, "v2", "harness_failed", false, "", true},
		{"flag", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "harness_failed", true, "", true},
		{"cancel_status", domain.RunCancelled, runexplanation.TerminalNoticeSchemaV1, "harness_failed", true, "", true},
		{"cancel_code_failed", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "cancelled", false, "", true},
		{"raw_text", domain.RunFailed, runexplanation.TerminalNoticeSchemaV1, "secret error text", false, "", true},
		{"nonterminal", domain.RunRunning, runexplanation.TerminalNoticeSchemaV1, "harness_failed", false, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, diag := runexplanation.MapTerminal(tc.status, runexplanation.NoticeInput{Schema: tc.schema, Code: tc.code, Cancelled: tc.cancelled})
			if got != tc.want || (diag != "") != tc.invalid {
				t.Errorf("input=%+v got=%s diag=%s", tc, got, diag)
			}
		})
	}
}

func TestTerminalExactReplayAndSuccessReset(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunFailed, time.Second)
	notice := runexplanation.NoticeInput{Schema: runexplanation.TerminalNoticeSchemaV1, Code: "harness_failed"}
	head, changed, diag, err := runexplanation.RecordTerminal(*s.Head, s.Run, *s.Attempt, &notice, "opaque-event-no-recipe", 77, s.ReadAt)
	if err != nil || !changed || diag != "" {
		t.Fatalf("record terminal changed=%v diag=%s err=%v", changed, diag, err)
	}
	s.Head = &head
	replay, changed, _, err := runexplanation.RecordTerminal(head, s.Run, *s.Attempt, &notice, "opaque-event-no-recipe", 77, s.ReadAt)
	if err != nil || changed || !reflect.DeepEqual(replay, head) {
		t.Errorf("terminal replay changed=%v err=%v", changed, err)
	}
	_, _, _, err = runexplanation.RecordTerminal(head, s.Run, *s.Attempt, &notice, "different-event", 78, s.ReadAt)
	if !errors.Is(err, runexplanation.ErrInvalidSource) {
		t.Errorf("conflicting replay err=%v", err)
	}
	if got := projection(t, s).Terminal; got.Availability != runexplanation.Recorded || got.EventID != "opaque-event-no-recipe" || got.EventSequence != 77 {
		t.Errorf("exact terminal locator got=%+v", got)
	}
	s.Run.Status = domain.RunSucceeded
	next, changed, _, err := runexplanation.RecordTerminal(head, s.Run, *s.Attempt, nil, "", 0, s.ReadAt)
	if err != nil || !changed || next.Terminal != nil {
		t.Errorf("success clears failure metadata got=%+v changed=%v err=%v", next, changed, err)
	}
}

func TestInvalidTerminalEvidenceRemainsUnknown(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunFailed, time.Second)
	notice := runexplanation.NoticeInput{Schema: "unsupported.v2", Code: "harness_failed"}
	head, changed, diag, err := runexplanation.RecordTerminal(*s.Head, s.Run, *s.Attempt, &notice, "exact-notice", 1, s.ReadAt)
	if err != nil || !changed || diag != runexplanation.InvalidTerminalNotice {
		t.Fatalf("invalid notice changed=%v diagnostic=%s err=%v", changed, diag, err)
	}
	s.Head = &head
	if got := projection(t, s); got.Terminal != (runexplanation.Terminal{Availability: runexplanation.Unknown}) || got.Attempt.Availability != runexplanation.Recorded || s.Run.Status != domain.RunFailed {
		t.Errorf("invalid notice changed authority or disclosed metadata: %+v", got)
	}
}

func TestAdmissionCannotOverwriteCanonicalTerminal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status domain.RunStatus
		notice runexplanation.NoticeInput
	}{
		{name: "failed", status: domain.RunFailed, notice: runexplanation.NoticeInput{Schema: runexplanation.TerminalNoticeSchemaV1, Code: "harness_failed"}},
		{name: "cancelled", status: domain.RunCancelled, notice: runexplanation.NoticeInput{Schema: runexplanation.TerminalNoticeSchemaV1, Code: "cancelled", Cancelled: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t, false)
			transition(t, &s, tc.status, time.Second)
			head, changed, _, err := runexplanation.RecordTerminal(*s.Head, s.Run, *s.Attempt, &tc.notice, "canonical-terminal", 19, s.ReadAt)
			if err != nil || !changed {
				t.Fatalf("record %s terminal: changed=%v err=%v", tc.status, changed, err)
			}
			s.Head = &head
			before := projection(t, s)
			for _, code := range []string{"future_code", "admitted", "subscription_slot_busy"} {
				next, changed, err := runexplanation.RecordAdmission(head, s.Run, *s.Attempt, code, s.ReadAt.Add(time.Second))
				if !errors.Is(err, runexplanation.ErrInvalidSource) || changed {
					t.Errorf("terminal=%s admission=%q changed=%v err=%v next=%+v; want rejected decision", tc.status, code, changed, err, next)
				}
				if got := projection(t, s); !reflect.DeepEqual(got, before) {
					t.Errorf("rejected admission mutated terminal evidence: got=%+v want=%+v", got, before)
				}
			}
			next, changed, err := runexplanation.RecordAdmission(head, s.Run, *s.Attempt, "dispatch_not_pending", s.ReadAt.Add(time.Second))
			if err != nil || changed || !reflect.DeepEqual(next, head) {
				t.Errorf("terminal dispatch observation changed=%v err=%v next=%+v; want unchanged canonical evidence", changed, err, next)
			}
		})
	}
}

func TestDenialDoesNotSurviveReenteredCanonicalPhase(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunQuotaBlocked, time.Second)
	recordAdmission(t, &s, "subscription_slot_busy")
	// Even a missing writer invalidation cannot revive this old denial just
	// because the Run happens to return to the same status.
	transition(t, &s, domain.RunAdmitted, time.Second)
	transition(t, &s, domain.RunQuotaBlocked, time.Second)
	if got := projection(t, s).Admission; got.Availability != runexplanation.Unknown || got.ReasonCode != "" {
		t.Errorf("stale denial revived from RunStatus: %+v", got)
	}
	recordAdmission(t, &s, "tenant_active_run_limit")
	if got := projection(t, s).Admission; got.ReasonCode != runexplanation.WorkspaceActiveRunLimit {
		t.Errorf("new explicit phase decision got=%+v", got)
	}
}

func TestInvalidAndMissingSourceBasis(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*runexplanation.Snapshot)
		fatal  bool
	}{
		{"no_historical_locator", func(s *runexplanation.Snapshot) { s.Head = nil }, false},
		{"no_attempt", func(s *runexplanation.Snapshot) { s.Attempt = nil }, false},
		{"foreign_locator", func(s *runexplanation.Snapshot) { s.Head.RunID = "other-run" }, false},
		{"foreign_attempt", func(s *runexplanation.Snapshot) { s.Attempt.RunID = "other-run" }, false},
		{"future_attempt", func(s *runexplanation.Snapshot) { s.Attempt.UpdatedAt = s.ReadAt.Add(time.Second) }, false},
		{"future_basis", func(s *runexplanation.Snapshot) { s.Head.Admission.Basis.ObservedAt = s.ReadAt.Add(time.Second) }, false},
		{"wrong_basis_version", func(s *runexplanation.Snapshot) { s.Head.Admission.Basis.Version = 99 }, false},
		{"wrong_basis_attempt", func(s *runexplanation.Snapshot) { s.Head.Admission.Basis.AttemptID = "other-attempt" }, false},
		{"wrong_basis_phase", func(s *runexplanation.Snapshot) { s.Head.Admission.Basis.RunStatus = domain.RunRunning }, false},
		{"future_revision", func(s *runexplanation.Snapshot) { s.Head.Admission.Revision = s.Head.Revision + 1 }, false},
		{"bad_replay_digest", func(s *runexplanation.Snapshot) { s.Head.Admission.Fingerprint = "bad" }, false},
		{"future_run", func(s *runexplanation.Snapshot) { s.Run.UpdatedAt = s.ReadAt.Add(time.Second) }, true},
		{"malformed_run", func(s *runexplanation.Snapshot) { s.Run.Status = "secret" }, true},
		{"oversized_record", func(s *runexplanation.Snapshot) {
			s.Head.Admission.Fingerprint = strings.Repeat("a", runexplanation.MaxProjectionBytes)
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t, false)
			transition(t, &s, domain.RunQuotaBlocked, time.Second)
			recordAdmission(t, &s, "subscription_slot_busy")
			tc.mutate(&s)
			got, err := runexplanation.Project(s)
			if tc.fatal {
				if err == nil {
					t.Errorf("corrupt input unexpectedly accepted: %+v", got)
				}
				return
			}
			if err != nil || got.Admission.Availability != runexplanation.Unknown || got.Admission.ReasonCode != "" || got.Admission.ObservedAt != nil {
				t.Errorf("invalid basis got=%+v err=%v", got, err)
			}
		})
	}
}

func TestReducerBoundsAndResetConsistency(t *testing.T) {
	t.Parallel()
	s := fixture(t, false)
	transition(t, &s, domain.RunQuotaBlocked, time.Second)
	recordAdmission(t, &s, "subscription_slot_busy")
	_, _, err := runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, "tenant_active_run_limit", s.ReadAt.Add(-time.Second))
	if err == nil {
		t.Error("stale source accepted")
	}
	s.Head.Revision = math.MaxUint64
	_, _, err = runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, "tenant_active_run_limit", s.ReadAt)
	if !errors.Is(err, runexplanation.ErrRevisionExhausted) {
		t.Errorf("revision exhaustion err=%v", err)
	}
	replay, changed, err := runexplanation.RecordAdmission(*s.Head, s.Run, *s.Attempt, "subscription_slot_busy", s.ReadAt)
	if err != nil || changed || replay.Revision != math.MaxUint64 {
		t.Errorf("exact replay at exhausted revision changed=%v err=%v", changed, err)
	}
	s.Head.Revision = 3
	next, changed, err := runexplanation.Invalidate(*s.Head, true, true)
	if err != nil || !changed || next.Admission != nil || next.Terminal != nil || next.Revision != 4 {
		t.Errorf("invalidate got=%+v changed=%v err=%v", next, changed, err)
	}
	_, changed, err = runexplanation.Invalidate(next, true, true)
	if err != nil || changed {
		t.Errorf("reset replay changed=%v err=%v", changed, err)
	}
}
