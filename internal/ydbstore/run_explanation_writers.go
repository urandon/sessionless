package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

func readExplanationHeadTx(ctx context.Context, tx *stateTx, id domain.RunID) (*runexplanation.HeadV1, error) {
	head, found, err := readExplanationJSON[runexplanation.HeadV1](ctx, explanationQueryTx(tx),
		`SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, tx.tenantID, id)
	if err != nil || !found {
		return nil, err
	}
	if err := head.Validate(); err != nil {
		return nil, err
	}
	return &head, nil
}

func writeExplanationHeadTx(ctx context.Context, tx *stateTx, head runexplanation.HeadV1) error {
	if err := head.Validate(); err != nil {
		return err
	}
	encoded, err := json.Marshal(head)
	if err != nil {
		return runexplanation.ErrCorruptProjection
	}
	_, err = tx.sqlTx.ExecContext(ctx, `UPSERT INTO run_explanation_heads_v1 (tenant_id,run_id,revision,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, head.TenantID, head.RunID, head.Revision, string(encoded))
	return err
}

// Selection is owned exclusively by ingress/admission, never by a read or an
// arbitrary lifecycle write. Historical rows are not repaired here.
func selectExplanationTx(ctx context.Context, tx *stateTx, run domain.Run, attempt domain.Attempt, placement domain.ExecutionPlacementV2, at time.Time) error {
	head, err := readExplanationHeadTx(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	if head != nil && head.SelectedAttemptID != attempt.ID {
		previous, found, err := tx.GetAttempt(ctx, head.SelectedAttemptID)
		if err != nil {
			return err
		}
		if found && attempt.Number <= previous.Number {
			return runexplanation.ErrStaleObservation
		}
	}
	next, changed, err := runexplanation.Select(head, run, attempt, placement, at)
	if err != nil || !changed {
		return err
	}
	return writeExplanationHeadTx(ctx, tx, next)
}

func recordAdmissionExplanationTx(ctx context.Context, tx *stateTx, run domain.Run, attempt domain.Attempt, placement domain.ExecutionPlacementV2, code string, at time.Time) error {
	if code == "dispatch_not_pending" {
		return nil
	}
	if err := selectExplanationTx(ctx, tx, run, attempt, placement, at); err != nil {
		return err
	}
	head, err := readExplanationHeadTx(ctx, tx, run.ID)
	if err != nil {
		return err
	}
	next, changed, err := runexplanation.RecordAdmission(*head, run, attempt, code, at)
	if err != nil || !changed {
		return err
	}
	return writeExplanationHeadTx(ctx, tx, next)
}

func invalidateRunExplanationTx(ctx context.Context, tx *stateTx, run domain.Run) error {
	head, err := readExplanationHeadTx(ctx, tx, run.ID)
	if err != nil || head == nil {
		return err
	}
	admission := head.Admission != nil && ((head.Admission.Outcome == runexplanation.Denied && (run.Status != domain.RunQuotaBlocked || !head.Admission.Basis.RunUpdatedAt.Equal(run.UpdatedAt))) || (head.Admission.Outcome == runexplanation.Admitted && (run.Status == domain.RunCreated || run.Status == domain.RunQuotaBlocked)))
	terminal := head.Terminal != nil && (head.Terminal.Basis.RunStatus != run.Status || !head.Terminal.Basis.RunUpdatedAt.Equal(run.UpdatedAt))
	next, changed, err := runexplanation.Invalidate(*head, admission, terminal)
	if err != nil || !changed {
		return err
	}
	return writeExplanationHeadTx(ctx, tx, next)
}

func invalidateAttemptExplanationTx(ctx context.Context, tx *stateTx, run domain.Run, attempt domain.Attempt) error {
	head, err := readExplanationHeadTx(ctx, tx, run.ID)
	if err != nil || head == nil || head.SelectedAttemptID == attempt.ID {
		return err
	}
	// An arbitrary Attempt write cannot select that Attempt or create evidence.
	// Older historical Attempt writes also cannot erase the current selector.
	selected, found, err := tx.GetAttempt(ctx, head.SelectedAttemptID)
	if err != nil || !found || attempt.Number <= selected.Number {
		return err
	}
	next, changed, err := runexplanation.Invalidate(*head, true, true)
	if err != nil || !changed {
		return err
	}
	return writeExplanationHeadTx(ctx, tx, next)
}

func recordTerminalExplanationTx(ctx context.Context, tx *stateTx, run domain.Run, input *runexplanation.NoticeInput, eventID domain.SessionEventID, sequence uint64, at time.Time) error {
	head, err := readExplanationHeadTx(ctx, tx, run.ID)
	if err != nil || head == nil {
		return err
	}
	attempt, found, err := tx.GetAttempt(ctx, head.SelectedAttemptID)
	if err != nil || !found {
		return err
	}
	// Legacy/unsupported finalizers have no notice metadata: leave unknown.
	if run.Status != domain.RunSucceeded && input == nil {
		return nil
	}
	next, changed, diagnostic, err := runexplanation.RecordTerminal(*head, run, attempt, input, eventID, sequence, at)
	if diagnostic != "" {
		slog.WarnContext(ctx, "run explanation diagnostic", "code", string(diagnostic))
	}
	if err != nil || !changed {
		return err
	}
	return writeExplanationHeadTx(ctx, tx, next)
}

func canonicalFailureNotice(code string, cancelled bool) *runexplanation.NoticeInput {
	return &runexplanation.NoticeInput{Schema: runexplanation.TerminalNoticeSchemaV1, Code: code, Cancelled: cancelled}
}

// Exact schema/code/flag are supplied by the owning finalization, then bound
// to the immutable event payload hash. No blob lookup, reconstructed ID or
// changed finalization/signed receipt digest is needed. Unsupported old
// payloads stay unknown; a durable replay leaves its original evidence intact.
func canonicalNoticeMatchesDraft(input *runexplanation.NoticeInput, draft domain.SessionEventDraft) bool {
	if input == nil || draft.Kind != domain.SessionEventSystemNotice || len(input.Schema) > 160 || domain.ValidateOpaqueID("notice_code", input.Code) != nil {
		return false
	}
	encoded, err := json.Marshal(struct {
		Schema    string `json:"schema"`
		Code      string `json:"code"`
		Cancelled bool   `json:"cancelled"`
	}{input.Schema, input.Code, input.Cancelled})
	if err != nil {
		return false
	}
	sum := sha256.Sum256(encoded)
	return draft.Payload.Size == int64(len(encoded)) && draft.Payload.SHA256 == hex.EncodeToString(sum[:])
}
