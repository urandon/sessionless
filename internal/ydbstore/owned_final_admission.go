package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedAdmissionSQL = `SELECT SUBSTRING(record,0,32769), SUBSTRING(CAST(digest AS String),0,65),
 SUBSTRING(CAST(run_id AS String),0,161), SUBSTRING(CAST(session_id AS String),0,161),
 SUBSTRING(CAST(owner_user_id AS String),0,161), SUBSTRING(CAST(reservation_id AS String),0,161), admitted_at
 FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2`

// This is a canonical child of the existing Attempt. It is NOT a reservation,
// installation approval, balance or capability. Recovery cannot fabricate it.
func readOwnedAdmission(ctx context.Context, q *ownedAuthorityQuery, scope domain.AttemptExecutionSourceScopeV1) (*domain.AttemptExecutionAdmissionV1, error) {
	if scope.Validate() != nil {
		return nil, q.budget.deny()
	}
	var raw, digest, run, session, owner, reservation string
	var at time.Time
	err := q.scanMetadata(ctx, ownedAdmissionSQL, []any{scope.TenantID, scope.AttemptID}, &raw, &digest, &run, &session, &owner, &reservation, &at)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || len(run) > 160 || len(session) > 160 || len(owner) > 160 || len(reservation) > 160 {
		return nil, q.budget.deny()
	}
	a, err := domain.DecodeAttemptExecutionAdmissionV1([]byte(raw))
	actual, digestErr := a.Digest()
	if err != nil || digestErr != nil || actual != digest || a.Source.Scope != scope || string(a.Source.Scope.RunID) != run || string(a.Source.Scope.SessionID) != session || string(a.Source.Scope.OwnerUserID) != owner || string(a.ReservationID) != reservation || !a.AdmittedAt.Equal(at) || at.After(q.at) {
		return nil, q.budget.deny()
	}
	return &a, nil
}

func writeOwnedAdmission(ctx context.Context, q *ownedAuthorityQuery, a domain.AttemptExecutionAdmissionV1) error {
	raw, err := domain.EncodeAttemptExecutionAdmissionV1(a)
	digest, digestErr := a.Digest()
	if !q.ownedAdmission || err != nil || digestErr != nil || !a.AdmittedAt.Equal(q.at) {
		return q.budget.deny()
	}
	s := a.Source.Scope
	// INSERT, not UPSERT: conflicting or missing recovery never reseals history.
	if _, err := q.ExecContext(ctx, `INSERT INTO attempt_execution_admissions_v1
 (tenant_id,attempt_id,run_id,session_id,owner_user_id,reservation_id,admitted_at,record,digest)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.TenantID, s.AttemptID, s.RunID, s.SessionID, s.OwnerUserID, a.ReservationID, a.AdmittedAt, string(raw), digest); err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `INSERT INTO attempt_execution_admissions_by_session_v1
 (tenant_id,session_id,attempt_id,run_id) VALUES ($1,$2,$3,$4)`, s.TenantID, s.SessionID, s.AttemptID, s.RunID)
	return err
}

func ownedAdmissionMatches(a *domain.AttemptExecutionAdmissionV1, source domain.AttemptExecutionSourceV1, original string, reservation domain.QuotaReservation, job domain.WorkerJob) bool {
	if a == nil || a.Validate() != nil || a.Source.Scope != source.Scope || a.ReservationID != reservation.ID || a.ReservationID != job.ReservationID ||
		!a.AdmittedAt.Equal(reservation.HeldAt) || !a.AdmittedAt.Equal(job.CreatedAt) {
		return false
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(a.Source)
	return err == nil && string(raw) == original && job.OwnedExecutionSourceRecord == original
}

func (q *ownedAuthorityQuery) flushAdmissionExplanation(ctx context.Context, run domain.RunID, tenant domain.TenantID) error {
	if len(q.explanationPending) == 0 {
		return nil
	}
	if len(q.explanationPending) != 1 {
		return q.budget.deny()
	}
	head, ok := q.explanationPending[run]
	if !ok || head.TenantID != tenant || head.Validate() != nil {
		return q.budget.deny()
	}
	raw, err := json.Marshal(head)
	if err != nil || len(raw) > 8192 {
		return q.budget.deny()
	}
	_, err = q.ExecContext(ctx, `UPSERT INTO run_explanation_heads_v1 (tenant_id,run_id,revision,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, tenant, run, head.Revision, string(raw))
	return err
}
