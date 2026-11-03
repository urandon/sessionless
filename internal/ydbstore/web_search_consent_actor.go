package ydbstore

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is the canonical initiating user, not the credential/resource owner.
// Callers first authenticate the original parent/Source and select the genuine
// V2 profile. Private-owner equality is checked only after this derivation.
func readOwnedConsentCanonicalActor(ctx context.Context, q *ownedAuthorityQuery, scope domain.AttemptExecutionSourceScopeV1, run domain.Run, job domain.WorkerJob) (domain.UserID, error) {
	deny := func() (domain.UserID, error) { return "", q.budget.deny() }
	if scope.Validate() != nil || run.Validate() != nil || job.ValidateForRun(run) != nil ||
		run.TenantID != scope.TenantID || run.ID != scope.RunID || run.SessionID != scope.SessionID || job.AttemptID != scope.AttemptID ||
		run.CreatedAt.After(q.at) || run.UpdatedAt.After(q.at) || job.CreatedAt.Before(run.CreatedAt) || job.CreatedAt.After(q.at) ||
		job.ContextWindow == nil || job.ContextWindow.Validate() != nil {
		return deny()
	}
	// Full primary key, bounded by the existing registered metadata reader.
	// No scan by event ID, new index or immutable-body I/O is needed.
	sequence := job.ContextWindow.ThroughSequence
	raw, err := q.readCanonicalJSON(ctx, "SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3", scope.TenantID, scope.SessionID, sequence)
	var event domain.SessionEvent
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &event) != nil || event.Validate() != nil ||
		event.ID != run.TriggerEventID || event.ID != job.TriggerEventID || event.TenantID != scope.TenantID || event.SessionID != scope.SessionID ||
		event.Sequence != sequence || event.Kind != domain.SessionEventUserMessage || event.RunID == nil || *event.RunID != run.ID ||
		event.AuthorUserID == nil || event.AuthorUserID.Validate() != nil || !event.CreatedAt.Equal(run.CreatedAt) || event.CreatedAt.After(q.at) {
		return deny()
	}
	return *event.AuthorUserID, nil
}

// Intent selection needs only Run and Job, not a second five-record worker
// aggregate. Existing exact-query caching reuses these rows within a callback.
func loadOwnedConsentCanonicalActor(ctx context.Context, q *ownedAuthorityQuery, scope domain.AttemptExecutionSourceScopeV1) (domain.UserID, error) {
	var run domain.Run
	var job domain.WorkerJob
	for _, item := range []struct {
		statement string
		target    any
	}{
		{"SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", &run},
		{"SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2", &job},
	} {
		raw, err := q.readCanonicalJSON(ctx, item.statement, scope.TenantID, scope.RunID)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, item.target) != nil {
			return "", q.budget.deny()
		}
	}
	return readOwnedConsentCanonicalActor(ctx, q, scope, run, job)
}
