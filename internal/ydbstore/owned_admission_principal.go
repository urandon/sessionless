package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// One SELECT, exact complete primary keys, not a multiple-statement batch.
// Missing membership/participation denies; deletion absence is observed in the
// SAME serializable snapshot. Every returned scalar is charged before parsing.
const ownedAdmissionPrincipalSQL = `SELECT SUBSTRING(CAST(s.record AS String),0,8193),
 SUBSTRING(CAST(s.created_by AS String),0,161), SUBSTRING(CAST(s.status AS String),0,161), s.last_event_sequence,
 CASE WHEN s.archived_at IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 SUBSTRING(CAST(m.record AS String),0,8193), SUBSTRING(CAST(m.role AS String),0,161), SUBSTRING(CAST(m.status AS String),0,161), m.security_version,
 SUBSTRING(CAST(p.record AS String),0,8193), SUBSTRING(CAST(p.role AS String),0,161), SUBSTRING(CAST(p.status AS String),0,161),
 CASE WHEN d.session_id IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END
 FROM sessions AS s
 INNER JOIN session_participants AS p ON p.tenant_id=s.tenant_id AND p.session_id=s.session_id
 INNER JOIN tenant_memberships AS m ON m.tenant_id=s.tenant_id AND m.user_id=p.user_id
 LEFT JOIN session_deletions AS d ON d.tenant_id=s.tenant_id AND d.session_id=s.session_id
 WHERE s.tenant_id=$1 AND s.session_id=$2 AND p.user_id=$3 AND m.user_id=$3 AND m.user_bucket=$4`

func readOwnedAdmissionPrincipal(ctx context.Context, q *ownedAuthorityQuery, scope domain.AttemptExecutionSourceScopeV1) error {
	if scope.Validate() != nil {
		return q.budget.deny()
	}
	bucket, err := webBucket(string(scope.OwnerUserID))
	if err != nil {
		return q.budget.deny()
	}
	var sessionRaw, creator, sessionStatus, memberRaw, memberRole, memberStatus, participantRaw, participantRole, participantStatus string
	var sequence, archived, version, deletion uint64
	err = q.scanMetadata(ctx, ownedAdmissionPrincipalSQL, []any{scope.TenantID, scope.SessionID, scope.OwnerUserID, bucket},
		&sessionRaw, &creator, &sessionStatus, &sequence, &archived, &memberRaw, &memberRole, &memberStatus, &version, &participantRaw, &participantRole, &participantStatus, &deletion)
	if err != nil || archived != 0 || deletion != 0 || len(creator) > 160 || len(sessionStatus) > 160 || len(memberRole) > 160 || len(memberStatus) > 160 || len(participantRole) > 160 || len(participantStatus) > 160 {
		return q.budget.deny()
	}
	var s domain.Session
	var m domain.TenantMembership
	var p domain.SessionParticipant
	if decodeCurrentExecutionRecord(sessionRaw, &s, computeChoiceSessionFields) != nil || s.Validate() != nil ||
		s.TenantID != scope.TenantID || s.ID != scope.SessionID || string(s.CreatedBy) != creator || string(s.Status) != sessionStatus || s.LastEventSequence != sequence || s.ArchivedAt != nil || s.Status != domain.SessionActive || s.UpdatedAt.After(q.at) ||
		decodeCurrentExecutionRecord(memberRaw, &m, computeChoiceMembershipFields) != nil || string(m.Role) != memberRole || string(m.Status) != memberStatus || m.SecurityVersion != version || m.UpdatedAt.After(q.at) || m.Authorize(scope.OwnerUserID, scope.TenantID, domain.TenantPermissionWrite) != nil ||
		decodeCurrentExecutionRecord(participantRaw, &p, computeChoiceParticipantFields) != nil || string(p.Role) != participantRole || string(p.Status) != participantStatus || p.UpdatedAt.After(q.at) || p.Authorize(scope.TenantID, scope.SessionID, scope.OwnerUserID, true) != nil {
		return q.budget.deny()
	}
	// These exact canonical facts have already been charged and validated.
	// Seed ONLY the same callback's existing point cache, never permission state.
	for _, item := range []struct {
		statement, raw string
		args           []any
		absent         bool
	}{
		{"SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2", sessionRaw, []any{scope.TenantID, scope.SessionID}, false},
		{"SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2", "", []any{scope.TenantID, scope.SessionID}, true},
	} {
		statement, limit, ok := ownedCanonicalJSONSelect(item.statement)
		if !ok || len(item.raw) > limit {
			return q.budget.deny()
		}
		args, err := json.Marshal(item.args)
		if err != nil {
			return q.budget.deny()
		}
		var absent error
		if item.absent {
			absent = sql.ErrNoRows
		}
		q.cacheMetadata(statement+"\n"+string(args), statement, item.raw, absent)
	}
	return nil
}
