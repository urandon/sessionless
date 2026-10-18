package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This representation is deliberately request-free. Before erasure the
// original composer authenticated each normalized body with its request. Here
// the original registered capsule hash preserves that derivation; this codec
// cannot grant execution or recreate an erased request/cleanup receipt.
type localRetainedTerminalCapsuleV1 struct {
	Schema         string                             `json:"schema"`
	Scope          domain.AttemptOperationScopeV1     `json:"scope"`
	ProfileDigest  string                             `json:"profile_digest"`
	CodecVersion   uint32                             `json:"codec_version"`
	FrontierDigest string                             `json:"frontier_digest"`
	Operations     []localRetainedTerminalOperationV1 `json:"operations"`
	DerivedContext *domain.RunDerivedContextV1        `json:"derived_context"`
	Events         []localCapsuleEventV1              `json:"events"`
	PublicManifest domain.ArtifactManifest            `json:"public_manifest"`
}

type localRetainedTerminalOperationV1 struct {
	Reservation    domain.AttemptOperationReservationV1 `json:"reservation"`
	Observation    domain.AttemptOperationObservationV1 `json:"observation"`
	NormalizedBody json.RawMessage                      `json:"normalized_body"`
	Projection     string                               `json:"projection"`
	EventIDs       []domain.SessionEventID              `json:"event_ids"`
}

func (c *localTerminalCompletionV2) readRetained(ctx context.Context) (_ *ports.CanonicalEventMaterialSetV1, err error) {
	if !c.original() || ctx == nil || ctx.Err() != nil || c.run.TerminalCustody == nil {
		return nil, domain.ErrMembershipDenied
	}
	p := c.cleanup.owner
	entry := p.parent.entry
	plan := c.run.TerminalCustody.Plan
	body, e := entry.readCompactionBlob(ctx, plan.Capsule, entry.terminalOutputs.CapsuleLimit())
	if e != nil {
		return nil, e
	}
	defer clear(body)
	// Account for the bounded decoded copy as well as the blob read itself.
	if chargeOwnedAuthorityBody(ctx, uint64(len(body))) != nil {
		return nil, domain.ErrMembershipDenied
	}
	var capsule localRetainedTerminalCapsuleV1
	if decodeOwnedEffectCanonicalJSON(string(body), &capsule) != nil || capsule.Schema != "sessionless.terminal-custody.v1" ||
		capsule.CodecVersion != 1 || capsule.Scope != plan.Scope || capsule.FrontierDigest != plan.FrontierDigest ||
		capsule.ProfileDigest != c.evidence.LocalV1.ProfileDigest || !domain.EqualRunDerivedContextV1(capsule.DerivedContext, c.run.DerivedContext) ||
		!reflect.DeepEqual(capsule.PublicManifest, c.manifest) || len(capsule.Events) != len(c.drafts) || len(capsule.Operations) == 0 || len(capsule.Operations) > 12 ||
		p.accounting == nil || len(capsule.Operations) != len(p.accounting.operations) {
		return nil, domain.ErrMembershipDenied
	}
	for i, draft := range c.drafts {
		if capsule.Events[i] != (localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}) {
			return nil, domain.ErrMembershipDenied
		}
	}
	var normalized uint64
	for i, op := range capsule.Operations {
		original := p.accounting.operations[i]
		if !reflect.DeepEqual(op.Reservation, original.Reservation) || !reflect.DeepEqual(op.Observation, original.Observation) ||
			len(op.NormalizedBody) == 0 || len(op.NormalizedBody) > 64<<10 || uint64(len(op.NormalizedBody)) > (384<<10)-normalized ||
			uint64(len(op.NormalizedBody)) != op.Observation.ResponseBytes {
			return nil, domain.ErrMembershipDenied
		}
		normalized += uint64(len(op.NormalizedBody))
		sum := sha256.Sum256(op.NormalizedBody)
		if hex.EncodeToString(sum[:]) != op.Observation.ResponseDigest {
			return nil, domain.ErrMembershipDenied
		}
		projection := "retained_only"
		if op.Reservation.Operation.Kind == domain.AttemptOperationCompactionV1 {
			projection = "compaction"
		}
		if i == len(capsule.Operations)-1 {
			projection = "assistant"
			if len(op.EventIDs) != 1 || op.EventIDs[0] != c.custody.Correspondence.TerminalAssistantEventID {
				return nil, domain.ErrMembershipDenied
			}
		} else if len(op.EventIDs) != 0 {
			return nil, domain.ErrMembershipDenied
		}
		if op.Projection != projection {
			return nil, domain.ErrMembershipDenied
		}
	}
	if p.verifyCleanupRetained(ctx) != nil {
		return nil, domain.ErrMembershipDenied
	}
	materials := ports.NewCanonicalEventMaterialSetV1()
	defer func() {
		if err != nil {
			materials.Release()
		}
	}()
	for _, draft := range append(append([]domain.SessionEventDraft(nil), c.drafts...), c.cancellation) {
		if draft.ValidateForRun(c.run) != nil || draft.Payload.Size <= 0 || draft.Payload.Size > 256<<10 {
			return nil, domain.ErrMembershipDenied
		}
		payload, e := entry.readCompactionBlob(ctx, draft.Payload, uint64(draft.Payload.Size))
		if e != nil {
			return nil, e
		}
		if chargeOwnedAuthorityBody(ctx, uint64(len(payload))) != nil {
			clear(payload)
			return nil, domain.ErrMembershipDenied
		}
		prepared := ports.PrepareCanonicalDraftV1(c.run, draft, payload)
		clear(payload)
		if !materials.Adopt(draft.ID, prepared) {
			return nil, domain.ErrMembershipDenied
		}
	}
	// The cancellation object is fixed before erasure. It is not a success
	// capsule event and may not be replaced with a notice chosen during replay.
	failure, e := canonicaloutput.EncodeFailureV1(c.run.ID, true, "cancelled")
	if e != nil {
		return nil, e
	}
	defer clear(failure.Payload)
	notice, e := canonicaloutput.PlanEventDraftV1(c.run, failure.ID, failure.Kind, failure.Payload, c.cancellation.CreatedAt)
	if e != nil || notice != c.cancellation || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	return materials, nil
}

const localTerminalMarkerSQL = `SELECT SUBSTRING(CAST(terminal_status AS String),0,33),SUBSTRING(CAST(content_digest AS String),0,74)
 FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2`

// One primary-index join, not two point reads per event. The input is the
// original bounded composer package (1..25), never caller SQL or identifiers.
// INNER JOIN plus exact count/identity checks rejects missing index or body.
func localTerminalReplayEventsSQL(count int) string {
	if count < 1 || count > 25 {
		return ""
	}
	keys := make([]string, count)
	for i := range keys {
		keys[i] = fmt.Sprintf("$%d", i+3)
	}
	return `SELECT i.sequence,SUBSTRING(CAST(i.event_id AS String),0,129),SUBSTRING(CAST(e.record AS String),0,32769)
 FROM session_event_idempotency AS i INNER JOIN session_events AS e
 ON i.tenant_id=e.tenant_id AND i.session_id=e.session_id AND i.sequence=e.sequence
 WHERE i.tenant_id=$1 AND i.session_id=$2 AND i.idempotency_key IN (` + strings.Join(keys, ",") + `)
 ORDER BY i.sequence LIMIT 26`
}

func (c *localTerminalCompletionV2) observeReplay(ctx context.Context, q *ownedAuthorityQuery) (bool, error) {
	conflict := func() (bool, error) { return false, ErrRunFinalizationConflict }
	if !c.original() {
		return false, q.budget.deny()
	}
	raw, err := q.readCanonicalJSON(ctx, `SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, c.run.TenantID, c.run.ID)
	var run domain.Run
	if err != nil || decodeOwnedEffectCanonicalJSON(raw, &run) != nil || run.Validate() != nil {
		return conflict()
	}
	var status, digest string
	err = q.scanMetadata(ctx, localTerminalMarkerSQL, []any{c.run.TenantID, c.run.ID}, &status, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		if run.Status.Terminal() {
			return conflict()
		}
		return false, nil
	}
	if err != nil || len(status) > 32 || status != string(run.Status) || run.FinishedAt == nil || !run.FinishedAt.Equal(run.UpdatedAt) {
		return conflict()
	}
	digest, err = decodeLocalFinalizationMarkerV2(digest)
	if err != nil {
		return conflict()
	}
	base := run.Clone()
	base.Status, base.UpdatedAt, base.FinishedAt = c.run.Status, c.run.UpdatedAt, c.run.FinishedAt
	drafts := c.drafts
	var manifest *domain.ArtifactManifest
	var evidence *domain.ExecutionEvidenceV2
	switch run.Status {
	case domain.RunSucceeded:
		if !domain.EqualRunTerminalCustodyV1(run.TerminalCustody, &c.custody) || run.CancellationRequestedAt != nil {
			return conflict()
		}
		base.TerminalCustody = c.run.TerminalCustody
		manifest, evidence = &c.manifest, &c.evidence
	case domain.RunCancelled:
		if run.CancellationRequestedAt == nil || run.CancellationRequestedAt.Before(c.run.UpdatedAt) || run.CancellationRequestedAt.After(run.UpdatedAt) || !domain.EqualRunTerminalCustodyV1(run.TerminalCustody, c.run.TerminalCustody) {
			return conflict()
		}
		base.CancellationRequestedAt = c.run.CancellationRequestedAt
		drafts = []domain.SessionEventDraft{c.cancellation}
	default:
		return conflict()
	}
	if !reflect.DeepEqual(base, c.run) {
		return conflict()
	}
	expected, err := runFinalizationDigestV2(run.Status, manifest, drafts, evidence, run.DerivedContext, run.TerminalCustody)
	if err != nil || digest != expected {
		return conflict()
	}
	// The same original atomic finalizer owns these transitions. Replaying an
	// otherwise matching marker with changed/missing terminal dependencies is a
	// conflict, not permission to repair or finalize them a second time.
	attempt, reservation := c.attempt, c.reservation
	attemptStatus, reservationStatus := domain.AttemptSucceeded, domain.ReservationCommitted
	if run.Status == domain.RunCancelled {
		attemptStatus, reservationStatus = domain.AttemptCancelled, domain.ReservationReleased
	}
	if attempt.Transition(attemptStatus, run.UpdatedAt) != nil || reservation.Transition(reservationStatus, run.UpdatedAt) != nil {
		return conflict()
	}
	for _, item := range []struct {
		query string
		args  []any
		want  any
		got   any
	}{
		{`SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2`, []any{run.TenantID, attempt.ID}, attempt, new(domain.Attempt)},
		{`SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2`, []any{run.TenantID, reservation.ID}, reservation, new(domain.QuotaReservation)},
	} {
		raw, err := q.readCanonicalJSON(ctx, item.query, item.args...)
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, item.got) != nil || !reflect.DeepEqual(reflect.ValueOf(item.got).Elem().Interface(), item.want) {
			return conflict()
		}
	}
	raw, err = q.readCanonicalJSON(ctx, `SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, run.TenantID, c.manifest.ID)
	if run.Status == domain.RunSucceeded {
		var stored domain.ArtifactManifest
		if err != nil || decodeOwnedEffectCanonicalJSON(raw, &stored) != nil || !reflect.DeepEqual(stored, c.manifest) {
			return conflict()
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return conflict()
	}
	var storedDigest, storedEvidence string
	err = q.scanMetadata(ctx, localTerminalEvidenceSQL, []any{run.TenantID, c.attempt.ID}, &storedDigest, &storedEvidence)
	if run.Status == domain.RunSucceeded {
		var stored domain.ExecutionEvidenceV2
		if err != nil || len(storedEvidence) > domain.MaxExecutionEvidenceBytesV2 || decodeOwnedEffectCanonicalJSON(storedEvidence, &stored) != nil ||
			storedDigest != c.evidence.LocalV1.EvidenceDigest || !reflect.DeepEqual(stored, c.evidence) {
			return conflict()
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return conflict()
	}
	args := []any{run.TenantID, run.SessionID}
	for _, draft := range drafts {
		args = append(args, draft.IdempotencyKey)
	}
	rows, err := q.QueryContext(ctx, localTerminalReplayEventsSQL(len(drafts)), args...)
	if err != nil {
		return conflict()
	}
	defer rows.Close()
	index := 0
	var previous uint64
	for rows.Next() {
		var sequence uint64
		var id, record string
		if index >= len(drafts) || q.scanReplayEvent(ctx, rows, &sequence, &id, &record) != nil || len(id) > 128 || len(record) > ownedAuthorityHeadBytes || (index > 0 && sequence != previous+1) {
			return conflict()
		}
		var event domain.SessionEvent
		draft := drafts[index]
		if decodeOwnedEffectCanonicalJSON(record, &event) != nil || event.Validate() != nil || event.ID != draft.ID || id != string(draft.ID) ||
			event.Sequence != sequence || event.TenantID != run.TenantID || event.SessionID != run.SessionID || event.RunID == nil || *event.RunID != run.ID ||
			event.Kind != draft.Kind || event.AuthorUserID != nil || event.IdempotencyKey != draft.IdempotencyKey || event.Payload != draft.Payload || event.CreatedAt.Before(draft.CreatedAt) {
			return conflict()
		}
		previous, index = sequence, index+1
	}
	if rows.Err() != nil || rows.Close() != nil || index != len(drafts) || ctx.Err() != nil {
		return conflict()
	}
	return true, nil
}

func (q *ownedAuthorityQuery) scanReplayEvent(ctx context.Context, rows *sql.Rows, sequence *uint64, id, record *string) error {
	n, err := scanOwnedCurrentRow(rows, sequence, id, record)
	if err != nil || ctx.Err() != nil || q.budget.materialized(q.callback, n) != nil {
		return q.budget.deny()
	}
	return nil
}
