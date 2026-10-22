package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

type retainedDeletionRunKey struct {
	ID        domain.RunID
	Attempt   domain.AttemptID
	Anchor    domain.SessionEventID
	Sequence  uint64
	RunDigest string
}
type sessionDeletionCapture struct {
	deletion    domain.SessionDeletion
	inventory   domain.SessionDeletionInventory
	metadata    string
	rows        uint64
	ownership   retainedDeletionOwnershipData
	runs        []retainedDeletionRunKey
	admissions  []ownedSessionAdmissionKey
	scopes      []deletionCaptureScope
	attemptRows map[domain.AttemptID]retainedDeletionAttemptRows
}
type retainedDeletionAttemptRows struct{ parents, evidence, operations, calls, proxies, reconciliation uint64 }

// A graph contains only bounded identities, references and row digests, never
// every Run payload or object body. Scopes come exclusively from original SQL
// owners. Blob manifests cannot add SQL scopes or physical deletion targets.
type deletionCaptureGraph struct {
	*sessionDeletionCapture
	query            stateSQLExecutor
	seen             map[string]bool
	objects          map[string]domain.BlobRef
	runDigests       map[domain.RunID]string
	required         map[string]bool
	expected         map[string]map[string]json.RawMessage
	optionalExpected map[string]bool
	work             *retainedDeletionWork
}

// This is prospective DATA work accounting, not a retained-deletion approval
// or a replacement for the original per-Run authority budget. It exists before
// the first SQL read, including on an ordinary stateTx, and is never reset
// between identity discovery, records and the complete postimage stream.
type deletionCaptureReadBudget struct {
	rows, bytes, ceiling uint64
	admitted, failed     bool
}
type deletionCaptureQuery struct {
	query  stateSQLExecutor
	budget *deletionCaptureReadBudget
}

func newDeletionCaptureQuery(query stateSQLExecutor) *deletionCaptureQuery {
	return &deletionCaptureQuery{query: query, budget: &deletionCaptureReadBudget{ceiling: ownedAuthorityMaxBytes}}
}
func (q *deletionCaptureQuery) QueryContext(ctx context.Context, s string, args ...any) (*sql.Rows, error) {
	if q.budget.failed {
		return nil, domain.ErrMembershipDenied
	}
	return q.query.QueryContext(ctx, s, args...)
}
func (q *deletionCaptureQuery) QueryRowContext(ctx context.Context, s string, args ...any) *sql.Row {
	if q.budget.failed {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		return q.query.QueryRowContext(cancelled, s, args...)
	}
	return q.query.QueryRowContext(ctx, s, args...)
}
func (q *deletionCaptureQuery) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	return nil, domain.ErrMembershipDenied
}
func (b *deletionCaptureReadBudget) charge(n uint64) error {
	if b.failed || b.rows >= maxSessionDeletionRows || b.bytes > b.ceiling || n > b.ceiling-b.bytes {
		b.failed = true
		return domain.ErrMembershipDenied
	}
	b.rows++
	b.bytes += n
	return nil
}
func (b *deletionCaptureReadBudget) admitOriginalRuns(n uint64) error {
	if b.failed || b.admitted || n > maxSessionDeletionRows || n > math.MaxUint64/ownedAuthorityMaxBytes-1 {
		b.failed = true
		return domain.ErrMembershipDenied
	}
	// Original indexed Run envelopes plus the existing bounded SQL inventory
	// envelope. Identity discovery itself must fit the initial inventory one.
	b.ceiling = (n + 1) * ownedAuthorityMaxBytes
	b.admitted = true
	return nil
}

func (g *deletionCaptureGraph) add(table string, prefix ...any) error {
	spec := deletionCaptureSpecs[table]
	if spec == nil || len(prefix) == 0 || len(prefix) > len(spec.keys) || len(g.scopes) >= int(maxSessionDeletionRows) {
		return domain.ErrMembershipDenied
	}
	b, err := json.Marshal(prefix)
	if err != nil {
		return err
	}
	key := table + "\x00" + string(b)
	if g.seen[key] {
		return nil
	}
	g.seen[key] = true
	scope := deletionCaptureScope{spec: spec, prefix: prefix, query: g.query, observe: g.observeRow}
	g.scopes = append(g.scopes, scope)
	if len(prefix) == len(spec.keys) {
		switch table {
		case "sessions", "runs", "artifact_manifests", "telegram_delivery_outbox", "checkpoints", "frontend_bindings", "frontend_binding_keys", "frontend_projection_outbox", "attempt_execution_admissions_v1", "session_lifecycle_backfill_state":
			g.required[deletionCaptureIdentity(table, captureScopeKey(scope))] = true
		}
	}
	return nil
}

func readDeletionCaptureJSON[T any](ctx context.Context, query stateSQLExecutor, table, column string, keys ...any) (value T, found bool, err error) {
	spec := deletionCaptureSpecs[table]
	if spec == nil || len(keys) != len(spec.keys) {
		return value, false, domain.ErrMembershipDenied
	}
	allowed := false
	for _, c := range spec.columns {
		if c.name == column && (c.kind == 'j' || c.kind == 's') {
			allowed = true
		}
	}
	if !allowed {
		return value, false, domain.ErrMembershipDenied
	}
	var where []string
	for i, k := range spec.keys {
		where = append(where, fmt.Sprintf("%s=$%d", k, i+1))
	}
	var raw sql.NullString
	err = query.QueryRowContext(ctx, fmt.Sprintf("SELECT SUBSTRING(CAST(%s AS String),0,32769) FROM %s WHERE %s", column, table, strings.Join(where, " AND ")), keys...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return value, false, nil
	}
	if err != nil {
		return value, false, err
	}
	if err := chargeDeletionCaptureSQL(query, uint64(len(raw.String))); err != nil {
		return value, false, err
	}
	if !raw.Valid || len(raw.String) > ownedAuthorityHeadBytes || decodeOwnedEffectCanonicalJSON(raw.String, &value) != nil {
		return value, false, domain.ErrMembershipDenied
	}
	return value, true, nil
}

func deletionRunDigest(run domain.Run) (string, error) {
	b, err := json.Marshal(run)
	if err != nil || len(b) > ownedAuthorityHeadBytes || run.Validate() != nil {
		return "", domain.ErrMembershipDenied
	}
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:]), nil
}

func (g *deletionCaptureGraph) addRun(ctx context.Context, tenant domain.TenantID, session domain.SessionID, id domain.RunID) error {
	previousQuery := g.query
	if g.work != nil {
		runQuery, err := g.work.forRun(id)
		if err != nil {
			return err
		}
		g.query = &deletionCaptureQuery{query: runQuery, budget: previousQuery.(*deletionCaptureQuery).budget}
		defer func() { g.query = previousQuery }()
	}
	run, found, err := readDeletionCaptureJSON[domain.Run](ctx, g.query, "runs", "payload", tenant, id)
	if err != nil || !found || run.Validate() != nil || run.TenantID != tenant || run.SessionID != session || run.ID != id {
		return domain.ErrMembershipDenied
	}
	if err := g.ownership.addRun(run); err != nil {
		return err
	}
	g.runDigests[id], err = deletionRunDigest(run)
	if err != nil {
		return err
	}
	if err := g.expect("runs_by_session", []any{tenant, session, run.CreatedAt, id}, map[string]any{"trigger_event_id": run.TriggerEventID, "status": run.Status}); err != nil {
		return err
	}
	refs, err := retainedDeletionRunRefsData(run)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := g.addObject(ref); err != nil {
			return err
		}
	}
	for _, table := range []string{"runs", "run_finalizations", "run_explanation_heads_v1", "worker_jobs", "artifact_manifests_by_run", "attached_worker_output_receipts", "checkpoint_objects_by_run", "telegram_deliveries_by_run", "frontend_projections_by_run"} {
		if err := g.add(table, tenant, id); err != nil {
			return err
		}
	}
	if err := g.add("run_idempotency", tenant, run.IdempotencyKey); err != nil {
		return err
	}
	if err := g.expectOptional("run_idempotency", []any{tenant, run.IdempotencyKey}, map[string]any{"run_id": id, "created_at": run.CreatedAt}); err != nil {
		return err
	}
	if run.TerminalCustody != nil {
		key := retainedDeletionRunKey{ID: id, Attempt: run.TerminalCustody.Plan.Scope.AttemptID}
		if run.TerminalCustody.Correspondence != nil {
			key.Anchor = run.TerminalCustody.Correspondence.TerminalAssistantEventID
			key.Sequence = g.ownership.events[key.Anchor].Sequence
		}
		key.RunDigest, err = deletionRunDigest(run)
		if err != nil {
			return err
		}
		g.runs = append(g.runs, key)
		if run.TerminalCustody.LocalEvidence != nil {
			e := run.TerminalCustody.LocalEvidence
			if err := g.expect("substrate_execution_evidence", []any{tenant, key.Attempt}, map[string]any{"run_id": id, "physical_invocation_claim_id": e.Scope.PhysicalInvocationClaimID, "evidence_digest": e.EvidenceDigest, "recorded_at": run.UpdatedAt}); err != nil {
				return err
			}
			if err := g.expect("run_finalizations", []any{tenant, id}, map[string]any{"terminal_status": run.Status, "created_at": run.UpdatedAt}); err != nil {
				return err
			}
		}
	}
	manifests, err := discoverDeletionCaptureIDs(ctx, g.query, "artifact_manifests_by_run", "artifact_manifest_id", tenant, id)
	if err != nil {
		return err
	}
	for _, discovered := range manifests {
		manifest := domain.ArtifactManifestID(discovered[0])
		if err := g.add("artifact_manifests", tenant, manifest); err != nil {
			return err
		}
		if err := g.expect("artifact_manifests", []any{tenant, manifest}, map[string]any{"run_id": id}); err != nil {
			return err
		}
		if err := g.expect("artifact_manifests_by_run", []any{tenant, id, manifest}, map[string]any{"artifact_manifest_id": manifest}); err != nil {
			return err
		}
	}
	deliveries, err := discoverDeletionCaptureIDs(ctx, g.query, "telegram_deliveries_by_run", "telegram_delivery_id", tenant, id)
	if err != nil {
		return err
	}
	for _, discovered := range deliveries {
		deliveryID := domain.TelegramDeliveryID(discovered[0])
		delivery, found, err := readDeletionCaptureJSON[domain.TelegramDeliveryOutbox](ctx, g.query, "telegram_deliveries_by_run", "record", tenant, id, deliveryID)
		if err != nil || !found || delivery.RunID != id || delivery.TenantID != tenant || delivery.ID != deliveryID {
			return domain.ErrMembershipDenied
		}
		if err := g.add("telegram_delivery_outbox", tenant, deliveryID); err != nil {
			return err
		}
		if err := g.expect("telegram_delivery_outbox", []any{tenant, deliveryID}, map[string]any{"payload": delivery}); err != nil {
			return err
		}
		if err := g.expect("telegram_deliveries_by_run", []any{tenant, id, deliveryID}, map[string]any{"record": delivery}); err != nil {
			return err
		}
		bucket, err := ydbpartition.BucketV1(string(delivery.ID))
		if err != nil {
			return err
		}
		if err := g.add("telegram_delivery_ready_v2", bucket, telegramDeliveryAvailableAt(delivery), tenant, delivery.ID); err != nil {
			return err
		}
		if err := g.expectOptional("telegram_delivery_ready_v2", []any{bucket, telegramDeliveryAvailableAt(delivery), tenant, delivery.ID}, map[string]any{"run_id": id}); err != nil {
			return err
		}
		// A ready locator is optional (delivered/dequeued), but if present it
		// must still name this exact Run. Absence remains a captured fact.
	}
	checkpoints, err := discoverDeletionCaptureIDs(ctx, g.query, "checkpoint_objects_by_run", "checkpoint_id", tenant, id)
	if err != nil {
		return err
	}
	for _, discovered := range checkpoints {
		checkpointID := domain.CheckpointID(discovered[0])
		checkpoint, found, err := readDeletionCaptureJSON[domain.Checkpoint](ctx, g.query, "checkpoint_objects_by_run", "record", tenant, id, checkpointID)
		if err != nil || !found || checkpoint.RunID != id || checkpoint.TenantID != tenant || checkpoint.ID != checkpointID {
			return domain.ErrMembershipDenied
		}
		if err := g.add("checkpoints", tenant, checkpoint.AttemptID, checkpoint.Sequence); err != nil {
			return err
		}
		if err := g.expect("checkpoints", []any{tenant, checkpoint.AttemptID, checkpoint.Sequence}, map[string]any{"payload": checkpoint}); err != nil {
			return err
		}
		if err := g.expect("checkpoint_objects_by_run", []any{tenant, id, checkpointID}, map[string]any{"record": checkpoint}); err != nil {
			return err
		}
	}
	return nil
}

func (g *deletionCaptureGraph) addFrontendOwners(ctx context.Context, tenant domain.TenantID, session domain.SessionID) error {
	bindings, err := discoverDeletionCaptureIDs(ctx, g.query, "frontend_bindings_by_session", "binding_id", tenant, session)
	if err != nil {
		return err
	}
	for _, discovered := range bindings {
		id := domain.FrontendBindingID(discovered[0])
		binding, found, err := readDeletionCaptureJSON[domain.FrontendBinding](ctx, g.query, "frontend_bindings", "record", tenant, id)
		if err != nil || !found || binding.Validate() != nil || binding.TenantID != tenant || binding.SessionID != session || binding.ID != id {
			return domain.ErrMembershipDenied
		}
		if err := g.add("frontend_bindings", tenant, id); err != nil {
			return err
		}
		if err := g.add("frontend_binding_keys", tenant, binding.Frontend, binding.ExternalConversationID); err != nil {
			return err
		}
		if err := g.expect("frontend_binding_keys", []any{tenant, binding.Frontend, binding.ExternalConversationID}, map[string]any{"binding_id": id}); err != nil {
			return err
		}
		if err := g.expect("frontend_bindings_by_session", []any{tenant, session, id}, map[string]any{"binding_id": id}); err != nil {
			return err
		}
	}
	projections, err := discoverDeletionCaptureIDs(ctx, g.query, "frontend_projections_by_session", "frontend_projection_id", tenant, session)
	if err != nil {
		return err
	}
	for _, discovered := range projections {
		id := domain.FrontendProjectionID(discovered[0])
		projection, found, err := readDeletionCaptureJSON[domain.FrontendProjection](ctx, g.query, "frontend_projection_outbox", "record", tenant, id)
		if err != nil || !found || projection.TenantID != tenant || projection.SessionID != session || projection.ID != id {
			return domain.ErrMembershipDenied
		}
		event, found := g.ownership.events[projection.EventID]
		if !found || event.Sequence != projection.EventSequence || event.RunID == nil {
			return domain.ErrMembershipDenied
		}
		if err := g.add("frontend_projection_outbox", tenant, id); err != nil {
			return err
		}
		if err := g.expect("frontend_projections_by_session", []any{tenant, session, id}, map[string]any{"frontend_projection_id": id}); err != nil {
			return err
		}
		if err := g.expect("frontend_projections_by_run", []any{tenant, *event.RunID, projection.Frontend, id}, map[string]any{"frontend_projection_id": id}); err != nil {
			return err
		}
		bucket, err := ydbpartition.BucketV1(string(projection.ID))
		if err != nil {
			return err
		}
		if err := g.add("frontend_projection_ready_v1", projection.Frontend, bucket, projection.CreatedAt, tenant, projection.ID); err != nil {
			return err
		}
		if err := g.expectOptional("frontend_projection_ready_v1", []any{projection.Frontend, bucket, projection.CreatedAt, tenant, projection.ID}, map[string]any{"run_id": *event.RunID}); err != nil {
			return err
		}
	}
	return nil
}

func captureScopeKey(scope deletionCaptureScope) []deletionCaptureCell {
	key := make([]deletionCaptureCell, len(scope.prefix))
	for i, v := range scope.prefix {
		var kind byte
		for _, c := range scope.spec.columns {
			if c.name == scope.spec.keys[i] {
				kind = c.kind
				break
			}
		}
		if at, ok := v.(time.Time); ok {
			v = at.UTC().Format("2006-01-02T15:04:05.000000Z")
		}
		b, _ := json.Marshal(v)
		key[i] = deletionCaptureCell{Kind: string(kind), Present: true, Value: b}
	}
	return key
}

// Pure SQL-owned graph construction. It establishes no successful writer or
// body proof; requested unknown writers may have a Plan but cannot get Start.
func buildRetainedDeletionCapture(ctx context.Context, tx *stateTx, sessionID domain.SessionID) (*sessionDeletionCapture, error) {
	return buildRetainedDeletionCaptureWithWork(ctx, tx, sessionID, nil)
}
func buildRetainedDeletionCaptureWithWork(ctx context.Context, tx *stateTx, sessionID domain.SessionID, work *retainedDeletionWork) (*sessionDeletionCapture, error) {
	q := newDeletionCaptureQuery(tx.sqlTx)
	if work != nil {
		if work.tx == nil {
			return nil, domain.ErrMembershipDenied
		}
		q.budget = work.readBudget
	}
	var rawRequest string
	err := q.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`, tx.tenantID, sessionID).Scan(&rawRequest)
	if err != nil || chargeDeletionCaptureSQL(q, uint64(len(rawRequest))) != nil || len(rawRequest) > ownedAuthorityHeadBytes {
		return nil, domain.ErrMembershipDenied
	}
	var d domain.SessionDeletion
	if decodeOwnedEffectCanonicalJSON(rawRequest, &d) != nil {
		return nil, domain.ErrMembershipDenied
	}
	if d.Validate() != nil || d.TenantID != tx.tenantID || d.SessionID != sessionID || d.State == domain.SessionDeletionCompleted {
		return nil, domain.ErrMembershipDenied
	}
	ready, err := sessionLifecycleIndexesReady(ctx, q)
	if err != nil || chargeDeletionCaptureSQL(q, 8) != nil || !ready {
		return nil, domain.ErrMembershipDenied
	} // No silent legacy-index fallback for retained proof.
	// Discover bounded identities only. Do not call the legacy JSON inventory:
	// its large-record projections predate retained verification's transfer
	// envelope. Full bounded postimages below build the exact inventory while
	// streaming, so no unbounded JSON is read before the verifier sees it.
	identities, err := discoverDeletionCaptureIDs(ctx, q, "runs_by_session", "run_id", tx.tenantID, sessionID)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	ids := make([]domain.RunID, len(identities))
	for i, v := range identities {
		ids[i] = domain.RunID(v[0])
	}
	var admitErr error
	if work != nil {
		admitErr = work.admitRuns(ids)
	} else {
		admitErr = q.budget.admitOriginalRuns(uint64(len(ids)))
	}
	if admitErr != nil {
		return nil, admitErr
	}
	inventory := domain.SessionDeletionInventory{TenantID: tx.tenantID, SessionID: sessionID, RunIDs: ids}
	ownership, err := newRetainedDeletionOwnershipData(tx.tenantID, sessionID)
	if err != nil {
		return nil, err
	}
	g := &deletionCaptureGraph{sessionDeletionCapture: &sessionDeletionCapture{deletion: d, inventory: inventory, ownership: ownership, attemptRows: map[domain.AttemptID]retainedDeletionAttemptRows{}}, query: q, seen: map[string]bool{}, objects: map[string]domain.BlobRef{}, runDigests: map[domain.RunID]string{}, required: map[string]bool{}, expected: map[string]map[string]json.RawMessage{}, optionalExpected: map[string]bool{}, work: work}
	for _, table := range []string{"sessions", "session_events", "session_snapshots", "session_participants", "session_displays", "session_event_idempotency", "session_material_current_v1", "session_material_boundaries_v1", "session_snapshot_material_v1", "frontend_bindings_by_session", "frontend_projections_by_session", "runs_by_session", "attempt_execution_admissions_by_session_v1"} {
		if err := g.add(table, tx.tenantID, sessionID); err != nil {
			return nil, err
		}
	}
	if err := g.add("session_lifecycle_backfill_state", sessionLifecycleBackfillID); err != nil {
		return nil, err
	}
	var after uint64
	for {
		events, err := deletionCaptureJSONRows[domain.SessionEvent](ctx, q, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id=$1 AND session_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4`, maxSessionPageSize, tx.tenantID, sessionID, after, maxSessionPageSize)
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			if err := g.ownership.addEvent(event); err != nil {
				return nil, err
			}
			if event.Sequence <= after {
				return nil, domain.ErrMembershipDenied
			}
			after = event.Sequence
			if err := g.expect("session_event_idempotency", []any{tx.tenantID, sessionID, event.IdempotencyKey}, map[string]any{"event_id": event.ID, "sequence": event.Sequence, "created_at": event.CreatedAt}); err != nil {
				return nil, err
			}
		}
		if uint64(len(events)) < maxSessionPageSize {
			break
		}
	}
	for _, id := range inventory.RunIDs {
		if err := g.addRun(ctx, tx.tenantID, sessionID, id); err != nil {
			return nil, err
		}
	}
	if err := g.addAdmissionScopes(ctx, tx.tenantID, sessionID); err != nil {
		return nil, err
	}
	if err := g.addFrontendOwners(ctx, tx.tenantID, sessionID); err != nil {
		return nil, err
	}
	session, found, err := readDeletionCaptureJSON[domain.Session](ctx, q, "sessions", "record", tx.tenantID, sessionID)
	if err != nil || !found || session.Validate() != nil || session.TenantID != tx.tenantID || session.ID != sessionID {
		return nil, domain.ErrMembershipDenied
	}
	if err := g.expect("sessions", []any{tx.tenantID, sessionID}, map[string]any{"record": session}); err != nil {
		return nil, err
	}
	if err := g.addParticipantScopes(ctx, session); err != nil {
		return nil, err
	}
	sort.Slice(g.scopes, func(i, j int) bool {
		a, b := g.scopes[i], g.scopes[j]
		if a.spec.table != b.spec.table {
			return a.spec.table < b.spec.table
		}
		return compareDeletionCaptureKey(captureScopeKey(a), captureScopeKey(b)) < 0
	})
	// The outer inventory is bounded independently. Actual materializations are
	// additionally charged to the original per-Run verifier during eligibility;
	// no decoder or I/O below can reset that logical budget.
	maxBytes := q.budget.ceiling
	if work != nil {
		maxBytes = (uint64(len(inventory.RunIDs)) + 1) * ownedAuthorityMaxBytes
	}
	g.metadata, g.rows, err = captureDeletionMetadata(ctx, g.scopes, maxBytes)
	if err != nil {
		return nil, err
	}
	if g.requiredRowsCaptured() != nil || g.inventory.RunRows != uint64(len(inventory.RunIDs)) {
		return nil, domain.ErrMembershipDenied
	}
	if err := g.completeInventory(ctx); err != nil {
		return nil, err
	}
	sort.Slice(g.runs, func(i, j int) bool { return g.runs[i].Sequence < g.runs[j].Sequence })
	return g.sessionDeletionCapture, nil
}

func (g *deletionCaptureGraph) completeInventory(ctx context.Context) error {
	baseRows, err := domain.SessionDeletionInventoryRowsV1(g.inventory)
	if err != nil || g.rows < baseRows {
		return domain.ErrMembershipDenied
	}
	g.inventory.RetainedRows = g.rows - baseRows
	g.inventory.Objects = nil
	g.inventory.TotalBytes = 0
	keys := make([]string, 0, len(g.objects))
	for key := range g.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ref := g.objects[key]
		if uint64(ref.Size) > math.MaxUint64-g.inventory.TotalBytes {
			return domain.ErrMembershipDenied
		}
		g.inventory.TotalBytes += uint64(ref.Size)
		g.inventory.Objects = append(g.inventory.Objects, ref)
	}
	return g.inventory.Validate(maxSessionDeletionRows)
}

func deletionCaptureIdentity(table string, key []deletionCaptureCell) string {
	b, _ := json.Marshal(key)
	return table + "\x00" + string(b)
}
func (g *deletionCaptureGraph) requiredRowsCaptured() error {
	if len(g.required) != 0 {
		return domain.ErrMembershipDenied
	}
	for key := range g.expected {
		if !g.optionalExpected[key] {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}
func (g *deletionCaptureGraph) expect(table string, key []any, values map[string]any) error {
	spec := deletionCaptureSpecs[table]
	if spec == nil || len(key) != len(spec.keys) {
		return domain.ErrMembershipDenied
	}
	identity := deletionCaptureIdentity(table, captureScopeKey(deletionCaptureScope{spec: spec, prefix: key}))
	if g.expected[identity] == nil {
		g.expected[identity] = map[string]json.RawMessage{}
	}
	for column, value := range values {
		for _, c := range spec.columns {
			if c.name == column && c.kind == 't' {
				at, ok := value.(time.Time)
				if !ok || at.Nanosecond()%1000 != 0 {
					return domain.ErrMembershipDenied
				}
				value = at.UTC().Format("2006-01-02T15:04:05.000000Z")
			}
		}
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if previous, found := g.expected[identity][column]; found && string(previous) != string(b) {
			// An additional reverse index cannot replace the original canonical
			// owner's postimage merely because both Runs belong to this Session.
			return domain.ErrMembershipDenied
		}
		g.expected[identity][column] = b
	}
	return nil
}
func (g *deletionCaptureGraph) expectOptional(table string, key []any, values map[string]any) error {
	if table != "telegram_delivery_ready_v2" && table != "frontend_projection_ready_v1" && table != "run_idempotency" {
		return domain.ErrMembershipDenied
	}
	if err := g.expect(table, key, values); err != nil {
		return err
	}
	identity := deletionCaptureIdentity(table, captureScopeKey(deletionCaptureScope{spec: deletionCaptureSpecs[table], prefix: key}))
	g.optionalExpected[identity] = true
	return nil
}

func (g *deletionCaptureGraph) addObject(ref domain.BlobRef) error {
	if ref.Validate() != nil || ref.TenantID != g.inventory.TenantID {
		return domain.ErrMembershipDenied
	}
	owned := strings.HasPrefix(ref.Key, domain.SessionObjectPrefix(g.inventory.TenantID, g.inventory.SessionID))
	for run := range g.runDigests {
		owned = owned || strings.HasPrefix(ref.Key, domain.RunObjectPrefix(g.inventory.TenantID, run))
	}
	if !owned {
		return domain.ErrMembershipDenied
	}
	if old, ok := g.objects[ref.Key]; ok && old != ref {
		return domain.ErrMembershipDenied
	}
	g.objects[ref.Key] = ref
	if uint64(len(g.objects)) > maxSessionDeletionRows {
		return domain.ErrMembershipDenied
	}
	return nil
}

func deletionCaptureJSONRows[T any](ctx context.Context, q stateSQLExecutor, statement string, limit uint64, args ...any) (out []T, err error) {
	if limit == 0 || limit > maxSessionDeletionRows+1 {
		return nil, domain.ErrMembershipDenied
	}
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := rows.Close(); err == nil {
			err = e
		}
	}()
	for rows.Next() {
		if uint64(len(out)) >= limit {
			return nil, domain.ErrMembershipDenied
		}
		var raw string
		var value T
		if rows.Scan(&raw) != nil || chargeDeletionCaptureSQL(q, uint64(len(raw))) != nil || len(raw) > ownedAuthorityHeadBytes || decodeOwnedEffectCanonicalJSON(raw, &value) != nil {
			return nil, domain.ErrMembershipDenied
		}
		out = append(out, value)
	}
	return out, rows.Err()
}

func chargeDeletionCaptureSQL(q stateSQLExecutor, n uint64) error {
	if capture, ok := q.(*deletionCaptureQuery); ok {
		if run, ok := capture.query.(*retainedDeletionRunQuery); ok {
			if capture.budget.charge(0) != nil {
				return domain.ErrMembershipDenied
			}
			return run.run.budget.materialized(run.run.query.callback, n)
		}
		if err := capture.budget.charge(n); err != nil {
			return err
		}
		if owned, ok := capture.query.(*ownedAuthorityQuery); ok {
			return owned.budget.materialized(owned.callback, n)
		}
		return nil
	}
	if owned, ok := q.(*ownedAuthorityQuery); ok {
		return owned.budget.materialized(owned.callback, n)
	}
	return domain.ErrMembershipDenied // No ordinary-Tx uncharged decoder path.
}

func (g *deletionCaptureGraph) addParticipantScopes(ctx context.Context, session domain.Session) (err error) {
	bucket, err := ydbpartition.BucketV1(string(session.ID))
	if err != nil {
		return err
	}
	rows, err := g.query.QueryContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 ORDER BY user_id LIMIT $3`, session.TenantID, session.ID, maxSessionDeletionRows+1)
	if err != nil {
		return err
	}
	defer func() {
		if e := rows.Close(); err == nil {
			err = e
		}
	}()
	var previous domain.UserID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		if chargeDeletionCaptureSQL(g.query, uint64(len(raw))) != nil || len(raw) > ownedAuthorityHeadBytes {
			return domain.ErrMembershipDenied
		}
		var p domain.SessionParticipant
		if decodeOwnedEffectCanonicalJSON(raw, &p) != nil || p.Validate() != nil || p.TenantID != session.TenantID || p.SessionID != session.ID || p.UserID <= previous {
			return domain.ErrMembershipDenied
		}
		previous = p.UserID
		if err := g.expect("session_participants", []any{p.TenantID, p.SessionID, p.UserID}, map[string]any{"record": p}); err != nil {
			return err
		}
		if p.Status == domain.SessionParticipantActive {
			if err := g.add("session_activity", p.TenantID, p.UserID, session.Status, bucket, session.UpdatedAt, session.ID); err != nil {
				return err
			}
		}
	}
	return rows.Err()
}

// Discovery transfers only bounded identities, never unbounded legacy JSON or
// an ID which is truncated and silently accepted. All SQL names are private
// schema-owned constants. The full capture later proves scalar/PK/EOF facts.
func discoverDeletionCaptureIDs(ctx context.Context, q stateSQLExecutor, table, columns string, prefix ...any) (out [][]string, err error) {
	spec := deletionCaptureSpecs[table]
	if spec == nil || len(prefix) == 0 || len(prefix) > len(spec.keys) {
		return nil, domain.ErrMembershipDenied
	}
	var selectSQL, where []string
	for _, column := range strings.Split(columns, ",") {
		found := false
		for _, c := range spec.columns {
			if c.name == column && c.kind == 's' {
				found = true
			}
		}
		if !found {
			return nil, domain.ErrMembershipDenied
		}
		selectSQL = append(selectSQL, "SUBSTRING(CAST("+column+" AS String),0,161)")
	}
	if len(selectSQL) == 0 || len(selectSQL) > 2 {
		return nil, domain.ErrMembershipDenied
	}
	for i, key := range spec.keys[:len(prefix)] {
		where = append(where, fmt.Sprintf("%s=$%d", key, i+1))
	}
	statement := "SELECT " + strings.Join(selectSQL, ",") + " FROM " + table + " WHERE " + strings.Join(where, " AND ") + " ORDER BY " + strings.Join(spec.keys, ",") + fmt.Sprintf(" LIMIT $%d", len(prefix)+1)
	args := append(append([]any(nil), prefix...), maxSessionDeletionRows+1)
	rows, err := q.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := rows.Close(); err == nil {
			err = e
		}
	}()
	seen := map[string]bool{}
	for rows.Next() {
		if uint64(len(out)) >= maxSessionDeletionRows {
			return nil, domain.ErrMembershipDenied
		}
		values := make([]string, len(selectSQL))
		dst := make([]any, len(values))
		for i := range values {
			dst[i] = &values[i]
		}
		if err := rows.Scan(dst...); err != nil {
			return nil, err
		}
		var materialized uint64
		for _, v := range values {
			materialized += uint64(len(v))
		}
		if chargeDeletionCaptureSQL(q, materialized) != nil {
			return nil, domain.ErrMembershipDenied
		}
		for _, v := range values {
			if len(v) > 160 || !utf8.ValidString(v) || domain.ValidateOpaqueID("retained_deletion.identity", v) != nil {
				return nil, domain.ErrMembershipDenied
			}
		}
		key, _ := json.Marshal(values)
		if seen[string(key)] {
			return nil, domain.ErrMembershipDenied
		}
		seen[string(key)] = true
		out = append(out, values)
	}
	return out, rows.Err()
}

func (g *deletionCaptureGraph) addAdmissionScopes(ctx context.Context, tenant domain.TenantID, session domain.SessionID) error {
	rows, err := discoverDeletionCaptureIDs(ctx, g.query, "attempt_execution_admissions_by_session_v1", "attempt_id,run_id", tenant, session)
	if err != nil {
		return err
	}
	for _, row := range rows {
		key := ownedSessionAdmissionKey{attempt: domain.AttemptID(row[0]), run: domain.RunID(row[1])}
		if g.runDigests[key.run] == "" {
			return domain.ErrMembershipDenied
		}
		g.admissions = append(g.admissions, key)
		if err := g.expect("attempt_execution_admissions_by_session_v1", []any{tenant, session, key.attempt}, map[string]any{"run_id": key.run}); err != nil {
			return err
		}
		if err := g.expect("attempt_execution_admissions_v1", []any{tenant, key.attempt}, map[string]any{"run_id": key.run, "session_id": session}); err != nil {
			return err
		}
		// Deliberately enumerate the whole parent KIND range and every child
		// range for EVERY reverse admission, not just Run.TerminalCustody's
		// selected Attempt. Unknown kinds cannot hide behind a point query.
		for _, table := range []string{"attempt_execution_admissions_v1", "attempt_effect_reservations", "substrate_execution_evidence", "attempt_operations_v1", "attempt_operation_calls_v1", "attempt_operation_proxies_v1", "attempt_effect_reconciliation_evidence"} {
			original := g.query
			if g.work != nil {
				runQuery, e := g.work.forRun(key.run)
				if e != nil {
					return e
				}
				g.query = &deletionCaptureQuery{query: runQuery, budget: original.(*deletionCaptureQuery).budget}
			}
			err := g.add(table, tenant, key.attempt)
			g.query = original
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// A complete physical postimage is committed, but it must also describe the
// same canonical owner as its JSON/scalar/index copies. A digest alone does
// not bless an already malformed initial database snapshot.
func (g *deletionCaptureGraph) observeRow(spec *deletionCaptureSpec, cells []deletionCaptureCell) error {
	byName := make(map[string]deletionCaptureCell, len(cells))
	for i, c := range spec.columns {
		byName[c.name] = cells[i]
	}
	if cell, ok := byName["attempt_id"]; ok && cell.Present {
		var attempt domain.AttemptID
		if json.Unmarshal(cell.Value, &attempt) != nil {
			return domain.ErrMembershipDenied
		}
		if g.attemptRows == nil {
			g.attemptRows = map[domain.AttemptID]retainedDeletionAttemptRows{}
		}
		counts := g.attemptRows[attempt]
		switch spec.table {
		case "attempt_effect_reservations":
			counts.parents++
		case "substrate_execution_evidence":
			counts.evidence++
		case "attempt_operations_v1":
			counts.operations++
		case "attempt_operation_calls_v1":
			counts.calls++
		case "attempt_operation_proxies_v1":
			counts.proxies++
		case "attempt_effect_reconciliation_evidence":
			counts.reconciliation++
		}
		g.attemptRows[attempt] = counts
	}
	key := make([]deletionCaptureCell, len(spec.keys))
	for i, k := range spec.keys {
		key[i] = byName[k]
	}
	identity := deletionCaptureIdentity(spec.table, key)
	delete(g.required, identity)
	switch spec.table {
	case "runs_by_session", "artifact_manifests_by_run", "checkpoint_objects_by_run", "telegram_deliveries_by_run", "frontend_bindings_by_session", "frontend_binding_keys", "frontend_projections_by_session", "frontend_projections_by_run", "session_event_idempotency", "attempt_execution_admissions_by_session_v1":
		if g.expected[identity] == nil {
			return domain.ErrMembershipDenied
		}
	}
	for column, want := range g.expected[identity] {
		got := byName[column]
		if !got.Present {
			return domain.ErrMembershipDenied
		}
		a, e := canonicalCaptureJSON(string(got.Value))
		b, f := canonicalCaptureJSON(string(want))
		if e != nil || f != nil || string(a) != string(b) {
			return domain.ErrMembershipDenied
		}
	}
	delete(g.expected, identity)
	var raw json.RawMessage
	for _, column := range []string{"record", "payload"} {
		if cell := byName[column]; cell.Present && cell.Kind == "j" {
			raw = cell.Value
			break
		}
	}
	if raw == nil {
		switch spec.table {
		case "attempt_execution_admissions_v1", "session_material_current_v1", "session_material_boundaries_v1":
			var record string
			if !byName["record"].Present || json.Unmarshal(byName["record"].Value, &record) != nil {
				return domain.ErrMembershipDenied
			}
			raw = json.RawMessage(record)
		}
	}
	if spec.table == "session_snapshot_material_v1" {
		return domain.ErrMembershipDenied
	} // No published producer/codec exists; never invent historical DATA.
	if spec.table == "attempt_effect_reservations" {
		return g.validateParentRow(byName)
	}
	if len(raw) != 0 {
		if err := validateDeletionCaptureScalars(spec.table, byName, raw); err != nil {
			return err
		}
	}
	decode := func(dst any) error {
		if len(raw) == 0 || json.Unmarshal(raw, dst) != nil {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	switch spec.table {
	case "runs":
		var run domain.Run
		if decode(&run) != nil || run.Validate() != nil || !run.Status.Terminal() {
			return domain.ErrMembershipDenied
		}
		digest, e := deletionRunDigest(run)
		if e != nil || g.runDigests[run.ID] != digest {
			return domain.ErrMembershipDenied
		}
		g.inventory.RunRows++
	case "session_events":
		var event domain.SessionEvent
		if decode(&event) != nil || event.Validate() != nil || !reflect.DeepEqual(g.ownership.events[event.ID], event) {
			return domain.ErrMembershipDenied
		}
		g.inventory.EventRows++
		return g.addObject(event.Payload)
	case "session_snapshots":
		var snapshot domain.SessionSnapshot
		if decode(&snapshot) != nil || snapshot.Validate() != nil {
			return domain.ErrMembershipDenied
		}
		g.inventory.SnapshotRows++
		return g.addObject(snapshot.Payload)
	case "session_participants":
		var participant domain.SessionParticipant
		if decode(&participant) != nil || participant.Validate() != nil || participant.TenantID != g.inventory.TenantID || participant.SessionID != g.inventory.SessionID {
			return domain.ErrMembershipDenied
		}
		g.inventory.ParticipantRows++
	case "attempt_execution_admissions_v1":
		admission, e := domain.DecodeAttemptExecutionAdmissionV1(raw)
		if e != nil || admission.Source.Scope.TenantID != g.inventory.TenantID || admission.Source.Scope.SessionID != g.inventory.SessionID || g.runDigests[admission.Source.Scope.RunID] == "" {
			return domain.ErrMembershipDenied
		}
		digest, e := admission.Digest()
		if e != nil || !deletionCaptureCellEqual(byName["digest"], digest) {
			return domain.ErrMembershipDenied
		}
	case "frontend_bindings_by_session":
		g.inventory.BindingRows++
	case "frontend_projections_by_session":
		g.inventory.ProjectionRows++
	case "attempt_execution_admissions_by_session_v1":
		g.inventory.OwnedAdmissionRows++
	case "session_material_current_v1":
		g.inventory.MaterialCurrentRows++
	case "session_material_boundaries_v1":
		g.inventory.MaterialBoundaryRows++
	case "session_snapshot_material_v1":
		g.inventory.MaterialSnapshotRows++
	case "artifact_manifests":
		var manifest domain.ArtifactManifest
		if decode(&manifest) != nil || manifest.ID.Validate() != nil || manifest.TenantID != g.inventory.TenantID || g.runDigests[manifest.RunID] == "" || manifest.CreatedAt.IsZero() {
			return domain.ErrMembershipDenied
		}
		g.inventory.ManifestRows++
		for _, a := range manifest.Artifacts {
			if a.Validate() != nil {
				return domain.ErrMembershipDenied
			}
			if err := g.addObject(a.Blob); err != nil {
				return err
			}
		}
	case "telegram_delivery_outbox":
		var delivery domain.TelegramDeliveryOutbox
		if decode(&delivery) != nil || delivery.RunID.Validate() != nil || g.runDigests[delivery.RunID] == "" {
			return domain.ErrMembershipDenied
		}
		g.inventory.DeliveryRows++
		if delivery.Payload.Key != "" {
			return g.addObject(delivery.Payload)
		}
	case "checkpoints":
		var checkpoint domain.Checkpoint
		if decode(&checkpoint) != nil || checkpoint.ID.Validate() != nil || checkpoint.AttemptID.Validate() != nil || checkpoint.Sequence == 0 || g.runDigests[checkpoint.RunID] == "" {
			return domain.ErrMembershipDenied
		}
		g.inventory.CheckpointRows++
		return g.addObject(checkpoint.State)
	case "attached_worker_output_receipts":
		receipt, err := decodeAttachedWorkerReceiptEnvelope(raw)
		if err != nil || !receipt.deletionSettled() {
			return domain.ErrMembershipDenied
		}
		var tenant domain.TenantID
		var run domain.RunID
		var owner domain.UserID
		var worker domain.AttachedWorkerID
		var attempt domain.AttemptID
		var generation uint64
		for k, dst := range map[string]any{"tenant_id": &tenant, "run_id": &run, "owner_user_id": &owner, "worker_id": &worker, "attempt_id": &attempt, "lease_generation": &generation} {
			if json.Unmarshal(byName[k].Value, dst) != nil {
				return domain.ErrMembershipDenied
			}
		}
		if receipt.validateIdentity(tenant, run, owner, worker, attempt, generation) != nil || g.runDigests[run] == "" {
			return domain.ErrMembershipDenied
		}
		g.inventory.ReceiptRows++
		for _, ref := range receipt.objectRefs() {
			if err := g.addObject(ref); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDeletionCaptureScalars(table string, cells map[string]deletionCaptureCell, body json.RawMessage) error {
	// Reuse the original typed shape, not a map that silently accepts missing
	// zero-valued fields. Optional SQL NULL/empty representations below are
	// explicit producer contracts, never a generic missing-field exception.
	var target any
	switch table {
	case "sessions":
		target = new(domain.Session)
	case "runs":
		target = new(domain.Run)
	case "session_events":
		target = new(domain.SessionEvent)
	case "session_snapshots":
		target = new(domain.SessionSnapshot)
	case "session_participants":
		target = new(domain.SessionParticipant)
	case "session_displays":
		target = new(domain.SessionDisplay)
	case "frontend_bindings":
		target = new(domain.FrontendBinding)
	case "frontend_projection_outbox":
		target = new(domain.FrontendProjection)
	case "artifact_manifests":
		target = new(domain.ArtifactManifest)
	case "telegram_delivery_outbox", "telegram_deliveries_by_run":
		target = new(domain.TelegramDeliveryOutbox)
	case "checkpoints", "checkpoint_objects_by_run":
		target = new(domain.Checkpoint)
	case "worker_jobs":
		target = new(domain.WorkerJob)
	case "run_explanation_heads_v1":
		target = new(runexplanation.HeadV1)
	case "attached_worker_output_receipts":
		envelope, err := decodeAttachedWorkerReceiptEnvelope(body)
		if err != nil {
			return domain.ErrMembershipDenied
		}
		if envelope.v1 != nil {
			target = new(AttachedWorkerOutputReceiptV1)
		} else {
			target = new(AttachedWorkerOutputReceiptV2)
		}
	case "substrate_execution_evidence":
		target = new(domain.ExecutionEvidenceV2)
	case "attempt_effect_reconciliation_evidence":
		target = new(domain.AttemptEffectReconciliationEvidenceV1)
	case "attempt_execution_admissions_v1":
		target = new(domain.AttemptExecutionAdmissionV1)
	case "session_material_current_v1", "session_material_boundaries_v1":
		if _, err := sessioncontext.DecodeSessionMaterialPrefixV1(body); err != nil {
			return domain.ErrMembershipDenied
		}
		target = new(sessioncontext.SessionMaterialPrefixV1)
	default:
		return domain.ErrMembershipDenied
	}
	if decodeOwnedEffectCanonicalJSON(string(body), target) != nil {
		return domain.ErrMembershipDenied
	}
	mapping := deletionCaptureScalarPaths[table]
	if mapping == "" {
		return domain.ErrMembershipDenied
	}
	return compareDeletionCapturePaths(cells, body, mapping)
}

// Every scalar which is a copy of the typed record has a REQUIRED path. Only
// separately computed physical TTLs are excluded from correspondence; they
// remain in the exact full-postimage commitment. '?' means absent=>SQL NULL;
// '~' means absent=>the original writer's present empty String (material seed).
var deletionCaptureScalarPaths = map[string]string{
	"sessions":                               "tenant_id,session_id=id,created_by,status,last_event_sequence,created_at,updated_at,archived_at=?archived_at",
	"runs":                                   "tenant_id,run_id=id,session_id,trigger_event_id,subscription_connection_id,status,created_at,updated_at",
	"session_events":                         "tenant_id,session_id,sequence,event_id=id,kind,author_user_id=?author_user_id,run_id=?run_id,idempotency_key,blob_key=payload.key,created_at",
	"session_snapshots":                      "tenant_id,session_id,version,snapshot_id=id,through_sequence,blob_key=payload.key,created_at",
	"session_participants":                   "tenant_id,session_id,user_id,role,status,created_at,updated_at",
	"session_displays":                       "tenant_id,session_id,updated_at",
	"frontend_bindings":                      "tenant_id,binding_id=id,frontend,external_conversation_id,session_id,revision,created_at,updated_at",
	"frontend_projection_outbox":             "tenant_id,frontend_projection_id=id,session_id,event_id,event_sequence,binding_id,binding_revision,frontend,status,created_at,updated_at",
	"artifact_manifests":                     "tenant_id,artifact_manifest_id=id,run_id,created_at",
	"telegram_delivery_outbox":               "tenant_id,telegram_delivery_id=id,run_id,status,next_attempt_at=?next_attempt_at,created_at,updated_at",
	"telegram_deliveries_by_run":             "tenant_id,run_id,telegram_delivery_id=id",
	"checkpoints":                            "tenant_id,attempt_id,sequence,checkpoint_id=id,run_id,blob_key=state.key,created_at",
	"checkpoint_objects_by_run":              "tenant_id,run_id,checkpoint_id=id",
	"worker_jobs":                            "tenant_id,run_id,attempt_id,reservation_id,created_at",
	"run_explanation_heads_v1":               "tenant_id=TenantID,run_id=RunID,revision=Revision",
	"attached_worker_output_receipts":        "tenant_id=binding.TenantID,run_id=binding.RunID,owner_user_id=binding.OwnerUserID,worker_id=binding.WorkerID,attempt_id=binding.AttemptID,lease_generation=binding.LeaseGeneration,canonical_digest,candidate_fingerprint,created_at",
	"substrate_execution_evidence":           "tenant_id=local_v1.scope.tenant_id,attempt_id=local_v1.scope.attempt_id,run_id=local_v1.scope.run_id,physical_invocation_claim_id=local_v1.scope.physical_invocation_claim_id,evidence_digest=local_v1.evidence_digest",
	"attempt_effect_reconciliation_evidence": "evidence_digest=EvidenceDigest,physical_invocation_claim_id=PhysicalInvocationClaimID,operation_state=Observation.State,observed_at=Observation.ObservedAt",
	"attempt_execution_admissions_v1":        "tenant_id=source.scope.tenant_id,attempt_id=source.scope.attempt_id,run_id=source.scope.run_id,session_id=source.scope.session_id,owner_user_id=source.scope.owner_user_id,reservation_id,admitted_at",
	"session_material_current_v1":            "tenant_id,session_id,version,through_sequence,last_event_id=~last_event_id,prefix_digest",
	"session_material_boundaries_v1":         "tenant_id,session_id,sequence=through_sequence,version,through_sequence,last_event_id=~last_event_id,prefix_digest",
}

func compareDeletionCapturePaths(cells map[string]deletionCaptureCell, body json.RawMessage, mapping string) error {
	for _, item := range strings.Split(mapping, ",") {
		column, path, split := strings.Cut(item, "=")
		if !split {
			path = column
		}
		optionalNull, optionalEmpty := strings.HasPrefix(path, "?"), strings.HasPrefix(path, "~")
		path = strings.TrimLeft(path, "?~")
		cell, exists := cells[column]
		if !exists {
			return domain.ErrMembershipDenied
		}
		want := body
		found := true
		for _, part := range strings.Split(path, ".") {
			var object map[string]json.RawMessage
			if json.Unmarshal(want, &object) != nil {
				found = false
				break
			}
			want, found = object[part]
			if !found {
				break
			}
		}
		if !found {
			switch {
			case optionalNull:
				if cell.Present {
					return domain.ErrMembershipDenied
				}
				continue
			case optionalEmpty:
				if !deletionCaptureCellEqual(cell, "") {
					return domain.ErrMembershipDenied
				}
				continue
			default:
				return domain.ErrMembershipDenied
			}
		}
		if !cell.Present || string(want) == "null" {
			return domain.ErrMembershipDenied
		}
		if cell.Kind == "t" {
			var value string
			if json.Unmarshal(want, &value) != nil {
				return domain.ErrMembershipDenied
			}
			at, e := time.Parse(time.RFC3339Nano, value)
			if e != nil || at.Nanosecond()%1000 != 0 {
				return domain.ErrMembershipDenied
			}
			want, _ = json.Marshal(at.UTC().Format("2006-01-02T15:04:05.000000Z"))
		}
		if string(want) != string(cell.Value) {
			return domain.ErrMembershipDenied
		}
	}
	return nil
}

func deletionCaptureCellEqual(cell deletionCaptureCell, value any) bool {
	expected, err := json.Marshal(value)
	return err == nil && cell.Present && string(expected) == string(cell.Value)
}

func (g *deletionCaptureGraph) validateParentRow(cells map[string]deletionCaptureCell) error {
	if !deletionCaptureCellEqual(cells["kind"], domain.ProviderEffectTurnV1) || cells["record"].Present || !cells["owned_record"].Present {
		return domain.ErrMembershipDenied
	}
	var raw string
	if json.Unmarshal(cells["owned_record"].Value, &raw) != nil {
		return domain.ErrMembershipDenied
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(raw))
	if err != nil {
		return domain.ErrMembershipDenied
	}
	if parent.Scope.TenantID != g.inventory.TenantID || parent.Scope.SessionID != g.inventory.SessionID || g.runDigests[parent.Scope.RunID] == "" {
		return domain.ErrMembershipDenied
	}
	found := false
	for _, a := range g.admissions {
		if a.attempt == parent.Scope.AttemptID && a.run == parent.Scope.RunID {
			found = true
		}
	}
	if !found {
		return domain.ErrMembershipDenied
	}
	digest, err := parent.Digest()
	if err != nil || !deletionCaptureCellEqual(cells["owned_digest"], digest) {
		return domain.ErrMembershipDenied
	}
	return compareDeletionCapturePaths(cells, json.RawMessage(raw), "tenant_id=reservation.tenant_id,attempt_id=reservation.attempt_id,kind=reservation.kind,run_id=reservation.run_id,lease_id=reservation.lease_id,fence_token=reservation.fence_token,physical_invocation_claim_id=reservation.physical_invocation_claim_id,effect_sequence=reservation.effect_sequence,invocation_authority_digest=reservation.invocation_authority_digest,reserved_at=reservation.reserved_at")
}
