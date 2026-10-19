package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionlifecycle"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// Test-only SQL postimages from a GENUINE closed producer. Neither this
// adapter nor its rows are trusted runtime capabilities or native YDB proof.
// Scalars project the original writer fixtures' durable values; no successful
// marker, receipt, original allocation, reservation or evidence is invented.
type retainedDeletionGraphFixture struct {
	base         driver.Connector
	tables       map[string][]map[string]driver.Value
	request      domain.SessionDeletion
	lastSQL      string
	membership   string
	writes       int
	hold         *domain.SessionLegalHold
	requested    bool
	lostStartACK bool
}

func newRetainedDeletionGraphFixture(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture) *retainedDeletionGraphFixture {
	t.Helper()
	s := completed.Snapshot(t)
	f := &retainedDeletionGraphFixture{base: retainedDeletionWriterConnector{base: freshRunHistoryConnector{base: terminalCompletionConnector{f: completed}, history: localFreshCommittedHistory(t, seed, s, nil)}, seed: seed}, tables: map[string][]map[string]driver.Value{}}
	f.request = domain.SessionDeletion{TenantID: s.Run.TenantID, SessionID: s.Run.SessionID, RequestedBy: seed.Scope.OwnerUserID, Reason: "controlled retained deletion fixture", State: domain.SessionDeletionRequested, RequestedAt: s.Run.UpdatedAt.Add(time.Microsecond)}
	f.requested = true
	if len(seed.parent.base.membership) == 0 {
		t.Fatal("original membership fixture")
	}
	f.membership = seed.parent.base.membership[0].(string)
	record := func(table string, v any, aliases map[string]any) {
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		var data map[string]json.RawMessage
		if json.Unmarshal(b, &data) != nil {
			t.Fatal("original typed record")
		}
		m := map[string]driver.Value{}
		for _, c := range deletionCaptureSpecs[table].columns {
			if c.name == "record" || c.name == "payload" {
				m[c.name] = string(b)
				continue
			}
			raw := data[c.name]
			if raw == nil || string(raw) == "null" {
				continue
			}
			switch c.kind {
			case 's':
				var v string
				if json.Unmarshal(raw, &v) != nil {
					t.Fatal(table, c.name)
				}
				m[c.name] = v
			case 'u', 'n':
				var v uint64
				if json.Unmarshal(raw, &v) != nil {
					t.Fatal(table, c.name)
				}
				m[c.name] = int64(v)
			case 't':
				var v time.Time
				if json.Unmarshal(raw, &v) != nil {
					t.Fatal(table, c.name)
				}
				m[c.name] = v.UTC()
			}
		}
		for k, v := range aliases {
			switch v := v.(type) {
			case domain.TenantID:
				m[k] = string(v)
			case domain.SessionID:
				m[k] = string(v)
			case domain.RunID:
				m[k] = string(v)
			case domain.ArtifactManifestID:
				m[k] = string(v)
			case domain.SessionEventID:
				m[k] = string(v)
			case uint64:
				m[k] = int64(v)
			default:
				m[k] = v
			}
		}
		f.tables[table] = append(f.tables[table], m)
	}
	record("sessions", s.Session, map[string]any{"session_id": s.Session.ID})
	var participant domain.SessionParticipant
	if len(seed.parent.base.participant) == 0 || json.Unmarshal([]byte(seed.parent.base.participant[0].(string)), &participant) != nil {
		t.Fatal("original participant durable fixture")
	}
	record("session_participants", participant, nil)
	bucket, e := ydbpartition.BucketV1(string(s.Session.ID))
	if e != nil {
		t.Fatal(e)
	}
	f.tables["session_activity"] = []map[string]driver.Value{{"tenant_id": string(s.Session.TenantID), "user_id": string(participant.UserID), "status": string(s.Session.Status), "activity_bucket": int64(bucket), "updated_at": s.Session.UpdatedAt, "session_id": string(s.Session.ID)}}
	for _, event := range append(seed.CanonicalSourceEvents(t), s.Events...) {
		record("session_events", event, map[string]any{"event_id": event.ID, "blob_key": event.Payload.Key})
		f.tables["session_event_idempotency"] = append(f.tables["session_event_idempotency"], map[string]driver.Value{"tenant_id": string(event.TenantID), "session_id": string(event.SessionID), "idempotency_key": string(event.IdempotencyKey), "sequence": int64(event.Sequence), "event_id": string(event.ID), "created_at": event.CreatedAt})
	}
	for key, row := range completed.state.material {
		if len(row) != 7 {
			t.Fatal("original material durable columns")
		}
		m := map[string]driver.Value{"record": row[0], "version": row[1], "through_sequence": row[2], "tenant_id": row[3], "session_id": row[4], "last_event_id": row[5], "prefix_digest": row[6]}
		table := "session_material_current_v1"
		if key != "current" {
			table = "session_material_boundaries_v1"
			m["sequence"] = row[2]
		}
		f.tables[table] = append(f.tables[table], m)
	}
	record("runs", s.Run, map[string]any{"run_id": s.Run.ID})
	f.tables["runs_by_session"] = []map[string]driver.Value{{"tenant_id": string(s.Run.TenantID), "session_id": string(s.Run.SessionID), "created_at": s.Run.CreatedAt, "run_id": string(s.Run.ID), "trigger_event_id": string(s.Run.TriggerEventID), "status": string(s.Run.Status)}}
	f.tables["run_finalizations"] = []map[string]driver.Value{{"tenant_id": string(s.Run.TenantID), "run_id": string(s.Run.ID), "terminal_status": s.FinalStatus, "content_digest": s.FinalDigest, "created_at": s.Run.UpdatedAt}}
	var job domain.WorkerJob
	if json.Unmarshal([]byte(seed.parent.graph.rows["worker_jobs"][0].(string)), &job) != nil {
		t.Fatal("original WorkerJob")
	}
	record("worker_jobs", job, nil)
	var input domain.ArtifactManifest
	if json.Unmarshal([]byte(seed.parent.graph.rows["artifact_manifests"][0].(string)), &input) != nil {
		t.Fatal("original input manifest")
	}
	for _, manifest := range []domain.ArtifactManifest{input, s.Manifest} {
		record("artifact_manifests", manifest, map[string]any{"artifact_manifest_id": manifest.ID})
		f.tables["artifact_manifests_by_run"] = append(f.tables["artifact_manifests_by_run"], map[string]driver.Value{"tenant_id": string(manifest.TenantID), "run_id": string(manifest.RunID), "artifact_manifest_id": string(manifest.ID)})
	}
	a := seed.parent.base.final
	if len(a) != 7 {
		t.Fatal("original admission columns")
	}
	f.tables["attempt_execution_admissions_v1"] = []map[string]driver.Value{{"tenant_id": string(seed.Scope.TenantID), "attempt_id": string(seed.Scope.AttemptID), "record": a[0], "digest": a[1], "run_id": a[2], "session_id": a[3], "owner_user_id": a[4], "reservation_id": a[5], "admitted_at": a[6]}}
	f.tables["attempt_execution_admissions_by_session_v1"] = []map[string]driver.Value{{"tenant_id": string(seed.Scope.TenantID), "session_id": string(seed.Scope.SessionID), "attempt_id": string(seed.Scope.AttemptID), "run_id": string(seed.Scope.RunID)}}
	p := seed.parent.sql.parentRow
	if len(p) != 15 {
		t.Fatal("original parent columns")
	}
	parent := map[string]driver.Value{}
	for i, k := range []string{"tenant_id", "attempt_id", "kind", "run_id", "lease_id", "fence_token", "physical_invocation_claim_id", "effect_sequence", "invocation_authority_digest", "reserved_at"} {
		parent[k] = p[i]
	}
	parent["owned_record"], parent["owned_digest"] = p[13], p[14]
	burn := seed.parent.sql.burnRow
	if len(burn) != 4 {
		t.Fatal("original preparation burn")
	}
	parent["preparation_started_at"], parent["preparation_parent_digest"] = burn[2], burn[3]
	allocation := seed.sql.localProxySQL.f.row
	if len(allocation) != 12 {
		t.Fatal("original allocation")
	}
	for i, k := range []string{"allocation_original_record", "allocation_original_digest", "allocation_current_record", "allocation_current_digest", "allocation_publication_digest", "allocation_transition_parent_digest"} {
		if allocation[i].(uint64) != 0 {
			parent[k] = allocation[i+6]
		}
	}
	proxy := seed.compaction.firstProxy
	if len(proxy) != 12 {
		t.Fatal("original first proxy")
	}
	for i, k := range []string{"proxy_original_record", "proxy_original_digest", "proxy_current_record", "proxy_current_digest", "proxy_policy_body", "proxy_attestation_body"} {
		if proxy[i].(uint64) != 0 {
			parent[k] = proxy[i+6]
		}
	}
	accounting := seed.parent.sql.accountingRow
	if len(accounting) != 4 {
		t.Fatal("original accounting")
	}
	parent["operation_accounting_record"], parent["operation_accounting_digest"] = accounting[2], accounting[3]
	f.tables["attempt_effect_reservations"] = []map[string]driver.Value{parent}
	for _, row := range seed.compaction.operations {
		m := map[string]driver.Value{"tenant_id": string(seed.Scope.TenantID), "attempt_id": string(seed.Scope.AttemptID), "sequence": row[0], "record": row[1], "digest": row[2], "operation_digest": row[3], "call_id": row[4]}
		if row[5].(uint64) != 0 {
			m["observation_record"] = row[7]
		}
		if row[6].(uint64) != 0 {
			m["observation_digest"] = row[8]
		}
		f.tables["attempt_operations_v1"] = append(f.tables["attempt_operations_v1"], m)
		f.tables["attempt_operation_calls_v1"] = append(f.tables["attempt_operation_calls_v1"], map[string]driver.Value{"tenant_id": m["tenant_id"], "attempt_id": m["attempt_id"], "sequence": row[9], "operation_digest": row[10], "call_id": m["call_id"]})
	}
	for _, row := range seed.compaction.proxies {
		m := map[string]driver.Value{"tenant_id": string(seed.Scope.TenantID), "attempt_id": string(seed.Scope.AttemptID), "sequence": row[0], "service_kind": row[13]}
		for i, k := range []string{"original_record", "original_digest", "current_record", "current_digest", "policy_body", "attestation_body"} {
			if row[i+1].(uint64) != 0 {
				m[k] = row[i+7]
			}
		}
		f.tables["attempt_operation_proxies_v1"] = append(f.tables["attempt_operation_proxies_v1"], m)
	}
	f.tables["substrate_execution_evidence"] = []map[string]driver.Value{{"tenant_id": string(seed.Scope.TenantID), "attempt_id": string(seed.Scope.AttemptID), "run_id": string(seed.Scope.RunID), "physical_invocation_claim_id": seed.Request.PhysicalInvocationClaimID, "evidence_digest": s.EvidenceDigest, "recorded_at": s.Run.UpdatedAt, "record": s.EvidenceRecord}}
	f.tables["session_lifecycle_backfill_state"] = []map[string]driver.Value{{"backfill_id": sessionLifecycleBackfillID, "completed_at": s.Run.CreatedAt}}
	return f
}

func (f *retainedDeletionGraphFixture) Driver() driver.Driver { return f.base.Driver() }
func (f *retainedDeletionGraphFixture) Connect(ctx context.Context) (driver.Conn, error) {
	c, e := f.base.Connect(ctx)
	if e != nil {
		return nil, e
	}
	return &retainedDeletionGraphConn{Conn: c, f: f}, nil
}

type retainedDeletionGraphConn struct {
	driver.Conn
	f *retainedDeletionGraphFixture
}

func (c *retainedDeletionGraphConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, e := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	rows := map[string][]map[string]driver.Value{}
	for table, items := range c.f.tables {
		for _, item := range items {
			copyRow := map[string]driver.Value{}
			for k, v := range item {
				copyRow[k] = v
			}
			rows[table] = append(rows[table], copyRow)
		}
	}
	return &retainedDeletionGraphTx{Tx: tx, f: c.f, request: c.f.request, requested: c.f.requested, writes: c.f.writes, rows: rows}, nil
}

type retainedDeletionGraphTx struct {
	driver.Tx
	f         *retainedDeletionGraphFixture
	request   domain.SessionDeletion
	writes    int
	rows      map[string][]map[string]driver.Value
	requested bool
}

func (tx *retainedDeletionGraphTx) Rollback() error {
	tx.f.request = tx.request
	tx.f.requested = tx.requested
	tx.f.writes = tx.writes
	tx.f.tables = tx.rows
	return tx.Tx.Rollback()
}
func (tx *retainedDeletionGraphTx) Commit() error {
	e := tx.Tx.Commit()
	if e != nil {
		tx.f.request = tx.request
		tx.f.requested = tx.requested
		tx.f.writes = tx.writes
		tx.f.tables = tx.rows
	}
	if e == nil && tx.f.lostStartACK && tx.request.State == domain.SessionDeletionRequested && tx.f.request.State == domain.SessionDeletionDeleting {
		tx.f.lostStartACK = false
		return errors.New("controlled durable Start lost commit acknowledgement")
	}
	return e
}
func (c *retainedDeletionGraphConn) ExecContext(ctx context.Context, s string, a []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if completionSQL(s) == completionSQL(`UPSERT INTO session_deletions (tenant_id, session_id, state, requested_at, started_at, completed_at, record) VALUES ($1, $2, $3, $4, $5, $6, CAST($7 AS JsonDocument))`) {
		if len(a) != 7 || a[0].Value != string(c.f.request.TenantID) || a[1].Value != string(c.f.request.SessionID) {
			return nil, errors.New("retained Start exact tombstone scope")
		}
		var v domain.SessionDeletion
		if json.Unmarshal([]byte(a[6].Value.(string)), &v) != nil || v.Validate() != nil || (v.State != domain.SessionDeletionRequested && v.State != domain.SessionDeletionDeleting && v.State != domain.SessionDeletionCompleted) || (v.State != domain.SessionDeletionRequested && v.RetainedVerification == nil) {
			return nil, errors.New("retained Start exact verification")
		}
		c.f.request = v
		c.f.requested = true
		c.f.writes++
		return driver.RowsAffected(1), nil
	}
	if completionSQL(s) == completionSQL(`UPSERT INTO audit_events (tenant_id, occurred_at, audit_event_id, actor_id, action, subject_kind, subject_id, outcome, metadata, expire_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument), $10)`) {
		if len(a) != 10 || a[4].Value != "session.deletion.start" && a[4].Value != "session.deletion.complete" && a[4].Value != "session.deletion.request" {
			return nil, errors.New("retained Start exact audit")
		}
		c.f.writes++
		return driver.RowsAffected(1), nil
	}
	if match := retainedDeletionDeletePattern.FindStringSubmatch(completionSQL(s)); match != nil {
		table := match[1]
		spec := deletionCaptureSpecs[table]
		if spec == nil || table == "session_lifecycle_backfill_state" {
			return nil, errors.New("retained Complete forbidden table")
		}
		parts := strings.Split(match[2], " AND ")
		if len(parts) != len(a) {
			return nil, errors.New("retained Complete exact delete scope")
		}
		var keys []string
		for i, part := range parts {
			pieces := strings.Split(part, "=")
			if len(pieces) != 2 || strings.TrimSpace(pieces[1]) != fmt.Sprintf("$%d", i+1) {
				return nil, errors.New("retained Complete exact placeholders")
			}
			keys = append(keys, strings.TrimSpace(pieces[0]))
		}
		removed := int64(0)
		var keep []map[string]driver.Value
		for _, row := range c.f.tables[table] {
			match := true
			for i, k := range keys {
				if !retainedDeletionFixtureEqual(row[k], a[i].Value) {
					match = false
					break
				}
			}
			if match {
				removed++
			} else {
				keep = append(keep, row)
			}
		}
		c.f.tables[table] = keep
		c.f.writes++
		return driver.RowsAffected(removed), nil
	}
	return nil, errors.New("capture fixture forbids unhandled writes")
}

var retainedDeletionDeletePattern = regexp.MustCompile(`^DELETE FROM ([a-z0-9_]+) WHERE (.+)$`)

func retainedDeletionFixtureEqual(a, b any) bool {
	if at, ok := a.(time.Time); ok {
		bt, ok := b.(time.Time)
		return ok && at.Equal(bt)
	}
	return reflect.DeepEqual(a, b) || fmt.Sprint(a) == fmt.Sprint(b)
}

func (c *retainedDeletionGraphConn) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	f.lastSQL = s
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	result := func(values [][]driver.Value, columns int) (driver.Rows, error) {
		return &retainedDeletionWriterRows{rows: values, columns: columns}, nil
	}
	if completionSQL(s) == completionSQL(`SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`) {
		if !f.requested {
			return result(nil, 1)
		}
		b, _ := json.Marshal(f.request)
		return result([][]driver.Value{{string(b)}}, 1)
	}
	if completionSQL(s) == completionSQL(`SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`) {
		return result([][]driver.Value{{f.membership}}, 1)
	}
	if completionSQL(s) == completionSQL(`SELECT record FROM session_legal_holds WHERE tenant_id = $1 AND session_id = $2`) {
		if f.hold == nil {
			return result(nil, 1)
		}
		b, _ := json.Marshal(f.hold)
		return result([][]driver.Value{{string(b)}}, 1)
	}
	if s == sessionDeletionRunsSQL {
		var values [][]driver.Value
		for _, row := range f.tables["runs_by_session"] {
			for _, run := range f.tables["runs"] {
				if run["run_id"] == row["run_id"] {
					values = append(values, []driver.Value{row["run_id"], row["status"], run["payload"]})
				}
			}
		}
		return result(values, 3)
	}
	if s == `SELECT CurrentUtcTimestamp()` {
		return result([][]driver.Value{{time.Now().UTC().Truncate(time.Microsecond)}}, 1)
	}
	if s == `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3` {
		if len(args) != 3 || args[1].Value != string(f.request.RequestedBy) || args[2].Value != string(f.request.TenantID) {
			return nil, errors.New("current membership exact scope")
		}
		return result([][]driver.Value{{f.membership}}, 1)
	}
	if s == `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_legal_holds WHERE tenant_id=$1 AND session_id=$2` {
		if len(args) != 2 || args[0].Value != string(f.request.TenantID) || args[1].Value != string(f.request.SessionID) {
			return nil, errors.New("legal hold exact scope")
		}
		if f.hold != nil {
			raw, _ := json.Marshal(f.hold)
			return result([][]driver.Value{{string(raw)}}, 1)
		}
		return result(nil, 1)
	}
	// Fixed existing canonical Complete readers (never a generic SQL engine).
	legacy := map[string]struct{ table, projection, keys string }{
		completionSQL(`SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`):                                                                                                             {"session_participants", "record", "tenant_id,session_id,user_id"},
		completionSQL(`SELECT payload FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2 LIMIT $3`):                                                                                                                 {"attached_worker_output_receipts", "payload", "tenant_id,run_id"},
		completionSQL(`SELECT run_id FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2 ORDER BY run_id ASC LIMIT $3`):                                                                                                      {"runs_by_session", "run_id", "tenant_id,session_id"},
		completionSQL(`SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`):                                                                                                                                          {"sessions", "record", "tenant_id,session_id"},
		completionSQL(`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`):                                                                                                                                                 {"runs", "payload", "tenant_id,run_id"},
		completionSQL(`SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4`):                                                                                                     {"session_participants", "record", "tenant_id,session_id,status"},
		completionSQL(`SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`):                                                                                                         {"frontend_bindings_by_session", "binding_id", "tenant_id,session_id"},
		completionSQL(`SELECT frontend_projection_id FROM frontend_projections_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3`):                                                                                          {"frontend_projections_by_session", "frontend_projection_id", "tenant_id,session_id"},
		completionSQL(`SELECT run_id FROM runs_by_session WHERE tenant_id = $1 AND session_id = $2 ORDER BY created_at ASC, run_id ASC LIMIT $3`):                                                                                      {"runs_by_session", "run_id", "tenant_id,session_id"},
		completionSQL(`SELECT artifact_manifest_id FROM artifact_manifests_by_run WHERE tenant_id = $1 AND run_id = $2 ORDER BY artifact_manifest_id ASC LIMIT $3`):                                                                    {"artifact_manifests_by_run", "artifact_manifest_id", "tenant_id,run_id"},
		completionSQL(`SELECT telegram_delivery_id FROM telegram_deliveries_by_run WHERE tenant_id = $1 AND run_id = $2 ORDER BY telegram_delivery_id ASC LIMIT $3`):                                                                   {"telegram_deliveries_by_run", "telegram_delivery_id", "tenant_id,run_id"},
		completionSQL(`SELECT checkpoint_id FROM checkpoint_objects_by_run WHERE tenant_id = $1 AND run_id = $2 ORDER BY checkpoint_id ASC LIMIT $3`):                                                                                  {"checkpoint_objects_by_run", "checkpoint_id", "tenant_id,run_id"},
		completionSQL(`SELECT SUBSTRING(CAST(attempt_id AS String),0,161), SUBSTRING(CAST(run_id AS String),0,161) FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 ORDER BY attempt_id LIMIT $3`): {"attempt_execution_admissions_by_session_v1", "attempt_id,run_id", "tenant_id,session_id"},
	}
	if old, ok := legacy[completionSQL(s)]; ok {
		keys := strings.Split(old.keys, ",")
		if len(args) < len(keys) {
			return nil, errors.New("canonical Complete reader keys")
		}
		rows, e := f.selectRows(old.table, args[:len(keys)], keys)
		if e != nil {
			return nil, e
		}
		var values [][]driver.Value
		for _, row := range rows {
			var v []driver.Value
			for _, key := range strings.Split(old.projection, ",") {
				v = append(v, row[key])
			}
			values = append(values, v)
		}
		return result(values, len(strings.Split(old.projection, ",")))
	}
	if s == `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2` {
		if !f.requested {
			return result(nil, 1)
		}
		b, _ := json.Marshal(f.request)
		return result([][]driver.Value{{string(b)}}, 1)
	}
	if s == `SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1` {
		return result([][]driver.Value{{f.tables["session_lifecycle_backfill_state"][0]["completed_at"]}}, 1)
	}
	for table, spec := range deletionCaptureSpecs {
		for prefix := 1; prefix <= len(spec.keys); prefix++ {
			want, _ := spec.sql(prefix)
			if s == want {
				rows, e := f.selectRows(table, args[:len(args)-1], spec.keys[:prefix])
				if e != nil {
					return nil, e
				}
				var values [][]driver.Value
				limit := args[len(args)-1].Value.(int64)
				for _, row := range rows {
					if int64(len(values)) >= limit {
						break
					}
					v := make([]driver.Value, len(spec.columns))
					for i, k := range spec.columns {
						v[i] = row[k.name]
					}
					values = append(values, v)
				}
				return result(values, len(spec.columns))
			}
		}
		// The exact private discovery SQL has one of these fixed projections.
		projections := map[string]string{"runs_by_session": "run_id", "artifact_manifests_by_run": "artifact_manifest_id", "telegram_deliveries_by_run": "telegram_delivery_id", "checkpoint_objects_by_run": "checkpoint_id", "frontend_projections_by_run": "frontend_projection_id,frontend", "attempt_execution_admissions_by_session_v1": "attempt_id,run_id", "frontend_bindings_by_session": "binding_id", "frontend_projections_by_session": "frontend_projection_id"}
		if columns := projections[table]; columns != "" {
			prefix := len(args) - 1
			if prefix > 0 && prefix <= len(spec.keys) {
				var fields, where []string
				for _, v := range strings.Split(columns, ",") {
					fields = append(fields, "SUBSTRING(CAST("+v+" AS String),0,161)")
				}
				for i, k := range spec.keys[:prefix] {
					where = append(where, fmt.Sprintf("%s=$%d", k, i+1))
				}
				want := "SELECT " + strings.Join(fields, ",") + " FROM " + table + " WHERE " + strings.Join(where, " AND ") + " ORDER BY " + strings.Join(spec.keys, ",") + fmt.Sprintf(" LIMIT $%d", prefix+1)
				if s == want {
					rows, e := f.selectRows(table, args[:prefix], spec.keys[:prefix])
					if e != nil {
						return nil, e
					}
					var values [][]driver.Value
					for _, row := range rows {
						var v []driver.Value
						for _, k := range strings.Split(columns, ",") {
							v = append(v, row[k])
						}
						values = append(values, v)
					}
					return result(values, len(fields))
				}
			}
		}
		for _, column := range []string{"record", "payload"} {
			var where []string
			for i, k := range spec.keys {
				where = append(where, fmt.Sprintf("%s=$%d", k, i+1))
			}
			if s == fmt.Sprintf("SELECT SUBSTRING(CAST(%s AS String),0,32769) FROM %s WHERE %s", column, table, strings.Join(where, " AND ")) {
				rows, e := f.selectRows(table, args, spec.keys)
				if e != nil {
					return nil, e
				}
				var values [][]driver.Value
				for _, row := range rows {
					values = append(values, []driver.Value{row[column]})
				}
				return result(values, 1)
			}
		}
	}
	if s == deletionCaptureParticipantTestSQL {
		rows, e := f.selectRows("session_participants", args[:2], []string{"tenant_id", "session_id"})
		if e != nil {
			return nil, e
		}
		var values [][]driver.Value
		for _, r := range rows {
			values = append(values, []driver.Value{r["record"]})
		}
		return result(values, 1)
	}
	if s == `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id=$1 AND session_id=$2 AND sequence>$3 ORDER BY sequence LIMIT $4` {
		rows, e := f.selectRows("session_events", args[:2], []string{"tenant_id", "session_id"})
		if e != nil {
			return nil, e
		}
		var values [][]driver.Value
		for _, r := range rows {
			if r["sequence"].(int64) > args[2].Value.(int64) {
				values = append(values, []driver.Value{r["record"]})
			}
		}
		return result(values, 1)
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, s, args)
}

func (f *retainedDeletionGraphFixture) selectRows(table string, args []driver.NamedValue, keys []string) ([]map[string]driver.Value, error) {
	if len(args) != len(keys) {
		return nil, errors.New("full graph exact prefix arity")
	}
	var result []map[string]driver.Value
	for _, row := range f.tables[table] {
		match := true
		for i, k := range keys {
			if !reflect.DeepEqual(row[k], args[i].Value) {
				match = false
				break
			}
		}
		if match {
			result = append(result, row)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		for _, key := range deletionCaptureSpecs[table].keys {
			a, b := result[i][key], result[j][key]
			if reflect.DeepEqual(a, b) {
				continue
			}
			switch a := a.(type) {
			case string:
				return a < b.(string)
			case int64:
				return a < b.(int64)
			case uint64:
				return a < b.(uint64)
			case time.Time:
				return a.Before(b.(time.Time))
			}
		}
		return false
	})
	return result, nil
}

func inspectGenuineRetainedDeletionCapture(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture) error {
	t.Helper()
	f := newRetainedDeletionGraphFixture(t, seed, completed)
	if err := f.request.Validate(); err != nil {
		return fmt.Errorf("fixture original request: %w", err)
	}
	encoded, _ := json.Marshal(f.request)
	var decoded domain.SessionDeletion
	if err := decodeOwnedEffectCanonicalJSON(string(encoded), &decoded); err != nil {
		return fmt.Errorf("fixture request decode: %w", err)
	}
	if decoded.TenantID != seed.Scope.TenantID || decoded.SessionID != seed.Scope.SessionID {
		return errors.New("fixture original requested physical scope mismatch")
	}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	defer db.Close()
	w := newRetainedDeletionWork(t.Context())
	defer w.close()
	store := &Store{db: db}
	var raw string
	if err := db.QueryRowContext(t.Context(), `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`, seed.Scope.TenantID, seed.Scope.SessionID).Scan(&raw); err != nil {
		return fmt.Errorf("fixture raw request read: %w", err)
	}
	if raw != string(encoded) {
		return errors.New("fixture request projection changed body")
	}
	var captured *sessionDeletionCapture
	err := w.transaction(store, seed.Scope.TenantID, func(tx *stateTx, _ time.Time) error {
		var e error
		captured, e = buildRetainedDeletionCaptureWithWork(t.Context(), tx, seed.Scope.SessionID, w)
		return e
	})
	if err != nil {
		return fmt.Errorf("genuine capture query=%s: %w", f.lastSQL, err)
	}
	if captured == nil || len(captured.runs) != 1 || captured.metadata == "" || captured.rows == 0 {
		return errors.New("genuine full graph missing metadata/Run")
	}
	return nil
}

type retainedDeletionProbeReader struct {
	reader     SessionDeletionBlobReader
	opened     int
	deny       bool
	beforeOpen func()
}

func (r *retainedDeletionProbeReader) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	r.opened++
	if r.beforeOpen != nil {
		fn := r.beforeOpen
		r.beforeOpen = nil
		fn()
	}
	if r.deny {
		return nil, errors.New("controlled absent body after original retained Start")
	}
	return r.reader.Open(ctx, tenant, ref)
}

func inspectGenuineRetainedDeletionStart(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, reader SessionDeletionBlobReader, mode string) error {
	t.Helper()
	f := newRetainedDeletionGraphFixture(t, seed, completed)
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	defer db.Close()
	probe := &retainedDeletionProbeReader{reader: reader}
	mutate := func() {
		switch mode {
		case "start_changed_metadata", "resume_changed_metadata", "complete_changed_index":
			row := f.tables["run_finalizations"][0]
			row["content_digest"] = "changed-original-marker"
		case "start_lost_membership":
			var m map[string]any
			_ = json.Unmarshal([]byte(f.membership), &m)
			m["status"] = "suspended"
			raw, _ := json.Marshal(m)
			f.membership = string(raw)
		case "start_new_hold":
			f.hold = &domain.SessionLegalHold{TenantID: seed.Scope.TenantID, SessionID: seed.Scope.SessionID, State: domain.SessionLegalHoldActive, Reason: "controlled concurrent hold", SetBy: seed.Scope.OwnerUserID, SetAt: time.Now().UTC().Truncate(time.Microsecond)}
		case "start_foreign_admission":
			f.tables["attempt_execution_admissions_by_session_v1"] = append(f.tables["attempt_execution_admissions_by_session_v1"], map[string]driver.Value{"tenant_id": string(seed.Scope.TenantID), "session_id": string(seed.Scope.SessionID), "attempt_id": "extra-prepared-attempt", "run_id": string(seed.Scope.RunID)})
		case "complete_extra_child":
			row := map[string]driver.Value{}
			for k, v := range f.tables["attempt_operation_calls_v1"][0] {
				row[k] = v
			}
			row["call_id"] = "extra-call-index"
			f.tables["attempt_operation_calls_v1"] = append(f.tables["attempt_operation_calls_v1"], row)
		}
	}
	if strings.HasPrefix(mode, "start_") && mode != "start_positive" {
		probe.beforeOpen = mutate
	}
	store := &Store{db: db, sessionDeletionBlobReader: probe}
	got, err := store.startRetainedSessionDeletion(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID)
	if strings.HasPrefix(mode, "start_") && mode != "start_positive" {
		if err == nil || f.writes != 0 || f.request.State != domain.SessionDeletionRequested {
			t.Fatal("causal Start mutation dispatched/installed deleting")
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("genuine trusted Start query=%s: %w", f.lastSQL, err)
	}
	if got.Validate() != nil || got.State != domain.SessionDeletionDeleting || got.RetainedVerification == nil || f.request.RetainedVerification == nil || probe.opened == 0 || f.writes != 2 {
		return errors.New("trusted original Start did not atomically install expected capture")
	}
	original := *got.RetainedVerification
	opened := probe.opened
	writes := f.writes
	probe.deny = true
	// Fresh Store has no Blob reader at all. The original SQL-owned commitment
	// must reconstruct the complete metadata/inventory WITHOUT rereading bodies.
	fresh := &Store{db: db}
	if strings.HasPrefix(mode, "resume_") {
		mutate()
	}
	replayed, err := fresh.startRetainedSessionDeletion(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID)
	if strings.HasPrefix(mode, "resume_") {
		if err == nil || f.writes != writes || probe.opened != opened {
			t.Fatal("changed resume performed write/body read or passed")
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("genuine SQL-only fresh Store resume query=%s: %w", f.lastSQL, err)
	}
	if replayed.RetainedVerification == nil || *replayed.RetainedVerification != original || probe.opened != opened || f.writes != writes {
		return errors.New("resume reread body/replaced original capture/time or issued write")
	}
	if strings.HasPrefix(mode, "complete_") {
		mutate()
	}
	finished, err := fresh.completeRetainedSessionDeletion(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID, original.Objects, original.Bytes)
	if strings.HasPrefix(mode, "complete_") {
		if err == nil || f.writes != writes || probe.opened != opened {
			t.Fatal("changed Complete performed destructive SQL/body read or passed")
		}
		return err
	}
	if err != nil {
		return fmt.Errorf("genuine SQL-only Complete query=%s: %w", f.lastSQL, err)
	}
	if finished.Validate() != nil || finished.State != domain.SessionDeletionCompleted || finished.RetainedVerification == nil || *finished.RetainedVerification != original || probe.opened != opened {
		return errors.New("Complete changed capture/read erased bodies")
	}
	for table, rows := range f.tables {
		if table != "session_lifecycle_backfill_state" && len(rows) != 0 {
			return fmt.Errorf("original owned rows not retired: %s count=%d", table, len(rows))
		}
	}
	return nil
}

type retainedDeletionServiceBlobs struct {
	reader                SessionDeletionBlobReader
	puts, deletes, failAt int
	deleted               map[string]bool
}

func (b *retainedDeletionServiceBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	b.puts++
	return domain.BlobRef{}, errors.New("deletion must never PUT")
}
func (b *retainedDeletionServiceBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	return b.reader.Open(ctx, tenant, ref)
}
func (b *retainedDeletionServiceBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("fake exact-object deletion invalid scope")
	}
	b.deletes++
	if b.failAt > 0 && b.deletes == b.failAt {
		return errors.New("controlled partial exact-object deletion")
	}
	b.deleted[ref.Key] = true
	return nil
}

func inspectGenuineRetainedDeletionService(t *testing.T, seed *LocalTerminalPublicationSessionConnectedSeed, completed *LocalTerminalCompletionSQLFixture, reader SessionDeletionBlobReader, mode string) error {
	t.Helper()
	f := newRetainedDeletionGraphFixture(t, seed, completed)
	f.requested = false
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	defer db.Close()
	probe := &retainedDeletionProbeReader{reader: reader}
	store := &Store{db: db, sessionDeletionBlobReader: probe}
	if mode == "service_no_reader" {
		store.sessionDeletionBlobReader = nil
	}
	if _, err := store.RequestSessionDeletion(t.Context(), f.request); err != nil {
		return fmt.Errorf("genuine public Request query=%s: %w", f.lastSQL, err)
	}
	if !f.requested || f.request.State != domain.SessionDeletionRequested || f.request.RetainedVerification != nil {
		t.Fatal("public Request granted physical eligibility")
	}
	blobs := &retainedDeletionServiceBlobs{reader: probe, failAt: 2, deleted: map[string]bool{}}
	service, e := sessionlifecycle.New(store, blobs, 0, 0)
	if e != nil {
		return e
	}
	plan, e := service.Plan(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID)
	if e != nil {
		return fmt.Errorf("genuine public Plan query=%s: %w", f.lastSQL, e)
	}
	if len(plan.Inventory.Objects) < 2 {
		t.Fatal("genuine complete SQL-owned exact inventory")
	}
	if mode == "service_lost_ack" {
		f.lostStartACK = true
	}
	_, e = service.Execute(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID, plan.Confirmation, time.Now().UTC())
	if mode == "service_no_reader" {
		if e == nil || blobs.deletes != 0 || probe.opened != 0 || f.request.State != domain.SessionDeletionRequested {
			t.Fatal("nil reader activated retained deletion")
		}
		return e
	}
	if e == nil || f.request.State != domain.SessionDeletionDeleting || f.request.RetainedVerification == nil || blobs.puts != 0 {
		t.Fatal("controlled partial/lost-ACK Service boundary missing", e)
	}
	if mode == "service_lost_ack" {
		if blobs.deletes != 0 {
			t.Fatal("unknown Start commit acknowledgement authorized Delete")
		}
	} else if blobs.deletes != 2 || len(blobs.deleted) != 1 {
		t.Fatal("partial Delete fixture did not preserve incomplete SQL owners")
	}
	if len(f.tables["runs"]) != 1 || len(f.tables["attempt_effect_reservations"]) != 1 {
		t.Fatal("SQL owners retired before full physical Delete completion")
	}
	original := *f.request.RetainedVerification
	opened := probe.opened
	probe.deny = true
	blobs.failAt = 0
	fresh := &Store{db: db}
	resumed, e := sessionlifecycle.New(fresh, blobs, 0, 0)
	if e != nil {
		return e
	}
	got, e := resumed.Execute(t.Context(), seed.Scope.TenantID, seed.Scope.SessionID, plan.Confirmation, time.Now().UTC())
	if e != nil {
		return fmt.Errorf("genuine fresh Store/Service SQL-only resume query=%s: %w", f.lastSQL, e)
	}
	if got.State != domain.SessionDeletionCompleted || got.Validate() != nil || got.RetainedVerification == nil || *got.RetainedVerification != original || probe.opened != opened || blobs.puts != 0 || uint64(len(blobs.deleted)) != original.Objects {
		t.Fatal("genuine Service resumed with read/rePUT/new capture or incomplete object set")
	}
	for table, rows := range f.tables {
		if table != "session_lifecycle_backfill_state" && len(rows) != 0 {
			t.Fatal("canonical Complete retained original rows", table, len(rows))
		}
	}
	return nil
}
