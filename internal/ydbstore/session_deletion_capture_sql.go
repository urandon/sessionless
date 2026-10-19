package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are schema-owned SQL projections, not request-controlled SQL. Every
// physical scalar is retained, including NULL versus a present empty value.
// String cells carry an overflow sentinel BEFORE crossing the SQL boundary.
type deletionCaptureColumn struct {
	name  string
	kind  byte // s=String/Utf8, j=JsonDocument, u=Uint64, n=Uint32, t=Timestamp
	limit uint64
}
type deletionCaptureSpec struct {
	table   string
	columns []deletionCaptureColumn
	keys    []string
}
type deletionCaptureScope struct {
	spec *deletionCaptureSpec
	// Only an exact leading primary-key prefix is selectable. Remaining keys
	// are enumerated in their original SQL order with an overflow row.
	prefix  []any
	query   stateSQLExecutor
	observe func(*deletionCaptureSpec, []deletionCaptureCell) error
}

// Column lists mirror the applied schemas (not migration writes). Each record
// and scalar/index copy is committed; choosing JSON alone would omit changes
// to an index key, marker, scalar timestamp, preparation flag or child range.
// Keys are declared separately so only an actual leading PK prefix is usable.
var deletionCaptureSpecs = map[string]*deletionCaptureSpec{
	"session_lifecycle_backfill_state":           deletionCaptureSchema("session_lifecycle_backfill_state", "backfill_id", "backfill_id:s,completed_at:t"),
	"telegram_delivery_ready_v2":                 deletionCaptureSchema("telegram_delivery_ready_v2", "shard_bucket,available_at,tenant_id,telegram_delivery_id", "shard_bucket:n,available_at:t,tenant_id:s,telegram_delivery_id:s,run_id:s"),
	"frontend_projection_ready_v1":               deletionCaptureSchema("frontend_projection_ready_v1", "frontend,shard_bucket,created_at,tenant_id,frontend_projection_id", "frontend:s,shard_bucket:n,created_at:t,tenant_id:s,frontend_projection_id:s,run_id:s"),
	"artifact_manifests":                         deletionCaptureSchema("artifact_manifests", "tenant_id,artifact_manifest_id", "tenant_id:s,artifact_manifest_id:s,run_id:s,created_at:t,payload:j"),
	"artifact_manifests_by_run":                  deletionCaptureSchema("artifact_manifests_by_run", "tenant_id,run_id,artifact_manifest_id", "tenant_id:s,run_id:s,artifact_manifest_id:s"),
	"attached_worker_output_receipts":            deletionCaptureSchema("attached_worker_output_receipts", "tenant_id,run_id,owner_user_id,worker_id,attempt_id,lease_generation", "tenant_id:s,run_id:s,owner_user_id:s,worker_id:s,attempt_id:s,lease_generation:u,canonical_digest:s,candidate_fingerprint:s,created_at:t,payload:j"),
	"attempt_effect_reconciliation_evidence":     deletionCaptureSchema("attempt_effect_reconciliation_evidence", "tenant_id,attempt_id,evidence_digest", "tenant_id:s,attempt_id:s,evidence_digest:s,run_id:s,physical_invocation_claim_id:s,operation_state:s,observed_at:t,record:j"),
	"attempt_effect_reservations":                deletionCaptureSchema("attempt_effect_reservations", "tenant_id,attempt_id,kind", "tenant_id:s,attempt_id:s,kind:s,run_id:s,lease_id:s,fence_token:u,physical_invocation_claim_id:s,effect_sequence:u,invocation_authority_digest:s,reserved_at:t,record:j,owned_record:s,owned_digest:s,preparation_started_at:t,preparation_parent_digest:s,allocation_original_record:s,allocation_original_digest:s,allocation_current_record:s,allocation_current_digest:s,allocation_publication_digest:s,allocation_transition_parent_digest:s,proxy_original_record:s,proxy_original_digest:s,proxy_current_record:s,proxy_current_digest:s,proxy_policy_body:s,proxy_attestation_body:s,operation_accounting_record:s,operation_accounting_digest:s,web_search_consent_record:s,web_search_consent_digest:s,operation_residual_record:s"),
	"attempt_execution_admissions_by_session_v1": deletionCaptureSchema("attempt_execution_admissions_by_session_v1", "tenant_id,session_id,attempt_id", "tenant_id:s,session_id:s,attempt_id:s,run_id:s"),
	"attempt_execution_admissions_v1":            deletionCaptureSchema("attempt_execution_admissions_v1", "tenant_id,attempt_id", "tenant_id:s,attempt_id:s,run_id:s,session_id:s,owner_user_id:s,reservation_id:s,admitted_at:t,record:s,digest:s"),
	"attempt_operation_calls_v1":                 deletionCaptureSchema("attempt_operation_calls_v1", "tenant_id,attempt_id,call_id", "tenant_id:s,attempt_id:s,call_id:s,sequence:u,operation_digest:s"),
	"attempt_operation_proxies_v1":               deletionCaptureSchema("attempt_operation_proxies_v1", "tenant_id,attempt_id,sequence", "tenant_id:s,attempt_id:s,sequence:u,service_kind:s,original_record:s,original_digest:s,current_record:s,current_digest:s,policy_body:s,attestation_body:s"),
	"attempt_operations_v1":                      deletionCaptureSchema("attempt_operations_v1", "tenant_id,attempt_id,sequence", "tenant_id:s,attempt_id:s,sequence:u,record:s,digest:s,operation_digest:s,call_id:s,observation_record:s,observation_digest:s"),
	"checkpoint_objects_by_run":                  deletionCaptureSchema("checkpoint_objects_by_run", "tenant_id,run_id,checkpoint_id", "tenant_id:s,run_id:s,checkpoint_id:s,record:j"),
	"checkpoints":                                deletionCaptureSchema("checkpoints", "tenant_id,attempt_id,sequence", "tenant_id:s,attempt_id:s,sequence:u,checkpoint_id:s,run_id:s,blob_key:s,created_at:t,retention_expire_at:t,payload:j"),
	"frontend_binding_keys":                      deletionCaptureSchema("frontend_binding_keys", "tenant_id,frontend,external_conversation_id", "tenant_id:s,frontend:s,external_conversation_id:s,binding_id:s"),
	"frontend_bindings":                          deletionCaptureSchema("frontend_bindings", "tenant_id,binding_id", "tenant_id:s,binding_id:s,frontend:s,external_conversation_id:s,session_id:s,revision:u,created_at:t,updated_at:t,record:j"),
	"frontend_bindings_by_session":               deletionCaptureSchema("frontend_bindings_by_session", "tenant_id,session_id,binding_id", "tenant_id:s,session_id:s,binding_id:s"),
	"frontend_projection_outbox":                 deletionCaptureSchema("frontend_projection_outbox", "tenant_id,frontend_projection_id", "tenant_id:s,frontend_projection_id:s,session_id:s,event_id:s,event_sequence:u,binding_id:s,binding_revision:u,frontend:s,status:s,created_at:t,updated_at:t,record:j"),
	"frontend_projections_by_run":                deletionCaptureSchema("frontend_projections_by_run", "tenant_id,run_id,frontend,frontend_projection_id", "tenant_id:s,run_id:s,frontend:s,frontend_projection_id:s"),
	"frontend_projections_by_session":            deletionCaptureSchema("frontend_projections_by_session", "tenant_id,session_id,frontend_projection_id", "tenant_id:s,session_id:s,frontend_projection_id:s"),
	"run_explanation_heads_v1":                   deletionCaptureSchema("run_explanation_heads_v1", "tenant_id,run_id", "tenant_id:s,run_id:s,revision:u,record:j"),
	"run_finalizations":                          deletionCaptureSchema("run_finalizations", "tenant_id,run_id", "tenant_id:s,run_id:s,terminal_status:s,content_digest:s,created_at:t"),
	"run_idempotency":                            deletionCaptureSchema("run_idempotency", "tenant_id,idempotency_key", "tenant_id:s,idempotency_key:s,run_id:s,created_at:t,expire_at:t"),
	"runs":                                       deletionCaptureSchema("runs", "tenant_id,run_id", "tenant_id:s,run_id:s,session_id:s,trigger_event_id:s,subscription_connection_id:s,status:s,created_at:t,updated_at:t,payload:j"),
	"runs_by_session":                            deletionCaptureSchema("runs_by_session", "tenant_id,session_id,created_at,run_id", "tenant_id:s,session_id:s,created_at:t,run_id:s,trigger_event_id:s,status:s"),
	"session_activity":                           deletionCaptureSchema("session_activity", "tenant_id,user_id,status,activity_bucket,updated_at,session_id", "tenant_id:s,user_id:s,status:s,activity_bucket:n,updated_at:t,session_id:s"),
	"session_displays":                           deletionCaptureSchema("session_displays", "tenant_id,session_id", "tenant_id:s,session_id:s,updated_at:t,record:j"),
	"session_event_idempotency":                  deletionCaptureSchema("session_event_idempotency", "tenant_id,session_id,idempotency_key", "tenant_id:s,session_id:s,idempotency_key:s,sequence:u,event_id:s,created_at:t"),
	"session_events":                             deletionCaptureSchema("session_events", "tenant_id,session_id,sequence", "tenant_id:s,session_id:s,sequence:u,event_id:s,kind:s,author_user_id:s,run_id:s,idempotency_key:s,blob_key:s,created_at:t,record:j"),
	"session_material_boundaries_v1":             deletionCaptureSchema("session_material_boundaries_v1", "tenant_id,session_id,sequence", "tenant_id:s,session_id:s,sequence:u,version:u,through_sequence:u,last_event_id:s,prefix_digest:s,record:s"),
	"session_material_current_v1":                deletionCaptureSchema("session_material_current_v1", "tenant_id,session_id", "tenant_id:s,session_id:s,version:u,through_sequence:u,last_event_id:s,prefix_digest:s,record:s"),
	"session_participants":                       deletionCaptureSchema("session_participants", "tenant_id,session_id,user_id", "tenant_id:s,session_id:s,user_id:s,role:s,status:s,created_at:t,updated_at:t,record:j"),
	"session_snapshot_material_v1":               deletionCaptureSchema("session_snapshot_material_v1", "tenant_id,session_id,snapshot_version", "tenant_id:s,session_id:s,snapshot_version:u,version:u,through_sequence:u,last_event_id:s,boundary_digest:s,record:s"),
	"session_snapshots":                          deletionCaptureSchema("session_snapshots", "tenant_id,session_id,version", "tenant_id:s,session_id:s,version:u,snapshot_id:s,through_sequence:u,blob_key:s,created_at:t,record:j"),
	"sessions":                                   deletionCaptureSchema("sessions", "tenant_id,session_id", "tenant_id:s,session_id:s,created_by:s,status:s,last_event_sequence:u,created_at:t,updated_at:t,archived_at:t,record:j"),
	"substrate_execution_evidence":               deletionCaptureSchema("substrate_execution_evidence", "tenant_id,attempt_id", "tenant_id:s,attempt_id:s,run_id:s,physical_invocation_claim_id:s,evidence_digest:s,recorded_at:t,record:j"),
	"telegram_deliveries_by_run":                 deletionCaptureSchema("telegram_deliveries_by_run", "tenant_id,run_id,telegram_delivery_id", "tenant_id:s,run_id:s,telegram_delivery_id:s,record:j"),
	"telegram_delivery_outbox":                   deletionCaptureSchema("telegram_delivery_outbox", "tenant_id,telegram_delivery_id", "tenant_id:s,telegram_delivery_id:s,run_id:s,status:s,next_attempt_at:t,created_at:t,updated_at:t,retention_expire_at:t,payload:j"),
	"worker_jobs":                                deletionCaptureSchema("worker_jobs", "tenant_id,run_id", "tenant_id:s,run_id:s,attempt_id:s,reservation_id:s,created_at:t,retention_expire_at:t,payload:j"),
}

func deletionCaptureSchema(table, keys, columns string) *deletionCaptureSpec {
	spec := &deletionCaptureSpec{table: table, keys: strings.Split(keys, ",")}
	for _, field := range strings.Split(columns, ",") {
		parts := strings.Split(field, ":")
		if len(parts) != 2 || len(parts[1]) != 1 {
			panic("invalid repository deletion capture schema")
		}
		spec.columns = append(spec.columns, deletionCaptureColumn{name: parts[0], kind: parts[1][0], limit: ownedAuthorityHeadBytes})
	}
	return spec
}

type deletionCaptureCell struct {
	Kind    string          `json:"kind"`
	Present bool            `json:"present"`
	Value   json.RawMessage `json:"value"`
}
type deletionMetadataStream struct {
	digest                hash.Hash
	rows, bytes, maxBytes uint64
	lastTable             string
	lastKey               []deletionCaptureCell
}

func newDeletionMetadataStream(maxBytes uint64) *deletionMetadataStream {
	h := sha256.New()
	_, _ = h.Write([]byte("sessionless.retained-deletion-metadata.v1\x00"))
	return &deletionMetadataStream{digest: h, maxBytes: maxBytes}
}
func deletionHashFrame(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

func (s *deletionMetadataStream) frame(kind string, value any) error {
	b, err := json.Marshal(value)
	if err != nil || uint64(len(b)) > s.maxBytes-s.bytes {
		return domain.ErrMembershipDenied
	}
	s.bytes += uint64(len(b))
	deletionHashFrame(s.digest, []byte(kind))
	deletionHashFrame(s.digest, b)
	return nil
}

func (spec *deletionCaptureSpec) sql(prefix int) (string, error) {
	if spec == nil || spec.table == "" || len(spec.columns) == 0 || len(spec.keys) == 0 || prefix < 1 || prefix > len(spec.keys) {
		return "", domain.ErrMembershipDenied
	}
	var fields, where []string
	for _, c := range spec.columns {
		if c.kind == 's' || c.kind == 'j' {
			if c.limit == 0 || c.limit > ownedAuthorityMaxBytes {
				return "", domain.ErrMembershipDenied
			}
			fields = append(fields, fmt.Sprintf("SUBSTRING(CAST(%s AS String),0,%d)", c.name, c.limit+1))
		} else {
			fields = append(fields, c.name)
		}
	}
	for i, k := range spec.keys[:prefix] {
		where = append(where, fmt.Sprintf("%s=$%d", k, i+1))
	}
	return "/*sessionless.retained-deletion-capture.v1:" + spec.table + "*/ SELECT " + strings.Join(fields, ",") + " FROM " + spec.table + " WHERE " + strings.Join(where, " AND ") + " ORDER BY " + strings.Join(spec.keys, ",") + fmt.Sprintf(" LIMIT $%d", prefix+1), nil
}

func canonicalCaptureJSON(raw string) ([]byte, error) {
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	if validateOwnedEffectJSONTokens(d, 0) != nil {
		return nil, domain.ErrMembershipDenied
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, domain.ErrMembershipDenied
	}
	d = json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var value any
	if d.Decode(&value) != nil {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(value)
}

// A capture row is scanned through nullable native SQL types. Decimal uint64
// values are never routed through float64 or signed int64.
func scanDeletionCaptureRow(rows *sql.Rows, spec *deletionCaptureSpec, charge func(uint64) error) ([]deletionCaptureCell, uint64, error) {
	dst := make([]any, len(spec.columns))
	for i, c := range spec.columns {
		switch c.kind {
		case 's', 'j':
			dst[i] = new(sql.NullString)
		case 'u', 'n':
			dst[i] = new(sql.Null[uint64])
		case 't':
			dst[i] = new(sql.NullTime)
		default:
			return nil, 0, domain.ErrMembershipDenied
		}
	}
	if err := rows.Scan(dst...); err != nil {
		return nil, 0, err
	}
	var scanned uint64
	for _, p := range dst {
		switch p := p.(type) {
		case *sql.NullString:
			if p.Valid {
				scanned += uint64(len(p.String))
			}
		case *sql.Null[uint64]:
			if p.Valid {
				scanned += 8
			}
		case *sql.NullTime:
			if p.Valid {
				scanned += 8
			}
		}
	}
	if charge == nil || charge(scanned) != nil {
		return nil, 0, domain.ErrMembershipDenied
	}
	result := make([]deletionCaptureCell, len(dst))
	var bytes uint64
	for i, p := range dst {
		c := spec.columns[i]
		v := deletionCaptureCell{Kind: string(c.kind), Value: json.RawMessage("null")}
		switch p := p.(type) {
		case *sql.NullString:
			if p.Valid {
				if uint64(len(p.String)) > c.limit || !utf8.ValidString(p.String) {
					return nil, 0, domain.ErrMembershipDenied
				}
				bytes += uint64(len(p.String))
				v.Present = true
				var err error
				if c.kind == 'j' {
					v.Value, err = canonicalCaptureJSON(p.String)
				} else {
					v.Value, err = json.Marshal(p.String)
				}
				if err != nil {
					return nil, 0, err
				}
			}
		case *sql.Null[uint64]:
			if p.Valid {
				if c.kind == 'n' && p.V > math.MaxUint32 {
					return nil, 0, domain.ErrMembershipDenied
				}
				v.Present = true
				v.Value, _ = json.Marshal(p.V)
				bytes += 8
			}
		case *sql.NullTime:
			if p.Valid {
				at := p.Time.UTC()
				if at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
					return nil, 0, domain.ErrMembershipDenied
				}
				v.Present = true
				v.Value, _ = json.Marshal(at.Format("2006-01-02T15:04:05.000000Z"))
				bytes += 8
			}
		}
		result[i] = v
	}
	return result, bytes, nil
}

func compareDeletionCaptureKey(a, b []deletionCaptureCell) int {
	for i := range min(len(a), len(b)) {
		if !a[i].Present || !b[i].Present {
			return 0
		} // NULL keys are rejected before comparison.
		if a[i].Kind == "u" || a[i].Kind == "n" {
			var x, y uint64
			if json.Unmarshal(a[i].Value, &x) != nil || json.Unmarshal(b[i].Value, &y) != nil {
				return 0
			}
			if x < y {
				return -1
			}
			if x > y {
				return 1
			}
		} else {
			var x, y string
			if json.Unmarshal(a[i].Value, &x) != nil || json.Unmarshal(b[i].Value, &y) != nil {
				return 0
			}
			if n := strings.Compare(x, y); n != 0 {
				return n
			}
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func (s *deletionMetadataStream) scope(ctx context.Context, scope deletionCaptureScope) (err error) {
	statement, err := scope.spec.sql(len(scope.prefix))
	if err != nil {
		return err
	}
	if scope.query == nil || ctx.Err() != nil || s.rows > maxSessionDeletionRows || s.bytes > s.maxBytes {
		return domain.ErrMembershipDenied
	}
	if s.lastTable > scope.spec.table {
		return domain.ErrMembershipDenied
	}
	if s.lastTable != scope.spec.table {
		s.lastTable = scope.spec.table
		s.lastKey = nil
	}
	if err := s.frame("range_begin", struct {
		Table  string
		Prefix []any
		Keys   []string
	}{scope.spec.table, scope.prefix, scope.spec.keys}); err != nil {
		return err
	}
	args := append([]any(nil), scope.prefix...)
	args = append(args, maxSessionDeletionRows-s.rows+1)
	rows, err := scope.query.QueryContext(ctx, statement, args...)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	var count uint64
	for rows.Next() {
		if s.rows >= maxSessionDeletionRows {
			return domain.ErrMembershipDenied
		}
		cells, _, err := scanDeletionCaptureRow(rows, scope.spec, func(n uint64) error {
			// Reserve BEFORE JSON tokenization/canonicalization. The same call
			// ledger has already paid discovery; no fresh post-discovery envelope.
			if n > s.maxBytes-s.bytes {
				return domain.ErrMembershipDenied
			}
			s.bytes += n
			switch scope.query.(type) {
			case *deletionCaptureQuery, *ownedAuthorityQuery:
				return chargeDeletionCaptureSQL(scope.query, n)
			}
			return nil // Standalone pure stream still has its own prospective cap.
		})
		if err != nil {
			return err
		}
		key := make([]deletionCaptureCell, len(scope.spec.keys))
		for i, k := range scope.spec.keys {
			found := false
			for j, c := range scope.spec.columns {
				if c.name == k {
					key[i] = cells[j]
					found = true
					break
				}
			}
			if !found || !key[i].Present {
				return domain.ErrMembershipDenied
			}
		}
		// A backend returning a valid ordered row from another scope must not
		// turn a point/absence commitment into ownership of that foreign row.
		if compareDeletionCaptureKey(key[:len(scope.prefix)], captureScopeKey(scope)) != 0 {
			return domain.ErrMembershipDenied
		}
		if scope.observe != nil {
			if err := scope.observe(scope.spec, cells); err != nil {
				return err
			}
		}
		if s.lastKey != nil && compareDeletionCaptureKey(s.lastKey, key) >= 0 {
			return domain.ErrMembershipDenied
		}
		if err := s.frame("row", struct {
			Table          string
			Key, Postimage []deletionCaptureCell
		}{scope.spec.table, key, cells}); err != nil {
			return err
		}
		s.lastKey = key
		s.rows++
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Both an absent point and an empty range are explicit facts. Even a full
	// final page must reach EOF; LIMIT's overflow row is never accepted.
	return s.frame("range_end", struct {
		Table  string
		Prefix []any
		Rows   uint64
	}{scope.spec.table, scope.prefix, count})
}

func captureDeletionMetadata(ctx context.Context, scopes []deletionCaptureScope, maxBytes uint64) (string, uint64, error) {
	if len(scopes) == 0 || len(scopes) > int(maxSessionDeletionRows) || maxBytes == 0 {
		return "", 0, domain.ErrMembershipDenied
	}
	sort.SliceStable(scopes, func(i, j int) bool { return scopes[i].spec.table < scopes[j].spec.table })
	s := newDeletionMetadataStream(maxBytes)
	for _, scope := range scopes {
		if err := s.scope(ctx, scope); err != nil {
			return "", 0, err
		}
	}
	return hex.EncodeToString(s.digest.Sum(nil)), s.rows, nil
}
