package ydbstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// CI's existing retained-compaction disposable-YDB gate selects this prefix.
// This proves exact production query grammar/types, prefix keys, joined call
// projection, cap-plus-one cells and 13-row sentinel. Seeded rows are DATA, not
// whole-source authority, actual generation or an EXPLAIN/query-plan claim.
func TestLocalRetainedCompactionYDBSourceQueries(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	tenant, session, attempt := f.scope.tenant, f.scope.session, f.scope.attempt
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, table := range []string{"session_events", "session_snapshots"} {
			if _, err := f.store.db.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1 AND session_id=$2", tenant, session); err != nil {
				t.Error(err)
			}
		}
		for _, table := range []string{"attempt_operations_v1", "attempt_operation_calls_v1", "attempt_operation_proxies_v1"} {
			if _, err := f.store.db.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1 AND attempt_id=$2", tenant, attempt); err != nil {
				t.Error(err)
			}
		}
	})
	s := currentWorkerAuthorityFixture(t)
	s.Run.TenantID, s.Run.SessionID, s.Job.CreatedAt = tenant, session, f.driver.at
	snapshot := canonicalSnapshot(t, s, 3)
	if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO session_snapshots (tenant_id,session_id,version,through_sequence,record) VALUES ($1,$2,$3,$4,CAST($5 AS JsonDocument))`, tenant, session, uint64(3), uint64(1), snapshot); err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := f.store.db.QueryRowContext(ctx, localCompactionSnapshotSQL, tenant, session, uint64(3)).Scan(&raw); err != nil {
		t.Fatalf("exact snapshot JsonDocument projection: %v", err)
	}
	var got domain.SessionSnapshot
	if len(raw) > 8192 || decodeOwnedEffectCanonicalJSON(raw, &got) != nil || got.Version != 3 || got.SessionID != session {
		t.Fatal("exact snapshot projection changed data/type")
	}
	large := `{"padding":"` + strings.Repeat("x", 9000) + `"}`
	for i := uint64(1); i <= 3; i++ {
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO session_events (tenant_id,session_id,sequence,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, tenant, session, i, large); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := f.store.db.QueryContext(ctx, localCompactionEventsSQL, tenant, session, uint64(1), uint64(3), uint64(3), uint64(257))
	if err != nil {
		t.Fatalf("exact bounded-tail JsonDocument SQL: %v", err)
	}
	count := 0
	for rows.Next() {
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if len(raw) != 257 {
			rows.Close()
			t.Fatalf("tail cap-plus-one=%d want257", len(raw))
		}
		count++
	}
	rowErr, closeErr := rows.Err(), rows.Close()
	if rowErr != nil || closeErr != nil || count != 2 {
		t.Fatalf("tail exact key/range count=%d err=%v close=%v", count, rowErr, closeErr)
	}
	for i := uint64(1); i <= 14; i++ {
		call := fmt.Sprintf("call-%d", i)
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id,observation_record,observation_digest) VALUES ($1,$2,$3,$4,$5,$6,$7,$4,$5)`, tenant, attempt, i, strings.Repeat("r", 40000), strings.Repeat("d", 80), strings.Repeat("o", 80), call); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, tenant, attempt, call, i, strings.Repeat("o", 80)); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO attempt_operation_proxies_v1 (tenant_id,attempt_id,sequence,service_kind,original_record,original_digest,current_record,current_digest,policy_body,attestation_body) VALUES ($1,$2,$3,$4,$5,$6,$5,$6,$7,$7)`, tenant, attempt, i, "provider", strings.Repeat("p", 40000), strings.Repeat("d", 80), strings.Repeat("b", 20000)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err = f.store.db.QueryContext(ctx, localCompactionOperationsSQL, tenant, attempt)
	if err != nil {
		t.Fatalf("exact full operation/call-index joined SQL: %v", err)
	}
	count = 0
	for rows.Next() {
		var c ownedOperationColumns
		var sequence, indexed uint64
		var indexedDigest string
		dest := append([]any{&sequence}, c.destinations()...)
		dest = append(dest, &indexed, &indexedDigest)
		if _, err := scanOwnedCurrentRow(rows, dest...); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		count++
		if sequence != uint64(count) || indexed != sequence || len(c.raw) != 32769 || len(c.observedRaw) != 32769 || len(c.digest) != 65 || len(indexedDigest) != 65 {
			rows.Close()
			t.Fatalf("bounded original operation/index projection sequence=%d indexed=%d count=%d", sequence, indexed, count)
		}
	}
	rowErr, closeErr = rows.Err(), rows.Close()
	if rowErr != nil || closeErr != nil || count != 13 {
		t.Fatalf("full operation sentinel=%d err=%v close=%v", count, rowErr, closeErr)
	}
	rows, err = f.store.db.QueryContext(ctx, localCompactionProxiesSQL, tenant, attempt)
	if err != nil {
		t.Fatalf("exact bounded proxy inventory SQL: %v", err)
	}
	count = 0
	for rows.Next() {
		var c ownedProxyColumns
		var sequence uint64
		dest := append([]any{&sequence}, c.destinations(2)...)
		if _, err := scanOwnedCurrentRow(rows, dest...); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		count++
		if sequence != uint64(count) || len(c.originalRaw) != 32769 || len(c.policyRaw) != 16385 || len(c.attestationRaw) != 16385 || c.flags != [6]uint64{1, 1, 1, 1, 1, 1} {
			rows.Close()
			t.Fatalf("bounded proxy projection sequence=%d count=%d", sequence, count)
		}
	}
	rowErr, closeErr = rows.Err(), rows.Close()
	if rowErr != nil || closeErr != nil || count != 13 {
		t.Fatalf("proxy sentinel=%d err=%v close=%v", count, rowErr, closeErr)
	}
	// Snapshot JSON is valid producer shape despite JsonDocument reserialization.
	var original domain.SessionSnapshot
	if json.Unmarshal([]byte(snapshot), &original) != nil || original.ID != got.ID {
		t.Fatal("native snapshot producer comparison")
	}
}
