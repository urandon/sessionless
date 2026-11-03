package ydbstore

import (
	"context"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Selected by the existing disposable retained-compaction CI gate. These rows
// are representation DATA, not authentic historical effects or send authority.
// Execute the actual production constant: scripted readers do not compile YQL.
func TestLocalRetainedCompactionYDBLegacyEvidenceJoin(t *testing.T) {
	f, parent := nativeOwnedCurrentFixture(t)
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	tenant, attempt := f.scope.tenant, f.scope.attempt
	foreign := domain.TenantID(string(tenant) + "-foreign")
	kind := domain.ProviderEffectTurnV1
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, owner := range []domain.TenantID{tenant, foreign} {
			for _, table := range []string{"substrate_execution_evidence", "attempt_effect_reservations"} {
				if _, err := f.store.db.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1 AND attempt_id=$2", owner, attempt); err != nil {
					t.Error(err)
				}
			}
		}
	})
	read := func(t *testing.T, owner domain.TenantID, selected domain.AttemptID, effectKind domain.ProviderEffectKindV1, wantRows int, wantOwned uint64) {
		t.Helper()
		rows, err := f.store.db.QueryContext(ctx, localTerminalLegacyEvidenceSQL, owner, selected, effectKind)
		if err != nil {
			t.Fatalf("exact three-argument legacy query: %v", err)
		}
		defer rows.Close()
		count := 0
		for rows.Next() {
			var run, gotAttempt, claim, digest, evidence, effect string
			var owned uint64
			if err := rows.Scan(&run, &gotAttempt, &claim, &digest, &evidence, &effect, &owned); err != nil {
				t.Fatalf("seven ordered legacy projection cells: %v", err)
			}
			count++
			if count > wantRows || gotAttempt != string(attempt) || len(run) != 161 || len(claim) != 161 || len(digest) != 65 || len(evidence) != 32769 || len(effect) != 32769 || owned != wantOwned {
				t.Fatalf("legacy projection count=%d want=%d attempt=%t bytes=%d/%d/%d/%d/%d owned=%d want=%d", count, wantRows, gotAttempt == string(attempt), len(run), len(claim), len(digest), len(evidence), len(effect), owned, wantOwned)
			}
		}
		rowErr, closeErr := rows.Err(), rows.Close()
		if rowErr != nil || closeErr != nil || count != wantRows {
			t.Fatalf("legacy query rows=%d want=%d deferred=%v close=%v", count, wantRows, rowErr, closeErr)
		}
	}
	t.Run("missing-original-rows-compile-to-empty", func(t *testing.T) { read(t, tenant, attempt, kind, 0, 0) })
	large := `{"representation_only":"` + strings.Repeat("x", 40000) + `"}`
	for _, owner := range []domain.TenantID{tenant, foreign} {
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO substrate_execution_evidence (tenant_id,attempt_id,run_id,physical_invocation_claim_id,evidence_digest,record) VALUES ($1,$2,$3,$4,$5,CAST($6 AS JsonDocument))`, owner, attempt, strings.Repeat("r", 170), strings.Repeat("c", 170), strings.Repeat("d", 80), large); err != nil {
			t.Fatal("seed representation-only evidence", err)
		}
		// A same-attempt different-kind row must not fan out into the selected
		// provider-turn read; the foreign tenant has no selected provider row.
		if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO attempt_effect_reservations (tenant_id,attempt_id,kind,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, owner, attempt, "different_kind", `{"foreign_kind":true}`); err != nil {
			t.Fatal("seed different-kind DATA", err)
		}
	}
	if _, err := f.store.db.ExecContext(ctx, `UPSERT INTO attempt_effect_reservations (tenant_id,attempt_id,kind,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, tenant, attempt, kind, large); err != nil {
		t.Fatal("seed selected representation-only effect", err)
	}
	t.Run("full-key-seven-bounded-cells", func(t *testing.T) { read(t, tenant, attempt, kind, 1, 0) })
	t.Run("foreign-evidence-cannot-borrow-effect", func(t *testing.T) { read(t, foreign, attempt, kind, 0, 0) })
	t.Run("missing-attempt", func(t *testing.T) { read(t, tenant, domain.AttemptID(string(attempt)+"-missing"), kind, 0, 0) })
	t.Run("missing-kind", func(t *testing.T) { read(t, tenant, attempt, "missing_kind", 0, 0) })
	// Even an empty non-NULL owned cell must set contamination; this DATA does
	// not bypass the unchanged historical verifier's rejection of owned rows.
	if _, err := f.store.db.ExecContext(ctx, `UPDATE attempt_effect_reservations SET owned_record=$1 WHERE tenant_id=$2 AND attempt_id=$3 AND kind=$4`, "", tenant, attempt, kind); err != nil {
		t.Fatal("set representation contamination", err)
	}
	t.Run("owned-contamination-is-not-legacy", func(t *testing.T) { read(t, tenant, attempt, kind, 1, 1) })
}
