//go:build ydbintegration

package ydbintegration

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestRunExplanationYDBCutoverRequiresTypedDeploymentReceiptAndSchema(t *testing.T) {
	f := newExplanationYDBFixture(t)
	const key = "run-explanation-rated-v1-writer-first-cutover"
	commitHash := sha256.Sum256([]byte(string(f.auth.TenantID)))
	commit := hex.EncodeToString(commitHash[:])[:40]
	var existing uint64
	if err := f.client.DB.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM run_explanation_cutover_state_v1 WHERE cutover_id=$1`, key).Scan(&existing); err != nil {
		t.Fatal(err)
	}
	if existing != 0 {
		t.Fatal("refusing to overwrite an existing deployment cutover receipt")
	}
	t.Cleanup(func() {
		// Exact fixture commit fence: never remove another actor's receipt.
		if _, err := f.client.DB.ExecContext(f.ctx, `DELETE FROM run_explanation_cutover_state_v1 WHERE cutover_id=$1 AND writer_commit=$2`, key, commit); err != nil {
			t.Errorf("cleanup fixture cutover: %v", err)
		}
	})
	if err := f.store.RequireRunExplanationCutover(f.ctx, commit); err == nil {
		t.Fatal("absent cutover accepted")
	}
	write := func(reader uint32, old uint64) {
		t.Helper()
		if _, err := f.client.DB.ExecContext(f.ctx, `UPSERT INTO run_explanation_cutover_state_v1 (cutover_id,version,writer_version,schema_version,rated_reader_version,writer_commit,drained_inventory_digest,old_writer_count,completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, key, uint32(1), uint32(1), uint32(1), reader, commit, strings.Repeat("b", 64), old, f.now); err != nil {
			t.Fatal(err)
		}
	}
	write(1, 1)
	if err := f.store.RequireRunExplanationCutover(f.ctx, commit); err == nil {
		t.Fatal("undrained writer accepted")
	}
	write(2, 0)
	if err := f.store.RequireRunExplanationCutover(f.ctx, commit); err == nil {
		t.Fatal("unsupported reader accepted")
	}
	write(1, 0)
	if err := f.store.RequireRunExplanationCutover(f.ctx, strings.Repeat("c", 40)); err == nil {
		t.Fatal("different deployed writer accepted")
	}
	if err := f.store.RequireRunExplanationCutover(f.ctx, commit); err != nil {
		t.Fatalf("exact committed cutover/schema: %v", err)
	}
	// A separate Store must independently read the same checked receipt.
	other, err := ydbstore.New(f.client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.RequireRunExplanationCutover(f.ctx, commit); err != nil {
		t.Fatal(err)
	}
}
