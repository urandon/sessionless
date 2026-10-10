package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"time"
)

const runExplanationCutoverID = "run-explanation-rated-v1-writer-first-cutover"

var fullCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var inventoryDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// RunExplanationCutoverReceiptV1 is deployment evidence, not an automatic
// migration/backfill or a claim that this process inspected other deployments.
// An operator records it only after checking the exact deployed writer and
// draining every old-writer instance; startup compares the deployment pin.
type RunExplanationCutoverReceiptV1 struct {
	Version                uint32
	WriterVersion          uint32
	SchemaVersion          uint32
	RatedReaderVersion     uint32
	WriterCommit           string
	DrainedInventoryDigest string
	OldWriterCount         uint64
	CompletedAt            time.Time
}

func (value RunExplanationCutoverReceiptV1) Validate(expectedWriterCommit string, now time.Time) error {
	if value.Version != 1 || value.WriterVersion != 1 || value.SchemaVersion != 1 || value.RatedReaderVersion != 1 ||
		!fullCommitPattern.MatchString(expectedWriterCommit) || value.WriterCommit != expectedWriterCommit ||
		!inventoryDigestPattern.MatchString(value.DrainedInventoryDigest) || value.OldWriterCount != 0 ||
		value.CompletedAt.IsZero() || now.IsZero() || value.CompletedAt.After(now) {
		return errors.New("run explanation writer/schema cutover is not verified")
	}
	return nil
}

// RequireRunExplanationCutover checks only; it never creates schema, writes a
// receipt, scans history or repairs an absent projection. Missing/corrupt state
// refuses enabled startup. Disabled startup does not call this method.
func (store *Store) RequireRunExplanationCutover(ctx context.Context, expectedWriterCommit string) error {
	if store == nil || store.db == nil || !fullCommitPattern.MatchString(expectedWriterCommit) {
		return errors.New("run explanation cutover requires an exact deployed writer commit")
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var now time.Time
	if err := store.db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&now); err != nil {
		return errors.New("run explanation cutover clock unavailable")
	}
	var receipt RunExplanationCutoverReceiptV1
	err := store.db.QueryRowContext(ctx, `SELECT version, writer_version, schema_version, rated_reader_version,
 writer_commit, drained_inventory_digest, old_writer_count, completed_at
 FROM run_explanation_cutover_state_v1 WHERE cutover_id=$1`, runExplanationCutoverID).Scan(
		&receipt.Version, &receipt.WriterVersion, &receipt.SchemaVersion, &receipt.RatedReaderVersion,
		&receipt.WriterCommit, &receipt.DrainedInventoryDigest, &receipt.OldWriterCount, &receipt.CompletedAt)
	if err != nil || receipt.Validate(expectedWriterCommit, now) != nil {
		return errors.New("run explanation writer/schema cutover is not verified")
	}
	// Exact harmless keys prove the expected tables/columns exist. They do not
	// authenticate a resource or require a fixture to exist in production.
	var record string
	if err := store.db.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, uint32(0)).Scan(&record); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return errors.New("run explanation rate schema unavailable")
	}
	if err := store.db.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, "cutover-schema-probe", "cutover-schema-probe").Scan(&record); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return errors.New("run explanation writer schema unavailable")
	}
	return nil
}
