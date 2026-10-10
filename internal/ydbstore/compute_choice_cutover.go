package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Private trusted deployment pins must describe the FULL reviewed binding
// manifest, not an owner's enumeration revision. They are never HTTP inputs.
// A matching receipt still cannot grant use, consent, capacity or execution.
type computeChoiceCutoverPins struct {
	manifestDigest string
	writerCommit   string
}

type computeChoiceCutoverRecord struct {
	Version                uint32    `json:"version"`
	ManifestDigest         string    `json:"manifest_digest"`
	WriterRevision         uint32    `json:"writer_revision"`
	SchemaRevision         uint32    `json:"schema_revision"`
	Enabled                bool      `json:"enabled"`
	WriterCommit           string    `json:"writer_commit"`
	DrainedInventoryDigest string    `json:"drained_inventory_digest"`
	OldWriterCount         uint64    `json:"old_writer_count"`
	CompletedAt            time.Time `json:"completed_at"`
}

var computeChoiceCutoverFields = map[string]bool{"version": true, "manifest_digest": true, "writer_revision": true, "schema_revision": true, "enabled": true, "writer_commit": true, "drained_inventory_digest": true, "old_writer_count": true, "completed_at": true}

const computeChoiceCutoverSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193),
 version, SUBSTRING(CAST(manifest_digest AS String),0,65), writer_revision, schema_revision, enabled,
 SUBSTRING(CAST(writer_commit AS String),0,41), SUBSTRING(CAST(drained_inventory_digest AS String),0,65), old_writer_count, completed_at
 FROM compute_choice_cutover_v1 WHERE cutover_id=$1`

// Always call inside the resource's current Serializable transaction after
// cookie/WRITE/Session checks. No startup-only check, installer, backfill, schema
// mutation or configuration repair is hidden in a serving reader.
func readComputeChoiceCutover(ctx context.Context, query *computeChoiceQuery, pins computeChoiceCutoverPins, at time.Time) error {
	if !inventoryDigestPattern.MatchString(pins.manifestDigest) || !fullCommitPattern.MatchString(pins.writerCommit) || at.IsZero() {
		return errComputeChoiceSource
	}
	var encoded string
	var scalar computeChoiceCutoverRecord
	err := query.QueryRowContext(ctx, computeChoiceCutoverSQL, "serving").Scan(&encoded, &scalar.Version, &scalar.ManifestDigest, &scalar.WriterRevision, &scalar.SchemaRevision, &scalar.Enabled, &scalar.WriterCommit, &scalar.DrainedInventoryDigest, &scalar.OldWriterCount, &scalar.CompletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return errComputeChoiceSource
	}
	if err != nil {
		return query.readError(err)
	}
	var record computeChoiceCutoverRecord
	if err := decodeComputeChoiceRecord(query.budget, encoded, &record, computeChoiceCutoverFields, scalar.ManifestDigest, scalar.WriterCommit, scalar.DrainedInventoryDigest); err != nil {
		return err
	}
	if record.Version != 1 || record.WriterRevision != 1 || record.SchemaRevision != 1 || !record.Enabled || record.ManifestDigest != pins.manifestDigest || record.WriterCommit != pins.writerCommit || !inventoryDigestPattern.MatchString(record.DrainedInventoryDigest) || record.OldWriterCount != 0 || record.CompletedAt.IsZero() || record.CompletedAt.After(at) ||
		record.Version != scalar.Version || record.ManifestDigest != scalar.ManifestDigest || record.WriterRevision != scalar.WriterRevision || record.SchemaRevision != scalar.SchemaRevision || record.Enabled != scalar.Enabled || record.WriterCommit != scalar.WriterCommit || record.DrainedInventoryDigest != scalar.DrainedInventoryDigest || record.OldWriterCount != scalar.OldWriterCount || !computeChoiceTimestampEqual(record.CompletedAt, scalar.CompletedAt) {
		return errComputeChoiceSource
	}
	return nil
}
