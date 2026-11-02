package ydbstore

import (
	"database/sql"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// A controlled row drives the actual collector's sql.Scanner destinations;
// it does not let database/sql coerce unsupported authority scalar types first.
type localCapacityScalarRow struct {
	values  []any
	visited int
	err     error
}

func (r *localCapacityScalarRow) Scan(destinations ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(destinations) != len(r.values) {
		return errors.New("controlled scalar row arity mismatch")
	}
	for i, destination := range destinations {
		scanner, ok := destination.(sql.Scanner)
		if !ok {
			return errors.New("controlled scalar destination is not a scanner")
		}
		r.visited++
		if err := scanner.Scan(r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

func TestLocalCapacityScalarNullableStringIsStrict(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  sql.NullString
		bytes uint64
	}{
		{name: "NULL", value: nil},
		{name: "string", value: "original-é", want: sql.NullString{String: "original-é", Valid: true}, bytes: uint64(len("original-é"))},
		{name: "bytes", value: []byte("original-é"), want: sql.NullString{String: "original-é", Valid: true}, bytes: uint64(len("original-é"))},
		{name: "empty string", value: "", want: sql.NullString{Valid: true}},
		{name: "empty bytes", value: []byte{}, want: sql.NullString{Valid: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &localCapacityScalarRow{values: []any{tc.value}}
			got := sql.NullString{String: "stale destination", Valid: true}
			n, err := scanOwnedCurrentRow(row, &got)
			if err != nil || got != tc.want || n != tc.bytes || row.visited != 1 {
				t.Fatalf("strict nullable scalar: error=%t gotValid=%t wantValid=%t bytes=%d/%d visited=%d", err != nil, got.Valid, tc.want.Valid, n, tc.bytes, row.visited)
			}
		})
	}
}

func TestLocalCapacityScalarDenialStillCountsEveryColumn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		bytes uint64
	}{
		{name: "bool", value: true, bytes: 8},
		{name: "float", value: float64(12), bytes: 8},
		{name: "signed integer", value: int64(12), bytes: 8},
		{name: "unsigned integer", value: uint64(12), bytes: 8},
		{name: "integer", value: int(12), bytes: 8},
		{name: "invalid UTF8 string", value: string([]byte{0xff}), bytes: 1},
		{name: "invalid UTF8 bytes", value: []byte{0xff}, bytes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &localCapacityScalarRow{values: []any{tc.value, "tail-record", []byte("tail-digest"), int64(7)}}
			first := sql.NullString{String: "stale destination", Valid: true}
			var raw string
			var digest sql.NullString
			var generation uint64
			n, err := scanOwnedCurrentRow(row, &first, &raw, &digest, &generation)
			wantBytes := tc.bytes + uint64(len("tail-record")+len("tail-digest")) + 8
			if !errors.Is(err, domain.ErrMembershipDenied) || n != wantBytes || row.visited != 4 ||
				raw != "tail-record" || digest != (sql.NullString{String: "tail-digest", Valid: true}) || generation != 7 {
				t.Fatalf("invalid scalar skipped/coerced remaining columns: denied=%t bytes=%d/%d visited=%d tailCollected=%t", errors.Is(err, domain.ErrMembershipDenied), n, wantBytes, row.visited, raw == "tail-record" && digest.Valid && generation == 7)
			}
		})
	}
	// An absent row is not a NULL local cell or a usable empty legacy record.
	row := &localCapacityScalarRow{err: sql.ErrNoRows}
	var value sql.NullString
	if n, err := scanOwnedCurrentRow(row, &value); err != sql.ErrNoRows || n != 0 || row.visited != 0 {
		t.Fatalf("no-row distinction lost: noRows=%t bytes=%d visited=%d", err == sql.ErrNoRows, n, row.visited)
	}
}
