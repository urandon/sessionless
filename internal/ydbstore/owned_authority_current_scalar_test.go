package ydbstore

import (
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestOwnedCurrentScalarUint32ExactTypes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  uint32
	}{
		{name: "native zero", value: uint32(0)},
		{name: "native one", value: uint32(1), want: 1},
		{name: "native max", value: ^uint32(0), want: ^uint32(0)},
		{name: "signed zero", value: int64(0)},
		{name: "signed max", value: int64(^uint32(0)), want: ^uint32(0)},
		{name: "unsigned zero", value: uint64(0)},
		{name: "unsigned max", value: uint64(^uint32(0)), want: ^uint32(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &localCapacityScalarRow{values: []any{tc.value, "tail", int64(7)}}
			var got uint32
			var tail string
			var generation uint64
			n, err := scanOwnedCurrentRow(row, &got, &tail, &generation)
			if err != nil || got != tc.want || n != 20 || row.visited != 3 || tail != "tail" || generation != 7 {
				t.Fatalf("Uint32 collection: type=%T got=%d want=%d bytes=%d/20 visited=%d tail=%q generation=%d err=%v", tc.value, got, tc.want, n, row.visited, tail, generation, err)
			}
		})
	}
}

func TestOwnedCurrentScalarUint32DenialsDrainAllColumns(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		bytes uint64
	}{
		{name: "NULL", value: nil},
		{name: "negative", value: int64(-1), bytes: 8},
		{name: "signed overflow", value: int64(^uint32(0)) + 1, bytes: 8},
		{name: "unsigned overflow", value: uint64(^uint32(0)) + 1, bytes: 8},
		{name: "unsigned max", value: ^uint64(0), bytes: 8},
		{name: "float32", value: float32(1), bytes: 8},
		{name: "float64", value: float64(1), bytes: 8},
		{name: "int", value: int(1), bytes: 8},
		{name: "uint16", value: uint16(1), bytes: 8},
		{name: "bool", value: true, bytes: 8},
		{name: "string", value: "1", bytes: 1},
		{name: "bytes", value: []byte("1"), bytes: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &localCapacityScalarRow{values: []any{tc.value, "tail", uint64(9), int64(7)}}
			var got uint32
			var nativeTail uint64
			var tail string
			var generation uint64
			n, err := scanOwnedCurrentRow(row, &got, &tail, &nativeTail, &generation)
			wantBytes := tc.bytes + 20
			if !errors.Is(err, domain.ErrMembershipDenied) || n != wantBytes || row.visited != 4 || tail != "tail" || nativeTail != 9 || generation != 7 {
				t.Fatalf("invalid Uint32 skipped/coerced columns: type=%T denied=%t bytes=%d/%d visited=%d tail=%q nativeTail=%d generation=%d", tc.value, errors.Is(err, domain.ErrMembershipDenied), n, wantBytes, row.visited, tail, nativeTail, generation)
			}
		})
	}
}
