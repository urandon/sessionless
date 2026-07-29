package idgen

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestGeneratorProducesOpaqueNonSortableIDs(t *testing.T) {
	reader := &counterHashReader{}
	generator := newWithReader(reader)
	seen := make(map[string]struct{}, 4096)
	distribution := make(map[byte]int)

	for i := 0; i < 4096; i++ {
		id, err := generator.NewID(context.Background(), ports.IDRun)
		if err != nil {
			t.Fatal(err)
		}
		if err := domain.RunID(id).Validate(); err != nil {
			t.Fatalf("generated invalid ID %q: %v", id, err)
		}
		if !strings.HasPrefix(id, "run_") || len(id) != len("run_")+26 {
			t.Fatalf("unexpected ID shape %q", id)
		}
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate ID %q", id)
		}
		seen[id] = struct{}{}
		distribution[id[len("run_")]]++
	}

	// A timestamp/sequence prefix would occupy one leading symbol. The
	// deterministic SHA-256 fixture must exercise every Base32 leading symbol.
	if len(distribution) != 32 {
		t.Fatalf("leading random-symbol coverage = %d, want 32", len(distribution))
	}
}

func TestGeneratorRejectsUnknownKind(t *testing.T) {
	_, err := newWithReader(&counterHashReader{}).NewID(
		context.Background(),
		ports.IDKind("unknown"),
	)
	if err == nil {
		t.Fatal("expected unsupported kind error")
	}
}

type counterHashReader struct {
	counter uint64
	buffer  []byte
}

func (reader *counterHashReader) Read(target []byte) (int, error) {
	for len(reader.buffer) < len(target) {
		var input [8]byte
		binary.BigEndian.PutUint64(input[:], reader.counter)
		sum := sha256.Sum256(input[:])
		reader.buffer = append(reader.buffer, sum[:]...)
		reader.counter++
	}
	n := copy(target, reader.buffer)
	reader.buffer = reader.buffer[n:]
	return n, nil
}

var _ io.Reader = (*counterHashReader)(nil)
