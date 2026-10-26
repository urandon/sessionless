package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"unicode/utf8"
)

const inlineReceiptHashMarkerV2 = "sessionless-inline-hash-marker-v2"

// projected is caller-owned typed metadata, not raw JSON or authority. Exactly
// one selected field must hold the marker. Replace only that VALUE while
// hashing the same default Marshal bytes, including original field order.
func inlineReceiptProjectedHashV2(prefix string, projected any, field string, original string) (string, error) {
	metadata, err := json.Marshal(projected)
	if err != nil {
		return "", err
	}
	defer clear(metadata)
	key, err := json.Marshal(field)
	if err != nil {
		return "", err
	}
	defer clear(key)
	marker, err := json.Marshal(inlineReceiptHashMarkerV2)
	if err != nil {
		return "", err
	}
	defer clear(marker)
	token := make([]byte, 0, len(key)+1+len(marker))
	token = append(token, key...)
	token = append(token, ':')
	token = append(token, marker...)
	defer clear(token)
	if bytes.Count(metadata, token) != 1 {
		return "", ErrAttachedWorkerAttemptConflict
	}
	valueStart := bytes.Index(metadata, token) + len(key) + 1
	valueEnd := valueStart + len(marker)
	h := sha256.New()
	// hash.Hash.Write cannot fail; keep the prefix separate, never append it
	// to a full encoded payload or allocate the original's quoted spelling.
	_, _ = io.WriteString(h, prefix)
	_, _ = h.Write(metadata[:valueStart])
	if err := streamInlineReceiptJSONStringV2(h, original); err != nil {
		return "", err
	}
	_, _ = h.Write(metadata[valueEnd:])
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Match json.Marshal's default string encoding, including HTML and invalid
// UTF-8 handling, with a single fixed scratch buffer. This is not a JSON pool
// capacity or whole-invocation memory/zeroization guarantee.
func streamInlineReceiptJSONStringV2(w io.Writer, original string) error {
	if w == nil {
		return ErrAttachedWorkerAttemptConflict
	}
	s := inlineReceiptJSONStreamV2{writer: w}
	defer clear(s.scratch[:])
	if err := s.raw(`"`); err != nil {
		return err
	}
	start := 0
	for i := 0; i < len(original); {
		var escape string
		n := 1
		b := original[i]
		if b < utf8.RuneSelf {
			switch b {
			case '"':
				escape = `\"`
			case '\\':
				escape = `\\`
			case '\b':
				escape = `\b`
			case '\f':
				escape = `\f`
			case '\n':
				escape = `\n`
			case '\r':
				escape = `\r`
			case '\t':
				escape = `\t`
			case '<':
				escape = `\u003c`
			case '>':
				escape = `\u003e`
			case '&':
				escape = `\u0026`
			default:
				if b < 0x20 {
					if err := s.raw(original[start:i]); err != nil {
						return err
					}
					const digits = "0123456789abcdef"
					escaped := [6]byte{'\\', 'u', '0', '0', digits[b>>4], digits[b&15]}
					if err := s.bytes(escaped[:]); err != nil {
						return err
					}
					i++
					start = i
					continue
				}
			}
		} else {
			r, size := utf8.DecodeRuneInString(original[i:])
			n = size
			switch {
			case r == utf8.RuneError && size == 1:
				escape = `\ufffd`
			case r == '\u2028':
				escape = `\u2028`
			case r == '\u2029':
				escape = `\u2029`
			}
		}
		if escape != "" {
			if err := s.raw(original[start:i]); err != nil {
				return err
			}
			if err := s.raw(escape); err != nil {
				return err
			}
			start = i + n
		}
		i += n
	}
	if err := s.raw(original[start:]); err != nil {
		return err
	}
	if err := s.raw(`"`); err != nil {
		return err
	}
	return s.flush()
}

type inlineReceiptJSONStreamV2 struct {
	writer  io.Writer
	scratch [1024]byte
	used    int
}

func (s *inlineReceiptJSONStreamV2) raw(value string) error {
	for len(value) != 0 {
		n := copy(s.scratch[s.used:], value)
		s.used += n
		value = value[n:]
		if s.used == len(s.scratch) {
			if err := s.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *inlineReceiptJSONStreamV2) bytes(value []byte) error {
	for len(value) != 0 {
		n := copy(s.scratch[s.used:], value)
		s.used += n
		value = value[n:]
		if s.used == len(s.scratch) {
			if err := s.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *inlineReceiptJSONStreamV2) flush() error {
	if s.used == 0 {
		return nil
	}
	n, err := s.writer.Write(s.scratch[:s.used])
	if err != nil {
		return err
	}
	if n != s.used {
		return io.ErrShortWrite
	}
	s.used = 0
	return nil
}
