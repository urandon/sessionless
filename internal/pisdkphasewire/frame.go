package pisdkphasewire

import (
	"bytes"
	"encoding/binary"
	"math"
	"unicode/utf8"
)

const (
	HeaderBytes           = 17
	ChildMetadataMaxBytes = 129
	ChildMetadataNodes    = 7
	UseMetadataBytes      = 9
	// Retain the inherited 1 MiB ceiling; callers must select smaller original bounds.
	TransportMaxFrameBytes      = 1 << 20
	Initialize             byte = 1
	PhysicalRequest        byte = 2
	UseResult              byte = 3
	StructuredFinal        byte = 4
	Retire                 byte = 5
)

// FrameLimits are structural caps, NOT an allocation/Issue grant. Concrete
// producers must qualify their complete original copy/node ledger and native
// fit, and supply the MIN of all inherited bounds before real allocation/use.
type FrameLimits struct {
	InitializeBodyBytes uint64
	RequestBodyBytes    uint64
	FinalBodyBytes      uint64
	FrameBytes          uint64
}

func (l FrameLimits) valid() bool {
	return l.FrameBytes >= HeaderBytes+ChildMetadataMaxBytes+1 && l.FrameBytes <= TransportMaxFrameBytes &&
		l.InitializeBodyBytes > 0 && l.InitializeBodyBytes <= l.FrameBytes-HeaderBytes &&
		l.RequestBodyBytes > 0 && l.RequestBodyBytes <= l.FrameBytes-HeaderBytes-ChildMetadataMaxBytes &&
		l.FinalBodyBytes > 0 && l.FinalBodyBytes <= l.FrameBytes-HeaderBytes
}

type Direction byte

const (
	ParentToChild Direction = 1
	ChildToParent Direction = 2
)

// Header is only a structural length preflight; no body is read or allocated.
// A stream consumer MUST compare Direction to its expected peer before reading
// or allocating either declared region. This classifier is not authentication.
type Header struct {
	Kind          byte
	Direction     Direction
	MetadataBytes uint32
	RawBodyBytes  uint64
	TotalBytes    uint64
}

// InspectHeader accepts EXACTLY 17 bytes and canonical SPU2 only. Run before
// allocating/reading either declared region; never resynchronize malformed IO.
func InspectHeader(b []byte, limits FrameLimits) (Header, error) {
	if len(b) != HeaderBytes || !limits.valid() || string(b[:4]) != "SPU2" {
		return Header{}, ErrInvalid
	}
	h := Header{Kind: b[4], MetadataBytes: binary.BigEndian.Uint32(b[5:9]), RawBodyBytes: binary.BigEndian.Uint64(b[9:17])}
	switch h.Kind {
	case Initialize:
		h.Direction = ParentToChild
		if h.MetadataBytes != 0 || h.RawBodyBytes == 0 || h.RawBodyBytes > limits.InitializeBodyBytes {
			return Header{}, ErrInvalid
		}
	case PhysicalRequest:
		h.Direction = ChildToParent
		if h.MetadataBytes != ChildMetadataMaxBytes || h.RawBodyBytes == 0 || h.RawBodyBytes > limits.RequestBodyBytes {
			return Header{}, ErrInvalid
		}
	case UseResult:
		h.Direction = ParentToChild
		if h.MetadataBytes != UseMetadataBytes || h.RawBodyBytes != 0 {
			return Header{}, ErrInvalid
		}
	case StructuredFinal:
		h.Direction = ChildToParent
		if h.MetadataBytes != 0 || h.RawBodyBytes == 0 || h.RawBodyBytes > limits.FinalBodyBytes {
			return Header{}, ErrInvalid
		}
	case Retire:
		h.Direction = ParentToChild
		if h.MetadataBytes != 0 || h.RawBodyBytes != 0 {
			return Header{}, ErrInvalid
		}
	default:
		return Header{}, ErrInvalid
	}
	fixed := uint64(HeaderBytes) + uint64(h.MetadataBytes)
	maxInt := uint64(^uint(0) >> 1)
	if fixed > maxInt || h.RawBodyBytes > maxInt-fixed || fixed > limits.FrameBytes || h.RawBodyBytes > limits.FrameBytes-fixed {
		return Header{}, ErrInvalid
	}
	h.TotalBytes = fixed + h.RawBodyBytes
	return h, nil
}

// UseAllowance is private relative data from the SAME original parent, not a
// Source reply or transferable permit. Subtract the full IPC elapsed duration
// under qualified clock conversions; receipt-now + N is NOT valid.
type UseAllowance struct {
	Allowed              bool
	RemainingNanoseconds uint64
}

func (u UseAllowance) valid() bool {
	if !u.Allowed {
		return u.RemainingNanoseconds == 0
	}
	return u.RemainingNanoseconds > 0 && u.RemainingNanoseconds <= math.MaxInt64
}

// Frame owns structural copies, not source/fit/send/final-custody authority.
// Direction/order/counts must be enforced separately by the owned stream pump.
type Frame struct {
	Kind                 byte
	BackendProfileDigest string
	Use                  UseAllowance
	Body                 []byte
}

// CompareBodyProfile proves byte identity only, not provenance or native fit.
func CompareBodyProfile(f Frame, expectedBody []byte, expectedProfileDigest string) error {
	if f.Kind != PhysicalRequest || !digest(expectedProfileDigest) || f.BackendProfileDigest != expectedProfileDigest ||
		len(expectedBody) == 0 || !bytes.Equal(f.Body, expectedBody) {
		return ErrInvalid
	}
	return nil
}

func writeChildMetadata(w *writer, digest string) {
	w.put("{\"version\":1,\"phase\":\"physical_send\",\"backend_profile_digest\":")
	w.str(digest)
	w.put("}")
}
func parseChildMetadata(p *parser) string {
	p.lit("{\"version\":1,\"phase\":\"physical_send\",\"backend_profile_digest\":")
	d := p.str(64, 'd', false)
	p.lit("}")
	return d
}

func EncodeFrame(f Frame, limits FrameLimits) ([]byte, error) {
	var metadataBytes uint32
	switch f.Kind {
	case Initialize, StructuredFinal:
		if f.BackendProfileDigest != "" || f.Use != (UseAllowance{}) {
			return nil, ErrInvalid
		}
	case PhysicalRequest:
		if !digest(f.BackendProfileDigest) || f.Use != (UseAllowance{}) {
			return nil, ErrInvalid
		}
		metadataBytes = ChildMetadataMaxBytes
	case UseResult:
		if f.BackendProfileDigest != "" || !f.Use.valid() {
			return nil, ErrInvalid
		}
		metadataBytes = UseMetadataBytes
	case Retire:
		if f.BackendProfileDigest != "" || f.Use != (UseAllowance{}) {
			return nil, ErrInvalid
		}
	default:
		return nil, ErrInvalid
	}
	var header [HeaderBytes]byte
	copy(header[:4], "SPU2")
	header[4] = f.Kind
	binary.BigEndian.PutUint32(header[5:9], metadataBytes)
	binary.BigEndian.PutUint64(header[9:17], uint64(len(f.Body)))
	h, err := InspectHeader(header[:], limits)
	if err != nil || !utf8.Valid(f.Body) {
		return nil, ErrInvalid
	}
	b := make([]byte, int(h.TotalBytes))
	copy(b, header[:])
	metadata := b[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]
	if f.Kind == PhysicalRequest {
		w := writer{b: metadata}
		writeChildMetadata(&w, f.BackendProfileDigest)
		if w.n != len(metadata) {
			return nil, ErrInvalid
		}
	} else if f.Kind == UseResult {
		if f.Use.Allowed {
			metadata[0] = 1
		}
		binary.BigEndian.PutUint64(metadata[1:], f.Use.RemainingNanoseconds)
	}
	copy(b[HeaderBytes+int(h.MetadataBytes):], f.Body)
	return b, nil
}

// DecodeFrame rejects truncation/trailing data and owns returned body/profile.
// It is not a stream pump, strict-final validator or Init schema. Real pumps
// must inspect headers before bounded assembly of legal fragmented progress.
func DecodeFrame(b []byte, limits FrameLimits) (Frame, error) {
	if len(b) < HeaderBytes {
		return Frame{}, ErrInvalid
	}
	h, err := InspectHeader(b[:HeaderBytes], limits)
	if err != nil || h.TotalBytes != uint64(len(b)) {
		return Frame{}, ErrInvalid
	}
	metadata := b[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]
	body := b[HeaderBytes+int(h.MetadataBytes):]
	if !utf8.Valid(body) {
		return Frame{}, ErrInvalid
	}
	f := Frame{Kind: h.Kind}
	switch h.Kind {
	case PhysicalRequest:
		pre := parser{b: metadata, ok: true}
		parseChildMetadata(&pre)
		if !pre.ok || pre.i != len(metadata) {
			return Frame{}, ErrInvalid
		}
		p := parser{b: metadata, ok: true, owned: true}
		f.BackendProfileDigest = parseChildMetadata(&p)
		if !p.ok || p.i != len(metadata) {
			return Frame{}, ErrInvalid
		}
	case UseResult:
		if metadata[0] > 1 {
			return Frame{}, ErrInvalid
		}
		f.Use = UseAllowance{Allowed: metadata[0] == 1, RemainingNanoseconds: binary.BigEndian.Uint64(metadata[1:])}
		if !f.Use.valid() {
			return Frame{}, ErrInvalid
		}
	}
	if len(body) != 0 {
		f.Body = bytes.Clone(body)
	}
	return f, nil
}

// WireAccounting counts one complete successful exchange including retire.
// It deliberately makes NO copy/node/heap/Issue-fit claim. Old SPU1 allocation
// totals cannot certify Init + complete request + relative use + final.
type WireAccounting struct {
	ParentStdinBytes uint64
	ChildStdoutBytes uint64
	TotalIPCBytes    uint64
}

func MeasureWireAccounting(initialize, request, final uint64) (WireAccounting, error) {
	if initialize == 0 || request == 0 || final == 0 || initialize > math.MaxUint64-223 ||
		request > math.MaxUint64-223-initialize || final > math.MaxUint64-223-initialize-request {
		return WireAccounting{}, ErrInvalid
	}
	return WireAccounting{ParentStdinBytes: initialize + 60, ChildStdoutBytes: request + final + 163, TotalIPCBytes: initialize + request + final + 223}, nil
}
