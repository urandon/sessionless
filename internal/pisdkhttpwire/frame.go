package pisdkhttpwire

import (
	"encoding/binary"
	"unicode/utf8"
)

type Kind byte

const (
	Initialize Kind = 1 + iota
	FetchRequest
	ResponseStart
	ResponsePart
	Pull
	ResponseEnd
	StructuredFinal
	Retire
)

type Direction byte

const (
	ParentToChild Direction = 1 + iota
	ChildToParent
)

type Header struct {
	Kind          Kind
	MetadataBytes uint32
	BodyBytes     uint64
	TotalBytes    uint64
}

// InspectHeader runs on exactly 17 bytes BEFORE reading declared regions. Peer
// direction comes from the owned pipe, never from untrusted wire metadata.
func InspectHeader(b []byte, peer Direction, l Limits) (Header, error) {
	a, err := Measure(l)
	if err != nil || len(b) != HeaderBytes || string(b[:4]) != "SPU3" {
		return Header{}, ErrInvalid
	}
	h := Header{Kind: Kind(b[4]), MetadataBytes: binary.BigEndian.Uint32(b[5:9]), BodyBytes: binary.BigEndian.Uint64(b[9:17])}
	want := ParentToChild
	valid := false
	switch h.Kind {
	case Initialize:
		valid = h.MetadataBytes == 96 && h.BodyBytes > 0 && h.BodyBytes <= l.InitializeBytes
	case FetchRequest:
		want = ChildToParent
		valid = h.MetadataBytes >= 70 && uint64(h.MetadataBytes) <= 68+l.RequestHeaderBytes && h.BodyBytes > 0 && h.BodyBytes <= l.RequestBytes
	case ResponseStart:
		valid = h.MetadataBytes == 46 && h.BodyBytes >= 2 && h.BodyBytes <= l.ResponseHeaderBytes
	case ResponsePart:
		valid = h.MetadataBytes == 4 && h.BodyBytes > 0 && h.BodyBytes <= a.ChunkBytes
	case Pull:
		want = ChildToParent
		valid = h.MetadataBytes == 4 && h.BodyBytes == 0
	case ResponseEnd:
		valid = h.MetadataBytes == 44 && h.BodyBytes == 0
	case StructuredFinal:
		want = ChildToParent
		valid = h.MetadataBytes == 0 && h.BodyBytes > 0 && h.BodyBytes <= l.FinalBytes
	case Retire:
		valid = h.MetadataBytes == 0 && h.BodyBytes == 0
	}
	fixed := uint64(HeaderBytes) + uint64(h.MetadataBytes)
	if !valid || want != peer || fixed > l.FrameBytes || h.BodyBytes > l.FrameBytes-fixed {
		return Header{}, ErrInvalid
	}
	h.TotalBytes = fixed + h.BodyBytes
	return h, nil
}

// Frame contains borrowed byte DATA, not an HTTP response or a permission.
// DecodeView views stay owned by the caller; do not mutate/clear them until all
// consumers join. Unused fields MUST be zero, never silently ignored.
type Frame struct {
	Kind                         Kind
	Nonce, Binding, Profile, MAC [32]byte
	Digest                       [32]byte
	Headers, Body                []byte
	Count, Index                 uint32
	Bytes                        uint64
}

func (Frame) String() string     { return "PiHTTPFrame{data:[redacted]}" }
func (f Frame) GoString() string { return f.String() }

// Only integer lengths enter this preflight: it cannot traverse a payload.
func inspectLengths(kind Kind, body, headers uint64, l Limits) (Header, error) {
	if _, err := Measure(l); err != nil {
		return Header{}, err
	}
	metadata := uint64(0)
	peer := ParentToChild
	switch kind {
	case Initialize:
		metadata = 96
	case FetchRequest:
		peer = ChildToParent
		if headers < 2 || headers > l.RequestHeaderBytes {
			return Header{}, ErrInvalid
		}
		metadata = 68 + headers
	case ResponseStart:
		if body != 0 || headers < 2 || headers > l.ResponseHeaderBytes {
			return Header{}, ErrInvalid
		}
		metadata, body = 46, headers
	case ResponsePart:
		metadata = 4
	case Pull:
		metadata, peer = 4, ChildToParent
	case ResponseEnd:
		metadata = 44
	case StructuredFinal:
		peer = ChildToParent
	case Retire:
	default:
		return Header{}, ErrInvalid
	}
	if kind != FetchRequest && kind != ResponseStart && headers != 0 {
		return Header{}, ErrInvalid
	}
	var b [HeaderBytes]byte
	copy(b[:4], "SPU3")
	b[4] = byte(kind)
	binary.BigEndian.PutUint32(b[5:9], uint32(metadata))
	binary.BigEndian.PutUint64(b[9:17], body)
	return InspectHeader(b[:], peer, l)
}

func validSummary(count uint32, size uint64, l Limits) bool {
	a, err := Measure(l)
	return err == nil && count > 0 && uint64(count) <= a.Chunks && size > 0 && size <= l.ResponseBytes &&
		uint64(count) == 1+(size-1)/a.ChunkBytes
}

func inspectFrame(f Frame, l Limits) (Header, error) {
	h, err := inspectLengths(f.Kind, uint64(len(f.Body)), uint64(len(f.Headers)), l)
	if err != nil {
		return Header{}, err
	}
	rest := f
	rest.Kind, rest.Body = 0, nil
	switch f.Kind {
	case Initialize:
		rest.Nonce, rest.Binding, rest.Profile = [32]byte{}, [32]byte{}, [32]byte{}
	case FetchRequest:
		if ValidateHeaders(f.Headers, l.RequestHeaderBytes, true) != nil {
			return Header{}, ErrInvalid
		}
		rest.Profile, rest.MAC, rest.Headers = [32]byte{}, [32]byte{}, nil
	case ResponseStart:
		if len(f.Body) != 0 || ValidateHeaders(f.Headers, l.ResponseHeaderBytes, false) != nil {
			return Header{}, ErrInvalid
		}
		rest.Headers, rest.Count, rest.Bytes, rest.Digest = nil, 0, 0, [32]byte{}
	case ResponsePart, Pull:
		rest.Index = 0
	case ResponseEnd:
		rest.Count, rest.Bytes, rest.Digest = 0, 0, [32]byte{}
	case StructuredFinal:
	case Retire:
	default:
		return Header{}, ErrInvalid
	}
	if rest.Nonce != [32]byte{} || rest.Binding != [32]byte{} || rest.Profile != [32]byte{} || rest.MAC != [32]byte{} ||
		rest.Digest != [32]byte{} || len(rest.Headers) != 0 || rest.Count != 0 || rest.Index != 0 || rest.Bytes != 0 {
		return Header{}, ErrInvalid
	}
	body := f.Body
	if f.Kind == ResponseStart {
		body = f.Headers
	}
	if (f.Kind == Initialize || f.Kind == FetchRequest || f.Kind == StructuredFinal) && !utf8.Valid(body) {
		return Header{}, ErrInvalid
	}
	if f.Kind == ResponseStart || f.Kind == ResponseEnd {
		if !validSummary(f.Count, f.Bytes, l) {
			return Header{}, ErrInvalid
		}
	}
	if f.Kind == Pull || f.Kind == ResponsePart {
		a, _ := Measure(l)
		if uint64(f.Index) >= a.Chunks {
			return Header{}, ErrInvalid
		}
	}
	return h, nil
}

// overlap uses bounded address comparisons without unsafe pointer arithmetic.
// It adds linear scan work which the eventual full producer must fund.
func overlap(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	for i := range a {
		if &a[i] == &b[0] {
			return true
		}
	}
	for i := range b {
		if &b[i] == &a[0] {
			return true
		}
	}
	return false
}

// EncodeInto writes only after all structural/capacity/alias checks. dst must
// be separately prepaid; this method allocates no payload arena. On failure
// it returns zero and leaves dst unchanged. No source field is normalized.
func EncodeInto(dst []byte, f Frame, l Limits) (int, error) {
	h, err := inspectFrame(f, l)
	if err != nil || uint64(len(dst)) < h.TotalBytes {
		return 0, ErrInvalid
	}
	out := dst[:int(h.TotalBytes)]
	if overlap(out, f.Body) || overlap(out, f.Headers) {
		return 0, ErrInvalid
	}
	copy(out[:4], "SPU3")
	out[4] = byte(f.Kind)
	binary.BigEndian.PutUint32(out[5:9], h.MetadataBytes)
	binary.BigEndian.PutUint64(out[9:17], h.BodyBytes)
	m := out[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]
	switch f.Kind {
	case Initialize:
		copy(m[:32], f.Nonce[:])
		copy(m[32:64], f.Binding[:])
		copy(m[64:96], f.Profile[:])
	case FetchRequest:
		copy(m[:32], f.Profile[:])
		copy(m[32:64], f.MAC[:])
		binary.BigEndian.PutUint32(m[64:68], uint32(len(f.Headers)))
		copy(m[68:], f.Headers)
	case ResponseStart:
		binary.BigEndian.PutUint16(m[:2], 200)
		binary.BigEndian.PutUint32(m[2:6], f.Count)
		binary.BigEndian.PutUint64(m[6:14], f.Bytes)
		copy(m[14:], f.Digest[:])
	case ResponseEnd:
		binary.BigEndian.PutUint32(m[:4], f.Count)
		binary.BigEndian.PutUint64(m[4:12], f.Bytes)
		copy(m[12:], f.Digest[:])
	case Pull, ResponsePart:
		binary.BigEndian.PutUint32(m, f.Index)
	}
	body := f.Body
	if f.Kind == ResponseStart {
		body = f.Headers
	}
	copy(out[HeaderBytes+int(h.MetadataBytes):], body)
	return len(out), nil
}

func DecodeView(b []byte, peer Direction, l Limits) (Frame, error) {
	if len(b) < HeaderBytes {
		return Frame{}, ErrInvalid
	}
	h, err := InspectHeader(b[:HeaderBytes], peer, l)
	if err != nil || h.TotalBytes != uint64(len(b)) {
		return Frame{}, ErrInvalid
	}
	offset := HeaderBytes + int(h.MetadataBytes)
	m := b[HeaderBytes:offset:offset]
	f, err := decodeMetadata(h, m)
	if err != nil {
		return Frame{}, err
	}
	if h.BodyBytes != 0 {
		if f.Kind == ResponseStart {
			f.Headers = b[offset:len(b):len(b)]
		} else {
			f.Body = b[offset:len(b):len(b)]
		}
	}
	if _, err := inspectFrame(f, l); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// Caller first obtained h from InspectHeader; metadata is checked before body
// assembly by the staged Sequence API. All returned slices borrow this region.
func decodeMetadata(h Header, m []byte) (Frame, error) {
	if uint64(len(m)) != uint64(h.MetadataBytes) {
		return Frame{}, ErrInvalid
	}
	m = m[:len(m):len(m)]
	f := Frame{Kind: h.Kind}
	switch h.Kind {
	case Initialize:
		copy(f.Nonce[:], m[:32])
		copy(f.Binding[:], m[32:64])
		copy(f.Profile[:], m[64:96])
	case FetchRequest:
		if uint64(binary.BigEndian.Uint32(m[64:68])) != uint64(len(m)-68) {
			return Frame{}, ErrInvalid
		}
		copy(f.Profile[:], m[:32])
		copy(f.MAC[:], m[32:64])
		f.Headers = m[68:]
	case ResponseStart:
		if binary.BigEndian.Uint16(m[:2]) != 200 {
			return Frame{}, ErrInvalid
		}
		f.Count, f.Bytes = binary.BigEndian.Uint32(m[2:6]), binary.BigEndian.Uint64(m[6:14])
		copy(f.Digest[:], m[14:])
	case ResponseEnd:
		f.Count, f.Bytes = binary.BigEndian.Uint32(m[:4]), binary.BigEndian.Uint64(m[4:12])
		copy(f.Digest[:], m[12:])
	case ResponsePart, Pull:
		f.Index = binary.BigEndian.Uint32(m)
	}
	return f, nil
}
