package pisdkphasewire

import (
	"bytes"
	"encoding/binary"
	"math"
	"unicode/utf8"
)

const (
	HeaderBytes                = 17
	ChildMetadataMaxBytes      = 129
	ChildMetadataNodes         = 7
	ChildRequest          byte = 1
	ParentReply           byte = 2
	ParentRetire          byte = 3
)

// Header is only a structural length preflight; it is not an allocation grant.
type Header struct {
	Kind          byte
	MetadataBytes uint32
	RawBodyBytes  uint64
	TotalBytes    uint64
}

// InspectHeader accepts EXACTLY the first 17 bytes, before any body buffer/read.
// maxNativeBodyBytes is an explicit positive NON-SERVING symbolic input; the
// real original source-owned P/platform/all-copy fit must precede genuine use.
func InspectHeader(b []byte, maxNativeBodyBytes uint64) (Header, error) {
	if len(b) != HeaderBytes || maxNativeBodyBytes == 0 || string(b[:4]) != "SPU1" {
		return Header{}, ErrInvalid
	}
	h := Header{Kind: b[4], MetadataBytes: binary.BigEndian.Uint32(b[5:9]), RawBodyBytes: binary.BigEndian.Uint64(b[9:17])}
	switch h.Kind {
	case ChildRequest:
		if h.MetadataBytes != ChildMetadataMaxBytes || h.RawBodyBytes == 0 || h.RawBodyBytes > maxNativeBodyBytes {
			return Header{}, ErrInvalid
		}
	case ParentReply:
		if h.MetadataBytes < DeniedReplyBytes || h.MetadataBytes > ReplyMaxBytes || h.RawBodyBytes != 0 {
			return Header{}, ErrInvalid
		}
	case ParentRetire:
		if h.MetadataBytes != 0 || h.RawBodyBytes != 0 {
			return Header{}, ErrInvalid
		}
	default:
		return Header{}, ErrInvalid
	}
	fixed := uint64(HeaderBytes) + uint64(h.MetadataBytes)
	maxInt := uint64(^uint(0) >> 1)
	if fixed > maxInt || h.RawBodyBytes > maxInt-fixed {
		return Header{}, ErrInvalid
	}
	h.TotalBytes = fixed + h.RawBodyBytes
	return h, nil
}

// Frame contains owned structural copies, not a source snapshot/send permit.
// Child body must remain the COMPLETE independently compared original UTF8.
type Frame struct {
	Kind                 byte
	BackendProfileDigest string
	Reply                Reply
	Body                 []byte
}

// CompareBodyProfile performs byte identity only. The independent original
// pre-Issue projection/source-owned P must already exist; equality never creates
// its provenance, SAME-RPC binding, native fit or permission.
func CompareBodyProfile(f Frame, expectedBody []byte, expectedProfileDigest string) error {
	if f.Kind != ChildRequest || !digest(expectedProfileDigest) || f.BackendProfileDigest != expectedProfileDigest ||
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

func EncodeFrame(f Frame, maxNativeBodyBytes uint64) ([]byte, error) {
	if maxNativeBodyBytes == 0 {
		return nil, ErrInvalid
	}
	w := writer{}
	switch f.Kind {
	case ChildRequest:
		if !digest(f.BackendProfileDigest) || f.Reply != (Reply{}) || len(f.Body) == 0 {
			return nil, ErrInvalid
		}
		writeChildMetadata(&w, f.BackendProfileDigest)
	case ParentReply:
		if f.BackendProfileDigest != "" || len(f.Body) != 0 || !validReply(f.Reply) {
			return nil, ErrInvalid
		}
		writeReply(&w, f.Reply)
	case ParentRetire:
		if f.BackendProfileDigest != "" || f.Reply != (Reply{}) || len(f.Body) != 0 {
			return nil, ErrInvalid
		}
	default:
		return nil, ErrInvalid
	}
	var header [HeaderBytes]byte
	copy(header[:4], "SPU1")
	header[4] = f.Kind
	binary.BigEndian.PutUint32(header[5:9], uint32(w.n))
	binary.BigEndian.PutUint64(header[9:17], uint64(len(f.Body)))
	h, err := InspectHeader(header[:], maxNativeBodyBytes)
	if err != nil {
		return nil, err
	}
	if f.Kind == ChildRequest && !utf8.Valid(f.Body) {
		return nil, ErrInvalid
	}
	b := make([]byte, int(h.TotalBytes))
	copy(b[:HeaderBytes], header[:])
	meta := writer{b: b[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]}
	if f.Kind == ChildRequest {
		writeChildMetadata(&meta, f.BackendProfileDigest)
	} else if f.Kind == ParentReply {
		writeReply(&meta, f.Reply)
	}
	if meta.n != int(h.MetadataBytes) {
		return nil, ErrInvalid
	}
	copy(b[HeaderBytes+int(h.MetadataBytes):], f.Body)
	return b, nil
}

// DecodeFrame rejects truncation, extra frames/trailing bytes and invalid UTF8.
// The header can and must be inspected separately before a real caller allocates
// or receives a complete body. This helper does not operate an IPC pump.
func DecodeFrame(b []byte, maxNativeBodyBytes uint64) (Frame, error) {
	if len(b) < HeaderBytes {
		return Frame{}, ErrInvalid
	}
	h, err := InspectHeader(b[:HeaderBytes], maxNativeBodyBytes)
	if err != nil || h.TotalBytes != uint64(len(b)) {
		return Frame{}, ErrInvalid
	}
	metadata := b[HeaderBytes : HeaderBytes+int(h.MetadataBytes)]
	body := b[HeaderBytes+int(h.MetadataBytes):]
	f := Frame{Kind: h.Kind}
	switch h.Kind {
	case ChildRequest:
		pre := parser{b: metadata, ok: true}
		parseChildMetadata(&pre)
		if !pre.ok || pre.i != len(metadata) || !utf8.Valid(body) {
			return Frame{}, ErrInvalid
		}
		p := parser{b: metadata, ok: true, owned: true}
		f.BackendProfileDigest = parseChildMetadata(&p)
		if !p.ok || p.i != len(metadata) {
			return Frame{}, ErrInvalid
		}
		f.Body = make([]byte, len(body))
		copy(f.Body, body)
	case ParentReply:
		f.Reply, err = DecodeReply(metadata)
		if err != nil {
			return Frame{}, err
		}
	}
	return f, nil
}

// Counts describes only caller-supplied counts. Passing it proves neither an
// actual single invocation nor measured source RPC/cancellation ownership.
type Counts struct {
	PreIssueRequests, PreIssueReplies           uint64
	PhysicalSendRequests, PhysicalSendReplies   uint64
	FinalizeRequests, FinalizeReplies           uint64
	ChildRequests, ParentReplies, ParentRetires uint64
}

func ValidateCounts(c Counts) error {
	for _, v := range [...]uint64{c.PreIssueRequests, c.PreIssueReplies, c.PhysicalSendRequests, c.PhysicalSendReplies, c.FinalizeRequests, c.FinalizeReplies, c.ChildRequests, c.ParentReplies, c.ParentRetires} {
		if v > 1 {
			return ErrInvalid
		}
	}
	if c.PreIssueReplies > c.PreIssueRequests || c.PhysicalSendReplies > c.PhysicalSendRequests || c.FinalizeReplies > c.FinalizeRequests ||
		c.ParentReplies > c.ChildRequests || c.ChildRequests > c.PhysicalSendRequests {
		return ErrInvalid
	}
	return nil
}

// Accounting is a pure conservative combined logical declaration. There is no
// credit pool/refund/default/grant, native/GC claim or original body replacement.
type Accounting struct {
	RemoteBodyBytes  uint64
	IPCBytes         uint64
	ApplicationBytes uint64
	LogicalCopyBytes uint64
	JSONNodes        uint64
}

func MeasureAccounting(n uint64) (Accounting, error) {
	if n == 0 || n > (math.MaxUint64-58421)/2 {
		return Accounting{}, ErrInvalid
	}
	return Accounting{RemoteBodyBytes: 15195, IPCBytes: 3170 + n, ApplicationBytes: 18365 + n, LogicalCopyBytes: 58421 + 2*n, JSONNodes: SharedNodes}, nil
}

// CheckSharedRemainder compares ONCE against an explicit original ledger
// remainder. It neither reserves it nor authorizes use/Issue/serving.
func CheckSharedRemainder(n, copyRemainder, nodeRemainder uint64) (Accounting, error) {
	a, err := MeasureAccounting(n)
	if err != nil || a.LogicalCopyBytes > copyRemainder || a.JSONNodes > nodeRemainder {
		return Accounting{}, ErrInvalid
	}
	return a, nil
}
