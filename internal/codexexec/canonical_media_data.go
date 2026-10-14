package codexexec

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"math"
	"strconv"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Logical payload/copy/node accounting only. Never native heap, model fit,
// currentness or a credential/send grant. One ledger spans stages A/B/C.
type CanonicalMediaLedgerV1 struct {
	copyLimit, liveLimit, nodeLimit        uint64
	copyBytes, liveBytes, peakBytes, nodes uint64
}
type CanonicalMediaAccountingV1 struct{ CopyBytes, LiveBytes, PeakBytes, Nodes uint64 }

func NewCanonicalMediaLedgerV1(limits domain.AttachedPiSDKNativeLimitsV1) (*CanonicalMediaLedgerV1, error) {
	if limits.PreSendAllocationBytes == 0 || limits.ProcessMemoryBytes == 0 || limits.JSONNodes == 0 || limits.PreSendAllocationBytes > 9007199254740991 || limits.ProcessMemoryBytes > 9007199254740991 || limits.JSONNodes > 9007199254740991 {
		return nil, ErrContract
	}
	return &CanonicalMediaLedgerV1{copyLimit: limits.PreSendAllocationBytes, liveLimit: limits.ProcessMemoryBytes, nodeLimit: limits.JSONNodes}, nil
}
func (l *CanonicalMediaLedgerV1) Reserve(copyBytes, liveBytes, nodes uint64) error {
	if l == nil || copyBytes > l.copyLimit-l.copyBytes || liveBytes > l.liveLimit-l.liveBytes || nodes > l.nodeLimit-l.nodes {
		return ErrContract
	}
	l.copyBytes += copyBytes
	l.liveBytes += liveBytes
	l.nodes += nodes
	if l.liveBytes > l.peakBytes {
		l.peakBytes = l.liveBytes
	}
	return nil
}
func (l *CanonicalMediaLedgerV1) EndLive(n uint64) error {
	if l == nil || n > l.liveBytes {
		return ErrContract
	}
	l.liveBytes -= n
	return nil
}
func (l *CanonicalMediaLedgerV1) Accounting() CanonicalMediaAccountingV1 {
	if l == nil {
		return CanonicalMediaAccountingV1{}
	}
	return CanonicalMediaAccountingV1{l.copyBytes, l.liveBytes, l.peakBytes, l.nodes}
}
func (l *CanonicalMediaLedgerV1) Remaining() CanonicalMediaAccountingV1 {
	if l == nil {
		return CanonicalMediaAccountingV1{}
	}
	return CanonicalMediaAccountingV1{CopyBytes: l.copyLimit - l.copyBytes, LiveBytes: l.liveLimit - l.liveBytes, Nodes: l.nodeLimit - l.nodes}
}
func mediaSum(v ...uint64) (uint64, error) {
	var n uint64
	for _, x := range v {
		if x > math.MaxUint64-n {
			return 0, ErrContract
		}
		n += x
	}
	return n, nil
}
func mediaMul(a, b uint64) (uint64, error) {
	if b != 0 && a > math.MaxUint64/b {
		return 0, ErrContract
	}
	return a * b, nil
}

type CanonicalMediaSourceDataV1 struct {
	EventID                     domain.SessionEventID
	Sequence                    uint64
	Ordinal                     int
	Name, MediaType             string
	Blob                        domain.BlobRef
	ImageOrdinal, Width, Height int
	Body                        []byte
}
type CanonicalMediaMessageV1 struct {
	Role      string                  `json:"role"`
	Content   []CanonicalMediaBlockV1 `json:"content"`
	Timestamp int64                   `json:"timestamp"`
}
type CanonicalMediaBlockV1 struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	MIMEType string `json:"mimeType,omitempty"`
	Data     []byte `json:"data,omitempty"`
}
type CanonicalMediaDataV1 struct {
	ctx                                      context.Context
	input                                    *attachedworkerdaemon.VerifiedAttachedInputV1
	media                                    *mediaInput
	frameDigest                              [32]byte
	ledger                                   *CanonicalMediaLedgerV1
	messages                                 []CanonicalMediaMessageV1
	timestamp                                int64
	prerequisiteLive, typedLive, messageWire uint64
}

func (CanonicalMediaSourceDataV1) String() string {
	return "CanonicalMediaSourceDataV1{data:[redacted]}"
}
func (v CanonicalMediaSourceDataV1) GoString() string { return v.String() }
func (CanonicalMediaMessageV1) String() string        { return "CanonicalMediaMessageV1{data:[redacted]}" }
func (v CanonicalMediaMessageV1) GoString() string    { return v.String() }
func (CanonicalMediaBlockV1) String() string          { return "CanonicalMediaBlockV1{data:[redacted]}" }
func (v CanonicalMediaBlockV1) GoString() string      { return v.String() }
func (CanonicalMediaDataV1) String() string           { return "CanonicalMediaDataV1{data:[redacted]}" }
func (v CanonicalMediaDataV1) GoString() string       { return v.String() }

// Stage A: bounded source payload envelopes before existing Clone/validation/
// kernel. L is the original full seal. Three canonical projection passes can
// emit a record+grow history per event and hold snapshot payloads. Reserve that
// cumulative work, clone encode/decode, prompt buffer/copy and metadata payload.
// Container/runtime/codec native allocations remain UNAVAILABLE. The existing
// 96MiB Go validator ceiling is NOT a child-P debit.
func ProjectCanonicalMediaDataV1(ctx context.Context, identity attachedworkerdaemon.InvocationIdentity, input *attachedworkerdaemon.VerifiedAttachedInputV1, ledger *CanonicalMediaLedgerV1) (*CanonicalMediaDataV1, error) {
	if ctx == nil || ctx.Err() != nil || ledger == nil || input == nil || identity.Validate() != nil || input.CanonicalContext == nil || input.Job.ContextWindow == nil || input.Job.Limits.MaxInputBytes == 0 || input.Job.Limits.MaxContextBytes == 0 || input.Job.WorkspaceSnapshot != nil || input.Job.SkillBundle != nil || len(input.Job.AllowedMCPServers) != 0 {
		return nil, ErrContract
	}
	a := input.AcceptedAttempt
	if identity.TenantID != a.TenantID || identity.OwnerUserID != a.OwnerUserID || identity.WorkerID != a.WorkerID || identity.RunID != a.RunID || identity.AttemptID != a.AttemptID || identity.LeaseID != a.LeaseID || identity.FenceToken != a.LeaseGeneration {
		return nil, ErrContract
	}
	limit := uint64(1 << 20)
	if input.Job.Limits.MaxInputBytes < limit {
		limit = input.Job.Limits.MaxInputBytes
	}
	if input.Job.Limits.MaxContextBytes < limit {
		limit = input.Job.Limits.MaxContextBytes
	}
	events := input.Job.ContextWindow.ThroughSequence
	if events == 0 || events > uint64(input.Job.Limits.EffectiveMaxContextEvents()) || events > ledger.Remaining().Nodes || uint64(len(input.CanonicalContext.Input.Events)) > events || len(input.CanonicalContext.Attachments) > 64 || len(input.CanonicalContext.EventBodies) != len(input.CanonicalContext.Input.Events) {
		return nil, ErrContract
	}
	held := uint64(len(input.Context))
	var err error
	for _, b := range input.CanonicalContext.EventBodies {
		held, err = mediaSum(held, uint64(len(b)))
		if err != nil {
			return nil, err
		}
	}
	held, err = mediaSum(held, uint64(len(input.CanonicalContext.SnapshotBytes)))
	if err != nil {
		return nil, err
	}
	for _, b := range input.CanonicalContext.Attachments {
		held, err = mediaSum(held, uint64(len(b.Body)))
		if err != nil {
			return nil, err
		}
	}
	for _, b := range input.Artifacts {
		held, err = mediaSum(held, uint64(len(b.Body)))
		if err != nil {
			return nil, err
		}
	}
	if held > limit {
		return nil, ErrContract
	}
	passes, err := mediaMul(events, 6)
	if err != nil {
		return nil, err
	}
	passes, err = mediaSum(passes, 14)
	if err != nil {
		return nil, err
	}
	work, err := mediaMul(passes, limit)
	if err != nil {
		return nil, err
	}
	live, err := mediaMul(limit, 8)
	if err != nil {
		return nil, err
	}
	indexBytes, err := mediaMul(events+uint64(len(input.CanonicalContext.Attachments))+1, 24)
	if err != nil {
		return nil, err
	}
	live, err = mediaSum(live, held, indexBytes)
	if err != nil {
		return nil, err
	}
	// At most one decoded key/scalar/container per bounded input byte, across
	// the three closed validating/projection appearances; no byte-per-node claim.
	sourceNodes, err := mediaMul(limit, 3)
	if err != nil {
		return nil, err
	}
	if ledger.Reserve(work, live, sourceNodes) != nil {
		return nil, ErrContract
	}
	if ctx.Err() != nil || input.ValidateFor(identity) != nil || !preparedIngressEnvelope(input) || ctx.Err() != nil {
		return nil, ErrContract
	}
	owned, err := input.Clone()
	if err != nil {
		return nil, ErrContract
	}
	if ctx.Err() != nil {
		owned.Clear()
		return nil, ErrContract
	}
	media, err := prepareCanonicalMediaIndexed(owned, "sessionless.canonical-media-data.v1", true)
	if err != nil {
		owned.Clear()
		return nil, err
	}
	var timestamp int64
	for _, e := range owned.CanonicalContext.Input.Events {
		if e.ID == owned.Job.TriggerEventID {
			timestamp = e.CreatedAt.UnixMilli()
		}
	}
	if timestamp <= 0 || timestamp > 9007199254740991 || ctx.Err() != nil {
		media.clear()
		owned.Clear()
		return nil, ErrContract
	}
	return &CanonicalMediaDataV1{ctx: ctx, input: owned, media: media, frameDigest: sha256.Sum256(media.prompt), ledger: ledger, timestamp: timestamp, prerequisiteLive: live}, nil
}
func (v *CanonicalMediaDataV1) FramesV1() []byte {
	if v == nil || v.media == nil {
		return nil
	}
	return v.media.prompt
}
func (v *CanonicalMediaDataV1) AssociationDigestV1() [32]byte {
	if v == nil || v.media == nil {
		return [32]byte{}
	}
	return v.media.associationDigest
}
func (v *CanonicalMediaDataV1) TimestampV1() int64 {
	if v == nil {
		return 0
	}
	return v.timestamp
}
func (v *CanonicalMediaDataV1) valid() bool {
	if v == nil || v.ctx == nil || v.ctx.Err() != nil || v.media == nil || v.input == nil || sha256.Sum256(v.media.prompt) != v.frameDigest {
		return false
	}
	for _, s := range v.media.sources {
		if !mediaBodyMatches(s) {
			return false
		}
	}
	return true
}

// Borrow only; no split, JSON maps, strings or wrapper slice to rediscover ordinals.
func (v *CanonicalMediaDataV1) VisitBlocksV1(visit func([]byte, *CanonicalMediaSourceDataV1) error) error {
	if !v.valid() || visit == nil {
		return ErrContract
	}
	for _, f := range v.media.frames {
		if v.ctx.Err() != nil {
			return ErrContract
		}
		if f.start < 0 || f.end < f.start || f.end > len(v.media.prompt) {
			return ErrContract
		}
		if err := visit(v.media.prompt[f.start:f.end], nil); err != nil {
			return err
		}
		if f.imageOrdinal != 0 {
			var source *mediaSource
			for i := range v.media.sources {
				if v.media.sources[i].ImageOrdinal == f.imageOrdinal {
					source = &v.media.sources[i]
					break
				}
			}
			if source == nil {
				return ErrContract
			}
			s := CanonicalMediaSourceDataV1{source.EventID, source.Sequence, source.Ordinal, source.Name, source.MediaType, source.Blob, source.ImageOrdinal, source.Width, source.Height, source.Body}
			if err := visit(nil, &s); err != nil {
				return err
			}
		}
	}
	if !v.valid() {
		return ErrContract
	}
	return nil
}
func (v *CanonicalMediaDataV1) SourcesV1() []CanonicalMediaSourceDataV1 {
	if !v.valid() {
		return nil
	}
	if v.ledger.Reserve(0, 0, uint64(len(v.media.sources))) != nil {
		return nil
	}
	result := make([]CanonicalMediaSourceDataV1, len(v.media.sources))
	for i, s := range v.media.sources {
		result[i] = CanonicalMediaSourceDataV1{s.EventID, s.Sequence, s.Ordinal, s.Name, s.MediaType, s.Blob, s.ImageOrdinal, s.Width, s.Height, s.Body}
	}
	return result
}
func mediaByteQuotedSize(b []byte) (uint64, error) {
	if !utf8.Valid(b) {
		return 0, ErrContract
	}
	n := uint64(2)
	for len(b) > 0 {
		r, k := utf8.DecodeRune(b)
		b = b[k:]
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			n += 2
		case '<', '>', '&', '\u2028', '\u2029':
			n += 6
		default:
			if r < 32 {
				n += 6
			} else {
				n += uint64(k)
			}
		}
	}
	return n, nil
}
func (v *CanonicalMediaDataV1) NativeSizesV1() (textBytes, textUnits, base64Bytes, nodes, blocks uint64, err error) {
	err = v.VisitBlocksV1(func(text []byte, image *CanonicalMediaSourceDataV1) error {
		blocks++
		if image != nil {
			b := uint64(base64.StdEncoding.EncodedLen(len(image.Body)))
			base64Bytes += b
			nodes += 7
			return nil
		}
		if !utf8.Valid(text) {
			return ErrContract
		}
		textBytes += uint64(len(text))
		for len(text) > 0 {
			r, k := utf8.DecodeRune(text)
			text = text[k:]
			textUnits++
			if r > 0xffff {
				textUnits++
			}
		}
		nodes += 5
		return nil
	})
	return
}

// Stage C output: precount full typed wire before strings/blocks. This does not
// reserve future native body/SDK work; full request compiler reserves that FIRST.
func (v *CanonicalMediaDataV1) OriginalMessagesV1() ([]CanonicalMediaMessageV1, error) {
	return v.PrepareOriginalMessagesV1(0, 0, 0)
}

// The complete body compiler combines its future emission/SDK/phase charges
// with the typed output's own costs in ONE atomic Stage-C reservation.
func (v *CanonicalMediaDataV1) PrepareOriginalMessagesV1(extraCopy, extraLive, extraNodes uint64) ([]CanonicalMediaMessageV1, error) {
	if !v.valid() {
		return nil, ErrContract
	}
	if v.messages != nil {
		limit := uint64(1 << 20)
		if v.input.Job.Limits.MaxInputBytes < limit {
			limit = v.input.Job.Limits.MaxInputBytes
		}
		if v.input.Job.Limits.MaxContextBytes < limit {
			limit = v.input.Job.Limits.MaxContextBytes
		}
		if v.messageWire > limit || extraCopy != 0 || extraLive != 0 || extraNodes != 0 {
			return nil, ErrContract
		}
		if len(v.messages) != 1 || v.messages[0].Role != "user" || v.messages[0].Timestamp != v.timestamp {
			return nil, ErrContract
		}
		at := 0
		if v.VisitBlocksV1(func(text []byte, image *CanonicalMediaSourceDataV1) error {
			if at >= len(v.messages[0].Content) {
				return ErrContract
			}
			b := v.messages[0].Content[at]
			at++
			if image == nil {
				if b.Type != "text" || b.MIMEType != "" || len(b.Data) != 0 || len(b.Text) != len(text) {
					return ErrContract
				}
				for i := range text {
					if b.Text[i] != text[i] {
						return ErrContract
					}
				}
			} else if b.Type != "image" || b.Text != "" || b.MIMEType != image.MediaType || len(b.Data) != len(image.Body) || sha256.Sum256(b.Data) != sha256.Sum256(image.Body) {
				return ErrContract
			}
			return nil
		}) != nil || at != len(v.messages[0].Content) {
			return nil, ErrContract
		}
		return v.messages, nil
	}
	wire := uint64(len(`[{"role":"user","content":[],"timestamp":}]`) + len(strconv.FormatInt(v.timestamp, 10)))
	var copying, nodes, blocks uint64
	err := v.VisitBlocksV1(func(text []byte, image *CanonicalMediaSourceDataV1) error {
		if blocks > 0 {
			wire++
		}
		blocks++
		if image != nil {
			b := uint64(base64.StdEncoding.EncodedLen(len(image.Body)))
			wire += uint64(len(`{"type":"image","mimeType":,"data":""}`)) + uint64(mediaQuotedSize(image.MediaType)) + b
			copying += 2 * b
			nodes += 7
		} else {
			q, e := mediaByteQuotedSize(text)
			if e != nil {
				return e
			}
			wire += uint64(len(`{"type":"text","text":}`)) + q
			copying += uint64(len(text)) * 2
			nodes += 5
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	limit := uint64(1 << 20)
	if v.input.Job.Limits.MaxInputBytes < limit {
		limit = v.input.Job.Limits.MaxInputBytes
	}
	if v.input.Job.Limits.MaxContextBytes < limit {
		limit = v.input.Job.Limits.MaxContextBytes
	}
	copyTotal, e := mediaSum(copying, extraCopy)
	if e != nil {
		return nil, e
	}
	liveTotal, e := mediaSum(copying, extraLive)
	if e != nil {
		return nil, e
	}
	nodeTotal, e := mediaSum(nodes, 7, extraNodes)
	if e != nil {
		return nil, e
	}
	if wire > limit || blocks > uint64(math.MaxInt) || v.ledger.Reserve(copyTotal, liveTotal, nodeTotal) != nil {
		return nil, ErrContract
	}
	result := []CanonicalMediaMessageV1{{Role: "user", Content: make([]CanonicalMediaBlockV1, 0, int(blocks)), Timestamp: v.timestamp}}
	if v.VisitBlocksV1(func(text []byte, image *CanonicalMediaSourceDataV1) error {
		if image != nil {
			result[0].Content = append(result[0].Content, CanonicalMediaBlockV1{Type: "image", MIMEType: image.MediaType, Data: image.Body})
		} else {
			result[0].Content = append(result[0].Content, CanonicalMediaBlockV1{Type: "text", Text: string(text)})
		}
		return nil
	}) != nil {
		return nil, ErrContract
	}
	v.messages = result
	v.typedLive = copying
	v.messageWire = wire
	return result, nil
}
func (v *CanonicalMediaDataV1) Clear() {
	if v != nil && v.media != nil {
		v.media.clear()
		v.input.Clear()
		if v.ledger != nil {
			_ = v.ledger.EndLive(v.prerequisiteLive + v.typedLive)
		}
		*v = CanonicalMediaDataV1{}
	}
}
