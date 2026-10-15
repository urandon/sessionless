package pisdkmedia

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// PrepareRequestDataV1 transfers one independent, prospectively funded DATA
// partition to the original composite owner before Issue. That owner must
// clear these exact four buffers and retire their live debit after consumers
// join. The SAME ledger is not reset; no native/fit/source grant is produced.
func (p *RequestProjectionV1) PrepareRequestDataV1(model, schema []byte) (out attachedworkerdaemon.PreparedAPIRequestV1, err error) {
	if p == nil || p.ctx == nil || p.ctx.Err() != nil || p.ledger == nil || len(p.body) == 0 || len(model) == 0 || len(schema) == 0 ||
		uint64(len(model)) > p.config.profile.Limits.NativeRequestBytes || uint64(len(schema)) > p.config.profile.Limits.NativeRequestBytes {
		return out, ErrProjection
	}
	l := p.config.profile.Limits
	var visit uint64
	for _, h := range p.config.headers {
		visit += uint64(len(h.Name) + len(h.Value))
	}
	if p.ledger.Reserve(8*visit+128, 0, 45) != nil {
		return out, ErrProjection
	}
	m := wireWriter{cap: l.RequestHeaderBytes, maxDepth: 1, maxNodes: l.JSONNodes}
	writeExecutionHeaders(&m, p.config.headers)
	if m.err != nil {
		return out, ErrProjection
	}
	metadata := uint64(reflect.TypeOf(out).Size())
	live := uint64(len(p.body)+len(model)+len(schema)) + m.bytes + metadata
	if p.ledger.Reserve(4*live+96, live+96, m.nodes) != nil {
		return out, ErrProjection
	}
	defer func() {
		_ = p.ledger.EndLive(96) // hash/hex scratch only; cumulative work remains
		if err != nil {
			clear(out.Body)
			clear(out.Model)
			clear(out.Schema)
			clear(out.Headers)
			out = attachedworkerdaemon.PreparedAPIRequestV1{}
			_ = p.ledger.EndLive(live)
		}
	}()
	hash := sha256.Sum256(schema)
	if hex.EncodeToString(hash[:]) != p.config.schemaDigest || sha256.Sum256(p.body) != p.bodyDigest {
		return out, ErrProjection
	}
	h := wireWriter{out: make([]byte, 0, int(m.bytes)), cap: m.bytes, maxDepth: 1, maxNodes: l.JSONNodes}
	writeExecutionHeaders(&h, p.config.headers)
	out = attachedworkerdaemon.PreparedAPIRequestV1{Profile: p.config.profile, Body: bytes.Clone(p.body), Model: bytes.Clone(model), Schema: bytes.Clone(schema), Headers: h.out}
	if h.err != nil || p.ctx.Err() != nil {
		return out, ErrProjection
	}
	return out, nil
}
