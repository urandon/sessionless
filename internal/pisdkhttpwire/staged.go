package pisdkhttpwire

func (v *sequenceState) expected(k Kind) bool {
	switch k {
	case Initialize:
		return v.phase == 0
	case FetchRequest:
		return v.phase == Initialize
	case ResponseStart:
		return v.phase == FetchRequest
	case Pull:
		return (v.phase == ResponseStart || v.phase == ResponsePart) && v.next < v.count
	case ResponsePart:
		return v.phase == Pull && v.next < v.count && v.seen < v.bytes
	case ResponseEnd:
		return v.phase == ResponsePart && v.next == v.count && v.seen == v.bytes
	case StructuredFinal:
		return v.phase == ResponseEnd
	case Retire:
		return true
	}
	return false
}

// BeginHeader is the stream-reader gate BEFORE reading either declared region.
// It checks phase/direction/caps using only the 17 header bytes. Call metadata
// then body once each; no other frame can interleave with this pending frame.
func (s *Sequence) BeginHeader(b []byte, peer Direction) (Header, error) {
	if s == nil || s.state == nil {
		return Header{}, ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		v.failed.Store(true)
		return Header{}, ErrInvalid
	}
	defer v.gate.Unlock()
	h, err := InspectHeader(b, peer, v.limits)
	if err != nil || v.retired || v.finished || v.pendingStage != 0 ||
		v.failed.Load() && h.Kind != Retire || !v.expected(h.Kind) ||
		h.Kind == ResponsePart && h.BodyBytes != min(v.chunk, v.bytes-v.seen) {
		v.failed.Store(true)
		return Header{}, ErrInvalid
	}
	v.pendingHeader, v.pendingPeer, v.pendingStage = h, peer, 1
	if v.failed.Load() && h.Kind != Retire {
		return Header{}, ErrInvalid
	}
	return h, nil
}

// PreflightMetadata must succeed BEFORE reading/assembling the declared body.
// This is where Fetch's embedded Hq and canonical header block are validated.
// m remains borrowed until AcceptBody/retirement joins; no body is accepted here.
func (s *Sequence) PreflightMetadata(m []byte) error {
	if s == nil || s.state == nil {
		return ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		v.failed.Store(true)
		return ErrInvalid
	}
	defer v.gate.Unlock()
	fail := func() error { v.failed.Store(true); return ErrInvalid }
	if v.pendingStage != 1 || v.retired || v.finished || v.failed.Load() && v.pendingHeader.Kind != Retire {
		return fail()
	}
	f, err := decodeMetadata(v.pendingHeader, m)
	if err != nil {
		return fail()
	}
	switch f.Kind {
	case FetchRequest:
		if f.Profile != v.profile || ValidateHeaders(f.Headers, v.limits.RequestHeaderBytes, true) != nil {
			return fail()
		}
	case ResponseStart:
		if !validSummary(f.Count, f.Bytes, v.limits) {
			return fail()
		}
	case Pull, ResponsePart:
		if f.Index != v.next || v.next >= v.count {
			return fail()
		}
	case ResponseEnd:
		if f.Count != v.count || f.Bytes != v.bytes || f.Digest != v.digest {
			return fail()
		}
	}
	v.pendingFrame, v.pendingStage = f, 2
	if v.failed.Load() && f.Kind != Retire {
		return ErrInvalid
	}
	return nil
}

// AcceptBody completes exactly the pending frame after its read. The body is
// still caller-owned, not copied/allocated here. Full wire-field validation and
// all actual response hash comparisons run before advancing the sequence.
func (s *Sequence) AcceptBody(body []byte) error {
	if s == nil || s.state == nil {
		return ErrInvalid
	}
	v := s.state
	if !v.gate.TryLock() {
		v.failed.Store(true)
		return ErrInvalid
	}
	defer v.gate.Unlock()
	if v.pendingStage != 2 || uint64(len(body)) != v.pendingHeader.BodyBytes || v.failed.Load() && v.pendingHeader.Kind != Retire {
		v.failed.Store(true)
		return ErrInvalid
	}
	f := v.pendingFrame
	if len(body) != 0 {
		if f.Kind == ResponseStart {
			f.Headers = body[:len(body):len(body)]
		} else {
			f.Body = body[:len(body):len(body)]
		}
	}
	v.pendingFrame, v.pendingStage = Frame{}, 0
	return v.acceptLocked(v.pendingPeer, f)
}
