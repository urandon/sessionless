package providercredential

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash"
	"io"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
)

const piAuthDomain = "Sessionless/Pi/SPU3/Auth/1\x00"

var piAuthHeaderNames = [...]string{"accept", "content-type", "user-agent", "x-stainless-arch", "x-stainless-lang", "x-stainless-os", "x-stainless-package-version", "x-stainless-retry-count", "x-stainless-runtime", "x-stainless-runtime-version", "x-stainless-timeout"}

type piRequestAuthentication struct {
	binding, profile              [32]byte
	endpoint                      string
	headers                       []byte
	requestHeaders                uint64
	responseHeaders, responseBody uint64
	responseLimits                domain.AttachedPiSDKNativeLimitsV1
}

// PrepareRequestAuthenticationV1 seals comparison DATA before original Issue.
// It neither qualifies a profile nor exposes a MAC/key/send capability. Only
// the future PRIVATE fixed HTTPS consumer may compare an actual SDK request.
// The existing SPU2/zero-argument fetch seam is not silently upgraded to SPU3.
func (p *PreparedPiFileUseV1) PrepareRequestAuthenticationV1(request attachedworkerdaemon.PreparedAPIRequestV1) (err error) {
	if p == nil || p.state == nil {
		return ErrInvalid
	}
	s := p.state
	if !s.mu.TryLock() {
		s.abort()
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.authPrepared || s.claimed.Load() || s.callback != 0 || s.file == nil || s.file.ctx.Err() != nil {
		s.abort()
		return ErrConsumed
	}
	s.authPrepared = true
	f := s.file
	// Hold the original registry lock until the seal is complete: actual Issue
	// cannot consume this recipe halfway through its pre-Issue reservation.
	f.service.mu.Lock()
	defer f.service.mu.Unlock()
	if f.service.preparedPiFiles[f.scope] != s {
		s.abort()
		return ErrConsumed
	}
	if len(request.Headers) == 0 || len(request.Headers) > 1<<20 || len(request.Body) != len(s.body) ||
		uint64(len(request.Headers)) > request.Profile.Limits.RequestHeaderBytes || len(request.Profile.Model.Endpoint) > 2048 {
		s.abort()
		return ErrInvalid
	}
	// Logical payload/validation schedule, NOT a full Go allocator/TLS/native
	// heap qualification. Profile JSON is bounded by its existing canonical
	// ceiling; original single-turn binding validation uses the 1MiB bound.
	capacity := uint64(len(request.Headers)) + 128
	metadata := uint64(reflect.TypeOf(piRequestAuthentication{}).Size())
	// Keep HMAC/hash/file-verifier scratch live until original callback cleanup
	// joins. Preparation-only parser/profile scratch is retired below.
	live := capacity + metadata + 16384 + 4096
	scratch := uint64(1<<20) + 4*domain.AttachedPiSDKCanonicalMaxBytesV1 + 16*uint64(len(request.Headers)) + 16384
	work := 8*scratch + 4*uint64(len(s.body)) + 8*uint64(f.maxBytes)
	if s.ledger.Reserve(work, live+scratch, 1) != nil {
		s.abort()
		return ErrInvalid
	}
	s.live += live
	defer func() {
		cleanup := s.ledger.EndLive(scratch)
		if err != nil {
			if e := s.ledger.EndLive(live); e == nil {
				s.live -= live
			} else if cleanup == nil {
				cleanup = e
			}
			s.abort()
		}
		if cleanup != nil {
			s.abort()
			err = ErrInvalid
		}
	}()
	if !bytes.Equal(request.Body, s.body) || sha256.Sum256(s.body) != s.bodyHash ||
		!piBoundedProfileStrings(request.Profile) ||
		request.Profile.Model.ModelVendorID != f.binding.value.ModelVendorID || request.Profile.Model.ModelID != f.binding.value.ModelID {
		return ErrInvalid
	}
	profile, e := request.Profile.Digest()
	if e != nil || profile != s.profile {
		return ErrInvalid
	}
	descriptor, e := request.Profile.Descriptor()
	if e != nil || descriptor != f.binding.value.Backend {
		return ErrInvalid
	}
	b := f.binding.value
	if !f.binding.evidenceExpiry.IsZero() {
		expiry := f.binding.evidenceExpiry
		b.EvidenceExpiresAt = &expiry
	}
	binding, e := b.Digest()
	if e != nil {
		return ErrInvalid
	}
	a := &piRequestAuthentication{endpoint: request.Profile.Model.Endpoint, requestHeaders: request.Profile.Limits.RequestHeaderBytes, responseHeaders: request.Profile.Limits.ResponseHeaderBytes, responseBody: request.Profile.Limits.ResponseBodyBytes}
	a.responseLimits = request.Profile.Limits
	if _, e = hex.Decode(a.binding[:], []byte(binding)); e != nil {
		return ErrInvalid
	}
	if _, e = hex.Decode(a.profile[:], []byte(profile)); e != nil {
		return ErrInvalid
	}
	a.headers, e = piCanonicalRequestHeaders(request.Headers, int(capacity))
	if e != nil || uint64(len(a.headers)) > request.Profile.Limits.RequestHeaderBytes {
		return ErrInvalid
	}
	s.auth = a
	return nil
}

// The domain validator may scan UTF-8/whitespace before rejecting a token's
// length. Cap the complete fixed-shape string payload with O(1) length reads
// first, so even a forged Go DATA profile cannot cause an unfunded huge scan.
func piBoundedProfileStrings(profile domain.AttachedPiSDKNativeProfileV1) bool {
	var total int
	var visit func(reflect.Value) bool
	visit = func(v reflect.Value) bool {
		for i := range v.NumField() {
			f := v.Field(i)
			if f.Kind() == reflect.String {
				if f.Len() > domain.AttachedPiSDKCanonicalMaxBytesV1-total {
					return false
				}
				total += f.Len()
			} else if f.Kind() == reflect.Struct && !visit(f) {
				return false
			}
		}
		return true
	}
	return visit(reflect.ValueOf(profile))
}

// The JSON comes from the original prepared request, not IPC. Token decoding
// rejects duplicates/unknowns/nesting and preserves the exact decoded value.
// Allocation/work must have been reserved before entry.
func piCanonicalRequestHeaders(raw []byte, capacity int) ([]byte, error) {
	var values [11]string
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrInvalid
	}
	var seen uint16
	for d.More() {
		name, err := d.Token()
		if err != nil {
			return nil, ErrInvalid
		}
		index := -1
		for i, allowed := range piAuthHeaderNames {
			if name == allowed {
				index = i
				break
			}
		}
		if index < 0 || seen&(1<<index) != 0 {
			return nil, ErrInvalid
		}
		token, err := d.Token()
		value, ok := token.(string)
		if err != nil || !ok || !piHeaderValue(value) {
			return nil, ErrInvalid
		}
		values[index] = value
		seen |= 1 << index
	}
	token, err = d.Token()
	if err != nil || token != json.Delim('}') || seen != (1<<11)-1 {
		return nil, ErrInvalid
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrInvalid
	}
	n := 2
	for i, value := range values {
		n += 6 + len(piAuthHeaderNames[i]) + len(value)
	}
	if n > capacity {
		return nil, ErrInvalid
	}
	out := make([]byte, n)
	binary.BigEndian.PutUint16(out[:2], 11)
	at := 2
	for i, value := range values {
		name := piAuthHeaderNames[i]
		binary.BigEndian.PutUint16(out[at:at+2], uint16(len(name)))
		at += 2
		at += copy(out[at:], name)
		binary.BigEndian.PutUint32(out[at:at+4], uint32(len(value)))
		at += 4
		at += copy(out[at:], value)
	}
	return out, nil
}

func piHeaderValue(v string) bool {
	if len(v) == 0 || v[0] <= 32 || v[len(v)-1] <= 32 {
		return false
	}
	for i := range len(v) {
		if v[i] < 32 || v[i] > 126 {
			return false
		}
	}
	return true
}

func piAuthField(h hash.Hash, b []byte) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(b)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(b)
}

// All parameters here are already bounded private DATA. No transcript buffer
// or plaintext-key copy is retained; HMAC state dies before the use gate exits.
func piRequestMAC(key []byte, nonce, binding, profile [32]byte, endpoint string, headers, body []byte) [32]byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(piAuthDomain))
	piAuthField(h, nonce[:])
	piAuthField(h, binding[:])
	piAuthField(h, profile[:])
	piAuthField(h, []byte("POST"))
	piAuthField(h, []byte(endpoint))
	headersHash, bodyHash := sha256.Sum256(headers), sha256.Sum256(body)
	piAuthField(h, headersHash[:])
	piAuthField(h, bodyHash[:])
	var out [32]byte
	h.Sum(out[:0])
	return out
}

// compareRequestMAC is PRIVATE verification only, not an executable success
// witness. It cannot call Source or emit a Bearer header. The future concrete
// fixed HTTPS path must still bind/consume the original send exactly once and
// quarantine/join the entire response before forwarding any bytes.
func (s *preparedPiFileUse) compareRequestMAC(frame pisdkhttpwire.Frame) (err error) {
	if s == nil {
		return ErrInvalid
	}
	if !s.macAttempted.CompareAndSwap(false, true) {
		s.abort()
		return ErrConsumed
	}
	defer func() {
		if err != nil {
			s.abort()
		}
	}()
	if !s.mu.TryLock() {
		return ErrConsumed
	}
	defer s.mu.Unlock()
	if s.stopped.Load() || s.auth == nil || s.file == nil || s.file.ctx.Err() != nil || s.callback != 1 {
		return ErrConsumed
	}
	use := s.use.Load()
	if use == nil {
		return ErrConsumed
	}
	select {
	case <-use.gate:
	default:
		return ErrConsumed
	}
	defer func() { use.gate <- struct{}{} }()
	return s.compareRequestMACLocked(use, frame)
}

// The concrete file-owned network consumer retains BOTH s.mu and the genuine
// use.gate while calling this helper, Source, TLS, receive and quarantine.
// A successful comparison is never a transferable authorization witness.
func (s *preparedPiFileUse) compareRequestMACLocked(use *immutableAPIFileUse, frame pisdkhttpwire.Frame) error {
	f := s.file
	if !use.active.Load() || use.ctx.Err() != nil || use.prepared != f || use.binding != f.binding || use.files != f.files ||
		use.file == nil || !f.service.clock.Now().Before(use.handle.ExpiresAt) ||
		frame.Kind != pisdkhttpwire.FetchRequest || frame.Nonce != ([32]byte{}) || frame.Binding != ([32]byte{}) || frame.Profile != s.auth.profile ||
		frame.Digest != ([32]byte{}) || frame.Count != 0 || frame.Index != 0 || frame.Bytes != 0 ||
		len(frame.Headers) != len(s.auth.headers) || len(frame.Body) != len(s.body) ||
		!bytes.Equal(frame.Headers, s.auth.headers) || !bytes.Equal(frame.Body, s.body) || sha256.Sum256(s.body) != s.bodyHash {
		return ErrDeliveryMismatch
	}
	if len(use.secret) == 0 || int64(len(use.secret)) > f.maxBytes {
		return ErrDeliveryMismatch
	}
	for _, c := range use.secret {
		if c < 33 || c > 126 {
			return ErrDeliveryMismatch
		}
	}
	if domain.FingerprintCredential(use.secret) != use.fingerprint || f.files.VerifyImmutable(use.ctx, use.file, use.fingerprint, f.maxBytes) != nil {
		return ErrDeliveryMismatch
	}
	want := piRequestMAC(use.secret, s.nonce, s.auth.binding, s.auth.profile, s.auth.endpoint, frame.Headers, frame.Body)
	match := hmac.Equal(want[:], frame.MAC[:])
	clear(want[:])
	if !match || s.stopped.Load() || !use.active.Load() || use.ctx.Err() != nil || f.ctx.Err() != nil {
		return ErrDeliveryMismatch
	}
	return nil
}
