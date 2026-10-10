package computechoice

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"
)

const (
	MaxCursorBytes     = 512
	MaxCursorLifetime  = 60 * time.Second
	cursorPayloadBytes = 91
	cursorBytes        = cursorPayloadBytes + sha256.Size
	cursorVersion      = 1
)

var (
	ErrChoiceStale      = errors.New("compute choice stale")
	ErrInvalidCursorKey = errors.New("compute choice cursor key invalid")
)

// CursorCodec has no cache, clock or permission state. The 32-byte MAC key is
// copied at construction. Resource-time arguments are authoritative DB samples,
// not browser/process clocks. The current transaction supplies the shortest
// authority expiry at encode AND decode; a MAC never authenticates eligibility.
type CursorCodec struct {
	key        [sha256.Size]byte
	configured bool
}

func NewCursorCodec(key []byte) (*CursorCodec, error) {
	if len(key) != sha256.Size {
		return nil, ErrInvalidCursorKey
	}
	codec := &CursorCodec{configured: true}
	copy(codec.key[:], key)
	return codec, nil
}

// Encode emits only scope/revision hashes, an input enum, position and times.
// A continuation must follow an examined position with more candidates.
func (codec *CursorCodec) Encode(scope Scope, registry *ScopedRegistry, kind InputKind, lastPosition uint32, now, expiresAt, shortestAuthorityExpiry time.Time) (string, error) {
	if codec == nil || !codec.configured || !cursorContextValid(scope, registry, kind, lastPosition, now, shortestAuthorityExpiry) || !validCursorTime(expiresAt) || !expiresAt.After(now) || expiresAt.After(now.Add(MaxCursorLifetime)) || expiresAt.After(shortestAuthorityExpiry) {
		return "", ErrChoiceStale
	}
	var encoded [cursorBytes]byte
	encoded[0] = cursorVersion
	copy(encoded[1:33], registry.scopeDigest[:])
	copy(encoded[33:65], registry.revision[:])
	encoded[65] = kindCode(kind)
	encoded[66] = byte(lastPosition)
	putCursorTime(encoded[67:79], now)
	putCursorTime(encoded[79:91], expiresAt)
	copy(encoded[cursorPayloadBytes:], codec.mac(encoded[:cursorPayloadBytes]))
	return base64.RawURLEncoding.EncodeToString(encoded[:]), nil
}

// Decode rejects all malformed/foreign/stale inputs with one content-free
// error. The fixed-size encoding bounds allocation and rules out JSON/map
// parsing, extra fields, mixed versions and raw private authority identifiers.
func (codec *CursorCodec) Decode(token string, scope Scope, registry *ScopedRegistry, kind InputKind, now, shortestAuthorityExpiry time.Time) (uint32, error) {
	if codec == nil || !codec.configured || len(token) > MaxCursorBytes || len(token) != base64.RawURLEncoding.EncodedLen(cursorBytes) {
		return 0, ErrChoiceStale
	}
	var encoded [cursorBytes]byte
	n, err := base64.RawURLEncoding.Strict().Decode(encoded[:], []byte(token))
	if err != nil || n != cursorBytes || base64.RawURLEncoding.EncodeToString(encoded[:]) != token || encoded[0] != cursorVersion || !hmac.Equal(encoded[cursorPayloadBytes:], codec.mac(encoded[:cursorPayloadBytes])) {
		return 0, ErrChoiceStale
	}
	position := uint32(encoded[66])
	if !cursorContextValid(scope, registry, kind, position, now, shortestAuthorityExpiry) || encoded[65] != kindCode(kind) || !hmac.Equal(encoded[1:33], registry.scopeDigest[:]) || !hmac.Equal(encoded[33:65], registry.revision[:]) {
		return 0, ErrChoiceStale
	}
	issuedAt, validIssued := readCursorTime(encoded[67:79])
	expiresAt, validExpires := readCursorTime(encoded[79:91])
	if !validIssued || !validExpires || now.Before(issuedAt) || !expiresAt.After(issuedAt) || expiresAt.After(issuedAt.Add(MaxCursorLifetime)) || !now.Before(expiresAt) || expiresAt.After(shortestAuthorityExpiry) {
		return 0, ErrChoiceStale
	}
	return position, nil
}

func cursorContextValid(scope Scope, registry *ScopedRegistry, kind InputKind, position uint32, now, authorityExpiry time.Time) bool {
	return scope.Validate() == nil && registry != nil && scopeHash(scope) == registry.scopeDigest && kindCode(kind) != 0 && position > 0 && position < uint32(len(registry.registrations)) && validCursorTime(now) && validCursorTime(authorityExpiry) && authorityExpiry.After(now)
}

func validCursorTime(value time.Time) bool {
	return !value.IsZero() && value.Year() >= 1 && value.Year() <= 9999
}

func putCursorTime(destination []byte, value time.Time) {
	binary.BigEndian.PutUint64(destination[:8], uint64(value.Unix()))
	binary.BigEndian.PutUint32(destination[8:], uint32(value.Nanosecond()))
}

func readCursorTime(encoded []byte) (time.Time, bool) {
	nanoseconds := binary.BigEndian.Uint32(encoded[8:])
	if nanoseconds >= 1_000_000_000 {
		return time.Time{}, false
	}
	value := time.Unix(int64(binary.BigEndian.Uint64(encoded[:8])), int64(nanoseconds)).UTC()
	return value, validCursorTime(value)
}

func (codec *CursorCodec) mac(payload []byte) []byte {
	mac := hmac.New(sha256.New, codec.key[:])
	frame(mac, []byte("sessionless.compute-choice.cursor.v1"))
	frame(mac, payload)
	return mac.Sum(nil)
}

func kindCode(kind InputKind) byte {
	switch kind {
	case InputText:
		return 1
	case InputImage:
		return 2
	case InputFile:
		return 3
	case InputMixed:
		return 4
	default:
		return 0
	}
}
