package computechoice

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const MaxListQueryBytes = 1024

var ErrInvalidQuery = errors.New("compute choice query invalid")

// ListQuery contains only bounded enumeration hints, never identity, resource
// authority or consent. HEAD/body handling belongs to the HTTP adapter.
type ListQuery struct {
	Limit     uint32
	InputKind InputKind
	Cursor    string
}

// ParseListQuery accepts only the three closed query keys, once each. URL
// escaping is decoded before duplicate detection; equivalent encodings have
// the same normalized query identity. It performs no resource resolution.
func ParseListQuery(raw string) (ListQuery, error) {
	invalid := ListQuery{}
	if len(raw) > MaxListQueryBytes || strings.Contains(raw, ";") {
		return invalid, ErrInvalidQuery
	}
	query := ListQuery{Limit: MaxPagePositions, InputKind: InputText}
	if raw == "" {
		return query, nil
	}
	var seen uint8
	for _, pair := range strings.Split(raw, "&") {
		keyRaw, valueRaw, present := strings.Cut(pair, "=")
		if !present {
			return invalid, ErrInvalidQuery
		}
		key, keyErr := url.QueryUnescape(keyRaw)
		value, valueErr := url.QueryUnescape(valueRaw)
		if keyErr != nil || valueErr != nil || value == "" {
			return invalid, ErrInvalidQuery
		}
		var bit uint8
		switch key {
		case "limit":
			bit = 1
			if len(value) != 1 || value[0] < '1' || value[0] > '4' {
				return invalid, ErrInvalidQuery
			}
			query.Limit = uint32(value[0] - '0')
		case "input_kind":
			bit = 2
			query.InputKind = InputKind(value)
		case "cursor":
			bit = 4
			query.Cursor = value
		default:
			return invalid, ErrInvalidQuery
		}
		if seen&bit != 0 {
			return invalid, ErrInvalidQuery
		}
		seen |= bit
	}
	if query.Validate() != nil {
		return invalid, ErrInvalidQuery
	}
	return query, nil
}

func (query ListQuery) Validate() error {
	if query.Limit == 0 || query.Limit > MaxPagePositions || kindCode(query.InputKind) == 0 || (query.Cursor != "" && !validListCursorSyntax(query.Cursor)) {
		return ErrInvalidQuery
	}
	return nil
}

// Digest binds an already validated query to its canonical Session for finite
// rate-debit receipts. The caller must independently authorize that Session.
// Defaults and query-key order normalize; the exact cursor bytes remain bound.
func (query ListQuery) Digest(sessionID domain.SessionID) (string, error) {
	if query.Validate() != nil || sessionID.Validate() != nil {
		return "", ErrInvalidQuery
	}
	digest := sha256.New()
	frame(digest, []byte("sessionless.compute-choice.list-query.v1"))
	frame(digest, []byte(sessionID))
	frameUint(digest, uint64(query.Limit))
	frame(digest, []byte(query.InputKind))
	frame(digest, []byte(query.Cursor))
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// Syntax is deliberately weaker than CursorCodec.Decode: a shaped but tampered,
// foreign or expired token must reach authenticated stale handling, not 400.
// Only the fixed encoding, closed fields and structural time window are checked.
func validListCursorSyntax(token string) bool {
	if len(token) != base64.RawURLEncoding.EncodedLen(cursorBytes) {
		return false
	}
	var encoded [cursorBytes]byte
	n, err := base64.RawURLEncoding.Strict().Decode(encoded[:], []byte(token))
	if err != nil || n != cursorBytes || base64.RawURLEncoding.EncodeToString(encoded[:]) != token || encoded[0] != cursorVersion || encoded[65] < 1 || encoded[65] > 4 || encoded[66] == 0 || encoded[66] >= MaxRegistrations {
		return false
	}
	issuedAt, validIssued := readCursorTime(encoded[67:79])
	expiresAt, validExpires := readCursorTime(encoded[79:91])
	return validIssued && validExpires && expiresAt.After(issuedAt) && !expiresAt.After(issuedAt.Add(MaxCursorLifetime))
}
