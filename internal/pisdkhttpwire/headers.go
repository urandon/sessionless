package pisdkhttpwire

import (
	"bytes"
	"encoding/binary"
	"strings"
)

var requestNames = [...]string{"accept", "content-type", "user-agent", "x-stainless-arch", "x-stainless-lang", "x-stainless-os", "x-stainless-package-version", "x-stainless-retry-count", "x-stainless-runtime", "x-stainless-runtime-version", "x-stainless-timeout"}

func headerName(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0 {
			continue
		}
		return false
	}
	return true
}

func headerValue(b []byte) bool {
	if len(b) == 0 || b[0] <= 32 || b[len(b)-1] <= 32 {
		return false
	}
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}

// ValidateHeaders scans a canonical block without constructing a map, strings
// or retained fields. Request=true checks the exact eleven nonsecret names;
// their pinned VALUES and response-header admission are separate owner checks.
// Internal ASCII spaces are legal; leading/trailing whitespace is not.
func ValidateHeaders(b []byte, maximum uint64, request bool) error {
	if len(b) < 2 || uint64(len(b)) > maximum {
		return ErrInvalid
	}
	n := int(binary.BigEndian.Uint16(b[:2]))
	if request && n != len(requestNames) || n > (len(b)-2)/8 {
		return ErrInvalid
	}
	at := 2
	var previous []byte
	for i := 0; i < n; i++ {
		if len(b)-at < 2 {
			return ErrInvalid
		}
		size := int(binary.BigEndian.Uint16(b[at : at+2]))
		at += 2
		if size == 0 || size > len(b)-at {
			return ErrInvalid
		}
		name := b[at : at+size]
		at += size
		if !headerName(name) || i > 0 && bytes.Compare(previous, name) >= 0 || request && string(name) != requestNames[i] {
			return ErrInvalid
		}
		previous = name
		if len(b)-at < 4 {
			return ErrInvalid
		}
		valueSize := uint64(binary.BigEndian.Uint32(b[at : at+4]))
		at += 4
		if valueSize > uint64(len(b)-at) {
			return ErrInvalid
		}
		value := b[at : at+int(valueSize)]
		at += int(valueSize)
		if !headerValue(value) {
			return ErrInvalid
		}
	}
	if at != len(b) {
		return ErrInvalid
	}
	return nil
}
