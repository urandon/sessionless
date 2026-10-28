package ydbstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// The still explicitly V1 writer must not silently project a V2 ownership
// tombstone into ExternalIdentity and reactivate it through ordinary login,
// bootstrap or Telegram ingress. This is a rejecting V1 reader, NOT a V2-to-V1
// fallback or the serving V2 cutover. V2 readers never call this decoder.
func decodeExternalIdentityV1(raw []byte) (domain.ExternalIdentity, error) {
	denied := func() (domain.ExternalIdentity, error) {
		return domain.ExternalIdentity{}, domain.ErrMembershipDenied
	}
	if len(raw) == 0 || len(raw) > domain.IdentityAuthMaxRecordBytesV2 || !utf8.Valid(raw) {
		return denied()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := externalIdentityObjectV1(decoder, false); err != nil {
		return denied()
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return denied()
	}
	var identity domain.ExternalIdentity
	decoder = json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&identity) != nil || identity.Validate() != nil {
		return denied()
	}
	return identity, nil
}

func externalIdentityObjectV1(decoder *json.Decoder, subject bool) error {
	fields := map[string]bool{"subject": true, "user_id": true, "created_at": true, "updated_at": true}
	if subject {
		fields = map[string]bool{"provider": true, "subject": true}
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return domain.ErrMembershipDenied
	}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		token, err := decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || !fields[name] || seen[name] {
			return domain.ErrMembershipDenied
		}
		seen[name] = true
		if !subject && name == "subject" {
			if err := externalIdentityObjectV1(decoder, true); err != nil {
				return err
			}
		} else {
			token, err := decoder.Token()
			value, ok := token.(string)
			if err != nil || !ok || value == "" {
				return domain.ErrMembershipDenied
			}
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || len(seen) != len(fields) {
		return domain.ErrMembershipDenied
	}
	return nil
}
