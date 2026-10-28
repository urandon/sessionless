package localoperator

import (
	"bytes"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

// DecodeProfileReceipt accepts the original Store's exact three-field DATA
// record. It authenticates no grant and grants no current execution authority.
func DecodeProfileReceipt(raw []byte) (ports.OwnedProfilePublicationReceiptV1, error) {
	deny := func() (ports.OwnedProfilePublicationReceiptV1, error) {
		return ports.OwnedProfilePublicationReceiptV1{}, ErrDenied
	}
	if _, err := strictjson.Parse(raw, domain.OwnedAgentCurrentMaxRecordBytesV1, 64, domain.OwnedAgentCurrentMaxRecordBytesV1); err != nil {
		return deny()
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 3 || fields["Profile"] == nil || fields["Digest"] == nil || fields["Replayed"] == nil {
		return deny()
	}
	profile, err := domain.DecodeOwnedAgentProfileCurrentV1(fields["Profile"])
	var digest string
	var replayed bool
	if err != nil || profile.ValidateInitial() != nil || json.Unmarshal(fields["Digest"], &digest) != nil ||
		(!bytes.Equal(fields["Replayed"], []byte("true")) && !bytes.Equal(fields["Replayed"], []byte("false"))) || json.Unmarshal(fields["Replayed"], &replayed) != nil {
		return deny()
	}
	want, err := profile.Digest()
	if err != nil || digest != want {
		return deny()
	}
	return ports.OwnedProfilePublicationReceiptV1{Profile: profile, Digest: digest, Replayed: replayed}, nil
}
