package domain

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"
)

// OwnedSpendingListV1 contains only actual authorizations. The sealed profile
// selects exactly two or four ordered records; counters are not funding heads.
type OwnedSpendingListV1 []AttemptSpendingAuthorizationCurrentV1

// RegisteredSpendingListV1 carries the same bounded actual-head grammar by
// reference, without creating another source or spending authority.
type RegisteredSpendingListV1 []RegisteredSpendingReferenceV1

// Decode each occurrence into fixed scratch before allocating the typed list.
// In particular, a later duplicate key cannot hide an earlier oversized list.
func decodeOwnedSpendingList[T any](raw []byte) ([]T, error) {
	denied := func() ([]T, error) {
		return nil, admissionError("spending", "requires a non-null bounded list of at most four actual records")
	}
	if len(raw) == 0 || len(raw) > OwnedAgentCurrentMaxRecordBytesV1 || !utf8.Valid(raw) {
		return denied()
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return denied()
	}
	var scratch [4]T
	n := 0
	for d.More() {
		start := d.InputOffset()
		if n == len(scratch) || d.Decode(&scratch[n]) != nil {
			return denied()
		}
		if bytes.Equal(bytes.Trim(bytes.TrimSpace(raw[start:d.InputOffset()]), ", \t\r\n"), []byte("null")) {
			return denied()
		}
		n++
	}
	if token, err = d.Token(); err != nil || token != json.Delim(']') || n == 0 {
		return denied()
	}
	if _, err := d.Token(); err != io.EOF {
		return denied()
	}
	return append([]T(nil), scratch[:n]...), nil
}

func (v *OwnedSpendingListV1) UnmarshalJSON(raw []byte) error {
	if v == nil {
		return admissionError("spending", "requires an owned list receiver")
	}
	decoded, err := decodeOwnedSpendingList[AttemptSpendingAuthorizationCurrentV1](raw)
	if err == nil {
		*v = decoded
	}
	return err
}

func (v *RegisteredSpendingListV1) UnmarshalJSON(raw []byte) error {
	if v == nil {
		return admissionError("spending", "requires an owned reference-list receiver")
	}
	decoded, err := decodeOwnedSpendingList[RegisteredSpendingReferenceV1](raw)
	if err == nil {
		*v = decoded
	}
	return err
}

// OwnedSpendingComponentsV1 returns independent ordered shape DATA, never
// current funding permission. Unknown profiles have no component language.
func OwnedSpendingComponentsV1(profile AgentLoopProfileV1) []AttemptSpendingComponentKindV1 {
	switch profile {
	case AgentLoopOwnedPublicV1:
		return []AttemptSpendingComponentKindV1{AttemptSpendingProviderV1, AttemptSpendingSubstrateV1, AttemptSpendingMCPToolV1, AttemptSpendingWebSearchV1}
	case AgentLoopOwnedLocalTextV1:
		return []AttemptSpendingComponentKindV1{AttemptSpendingProviderV1, AttemptSpendingSubstrateV1}
	default:
		return nil
	}
}
