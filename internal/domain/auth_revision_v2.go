package domain

import (
	"errors"
	"math"
	"strconv"
)

// AuthRevisionV2 is a positive identity-derived revision. Its ONLY JSON form
// is a quoted canonical decimal, including in JsonDocument storage and DTOs.
// Existing membership security versions have a different, unchanged contract.
type AuthRevisionV2 uint64

var ErrAuthRevisionOverflowV2 = errors.New("identity security revision cannot advance")

func ParseAuthRevisionV2(value string) (AuthRevisionV2, error) {
	if len(value) == 0 || len(value) > 20 || value[0] < '1' || value[0] > '9' {
		return 0, invalidAuthRevisionV2()
	}
	for _, character := range []byte(value) {
		if character < '0' || character > '9' {
			return 0, invalidAuthRevisionV2()
		}
	}
	number, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, invalidAuthRevisionV2()
	}
	return AuthRevisionV2(number), nil
}

func (revision AuthRevisionV2) Validate() error {
	if revision == 0 {
		return invalidAuthRevisionV2()
	}
	return nil
}

func (revision AuthRevisionV2) String() string { return strconv.FormatUint(uint64(revision), 10) }

func (revision AuthRevisionV2) MarshalJSON() ([]byte, error) {
	if err := revision.Validate(); err != nil {
		return nil, err
	}
	return []byte(`"` + revision.String() + `"`), nil
}

func (revision *AuthRevisionV2) UnmarshalJSON(raw []byte) error {
	if revision == nil {
		return invalidAuthRevisionV2()
	}
	*revision = 0
	if len(raw) < 3 || len(raw) > 22 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return invalidAuthRevisionV2()
	}
	parsed, err := ParseAuthRevisionV2(string(raw[1 : len(raw)-1]))
	if err != nil {
		return err
	}
	*revision = parsed
	return nil
}

// NextAuthRevisionV2 advances the maximum of strictly positive input revisions.
// Callers omit an absent target; zero is never an alternate absence encoding.
func NextAuthRevisionV2(revisions ...AuthRevisionV2) (AuthRevisionV2, error) {
	if len(revisions) == 0 {
		return 0, invalidAuthRevisionV2()
	}
	var maximum AuthRevisionV2
	for _, revision := range revisions {
		if err := revision.Validate(); err != nil {
			return 0, err
		}
		if revision > maximum {
			maximum = revision
		}
	}
	if maximum == AuthRevisionV2(math.MaxUint64) {
		return 0, ErrAuthRevisionOverflowV2
	}
	return maximum + 1, nil
}

func invalidAuthRevisionV2() error {
	return ValidationError{Field: "auth_revision_v2", Reason: "must be a canonical positive uint64 decimal string"}
}
