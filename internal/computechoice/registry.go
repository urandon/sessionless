// Package computechoice indexes private, reviewed registration metadata. It
// neither establishes eligibility nor grants use, consent or execution rights.
package computechoice

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash"
	"sort"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	MaxRegistrations = 64
	MaxPagePositions = 4
	MaxChoiceIDBytes = 128
)

var (
	ErrInvalidRegistry                    = errors.New("compute choice registry invalid")
	ErrInvalidScope                       = errors.New("compute choice scope invalid")
	ErrInvalidPage                        = errors.New("compute choice page invalid")
	ErrChoiceUnavailable                  = errors.New("compute choice unavailable")
	ErrPrivateRegistrationNotSerializable = errors.New("compute choice private metadata is not serializable")
)

type InputKind string

const (
	InputText  InputKind = "text"
	InputImage InputKind = "image"
	InputFile  InputKind = "file"
	InputMixed InputKind = "mixed"
)

func (kind InputKind) Validate() error {
	if kindCode(kind) == 0 {
		return ErrChoiceStale
	}
	return nil
}

// Scope MUST be derived after current cookie, WRITE membership and Session
// participation checks in the resource transaction. Shape validation here is
// not authentication. HTTP selectors cannot supply this authority projection.
type Scope struct {
	TenantID                  domain.TenantID
	UserID                    domain.UserID
	SessionID                 domain.SessionID
	MembershipSecurityVersion uint64
}

func (scope Scope) Validate() error {
	if scope.TenantID.Validate() != nil || scope.UserID.Validate() != nil || scope.SessionID.Validate() != nil || scope.MembershipSecurityVersion == 0 {
		return ErrInvalidScope
	}
	return nil
}

func (Scope) MarshalJSON() ([]byte, error) { return nil, ErrPrivateRegistrationNotSerializable }

// Registration contains only enumeration metadata for an already reviewed,
// disclosable owner tuple. It intentionally has no resource/binding/grant data,
// enabled/eligible flag, or method that can reserve work. Trusted installation
// owns changes to this index; a database cutover/current reader remains required.
type Registration struct {
	TenantID             domain.TenantID
	OwnerUserID          domain.UserID
	ChoiceID             string
	RegistrationRevision uint64
	DisclosureRevision   string
}

func (Registration) MarshalJSON() ([]byte, error) { return nil, ErrPrivateRegistrationNotSerializable }

type ownerScope struct {
	tenant domain.TenantID
	owner  domain.UserID
}

type Registry struct{ owners map[ownerScope][]Registration }

// NewRegistry rejects global overflow rather than truncating. Subsets are
// sorted before any scope's positions or revision exist; foreign entries never
// influence that order. All input data is copied, with no runtime global scan.
func NewRegistry(registrations []Registration) (*Registry, error) {
	if len(registrations) > MaxRegistrations {
		return nil, ErrInvalidRegistry
	}
	registry := &Registry{owners: make(map[ownerScope][]Registration)}
	seen := make(map[ownerScope]map[string]bool)
	for _, registration := range registrations {
		if registration.TenantID.Validate() != nil || registration.OwnerUserID.Validate() != nil || registration.RegistrationRevision == 0 || !validChoiceID(registration.ChoiceID) || !validRevision(registration.DisclosureRevision) {
			return nil, ErrInvalidRegistry
		}
		key := ownerScope{registration.TenantID, registration.OwnerUserID}
		if seen[key] == nil {
			seen[key] = make(map[string]bool)
		}
		if seen[key][registration.ChoiceID] {
			return nil, ErrInvalidRegistry
		}
		seen[key][registration.ChoiceID] = true
		registry.owners[key] = append(registry.owners[key], registration)
	}
	for key := range registry.owners {
		sort.Slice(registry.owners[key], func(i, j int) bool { return registry.owners[key][i].ChoiceID < registry.owners[key][j].ChoiceID })
	}
	return registry, nil
}

// ScopedRegistry is a defensive immutable snapshot of one authenticated owner
// subset. It contains no foreign registration/count/global manifest digest.
type ScopedRegistry struct {
	scopeDigest   [sha256.Size]byte
	revision      [sha256.Size]byte
	registrations []Registration
}

func (registry *Registry) Scoped(scope Scope) (*ScopedRegistry, error) {
	if registry == nil || scope.Validate() != nil {
		return nil, ErrInvalidScope
	}
	entries := append([]Registration(nil), registry.owners[ownerScope{scope.TenantID, scope.UserID}]...)
	result := &ScopedRegistry{scopeDigest: scopeHash(scope), registrations: entries}
	digest := sha256.New()
	frame(digest, []byte("sessionless.compute-choice.scoped-registry.v1"))
	frame(digest, result.scopeDigest[:])
	frameUint(digest, uint64(len(entries)))
	for _, entry := range entries {
		frame(digest, []byte(entry.ChoiceID))
		frameUint(digest, entry.RegistrationRevision)
		frame(digest, []byte(entry.DisclosureRevision))
	}
	copy(result.revision[:], digest.Sum(nil))
	return result, nil
}

func (registry *ScopedRegistry) Revision() string {
	if registry == nil {
		return ""
	}
	return hex.EncodeToString(registry.revision[:])
}

func (registry *ScopedRegistry) Lookup(choiceID string) (Registration, error) {
	if registry == nil || !validChoiceID(choiceID) {
		return Registration{}, ErrChoiceUnavailable
	}
	index := sort.Search(len(registry.registrations), func(i int) bool { return registry.registrations[i].ChoiceID >= choiceID })
	if index == len(registry.registrations) || registry.registrations[index].ChoiceID != choiceID {
		return Registration{}, ErrChoiceUnavailable
	}
	return registry.registrations[index], nil
}

type CandidatePage struct {
	Registrations []Registration
	LastPosition  uint32
	More          bool
}

// Page examines exactly the next bounded subset positions, not enough entries
// to fill an eligible-result page. Callers must evaluate each returned candidate
// in their transaction; filtering cannot make this method scan further.
// Positions are one-based; afterPosition=0 starts before the first position.
func (registry *ScopedRegistry) Page(afterPosition uint32, limit uint32) (CandidatePage, error) {
	if registry == nil || limit < 1 || limit > MaxPagePositions || afterPosition > uint32(len(registry.registrations)) {
		return CandidatePage{}, ErrInvalidPage
	}
	end := afterPosition + limit
	if end > uint32(len(registry.registrations)) {
		end = uint32(len(registry.registrations))
	}
	return CandidatePage{Registrations: append([]Registration(nil), registry.registrations[afterPosition:end]...), LastPosition: end, More: end < uint32(len(registry.registrations))}, nil
}

func scopeHash(scope Scope) [sha256.Size]byte {
	digest := sha256.New()
	frame(digest, []byte("sessionless.compute-choice.scope.v1"))
	for _, value := range []string{string(scope.TenantID), string(scope.UserID), string(scope.SessionID)} {
		frame(digest, []byte(value))
	}
	frameUint(digest, scope.MembershipSecurityVersion)
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func frame(digest hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write(value)
}

func frameUint(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	frame(digest, encoded[:])
}

func validChoiceID(value string) bool {
	return len(value) <= MaxChoiceIDBytes && domain.ValidateOpaqueID("choice_id", value) == nil
}

func validRevision(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, digit := range []byte(value) {
		if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f') {
			return false
		}
	}
	return true
}
