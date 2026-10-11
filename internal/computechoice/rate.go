package computechoice

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

var ErrInvalidListRateState = errors.New("compute choice list rate state invalid")

// ListRateIdentityKeys addresses a separate finite compute-choice table. Its
// identity domain is distinct from Run explanation and CHAT-1; finite slots can
// still collide. Tenant/user MUST come from current transactional authorization,
// not HTTP selectors. Shape validation here does not grant access.
func ListRateIdentityKeys(tenantID domain.TenantID, userID domain.UserID) (string, [runexplanationrate.ProbeCount]uint32, error) {
	var keys [runexplanationrate.ProbeCount]uint32
	if tenantID.Validate() != nil || userID.Validate() != nil {
		return "", keys, ErrInvalidListRateState
	}
	digest := sha256.New()
	frame(digest, []byte("sessionless.compute-choice-rate.identity.v1"))
	frame(digest, []byte(tenantID))
	frame(digest, []byte(userID))
	encoded := digest.Sum(nil)
	// This is the established finite addressing algorithm, not a second
	// limiter: Evaluate below delegates all GCRA/receipt/row semantics.
	base := uint32(binary.BigEndian.Uint64(encoded[:8]) % runexplanationrate.MaxSlots)
	for i := range keys {
		keys[i] = (base + uint32(i)) % runexplanationrate.MaxSlots
	}
	return hex.EncodeToString(encoded), keys, nil
}

// ListRateSelectorDigest seals current scope (including membership security
// version) and normalized exact query. Cookies and sampled clocks are not part
// of retry identity. This digest grants neither access nor resource eligibility.
func ListRateSelectorDigest(scope Scope, query ListQuery) (string, error) {
	if scope.Validate() != nil {
		return "", ErrInvalidListRateState
	}
	queryDigest, err := query.Digest(scope.SessionID)
	if err != nil {
		return "", ErrInvalidListRateState
	}
	scoped := scopeHash(scope)
	digest := sha256.New()
	frame(digest, []byte("sessionless.compute-choice-rate.selector.v1"))
	frame(digest, scoped[:])
	frame(digest, []byte(queryDigest))
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// EvaluateListRate binds the established pure limiter to compute-choice list
// context without forking its algorithm or types. The same-transaction caller
// must freshly authorize every attempt/replay, supply authoritative UTC DB time,
// read exactly these keys from compute_choice_rate_slots_v1 and atomically commit
// any detached debit with its resource read. A decision alone is not permission
// or confirmed commit; unknown commit must expose no speculative list/refund.
func EvaluateListRate(now time.Time, scope Scope, query ListQuery, slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot, requestID string) (runexplanationrate.Decision, error) {
	identity, keys, err := ListRateIdentityKeys(scope.TenantID, scope.UserID)
	if err != nil {
		return runexplanationrate.Decision{}, ErrInvalidListRateState
	}
	selector, err := ListRateSelectorDigest(scope, query)
	if err != nil {
		return runexplanationrate.Decision{}, ErrInvalidListRateState
	}
	decision, err := runexplanationrate.Evaluate(now, identity, keys, slots, requestID, selector)
	if err != nil {
		return runexplanationrate.Decision{}, ErrInvalidListRateState
	}
	return decision, nil
}
