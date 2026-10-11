package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func ownedConsentTestHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func ownedConsentCodecFixture(t *testing.T) (domain.OwnedAttemptEffectParentV1, ownedConsentStates, time.Time) {
	t.Helper()
	f := newOwnedAttemptEffectParentFixture(t)
	parent := f.parent
	scope := ownedConsentScope(parent)
	body := []byte(`{"query":"public compiler documentation","max_results":5}`)
	at := parent.Reservation.ReservedAt
	expires := at.Add(time.Second)
	if parent.Authority.InvocationDeadline.Before(expires) {
		expires = parent.Authority.InvocationDeadline
	}
	r := domain.WebSearchConsentRequestV1{
		Version: 1, ID: "consent-codec", Scope: scope, InitiatingUserID: parent.Scope.OwnerUserID,
		ModelResponseDigest: ownedConsentTestHash([]byte("model-response")), CallID: "search-codec-call",
		ArgumentsDigest: ownedConsentTestHash(body), QueryDigest: ownedConsentTestHash([]byte("public compiler documentation")),
		GrantDigest:     parent.Authority.HarnessBinding.AgentLoopPolicy.WebSearchGrantDigest,
		ConsentRevision: 1, RevocationGeneration: 1,
		Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent/codec.json", Size: int64(len(body)), SHA256: ownedConsentTestHash(body)},
		CreatedAt: at, ExpiresAt: expires,
	}
	pd, err := parent.Digest()
	if err != nil || r.ValidateArguments(body) != nil {
		t.Fatalf("valid immutable codec fixture: parent=%v request=%v", err, r.ValidateArguments(body))
	}
	v := ownedConsentStates{Version: 1, ParentDigest: pd, States: []domain.WebSearchConsentStateV1{{Request: r, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: at}}}
	if err := v.validate(parent, at); err != nil {
		t.Fatalf("valid bounded structural fixture: %v", err)
	}
	return parent, v, at
}

func TestOwnedConsentCodecCanonicalParentCorrelation(t *testing.T) {
	parent, value, at := ownedConsentCodecFixture(t)
	raw, digest, err := ownedConsentBytes(value, parent, at)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeOwnedConsentStates(string(raw), digest, parent, at)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, actual, err := ownedConsentBytes(decoded, parent, at)
	if err != nil || !bytes.Equal(raw, reencoded) || actual != digest {
		t.Fatal("canonical immutable roundtrip changed exact request projection")
	}
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"whitespace", append(bytes.Clone(raw), ' ')},
		{"duplicate-key", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"unknown-field", bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"approved":true`), 1)},
		{"oversized", []byte(strings.Repeat(" ", ownedAuthorityHeadBytes+1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeOwnedConsentStates(string(tc.body), ownedConsentTestHash(tc.body), parent, at); err == nil {
				t.Fatal("even matching caller hash must not admit noncanonical/oversized structural body")
			}
		})
	}
	foreign := parent.Clone()
	foreign.Reservation.PhysicalInvocationClaimID = "foreign-claim"
	if _, err := decodeOwnedConsentStates(string(raw), digest, foreign, at); err == nil {
		t.Fatal("original structural body must not transfer to a foreign physical claim")
	}
}

func TestOwnedConsentCodecCapsAndOriginalTime(t *testing.T) {
	parent, initial, at := ownedConsentCodecFixture(t)
	for _, tc := range []struct {
		name   string
		change func(*ownedConsentStates)
	}{
		{"zero-version", func(v *ownedConsentStates) { v.Version = 0 }},
		{"wrong-parent", func(v *ownedConsentStates) { v.ParentDigest = strings.Repeat("a", 64) }},
		{"empty", func(v *ownedConsentStates) { v.States = nil }},
		{"three-total", func(v *ownedConsentStates) { v.States = append(v.States, v.States[0], v.States[0]) }},
		{"two-pending", func(v *ownedConsentStates) { v.States = append(v.States, v.States[0]) }},
		{"other-owner", func(v *ownedConsentStates) { v.States[0].Request.Scope.OwnerUserID = "foreign-owner" }},
		{"other-session", func(v *ownedConsentStates) { v.States[0].Request.Scope.SessionID = "foreign-session" }},
		{"other-claim", func(v *ownedConsentStates) { v.States[0].Request.Scope.PhysicalInvocationClaimID = "foreign-claim" }},
		{"future-observation", func(v *ownedConsentStates) { v.States[0].UpdatedAt = at.Add(time.Microsecond) }},
		{"before-parent", func(v *ownedConsentStates) {
			v.States[0].Request.CreatedAt = at.Add(-time.Microsecond)
			v.States[0].UpdatedAt = at.Add(-time.Microsecond)
		}},
		{"extended-parent-window", func(v *ownedConsentStates) {
			v.States[0].Request.ExpiresAt = parent.Authority.InvocationDeadline.Add(time.Microsecond)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(initial)
			var value ownedConsentStates
			if err := json.Unmarshal(body, &value); err != nil {
				t.Fatal(err)
			}
			tc.change(&value)
			if _, _, err := ownedConsentBytes(value, parent, at); err == nil {
				t.Fatal("invalid bounded structural projection admitted")
			}
		})
	}
}

func TestOwnedConsentStorageDebitAndImmutableCAS(t *testing.T) {
	parent, initial, at := ownedConsentCodecFixture(t)
	accounting := ownedAttemptAccounting{Version: 1, ParentDigest: initial.ParentDigest, SetupReservedAt: at, UpdatedAt: at}
	created, charged, fresh, err := appendOwnedConsentState(nil, accounting, parent, initial.States[0].Request, at)
	if err != nil || !fresh || charged.Aggregate.BodyBytes != ownedConsentMetadataReservationBytes+uint64(initial.States[0].Request.Arguments.Size) || charged.Aggregate.Operations != 0 || charged.Aggregate.AmountMicrounits != 0 || charged.Amounts != accounting.Amounts {
		t.Fatalf("bounded nonmonetary consent storage debit: fresh=%t err=%v body=%d", fresh, err, charged.Aggregate.BodyBytes)
	}
	replay, unchanged, fresh, err := appendOwnedConsentState(&created, charged, parent, initial.States[0].Request, at)
	if err != nil || fresh || unchanged != charged {
		t.Fatalf("exact original request must not redebit: fresh=%t err=%v", fresh, err)
	}
	replay.States[0].Request.CallID = "changed-returned-copy"
	if created.States[0].Request.CallID == replay.States[0].Request.CallID {
		t.Fatal("returned replay aliases original state")
	}
	wrong := initial.States[0].Request
	wrong.CallID = "replacement-intent"
	if _, _, _, err := appendOwnedConsentState(&created, charged, parent, wrong, at); err == nil {
		t.Fatal("same request ID must not replace immutable intent")
	}
	tooFull := charged
	tooFull.Aggregate.BodyBytes = parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxAggregateBodyBytes
	if _, _, _, err := appendOwnedConsentState(nil, tooFull, parent, initial.States[0].Request, at); err == nil {
		t.Fatal("consent must not escape existing aggregate body ceiling")
	}
	request := created.States[0].Request
	rd, _ := request.Digest()
	next := created.States[0].Clone()
	next.Status, next.Revision = domain.WebSearchConsentApprovedV1, 2
	next.Decision = &domain.WebSearchConsentDecisionV1{Version: 1, Request: request, RequestDigest: rd, RequestRevision: 1, ActorUserID: request.InitiatingUserID, WebSessionDigest: domain.SecretDigest(strings.Repeat("b", 64)), MembershipSecurityVersion: 1, Choice: domain.WebSearchConsentApproveV1, ObservedAt: at}
	approved, err := replaceOwnedConsentState(created, next, parent, at)
	if err != nil {
		t.Fatalf("structural fixture receipt append: %v", err)
	}
	stopped := approved.States[0].Clone()
	stopped.Status, stopped.Revision, stopped.UpdatedAt = domain.WebSearchConsentCancelledV1, 3, at.Add(time.Microsecond)
	retained, err := replaceOwnedConsentState(approved, stopped, parent, at.Add(time.Microsecond))
	if err != nil || retained.States[0].Decision == nil {
		t.Fatalf("stop must retain original receipt: %v", err)
	}
	changed := approved.States[0].Clone()
	changed.Decision.WebSessionDigest = domain.SecretDigest(strings.Repeat("c", 64))
	if _, err := replaceOwnedConsentState(approved, changed, parent, at); err == nil {
		t.Fatal("approved receipt cannot be replaced by another session")
	}
	if _, err := replaceOwnedConsentState(retained, approved.States[0], parent, at.Add(time.Microsecond)); err == nil {
		t.Fatal("stopped consent cannot reopen approval")
	}
}
