//go:build ydbintegration

package ydbstore

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
)

type taggedProducerClock struct{ at time.Time }

func (c taggedProducerClock) Now() time.Time { return c.at }

type taggedProducerFixture struct {
	allocation domain.RegisteredAllocationV2
	source     domain.AttemptExecutionSourceV1
	parent     domain.OwnedAttemptEffectParentV1
	operation  domain.AttemptOperationV1
	proof      ports.OwnedProxyPublicationV2
	peer       *serverlessegress.OwnedHTTPSFixtureV1
	at         time.Time
	cancel     context.CancelFunc
}

// Actual retained TLS and its authenticator are exercised against scripted
// allocation SQL and an explicitly synthetic source. This is not native YDB,
// model/source approval, local connector qualification, or an HTTP invocation.
func newTaggedProducerFixture(t *testing.T) taggedProducerFixture {
	t.Helper()
	// Sample once so the real TLS client can validate the fixture certificate.
	// All authority windows below are derived from this frozen instant before
	// their first seal; no historical receipt is rewritten or renewed.
	at := time.Now().UTC().Truncate(time.Microsecond)
	f := newOwnedAllocationFixture(t, ownedFinalPrebuild{at: at, profile: func(profile *domain.OwnedAgentProfileCurrentV1) {
		policy, err := serverlessegress.OwnedHTTPSFixturePolicyV1(*profile)
		if err != nil {
			t.Fatalf("derive fixture policy before source sealing: %v", err)
		}
		profile.Substrate.EgressPolicyDigest, err = policy.Digest()
		if err != nil {
			t.Fatal(err)
		}
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	receipt, err := f.parent.store.publishBoundedOwnedAllocation(ctx, f.publication)
	if err != nil {
		t.Fatalf("scripted allocation publication prerequisite: %v", err)
	}
	parent, err := domain.DecodeOwnedAttemptEffectParentV1([]byte(f.parent.sql.parentRow[13].(string)))
	if err != nil {
		t.Fatal(err)
	}
	profile := f.parent.base.reader.profile
	peer, err := serverlessegress.NewOwnedHTTPSFixtureV1(ctx, profile, taggedProducerClock{at}, []byte(`{"version":1,"kind":"model_response","text":"unused","calls":[]}`))
	if err != nil {
		t.Fatalf("create retained TLS fixture: %v", err)
	}
	t.Cleanup(peer.Close)
	policy, err := serverlessegress.OwnedHTTPSFixturePolicyV1(profile)
	if err != nil {
		t.Fatal(err)
	}
	preflight, err := peer.Proxy().Preflight(ctx, policy)
	if err != nil {
		t.Fatalf("actual TLS preflight with frozen source clock: %v", err)
	}
	deadline := at.Add(10 * time.Second)
	for _, expiry := range []time.Time{preflight.ExpiresAt, receipt.Original.Window.ExpiresAt, parent.Authority.InvocationDeadline} {
		if expiry.Before(deadline) {
			deadline = expiry
		}
	}
	request := []byte(`{"kind":"model","input":"fixture"}`)
	cost := parent.Authority.AdmissionCostCeiling
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: ownedOperationScope(parent), Sequence: 1, Kind: domain.AttemptOperationModelV1,
		CallID: "tagged-producer-model", RequestRef: "tagged-producer-request", RequestDigest: ports.OwnedFixtureBytesDigestV1(request),
		Deadline: deadline, CleanupDuration: parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration,
		RequestBytes: uint64(len(request)), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(request)) + 512, ResponseBytes: 512, AmountMicrounits: *f.parent.source.Spending[0].MaxAmountMicrounits, ActiveDuration: 10 * time.Second}}
	if err := op.ValidateForParent(parent.Authority, parent.Reservation); err != nil {
		t.Fatalf("frozen original operation prerequisite: %v", err)
	}
	proof, err := peer.Proxy().ObserveOwnedProxy(receipt.Original, f.parent.source, op)
	if err != nil {
		t.Fatalf("actual retained TLS producer observation: %v", err)
	}
	return taggedProducerFixture{receipt.Original, f.parent.source, parent, op, proof, peer, at, cancel}
}

func (f taggedProducerFixture) requireNoHTTP(t *testing.T) {
	t.Helper()
	if got := f.peer.Requests(); got != 0 || len(f.peer.RequestBody()) != 0 {
		t.Errorf("observation/authentication sent HTTP payload: requests=%d", got)
	}
}

func TestOwnedProxyTaggedProducerBindsOriginalOperationAndSource(t *testing.T) {
	f := newTaggedProducerFixture(t)
	defer f.requireNoHTTP(t)
	p := f.peer.Proxy()
	if err := p.VerifyOwnedProxyPublication(f.proof, f.at); err != nil {
		t.Fatalf("original historical authentication: %v", err)
	}
	if err := p.VerifyOwnedProxyPublicationCurrent(f.proof, f.at); err != nil {
		t.Fatalf("original retained-connection authentication: %v", err)
	}
	od, err := f.operation.Digest()
	if err != nil || f.proof.TransportKind != domain.RegisteredProxyTransportPinnedHTTPSV2 || f.proof.OperationDigest != od || !f.proof.OperationDeadline.Equal(f.operation.Deadline) {
		t.Fatal("actual producer did not seal explicit transport, whole operation and original deadline")
	}
	var missing serverlessegress.PinnedHTTPSProxyV1
	if _, err := missing.ObserveOwnedProxy(f.allocation, f.source, f.operation); err == nil {
		t.Error("missing concrete producer issued a proof")
	}
	for _, tc := range []struct {
		name   string
		change func(*domain.AttemptOperationV1)
	}{
		{"same-sequence-body-changed-debit", func(op *domain.AttemptOperationV1) { op.Debit.InputTokens++ }},
		{"same-sequence-body-changed-call", func(op *domain.AttemptOperationV1) { op.CallID = "other-model-call" }},
		{"same-sequence-body-changed-deadline", func(op *domain.AttemptOperationV1) { op.Deadline = op.Deadline.Add(-time.Microsecond) }},
		{"other-sequence", func(op *domain.AttemptOperationV1) {
			op.Sequence = 2
			op.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(strings.Repeat("d", 64))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := f.operation.Clone()
			tc.change(&op)
			if err := op.ValidateForParent(f.parent.Authority, f.parent.Reservation); err != nil {
				t.Fatalf("substitution must remain structurally valid: %v", err)
			}
			changedDigest, _ := op.Digest()
			if changedDigest == od || op.RequestDigest != f.operation.RequestDigest {
				t.Fatal("control must change whole operation but retain original request body")
			}
			if _, err := p.ObserveOwnedProxy(f.allocation, f.source, op); err == nil {
				t.Error("retained producer rebound to a different frozen operation")
			}
		})
	}
	source := f.source.Clone()
	source.IssuedAt = source.IssuedAt.Add(time.Microsecond)
	if err := source.Validate(); err != nil {
		t.Fatalf("changed source control must remain structurally valid: %v", err)
	}
	if _, err := p.ObserveOwnedProxy(f.allocation, source, f.operation); err == nil {
		t.Error("retained producer accepted a different original source")
	}
	repeated, err := p.ObserveOwnedProxy(f.allocation, f.source, f.operation.Clone())
	if err != nil || !reflect.DeepEqual(repeated, f.proof) {
		t.Fatalf("rejected substitutions damaged exact original observation: %v", err)
	}
}

func TestOwnedProxyTaggedProducerRejectsChangedCarrierAndDeadline(t *testing.T) {
	f := newTaggedProducerFixture(t)
	defer f.requireNoHTTP(t)
	p := f.peer.Proxy()
	originalIntent, err := f.proof.IntentBytes()
	if err != nil {
		t.Fatalf("original signed intent: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*ports.OwnedProxyPublicationV2)
	}{
		{"operation", func(v *ports.OwnedProxyPublicationV2) {
			v.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("e", 64))
		}},
		{"source", func(v *ports.OwnedProxyPublicationV2) { v.SourceDigest = strings.Repeat("e", 64) }},
		{"sequence", func(v *ports.OwnedProxyPublicationV2) { v.OperationSequence++ }},
		{"deadline", func(v *ports.OwnedProxyPublicationV2) {
			v.OperationDeadline = v.OperationDeadline.Add(-time.Microsecond)
		}},
		{"transport-tag", func(v *ports.OwnedProxyPublicationV2) {
			v.TransportKind = domain.RegisteredProxyTransportLocalLoopbackHTTPV2
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := f.proof.Clone()
			tc.change(&v)
			if tc.name != "transport-tag" && v.Validate() != nil {
				t.Fatal("changed signed-carrier control must remain structurally valid")
			}
			if tc.name != "transport-tag" {
				changedIntent, err := v.IntentBytes()
				if err != nil || bytes.Equal(changedIntent, originalIntent) {
					t.Fatalf("changed carrier field was absent from signed intent: %v", err)
				}
			}
			if p.VerifyOwnedProxyPublication(v, f.at) == nil || p.VerifyOwnedProxyPublicationCurrent(v, f.at) == nil {
				t.Error("changed original carrier retained producer authentication")
			}
		})
	}
	if err := p.VerifyOwnedProxyPublicationCurrent(f.proof, f.operation.Deadline.Add(-time.Microsecond)); err != nil {
		t.Fatalf("original retained connection immediately before operation deadline: %v", err)
	}
	if err := p.VerifyOwnedProxyPublicationCurrent(f.proof, f.operation.Deadline); err == nil {
		t.Error("current publication survived exact original operation deadline")
	}
	if err := p.VerifyOwnedProxyPublication(f.proof, f.operation.Deadline); err != nil {
		t.Fatalf("deadline erased historical authenticity: %v", err)
	}
}

func TestOwnedProxyTaggedProducerCloseAndCancelRetainOnlyHistory(t *testing.T) {
	for _, action := range []string{"close", "cancel"} {
		t.Run(action, func(t *testing.T) {
			f := newTaggedProducerFixture(t)
			defer f.requireNoHTTP(t)
			p := f.peer.Proxy()
			if err := p.VerifyOwnedProxyPublicationCurrent(f.proof, f.at); err != nil {
				t.Fatalf("live control before %s: %v", action, err)
			}
			if action == "close" {
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				f.cancel()
			}
			if err := p.VerifyOwnedProxyPublicationCurrent(f.proof, f.at); err == nil {
				t.Errorf("%s retained fresh publication authority", action)
			}
			if _, err := p.ObserveOwnedProxy(f.allocation, f.source, f.operation); err == nil {
				t.Errorf("%s issued another original observation", action)
			}
			if err := p.VerifyOwnedProxyPublication(f.proof, f.at); err != nil {
				t.Errorf("%s erased historical authentic receipt: %v", action, err)
			}
		})
	}
}
