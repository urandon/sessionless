package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type searchWireDenyBodies struct{ reads int }

func (b *searchWireDenyBodies) ReadAttemptOperationBody(context.Context, domain.AttemptOperationScopeV1, string, uint64) ([]byte, error) {
	b.reads++
	return nil, errors.New("controlled body reader must not be reached without original search producer")
}

type searchWireCountingIssuer struct {
	ports.AttemptOperationCapabilityIssuerV1
	mints, consumes int
}

func (i *searchWireCountingIssuer) MintAttemptOperationCapability(r domain.AttemptOperationReservationV1, at, until time.Time) (ports.AttemptOperationCapabilityV1, error) {
	i.mints++
	return i.AttemptOperationCapabilityIssuerV1.MintAttemptOperationCapability(r, at, until)
}
func (i *searchWireCountingIssuer) MintAttemptOperationObservationGrant(r domain.AttemptOperationReservationV1, at, until time.Time) (ports.AttemptOperationObservationGrantV1, error) {
	i.mints++
	return i.AttemptOperationCapabilityIssuerV1.MintAttemptOperationObservationGrant(r, at, until)
}
func (i *searchWireCountingIssuer) ConsumeAttemptOperationCapability(c ports.AttemptOperationCapabilityV1) error {
	i.consumes++
	return i.AttemptOperationCapabilityIssuerV1.ConsumeAttemptOperationCapability(c)
}

func searchWireStoreRequest(t *testing.T, f *ownedAttemptEffectParentFixture) ports.ReserveAttemptOperationRequestV1 {
	t.Helper()
	scope := ownedOperationScope(f.parent)
	cs := ownedConsentScope(f.parent)
	d := strings.Repeat("a", 64)
	args := []byte(`{"query":"public query"}`)
	cost := f.parent.Authority.AdmissionCostCeiling
	component := cost.WebSearchCost
	request := domain.WebSearchConsentRequestV1{Version: 1, ID: "search-wire-consent", Scope: cs, InitiatingUserID: f.source.Scope.OwnerUserID, ModelResponseDigest: d, CallID: "search-wire-call", ArgumentsDigest: ownedConsentTestHash(args), QueryDigest: ownedConsentTestHash([]byte("public query")), GrantDigest: component.ScopedGrantDigest, ConsentRevision: 1, RevocationGeneration: 1,
		Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent/search-wire.json", Size: int64(len(args)), SHA256: ownedConsentTestHash(args)}, CreatedAt: f.at, ExpiresAt: f.at.Add(time.Second)}
	if err := request.ValidateArguments(args); err != nil {
		t.Fatal(err)
	}
	rd, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	cd, err := cost.Digest()
	if err != nil {
		t.Fatal(err)
	}
	op := domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: request.CallID, RequestRef: "compiled-search-wire", RequestDigest: d, PredecessorObservationDigest: domain.AttemptOperationObservationDigestV1(d), ModelResponseDigest: d, ArgumentsDigest: request.ArgumentsDigest, QueryDigest: request.QueryDigest, ToolGrantDigest: request.GrantDigest, ConsentRequestDigest: string(rd), ConsentDecisionDigest: d, ConsentDecisionRef: string(request.ID), Deadline: request.ExpiresAt, CleanupDuration: f.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxCleanupDuration,
		Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: 1024, ResponseBytes: 512, AmountMicrounits: *component.MaxAmountMicrounits, ActiveDuration: time.Second}, RequestBytes: 128, ResultBytes: 512, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, ExternalChargeEvidenceDigest: component.PriceEvidenceDigest,
		SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(len(args)), CompilerArtifactDigest: d}}
	result := ports.ReserveAttemptOperationRequestV1{Operation: op, ConsentReceipt: &domain.WebSearchConsentReceiptBindingV1{Request: request, RequestDigest: rd, DecisionDigest: domain.WebSearchConsentDecisionDigestV1(d)}}
	if err := result.Validate(); err != nil {
		t.Fatalf("shape-valid search request prerequisite: %v", err)
	}
	return result
}

func searchWireCapability(t *testing.T, request ports.ReserveAttemptOperationRequestV1, at time.Time) ports.AttemptOperationCapabilityV1 {
	t.Helper()
	op := request.Operation
	od, err := op.Digest()
	if err != nil {
		t.Fatal(err)
	}
	aggregate := op.Debit
	aggregate.Operations = 2
	aggregate.ModelCalls = 1
	r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: at, AggregateDebit: aggregate, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(strings.Repeat("a", 64)), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(strings.Repeat("b", 64))}
	r.ComponentAmounts[3] = op.Debit.AmountMicrounits
	capability := ports.AttemptOperationCapabilityV1{Version: 1, Reservation: r, IssuedAt: at, ExpiresAt: op.Deadline, Authenticator: []byte(strings.Repeat("c", 32))}
	if err := capability.Validate(); err != nil {
		t.Fatalf("shape-valid capability prerequisite (not minted authority): %v", err)
	}
	return capability
}

func TestAttemptOperationSearchWireParentUsesArgumentNotPhysicalCeiling(t *testing.T) {
	f := newOwnedAttemptEffectParentFixture(t)
	op := searchWireStoreRequest(t, f).Operation
	op.RequestBytes = 8192
	op.Debit.BodyBytes = op.RequestBytes + op.ResultBytes
	if err := op.ValidateForParent(f.parent.Authority, f.parent.Reservation); err != nil {
		t.Fatalf("authentic parent must compare its argument ceiling to arguments_bytes, not 8192 physical bytes: %v", err)
	}
	op.SearchWire.ArgumentsBytes = f.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxToolArgumentBytes + 1
	if op.ValidateForParent(f.parent.Authority, f.parent.Reservation) == nil {
		t.Fatal("new arm escaped the original parent's argument ceiling")
	}
}

func TestAttemptOperationSearchWireOriginalStoreDeniesBeforeGenericEvidence(t *testing.T) {
	for _, entry := range []string{"prepare-original-evidence", "public-reserve", "public-before-send"} {
		t.Run(entry, func(t *testing.T) {
			f := newOwnedAttemptEffectParentFixture(t)
			installOperationIssuer(t, f)
			bodies := &searchWireDenyBodies{}
			f.store.attemptOperationBodies = bodies
			issuer := &searchWireCountingIssuer{AttemptOperationCapabilityIssuerV1: f.store.attemptOperationIssuer}
			f.store.attemptOperationIssuer = issuer
			request := searchWireStoreRequest(t, f)
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			if !f.store.operationReady(ctx) {
				t.Fatal("unrelated operation readiness guard would mask missing-producer route")
			}
			statements, writes := len(f.graph.statements), f.graph.writes
			var err error
			switch entry {
			case "prepare-original-evidence":
				_, err = f.store.prepareOwnedOperationEvidence(ctx, request.Operation)
			case "public-reserve":
				var result ports.ReserveAttemptOperationResultV1
				result, err = f.store.ReserveAttemptOperation(ctx, request)
				if result.Capability != nil {
					t.Fatal("missing producer minted capability")
				}
			case "public-before-send":
				capability := searchWireCapability(t, request, f.at)
				err = f.store.AuthorizeAttemptOperationBeforeSend(ctx, capability)
			}
			if err == nil || bodies.reads != 0 || len(f.graph.statements) != statements || f.graph.writes != writes || issuer.mints != 0 || issuer.consumes != 0 {
				t.Fatalf("missing authentic search producer reached generic machinery: entry=%s err=%v body_reads=%d SQL=%d writes=%d mints=%d consumes=%d", entry, err, bodies.reads, len(f.graph.statements)-statements, f.graph.writes-writes, issuer.mints, issuer.consumes)
			}
		})
	}
	// Distinguish the new-arm denial from blanket breakage of the original
	// fixture route. This is a read-routing control, not successful dispatch.
	t.Run("legacy-fixture-route-is-not-new-arm", func(t *testing.T) {
		f := newOwnedAttemptEffectParentFixture(t)
		request := searchWireStoreRequest(t, f)
		request.Operation.SearchWire = nil
		bodies := &searchWireDenyBodies{}
		f.store.attemptOperationBodies = bodies
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		_, err := f.store.prepareOwnedOperationEvidence(ctx, request.Operation)
		if err == nil || bodies.reads != 1 {
			t.Fatalf("legacy fixture route changed: reads=%d err=%v", bodies.reads, err)
		}
	})
}

func searchWireColumns(t *testing.T, r domain.AttemptOperationReservationV1) ownedOperationColumns {
	t.Helper()
	raw, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	d, _ := r.Digest()
	return ownedOperationColumns{raw: string(raw), digest: string(d), operationDigest: string(r.OperationDigest), call: r.Operation.CallID}
}

func TestAttemptOperationSearchWireSharedHistoricalDecoder(t *testing.T) {
	// The actual production decoder is also used by retained-deletion writer
	// inventory. This does not claim a successful search terminal/capsule.
	v := ownedResidualInventoryFixture(t)
	legacy := v.Operations[0].Reservation
	decode := func(c ownedOperationColumns, sequence uint64) (*ownedOperationRow, error) {
		q := &ownedAuthorityQuery{budget: &ownedAuthorityBudget{}, at: legacy.ReservedAt.Add(time.Second)}
		return c.decode(q, legacy.Operation.Scope, sequence)
	}
	if _, err := decode(searchWireColumns(t, legacy), legacy.Operation.Sequence); err != nil {
		t.Fatalf("original V1 historical row: %v", err)
	}
	t.Run("explicit-null-with-valid-legacy-digests", func(t *testing.T) {
		c := searchWireColumns(t, legacy)
		original := c.raw
		c.raw = strings.Replace(c.raw, `"external_charge_evidence_digest":""}`, `"external_charge_evidence_digest":"","search_wire":null}`, 1)
		if c.raw == original {
			t.Fatal("explicit-null fixture did not modify the original operation")
		}
		var parsed domain.AttemptOperationReservationV1
		if err := json.Unmarshal([]byte(c.raw), &parsed); err != nil || parsed.Validate() != nil {
			t.Fatalf("null control must retain a valid legacy reservation: %v", err)
		}
		digest, err := parsed.Digest()
		if err != nil || string(digest) != c.digest || string(parsed.OperationDigest) != c.operationDigest {
			t.Fatalf("null control changed semantic identities: reservation=%s operation=%s err=%v", digest, parsed.OperationDigest, err)
		}
		if _, err := decode(c, legacy.Operation.Sequence); err == nil {
			t.Fatal("explicit-null noncanonical bytes accepted despite unchanged valid legacy digests")
		}
	})
	for _, kind := range []string{"valid-new-arm", "null-arm", "partial-arm", "unknown-arm-version", "duplicate-arm", "aliased-arm", "extra-arm-field", "trailing-JSON"} {
		t.Run(kind, func(t *testing.T) {
			r := legacy.Clone()
			op := r.Operation
			op.Kind = domain.AttemptOperationWebSearchV1
			op.Sequence = 2
			op.CallID = "historical-search"
			op.ContextBytes = 0
			op.Debit.ModelCalls = 0
			op.Debit.InputTokens = 0
			op.Debit.OutputTokens = 0
			op.Debit.ToolDispatches = 1
			op.Debit.WebSearches = 1
			d := strings.Repeat("a", 64)
			op.PredecessorObservationDigest = domain.AttemptOperationObservationDigestV1(d)
			op.ModelResponseDigest = d
			op.ArgumentsDigest = d
			op.QueryDigest = d
			op.ToolGrantDigest = d
			op.ConsentRequestDigest = d
			op.ConsentDecisionDigest = d
			op.ConsentDecisionRef = "historical-consent"
			op.ExternalChargeEvidenceDigest = d
			op.SearchWire = &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: 23, CompilerArtifactDigest: d}
			if kind == "partial-arm" {
				op.SearchWire.APIProfile = ""
			}
			if kind == "unknown-arm-version" {
				op.SearchWire.Version = 2
			}
			r.Operation = op
			r.OperationDigest, _ = op.Digest()
			r.AggregateDebit = op.Debit
			r.AggregateDebit.Operations = 2
			r.AggregateDebit.ModelCalls = 1
			c := searchWireColumns(t, r)
			switch kind {
			case "null-arm":
				c.raw = strings.Replace(c.raw, `"search_wire":{"version":1,"api_profile":"tavily.search.basic.v1","arguments_bytes":23,"compiler_artifact_digest":"`+d+`"}`, `"search_wire":null`, 1)
			case "duplicate-arm":
				c.raw = strings.Replace(c.raw, `"search_wire":{`, `"search_wire":{},"search_wire":{`, 1)
			case "aliased-arm":
				c.raw = strings.Replace(c.raw, `"search_wire":{`, `"Search_Wire":{`, 1)
			case "extra-arm-field":
				c.raw = strings.Replace(c.raw, `"search_wire":{"version":1`, `"search_wire":{"extra":true,"version":1`, 1)
			case "trailing-JSON":
				c.raw += `{}`
			}
			_, err := decode(c, r.Operation.Sequence)
			if (err == nil) != (kind == "valid-new-arm") {
				t.Fatalf("strict shared historical decoder: kind=%s err=%v", kind, err)
			}
		})
	}
}
