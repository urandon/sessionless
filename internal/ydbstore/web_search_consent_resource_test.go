package ydbstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func presealResourceConsentGrant(t *testing.T, p *domain.OwnedAgentProfileCurrentV1) ([]byte, string, uint64, uint64) {
	t.Helper()
	cost := p.CostCeiling.WebSearchCost.Clone()
	old := *cost.MaxAmountMicrounits
	total := *p.CostCeiling.MaxTotalAmountMicrounits - old + 20000
	amount := uint64(20000)
	p.CostCeiling.MaxTotalAmountMicrounits = &total
	cost.MaxAmountMicrounits = &amount
	cost.PriceState = domain.ExternalServicePriceKnownPaidV1
	cost.ProfileID = domain.WebSearchProfileV1
	g := domain.WebSearchResourceGrantV2{Version: 2, TenantID: cost.TenantID, OwnerUserID: cost.OwnerUserID, BillingOwnerUserID: cost.BillingOwnerUserID,
		ResourceID: cost.ResourceID, ResourceRevision: cost.ResourceRevision, RevocationGeneration: cost.RevocationGeneration, ConsentRevision: 3, ProfileID: cost.ProfileID, ProfileRevision: cost.ProfileRevision,
		Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "private-resource-credential-locator", CredentialAudience: domain.WebSearchCredentialAudienceV1,
		DataPolicyDigest: strings.Repeat("a", 64), APISchemaDigest: strings.Repeat("b", 64), AdapterArtifactDigest: strings.Repeat("c", 64), Currency: cost.Currency, PriceRevision: cost.PriceRevision,
		PriceEvidenceDigest: cost.PriceEvidenceDigest, TermsEvidenceDigest: cost.TermsEvidenceDigest, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: cost.PriceObservedAt, ExpiresAt: cost.PriceExpiresAt}
	raw, err := domain.EncodeWebSearchResourceGrantV2(g)
	if err != nil {
		t.Fatalf("invalid pre-seal resource grant: %v", err)
	}
	semantic, _ := g.Digest()
	cost.ScopedGrantDigest = semantic
	p.CostCeiling.WebSearchCost = &cost
	p.WebSearchResourceGrant = &domain.WebSearchResourceGrantBodyV2{Version: 2, Body: domain.OwnedAgentAuthorityBodyRefV1{Ref: "original-private-resource-grant", Digest: ownedConsentTestHash(raw), Bytes: uint64(len(raw))}}
	return raw, semantic, g.ConsentRevision, g.RevocationGeneration
}

func newResourceConsentViewFixture(t *testing.T, setup ...privateConsentGrantSetup) (*privateConsentViewFixture, *resourceConsentCustodyFixture) {
	t.Helper()
	configure := privateConsentGrantSetup(presealResourceConsentGrant)
	if len(setup) > 1 {
		t.Fatal("one resource grant setup is supported")
	}
	if len(setup) == 1 {
		configure = setup[0]
	}
	f := newPrivateConsentViewFixture(t, configure)
	p := f.allocation.parent.base.reader.profile.Clone()
	pd, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := ports.OwnedFixtureGrantReferenceV1(f.state.Request.GrantDigest)
	c := &resourceConsentCustodyFixture{f: f, body: bytes.Clone(f.bodies[ref]), ref: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: p.RevocationGeneration, Digest: pd}}
	lifetime, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	c.lifetime, c.cancel = resourceConsentLifetime{Context: lifetime, deadline: p.Window.ExpiresAt}, cancel
	f.allocation.parent.store.ownedWebSearchResourceGrantCustody = c
	return f, c
}

func TestPrivateWebSearchConsentResourceOriginalLifetimeShapeAndDeadline(t *testing.T) {
	for _, kind := range []string{"nil-context", "nil-Done", "missing-deadline", "zero-deadline", "expired-before-body", "deadline-final-read", "deadline-final-decision"} {
		t.Run(kind, func(t *testing.T) {
			f, c := newResourceConsentViewFixture(t)
			at := f.allocation.parent.graph.at
			wantReads := 0
			switch kind {
			case "nil-context":
				c.lifetime = nil
			case "nil-Done":
				c.lifetime = resourceConsentLifetime{Context: context.Background(), deadline: at.Add(time.Hour)}
			case "missing-deadline":
				c.lifetime = resourceConsentNoDeadline{Context: c.lifetime}
			case "zero-deadline":
				c.lifetime = resourceConsentLifetime{Context: c.lifetime}
			case "expired-before-body":
				c.lifetime = resourceConsentLifetime{Context: c.lifetime, deadline: at}
			default:
				// Only the original DB observation advances. Done remains open;
				// final SQL must still respect the original finite deadline.
				deadline := at.Add(500 * time.Millisecond)
				c.lifetime = resourceConsentLifetime{Context: c.lifetime, deadline: deadline}
				c.fault = func(context.Context) { f.allocation.parent.graph.at = deadline }
				wantReads = 1
			}
			initial := f.state.Clone()
			if kind == "deadline-final-decision" {
				got, err := f.allocation.parent.store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f))
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) {
					t.Fatalf("invalid original Source lifetime released approval: err=%v", err)
				}
			} else {
				got, err := f.read(t, t.Context())
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) {
					t.Fatalf("invalid original Source lifetime released private query: err=%v query=%q", err, got.Query)
				}
			}
			if c.reads != wantReads || f.decisionWrites != 0 || !reflect.DeepEqual(f.state, initial) {
				t.Fatalf("lifetime denial changed owned effects: reads=%d want=%d writes=%d", c.reads, wantReads, f.decisionWrites)
			}
		})
	}
}

func TestPrivateWebSearchConsentResourcePostReadSourceClose(t *testing.T) {
	for _, kind := range []string{"arguments-read", "before-final-SQL-read", "before-final-SQL-decision", "commit-return-read", "commit-return-decision"} {
		t.Run(kind, func(t *testing.T) {
			f, c := newResourceConsentViewFixture(t)
			store := f.allocation.parent.store
			decision := strings.HasSuffix(kind, "decision")
			if kind == "arguments-read" {
				f.bodyFault = func(_ context.Context, ref string) {
					if ref == f.state.Request.Arguments.Key {
						if c.reads != 1 {
							t.Fatal("grant read did not complete before argument custody")
						}
						c.close()
					}
				}
			} else if strings.HasPrefix(kind, "before-final-SQL") {
				f.beforeBegin = func() {
					if c.reads == 1 {
						c.close()
					}
				}
			} else {
				f.afterCommit = func() {
					if f.commits == 2 {
						c.close()
					}
				}
			}
			initial := f.state.Clone()
			if decision {
				got, err := store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f))
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) {
					t.Fatalf("closed Source released approval: err=%v decision=%+v", err, got.Decision)
				}
			} else {
				got, err := f.read(t, t.Context())
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) {
					t.Fatalf("closed Source released private query: err=%v query=%q", err, got.Query)
				}
			}
			if c.reads != 1 || !c.closed {
				t.Fatalf("post-read Source Close control not exercised reads=%d closed=%t", c.reads, c.closed)
			}
			if kind == "commit-return-decision" {
				// The receipt committed before Close; hide output, never pretend to
				// roll it back or re-execute an ambiguous/closed-owner decision.
				if f.decisionWrites != 1 || f.state.Decision == nil {
					t.Fatal("committed evidence lost before output revocation")
				}
				committed := f.state.Clone()
				if got, err := store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f)); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.decisionWrites != 1 || !reflect.DeepEqual(f.state, committed) {
					t.Fatal("closed Source replayed or replaced committed decision")
				}
			} else if f.decisionWrites != 0 || !reflect.DeepEqual(f.state, initial) {
				t.Fatal("Source Close before decision gate wrote approval")
			}
		})
	}
}

func TestPrivateWebSearchConsentResourceExactQueryAndBudget(t *testing.T) {
	f, c := newResourceConsentViewFixture(t)
	view, err := f.read(t, t.Context())
	if err != nil || view.Query != f.query || !reflect.DeepEqual(view.State, f.state) {
		t.Fatalf("actual current V2 private view query=%q err=%v", view.Query, err)
	}
	if c.reads != 1 || len(c.caps) != 1 || c.caps[0] != uint64(len(c.body)) || f.commits != 2 || f.budget.callbacks != 2 || f.budget.statements > 96 || f.budget.bytes > ownedAuthorityMaxBytes || !reflect.DeepEqual(f.reads, []string{f.state.Request.Arguments.Key}) || f.decisionWrites != 0 {
		t.Fatalf("wrong V2 original custody/budget: grantreads=%d caps=%v callbacks=%d statements=%d bodyreads=%v writes=%d", c.reads, c.caps, f.budget.callbacks, f.budget.statements, f.reads, f.decisionWrites)
	}
}

func TestPrivateWebSearchConsentResourceBodyAndCurrentDenials(t *testing.T) {
	for _, name := range []string{"nil-custody", "closed", "malformed", "semantic-mismatch", "oversize", "cancel", "current-replaced-during-I/O", "current-corrupt-during-I/O", "request-changed-during-I/O", "revoked-before-I/O", "wrong-actor", "commit-unknown"} {
		t.Run(name, func(t *testing.T) {
			f, c := newResourceConsentViewFixture(t)
			store := f.allocation.parent.store
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "nil-custody":
				store.ownedWebSearchResourceGrantCustody = nil
			case "closed":
				c.closed = true
			case "malformed":
				c.body = []byte(`{"version":2}`)
			case "semantic-mismatch":
				var g domain.WebSearchResourceGrantV2
				if json.Unmarshal(c.body, &g) != nil {
					t.Fatal("fixture decode")
				}
				g.ConsentRevision++
				c.body, _ = domain.EncodeWebSearchResourceGrantV2(g)
			case "oversize":
				c.body = bytes.Repeat([]byte{' '}, 8193)
			case "cancel":
				c.fault = func(context.Context) { cancel() }
			case "current-replaced-during-I/O":
				c.fault = func(context.Context) {
					f.allocation.parent.base.reader.profile.RevocationGeneration++
					f.allocation.parent.base.reader.sync(t)
				}
			case "current-corrupt-during-I/O":
				c.fault = func(context.Context) { f.allocation.parent.base.reader.driver.profileRow[0] = "{" }
			case "request-changed-during-I/O":
				c.fault = func(context.Context) { f.state.Request.QueryDigest = strings.Repeat("b", 64) }
			case "revoked-before-I/O":
				f.allocation.parent.base.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				f.allocation.parent.base.reader.sync(t)
			case "wrong-actor":
				f.web.UserID = "foreign-user"
			case "commit-unknown":
				f.commitErrorAt = 2
			}
			view, err := f.read(t, ctx)
			if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(view, ports.WebSearchConsentPrivateViewV1{}) || f.decisionWrites != 0 {
				t.Fatalf("%s leaked query/decision: view=%+v err=%v", name, view, err)
			}
			if len(f.reads) > 1 || len(f.reads) == 1 && f.reads[0] != f.state.Request.Arguments.Key {
				t.Fatalf("V2 failure fell back to fixture grant: %v", f.reads)
			}
			if (name == "wrong-actor" || name == "revoked-before-I/O") && c.reads != 0 {
				t.Fatal("unauthorized actor materialized grant body")
			}
		})
	}
}

func TestPrivateWebSearchConsentResourceClosedGrammarAndSemanticBinding(t *testing.T) {
	for _, kind := range []string{"unknown", "duplicate", "alias", "null", "trailing", "semantic"} {
		t.Run(kind, func(t *testing.T) {
			f, c := newResourceConsentViewFixture(t, func(t *testing.T, p *domain.OwnedAgentProfileCurrentV1) ([]byte, string, uint64, uint64) {
				raw, semantic, consent, revocation := presealResourceConsentGrant(t, p)
				switch kind {
				case "unknown":
					raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"extra":1}`)
				case "duplicate":
					raw = []byte(strings.Replace(string(raw), `"version":2`, `"version":2,"version":2`, 1))
				case "alias":
					raw = []byte(strings.Replace(string(raw), `"version":2`, `"Version":2`, 1))
				case "null":
					raw = []byte(strings.Replace(string(raw), `"version":2`, `"version":null`, 1))
				case "trailing":
					raw = append(raw, []byte(`{}`)...)
				case "semantic":
					var g domain.WebSearchResourceGrantV2
					if json.Unmarshal(raw, &g) != nil {
						t.Fatal("fixture decode")
					}
					g.ConsentRevision++
					raw, _ = domain.EncodeWebSearchResourceGrantV2(g)
				}
				p.WebSearchResourceGrant.Body.Digest = ownedConsentTestHash(raw)
				p.WebSearchResourceGrant.Body.Bytes = uint64(len(raw))
				return raw, semantic, consent, revocation
			})
			if uint64(len(c.body)) != f.allocation.parent.base.reader.profile.WebSearchResourceGrant.Body.Bytes || ownedConsentTestHash(c.body) != f.allocation.parent.base.reader.profile.WebSearchResourceGrant.Body.Digest {
				t.Fatal("coherent raw identity prerequisite failed")
			}
			if view, err := f.read(t, t.Context()); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(view, ports.WebSearchConsentPrivateViewV1{}) || c.reads != 1 || len(f.reads) != 0 {
				t.Fatalf("%s malformed/resource-substituted body was disclosed: reads=%d args=%v err=%v", kind, c.reads, f.reads, err)
			}
		})
	}
}

func TestPrivateWebSearchConsentResourceOriginalBudgetAndDecision(t *testing.T) {
	t.Run("budget-before-body", func(t *testing.T) {
		f, c := newResourceConsentViewFixture(t)
		f.afterCommit = func() {
			if f.commits == 1 {
				b := f.budget
				b.mu.Lock()
				b.bytes = ownedAuthorityMaxBytes - (2*ownedAuthorityHeadBytes + 4*uint64(len(c.body))) + 1
				b.mu.Unlock()
			}
		}
		if view, err := f.read(t, t.Context()); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(view, ports.WebSearchConsentPrivateViewV1{}) || c.reads != 0 || len(f.reads) != 0 {
			t.Fatalf("exhausted original budget read custody: grant=%d args=%v err=%v", c.reads, f.reads, err)
		}
	})
	t.Run("actual-canonical-Read-Decide-CAS", func(t *testing.T) {
		f, c := newResourceConsentViewFixture(t)
		store := f.allocation.parent.store
		input := privateConsentDecisionInput(f)
		if state, err := store.ReadWebSearchConsent(t.Context(), privateConsentCanonicalInput(f)); err != nil || !reflect.DeepEqual(state, f.state) {
			t.Fatalf("V2 canonical Read failed %v", err)
		}
		approved, err := store.DecideWebSearchConsent(t.Context(), input)
		if err != nil || approved.Decision == nil || approved.Status != domain.WebSearchConsentApprovedV1 || f.decisionWrites != 1 {
			t.Fatalf("V2 actual decision CAS failed %+v err=%v", approved, err)
		}
		retried, err := store.DecideWebSearchConsent(t.Context(), input)
		if err != nil || !reflect.DeepEqual(approved, retried) || f.decisionWrites != 1 {
			t.Fatalf("V2 decision retry changed receipt: %v", err)
		}
		for _, kind := range []string{"conflict", "csrf"} {
			bad := input
			if kind == "conflict" {
				bad.Choice = domain.WebSearchConsentDenyV1
			} else {
				bad.CSRFTokenDigest = domain.DigestSecret("foreign")
			}
			if got, err := store.DecideWebSearchConsent(t.Context(), bad); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.decisionWrites != 1 {
				t.Fatalf("%s changed receipt: %v", kind, err)
			}
		}
		if c.reads != 5 || len(f.reads) != 0 {
			t.Fatalf("canonical V2 Read/Decide read query/fixture bytes: grant=%d args=%v", c.reads, f.reads)
		}
	})
}

func TestPrivateWebSearchConsentResourceActualArgumentCustody(t *testing.T) {
	for _, kind := range []string{"exact", "corrupt", "missing"} {
		t.Run(kind, func(t *testing.T) {
			f, c := newResourceConsentViewFixture(t)
			args := bytes.Clone(f.bodies[f.state.Request.Arguments.Key])
			defer clear(args)
			blobs := &localEntryBlobs{}
			scope := ownedConsentOperationScope(f.state.Request.Scope)
			owner, err := operationbody.NewV1(scope, f.allocation.parent.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits, blobs)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := owner.WriteConsentArguments(t.Context(), args)
			if err != nil {
				t.Fatal(err)
			}
			delete(f.bodies, f.state.Request.Arguments.Key)
			f.state.Request.Arguments, f.argumentBodies = ref, owner
			if kind == "corrupt" {
				blobs.objects[ref.Key][0] ^= 1
			}
			if kind == "missing" {
				delete(blobs.objects, ref.Key)
			}
			view, err := f.read(t, t.Context())
			if kind == "exact" {
				if err != nil || view.Query != f.query {
					t.Fatalf("original V2 grant+actual argument owner denied: %v", err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(view, ports.WebSearchConsentPrivateViewV1{}) {
				t.Fatalf("%s object disclosed query: %v", kind, err)
			}
			if c.reads != 1 || !reflect.DeepEqual(f.reads, []string{ref.Key}) || f.readCaps[0] != uint64(ref.Size) {
				t.Fatalf("actual bounded body reader not exercised: grant=%d args=%v caps=%v", c.reads, f.reads, f.readCaps)
			}
		})
	}
}
