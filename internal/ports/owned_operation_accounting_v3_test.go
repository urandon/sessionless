package ports_test

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

func accountingV3Hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
func accountingV3Amount(n uint64) *uint64 { return &n }

// Strict structural DATA only: these declarations authenticate no real
// installation, price documentation, physical effect or erasure receipt.
func accountingV3Fixture(t *testing.T, amount uint64) (domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, []ports.OwnedOperationResidualRowV1, ports.OwnedOperationAccountingDispositionV3) {
	t.Helper()
	at := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	h := strings.Repeat("a", 64)
	a, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant-v3", "owner-v3", "run-v3", "attempt-v3", "subscription-v3", at)
	if err != nil {
		t.Fatal(err)
	}
	c := a.AdmissionCostCeiling.Clone()
	c.PriceExpiresAt = at.Add(time.Hour)
	service := func(kind domain.ExternalServiceKindV1, n uint64) *domain.ExternalServiceCostCeilingV1 {
		state := domain.ExternalServicePriceKnownFreeV1
		if n > 0 {
			state = domain.ExternalServicePriceKnownPaidV1
		}
		return &domain.ExternalServiceCostCeilingV1{Version: 1, Kind: kind, TenantID: "tenant-v3", OwnerUserID: "owner-v3", BillingOwnerUserID: "owner-v3", ResourceID: string(kind) + "-resource", ResourceRevision: 1, RevocationGeneration: 1, ProfileID: string(kind) + "-profile", ProfileRevision: 1, ScopedGrantDigest: accountingV3Hash(string(kind) + "grant"), Currency: c.Currency, PriceRevision: "service-price-v3", PriceObservedAt: at, PriceExpiresAt: at.Add(time.Hour), PriceEvidenceDigest: accountingV3Hash(string(kind) + "price"), TermsEvidenceDigest: accountingV3Hash(string(kind) + "terms"), PriceState: state, MaxAmountMicrounits: accountingV3Amount(n)}
	}
	c.MCPToolCost, c.WebSearchCost = service(domain.ExternalServiceMCPToolV1, amount), service(domain.ExternalServiceWebSearchV1, 0)
	c.MaxTotalAmountMicrounits = accountingV3Amount(amount)
	sub := a.SubstrateBinding
	sub.Kind, sub.Region, sub.ImageDigest, sub.WorkloadMode = domain.SubstrateLocalDevelopmentV1, "", "", domain.SubstrateWorkloadInProcessDirectV1
	sub.LocalInstallationID, sub.LocalInstallationRevision = "installation-v3", 1
	sub.EgressProxyArtifactDigest = sub.OuterHarnessArtifactDigest
	sub.LocalHostIdentityDigest, sub.LocalNetworkNamespaceIdentityDigest = h, accountingV3Hash("namespace")
	sub.ProfileEvidenceExpiresAt = at.Add(time.Hour)
	sub.AdmissionCostCeilingDigest, err = c.Digest()
	if err != nil {
		t.Fatal(err)
	}
	b := a.HarnessBinding.Backend
	b.BackendKind, b.ProviderContractKind, b.NativeProtocolVersion = domain.HarnessBackendDirectOpenAICompatibleV1, domain.ProviderContractConfiguredEndpointV1, domain.ConfiguredInferenceProtocolChatCompletionsV1
	r := a.HarnessBinding.Resource
	r.Kind, r.CredentialMode, r.CredentialGeneration = domain.ProviderResourceLocalEndpointV1, domain.ProviderCredentialNoneV1, 0
	limits := domain.AgentLoopLimitsV1{MaxConcurrentOperations: 1, MaxModelCalls: 8, MaxCompactionCalls: 2, MaxToolDispatches: 4, MaxWebSearches: 2, MaxInputTokensPerCall: 32768, MaxAggregateInputTokens: 131072, MaxOutputTokensPerCall: 2048, MaxAggregateOutputTokens: 16384, MaxActiveDuration: 120 * time.Second, MaxNetworkOperationDuration: 30 * time.Second, MaxCleanupDuration: 30 * time.Second, MaxContextViewBytes: 1 << 20, MaxSummaryBytes: 16 << 10, MaxToolArgumentBytes: 4 << 10, MaxToolResultBytes: 64 << 10, MaxMCPOutboundMessages: 12, MaxMCPCatalogPages: 1, MaxMCPInboundFrames: 128, MaxMCPInboundBytes: 256 << 10, MaxOperations: 12, MaxAggregateBodyBytes: 1 << 20}
	p := domain.OwnedAgentProfileCurrentV1{Version: 1, Locator: domain.OwnedAgentProfileLocatorV1{TenantID: "tenant-v3", OwnerUserID: "owner-v3", Profile: domain.AgentLoopOwnedPublicV1, BackendProfileDigest: b.BackendProfileDigest, ResourceKind: r.Kind, ResourceID: r.ResourceID, ModelVendorID: "vendor-v3", ModelID: a.HarnessBinding.ModelID, InputDataClass: domain.ProviderDataPublicV1}, ProfileRevision: 1, RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Backend: b, Resource: r, Substrate: sub, InstallationID: "installation-v3", InstallationRevision: 1, InstalledManifest: domain.OwnedAgentAuthorityBodyRefV1{Ref: "manifest-v3", Digest: h, Bytes: 32}, ReviewReceipt: domain.OwnedAgentAuthorityBodyRefV1{Ref: "review-v3", Digest: h, Bytes: 32}, ProviderCatalogDigest: h, ProviderRouteDigest: h, CapabilityEvidenceDigest: h, PrivacyPolicyDigest: h, EffectivePolicyDigest: h, ProviderPriceDigest: h, ProviderTermsDigest: h, SubstratePriceDigest: h, SubstrateTermsDigest: h, SubstratePriceState: domain.ExternalServicePriceKnownFreeV1, BillingOwnerUserID: "owner-v3", InputModalities: []domain.AgentInputModalityV1{domain.AgentInputTextV1}, ModelWindowTokens: 65536, TokenEstimatorVersion: "estimator-v3", CalibrationDigest: h, InstructionVersion: "instructions-v3", CompactionPolicyDigest: h, MCPToolGrantDigest: c.MCPToolCost.ScopedGrantDigest, WebSearchGrantDigest: c.WebSearchCost.ScopedGrantDigest, Limits: limits, CostCeiling: c, Window: domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: at.Add(time.Hour)}, UpdatedAt: at, MutationID: "install-v3"}
	if err = p.Validate(); err != nil {
		t.Fatal("profile DATA", err)
	}
	pd, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: "tenant-v3", OwnerUserID: "owner-v3", SessionID: "session-v3", RunID: "run-v3", AttemptID: "attempt-v3"}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for index, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		s := domain.AttemptSpendingAuthorizationCurrentV1{Version: 1, Locator: domain.AttemptSpendingAuthorizationLocatorV1{TenantID: scope.TenantID, AttemptID: scope.AttemptID, ComponentKind: kind, BillingOwnerUserID: scope.OwnerUserID}, RunID: scope.RunID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, SourceKind: domain.AttemptSpendingOwnerLocalSourceV1, SourceInstallationID: p.InstallationID, SourceInstallationRevision: 1, SourceProfile: domain.OwnedAgentProfileReferenceV1{Locator: p.Locator, RevocationGeneration: 1, Digest: pd}, ResourceID: r.ResourceID, ResourceRevision: r.Revision, ResourceGeneration: r.CredentialGeneration, ProfileID: string(p.Locator.Profile), ProfileRevision: 1, ScopedGrantDigest: h, Currency: c.Currency, PriceRevision: c.PriceRevision, PriceEvidenceDigest: h, TermsEvidenceDigest: h, PriceState: domain.ExternalServicePriceKnownFreeV1, MaxAmountMicrounits: accountingV3Amount(0), RevocationGeneration: 1, State: domain.OwnedAgentCurrentActiveV1, Window: p.Window, UpdatedAt: at, MutationID: "spending-v3"}
		if index == 1 {
			s.ResourceID, s.ResourceRevision, s.ResourceGeneration = sub.ProfileID, sub.ProfileRevision, 1
			s.ProfileID, s.ProfileRevision = sub.ProfileID, sub.ProfileRevision
			s.ScopedGrantDigest = sub.ProfileDigest
		}
		if index >= 2 {
			cost := c.MCPToolCost
			if index == 3 {
				cost = c.WebSearchCost
			}
			s.ResourceID, s.ResourceRevision, s.ResourceGeneration, s.ProfileID, s.ProfileRevision = cost.ResourceID, cost.ResourceRevision, cost.RevocationGeneration, cost.ProfileID, cost.ProfileRevision
			s.ScopedGrantDigest, s.PriceRevision, s.PriceEvidenceDigest, s.TermsEvidenceDigest, s.PriceState, s.MaxAmountMicrounits = cost.ScopedGrantDigest, cost.PriceRevision, cost.PriceEvidenceDigest, cost.TermsEvidenceDigest, cost.PriceState, accountingV3Amount(*cost.MaxAmountMicrounits)
		}
		raw, e := s.SourceCanonicalBytes()
		if e != nil {
			t.Fatal(e)
		}
		s.SourceAuthorization = domain.OwnedAgentAuthorityBodyRefV1{Ref: "funding-v3", Digest: accountingV3Hash(string(raw)), Bytes: uint64(len(raw))}
		spending[index] = s
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal("execution package DATA", err)
	}
	final, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "quota-v3", at.Add(time.Microsecond))
	if err != nil {
		t.Fatal(err)
	}
	inv := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost, Lease: domain.Lease{ID: "lease-v3", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "worker-v3", FenceToken: 1, AcquiredAt: final.AdmittedAt, ExpiresAt: at.Add(5 * time.Minute)}, ContextManifestDigest: h, InputManifestDigest: h, InvocationDeadline: at.Add(120 * time.Second)}
	parent, err := domain.BuildOwnedAttemptEffectParentV1(final, inv, "claim-v3", nil, at.Add(2*time.Microsecond))
	if err != nil {
		t.Fatal("parent DATA", err)
	}
	rd, _ := parent.Reservation.DigestForAuthority(inv)
	bd, _ := pkg.Binding.Digest()
	policyDigest, _ := pkg.Binding.AgentLoopPolicy.Digest()
	opScope := domain.AttemptOperationScopeV1{TenantID: scope.TenantID, SessionID: scope.SessionID, OwnerUserID: scope.OwnerUserID, RunID: scope.RunID, AttemptID: scope.AttemptID, LeaseID: inv.Lease.ID, FenceToken: 1, PhysicalInvocationClaimID: "claim-v3", EffectReservationDigest: rd, InvocationAuthorityDigest: parent.Reservation.InvocationAuthorityDigest, HarnessBindingDigest: bd, AgentLoopPolicyDigest: policyDigest}
	d := ports.OwnedOperationAccountingDispositionV3{Version: 3, Scope: opScope, AccountingDigest: h, ProviderInvoice: ports.OwnedLocalInvoiceIdentityV2{ProfileDigest: pkg.Source.Profile.Digest, TermsDigest: h, Revision: c.PriceRevision, Currency: c.Currency, IssuedAt: at, ExpiresAt: at.Add(time.Hour)}}
	aggregate := domain.AttemptOperationDebitV1{BodyBytes: 8 << 10}
	var rows []ports.OwnedOperationResidualRowV1
	for index, kind := range []domain.AttemptOperationKindV1{domain.AttemptOperationModelV1, domain.AttemptOperationMCPToolV1, domain.AttemptOperationModelV1} {
		op := domain.AttemptOperationV1{Version: 1, Scope: opScope, Sequence: uint64(index + 1), Kind: kind, CallID: "model-v3-" + string(rune('a'+index)), RequestRef: "request-v3", RequestDigest: h, Deadline: at.Add(time.Duration(index+1)*time.Second + 10*time.Second), CleanupDuration: 30 * time.Second, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 100, OutputTokens: 20, BodyBytes: 1000, ResponseBytes: 1000, ActiveDuration: 10 * time.Second}, RequestBytes: 100, ResultBytes: 900, ContextBytes: 100, Currency: c.Currency, PriceRevision: c.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest}
		if index > 0 {
			op.PredecessorObservationDigest, _ = rows[index-1].Observation.Digest()
		}
		if kind == domain.AttemptOperationMCPToolV1 {
			op.CallID = "call-v3"
			op.ContextBytes = 0
			op.ModelResponseDigest = rows[index-1].Observation.ResponseDigest
			op.ArgumentsDigest = h
			op.ToolGrantDigest = c.MCPToolCost.ScopedGrantDigest
			op.ExternalChargeEvidenceDigest = c.MCPToolCost.PriceEvidenceDigest
			op.Debit = domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: 1000, ResponseBytes: 1000, MCPOutboundMessages: 4, MCPCatalogPages: 1, MCPInboundFrames: 4, MCPInboundBytes: 1000, AmountMicrounits: amount, ActiveDuration: 10 * time.Second}
		}
		od, e := op.Digest()
		if e != nil {
			t.Fatal(e)
		}
		aggregate, e = aggregate.Add(op.Debit)
		if e != nil {
			t.Fatal(e)
		}
		amounts := [4]uint64{}
		if index >= 1 {
			amounts[2] = amount
		}
		reservation := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: od, ReservedAt: at.Add(time.Duration(index+1) * time.Second), AggregateDebit: aggregate, ComponentAmounts: amounts, RegisteredAllocationDigest: domain.RegisteredAllocationDigestV2(h), RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(h)}
		resDigest, e := reservation.Digest()
		if e != nil {
			t.Fatal(e)
		}
		obs := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: od, ReservationDigest: resDigest, Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1, ResponseRef: "response-v3", ResponseDigest: h, ResponseBytes: 50, ObservedAt: reservation.ReservedAt.Add(time.Millisecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
		row, e := ports.BuildOwnedOperationAccountingRowV3(reservation, obs, parent, pkg.Source)
		if e != nil {
			t.Fatal("exact row DATA", index, e)
		}
		d.Operations = append(d.Operations, row)
		rows = append(rows, ports.OwnedOperationResidualRowV1{Reservation: reservation, Observation: obs})
		d.ComponentAmounts = amounts
	}
	d.AggregateDebit = aggregate
	if err = d.ValidateFor(parent, pkg.Source); err != nil {
		t.Fatal("disposition DATA", err)
	}
	return parent, pkg.Source, rows, d
}

func TestOwnedOperationAccountingV3ZeroAndPaidUnknownFullReservation(t *testing.T) {
	for _, amount := range []uint64{0, 17} {
		t.Run(map[bool]string{true: "zero", false: "paid"}[amount == 0], func(t *testing.T) {
			p, s, rows, d := accountingV3Fixture(t, amount)
			tool := d.Operations[1]
			if tool.Disposition != ports.OwnedOperationUnknownFullReservedV3 || tool.Usage.State != domain.AttemptOperationUsageUnknownV1 || tool.Usage.AmountMicrounits != nil || tool.Reserved.AmountMicrounits != amount || d.ComponentAmounts[2] != amount || d.AggregateDebit.AmountMicrounits != amount {
				t.Fatal("full reservation or truthful unknown changed")
			}
			if _, err := ports.BuildOwnedOperationAccountingRowV2(rows[1].Reservation, rows[1].Observation); err == nil {
				t.Fatal("V2 unknown MCP gate weakened")
			}
			before, err := d.Digest()
			if err != nil {
				t.Fatal(err)
			}
			copy := d.Clone()
			copy.AccountingDigest = accountingV3Hash("changed")
			after, err := copy.Digest()
			if err != nil || before == after {
				t.Fatal("digest omits exact accounting", err)
			}
			copy = d.Clone()
			copy.Operations[0].Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: accountingV3Amount(1)}
			isolated := copy.Clone()
			*isolated.Operations[0].Usage.InputTokens = 2
			if *copy.Operations[0].Usage.InputTokens != 1 {
				t.Fatal("clone aliases usage")
			}
			if err = d.ValidateFor(p, s); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOwnedOperationAccountingV3RejectsTampering(t *testing.T) {
	p, s, rows, d := accountingV3Fixture(t, 17)
	for _, name := range []string{"version", "missing_mcp", "spending", "refund", "amount", "usage_zero", "search", "scope", "row_digest", "invoice", "aggregate", "extra_rows"} {
		t.Run(name, func(t *testing.T) {
			v := d.Clone()
			switch name {
			case "version":
				v.Version = 2
			case "missing_mcp":
				v.Operations[1].Kind = domain.AttemptOperationModelV1
			case "spending":
				v.Operations[1].MCPSpendingDigest = accountingV3Hash("other")
			case "refund":
				v.ComponentAmounts[2]--
			case "amount":
				v.Operations[1].Reserved.AmountMicrounits--
			case "usage_zero":
				v.Operations[1].Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: accountingV3Amount(0)}
			case "search":
				v.Operations[1].Kind = domain.AttemptOperationWebSearchV1
			case "scope":
				v.Scope.SessionID = "foreign"
			case "row_digest":
				v.Operations[1].ObservationDigest = "bad"
			case "invoice":
				v.ProviderInvoice.TermsDigest = accountingV3Hash("other")
			case "aggregate":
				v.AggregateDebit.BodyBytes--
			case "extra_rows":
				v.Operations = append(v.Operations, make([]ports.OwnedOperationAccountingRowV3, 13)...)
			}
			if err := v.ValidateFor(p, s); err == nil {
				t.Fatal("tampered disposition accepted", name)
			}
		})
	}
	for _, name := range []string{"effect_unknown", "service_error", "overrun", "foreign_scope", "envelope", "source_generation", "payer"} {
		t.Run(name, func(t *testing.T) {
			r, o, source := rows[1].Reservation.Clone(), rows[1].Observation.Clone(), s.Clone()
			switch name {
			case "effect_unknown":
				o.Outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
				o.Reason = domain.AttemptOperationReasonTransportUnknownV1
				o.ResponseRef = ""
				o.ResponseDigest = ""
				o.ResponseBytes = 0
			case "service_error":
				o.Reason = domain.AttemptOperationReasonServiceErrorV1
			case "overrun":
				o.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1, AmountMicrounits: accountingV3Amount(18)}
			case "foreign_scope":
				r.Operation.Scope.SessionID = "foreign"
			case "envelope":
				r.Operation.Debit.AmountMicrounits--
			case "source_generation":
				source.Spending[2].ResourceGeneration++
			case "payer":
				source.Spending[2].Locator.BillingOwnerUserID = "foreign"
			}
			if _, err := ports.BuildOwnedOperationAccountingRowV3(r, o, p, source); err == nil {
				t.Fatal("tampered row accepted", name)
			}
		})
	}
	if reflect.DeepEqual(d.Operations[1].Usage, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageObservedV1}) {
		t.Fatal("unknown became measured")
	}
}
