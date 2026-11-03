package ydbstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// Canonical historical DATA over the real codecs and mounted SQL readers,
// not a serving producer or cleanup receipt. All installation, funding,
// observation/retirement rows below are controlled nonsecret fixture records.
// No original pending/search/failure writer exists or is fabricated here.
type failureSearchV4Fixture struct {
	history           localFailureHistoryV1
	capsule           localFailureCapsuleV4
	records           []sessioncontext.EventPayload
	blobs             map[string][]byte
	proxies           []domain.RegisteredProxyAttestationV2
	proxyPolicies     [][]byte
	proxyAttestations [][]byte
}

func failureSearchV4Data(t *testing.T, stop string, outcome domain.AttemptOperationOutcomeV1, tailUsage ...domain.AttemptOperationUsageV1) *failureSearchV4Fixture {
	t.Helper()
	if len(tailUsage) > 1 {
		t.Fatal("at most one actual MODEL tail usage projection")
	}
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	r := newOwnedReaderFixtureAt(t, at, "-failure-v4")
	g := syntheticLocalInstallationGrant(t, r.profile)
	p, config := g.Profile.Clone(), g.LocalInstallation.Profile.Clone()
	total := *p.CostCeiling.MaxTotalAmountMicrounits - *p.CostCeiling.WebSearchCost.MaxAmountMicrounits + 20000
	search := uint64(20000)
	p.CostCeiling.MaxTotalAmountMicrounits, p.CostCeiling.WebSearchCost.MaxAmountMicrounits = &total, &search
	p.Substrate.EgressProxyArtifactDigest = p.Substrate.OuterHarnessArtifactDigest
	loopbackPolicy, err := ports.BuildLocalLoopbackEgressPolicyV1(config)
	if err != nil {
		t.Fatal(err)
	}
	p.Substrate.EgressPolicyDigest, err = loopbackPolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	searchCost := p.CostCeiling.WebSearchCost
	searchGrant := domain.WebSearchResourceGrantV2{Version: 2, TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, BillingOwnerUserID: p.BillingOwnerUserID, ResourceID: searchCost.ResourceID, ResourceRevision: searchCost.ResourceRevision, RevocationGeneration: searchCost.RevocationGeneration, ConsentRevision: 1, ProfileID: domain.WebSearchProfileV1, ProfileRevision: searchCost.ProfileRevision, Provider: "tavily", APIProfile: domain.WebSearchAPIProfileV1, Endpoint: domain.WebSearchEndpointV1, Method: "POST", CredentialRef: "search-credential-fixture", CredentialAudience: domain.WebSearchCredentialAudienceV1, DataPolicyDigest: strings.Repeat("a", 64), APISchemaDigest: strings.Repeat("b", 64), AdapterArtifactDigest: strings.Repeat("f", 64), Currency: searchCost.Currency, PriceRevision: searchCost.PriceRevision, PriceEvidenceDigest: searchCost.PriceEvidenceDigest, TermsEvidenceDigest: searchCost.TermsEvidenceDigest, PerCallReservationMicrounits: 10000, AggregateReservationMicrounits: 20000, IssuedAt: at, ExpiresAt: searchCost.PriceExpiresAt}
	searchCost.ProfileID = searchGrant.ProfileID
	searchCost.ScopedGrantDigest, err = searchGrant.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p.WebSearchGrantDigest = searchCost.ScopedGrantDigest
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: r.scope.tenant, OwnerUserID: r.scope.owner, SessionID: r.scope.session, RunID: r.scope.run, AttemptID: r.scope.attempt}
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		spending[i] = localRuntimeStructuralSpending(t, p, scope, kind)
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal("historical execution source DATA", err)
	}
	admission, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, "quota-failure-v4", at)
	if err != nil {
		t.Fatal(err)
	}
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost, Lease: domain.Lease{ID: "lease-failure-v4", TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "worker-failure-v4", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)}, ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64), InvocationDeadline: at.Add(120 * time.Second)}
	parent, err := domain.BuildOwnedAttemptEffectParentV1(admission, a, "claim-failure-v4", nil, at)
	if err != nil {
		t.Fatal("historical parent DATA", err)
	}
	sd, _ := a.SubstrateBinding.Digest()
	prepared := domain.PreparedAllocationV2{Version: 2, SubstrateBindingDigest: sd, ObservedOuterHarnessDigest: p.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: p.Substrate.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: p.Substrate.EgressProxyIdentityDigest, WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1, LocalRuntime: &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, InstallationID: p.InstallationID, InstallationRevision: p.InstallationRevision, WorkerExecutableDigest: p.Substrate.OuterHarnessArtifactDigest, HostIdentityDigest: p.Substrate.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: p.Substrate.LocalNetworkNamespaceIdentityDigest, Limits: config.Limits}}
	original, err := domain.BuildRegisteredAllocationV2(a, parent.Reservation, prepared, domain.PreparedInvocationDigestV1(strings.Repeat("c", 64)), parent.PreparationExpiresAt, pkg.Source, "allocation-failure-v4", 1, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: a.InvocationDeadline})
	if err != nil {
		t.Fatal("historical original allocation DATA", err)
	}
	od, _ := original.Digest()
	f := &failureSearchV4Fixture{blobs: map[string][]byte{}}
	c := localFailureCapsuleV4{Schema: "sessionless.terminal-failure-custody.v4", Version: 4, Parent: parent, Source: pkg.Source, ProfileDigest: p.Backend.BackendProfileDigest, Stop: stop, Cleanup: "joined_retained", CapturedAt: at.Add(10 * time.Second), Consents: []domain.WebSearchConsentStateV1{}, ModelCatalogs: []localFailureModelCatalogV4{}, Events: []localCapsuleEventV1{}, Bodies: []domain.BlobRef{}}
	policy := parent.Authority.HarnessBinding.AgentLoopPolicy
	opScope := ownedOperationScope(parent)
	accounting := ownedAttemptAccounting{Version: 1, SetupReservedAt: at, UpdatedAt: at, Aggregate: domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(parent)}}
	accounting.ParentDigest, _ = parent.Digest()
	accounting.Amounts[1] = *pkg.Source.Spending[1].MaxAmountMicrounits
	addBody := func(body []byte) string {
		token := fmt.Sprintf("opbody-v1.%d.%s", len(body), rawLocalFailureHash(body))
		ref, e := operationbody.RetainedOperationBodyRefV1(opScope, token, uint64(len(body)), rawLocalFailureHash(body))
		if e != nil {
			t.Fatal(e)
		}
		f.blobs[ref.Key] = bytes.Clone(body)
		return token
	}
	appendOperation := func(op domain.AttemptOperationV1, normalized []byte, result domain.AttemptOperationOutcomeV1, reason domain.AttemptOperationReasonV1, when time.Time) {
		t.Helper()
		if len(c.Operations) > 0 {
			op.PredecessorObservationDigest, _ = c.Operations[len(c.Operations)-1].Observation.Digest()
		}
		digest, e := op.Digest()
		if e != nil {
			t.Fatal("operation DATA", e)
		}
		proxy, pb, ab := failureSearchProxyData(t, original, pkg.Source, config, searchGrant, op, when)
		proxyDigest, e := proxy.Digest()
		if e != nil {
			t.Fatal(e)
		}
		f.proxies = append(f.proxies, proxy)
		f.proxyPolicies = append(f.proxyPolicies, pb)
		f.proxyAttestations = append(f.proxyAttestations, ab)
		accounting.Aggregate, e = accounting.Aggregate.Add(op.Debit)
		if e != nil {
			t.Fatal(e)
		}
		index := 0
		if op.Kind == domain.AttemptOperationWebSearchV1 {
			index = 3
		}
		accounting.Amounts[index] += op.Debit.AmountMicrounits
		accounting.UpdatedAt = when
		rsv := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: digest, ReservedAt: when, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, RegisteredAllocationDigest: od, RegisteredProxyDigest: proxyDigest}
		rd, e := rsv.Digest()
		if e != nil {
			t.Fatal("reservation DATA", e)
		}
		obs := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: digest, ReservationDigest: rd, Outcome: result, Reason: reason, ObservedAt: when.Add(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
		if op.Kind == domain.AttemptOperationModelV1 && op.Sequence > 1 && result != domain.AttemptOperationCompletedV1 && len(tailUsage) == 1 {
			obs.Usage = tailUsage[0].Clone()
		}
		if result == domain.AttemptOperationCompletedV1 {
			obs.ResponseRef = addBody(normalized)
			obs.ResponseDigest = rawLocalFailureHash(normalized)
			obs.ResponseBytes = uint64(len(normalized))
		} else {
			normalized = []byte("null")
		}
		if e := obs.ValidateForReservation(rsv); e != nil {
			t.Fatal("observation DATA", e)
		}
		c.Operations = append(c.Operations, localFailureOperationV1{Reservation: rsv, Observation: obs, Normalized: bytes.Clone(normalized)})
	}
	tools := []configuredinference.ToolV1{websearch.ModelToolV1()}
	frozen, err := configuredinference.FreezeRequestV1(config, config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "public documentation"}}, Tools: tools, MaxOutputTokens: 100})
	if err != nil {
		t.Fatal("original frozen catalog DATA", err)
	}
	request := frozen.Body()
	defer clear(request)
	var wireCatalog struct {
		Tools json.RawMessage `json:"tools"`
	}
	if json.Unmarshal(request, &wireCatalog) != nil {
		t.Fatal("catalog extraction")
	}
	modelWire := []byte(fmt.Sprintf(`{"model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"search-call-v4","type":"function","function":{"name":"web_search","arguments":"{\"query\":\"public compiler documentation\",\"max_results\":5}"}}]},"finish_reason":"tool_calls"}]}`, config.ModelID))
	normalized, err := frozen.NormalizeResponseBodyV1(modelWire, domain.AttemptOperationModelV1, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(normalized)
	model := domain.AttemptOperationV1{Version: 1, Scope: opScope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "model-one-v4", RequestRef: addBody(request), RequestDigest: rawLocalFailureHash(request), Deadline: at.Add(2 * time.Second), CleanupDuration: policy.Limits.MaxCleanupDuration, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1000, OutputTokens: 100, BodyBytes: uint64(len(request)) + (64 << 10), ResponseBytes: 64 << 10, ActiveDuration: time.Second}, RequestBytes: uint64(len(request)), ResultBytes: 64 << 10, ContextBytes: uint64(len(request)), Currency: pkg.Cost.Currency, PriceRevision: pkg.Cost.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest}
	appendOperation(model, normalized, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1, at.Add(time.Second))
	c.ModelCatalogs = append(c.ModelCatalogs, localFailureModelCatalogV4{Sequence: 1, Tools: bytes.Clone(wireCatalog.Tools)})
	decoded, err := configuredinference.DecodeSealedRetainedResponseV2(c.ProfileDigest, c.Operations[0].Reservation, c.Operations[0].Observation, normalized)
	if err != nil || decoded.Call == nil {
		t.Fatal("complete search call", err)
	}
	defer clear(decoded.Call.Arguments)
	args := decoded.Call.Arguments
	argumentRef, err := domain.AttemptOperationConsentArgumentsRefV1(opScope, uint64(len(args)), rawLocalFailureHash(args))
	if err != nil {
		t.Fatal(err)
	}
	f.blobs[argumentRef.Key] = bytes.Clone(args)
	consentRequest := domain.WebSearchConsentRequestV1{Version: 1, ID: "consent-failure-v4", Scope: ownedConsentScope(parent), InitiatingUserID: scope.OwnerUserID, ModelResponseDigest: rawLocalFailureHash(normalized), CallID: decoded.Call.ID, ArgumentsDigest: rawLocalFailureHash(args), QueryDigest: rawLocalFailureHash([]byte("public compiler documentation")), GrantDigest: pkg.Cost.WebSearchCost.ScopedGrantDigest, ConsentRevision: 1, RevocationGeneration: pkg.Cost.WebSearchCost.RevocationGeneration, Arguments: argumentRef, CreatedAt: at.Add(2 * time.Second), ExpiresAt: at.Add(30 * time.Second)}
	rd, err := consentRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	decision := domain.WebSearchConsentDecisionV1{Version: 1, Request: consentRequest, RequestDigest: rd, RequestRevision: 1, ActorUserID: scope.OwnerUserID, WebSessionDigest: domain.SecretDigest(strings.Repeat("e", 64)), MembershipSecurityVersion: 1, Choice: domain.WebSearchConsentApproveV1, ObservedAt: consentRequest.CreatedAt}
	state := domain.WebSearchConsentStateV1{Request: consentRequest, Revision: 2, Status: domain.WebSearchConsentApprovedV1, Decision: &decision, UpdatedAt: decision.ObservedAt}
	if stop == domain.SearchConsentStoppedV4 {
		state.Status = domain.WebSearchConsentDeniedV1
		state.Decision.Choice = domain.WebSearchConsentDenyV1
	}
	if err := state.Validate(); err != nil {
		t.Fatal("actual bounded consent DATA", err)
	}
	c.Consents = append(c.Consents, state)
	accounting.Aggregate, err = accounting.Aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: ownedConsentMetadataReservationBytes + uint64(len(args))})
	if err != nil {
		t.Fatal(err)
	}
	accounting.UpdatedAt = consentRequest.CreatedAt
	if stop != domain.SearchConsentStoppedV4 {
		dd, _ := decision.Digest()
		compiled, err := websearch.CompileV2(searchGrant, *pkg.Cost.WebSearchCost, websearch.BindingV1{Scope: opScope, Sequence: 2, CallID: decoded.Call.ID, ModelResponseDigest: rawLocalFailureHash(normalized), InitiatingUserID: scope.OwnerUserID, ParentCostCeilingDigestV2: pkg.Source.AdmissionCostCeilingDigest, ParentPriceRevision: pkg.Cost.PriceRevision}, args, at.Add(3*time.Second))
		if err != nil {
			t.Fatal("original pure SEARCH compiler DATA", err)
		}
		searchWire := compiled.Body()
		defer clear(searchWire)
		op := domain.AttemptOperationV1{Version: 1, Scope: opScope, Sequence: 2, Kind: domain.AttemptOperationWebSearchV1, CallID: decoded.Call.ID, RequestRef: addBody(searchWire), RequestDigest: rawLocalFailureHash(searchWire), ModelResponseDigest: rawLocalFailureHash(normalized), ArgumentsDigest: consentRequest.ArgumentsDigest, QueryDigest: consentRequest.QueryDigest, ToolGrantDigest: consentRequest.GrantDigest, ConsentRequestDigest: string(rd), ConsentDecisionDigest: string(dd), ConsentDecisionRef: string(consentRequest.ID), Deadline: at.Add(8 * time.Second), CleanupDuration: policy.Limits.MaxCleanupDuration, Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, WebSearches: 1, BodyBytes: uint64(len(searchWire)) + domain.WebSearchResultBytesV1, ResponseBytes: 256 << 10, AmountMicrounits: 10000, ActiveDuration: 5 * time.Second}, RequestBytes: uint64(len(searchWire)), ResultBytes: domain.WebSearchResultBytesV1, Currency: pkg.Cost.Currency, PriceRevision: pkg.Cost.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest, ExternalChargeEvidenceDigest: pkg.Cost.WebSearchCost.PriceEvidenceDigest, SearchWire: &domain.AttemptOperationSearchWireV1{Version: 1, APIProfile: domain.WebSearchAPIProfileV1, ArgumentsBytes: uint64(len(args)), CompilerArtifactDigest: strings.Repeat("f", 64)}}
		op.PredecessorObservationDigest, _ = c.Operations[0].Observation.Digest()
		operationDigest, err := op.Digest()
		if err != nil {
			t.Fatal(err)
		}
		resultOutcome, reason := outcome, domain.AttemptOperationReasonTransportUnknownV1
		if stop == domain.SearchServiceErrorV4 || stop == domain.ModelAfterSearchFailureV4 {
			resultOutcome = domain.AttemptOperationCompletedV1
			reason = domain.AttemptOperationReasonNoneV1
		}
		if stop == domain.SearchServiceErrorV4 {
			reason = domain.AttemptOperationReasonServiceErrorV1
		}
		typed := domain.WebSearchResultV1{Version: 1, Scope: opScope, OperationDigest: operationDigest, Sequence: 2, CallID: op.CallID, ProfileRevision: pkg.Source.Spending[3].ProfileRevision, GrantDigest: op.ToolGrantDigest, QueryDigest: op.QueryDigest, WireRequestDigest: op.RequestDigest, RawResponseDigest: strings.Repeat("a", 64), Provider: "tavily", RetrievedAt: at.Add(3 * time.Second), Outcome: "known_empty", Usage: domain.WebSearchUsageV1{State: "unknown"}, Sources: []domain.WebSearchSourceV1{}, Warnings: []domain.WebSearchWarningV1{}}
		if stop == domain.SearchServiceErrorV4 {
			typed.Outcome = "known_service_error"
		}
		body, err := domain.EncodeWebSearchResultV1(typed)
		if err != nil {
			t.Fatal(err)
		}
		appendOperation(op, body, resultOutcome, reason, at.Add(3*time.Second))
		if stop == domain.ModelAfterSearchFailureV4 {
			model.Sequence, model.CallID, model.Deadline = 3, "model-two-v4", at.Add(5*time.Second)
			appendOperation(model, nil, outcome, domain.AttemptOperationReasonTransportUnknownV1, at.Add(4*time.Second))
			c.ModelCatalogs = append(c.ModelCatalogs, localFailureModelCatalogV4{Sequence: 3, Tools: bytes.Clone(wireCatalog.Tools)})
		}
	}
	accounting.UpdatedAt = accounting.UpdatedAt.Add(time.Microsecond) // actual row timestamp is deliberately NOT the last reservation.
	c.Accounting = ports.OwnedOperationFailureAccountingV4{Version: 4, Scope: opScope, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, ConsentReservedBytes: ownedConsentMetadataReservationBytes + uint64(len(args))}
	_, c.Accounting.AccountingDigest, err = accountingBytes(accounting)
	if err != nil {
		t.Fatal(err)
	}
	c.Accounting.ConsentStatesDigest, err = localFailureV4Digest(c.Consents)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range c.Operations {
		row, e := ports.BuildOwnedOperationFailureRowV4(item.Reservation, item.Observation, parent, pkg.Source)
		if e != nil {
			t.Fatal("correlated row V4", e)
		}
		c.Accounting.Operations = append(c.Accounting.Operations, row)
	}
	c.Bodies, err = localFailureV4Bodies(c)
	if err != nil {
		t.Fatal(err)
	}
	c.FrontierDigest, err = localFailureV4Frontier(c)
	if err != nil {
		t.Fatal(err)
	}
	finish := c.CapturedAt.Add(time.Microsecond)
	run := domain.Run{ID: scope.RunID, TenantID: scope.TenantID, SessionID: scope.SessionID, TriggerEventID: "task-failure-v4", LocalCapacityAssociation: &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, ResourceID: p.Resource.ResourceID}, Status: domain.RunFailed, IdempotencyKey: "run-failure-v4", CreatedAt: at, UpdatedAt: finish, StartedAt: &at, FinishedAt: &finish}
	plan, err := localFailureV4Plan(run, c)
	if err != nil {
		t.Fatal("bounded V4 plan DATA", stop, err)
	}
	defer plan.Release()
	for i, event := range plan.Events() {
		draft, e := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, finish)
		if e != nil {
			t.Fatal(e)
		}
		identity := localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText}
		c.Events = append(c.Events, identity)
		record := domain.SessionEvent{ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i + 1), Kind: draft.Kind, RunID: &run.ID, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, CreatedAt: finish}
		f.records = append(f.records, sessioncontext.EventPayload{Event: record, Payload: bytes.Clone(event.Payload)})
		f.blobs[draft.Payload.Key] = bytes.Clone(event.Payload)
	}
	retired := original.Clone()
	retired.State, retired.RevocationGeneration, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, c.CapturedAt
	retiredDigest, err := retired.Digest()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	key, err := domain.TerminalCustodyObjectKeyV1(opScope, rawLocalFailureHash(raw))
	if err != nil {
		t.Fatal(err)
	}
	capsuleRef := domain.BlobRef{TenantID: opScope.TenantID, Key: key, Size: int64(len(raw)), SHA256: rawLocalFailureHash(raw)}
	f.blobs[capsuleRef.Key] = raw
	run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: opScope, FrontierDigest: c.FrontierDigest, Capsule: capsuleRef}}
	metadata, err := localFailureV4Metadata(run.TerminalCustody.Plan, c, string(retiredDigest))
	if err != nil {
		t.Fatal(err)
	}
	run.TerminalFailure = &metadata
	if err := run.Validate(); err != nil {
		t.Fatal("retained failed Run DATA", err)
	}
	seal, err := localFailureSeal(run, c.Events)
	if err != nil {
		t.Fatal(err)
	}
	cs := ownedConsentStates{Version: 1, ParentDigest: accounting.ParentDigest, States: c.Consents}
	f.history = localFailureHistoryV1{Run: run, Anchor: f.records[len(f.records)-1].Event, Parent: parent, Admission: admission, Accounting: accounting, Original: original, Retired: retired, Seal: seal, ConsentsV4: &cs}
	for i := 1; i < len(f.proxies); i++ {
		f.history.ProxiesV4 = append(f.history.ProxiesV4, localFailureProxyV4{Original: f.proxies[i], Current: f.proxies[i], PolicyBody: f.proxyPolicies[i], AttestationBody: f.proxyAttestations[i]})
	}
	f.capsule = c
	t.Cleanup(func() {
		for _, body := range f.blobs {
			clear(body)
		}
		for _, record := range f.records {
			clear(record.Payload)
		}
		clearFailureOperations(f.capsule.Operations)
	})
	return f
}

func TestLocalFailureSearchV4FiveClosedTails(t *testing.T) {
	for _, stop := range []string{domain.SearchConsentStoppedV4, domain.SearchRejectedBeforeSendV4, domain.SearchServiceErrorV4, domain.SearchOutcomeUnknownV4, domain.ModelAfterSearchFailureV4} {
		t.Run(stop, func(t *testing.T) {
			outcome := domain.AttemptOperationAcceptedOutcomeUnknownV1
			if stop == domain.SearchRejectedBeforeSendV4 {
				outcome = domain.AttemptOperationRejectedBeforeSendV1
			}
			f := failureSearchV4Data(t, stop, outcome)
			body := f.blobs[f.history.Run.TerminalCustody.Plan.Capsule.Key]
			decoded, err := decodeLocalFailureReadCapsule(body)
			if err != nil {
				t.Fatal("closed V4 dispatch", err)
			}
			defer decoded.release()
			if err := validateFailureReadHistoryData(f.history, decoded, f.records); err != nil {
				t.Fatal("exact original retained history DATA", err)
			}
			if _, err := decodeLocalFailureCapsule(body); err == nil {
				t.Fatal("old V1–3 reader silently widened")
			}
			if stop == domain.SearchConsentStoppedV4 && (len(f.capsule.Operations) != 1 || f.capsule.Accounting.AggregateDebit.WebSearches != 0 || len(f.records) != 1) {
				t.Fatal("stopped consent manufactured SEARCH execution")
			}
			for _, record := range f.records {
				if bytes.Contains(record.Payload, []byte("public compiler documentation")) || bytes.Contains(record.Payload, []byte("consent-failure-v4")) {
					t.Fatal("private query/receipt leaked in public failure projection")
				}
			}
		})
	}
}

// Rebind every public capsule/Run/seal hash in the controlled snapshot. These
// negative controls must reach the original semantic reader, not merely fail
// the blob SHA check. This cannot create a production finalizer or capability.
func rebindFailureSearchCapsuleData(t *testing.T, f *failureSearchV4Fixture, sql *failureSearchSQL, c localFailureCapsuleV4) {
	t.Helper()
	var err error
	c.FrontierDigest, err = localFailureV4Frontier(c)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	run := f.history.Run.Clone()
	key, err := domain.TerminalCustodyObjectKeyV1(c.Accounting.Scope, rawLocalFailureHash(body))
	if err != nil {
		t.Fatal(err)
	}
	run.TerminalCustody.Plan.Capsule = domain.BlobRef{TenantID: run.TenantID, Key: key, Size: int64(len(body)), SHA256: rawLocalFailureHash(body)}
	run.TerminalCustody.Plan.FrontierDigest = c.FrontierDigest
	metadata, err := localFailureV4Metadata(run.TerminalCustody.Plan, c, run.TerminalFailure.RetiredAllocationDigest)
	if err != nil {
		t.Fatal(err)
	}
	run.TerminalFailure = &metadata
	seal, err := localFailureSeal(run, c.Events)
	if err != nil {
		t.Fatal(err)
	}
	f.history.Run, f.history.Seal, f.capsule = run, seal, c
	f.blobs[key] = body
	sql.graph.tables["run_finalizations"][0]["content_digest"] = seal
}

func TestLocalFailureSearchV4StrictWireFromValidData(t *testing.T) {
	f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
	body := f.blobs[f.history.Run.TerminalCustody.Plan.Capsule.Key]
	for _, bad := range []string{strings.Replace(string(body), `"version":4`, `"version":3`, 1), strings.Replace(string(body), `"schema":"sessionless.terminal-failure-custody.v4"`, `"schema":"sessionless.terminal-failure-custody.v3"`, 1), strings.Replace(string(body), `"normalized":null`, `"normalized":{}`, 1), strings.Replace(string(body), `"normalized":null`, `"normalized":"null"`, 1), strings.Replace(string(body), `,"normalized":null`, "", 1), strings.Replace(string(body), `"schema":`, `"schema":null,"schema":`, 1), strings.Replace(string(body), `"model_catalogs":[`, `"model_catalogs":null,"extra":[`, 1), strings.Replace(string(body), `"consents":[`, `"Consents":[`, 1), strings.Replace(string(body), `"model_catalogs":[`, `"extra":true,"model_catalogs":[`, 1), string(body) + " "} {
		if bad == string(body) {
			t.Fatal("strict codec mutation made no change")
		}
		decoded, err := decodeLocalFailureReadCapsule([]byte(bad))
		decoded.release()
		if err == nil {
			t.Fatal("ambiguous/crossed V4 capsule parsed")
		}
	}
}
