package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpadmission"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

type failureSearchSQL struct {
	data       *failureSearchV4Fixture
	graph      *retainedDeletionGraphFixture
	statements []string
	writes     int
}
type failureSearchConn struct{ f *failureSearchSQL }

func (f *failureSearchSQL) Connect(context.Context) (driver.Conn, error) {
	return &failureSearchConn{f}, nil
}
func (*failureSearchSQL) Driver() driver.Driver { return ownedAdmissionJoinDriver{} }
func (*failureSearchConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unused prepared statement")
}
func (*failureSearchConn) Close() error              { return nil }
func (*failureSearchConn) Begin() (driver.Tx, error) { return ownedAdmissionJoinTx{}, nil }
func (*failureSearchConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return ownedAdmissionJoinTx{}, nil
}
func (c *failureSearchConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	c.f.writes++
	return nil, errors.New("historical fixture rejects every write")
}
func (c *failureSearchConn) QueryContext(_ context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	f.statements = append(f.statements, statement)
	v4 := strings.Replace(localFailureHistorySQL, "\n FROM", ",\n SUBSTRING(p.web_search_consent_record,0,32769),SUBSTRING(CAST(p.web_search_consent_digest AS String),0,65)\n FROM", 1)
	if statement == v4 {
		legacy, found, err := f.graph.originalRows(localFailureHistorySQL, args)
		if err != nil || !found {
			return nil, errors.New("exact original V4 join prerequisites")
		}
		defer legacy.Close()
		row := make([]driver.Value, 12)
		if err := legacy.Next(row); err != nil {
			return nil, err
		}
		p := f.graph.tables["attempt_effect_reservations"][0]
		row = append(row, p["web_search_consent_record"], p["web_search_consent_digest"])
		return &retainedDeletionWriterRows{rows: [][]driver.Value{row}, columns: 14}, nil
	}
	rows, found, err := f.graph.originalRows(statement, args)
	if found {
		return rows, err
	}
	return nil, errors.New("unexpected historical query: " + statement)
}

func failureSearchSQLData(t *testing.T, f *failureSearchV4Fixture) *failureSearchSQL {
	t.Helper()
	h := f.history
	p := h.Parent
	r := p.Reservation
	raw := func(v any) string {
		t.Helper()
		b, e := json.Marshal(v)
		if e != nil {
			t.Fatal(e)
		}
		return string(b)
	}
	pd, _ := p.Digest()
	ad, _ := h.Admission.Digest()
	od, _ := h.Original.Digest()
	rd, _ := h.Retired.Digest()
	intent, _ := ports.OwnedAllocationRegistrationIntentDigestV2(h.Original)
	accounting, accountingDigest, _ := accountingBytes(h.Accounting)
	consents, consentsDigest, e := ownedConsentBytes(*h.ConsentsV4, p, h.Run.TerminalFailure.CapturedAt)
	if e != nil {
		t.Fatal(e)
	}
	parent := map[string]driver.Value{"tenant_id": string(r.TenantID), "attempt_id": string(r.AttemptID), "kind": string(r.Kind), "run_id": string(r.RunID), "lease_id": string(r.LeaseID), "fence_token": int64(r.FenceToken), "physical_invocation_claim_id": r.PhysicalInvocationClaimID, "effect_sequence": int64(r.EffectSequence), "invocation_authority_digest": string(r.InvocationAuthorityDigest), "reserved_at": r.ReservedAt, "owned_record": raw(p), "owned_digest": pd, "preparation_started_at": h.Original.Window.ObservedAt, "preparation_parent_digest": pd, "allocation_original_record": raw(h.Original), "allocation_original_digest": string(od), "allocation_current_record": raw(h.Retired), "allocation_current_digest": string(rd), "allocation_publication_digest": intent, "allocation_transition_parent_digest": string(od), "operation_accounting_record": string(accounting), "operation_accounting_digest": accountingDigest, "web_search_consent_record": string(consents), "web_search_consent_digest": consentsDigest}
	graph := &retainedDeletionGraphFixture{tables: map[string][]map[string]driver.Value{}}
	graph.tables["attempt_effect_reservations"] = []map[string]driver.Value{parent}
	a := h.Admission
	graph.tables["attempt_execution_admissions_v1"] = []map[string]driver.Value{{"tenant_id": string(a.Source.Scope.TenantID), "attempt_id": string(a.Source.Scope.AttemptID), "record": raw(a), "digest": ad, "run_id": string(a.Source.Scope.RunID), "session_id": string(a.Source.Scope.SessionID), "owner_user_id": string(a.Source.Scope.OwnerUserID), "reservation_id": string(a.ReservationID), "admitted_at": a.AdmittedAt}}
	graph.tables["run_finalizations"] = []map[string]driver.Value{{"tenant_id": string(h.Run.TenantID), "run_id": string(h.Run.ID), "terminal_status": string(h.Run.Status), "content_digest": h.Seal}}
	for i, item := range f.capsule.Operations {
		rd, _ := item.Reservation.Digest()
		od, _ := item.Observation.Digest()
		op := item.Reservation.Operation
		graph.tables["attempt_operations_v1"] = append(graph.tables["attempt_operations_v1"], map[string]driver.Value{"tenant_id": string(op.Scope.TenantID), "attempt_id": string(op.Scope.AttemptID), "sequence": int64(op.Sequence), "record": raw(item.Reservation), "digest": string(rd), "operation_digest": string(item.Reservation.OperationDigest), "call_id": op.CallID, "observation_record": raw(item.Observation), "observation_digest": string(od)})
		graph.tables["attempt_operation_calls_v1"] = append(graph.tables["attempt_operation_calls_v1"], map[string]driver.Value{"tenant_id": string(op.Scope.TenantID), "attempt_id": string(op.Scope.AttemptID), "call_id": op.CallID, "sequence": int64(op.Sequence), "operation_digest": string(item.Reservation.OperationDigest)})
		proxy := f.proxies[i]
		digest, _ := proxy.Digest()
		prefix := ""
		if i == 0 {
			prefix = "proxy_"
		}
		row := map[string]driver.Value{prefix + "original_record": raw(proxy), prefix + "original_digest": string(digest), prefix + "current_record": raw(proxy), prefix + "current_digest": string(digest), prefix + "policy_body": string(f.proxyPolicies[i]), prefix + "attestation_body": string(f.proxyAttestations[i])}
		if i == 0 {
			for k, v := range row {
				parent[k] = v
			}
		} else {
			row["tenant_id"], row["attempt_id"], row["sequence"], row["service_kind"] = string(op.Scope.TenantID), string(op.Scope.AttemptID), int64(op.Sequence), string(proxy.ServiceKind)
			graph.tables["attempt_operation_proxies_v1"] = append(graph.tables["attempt_operation_proxies_v1"], row)
		}
	}
	return &failureSearchSQL{data: f, graph: graph}
}

func failureSearchQuery(t *testing.T, f *failureSearchSQL, fn func(context.Context, *ownedAuthorityQuery) error) error {
	t.Helper()
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	defer db.Close()
	ctx, cancel := ownedAuthorityCallContext(t.Context())
	defer cancel()
	budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
	callback, e := budget.begin()
	if e != nil {
		t.Fatal(e)
	}
	defer budget.finish(callback)
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	q := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback, at: f.data.history.Run.UpdatedAt}
	return fn(ctx, q)
}

func readFailureSearchHistoricalData(ctx context.Context, q *ownedAuthorityQuery, run domain.Run, anchor domain.SessionEvent) (localFailureHistoryV1, error) {
	v, err := readHistoricalFailure(ctx, q, run, anchor)
	if err == nil && run.TerminalFailure.CapsuleSchema == "sessionless.terminal-failure-custody.v4" && v.Accounting.Aggregate.Operations > 1 {
		v.ProxiesV4, err = readFailureV4ProxyRange(ctx, q, v)
	}
	return v, err
}

func TestLocalFailureSearchV4MountedHistoricalSQL(t *testing.T) {
	for _, stop := range []string{domain.SearchConsentStoppedV4, domain.SearchRejectedBeforeSendV4, domain.SearchServiceErrorV4, domain.SearchOutcomeUnknownV4, domain.ModelAfterSearchFailureV4} {
		t.Run(stop, func(t *testing.T) {
			outcome := domain.AttemptOperationAcceptedOutcomeUnknownV1
			if stop == domain.SearchRejectedBeforeSendV4 {
				outcome = domain.AttemptOperationRejectedBeforeSendV1
			}
			f := failureSearchV4Data(t, stop, outcome)
			sql := failureSearchSQLData(t, f)
			if err := failureSearchQuery(t, sql, func(ctx context.Context, q *ownedAuthorityQuery) error {
				v, e := readFailureSearchHistoricalData(ctx, q, f.history.Run, f.history.Anchor)
				if e != nil {
					return e
				}
				body := f.blobs[v.Run.TerminalCustody.Plan.Capsule.Key]
				c, e := decodeLocalFailureReadCapsule(body)
				if e != nil {
					return e
				}
				defer c.release()
				return validateFailureReadHistoryData(v, c, f.records)
			}); err != nil {
				t.Fatal("actual mounted historical SQL/capsule/canonical rejoin", err)
			}
			wanted := 2
			if stop == domain.SearchConsentStoppedV4 {
				wanted = 1
			}
			if len(sql.statements) != wanted || sql.writes != 0 {
				t.Fatal("history reader started another query or write", len(sql.statements), sql.writes)
			}
		})
	}
}

func TestLocalFailureSearchV4HistoricalSQLCrossBindings(t *testing.T) {
	for _, mode := range []string{"accounting_timestamp", "consent_snapshot", "foreign_claim", "seal", "retired", "canonical", "schema"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchConsentStoppedV4, domain.AttemptOperationCompletedV1)
			sql := failureSearchSQLData(t, f)
			parent := sql.graph.tables["attempt_effect_reservations"][0]
			switch mode {
			case "accounting_timestamp":
				v := f.history.Accounting
				v.UpdatedAt = v.UpdatedAt.Add(-1)
				b, d, _ := accountingBytes(v)
				parent["operation_accounting_record"], parent["operation_accounting_digest"] = string(b), d
			case "consent_snapshot":
				v := *f.history.ConsentsV4
				v.States = append([]domain.WebSearchConsentStateV1(nil), v.States...)
				v.States[0].Request.Scope.PhysicalInvocationClaimID = "foreign"
				b, _ := json.Marshal(v)
				parent["web_search_consent_record"], parent["web_search_consent_digest"] = string(b), rawLocalFailureHash(b)
			case "foreign_claim":
				parent["owned_record"] = strings.Replace(parent["owned_record"].(string), "claim-failure-v4", "foreign-claim", 1)
			case "seal":
				sql.graph.tables["run_finalizations"][0]["content_digest"] = "local_f1:" + strings.Repeat("a", 64)
			case "retired":
				parent["allocation_current_record"] = parent["allocation_original_record"]
			case "canonical":
				f.records[0].Payload = []byte(`{"invented":"not executed"}`)
			case "schema":
				f.history.Run.TerminalFailure.CapsuleSchema = "sessionless.terminal-failure-custody.v3"
			}
			if err := failureSearchQuery(t, sql, func(ctx context.Context, q *ownedAuthorityQuery) error {
				v, e := readFailureSearchHistoricalData(ctx, q, f.history.Run, f.history.Anchor)
				if e != nil {
					return e
				}
				c, e := decodeLocalFailureReadCapsule(f.blobs[f.history.Run.TerminalCustody.Plan.Capsule.Key])
				if e != nil {
					return e
				}
				defer c.release()
				return validateFailureReadHistoryData(v, c, f.records)
			}); err == nil {
				t.Fatalf("crossed historical %s admitted", mode)
			}
		})
	}
}

func TestLocalFailureSearchV4HistoricalExactChildProxySet(t *testing.T) {
	for _, mode := range []string{"missing", "foreign_grant", "extra", "consent_revision"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			sql := failureSearchSQLData(t, f)
			read := func() error {
				return failureSearchQuery(t, sql, func(ctx context.Context, q *ownedAuthorityQuery) error {
					v, err := readFailureSearchHistoricalData(ctx, q, f.history.Run, f.history.Anchor)
					if err != nil {
						return err
					}
					return validateFailureHistoryDataV4(v, f.capsule, f.records)
				})
			}
			if err := read(); err != nil {
				t.Fatal("valid original complete proxy range", err)
			}
			if mode == "consent_revision" {
				sql = rebindFailureSearchSemanticData(t, f, mode)
			} else if mode == "missing" {
				sql.graph.tables["attempt_operation_proxies_v1"] = nil
			} else if mode == "extra" {
				row := map[string]driver.Value{}
				for k, v := range sql.graph.tables["attempt_operation_proxies_v1"][0] {
					row[k] = v
				}
				row["sequence"] = int64(3)
				sql.graph.tables["attempt_operation_proxies_v1"] = append(sql.graph.tables["attempt_operation_proxies_v1"], row)
			} else {
				row := sql.graph.tables["attempt_operation_proxies_v1"][0]
				var proxy domain.RegisteredProxyAttestationV2
				if json.Unmarshal([]byte(row["original_record"].(string)), &proxy) != nil {
					t.Fatal("original proxy")
				}
				proxy.GrantDigest = strings.Repeat("b", 64)
				body, _ := json.Marshal(proxy)
				digest, _ := proxy.Digest()
				row["original_record"], row["current_record"], row["original_digest"], row["current_digest"] = string(body), string(body), string(digest), string(digest)
			}
			if err := read(); err == nil {
				t.Fatal("historical read selected a favorable/missing/crossed proxy grant", mode)
			}
			if sql.writes != 0 {
				t.Fatal("history read performed a write")
			}
		})
	}
}

// Explicit old retained DATA uses the existing nonheld grant/descriptor fixture
// and real configured/MCP encoders. It does not manufacture an original serving
// holder, finalizer issuance or physical cleanup receipt. Each original SQL
// tuple remains independently checked by the mounted historical reader.
func mixedLegacyFailureData(t *testing.T, version uint32, grant mcpadmission.MCPToolGrantV1) (*failureSearchV4Fixture, localFailureCapsule) {
	t.Helper()
	at := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	reader := newOwnedReaderFixtureAt(t, at, "-failure-v4")
	installation := syntheticLocalInstallationGrant(t, reader.profile)
	p, config := installation.Profile.Clone(), installation.LocalInstallation.Profile.Clone()
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: reader.scope.tenant, OwnerUserID: reader.scope.owner, SessionID: reader.scope.session, RunID: domain.RunID(fmt.Sprintf("mixed-old-run-%d", version)), AttemptID: domain.AttemptID(fmt.Sprintf("mixed-old-attempt-%d", version))}
	grant.TenantID, grant.OwnerUserID, grant.BillingOwnerUserID, grant.IssuedAt, grant.ExpiresAt = scope.TenantID, scope.OwnerUserID, p.BillingOwnerUserID, at, p.Window.ExpiresAt
	gd, err := grant.Digest()
	if err != nil {
		t.Fatal(err)
	}
	cost := p.CostCeiling.MCPToolCost
	cost.ResourceID, cost.ResourceRevision, cost.RevocationGeneration, cost.ProfileID, cost.ProfileRevision, cost.ScopedGrantDigest = grant.ResourceID, grant.ResourceRevision, grant.RevocationGeneration, grant.ProfileID, grant.ProfileRevision, gd
	p.MCPToolGrantDigest = gd
	p.Substrate.AdmissionCostCeilingDigest, _ = p.CostCeiling.Digest()
	var spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range []domain.AttemptSpendingComponentKindV1{domain.AttemptSpendingProviderV1, domain.AttemptSpendingSubstrateV1, domain.AttemptSpendingMCPToolV1, domain.AttemptSpendingWebSearchV1} {
		spending[i] = localRuntimeStructuralSpending(t, p, scope, kind)
	}
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(scope, p, spending, at)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := domain.BuildAttemptExecutionAdmissionV1(pkg.Source, domain.QuotaReservationID(fmt.Sprintf("mixed-quota-%d", version)), at)
	if err != nil {
		t.Fatal(err)
	}
	a := domain.ServerlessInvocationAuthorityV1{Version: 1, HarnessBinding: pkg.Binding, ExecutionPlacementV2: pkg.Placement, SubstrateBinding: pkg.Substrate, AdmissionCostCeiling: pkg.Cost, Lease: domain.Lease{ID: domain.LeaseID(fmt.Sprintf("mixed-lease-%d", version)), TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: "mixed-worker", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)}, ContextManifestDigest: strings.Repeat("a", 64), InputManifestDigest: strings.Repeat("b", 64), InvocationDeadline: at.Add(120 * time.Second)}
	parent, err := domain.BuildOwnedAttemptEffectParentV1(admission, a, fmt.Sprintf("mixed-claim-%d", version), nil, at)
	if err != nil {
		t.Fatal(err)
	}
	opScope := ownedOperationScope(parent)
	sd, _ := a.SubstrateBinding.Digest()
	prepared := domain.PreparedAllocationV2{Version: 2, SubstrateBindingDigest: sd, ObservedOuterHarnessDigest: p.Substrate.OuterHarnessArtifactDigest, ObservedProxyArtifactDigest: p.Substrate.EgressProxyArtifactDigest, ObservedProxyIdentityDigest: p.Substrate.EgressProxyIdentityDigest, WorkloadMode: domain.SubstrateWorkloadInProcessDirectV1, LocalRuntime: &domain.LocalRuntimeAttestationV2{ConfiguredProfileDigest: p.Backend.BackendProfileDigest, InstallationID: p.InstallationID, InstallationRevision: p.InstallationRevision, WorkerExecutableDigest: p.Substrate.OuterHarnessArtifactDigest, HostIdentityDigest: p.Substrate.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: p.Substrate.LocalNetworkNamespaceIdentityDigest, Limits: config.Limits}}
	original, err := domain.BuildRegisteredAllocationV2(a, parent.Reservation, prepared, domain.PreparedInvocationDigestV1(strings.Repeat("c", 64)), parent.PreparationExpiresAt, pkg.Source, fmt.Sprintf("mixed-allocation-%d", version), 1, domain.OwnedAgentAuthorityWindowV1{ObservedAt: at, PublishedAt: at, ExpiresAt: a.InvocationDeadline})
	if err != nil {
		t.Fatal(err)
	}
	allocationDigest, _ := original.Digest()
	pd, _ := parent.Digest()
	f := &failureSearchV4Fixture{blobs: map[string][]byte{}}
	accounting := ownedAttemptAccounting{Version: 1, ParentDigest: pd, SetupReservedAt: at, UpdatedAt: at, Aggregate: domain.AttemptOperationDebitV1{BodyBytes: ownedSetupBodyBytes(parent)}}
	accounting.Amounts[1] = *pkg.Source.Spending[1].MaxAmountMicrounits
	c := localFailureCapsule{Schema: fmt.Sprintf("sessionless.terminal-failure-custody.v%d", version), Version: version, Parent: parent, Source: pkg.Source, ProfileDigest: p.Backend.BackendProfileDigest, CapturedAt: at.Add(10 * time.Second), Cleanup: "joined_retained", Stop: domain.KnownMCPServiceErrorV1}
	if version == 2 {
		c.Stop = domain.ModelOutcomeUnknownV2
	} else if version == 3 {
		c.Stop = domain.MCPOutcomeUnknownV3
	}
	bodyRef := func(body []byte) string {
		t.Helper()
		token := fmt.Sprintf("opbody-v1.%d.%s", len(body), rawLocalFailureHash(body))
		ref, e := operationbody.RetainedOperationBodyRefV1(opScope, token, uint64(len(body)), rawLocalFailureHash(body))
		if e != nil {
			t.Fatal(e)
		}
		f.blobs[ref.Key] = append([]byte(nil), body...)
		return token
	}
	appendOperation := func(op domain.AttemptOperationV1, body []byte, outcome domain.AttemptOperationOutcomeV1, reason domain.AttemptOperationReasonV1) {
		t.Helper()
		when := at.Add(time.Duration(op.Sequence) * time.Second)
		if len(c.Operations) > 0 {
			op.PredecessorObservationDigest, _ = c.Operations[len(c.Operations)-1].Observation.Digest()
		}
		digest, e := op.Digest()
		if e != nil {
			t.Fatal(e)
		}
		accounting.Aggregate, e = accounting.Aggregate.Add(op.Debit)
		if e != nil {
			t.Fatal(e)
		}
		index := 0
		if op.Kind == domain.AttemptOperationMCPToolV1 {
			index = 2
		}
		accounting.Amounts[index] += op.Debit.AmountMicrounits
		accounting.UpdatedAt = when
		r := domain.AttemptOperationReservationV1{Version: 1, Operation: op, OperationDigest: digest, ReservedAt: when, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts, RegisteredAllocationDigest: allocationDigest, RegisteredProxyDigest: domain.RegisteredProxyAttestationDigestV2(strings.Repeat("d", 64))}
		rd, _ := r.Digest()
		o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: digest, ReservationDigest: rd, Outcome: outcome, Reason: reason, ObservedAt: when.Add(time.Microsecond), Usage: domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}}
		if outcome == domain.AttemptOperationCompletedV1 {
			o.ResponseRef, o.ResponseDigest, o.ResponseBytes = bodyRef(body), rawLocalFailureHash(body), uint64(len(body))
		} else {
			body = []byte("null")
		}
		if r.ValidateForParent(a, parent.Reservation) != nil || o.ValidateForReservation(r) != nil {
			t.Fatal("old operation correlations")
		}
		c.Operations = append(c.Operations, localFailureOperationV1{Reservation: r, Observation: o, Normalized: append([]byte(nil), body...)})
	}
	descriptor, err := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := configuredinference.FreezeRequestV1(config, config.Limits, configuredinference.RequestV1{Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "bounded original public docs"}}, Tools: []configuredinference.ToolV1{{Name: descriptor.Name(), Description: grant.ModelDescription, Parameters: descriptor.InputSchema()}}, MaxOutputTokens: 100})
	if err != nil {
		t.Fatal(err)
	}
	request := frozen.Body()
	wire, _ := json.Marshal(map[string]any{"model": config.ModelID, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "mixed-tool-call", "type": "function", "function": map[string]any{"name": descriptor.Name(), "arguments": `{"query":"public docs"}`}}}}, "finish_reason": "tool_calls"}}})
	normalized, err := frozen.NormalizeResponseBodyV1(wire, domain.AttemptOperationModelV1, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	model := domain.AttemptOperationV1{Version: 1, Scope: opScope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "mixed-model-one", RequestRef: bodyRef(request), RequestDigest: rawLocalFailureHash(request), Deadline: at.Add(6 * time.Second), CleanupDuration: p.Limits.MaxCleanupDuration, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1000, OutputTokens: 100, BodyBytes: uint64(len(request)) + (64 << 10), ResponseBytes: 64 << 10, ActiveDuration: 5 * time.Second}, RequestBytes: uint64(len(request)), ResultBytes: 64 << 10, ContextBytes: uint64(len(request)), Currency: pkg.Cost.Currency, PriceRevision: pkg.Cost.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest}
	appendOperation(model, normalized, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1)
	call, err := mcpadmission.BindConfiguredCallV1(config, grant, c.Operations[0].Reservation, c.Operations[0].Observation, request, normalized)
	if err != nil {
		t.Fatal(err)
	}
	toolRequest, err := call.Request()
	if err != nil {
		t.Fatal(err)
	}
	toolBody := toolRequest.Body()
	predecessor, _ := c.Operations[0].Observation.Digest()
	op := domain.AttemptOperationV1{Version: 1, Scope: opScope, Sequence: 2, Kind: domain.AttemptOperationMCPToolV1, CallID: call.ID(), RequestRef: bodyRef(toolBody), RequestDigest: rawLocalFailureHash(toolBody), PredecessorObservationDigest: predecessor, ModelResponseDigest: call.ModelResponseDigest(), ArgumentsDigest: call.ArgumentsDigest(), ToolGrantDigest: gd, Deadline: at.Add(7 * time.Second), CleanupDuration: p.Limits.MaxCleanupDuration, Debit: domain.AttemptOperationDebitV1{Operations: 1, ToolDispatches: 1, BodyBytes: uint64(len(toolBody)) + (64 << 10), ResponseBytes: 64 << 10, MCPOutboundMessages: 4, MCPCatalogPages: 1, MCPInboundFrames: 4, MCPInboundBytes: 64 << 10, AmountMicrounits: *pkg.Source.Spending[2].MaxAmountMicrounits, ActiveDuration: 5 * time.Second}, RequestBytes: uint64(len(toolBody)), ResultBytes: 64 << 10, Currency: pkg.Cost.Currency, PriceRevision: pkg.Cost.PriceRevision, AdmissionCostCeilingDigest: pkg.Source.AdmissionCostCeilingDigest, ExternalChargeEvidenceDigest: pkg.Cost.MCPToolCost.PriceEvidenceDigest}
	if version == 3 {
		appendOperation(op, nil, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1)
	} else {
		wire, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID(), "result": map[string]any{"content": []any{map[string]any{"type": "text", "text": "bounded legacy result"}}, "isError": version == 1}})
		result, e := toolRequest.DecodeToolResultV1(wire)
		if e != nil {
			t.Fatal(e)
		}
		body, e := mcpadmission.EncodeToolResultV1(call, op, result, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1})
		if e != nil {
			t.Fatal(e)
		}
		reason := domain.AttemptOperationReasonNoneV1
		if version == 1 {
			reason = domain.AttemptOperationReasonServiceErrorV1
		}
		appendOperation(op, body, domain.AttemptOperationCompletedV1, reason)
	}
	if version == 2 {
		model.Sequence, model.CallID, model.Deadline = 3, "mixed-model-two", at.Add(8*time.Second)
		appendOperation(model, nil, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonTransportUnknownV1)
	}
	_, accountingDigest, _ := accountingBytes(accounting)
	c.Accounting = ports.OwnedOperationFailureAccountingV1{Version: version, Scope: opScope, AccountingDigest: accountingDigest, AggregateDebit: accounting.Aggregate, ComponentAmounts: accounting.Amounts}
	for _, item := range c.Operations {
		row, e := ports.BuildOwnedOperationFailureRowV1(item.Reservation, item.Observation, parent, pkg.Source)
		if version == 2 {
			row, e = ports.BuildOwnedOperationFailureRowV2(item.Reservation, item.Observation, parent, pkg.Source)
		} else if version == 3 {
			row, e = ports.BuildOwnedOperationFailureRowV3(item.Reservation, item.Observation, parent, pkg.Source)
		}
		if e != nil {
			t.Fatal(e)
		}
		c.Accounting.Operations = append(c.Accounting.Operations, row)
	}
	c.FrontierDigest, err = localFailureFrontierDigest(parent, c.Accounting, c.Operations)
	if err != nil {
		t.Fatal(err)
	}
	c.Bodies, err = localFailureOperationBodies(c.Operations)
	if err != nil {
		t.Fatal(err)
	}
	finish := c.CapturedAt.Add(time.Microsecond)
	run := domain.Run{ID: scope.RunID, TenantID: scope.TenantID, SessionID: scope.SessionID, TriggerEventID: "mixed-task", LocalCapacityAssociation: &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: scope.TenantID, OwnerUserID: scope.OwnerUserID, ResourceID: p.Resource.ResourceID}, Status: domain.RunFailed, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("mixed-run-%d", version)), CreatedAt: at, UpdatedAt: finish, StartedAt: &at, FinishedAt: &finish}
	plan, err := localFailurePlan(run, domain.ProductLimits{MaxToolEvents: 8, MaxToolEventBytes: domain.MaxTerminalCapsuleBytesV1}, c)
	if err != nil {
		t.Fatal("old complete semantic plan", version, err)
	}
	defer plan.Release()
	for i, event := range plan.Events() {
		draft, e := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, finish)
		if e != nil {
			t.Fatal(e)
		}
		c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
		record := domain.SessionEvent{ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i + 1), Kind: draft.Kind, RunID: &run.ID, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, CreatedAt: finish}
		f.records = append(f.records, sessioncontext.EventPayload{Event: record, Payload: append([]byte(nil), event.Payload...)})
		f.blobs[draft.Payload.Key] = append([]byte(nil), event.Payload...)
	}
	retired := original.Clone()
	retired.State, retired.RevocationGeneration, retired.UpdatedAt = domain.RegisteredPreparationRetiredV1, 2, c.CapturedAt
	retiredDigest, _ := retired.Digest()
	body, err := encodeLocalFailureCapsule(c, domain.MaxTerminalCapsuleBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := domain.TerminalCustodyObjectKeyV1(opScope, rawLocalFailureHash(body))
	run.TerminalCustody = &domain.RunTerminalCustodyV1{Version: 1, Plan: domain.TerminalCustodyPlanV1{Scope: opScope, FrontierDigest: c.FrontierDigest, Capsule: domain.BlobRef{TenantID: scope.TenantID, Key: key, Size: int64(len(body)), SHA256: rawLocalFailureHash(body)}}}
	metadata, err := failureMetadata(run.TerminalCustody.Plan, c, string(retiredDigest))
	if err != nil {
		t.Fatal(err)
	}
	run.TerminalFailure = &metadata
	seal, err := localFailureSeal(run, c.Events)
	if err != nil {
		t.Fatal(err)
	}
	f.blobs[key] = body
	f.history = localFailureHistoryV1{Run: run, Anchor: f.records[len(f.records)-1].Event, Parent: parent, Admission: admission, Accounting: accounting, Original: original, Retired: retired, Seal: seal}
	return f, c
}

func TestLocalFailureSearchV4MountedMixedLegacyHistory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, original := completedOriginalMCPFixture(t)
		current := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
		sql := failureSearchSQLData(t, current)
		fixtures := []*failureSearchV4Fixture{}
		for version := uint32(1); version <= 3; version++ {
			old, _ := mixedLegacyFailureData(t, version, original.grant)
			fixtures = append(fixtures, old)
			h := old.history
			raw := func(v any) string {
				b, e := json.Marshal(v)
				if e != nil {
					t.Fatal(e)
				}
				return string(b)
			}
			pd, _ := h.Parent.Digest()
			ad, _ := h.Admission.Digest()
			od, _ := h.Original.Digest()
			rd, _ := h.Retired.Digest()
			accounting, digest, _ := accountingBytes(h.Accounting)
			sql.graph.tables["attempt_effect_reservations"] = append(sql.graph.tables["attempt_effect_reservations"], map[string]driver.Value{"tenant_id": string(h.Run.TenantID), "attempt_id": string(h.Parent.Scope.AttemptID), "kind": string(h.Parent.Reservation.Kind), "run_id": string(h.Run.ID), "owned_record": raw(h.Parent), "owned_digest": pd, "operation_accounting_record": string(accounting), "operation_accounting_digest": digest, "allocation_original_record": raw(h.Original), "allocation_original_digest": string(od), "allocation_current_record": raw(h.Retired), "allocation_current_digest": string(rd)})
			sql.graph.tables["attempt_execution_admissions_v1"] = append(sql.graph.tables["attempt_execution_admissions_v1"], map[string]driver.Value{"tenant_id": string(h.Run.TenantID), "attempt_id": string(h.Parent.Scope.AttemptID), "record": raw(h.Admission), "digest": ad})
			sql.graph.tables["run_finalizations"] = append(sql.graph.tables["run_finalizations"], map[string]driver.Value{"tenant_id": string(h.Run.TenantID), "run_id": string(h.Run.ID), "terminal_status": string(h.Run.Status), "content_digest": h.Seal})
		}
		fixtures = append(fixtures, current)
		var records []sessioncontext.EventPayload
		ids := []domain.RunID{}
		runs := map[domain.RunID]domain.Run{}
		markers, statuses := map[domain.RunID]string{}, map[domain.RunID]string{}
		byID := map[domain.RunID]*failureSearchV4Fixture{}
		for _, f := range fixtures {
			for i := range f.records {
				f.records[i].Event.Sequence = uint64(len(records) + 1)
				records = append(records, f.records[i])
			}
			f.history.Anchor = f.records[len(f.records)-1].Event
			id := f.history.Run.ID
			ids = append(ids, id)
			runs[id], markers[id], statuses[id], byID[id] = f.history.Run, f.history.Seal, string(domain.RunFailed), f
		}
		// Receiver supplies only the original collection's scope DATA; it is not
		// an original serving entry and cannot pass original()/dispatch guards.
		source := &LocalCompactionSourceV1{entry: &LocalInvocationEntryV1{parent: current.history.Parent}}
		before := map[domain.RunID][]byte{}
		for id, f := range byID {
			before[id] = append([]byte(nil), f.blobs[f.history.Run.TerminalCustody.Plan.Capsule.Key]...)
		}
		err := failureSearchQuery(t, sql, func(ctx context.Context, q *ownedAuthorityQuery) error {
			history, e := source.readFailureHistories(ctx, q, records, ids, runs, markers, statuses)
			if e != nil {
				return e
			}
			if len(history) != 4 {
				return errors.New("mixed collection dropped a retained format")
			}
			for i, h := range history {
				f := byID[h.Run.ID]
				body := f.blobs[h.Run.TerminalCustody.Plan.Capsule.Key]
				c, e := decodeLocalFailureReadCapsule(body)
				if e != nil {
					return e
				}
				e = validateFailureReadHistoryData(h, c, records)
				if c.legacy != nil {
					wire, ee := encodeLocalFailureCapsule(*c.legacy, domain.MaxTerminalCapsuleBytesV1)
					if ee != nil || !bytes.Equal(wire, before[h.Run.ID]) {
						c.release()
						return errors.New("mixed reader rewrote old bytes")
					}
					if c.legacy.Version != uint32(i+1) {
						c.release()
						return errors.New("mixed reader reinterpreted a legacy schema")
					}
				} else if i != 3 || c.search == nil {
					c.release()
					return errors.New("mixed reader lost V4 tagged arm")
				}
				c.release()
				if e != nil {
					return e
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal("actual mixed V1/V2/V3/V4 collection + original SQL/capsule/canonical joins", err)
		}
		if len(sql.statements) != 5 || sql.writes != 0 {
			t.Fatal("mixed collection added SQL or writes", len(sql.statements), sql.writes)
		}
	})
}
