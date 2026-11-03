package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type failureSearchBlobs struct {
	f     *failureSearchV4Fixture
	reads int
}

// Rebind the complete controlled physical graph, not just its outer capsule.
// All mutations preserve actual outcomes and reconstruct exact body debits. This helper
// deliberately does not call the semantic plan validator being tested.
func rebindFailureSearchSemanticData(t *testing.T, f *failureSearchV4Fixture, mode string) *failureSearchSQL {
	t.Helper()
	c := f.capsule
	old := append([]localFailureOperationV1(nil), c.Operations...)
	put := func(body []byte) string {
		t.Helper()
		hash := rawLocalFailureHash(body)
		token := fmt.Sprintf("opbody-v1.%d.%s", len(body), hash)
		ref, err := operationbody.RetainedOperationBodyRefV1(c.Accounting.Scope, token, uint64(len(body)), hash)
		if err != nil {
			t.Fatal(err)
		}
		f.blobs[ref.Key] = bytes.Clone(body)
		return token
	}
	if mode == "arguments_bytes" {
		wire := *c.Operations[1].Reservation.Operation.SearchWire
		wire.ArgumentsBytes++
		c.Operations[1].Reservation.Operation.SearchWire = &wire
	} else if mode == "catalog_only" || mode == "catalog_and_request" || mode == "unknown_unused_both" || mode == "supported_unused_both" || mode == "tools_case_alias" {
		before := c.ModelCatalogs[0].Tools
		if mode == "unknown_unused_both" || mode == "supported_unused_both" {
			var tools []json.RawMessage
			if json.Unmarshal(before, &tools) != nil {
				t.Fatal("original catalog array")
			}
			name := "unknown_unused"
			if mode == "supported_unused_both" {
				name = "search_openai_docs"
			}
			tools = append(tools, json.RawMessage(fmt.Sprintf(`{"type":"function","function":{"name":%q,"description":"unused immutable MCP advertisement DATA","parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false}}}`, name)))
			c.ModelCatalogs[0].Tools, _ = json.Marshal(tools)
		} else if mode != "tools_case_alias" {
			c.ModelCatalogs[0].Tools = bytes.Replace(before, []byte("Search public web sources for a query."), []byte("Search other! web sources for a query."), 1)
		}
		if mode != "tools_case_alias" && bytes.Equal(before, c.ModelCatalogs[0].Tools) {
			t.Fatal("catalog mutation was vacuous")
		}
		if mode != "catalog_only" {
			op := &c.Operations[0].Reservation.Operation
			ref, err := operationbody.RetainedOperationBodyRefV1(op.Scope, op.RequestRef, op.RequestBytes, op.RequestDigest)
			if err != nil {
				t.Fatal(err)
			}
			body := bytes.Replace(f.blobs[ref.Key], before, c.ModelCatalogs[0].Tools, 1)
			if mode == "tools_case_alias" {
				body = bytes.Replace(body, []byte(`"tools":`), []byte(`"TOOLS":`), 1)
			}
			if bytes.Equal(body, f.blobs[ref.Key]) || len(body) < int(op.RequestBytes) {
				t.Fatal("coherent catalog mutation must change the actual request")
			}
			delta := uint64(len(body)) - op.RequestBytes
			op.Debit.BodyBytes += delta
			op.ContextBytes += delta
			op.RequestBytes = uint64(len(body))
			for i := range c.Operations {
				c.Operations[i].Reservation.AggregateDebit.BodyBytes += delta
			}
			f.history.Accounting.Aggregate.BodyBytes += delta
			c.Accounting.AggregateDebit.BodyBytes += delta
			_, c.Accounting.AccountingDigest, err = accountingBytes(f.history.Accounting)
			if err != nil {
				t.Fatal("rebound actual accounting body charges", err)
			}
			prior := op.RequestDigest
			op.RequestRef, op.RequestDigest = put(body), rawLocalFailureHash(body)
			c.Operations[0].Normalized = bytes.Replace(c.Operations[0].Normalized, []byte(prior), []byte(op.RequestDigest), 1)
			obs := &c.Operations[0].Observation
			obs.ResponseRef, obs.ResponseDigest = put(c.Operations[0].Normalized), rawLocalFailureHash(c.Operations[0].Normalized)
			c.Consents[0].Request.ModelResponseDigest = obs.ResponseDigest
			c.Operations[1].Reservation.Operation.ModelResponseDigest = obs.ResponseDigest
		}
	} else if mode == "consent_revision" {
		c.Consents[0].Request.ConsentRevision++
	} else {
		t.Fatal("unknown semantic mutation", mode)
	}
	state := &c.Consents[0]
	d := *state.Decision
	d.Request = state.Request
	d.RequestDigest, _ = state.Request.Digest()
	state.Decision = &d
	op := &c.Operations[1].Reservation.Operation
	op.ConsentRequestDigest = string(d.RequestDigest)
	dd, _ := d.Digest()
	op.ConsentDecisionDigest = string(dd)
	if state.Validate() != nil {
		t.Fatal("mutation must preserve valid durable consent language")
	}
	var replacements [][2]string
	c.Accounting.Operations = nil
	for i := range c.Operations {
		item := &c.Operations[i]
		if i > 0 {
			item.Reservation.Operation.PredecessorObservationDigest, _ = c.Operations[i-1].Observation.Digest()
		}
		item.Reservation.OperationDigest, _ = item.Reservation.Operation.Digest()
		f.proxies[i].OperationDigest = item.Reservation.OperationDigest
		item.Reservation.RegisteredProxyDigest, _ = f.proxies[i].Digest()
		rd, err := item.Reservation.Digest()
		if err != nil {
			t.Fatal("coherently rebound reservation", mode, err)
		}
		item.Observation.OperationDigest, item.Observation.ReservationDigest = item.Reservation.OperationDigest, rd
		od, err := item.Observation.Digest()
		if err != nil || item.Observation.ValidateForReservation(item.Reservation) != nil {
			t.Fatal("coherently rebound observation", mode, err)
		}
		oldR, _ := old[i].Reservation.Digest()
		oldO, _ := old[i].Observation.Digest()
		replacements = append(replacements, [2]string{string(old[i].Reservation.OperationDigest), string(item.Reservation.OperationDigest)}, [2]string{string(oldR), string(rd)}, [2]string{string(oldO), string(od)})
		row, err := ports.BuildOwnedOperationFailureRowV4(item.Reservation, item.Observation, c.Parent, c.Source)
		if err != nil {
			t.Fatal(err)
		}
		c.Accounting.Operations = append(c.Accounting.Operations, row)
	}
	c.Accounting.ConsentStatesDigest, _ = localFailureV4Digest(c.Consents)
	f.history.ConsentsV4.States = c.Consents
	c.Bodies, _ = localFailureV4Bodies(c)
	c.Events = nil
	for i := range f.records {
		record := &f.records[i]
		body := bytes.Clone(record.Payload)
		for _, pair := range replacements {
			body = bytes.ReplaceAll(body, []byte(pair[0]), []byte(pair[1]))
		}
		draft, err := canonicaloutput.PlanEventDraftV1(f.history.Run, record.Event.ID, record.Event.Kind, body, record.Event.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		record.Event.Payload, record.Payload = draft.Payload, body
		f.blobs[draft.Payload.Key] = bytes.Clone(body)
		c.Events = append(c.Events, localCapsuleEventV1{ID: draft.ID, Kind: draft.Kind, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload, DisplayText: draft.DisplayText})
	}
	f.history.Anchor = f.records[len(f.records)-1].Event
	f.capsule = c
	sql := failureSearchSQLData(t, f)
	rebindFailureSearchCapsuleData(t, f, sql, c)
	return sql
}

func TestLocalFailureSearchV4RejectsCoherentCatalogLanguage(t *testing.T) {
	for _, mode := range []string{"unknown_unused_both", "tools_case_alias"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			if _, err := failureSearchReadDeletion(t, failureSearchSQLData(t, f), nil); err != nil {
				t.Fatal("valid full mounted baseline before catalog-language mutation", err)
			}
			original := f.capsule.Operations[0].Reservation.Operation.RequestDigest
			sql := rebindFailureSearchSemanticData(t, f, mode)
			op := f.capsule.Operations[0].Reservation.Operation
			ref, err := operationbody.RetainedOperationBodyRefV1(op.Scope, op.RequestRef, op.RequestBytes, op.RequestDigest)
			if err != nil || op.RequestDigest == original || !bytes.Contains(f.blobs[ref.Key], f.capsule.ModelCatalogs[0].Tools) || mode == "unknown_unused_both" && !bytes.Contains(f.capsule.ModelCatalogs[0].Tools, []byte(`"name":"unknown_unused"`)) || mode == "tools_case_alias" && (!bytes.Contains(f.blobs[ref.Key], []byte(`"TOOLS":`)) || bytes.Contains(f.blobs[ref.Key], []byte(`"tools":`))) {
				t.Fatal("actual request/capsule language mutation was vacuous", mode, err)
			}
			if _, err := failureSearchReadDeletion(t, sql, nil); err == nil {
				t.Fatal("fully rebound original request and capsule accepted unsupported catalog language", mode)
			}
		})
	}
}

func TestLocalFailureSearchV4RetainsOriginalUnusedSupportedMCPAdvertisement(t *testing.T) {
	f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
	if _, err := failureSearchReadDeletion(t, failureSearchSQLData(t, f), nil); err != nil {
		t.Fatal("original canonical SEARCH baseline", err)
	}
	sql := rebindFailureSearchSemanticData(t, f, "supported_unused_both")
	if !bytes.Contains(f.capsule.ModelCatalogs[0].Tools, []byte(`"name":"search_openai_docs"`)) {
		t.Fatal("missing supported unused slot")
	}
	if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
		t.Fatal("unchanged actual original unused MCP advertisement must not require hash inversion/current grant archive", err)
	}
}

func TestLocalFailureSearchV4RejectsCoherentSemanticContradictions(t *testing.T) {
	for _, mode := range []string{"arguments_bytes", "catalog_only", "catalog_and_request", "consent_revision"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			if _, err := failureSearchReadDeletion(t, failureSearchSQLData(t, f), nil); err != nil {
				t.Fatal("valid full mounted baseline before coherent mutation", err)
			}
			originalOperation := f.capsule.Operations[1].Reservation.OperationDigest
			originalCatalog := bytes.Clone(f.capsule.ModelCatalogs[0].Tools)
			sql := rebindFailureSearchSemanticData(t, f, mode)
			if mode == "arguments_bytes" && (f.capsule.Operations[1].Reservation.Operation.SearchWire.ArgumentsBytes == uint64(f.capsule.Consents[0].Request.Arguments.Size) || originalOperation == f.capsule.Operations[1].Reservation.OperationDigest) || (mode == "catalog_only" || mode == "catalog_and_request") && bytes.Equal(originalCatalog, f.capsule.ModelCatalogs[0].Tools) {
				t.Fatal("semantic contradiction was vacuous", mode)
			}
			if mode == "consent_revision" {
				p, _, err := ports.DecodeLocalSearchHTTPSProxyDataV1(f.proxyPolicies[1], f.proxyAttestations[1])
				if err != nil || p.Grant.ConsentRevision == f.capsule.Consents[0].Request.ConsentRevision {
					t.Fatal("revision mutation must retain the different actual SQL policy grant", err)
				}
			}
			if _, err := failureSearchReadDeletion(t, sql, nil); err == nil {
				t.Fatalf("fully rehashed physical contradiction accepted: %s", mode)
			}
		})
	}
}

func TestLocalFailureSearchV4AuthenticatesEveryOriginalModelCatalog(t *testing.T) {
	for _, mode := range []string{"unused_advertisement", "failed_model_two_omitted", "failed_model_two_empty"} {
		t.Run(mode, func(t *testing.T) {
			stop := domain.SearchOutcomeUnknownV4
			if mode != "unused_advertisement" {
				stop = domain.ModelAfterSearchFailureV4
			}
			f := failureSearchV4Data(t, stop, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			sql := failureSearchSQLData(t, f)
			if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
				t.Fatal("valid original catalog baseline", err)
			}
			c := f.capsule
			if mode == "unused_advertisement" {
				var tools []json.RawMessage
				if json.Unmarshal(c.ModelCatalogs[0].Tools, &tools) != nil {
					t.Fatal("original tool array")
				}
				tools = append(tools, json.RawMessage(`{"type":"function","function":{"name":"search_openai_docs","description":"unused MCP advertisement DATA","parameters":{"type":"object","properties":{},"required":[],"additionalProperties":false}}}`))
				c.ModelCatalogs[0].Tools, _ = json.Marshal(tools)
			} else if mode == "failed_model_two_omitted" {
				c.ModelCatalogs[1].Tools = nil
			} else {
				c.ModelCatalogs[1].Tools = json.RawMessage(`[]`)
			}
			// This is independently valid capsule/catalog language. Only its
			// missing correspondence to the actual owned MODEL request is false.
			c.FrontierDigest, _ = localFailureV4Frontier(c)
			plan, err := localFailureV4Plan(f.history.Run, c)
			if err != nil {
				t.Fatal("original-body join must be the causal guard", mode, err)
			}
			plan.Release()
			rebindFailureSearchCapsuleData(t, f, sql, c)
			if _, err := failureSearchReadDeletion(t, sql, nil); err == nil {
				t.Fatal("capsule catalog replaced actual original MODEL request", mode)
			}
		})
	}
}

func (b *failureSearchBlobs) Open(_ context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.reads++
	body, found := b.f.blobs[ref.Key]
	if !found || tenant != b.f.history.Run.TenantID {
		return nil, errors.New("missing/foreign retained body")
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

// Actual mounted original writer-inventory and body consumers under ONE
// existing logical budget, with complete physical SQL rows as controlled DATA.
// There is no alternate production hook, fabricated serving owner or erasure.
func failureSearchReadDeletion(t *testing.T, f *failureSearchSQL, counts *retainedDeletionAttemptRows) (int, error) {
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
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		budget.finish(callback)
		t.Fatal(e)
	}
	q := &ownedAuthorityQuery{tx: tx, budget: budget, callback: callback, at: f.data.history.Run.UpdatedAt}
	n := uint64(len(f.data.capsule.Operations))
	wanted := retainedDeletionAttemptRows{parents: 1, operations: n, calls: n, proxies: n - 1}
	if counts != nil {
		wanted = *counts
	}
	writer, e := readRetainedDeletionWriter(ctx, q, f.data.history.Run, f.data.history.Anchor, wanted)
	rollback := tx.Rollback()
	budget.finish(callback)
	if e != nil {
		return 0, e
	}
	if rollback != nil {
		return 0, rollback
	}
	ownership, e := newRetainedDeletionOwnershipData(f.data.history.Run.TenantID, f.data.history.Run.SessionID)
	if e != nil {
		return 0, e
	}
	if e = ownership.addRun(f.data.history.Run); e != nil {
		return 0, e
	}
	for _, record := range f.data.records {
		if e = ownership.addEvent(record.Event); e != nil {
			return 0, e
		}
	}
	blobs := &failureSearchBlobs{f: f.data}
	_, e = verifyRetainedDeletionFailureBodies(ctx, blobs, writer, ownership, nil)
	return blobs.reads, e
}

func TestLocalFailureSearchV4MountedFullDeletion(t *testing.T) {
	for _, stop := range []string{domain.SearchConsentStoppedV4, domain.SearchRejectedBeforeSendV4, domain.SearchServiceErrorV4, domain.SearchOutcomeUnknownV4, domain.ModelAfterSearchFailureV4} {
		t.Run(stop, func(t *testing.T) {
			outcome := domain.AttemptOperationAcceptedOutcomeUnknownV1
			if stop == domain.SearchRejectedBeforeSendV4 {
				outcome = domain.AttemptOperationRejectedBeforeSendV1
			}
			f := failureSearchV4Data(t, stop, outcome)
			sql := failureSearchSQLData(t, f)
			reads, err := failureSearchReadDeletion(t, sql, nil)
			if err != nil {
				t.Fatalf("actual full deletion SQL/proxy/call/reconciliation/body consumer stop=%s queries=%d last=%s reads=%d: %v", stop, len(sql.statements), sql.statements[len(sql.statements)-1], reads, err)
			}
			if sql.writes != 0 || reads != 1+len(f.records)+len(f.capsule.Bodies) {
				t.Fatal("read-only exact physical body inventory", reads, sql.writes)
			}
			for _, required := range []string{ownedAttemptEffectParentSQL, ownedAdmissionSQL, ownedAttemptPreparationBurnSQL, ownedAllocationRegistrationSQL, localCompactionOperationsSQL, retainedDeletionCallsSQL, ownedProxyRegistrationSQL, localCompactionProxiesSQL, retainedDeletionReconciliationSQL} {
				found := 0
				for _, statement := range sql.statements {
					if statement == required {
						found++
					}
				}
				if found != 1 {
					t.Fatalf("original required SQL join executions=%d want=1: %s", found, required)
				}
			}
		})
	}
}

func TestLocalFailureSearchV4ModelAfterSearchRejectedTail(t *testing.T) {
	f := failureSearchV4Data(t, domain.ModelAfterSearchFailureV4, domain.AttemptOperationRejectedBeforeSendV1)
	sql := failureSearchSQLData(t, f)
	if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
		t.Fatal("actual MODEL2 rejected-before-send reservation/observation", err)
	}
	last := f.capsule.Operations[len(f.capsule.Operations)-1]
	if last.Reservation.Operation.Kind != domain.AttemptOperationModelV1 || last.Observation.Outcome != domain.AttemptOperationRejectedBeforeSendV1 || last.Observation.ResponseRef != "" || string(last.Normalized) != "null" {
		t.Fatal("MODEL2 tail was replaced with prior search/final fiction")
	}
}

func TestLocalFailureSearchV4ModelAfterSearchPreservesReportedOverrun(t *testing.T) {
	input := uint64(1001)
	f := failureSearchV4Data(t, domain.ModelAfterSearchFailureV4, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageProviderReportedV1, InputTokens: &input})
	sql := failureSearchSQLData(t, f)
	if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
		t.Fatal("actual MODEL2 reported overrun must remain retained, not erased/refunded", err)
	}
	last := f.capsule.Accounting.Operations[len(f.capsule.Accounting.Operations)-1]
	if !last.Usage.ExceedsReservation(last.Reserved) || last.Usage.InputTokens == nil || *last.Usage.InputTokens != input || last.Usage.AmountMicrounits != nil || f.capsule.Accounting.ComponentAmounts[3] != 10000 {
		t.Fatal("MODEL2 usage lost provenance or SEARCH reservation refunded")
	}
}

func TestLocalFailureSearchV4RehashedCapsuleSemanticDenial(t *testing.T) {
	for _, mode := range []string{"unknown_body", "missing_catalog", "wrong_catalog_call", "extra_body", "crossed_consent", "invented_search_receipt"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			sql := failureSearchSQLData(t, f)
			if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
				t.Fatal("full positive before rehashed mutation", err)
			}
			body, _ := json.Marshal(f.capsule)
			var c localFailureCapsuleV4
			if json.Unmarshal(body, &c) != nil {
				t.Fatal("capsule mutation clone")
			}
			switch mode {
			case "unknown_body":
				c.Operations[len(c.Operations)-1].Normalized = json.RawMessage(`{}`)
			case "missing_catalog":
				c.ModelCatalogs = []localFailureModelCatalogV4{}
			case "wrong_catalog_call":
				c.ModelCatalogs[0].Tools = bytes.Replace(c.ModelCatalogs[0].Tools, []byte(`"name":"web_search"`), []byte(`"name":"other_search"`), 1)
			case "extra_body":
				ref := c.Bodies[0]
				ref.Key += "-invented"
				c.Bodies = append(c.Bodies, ref)
			case "crossed_consent":
				c.Consents[0].Request.Scope.PhysicalInvocationClaimID = "foreign-claim"
			case "invented_search_receipt":
				c.Operations[1].Reservation.Operation.ConsentDecisionRef = "invented-receipt"
			}
			rebindFailureSearchCapsuleData(t, f, sql, c)
			if _, err := failureSearchReadDeletion(t, sql, nil); err == nil {
				t.Fatalf("semantically crossed but fully rehashed %s accepted", mode)
			}
		})
	}
}

func TestLocalFailureSearchV4FullDeletionRejectsCrossedPhysicalRows(t *testing.T) {
	for _, mode := range []string{"missing_call", "extra_call", "call_operation", "missing_proxy", "proxy_operation", "proxy_grant", "proxy_policy", "proxy_kind", "extra_reconciliation", "ordinary_body", "consent_body", "consent_frontier", "accounting_timestamp", "capsule_null", "model_catalog"} {
		t.Run(mode, func(t *testing.T) {
			f := failureSearchV4Data(t, domain.SearchOutcomeUnknownV4, domain.AttemptOperationAcceptedOutcomeUnknownV1)
			sql := failureSearchSQLData(t, f)
			if _, err := failureSearchReadDeletion(t, sql, nil); err != nil {
				t.Fatal("unmutated full mounted baseline", mode, err)
			}
			switch mode {
			case "missing_call":
				sql.graph.tables["attempt_operation_calls_v1"] = sql.graph.tables["attempt_operation_calls_v1"][:1]
			case "extra_call":
				row := map[string]driver.Value{}
				for k, v := range sql.graph.tables["attempt_operation_calls_v1"][0] {
					row[k] = v
				}
				row["call_id"] = "extra-call"
				sql.graph.tables["attempt_operation_calls_v1"] = append(sql.graph.tables["attempt_operation_calls_v1"], row)
			case "call_operation":
				sql.graph.tables["attempt_operation_calls_v1"][1]["operation_digest"] = strings.Repeat("b", 64)
			case "missing_proxy":
				sql.graph.tables["attempt_operation_proxies_v1"] = nil
			case "proxy_operation", "proxy_grant", "proxy_kind":
				row := sql.graph.tables["attempt_operation_proxies_v1"][0]
				var proxy domain.RegisteredProxyAttestationV2
				if json.Unmarshal([]byte(row["original_record"].(string)), &proxy) != nil {
					t.Fatal("proxy mutation prerequisite")
				}
				if mode == "proxy_operation" {
					proxy.OperationDigest = domain.AttemptOperationDigestV1(strings.Repeat("b", 64))
				} else if mode == "proxy_grant" {
					proxy.GrantDigest = f.history.Admission.Source.Spending[0].ScopedGrantDigest
				} else {
					proxy.ServiceKind = domain.RegisteredProxyMCPToolV1
				}
				b, _ := json.Marshal(proxy)
				d, _ := proxy.Digest()
				row["original_record"], row["current_record"], row["original_digest"], row["current_digest"] = string(b), string(b), string(d), string(d)
			case "proxy_policy":
				row := sql.graph.tables["attempt_operation_proxies_v1"][0]
				row["policy_body"] = strings.Replace(row["policy_body"].(string), `"automatic_retries_denied":true`, `"automatic_retries_denied":false`, 1)
			case "extra_reconciliation":
				sql.graph.tables["attempt_effect_reconciliation_evidence"] = []map[string]driver.Value{{"tenant_id": string(f.history.Run.TenantID), "attempt_id": string(f.history.Parent.Scope.AttemptID), "evidence_digest": strings.Repeat("b", 64), "run_id": string(f.history.Run.ID), "physical_invocation_claim_id": f.history.Parent.Reservation.PhysicalInvocationClaimID, "operation_state": "unknown", "observed_at": f.history.Run.UpdatedAt, "record": "{}"}}
			case "ordinary_body":
				ref := f.capsule.Bodies[0]
				f.blobs[ref.Key] = []byte("changed ordinary body")
			case "consent_body":
				f.blobs[f.capsule.Consents[0].Request.Arguments.Key] = []byte("changed consent body")
			case "consent_frontier":
				row := sql.graph.tables["attempt_operations_v1"][1]
				var r domain.AttemptOperationReservationV1
				_ = json.Unmarshal([]byte(row["record"].(string)), &r)
				r.AggregateDebit.BodyBytes -= ownedConsentMetadataReservationBytes + uint64(f.capsule.Consents[0].Request.Arguments.Size)
				b, _ := json.Marshal(r)
				d, _ := r.Digest()
				row["record"], row["digest"] = string(b), string(d)
			case "accounting_timestamp":
				v := f.history.Accounting
				v.UpdatedAt = v.UpdatedAt.Add(-1)
				b, d, _ := accountingBytes(v)
				row := sql.graph.tables["attempt_effect_reservations"][0]
				row["operation_accounting_record"], row["operation_accounting_digest"] = string(b), d
			case "capsule_null":
				key := f.history.Run.TerminalCustody.Plan.Capsule.Key
				f.blobs[key] = bytes.Replace(f.blobs[key], []byte(`"normalized":null`), []byte(`"normalized":{}`), 1)
			case "model_catalog":
				key := f.history.Run.TerminalCustody.Plan.Capsule.Key
				f.blobs[key] = bytes.Replace(f.blobs[key], []byte(`"model_catalogs":[`), []byte(`"model_catalogs":null,"extra":[`), 1)
			}
			if _, err := failureSearchReadDeletion(t, sql, nil); err == nil {
				t.Fatalf("crossed full deletion %s accepted", mode)
			}
			if sql.writes != 0 {
				t.Fatal("historical negative performed a write")
			}
		})
	}
}
