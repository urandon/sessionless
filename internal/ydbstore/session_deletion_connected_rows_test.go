package ydbstore

import (
	"database/sql/driver"
	"errors"
	"time"
)

// Every historical decoder and full-postimage capture sees the SAME durable
// test snapshot. A query-specific frozen producer response must not hide a
// mutated physical marker, scalar, child range or accounting attachment.
func (f *retainedDeletionGraphFixture) originalRows(s string, a []driver.NamedValue) (driver.Rows, bool, error) {
	result := func(values [][]driver.Value, columns int) (driver.Rows, bool, error) {
		return &retainedDeletionWriterRows{rows: values, columns: columns}, true, nil
	}
	point := func(table string, args []driver.NamedValue, keys []string) (map[string]driver.Value, error) {
		rows, e := f.selectRows(table, args, keys)
		if e != nil {
			return nil, e
		}
		if len(rows) > 1 {
			return nil, errors.New("original physical point duplicated")
		}
		if len(rows) == 0 {
			return nil, nil
		}
		return rows[0], nil
	}
	flag := func(row map[string]driver.Value, key string) driver.Value {
		if row[key] != nil {
			return int64(1)
		}
		return int64(0)
	}
	text := func(row map[string]driver.Value, key string) driver.Value {
		if row[key] == nil {
			return ""
		}
		return row[key]
	}
	named := func(v ...driver.Value) []driver.NamedValue {
		out := make([]driver.NamedValue, len(v))
		for i, x := range v {
			out[i] = driver.NamedValue{Ordinal: i + 1, Value: x}
		}
		return out
	}
	parentSQL := s == ownedAttemptEffectParentSQL || s == ownedAttemptPreparationBurnSQL || s == ownedAllocationRegistrationSQL || s == ownedOperationAccountingSQL || s == ownedProxyRegistrationSQL || s == localTerminalHistorySQL
	if parentSQL {
		if len(a) < 3 {
			return nil, true, errors.New("original parent exact key arity")
		}
		p, e := point("attempt_effect_reservations", a[:3], []string{"tenant_id", "attempt_id", "kind"})
		if e != nil {
			return nil, true, e
		}
		if p == nil {
			return result(nil, map[string]int{ownedAttemptEffectParentSQL: 15, ownedAttemptPreparationBurnSQL: 5, ownedAllocationRegistrationSQL: 12, ownedOperationAccountingSQL: 4, ownedProxyRegistrationSQL: 12, localTerminalHistorySQL: 11}[s])
		}
		switch s {
		case ownedAttemptEffectParentSQL:
			var row []driver.Value
			for _, k := range []string{"tenant_id", "attempt_id", "kind", "run_id", "lease_id", "fence_token", "physical_invocation_claim_id", "effect_sequence", "invocation_authority_digest", "reserved_at"} {
				row = append(row, p[k])
			}
			row = append(row, flag(p, "record"), flag(p, "owned_record"), flag(p, "owned_digest"), text(p, "owned_record"), text(p, "owned_digest"))
			return result([][]driver.Value{row}, 15)
		case ownedAttemptPreparationBurnSQL:
			at := p["preparation_started_at"]
			if at == nil {
				at = time.Unix(0, 0).UTC()
			}
			present := int64(0)
			for _, k := range []string{"allocation_original_record", "allocation_original_digest", "allocation_current_record", "allocation_current_digest", "allocation_publication_digest", "allocation_transition_parent_digest"} {
				if p[k] != nil {
					present = 1
				}
			}
			return result([][]driver.Value{{flag(p, "preparation_started_at"), flag(p, "preparation_parent_digest"), at, text(p, "preparation_parent_digest"), present}}, 5)
		case ownedAllocationRegistrationSQL:
			keys := []string{"allocation_original_record", "allocation_original_digest", "allocation_current_record", "allocation_current_digest", "allocation_publication_digest", "allocation_transition_parent_digest"}
			var row []driver.Value
			for _, k := range keys {
				row = append(row, flag(p, k))
			}
			for _, k := range keys {
				row = append(row, text(p, k))
			}
			return result([][]driver.Value{row}, 12)
		case ownedOperationAccountingSQL:
			return result([][]driver.Value{{flag(p, "operation_accounting_record"), flag(p, "operation_accounting_digest"), text(p, "operation_accounting_record"), text(p, "operation_accounting_digest")}}, 4)
		case ownedProxyRegistrationSQL:
			keys := []string{"proxy_original_record", "proxy_original_digest", "proxy_current_record", "proxy_current_digest", "proxy_policy_body", "proxy_attestation_body"}
			var row []driver.Value
			for _, k := range keys {
				row = append(row, flag(p, k))
			}
			for _, k := range keys {
				row = append(row, text(p, k))
			}
			return result([][]driver.Value{row}, 12)
		case localTerminalHistorySQL:
			if len(a) != 4 {
				return nil, true, errors.New("original history manifest exact key")
			}
			ad, e := point("attempt_execution_admissions_v1", a[:2], []string{"tenant_id", "attempt_id"})
			if e != nil {
				return nil, true, e
			}
			ev, e := point("substrate_execution_evidence", a[:2], []string{"tenant_id", "attempt_id"})
			if e != nil {
				return nil, true, e
			}
			marker, e := point("run_finalizations", named(a[0].Value, p["run_id"]), []string{"tenant_id", "run_id"})
			if e != nil {
				return nil, true, e
			}
			manifest, e := point("artifact_manifests", named(a[0].Value, a[3].Value), []string{"tenant_id", "artifact_manifest_id"})
			if e != nil {
				return nil, true, e
			}
			if ad == nil || ev == nil || marker == nil || manifest == nil {
				return result(nil, 11)
			}
			return result([][]driver.Value{{p["owned_record"], p["owned_digest"], ad["record"], ad["digest"], ev["record"], ev["evidence_digest"], marker["terminal_status"], marker["content_digest"], manifest["payload"], p["operation_accounting_record"], p["operation_accounting_digest"]}}, 11)
		}
	}
	if s == ownedAdmissionSQL {
		p, e := point("attempt_execution_admissions_v1", a, []string{"tenant_id", "attempt_id"})
		if e != nil {
			return nil, true, e
		}
		if p == nil {
			return result(nil, 7)
		}
		return result([][]driver.Value{{p["record"], p["digest"], p["run_id"], p["session_id"], p["owner_user_id"], p["reservation_id"], p["admitted_at"]}}, 7)
	}
	if s == localCompactionOperationsSQL || s == retainedDeletionCallsSQL || s == localCompactionProxiesSQL || s == retainedDeletionReconciliationSQL {
		if len(a) != 2 {
			return nil, true, errors.New("original child exact parent scope")
		}
		table := map[string]string{localCompactionOperationsSQL: "attempt_operations_v1", retainedDeletionCallsSQL: "attempt_operation_calls_v1", localCompactionProxiesSQL: "attempt_operation_proxies_v1", retainedDeletionReconciliationSQL: "attempt_effect_reconciliation_evidence"}[s]
		rows, e := f.selectRows(table, a, []string{"tenant_id", "attempt_id"})
		if e != nil {
			return nil, true, e
		}
		var values [][]driver.Value
		columns := 0
		switch s {
		case localCompactionOperationsSQL:
			columns = 11
			for _, p := range rows {
				if len(values) >= 13 {
					break
				}
				calls, e := f.selectRows("attempt_operation_calls_v1", named(p["tenant_id"], p["attempt_id"], p["call_id"]), []string{"tenant_id", "attempt_id", "call_id"})
				if e != nil {
					return nil, true, e
				}
				seq, digest := driver.Value(int64(0)), driver.Value("")
				if len(calls) > 0 {
					seq, digest = calls[0]["sequence"], calls[0]["operation_digest"]
				}
				values = append(values, []driver.Value{p["sequence"], p["record"], p["digest"], p["operation_digest"], p["call_id"], flag(p, "observation_record"), flag(p, "observation_digest"), text(p, "observation_record"), text(p, "observation_digest"), seq, digest})
			}
		case retainedDeletionCallsSQL:
			columns = 3
			for _, p := range rows {
				if len(values) >= 13 {
					break
				}
				values = append(values, []driver.Value{p["call_id"], p["sequence"], p["operation_digest"]})
			}
		case localCompactionProxiesSQL:
			columns = 14
			keys := []string{"original_record", "original_digest", "current_record", "current_digest", "policy_body", "attestation_body"}
			for _, p := range rows {
				if len(values) >= 13 {
					break
				}
				row := []driver.Value{p["sequence"]}
				for _, k := range keys {
					row = append(row, flag(p, k))
				}
				for _, k := range keys {
					row = append(row, text(p, k))
				}
				row = append(row, p["service_kind"])
				values = append(values, row)
			}
		case retainedDeletionReconciliationSQL:
			columns = 6
			for _, p := range rows {
				if len(values) >= 10001 {
					break
				}
				values = append(values, []driver.Value{p["evidence_digest"], p["run_id"], p["physical_invocation_claim_id"], p["operation_state"], p["observed_at"], p["record"]})
			}
		}
		return result(values, columns)
	}
	return nil, false, nil
}
