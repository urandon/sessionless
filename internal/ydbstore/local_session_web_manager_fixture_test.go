//go:build sessionless_mcp_offline

package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlifecycle"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// This is installation DATA, not the rich running fixture. Only the original
// profile is published. In particular no Attempt or spending grant exists.
func newWebManagerInstallation(t *testing.T, endpoint string) *originalLocalRecoveryFixture {
	t.Helper()
	root, _, manifest := localOperatorFiles(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	exe, err = filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := localoperator.InspectRuntime(t.Context(), exe)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := localoperator.LinkedConnectorIdentity(identity.WorkerExecutableDigest)
	if err != nil {
		t.Fatal(err)
	}
	c, p := &manifest.Configuration, &manifest.Profile
	c.BaseURL, manifest.InvoiceTerms.BaseURL = endpoint, endpoint
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.OuterHarnessArtifactDigest, p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.EgressProxyArtifactDigest, p.Substrate.EgressProxyIdentityDigest = identity.WorkerExecutableDigest, connector
	p.CostCeiling.MaxPreEffectDurationPerDelivery = 10 * time.Second
	c.Limits.OperationTimeout, p.Limits.MaxNetworkOperationDuration = 5*time.Second, 5*time.Second
	p.InstructionVersion, p.CompactionPolicyDigest = compactioncontext.InstructionVersionV1, compactioncontext.CompactionPolicyDigestV1()
	calibration, err := json.Marshal(map[string]any{"version": 1, "algorithm": "utf8_affine.v1", "input_domain": "chat_completions_text.v1", "tenant_id": c.TenantID, "owner_user_id": c.OwnerUserID, "model_vendor_id": c.ModelVendorID, "model_id": c.ModelID, "response_model_ids": c.ResponseModelIDs, "server_id": c.ServerID, "server_version": c.ServerVersion, "protocol": c.NativeProtocolVersion, "chat_template_digest": c.ChatTemplateDigest, "estimator_version": c.TokenEstimatorVersion, "capability_evidence_digest": c.CapabilityEvidence.Digest, "model_window_tokens": c.ModelWindowTokens, "max_wire_bytes": c.Limits.RequestBytes, "max_messages": c.Limits.Messages, "max_tools": c.Limits.Tools, "byte_numerator": 1, "byte_denominator": 1, "fixed_tokens": 1, "message_tokens": 1, "tool_tokens": 1})
	if err != nil {
		t.Fatal(err)
	}
	body := func(ref string, raw []byte) localoperator.EvidenceBodyV1 {
		sum := sha256.Sum256(raw)
		return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: ref, Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw))}, Body: raw}
	}
	evidence := body("synthetic-compiled-calibration", calibration)
	c.CalibrationDigest, p.CalibrationDigest = evidence.Reference.Digest, evidence.Reference.Digest
	manifest.Evidence = append(manifest.Evidence, evidence)
	invoice, err := json.Marshal(manifest.InvoiceTerms)
	if err != nil {
		t.Fatal(err)
	}
	evidence = body(c.OperatorTerms.Ref, invoice)
	for i := range manifest.Evidence {
		if manifest.Evidence[i].Reference.Ref == c.OperatorTerms.Ref {
			manifest.Evidence[i] = evidence
		}
	}
	c.OperatorTerms = evidence.Reference
	p.ProviderPriceDigest, p.ProviderTermsDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest = evidence.Reference.Digest, evidence.Reference.Digest, evidence.Reference.Digest, evidence.Reference.Digest
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(*c)
	if err != nil {
		t.Fatal(err)
	}
	p.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	observed, expires := p.CostCeiling.PriceObservedAt, p.CostCeiling.PriceExpiresAt
	localSearchDeclaration(t, &manifest)
	localSourceMCPDeclaration(t, &manifest, "local fixture MODEL produces a bounded MCP call then its final answer")
	if p.CostCeiling.PriceObservedAt.Before(observed) {
		p.CostCeiling.PriceObservedAt = observed
	}
	if p.CostCeiling.PriceExpiresAt.After(expires) {
		p.CostCeiling.PriceExpiresAt = expires
	}
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	writeLocalOperatorManifest(t, root, manifest)
	source, err := localoperator.OpenFresh(t.Context(), root, exe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	publication, err := source.Publication()
	if err != nil {
		t.Fatal(err)
	}
	store, f, _, _ := newLocalInstallationFixture(t)
	f.at = publication.IssuedAt
	f.fundingKeys = make(map[string]bool)
	store.ownedProfilePublicationVerifier, store.attemptSpendingPublicationVerifier, store.ownedAttemptEffectSourceVerifier = source, source, source
	receipt, err := store.PublishOwnedProfile(t.Context(), publication)
	if err != nil {
		t.Fatal(err)
	}
	if err = source.AcceptReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 {
		t.Fatal("installation published Attempt spending")
	}
	return &originalLocalRecoveryFixture{root: root, exe: exe, source: source, store: store, sql: f, receipt: receipt}
}

// Transparent observation of ACTUAL service requests only registers exact SQL
// test inventory. The real binder/Store still produce and validate authority.
type webManagerBinder struct {
	ports.HarnessBinder
	f *webManagerSQL
}

func (b webManagerBinder) BindHarness(ctx context.Context, r ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	for _, kind := range ownedSpendingKinds {
		b.f.install.sql.fundingKeys[localInstallationSQLKey([]driver.Value{string(r.TenantID), string(r.AttemptID), string(kind), string(b.f.install.receipt.Profile.BillingOwnerUserID)})] = true
	}
	a, err := b.HarnessBinder.BindHarness(ctx, r)
	if err != nil {
		b.f.lastError = "actual binder: " + err.Error()
	}
	return a, err
}

type webManagerSQL struct {
	*localCanonicalJoinSQL
	install                  *originalLocalRecoveryFixture
	scope                    domain.AttemptExecutionSourceScopeV1
	bindings                 int
	bindingIndex, bindingKey []driver.Value
	manager                  *canonicalManagerSQL
	sessionPeer              *canonicalSessionSQL
	lastError                string
	lastQuery                string
	callStatements           int
	callTrace                []string
	lastCommitTrace          []string
	priorFailure             *retainedDeletionGraphFixture
	priorRecords             map[string]string
	priorAttempt             string
	modelTailFault           string
	modelTailFaults          int
}

func (f *webManagerSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *webManagerSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	a, e := f.install.sql.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	var b driver.Tx
	if f.manager != nil {
		b, e = f.manager.BeginTx(ctx, o)
	} else if f.sessionPeer != nil {
		b, e = f.sessionPeer.BeginTx(ctx, o)
	} else {
		b, e = f.localCanonicalJoinSQL.BeginTx(ctx, o)
	}
	if e != nil {
		_ = a.Rollback()
		return nil, e
	}
	return &webManagerTx{a: a, b: b, f: f, index: append([]driver.Value(nil), f.bindingIndex...), key: append([]driver.Value(nil), f.bindingKey...), bindings: f.bindings}, nil
}

type webManagerTx struct {
	a, b       driver.Tx
	f          *webManagerSQL
	index, key []driver.Value
	bindings   int
	done       bool
}

func (t *webManagerTx) restore() {
	t.f.bindingIndex = append([]driver.Value(nil), t.index...)
	t.f.bindingKey = append([]driver.Value(nil), t.key...)
	t.f.bindings = t.bindings
}
func (t *webManagerTx) Commit() error {
	mode := ""
	if len(t.f.commitModes) > 0 {
		mode = t.f.commitModes[0]
	}
	if t.f.manager != nil && t.f.manager.completionFault != "" {
		mode = t.f.manager.completionFault
	}
	if err := t.b.Commit(); err != nil {
		t.done = true
		if mode == "rollback" {
			_ = t.a.Rollback()
			t.restore()
		} else {
			if e := t.a.Commit(); e != nil {
				return errors.Join(err, e)
			}
		}
		return err
	}
	err := t.a.Commit()
	t.done = true
	return err
}
func (t *webManagerTx) Rollback() error {
	if !t.done {
		t.restore()
		t.done = true
	}
	a := t.a.Rollback()
	b := t.b.Rollback()
	return errors.Join(a, b)
}

func (f *webManagerSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (rows driver.Rows, err error) {
	defer func() {
		if err != nil {
			f.lastError = err.Error()
		}
	}()
	flat := canonicalJoinSQL(s)
	if s == "SELECT CurrentUtcTimestamp()" {
		f.callStatements = 0
		f.callTrace = nil
	}
	f.callStatements++
	if len(f.callTrace) < 40 {
		f.callTrace = append(f.callTrace, flat)
	}
	f.lastQuery = flat
	if f.modelTailFault != "" && f.manager != nil {
		effects := f.manager.seed.peer.effects
		if len(effects.operations) == 3 {
			last := effects.operations[2]
			observed := len(last) == 11 && last[5] == uint64(1) && last[6] == uint64(1) && last[7] != nil
			if observed && s == ownedOperationReadSQL && len(args) == 3 && args[2].Value == int64(3) && f.modelTailFault == "missing_readback" {
				f.modelTailFaults++
				return &explanationFixtureRows{}, nil
			}
			if observed && s == localCompactionOperationsSQL && (f.modelTailFault == "missing_tail" || f.modelTailFault == "changed_tail") {
				f.modelTailFaults++
				rows := append([][]driver.Value(nil), effects.operations...)
				if f.modelTailFault == "missing_tail" {
					rows = rows[:2]
				} else {
					rows[2] = append([]driver.Value(nil), rows[2]...)
					var o domain.AttemptOperationObservationV1
					if json.Unmarshal([]byte(rows[2][7].(string)), &o) != nil {
						return nil, errors.New("original MODEL tail fault data")
					}
					o.Reason = domain.AttemptOperationReasonTransportUnknownV1
					raw, e := json.Marshal(o)
					digest, de := o.Digest()
					if e != nil || de != nil {
						return nil, errors.New("changed MODEL tail must remain valid DATA")
					}
					rows[2][7], rows[2][8] = string(raw), string(digest)
				}
				return &compactionSourceRows{rows: rows}, nil
			}
		}
	}
	if f.priorFailure != nil && f.manager != nil && s == localTerminalHistoryRunsSQL(len(args)-3) {
		if err := f.manager.seed.loop.countStatement(ctx); err != nil {
			return nil, err
		}
		return f.manager.seed.peer.effects.compactionSourceSQL.QueryContext(ctx, s, args)
	}
	if f.priorFailure != nil && s == quotaReservationStorageSQLV1 {
		// The old lifecycle fixture has a literal first-Run key. A new Run must
		// use the SAME physical quota decoder with its actual persisted key,
		// never reuse or rewrite the retained first reservation.
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return f.managedAdmissionGraphFixture.QueryContext(ctx, s, args)
	}
	if f.priorFailure != nil && flat == "SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2" {
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return f.localCanonicalJoinSQL.QueryContext(ctx, s, args)
	}
	if s == ownedSessionMaterialPredecessorSQL {
		if ctx.Err() != nil || len(args) != 3 || args[0].Value != string(f.scope.TenantID) || args[1].Value != string(f.scope.SessionID) {
			return nil, errors.New("Web material predecessor exact Session")
		}
		sequence, ok := args[2].Value.(int64)
		if !ok || sequence < 1 || sequence > 4096 {
			return nil, errors.New("Web material predecessor bounded exact sequence")
		}
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		var rows [][]driver.Value
		if current := f.rows["session_material_current_v1"]; len(current) != 0 {
			if len(current) != 7 {
				return nil, errors.New("Web material current physical shape")
			}
			rows = append(rows, append([]driver.Value{int64(0)}, current...))
		}
		if boundary := f.rows["session_material_boundaries_v1"]; len(boundary) != 0 {
			if len(boundary) != 7 {
				return nil, errors.New("Web material boundary physical shape")
			}
			if retainedDeletionFixtureEqual(boundary[2], sequence) {
				rows = append(rows, append([]driver.Value{int64(1)}, boundary...))
			}
		}
		return &compactionSourceRows{rows: rows}, nil
	}
	if flat == "SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM attempts WHERE tenant_id = $1 AND attempt_id = $2" && len(args) == 2 {
		scope := "foreign"
		if args[1].Value == string(f.request.AttemptID) {
			scope = "current"
		}
		if f.priorFailure != nil && args[1].Value == f.priorFailure.tables["attempt_execution_admissions_v1"][0]["attempt_id"] {
			scope = "prior"
		}
		f.lastQuery += " [scope=" + scope + "]"
	}
	if prior := f.priorFailure; prior != nil {
		if flat == "SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM attempts WHERE tenant_id = $1 AND attempt_id = $2" && len(args) == 2 && args[0].Value == prior.tables["runs"][0]["tenant_id"] && args[1].Value == prior.tables["attempt_execution_admissions_v1"][0]["attempt_id"] {
			return &explanationFixtureRows{values: []driver.Value{f.priorAttempt}}, nil
		}
		// Exact old-Run/Attempt historical point reads only. Current execution
		// still routes to its real new admission/Store; history cannot authorize
		// a current reservation or make writes against the old producer.
		if s == localFailureHistorySQL || (len(args) == 2 && (args[1].Value == prior.tables["runs"][0]["run_id"] || args[1].Value == prior.tables["attempt_execution_admissions_v1"][0]["attempt_id"])) {
			if historical, handled, e := prior.originalRows(s, args); handled {
				return historical, e
			}
		}
		if s == sessionMaterialBoundarySQL && len(args) == 3 {
			for _, row := range prior.tables["session_material_boundaries_v1"] {
				if retainedDeletionFixtureEqual(row["sequence"], args[2].Value) {
					return &explanationFixtureRows{values: []driver.Value{row["record"], row["version"], row["through_sequence"], row["tenant_id"], row["session_id"], row["last_event_id"], row["prefix_digest"]}}, nil
				}
			}
		}
	}
	if f.priorFailure != nil && s == ownedAdmissionSQL {
		if ctx.Err() != nil || len(args) != 2 || args[0].Value != string(f.scope.TenantID) {
			return nil, errors.New("Web Final exact tenant/Attempt")
		}
		var row []driver.Value
		if len(f.final) != 0 {
			if len(f.final) != 7 {
				return nil, errors.New("Web Final physical shape")
			}
			final, err := domain.DecodeAttemptExecutionAdmissionV1([]byte(f.final[0].(string)))
			if err != nil {
				return nil, errors.New("Web Final original row")
			}
			if args[1].Value == string(final.Source.Scope.AttemptID) {
				row = append(row, f.final...)
			}
		}
		f.recordQuery(s, args)
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if f.manager != nil && f.manager.rawCanonical && flat == "SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3" {
		if !reflect.DeepEqual(localInstallationNamedValues(args), []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID), int64(maxSessionDeletionRows + 1)}) {
			return nil, errors.New("Web terminal exact binding index range")
		}
		if err := f.manager.rawCount(ctx); err != nil {
			return nil, err
		}
		var row []driver.Value
		if len(f.bindingIndex) == 3 {
			row = []driver.Value{f.bindingIndex[2]}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if f.manager != nil && f.manager.rawCanonical {
		return f.manager.QueryContext(ctx, s, args)
	}
	if s == "SELECT CurrentUtcTimestamp()" {
		f.at = time.Now().UTC().Truncate(time.Microsecond)
		f.install.sql.at = f.at
	}
	switch s {
	case ownedSpendingCurrentSQL, spendingAuditSQL, ownedProfileAuditSQL:
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return f.install.sql.QueryContext(ctx, s, args)
	case ownedProfileCurrentSQL:
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return f.install.sql.QueryContext(ctx, s, args)
	case ownedAdmissionSourcesJoinSQL:
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		if len(args) != 25 {
			return nil, errors.New("Web exact source join arity")
		}
		values := localInstallationNamedValues(args)
		profile := f.install.sql.heads[localInstallationSQLKey(values[:9])]
		if len(profile) != 11 {
			return nil, errors.New("Web original profile absent")
		}
		rows := append([]driver.Value(nil), profile...)
		for i := range 4 {
			row := f.install.sql.fundingHeads[localInstallationSQLKey(values[9+4*i:13+4*i])]
			if len(row) != 11 {
				return nil, errors.New("Web original funding absent")
			}
			rows = append(rows, row...)
		}
		f.recordQuery(s, args)
		return &explanationFixtureRows{values: rows}, nil
	}
	if flat == "SELECT binding_id FROM frontend_binding_keys WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3" {
		if !reflect.DeepEqual(localInstallationNamedValues(args), []driver.Value{string(f.scope.TenantID), string(domain.FrontendWeb), string(f.scope.SessionID)}) {
			return nil, errors.New("Web binding exact lookup")
		}
		var row []driver.Value
		if len(f.bindingKey) == 4 {
			row = []driver.Value{f.bindingKey[3]}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if flat == "SELECT session_id, sequence, event_id, run_id, mutation_digest FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3 AND expire_at > CurrentUtcTimestamp()" {
		return f.localCanonicalJoinSQL.QueryContext(ctx, s, args)
	}
	if flat == "SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2" || flat == "SELECT SUBSTRING(CAST(record AS String),0,32769) FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2" {
		if len(args) != 2 {
			return nil, errors.New("Web binding point arity")
		}
		id, ok := args[1].Value.(string)
		if !ok || domain.FrontendBindingID(id).Validate() != nil {
			return nil, errors.New("Web binding point identity")
		}
		var row []driver.Value
		if raw := f.rows["frontend_bindings"]; len(raw) == 1 {
			var b domain.FrontendBinding
			if json.Unmarshal([]byte(raw[0].(string)), &b) != nil {
				return nil, errors.New("Web binding malformed")
			}
			if args[0].Value == string(b.TenantID) && id == string(b.ID) {
				row = append([]driver.Value(nil), raw...)
			}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	// Public service reads retain exact owner/tenant point keys. Foreign keys
	// return absent DATA, allowing actual Store/domain authorization to deny.
	for _, entry := range []struct {
		sql, table string
		key        []driver.Value
	}{
		{"SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2", "sessions", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID)}},
		{"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3", "session_participants", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID), string(f.scope.OwnerUserID)}},
		{"SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2", "session_deletions", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID)}},
	} {
		if flat == entry.sql {
			var row []driver.Value
			if reflect.DeepEqual(localInstallationNamedValues(args), entry.key) {
				row = append([]driver.Value(nil), f.rows[entry.table]...)
			}
			return &explanationFixtureRows{values: row}, nil
		}
	}
	if strings.HasPrefix(flat, "SELECT record FROM tenant_memberships WHERE") {
		bucket, _ := ydbpartition.BucketV1(string(f.scope.OwnerUserID))
		want := []driver.Value{int64(bucket), string(f.scope.OwnerUserID), string(f.scope.TenantID)}
		if flat != "SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3" {
			return nil, fmt.Errorf("Web unregistered membership SQL %s", flat)
		}
		var row []driver.Value
		if reflect.DeepEqual(localInstallationNamedValues(args), want) {
			row = append([]driver.Value(nil), f.rows["tenant_memberships"]...)
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if flat == "SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3" {
		if len(args) != 3 || args[0].Value != string(f.scope.TenantID) || args[1].Value != string(f.scope.SessionID) {
			return nil, errors.New("Web exact event point")
		}
		sequence, ok := args[2].Value.(int64)
		if !ok || sequence < 1 || sequence > 8 {
			return nil, errors.New("Web bounded event sequence")
		}
		records := map[string]string{}
		for id, raw := range f.priorRecords {
			records[id] = raw
		}
		if f.manager != nil {
			for id, raw := range f.manager.state.eventRecords {
				records[id] = raw
			}
		} else if len(f.rows["session_events"]) == 1 {
			records["first"] = f.rows["session_events"][0].(string)
		}
		var row []driver.Value
		for _, raw := range records {
			var event domain.SessionEvent
			if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil {
				return nil, errors.New("Web invalid stored event")
			}
			if event.Sequence == uint64(sequence) {
				if row != nil {
					return nil, errors.New("Web duplicate stored sequence")
				}
				row = []driver.Value{raw}
			}
		}
		return &explanationFixtureRows{values: row}, nil
	}
	if flat == "SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3 ORDER BY sequence ASC LIMIT $4" {
		if len(args) != 4 || args[0].Value != string(f.scope.TenantID) || args[1].Value != string(f.scope.SessionID) {
			return nil, errors.New("Web exact history Session")
		}
		after, aok := args[2].Value.(int64)
		limit, lok := args[3].Value.(int64)
		if !aok || !lok || after < 0 || limit < 1 || limit > 101 {
			return nil, errors.New("Web bounded history range")
		}
		records := map[string]string{}
		for id, raw := range f.priorRecords {
			records[id] = raw
		}
		if f.manager != nil {
			for id, raw := range f.manager.state.eventRecords {
				records[id] = raw
			}
		} else if len(f.rows["session_events"]) == 1 {
			records["first"] = f.rows["session_events"][0].(string)
		}
		type item struct {
			sequence uint64
			record   string
		}
		items := make([]item, 0, len(records))
		for _, raw := range records {
			var event domain.SessionEvent
			if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil || event.TenantID != f.scope.TenantID || event.SessionID != f.scope.SessionID {
				return nil, errors.New("Web stored history malformed")
			}
			if event.Sequence > uint64(after) {
				items = append(items, item{event.Sequence, raw})
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].sequence < items[j].sequence })
		if int64(len(items)) > limit {
			items = items[:limit]
		}
		rows := make([][]driver.Value, 0, len(items))
		for _, item := range items {
			rows = append(rows, []driver.Value{item.record})
		}
		return &compactionSourceRows{rows: rows}, nil
	}
	if f.manager != nil {
		return f.manager.QueryContext(ctx, s, args)
	}
	if f.sessionPeer != nil {
		return f.sessionPeer.QueryContext(ctx, s, args)
	}
	return f.localCanonicalJoinSQL.QueryContext(ctx, s, args)
}
func (f *webManagerSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (result driver.Result, err error) {
	defer func() {
		if err != nil {
			f.lastError = err.Error()
		}
	}()
	flat := canonicalJoinSQL(s)
	f.callStatements++
	if len(f.callTrace) < 40 {
		f.callTrace = append(f.callTrace, flat)
	}
	switch s {
	case spendingWriteSQL, spendingAuditWriteSQL, ownedProfileWriteSQL, ownedProfileAuditWriteSQL:
		return f.install.sql.ExecContext(ctx, s, args)
	}
	if flat == "UPSERT INTO frontend_bindings (tenant_id, binding_id, frontend, external_conversation_id, session_id, revision, created_at, updated_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))" {
		var b domain.FrontendBinding
		if len(args) != 9 || json.Unmarshal([]byte(args[8].Value.(string)), &b) != nil || b.Validate() != nil || b.TenantID != f.scope.TenantID || b.SessionID != f.scope.SessionID || b.Frontend != domain.FrontendWeb || b.ExternalConversationID != string(f.scope.SessionID) || b.Revision != 1 {
			return nil, errors.New("Web actual binding write")
		}
		if !reflect.DeepEqual(localInstallationNamedValues(args[:8]), []driver.Value{string(b.TenantID), string(b.ID), string(b.Frontend), b.ExternalConversationID, string(b.SessionID), int64(b.Revision), b.CreatedAt, b.UpdatedAt}) {
			return nil, errors.New("Web binding scalar/record mismatch")
		}
		f.rows["frontend_bindings"] = []driver.Value{args[8].Value}
		f.bindings++
		f.reads["SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2"] = struct {
			table string
			args  []driver.Value
		}{"frontend_bindings", []driver.Value{string(b.TenantID), string(b.ID)}}
		f.reads["SELECT SUBSTRING(CAST(record AS String),0,32769) FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2"] = f.reads["SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2"]
		return driver.RowsAffected(1), nil
	}
	if flat == "UPSERT INTO frontend_bindings_by_session (tenant_id, session_id, binding_id) VALUES ($1, $2, $3)" || flat == "UPSERT INTO frontend_binding_keys (tenant_id, frontend, external_conversation_id, binding_id) VALUES ($1, $2, $3, $4)" {
		var b domain.FrontendBinding
		if len(f.rows["frontend_bindings"]) != 1 || json.Unmarshal([]byte(f.rows["frontend_bindings"][0].(string)), &b) != nil {
			return nil, errors.New("Web binding index before record")
		}
		want := []driver.Value{string(b.TenantID), string(b.SessionID), string(b.ID)}
		if strings.Contains(flat, "frontend_binding_keys") {
			want = []driver.Value{string(b.TenantID), string(b.Frontend), b.ExternalConversationID, string(b.ID)}
		}
		if !reflect.DeepEqual(localInstallationNamedValues(args), want) {
			return nil, errors.New("Web binding index exact projection")
		}
		if len(want) == 3 {
			f.bindingIndex = append([]driver.Value(nil), want...)
		} else {
			f.bindingKey = append([]driver.Value(nil), want...)
		}
		return driver.RowsAffected(1), nil
	}
	if f.manager != nil {
		return f.manager.ExecContext(ctx, s, args)
	}
	if f.sessionPeer != nil {
		return f.sessionPeer.ExecContext(ctx, s, args)
	}
	return f.localCanonicalJoinSQL.ExecContext(ctx, s, args)
}

type webManagerIngress struct {
	*Store
	f *webManagerSQL
}

func (w webManagerIngress) LookupCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventLookup) (ports.CanonicalUserEventLookupResult, error) {
	w.f.request.TenantID, w.f.request.UserID, w.f.request.SessionID, w.f.request.BindingID, w.f.request.IdempotencyKey = r.TenantID, r.UserID, w.f.scope.SessionID, r.BindingID, r.IdempotencyKey
	w.f.addRead("SELECT session_id, sequence, event_id, run_id, mutation_digest FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3 AND expire_at > CurrentUtcTimestamp()", "frontend_ingress_idempotency", string(r.TenantID), string(r.BindingID), string(r.IdempotencyKey))
	result, err := w.Store.LookupCanonicalUserEvent(ctx, r)
	if err != nil {
		w.f.lastError = "actual ingress lookup: " + err.Error()
	}
	return result, err
}
func (w webManagerIngress) CommitCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	w.f.request = r
	w.f.registerCanonicalReads(r)
	result, err := w.Store.CommitCanonicalUserEvent(ctx, r)
	w.f.lastCommitTrace = append([]string(nil), w.f.callTrace...)
	if err != nil {
		w.f.lastError = "actual ingress commit: " + err.Error() + " lastSQL=" + w.f.lastQuery + fmt.Sprintf(" observed_statements=%d", w.f.callStatements)
	}
	if err == nil {
		w.f.canonical = false
	}
	return result, err
}
func (f *webManagerSQL) addRead(s, table string, args ...driver.Value) {
	f.reads[canonicalJoinSQL(s)] = struct {
		table string
		args  []driver.Value
	}{table, args}
}
func (f *webManagerSQL) registerCanonicalReads(r ports.CanonicalUserEventCommit) {
	tenant, session, user := string(r.TenantID), string(r.SessionID), string(r.UserID)
	bucket, _ := ydbpartition.BucketV1(user)
	for statement := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(statement)
		table := strings.Fields(statement)[3]
		var key []driver.Value
		switch table {
		case "sessions", "session_deletions", "session_displays":
			key = []driver.Value{tenant, session}
		case "session_participants":
			key = []driver.Value{tenant, session, user}
		case "tenant_memberships":
			key = []driver.Value{int64(bucket), user, tenant}
		case "runs", "worker_jobs":
			key = []driver.Value{tenant, string(r.RunID)}
		case "attempts":
			key = []driver.Value{tenant, string(r.AttemptID)}
		case "artifact_manifests":
			key = []driver.Value{tenant, string(r.ManifestID)}
		case "frontend_bindings":
			key = []driver.Value{tenant, string(r.BindingID)}
		case "session_events":
			key = []driver.Value{tenant, session, int64(1)}
		case "dispatch_outbox":
			key = []driver.Value{tenant, string(r.DispatchID)}
		}
		if key != nil {
			f.addRead(bounded, table, key...)
		}
	}
	f.addRead(ownedCanonicalIngressIdempotencySQL, "frontend_ingress_idempotency", tenant, string(r.BindingID), string(r.IdempotencyKey))
	f.addRead(sessionMaterialCurrentSQL, "session_material_current_v1", tenant, session)
	f.addRead(sessionMaterialBoundarySQL, "session_material_boundaries_v1", tenant, session, int64(0))
	f.addRead(ownedActiveSessionParticipantsSQL, "active_session_participants", tenant, session, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	f.addRead("SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4", "session_participants", tenant, session, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	f.addRead("SELECT SUBSTRING(CAST(run_id AS String),0,161) FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2", "run_idempotency", tenant, string(r.IdempotencyKey))
	f.addRead("SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2", "run_explanation_heads_v1", tenant, string(r.RunID))
	f.addRead("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_displays WHERE tenant_id = $1 AND session_id = $2", "session_displays", tenant, session)
	f.addRead("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3", "session_events", tenant, session, int64(1))
	f.addRead("SELECT SUBSTRING(CAST(payload AS String),0,131073) FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2", "dispatch_outbox", tenant, string(r.DispatchID))
	f.addRead("SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", "runs", tenant, string(r.RunID))
	f.addRead("SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2", "worker_jobs", tenant, string(r.RunID))
	f.addRead("SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2", "attempts", tenant, string(r.AttemptID))
	f.addRead("SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2", "artifact_manifests", tenant, string(r.ManifestID))
	f.addRead("SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1", "checkpoints", tenant, string(r.AttemptID))
}

// Real service dependencies; no run/attempt, binding or execution source is
// prebuilt. Session/membership/participant/genesis are declared initial DATA.
type LocalWebManagerSeed struct {
	Source  *localoperator.Source
	Store   *Store
	DB      *sql.DB
	Options Options
	Tenant  domain.TenantID
	Owner   domain.UserID
	Session domain.SessionID
	peer    *webManagerSQL
}

func NewLocalWebManagerSeed(t *testing.T, endpoint string) *LocalWebManagerSeed {
	t.Helper()
	original := newWebManagerInstallation(t, endpoint)
	p := original.receipt.Profile
	at := original.sql.at
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, SessionID: "session-web-manager"}
	rated := &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}
	graph := &managedAdmissionGraphFixture{canonicalBudgetFixture: &canonicalBudgetFixture{ratedSQLFixture: rated, at: at}}
	script := &ownedFinalScript{managedAdmissionGraphFixture: graph}
	base := &localCanonicalJoinSQL{localCancellationSQL: &localCancellationSQL{ownedFinalScript: script}, canonical: true, reads: make(map[string]struct {
		table string
		args  []driver.Value
	})}
	f := &webManagerSQL{localCanonicalJoinSQL: base, install: original, scope: scope}
	session := domain.Session{ID: scope.SessionID, TenantID: scope.TenantID, CreatedBy: scope.OwnerUserID, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	member := domain.TenantMembership{TenantID: scope.TenantID, UserID: scope.OwnerUserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
	participant := domain.SessionParticipant{TenantID: scope.TenantID, SessionID: scope.SessionID, UserID: scope.OwnerUserID, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	for table, v := range map[string]any{"sessions": session, "tenant_memberships": member, "session_participants": participant} {
		ratedFixturePut(t, rated, table, v)
	}
	script.membership = []driver.Value{rated.rows["tenant_memberships"][0], string(member.Role), string(member.Status), int64(member.SecurityVersion)}
	script.participant = []driver.Value{rated.rows["session_participants"][0], string(participant.Role), string(participant.Status), int64(0)}
	rated.rows["active_session_participants"] = []driver.Value{rated.rows["session_participants"][0], string(scope.TenantID), string(scope.SessionID), string(scope.OwnerUserID), string(participant.Status)}
	genesis, e := sessioncontext.NewSessionMaterialGenesisV1(scope.TenantID, scope.SessionID)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := sessioncontext.EncodeSessionMaterialPrefixV1(genesis)
	if e != nil {
		t.Fatal(e)
	}
	rated.rows["session_material_current_v1"] = []driver.Value{string(raw), int64(1), int64(0), string(scope.TenantID), string(scope.SessionID), "", genesis.PrefixDigest}
	base.request.TenantID, base.request.SessionID, base.request.UserID = scope.TenantID, scope.SessionID, scope.OwnerUserID
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	original.store.db = db
	return &LocalWebManagerSeed{Source: original.source, Store: original.store, DB: db, Options: Options{OwnedProfilePublicationVerifier: original.source, AttemptSpendingPublicationVerifier: original.source, OwnedAttemptEffectSourceVerifier: original.source}, Tenant: scope.TenantID, Owner: scope.OwnerUserID, Session: scope.SessionID, peer: f}
}
func (s *LocalWebManagerSeed) Binder(t *testing.T) ports.HarnessBinder {
	t.Helper()
	b, e := NewLocalHarnessBinder(s.Store, s.Source, s.peer.install.receipt)
	if e != nil {
		t.Fatal(e)
	}
	return webManagerBinder{b, s.peer}
}
func (s *LocalWebManagerSeed) IngressStore() ports.CanonicalIngressStore {
	return webManagerIngress{s.Store, s.peer}
}
func (s *LocalWebManagerSeed) Resources(t *testing.T) ports.WebResourceStore {
	t.Helper()
	r, e := NewLocalWebResourceStore(s.Store, s.Source, s.peer.install.receipt)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func (s *LocalWebManagerSeed) Empty() bool {
	return len(s.peer.install.sql.fundingHeads) == 0 && len(s.peer.install.sql.fundingAudits) == 0 && len(s.peer.rows["runs"]) == 0 && len(s.peer.rows["attempts"]) == 0 && len(s.peer.rows["dispatch_outbox"]) == 0 && len(s.peer.rows["worker_jobs"]) == 0 && len(s.peer.rows["leases"]) == 0 && len(s.peer.rows["frontend_bindings"]) == 0 && s.peer.bindings == 0 && len(s.peer.bindingIndex) == 0 && len(s.peer.bindingKey) == 0
}
func (s *LocalWebManagerSeed) FundingCounts() (int, int) {
	return len(s.peer.install.sql.fundingHeads), len(s.peer.install.sql.fundingAudits)
}
func (s *LocalWebManagerSeed) LastError() string { return s.peer.lastError }
func (s *LocalWebManagerSeed) LastCommitStatements() []string {
	return append([]string(nil), s.peer.lastCommitTrace...)
}
func (s *LocalWebManagerSeed) OriginalStorageDigest(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(struct{ Profile, Funding, Audit map[string][]driver.Value }{s.peer.install.sql.heads, s.peer.install.sql.fundingHeads, s.peer.install.sql.fundingAudits})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Attach only empty DATA overlays AFTER actual Web canonical commit. Every
// invocation/finalization fact below must subsequently come from Store writes.
func (s *LocalWebManagerSeed) AdmitManager(t *testing.T) *LocalCanonicalManagerSeed {
	t.Helper()
	f := s.peer
	r := f.request
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	wantFunding := 4
	if f.priorFailure != nil {
		wantFunding = 8
	}
	if r.PreparedMaterial == nil || len(f.rows["runs"]) != 1 || len(f.rows["attempts"]) != 1 || len(f.install.sql.fundingHeads) != wantFunding || len(f.install.sql.fundingAudits) != wantFunding || f.bindings != 1 {
		t.Fatal("Web did not produce original canonical request")
	}
	reservationID := domain.QuotaReservationID("reservation-local-lifecycle")
	if f.priorFailure != nil {
		reservationID = domain.QuotaReservationID("hold-" + string(r.RunID))
	}
	admission := ports.DispatchAdmissionRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, OutboxID: r.DispatchID, ReservationID: reservationID, Now: f.at, HoldUntil: f.at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if result, e := s.Store.AdmitDispatch(ctx, admission); e != nil || !result.Admitted {
		t.Fatalf("actual Web canonical admission: admitted=%t err=%v SQL=%s last=%s statements=%d", result.Admitted, e, f.lastError, f.lastQuery, f.callStatements)
	}
	if f.priorFailure != nil {
		f.addRead("SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", "quota_reservations", string(r.TenantID), string(reservationID))
	}
	parent := &ownedAttemptEffectParentSQLFixture{base: f.ownedFinalScript, graph: f.managedAdmissionGraphFixture}
	alloc := &allocationSQLFixture{parent: parent}
	sessionPeer := &canonicalSessionSQL{localWorkerLifecycleSQL: &localWorkerLifecycleSQL{localCanonicalJoinSQL: f.localCanonicalJoinSQL, worker: "worker-web-manager"}, base: f.localCanonicalJoinSQL, parent: parent, alloc: alloc}
	f.sessionPeer = sessionPeer
	operations := &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{alloc}}}
	loop := &canonicalLoopSQL{canonicalSessionSQL: sessionPeer, operations: operations}
	prepare, e := serverlessharness.NewCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{19}, 32)))
	if e != nil {
		t.Fatal(e)
	}
	operate, e := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{23}, 32)), 16)
	if e != nil {
		t.Fatal(e)
	}
	var event domain.SessionEvent
	if json.Unmarshal([]byte(f.rows["session_events"][0].(string)), &event) != nil || event.ID != r.EventID {
		t.Fatal("Web original event absent")
	}
	loaded, found, e := s.Store.LoadWorkerJob(ctx, r.TenantID, r.RunID)
	if e != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatalf("Web queued load: found=%t err=%v SQL=%s", found, e, f.lastError)
	}
	seed := &LocalCanonicalSessionSeed{Source: s.Source, DB: s.DB, Options: s.Options, Prepare: prepare, Operate: operate, Backend: loaded.Job.HarnessBinding.Backend, Substrate: *loaded.Job.SubstrateBinding, Loaded: loaded, Claim: ports.WorkerLeaseRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, WorkerID: sessionPeer.worker}, peer: sessionPeer}
	loopSeed := &LocalCanonicalLoopSeed{LocalCanonicalSessionSeed: seed, Payload: event.Payload, Event: event, loop: loop}
	source, e := domain.DecodeAttemptExecutionSourceV1([]byte(loaded.Job.OwnedExecutionSourceRecord))
	if e != nil {
		t.Fatal(e)
	}
	grant, e := s.Source.ReadMCPToolGrant(t.Context(), source.Profile, seed.Now())
	if e != nil {
		t.Fatal(e)
	}
	var events []domain.SessionEvent
	for _, raw := range f.priorRecords {
		var event domain.SessionEvent
		if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil {
			t.Fatal("actual prior canonical event")
		}
		events = append(events, event)
	}
	events = append(events, event)
	sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
	var eventRows [][]driver.Value
	for _, item := range events {
		raw, e := json.Marshal(item)
		if e != nil {
			t.Fatal(e)
		}
		eventRows = append(eventRows, []driver.Value{string(raw)})
	}
	rows := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: operations}, events: eventRows}
	if f.priorFailure != nil {
		rows.historyRuns = make(map[domain.RunID]string)
		rows.historyMarkers = make(map[domain.RunID][2]string)
		for _, row := range f.priorFailure.tables["runs"] {
			rows.historyRuns[domain.RunID(row["run_id"].(string))] = row["payload"].(string)
		}
		for _, row := range f.priorFailure.tables["run_finalizations"] {
			rows.historyMarkers[domain.RunID(row["run_id"].(string))] = [2]string{row["terminal_status"].(string), row["content_digest"].(string)}
		}
	}
	effects := &compactionEffectSQL{compactionSourceSQL: rows}
	toolPeer := &canonicalToolSQL{seed: loopSeed, effects: effects}
	toolSeed := &LocalCanonicalToolSeed{LocalCanonicalLoopSeed: loopSeed, Grant: grant, peer: toolPeer, hold: append([]driver.Value(nil), f.rows["quota_reservations"]...)}
	p := &terminalPublicationSQL{base: &compactionPublicationSQL{compactionEffectSQL: effects}}
	terminal := &canonicalTerminalSQL{seed: toolSeed, publication: p, publicationConn: &terminalPublicationConn{Conn: p.base, f: p}, projection: map[string]domain.FrontendProjection{}, physical: map[string][][]driver.Value{}, state: LocalTerminalCompletionSnapshot{material: map[string][]driver.Value{}, eventIndex: map[string][]driver.Value{}, eventRecords: map[string]string{}}}
	if json.Unmarshal([]byte(f.rows["sessions"][0].(string)), &terminal.state.Session) != nil || terminal.state.Session.LastEventSequence != event.Sequence {
		t.Fatal("Web original Session frontier")
	}
	terminal.state.material["current"] = append([]driver.Value(nil), f.rows["session_material_current_v1"]...)
	terminal.state.material[fmt.Sprint(event.Sequence)] = append([]driver.Value(nil), f.rows["session_material_boundaries_v1"]...)
	terminal.state.eventIndex[string(event.IdempotencyKey)] = []driver.Value{int64(event.Sequence), string(event.ID)}
	terminal.state.eventRecords[string(event.ID)] = f.rows["session_events"][0].(string)
	terminalSeed := &LocalCanonicalTerminalSeed{LocalCanonicalToolSeed: toolSeed, terminal: terminal}
	manager := &canonicalManagerSQL{canonicalTerminalSQL: terminal}
	if len(f.rows["leases"]) != 0 || len(f.rows["lease_heads"]) != 0 || parent.parentRow != nil || alloc.row != nil || effects.operations != nil || effects.proxies != nil || effects.row != nil {
		t.Fatal("Web setup fabricated invocation effects")
	}
	f.manager = manager
	return &LocalCanonicalManagerSeed{LocalCanonicalTerminalSeed: terminalSeed, manager: manager}
}

// Switch only test-owned dispatch after the ACTUAL original owner completed
// and released. Historical rows remain immutable, current SQL scopes still
// come from the next real Web binder/admission. No Run or grant is synthesized.
func (s *LocalWebManagerSeed) BeginNextAfterKnownFailure(t *testing.T, manager *LocalCanonicalManagerSeed) {
	t.Helper()
	f := s.peer
	if f.priorFailure != nil || !manager.TerminalStorageReleased() {
		t.Fatal("next message before original F1 release")
	}
	f.priorFailure = s.knownFailureGraph(t, manager)
	oldAttempt, err := json.Marshal(manager.TerminalSnapshot(t).Attempt)
	if err != nil {
		t.Fatal(err)
	}
	f.priorAttempt = string(oldAttempt)
	f.priorRecords = map[string]string{}
	for id, raw := range manager.terminal.state.eventRecords {
		f.priorRecords[id] = raw
	}
	f.rows["session_material_current_v1"] = append([]driver.Value(nil), manager.terminal.state.material["current"]...)
	f.rows["session_material_boundaries_v1"] = append([]driver.Value(nil), manager.terminal.state.material[fmt.Sprint(manager.terminal.state.Session.LastEventSequence)]...)
	f.rows["frontend_ingress_idempotency"] = nil
	f.rows["run_idempotency"] = nil
	f.rows["runs"] = nil
	f.rows["attempts"] = nil
	f.rows["run_explanation_heads_v1"] = nil
	f.rows["artifact_manifests"] = nil
	f.rows["leases"] = nil
	f.rows["worker_jobs"] = nil
	f.rows["dispatch_outbox"] = nil
	f.rows["quota_reservations"] = nil
	f.manager = nil
	f.sessionPeer = nil
	f.canonical = true
}

// A closed storage peer fault after an ACTUAL MODEL2 observation was committed.
// It never supplies an authenticated replacement or mutates production state.
func (s *LocalWebManagerSeed) SetModelTailObservationFault(t *testing.T, mode string) {
	t.Helper()
	if mode != "missing_readback" && mode != "missing_tail" && mode != "changed_tail" {
		t.Fatal("closed original MODEL tail fault")
	}
	if s.peer.modelTailFault != "" {
		t.Fatal("MODEL tail fault already selected")
	}
	s.peer.modelTailFault = mode
}
func (s *LocalWebManagerSeed) ModelTailObservationFaults() int { return s.peer.modelTailFaults }
func (s *LocalWebManagerSeed) EndModelTailObservationFault()   { s.peer.modelTailFault = "" }

// Corrupt only canonical history DATA after the genuine first F1 committed.
// The original failed Run/F1 marker/capsule/frontier and funding remain exact;
// a payload-based classifier must not hide this producing Run from validation.
func (s *LocalWebManagerSeed) ReplacePriorKnownFailureNotice(t *testing.T, ref domain.BlobRef) {
	t.Helper()
	f := s.peer
	if f.priorFailure == nil || f.manager != nil || ref.Validate() != nil || ref.TenantID != s.Tenant {
		t.Fatal("prior F1 corruption requires genuine retained snapshot")
	}
	found := 0
	for id, raw := range f.priorRecords {
		var event domain.SessionEvent
		if json.Unmarshal([]byte(raw), &event) != nil {
			t.Fatal("actual retained canonical row")
		}
		if event.Kind == domain.SessionEventSystemNotice && event.RunID != nil {
			event.Payload = ref
			if event.Validate() != nil {
				t.Fatal("changed canonical notice must remain structurally valid")
			}
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			f.priorRecords[id] = string(raw)
			found++
		}
	}
	if found != 1 {
		t.Fatal("exactly one retained failure notice required")
	}
}

func (s *LocalWebManagerSeed) CorruptPriorKnownFailureCorrespondence(t *testing.T, mode string) {
	t.Helper()
	f := s.peer
	if f.priorFailure == nil || f.manager != nil {
		t.Fatal("prior F1 corruption requires genuine retained snapshot")
	}
	switch mode {
	case "missing_notice":
		found := 0
		for id, raw := range f.priorRecords {
			var event domain.SessionEvent
			if json.Unmarshal([]byte(raw), &event) != nil {
				t.Fatal("actual retained event")
			}
			if event.Kind == domain.SessionEventSystemNotice && event.RunID != nil {
				event.RunID = nil
				if event.Validate() != nil {
					t.Fatal("generic unlinked notice must remain valid")
				}
				raw, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				f.priorRecords[id] = string(raw)
				found++
			}
		}
		if found != 1 {
			t.Fatal("one actual F1 anchor required")
		}
	case "metadata_stripped":
		rows := f.priorFailure.tables["runs"]
		if len(rows) != 1 {
			t.Fatal("one original retained Run")
		}
		var run domain.Run
		if json.Unmarshal([]byte(rows[0]["payload"].(string)), &run) != nil || run.TerminalFailure == nil {
			t.Fatal("actual F1 Run")
		}
		run.TerminalFailure = nil
		if run.Validate() != nil {
			t.Fatal("stripped Run must remain structurally valid")
		}
		raw, err := json.Marshal(run)
		if err != nil {
			t.Fatal(err)
		}
		rows[0]["payload"] = string(raw)
	case "marker_stripped":
		rows := f.priorFailure.tables["run_finalizations"]
		if len(rows) != 1 {
			t.Fatal("one actual F1 marker")
		}
		rows[0]["content_digest"] = ""
	default:
		t.Fatal("closed F1 history correspondence fault")
	}
}

// Project only ACTUAL committed Source/Manager rows into the SAME complete
// deletion graph used by the original successful producer. No terminal marker,
// private producer or auth receipt is fabricated by this DATA adapter.
func (s *LocalWebManagerSeed) knownFailureGraph(t *testing.T, manager *LocalCanonicalManagerSeed) *retainedDeletionGraphFixture {
	t.Helper()
	snapshot := manager.TerminalSnapshot(t)
	parent := manager.ManagerParent(t)
	if snapshot.Run.TerminalFailure == nil || snapshot.Run.Status != domain.RunFailed || snapshot.Finalizations != 1 || len(snapshot.Events) != 3 {
		t.Fatal("requires genuine completed F1 producer")
	}
	f := s.peer
	if manager.LocalCanonicalLoopSeed.peer.base != f.localCanonicalJoinSQL {
		t.Fatal("terminal Session/head projection was not the original Web map")
	}
	effects := manager.peer.effects
	scope := parent.Scope
	var extra []retainedDeletionFixtureRecord
	var binding domain.FrontendBinding
	if len(f.rows["frontend_bindings"]) != 1 || json.Unmarshal([]byte(f.rows["frontend_bindings"][0].(string)), &binding) != nil {
		t.Fatal("actual Web binding missing")
	}
	extra = append(extra, retainedDeletionFixtureRecord{"frontend_bindings", binding, map[string]any{"binding_id": string(binding.ID)}}, retainedDeletionFixtureRecord{"frontend_bindings_by_session", struct{}{}, map[string]any{"tenant_id": string(binding.TenantID), "session_id": string(binding.SessionID), "binding_id": string(binding.ID)}}, retainedDeletionFixtureRecord{"frontend_binding_keys", struct{}{}, map[string]any{"tenant_id": string(binding.TenantID), "frontend": string(binding.Frontend), "external_conversation_id": binding.ExternalConversationID, "binding_id": string(binding.ID)}})
	for _, p := range manager.terminal.projection {
		bucket, e := ydbpartition.BucketV1(string(p.ID))
		if e != nil {
			t.Fatal(e)
		}
		extra = append(extra, retainedDeletionFixtureRecord{"frontend_projection_outbox", p, map[string]any{"frontend_projection_id": string(p.ID)}}, retainedDeletionFixtureRecord{"frontend_projections_by_session", struct{}{}, map[string]any{"tenant_id": string(p.TenantID), "session_id": string(p.SessionID), "frontend_projection_id": string(p.ID)}}, retainedDeletionFixtureRecord{"frontend_projections_by_run", struct{}{}, map[string]any{"tenant_id": string(p.TenantID), "run_id": string(snapshot.Run.ID), "frontend": string(p.Frontend), "frontend_projection_id": string(p.ID)}}, retainedDeletionFixtureRecord{"frontend_projection_ready_v1", struct{}{}, map[string]any{"tenant_id": string(p.TenantID), "run_id": string(snapshot.Run.ID), "frontend": string(p.Frontend), "frontend_projection_id": string(p.ID), "shard_bucket": uint64(bucket), "created_at": p.CreatedAt}})
	}
	if len(f.rows["run_explanation_heads_v1"]) == 1 {
		var head runexplanation.HeadV1
		if json.Unmarshal([]byte(f.rows["run_explanation_heads_v1"][0].(string)), &head) != nil {
			t.Fatal("actual Run explanation")
		}
		extra = append(extra, retainedDeletionFixtureRecord{"run_explanation_heads_v1", head, nil})
	}
	if len(f.rows["session_displays"]) == 1 {
		var display domain.SessionDisplay
		if json.Unmarshal([]byte(f.rows["session_displays"][0].(string)), &display) != nil {
			t.Fatal("actual Session display")
		}
		extra = append(extra, retainedDeletionFixtureRecord{"session_displays", display, nil})
	}
	return newRetainedDeletionGraphData(t, retainedDeletionFixtureData{base: f, snapshot: snapshot, scope: scope, membership: f.membership, participant: f.participant, sourceEvents: []domain.SessionEvent{manager.Event}, material: manager.terminal.state.material, job: f.rows["worker_jobs"], input: f.rows["artifact_manifests"], admission: f.final, parent: f.sessionPeer.parent.parentRow, burn: f.sessionPeer.parent.burnRow, allocation: f.sessionPeer.alloc.row, proxy: effects.firstProxy, accounting: f.sessionPeer.parent.accountingRow, operations: effects.operations, proxies: effects.proxies, extra: extra})
}

// Execute the public lifecycle over an isolated copy of genuine durable SQL
// DATA and an exact-object controlled delete peer. The original execution rows,
// Source and objects are not modified. This is no native/storage qualification.
func (s *LocalWebManagerSeed) CheckKnownFailureDeletion(t *testing.T, manager *LocalCanonicalManagerSeed, reader SessionDeletionBlobReader, mode string) error {
	t.Helper()
	f := s.knownFailureGraph(t, manager)
	f.requested = false
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	defer db.Close()
	probe := &retainedDeletionProbeReader{reader: reader}
	store := &Store{db: db, sessionDeletionBlobReader: probe}
	if _, err := store.RequestSessionDeletion(t.Context(), f.request); err != nil {
		return fmt.Errorf("F1 Request %s: %w", f.lastSQL, err)
	}
	switch mode {
	case "positive":
	case "missing_observation":
		delete(f.tables["attempt_operations_v1"][1], "observation_record")
	case "missing_parent":
		f.tables["attempt_effect_reservations"] = nil
	case "changed_marker":
		f.tables["run_finalizations"][0]["content_digest"] = "local_f1:" + strings.Repeat("f", 64)
	case "changed_accounting":
		f.tables["attempt_effect_reservations"][0]["operation_accounting_digest"] = strings.Repeat("f", 64)
	case "missing_body":
		probe.deny = true
	default:
		t.Fatal("closed F1 deletion fault")
	}
	requestWrites := f.writes
	blobs := &retainedDeletionServiceBlobs{reader: probe, deleted: map[string]bool{}}
	service, err := sessionlifecycle.New(store, blobs, 0, 0)
	if err != nil {
		return err
	}
	plan, err := service.Plan(t.Context(), s.Tenant, s.Session)
	if err == nil {
		_, err = service.Execute(t.Context(), s.Tenant, s.Session, plan.Confirmation, time.Now().UTC())
	}
	if mode != "positive" {
		if err == nil || blobs.deletes != 0 || blobs.puts != 0 || f.writes != requestWrites || f.request.State != domain.SessionDeletionRequested {
			return fmt.Errorf("F1 corrupt %s granted destructive effects: %v", mode, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("F1 full deletion %s: %w", f.lastSQL, err)
	}
	if f.request.State != domain.SessionDeletionCompleted || f.request.RetainedVerification == nil || blobs.puts != 0 || blobs.deletes != len(plan.Inventory.Objects) || blobs.deletes < 8 || len(f.tables["attempt_effect_reservations"]) != 0 {
		return fmt.Errorf("F1 deletion omitted retained inventory: state=%s deletes=%d inventory=%d", f.request.State, blobs.deletes, len(plan.Inventory.Objects))
	}
	return nil
}
