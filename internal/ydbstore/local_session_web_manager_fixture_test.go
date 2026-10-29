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
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
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
	return b.HarnessBinder.BindHarness(ctx, r)
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
		if !ok || sequence < 1 || sequence > 4 {
			return nil, errors.New("Web bounded event sequence")
		}
		records := map[string]string{}
		if f.manager != nil {
			records = f.manager.state.eventRecords
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
		if f.manager != nil {
			records = f.manager.state.eventRecords
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
	return w.Store.LookupCanonicalUserEvent(ctx, r)
}
func (w webManagerIngress) CommitCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	w.f.request = r
	w.f.registerCanonicalReads(r)
	result, err := w.Store.CommitCanonicalUserEvent(ctx, r)
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
	if r.PreparedMaterial == nil || len(f.rows["runs"]) != 1 || len(f.rows["attempts"]) != 1 || len(f.install.sql.fundingHeads) != 4 || len(f.install.sql.fundingAudits) != 4 || f.bindings != 1 {
		t.Fatal("Web did not produce original canonical request")
	}
	admission := ports.DispatchAdmissionRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, OutboxID: r.DispatchID, ReservationID: "reservation-local-lifecycle", Now: f.at, HoldUntil: f.at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if result, e := s.Store.AdmitDispatch(ctx, admission); e != nil || !result.Admitted {
		t.Fatalf("actual Web canonical admission: admitted=%t err=%v SQL=%s", result.Admitted, e, f.lastError)
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
	rows := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: operations}, events: [][]driver.Value{append([]driver.Value(nil), f.rows["session_events"]...)}}
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
