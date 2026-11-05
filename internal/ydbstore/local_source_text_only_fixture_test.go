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
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func textOnlyEvidence(ref string, raw []byte) localoperator.EvidenceBodyV1 {
	sum := sha256.Sum256(raw)
	return localoperator.EvidenceBodyV1{Reference: domain.OwnedAgentAuthorityBodyRefV1{Ref: ref, Digest: hex.EncodeToString(sum[:]), Bytes: uint64(len(raw))}, Body: raw}
}

// Only the private installed profile is published. There is no Attempt, lease,
// outbox, service declaration, funding head or effect reservation to inherit.
// Metadata/calibration are synthetic, never model or live-service evidence.
func newLocalTextInstallation(t *testing.T, endpoint string, configure ...func(*localoperator.ManifestV1)) *originalLocalRecoveryFixture {
	t.Helper()
	root, _, m := localOperatorFiles(t)
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
	c, p := &m.Configuration, &m.Profile
	c.BaseURL, m.InvoiceTerms.BaseURL = endpoint, endpoint
	c.Auth, c.Capabilities.ToolCalls = domain.ConfiguredInferenceAuthNoneV1, domain.ProviderEvidenceUnknownV1
	c.WorkerExecutableDigest, c.LocalHostIdentityDigest, c.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Locator.Profile = domain.AgentLoopOwnedLocalTextV1
	p.MCPToolGrantDigest, p.WebSearchGrantDigest = "", ""
	p.WebSearchResourceGrant, m.SearchExpenditure, m.SearchCredential = nil, nil, nil
	p.CostCeiling.MCPToolCost, p.CostCeiling.WebSearchCost = nil, nil
	p.CostCeiling.MaxTotalAmountMicrounits = new(uint64)
	p.Substrate.OuterHarnessArtifactDigest, p.Substrate.LocalHostIdentityDigest, p.Substrate.LocalNetworkNamespaceIdentityDigest = identity.WorkerExecutableDigest, identity.LocalHostIdentityDigest, identity.LocalNetworkNamespaceIdentityDigest
	p.Substrate.EgressProxyArtifactDigest, p.Substrate.EgressProxyIdentityDigest = identity.WorkerExecutableDigest, connector
	p.CostCeiling.MaxPreEffectDurationPerDelivery = 10 * time.Second
	c.Limits.OperationTimeout, p.Limits.MaxNetworkOperationDuration = 5*time.Second, 5*time.Second
	p.InstructionVersion, p.CompactionPolicyDigest = compactioncontext.InstructionVersionV1, compactioncontext.CompactionPolicyDigestV1()
	l := &p.Limits
	l.MaxConcurrentOperations, l.MaxModelCalls, l.MaxOperations = 1, 1, 1
	l.MaxAggregateInputTokens, l.MaxAggregateOutputTokens = l.MaxInputTokensPerCall, l.MaxOutputTokensPerCall
	l.MaxCompactionCalls, l.MaxToolDispatches, l.MaxWebSearches = 0, 0, 0
	l.MaxSummaryBytes, l.MaxToolArgumentBytes, l.MaxToolResultBytes = 0, 0, 0
	l.MaxMCPOutboundMessages, l.MaxMCPCatalogPages, l.MaxMCPInboundFrames, l.MaxMCPInboundBytes = 0, 0, 0, 0
	calibration, err := json.Marshal(map[string]any{"version": 1, "algorithm": "utf8_affine.v1", "input_domain": "chat_completions_text.v1", "tenant_id": c.TenantID, "owner_user_id": c.OwnerUserID, "model_vendor_id": c.ModelVendorID, "model_id": c.ModelID, "response_model_ids": c.ResponseModelIDs, "server_id": c.ServerID, "server_version": c.ServerVersion, "protocol": c.NativeProtocolVersion, "chat_template_digest": c.ChatTemplateDigest, "estimator_version": c.TokenEstimatorVersion, "capability_evidence_digest": c.CapabilityEvidence.Digest, "model_window_tokens": c.ModelWindowTokens, "max_wire_bytes": c.Limits.RequestBytes, "max_messages": c.Limits.Messages, "max_tools": c.Limits.Tools, "byte_numerator": 1, "byte_denominator": 1, "fixed_tokens": 1, "message_tokens": 1, "tool_tokens": 1})
	if err != nil {
		t.Fatal(err)
	}
	body := textOnlyEvidence("synthetic-text-calibration", calibration)
	c.CalibrationDigest, p.CalibrationDigest = body.Reference.Digest, body.Reference.Digest
	// localOperatorFiles has only the original three owner-role bodies, never
	// the MCP/search declaration helpers or their fabricated service heads.
	if len(m.Evidence) != 3 {
		t.Fatal("generic private-file fixture acquired unexpected service evidence")
	}
	m.Evidence = append(m.Evidence, body)
	invoice, err := json.Marshal(m.InvoiceTerms)
	if err != nil {
		t.Fatal(err)
	}
	body = textOnlyEvidence(c.OperatorTerms.Ref, invoice)
	for i := range m.Evidence {
		if m.Evidence[i].Reference.Ref == c.OperatorTerms.Ref {
			m.Evidence[i] = body
		}
	}
	c.OperatorTerms = body.Reference
	p.ProviderPriceDigest, p.ProviderTermsDigest, p.SubstratePriceDigest, p.SubstrateTermsDigest = body.Reference.Digest, body.Reference.Digest, body.Reference.Digest, body.Reference.Digest
	policy, err := ports.BuildLocalLoopbackEgressPolicyV1(*c)
	if err != nil {
		t.Fatal(err)
	}
	p.Substrate.EgressPolicyDigest, err = policy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	p.Substrate.AdmissionCostCeilingDigest, err = p.CostCeiling.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if len(configure) > 1 {
		t.Fatal("one bounded pre-intake text DATA variant")
	}
	if len(configure) == 1 {
		configure[0](&m)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := localoperator.ValidateManifestTemplate(t.Context(), raw, exe); err != nil {
		t.Fatalf("text-only complete template: %v", err)
	}
	writeLocalOperatorManifest(t, root, m)
	source, err := localoperator.OpenFresh(t.Context(), root, exe)
	if err != nil {
		t.Fatalf("genuine text-only Source intake: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Error(err)
		}
	})
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
		t.Fatalf("actual text-only profile publication: %v", err)
	}
	if err := source.AcceptReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	if len(f.fundingHeads) != 0 || len(f.fundingAudits) != 0 {
		t.Fatal("text installation seeded funding")
	}
	return &originalLocalRecoveryFixture{root: root, exe: exe, source: source, store: store, sql: f, receipt: receipt}
}

func textOnlyRegisterAttemptKeys(f *originalLocalRecoveryFixture, scope domain.AttemptExecutionSourceScopeV1) {
	for _, kind := range ownedSpendingKinds {
		f.sql.fundingKeys[localInstallationSQLKey([]driver.Value{string(scope.TenantID), string(scope.AttemptID), string(kind), string(f.receipt.Profile.BillingOwnerUserID)})] = true
	}
}

type localTextWebSQL struct {
	*localCanonicalJoinSQL
	install                  *originalLocalRecoveryFixture
	scope                    domain.AttemptExecutionSourceScopeV1
	bindings                 int
	bindingIndex, bindingKey []driver.Value
	manager                  *localTextManagerSQL
	sessionPeer              *canonicalSessionSQL
	lastError                string
	lastSQL                  string
	budget                   *ownedAuthorityBudget
	priorHistory             *LocalFreshRunHistorySQLFixture
	priorRecords             map[string]string
	priorMaterial            map[string][]driver.Value
}

func (f *localTextWebSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localTextWebSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	// One real synchronous authority for this transaction and its persisted
	// postimages; no synthetic time surrounds socket I/O.
	f.at = time.Now().UTC().Truncate(time.Microsecond)
	f.install.sql.at = f.at
	a, err := f.install.sql.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	var b driver.Tx
	if f.manager != nil {
		b, err = f.manager.BeginTx(ctx, options)
	} else if f.sessionPeer != nil {
		b, err = f.sessionPeer.BeginTx(ctx, options)
	} else {
		b, err = f.localCanonicalJoinSQL.BeginTx(ctx, options)
	}
	if err != nil {
		_ = a.Rollback()
		return nil, err
	}
	return &localTextWebTx{a: a, b: b, f: f, index: append([]driver.Value(nil), f.bindingIndex...), key: append([]driver.Value(nil), f.bindingKey...), bindings: f.bindings}, nil
}

type localTextWebTx struct {
	a, b       driver.Tx
	f          *localTextWebSQL
	index, key []driver.Value
	bindings   int
	done       bool
}

func (tx *localTextWebTx) Commit() error {
	if err := tx.b.Commit(); err != nil {
		_ = tx.a.Rollback()
		return err
	}
	tx.done = true
	return tx.a.Commit()
}
func (tx *localTextWebTx) Rollback() error {
	if !tx.done {
		tx.f.bindingIndex, tx.f.bindingKey, tx.f.bindings = tx.index, tx.key, tx.bindings
		tx.done = true
	}
	return errors.Join(tx.a.Rollback(), tx.b.Rollback())
}

func (f *localTextWebSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Rows, err error) {
	defer func() {
		if err != nil {
			f.lastError = err.Error()
		}
	}()
	flat := canonicalJoinSQL(s)
	f.lastSQL = flat
	if budget, ok := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget); ok {
		f.budget = budget
	}
	values := localInstallationNamedValues(args)
	if s == quotaReservationStorageSQLV1 || flat == "SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2" {
		return f.localCanonicalJoinSQL.QueryContext(ctx, s, args)
	}
	if f.priorHistory != nil && (s == localTerminalHistorySQL || s == localTerminalHistoryRunsSQL(len(args)-3)) {
		return (&freshRunHistoryConn{history: f.priorHistory}).QueryContext(ctx, s, args)
	}
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	if s == "SELECT CurrentUtcTimestamp()" {
		f.install.sql.at = f.at
	}
	switch s {
	case ownedSpendingCurrentSQL, spendingAuditSQL, ownedProfileAuditSQL, ownedProfileCurrentSQL, ownedTextAdmissionSourcesJoinSQL:
		if f.manager != nil {
			if err := f.manager.seed.loop.countStatement(ctx); err != nil {
				return nil, err
			}
		}
		return (&localTextSourcesSQL{localInstallationSQLFixture: f.install.sql}).QueryContext(ctx, s, args)
	}
	if f.manager != nil && f.manager.rawCanonical {
		if flat == "SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3" {
			if !reflect.DeepEqual(values, []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID), int64(maxSessionDeletionRows + 1)}) {
				return nil, errors.New("text terminal exact binding range")
			}
			if err := f.manager.rawCount(ctx); err != nil {
				return nil, err
			}
			if len(f.bindingIndex) == 3 {
				return row([]driver.Value{f.bindingIndex[2]})
			}
			return row(nil)
		}
		return f.manager.QueryContext(ctx, s, args)
	}
	if flat == "SELECT binding_id FROM frontend_binding_keys WHERE tenant_id = $1 AND frontend = $2 AND external_conversation_id = $3" {
		if !reflect.DeepEqual(values, []driver.Value{string(f.scope.TenantID), string(domain.FrontendWeb), string(f.scope.SessionID)}) {
			return nil, errors.New("text Web binding exact key")
		}
		if len(f.bindingKey) == 4 {
			return row([]driver.Value{f.bindingKey[3]})
		}
		return row(nil)
	}
	if s == ownedCanonicalIngressIdempotencySQL || flat == "SELECT session_id, sequence, event_id, run_id, mutation_digest FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3 AND expire_at > CurrentUtcTimestamp()" {
		return f.localCanonicalJoinSQL.QueryContext(ctx, s, args)
	}
	if flat == "SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2" || flat == "SELECT SUBSTRING(CAST(record AS String),0,32769) FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2" {
		if len(values) != 2 {
			return nil, errors.New("text binding point shape")
		}
		var binding domain.FrontendBinding
		stored := f.rows["frontend_bindings"]
		if len(stored) == 1 && json.Unmarshal([]byte(stored[0].(string)), &binding) == nil && reflect.DeepEqual(values, []driver.Value{string(binding.TenantID), string(binding.ID)}) {
			return row(stored)
		}
		return row(nil)
	}
	for _, entry := range []struct {
		sql, table string
		key        []driver.Value
	}{
		{"SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2", "sessions", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID)}},
		{"SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3", "session_participants", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID), string(f.scope.OwnerUserID)}},
		{"SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2", "session_deletions", []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID)}},
	} {
		if flat == entry.sql {
			if reflect.DeepEqual(values, entry.key) {
				return row(f.rows[entry.table])
			}
			return row(nil)
		}
	}
	if flat == "SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3" {
		bucket, _ := ydbpartition.BucketV1(string(f.scope.OwnerUserID))
		if reflect.DeepEqual(values, []driver.Value{int64(bucket), string(f.scope.OwnerUserID), string(f.scope.TenantID)}) {
			return row(f.rows["tenant_memberships"])
		}
		return row(nil)
	}
	if flat == "SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3 ORDER BY sequence ASC LIMIT $4" || flat == "SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3" {
		if len(values) < 3 || values[0] != string(f.scope.TenantID) || values[1] != string(f.scope.SessionID) {
			return nil, errors.New("text Web history exact scope")
		}
		sequence, ok := values[2].(int64)
		if !ok || sequence < 0 {
			return nil, errors.New("text Web history sequence")
		}
		records := map[string]string{}
		if f.manager != nil {
			records = f.manager.state.eventRecords
		} else {
			for id, raw := range f.priorRecords {
				records[id] = raw
			}
			if stored := f.rows["session_events"]; len(stored) == 1 {
				records["current"] = stored[0].(string)
			}
		}
		type item struct {
			sequence uint64
			raw      string
		}
		var ordered []item
		for _, raw := range records {
			var event domain.SessionEvent
			if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil {
				return nil, errors.New("text history stored record")
			}
			if len(values) == 3 && event.Sequence == uint64(sequence) {
				return row([]driver.Value{raw})
			}
			if len(values) == 4 && event.Sequence > uint64(sequence) {
				ordered = append(ordered, item{event.Sequence, raw})
			}
		}
		if len(values) == 3 {
			return row(nil)
		}
		limit, ok := values[3].(int64)
		if !ok || limit < 1 || limit > 101 {
			return nil, errors.New("text history bounded range")
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].sequence < ordered[j].sequence })
		ordered = ordered[:min(len(ordered), int(limit))]
		rows := make([][]driver.Value, len(ordered))
		for i, item := range ordered {
			rows[i] = []driver.Value{item.raw}
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

func (f *localTextWebSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func() {
		if err != nil {
			f.lastError = err.Error()
		}
	}()
	switch s {
	case spendingWriteSQL, spendingAuditWriteSQL, ownedProfileWriteSQL, ownedProfileAuditWriteSQL:
		return f.install.sql.ExecContext(ctx, s, args)
	}
	flat := canonicalJoinSQL(s)
	if flat == "UPSERT INTO frontend_bindings (tenant_id, binding_id, frontend, external_conversation_id, session_id, revision, created_at, updated_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))" {
		var b domain.FrontendBinding
		if len(args) != 9 || json.Unmarshal([]byte(args[8].Value.(string)), &b) != nil || b.Validate() != nil || b.TenantID != f.scope.TenantID || b.SessionID != f.scope.SessionID || b.Frontend != domain.FrontendWeb || b.ExternalConversationID != string(f.scope.SessionID) || b.Revision != 1 {
			return nil, errors.New("text original Web binding write")
		}
		if !reflect.DeepEqual(localInstallationNamedValues(args[:8]), []driver.Value{string(b.TenantID), string(b.ID), string(b.Frontend), b.ExternalConversationID, string(b.SessionID), int64(b.Revision), b.CreatedAt, b.UpdatedAt}) {
			return nil, errors.New("text binding scalar/record correspondence")
		}
		f.rows["frontend_bindings"] = []driver.Value{args[8].Value}
		f.bindings++
		return driver.RowsAffected(1), nil
	}
	if flat == "UPSERT INTO frontend_bindings_by_session (tenant_id, session_id, binding_id) VALUES ($1, $2, $3)" || flat == "UPSERT INTO frontend_binding_keys (tenant_id, frontend, external_conversation_id, binding_id) VALUES ($1, $2, $3, $4)" {
		var b domain.FrontendBinding
		if len(f.rows["frontend_bindings"]) != 1 || json.Unmarshal([]byte(f.rows["frontend_bindings"][0].(string)), &b) != nil {
			return nil, errors.New("text binding index before record")
		}
		want := []driver.Value{string(b.TenantID), string(b.SessionID), string(b.ID)}
		if strings.Contains(flat, "frontend_binding_keys") {
			want = []driver.Value{string(b.TenantID), string(b.Frontend), b.ExternalConversationID, string(b.ID)}
		}
		if !reflect.DeepEqual(localInstallationNamedValues(args), want) {
			return nil, errors.New("text binding index correspondence")
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

type localTextIngress struct {
	*Store
	f *localTextWebSQL
}

func (w localTextIngress) LookupCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventLookup) (ports.CanonicalUserEventLookupResult, error) {
	w.f.request.TenantID, w.f.request.UserID, w.f.request.SessionID, w.f.request.BindingID, w.f.request.IdempotencyKey = r.TenantID, r.UserID, w.f.scope.SessionID, r.BindingID, r.IdempotencyKey
	w.f.addRead(ownedCanonicalIngressIdempotencySQL, "frontend_ingress_idempotency", string(r.TenantID), string(r.BindingID), string(r.IdempotencyKey))
	w.f.addRead("SELECT session_id, sequence, event_id, run_id, mutation_digest FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3 AND expire_at > CurrentUtcTimestamp()", "frontend_ingress_idempotency", string(r.TenantID), string(r.BindingID), string(r.IdempotencyKey))
	result, err := w.Store.LookupCanonicalUserEvent(ctx, r)
	if err != nil {
		w.f.lastError = "text ingress lookup: " + err.Error()
	}
	return result, err
}
func (w localTextIngress) CommitCanonicalUserEvent(ctx context.Context, r ports.CanonicalUserEventCommit) (ports.CanonicalUserEventResult, error) {
	w.f.request = r
	w.f.registerCanonicalReads(r)
	result, err := w.Store.CommitCanonicalUserEvent(ctx, r)
	if err != nil {
		w.f.lastError = "text ingress commit: " + err.Error()
	}
	if err == nil {
		w.f.canonical = false
	}
	return result, err
}
func (f *localTextWebSQL) addRead(s, table string, args ...driver.Value) {
	f.reads[canonicalJoinSQL(s)] = struct {
		table string
		args  []driver.Value
	}{table, args}
}

// Reused canonical terminal SQL DATA machinery over the ordinary one-MODEL
// loop, not the tagged MCP constructor or any preseeded invocation facts.
type localTextTerminalSQL struct {
	seed                            *LocalCanonicalLoopSeed
	publication                     *terminalPublicationSQL
	publicationConn                 *terminalPublicationConn
	state                           LocalTerminalCompletionSnapshot
	markerSeen, rawCanonical        bool
	rawStatements, maxRawStatements int
	projection                      map[string]domain.FrontendProjection
	physical                        map[string][][]driver.Value
	lastSQL, lastError              string
	completionFault                 string
	lostACKs                        int
}

func (f *localTextTerminalSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localTextTerminalSQL) Driver() driver.Driver                        { return f.seed.loop.Driver() }
func (f *localTextTerminalSQL) Prepare(s string) (driver.Stmt, error)        { return f.seed.loop.Prepare(s) }
func (f *localTextTerminalSQL) Close() error                                 { return f.seed.loop.Close() }
func (f *localTextTerminalSQL) Begin() (driver.Tx, error) {
	return nil, errors.New("terminal requires explicit bounded transaction")
}

type localTextTerminalTx struct {
	driver.Tx
	f          *localTextTerminalSQL
	state      LocalTerminalCompletionSnapshot
	physical   map[string][][]driver.Value
	projection map[string]domain.FrontendProjection
	writes     int
	expiry     []driver.Value
	done       bool
}

func cloneLocalTextTerminalState(v LocalTerminalCompletionSnapshot) LocalTerminalCompletionSnapshot {
	v.Events = append([]domain.SessionEvent(nil), v.Events...)
	m := make(map[string][]driver.Value, len(v.material))
	for k, r := range v.material {
		m[k] = append([]driver.Value(nil), r...)
	}
	v.material = m
	i := make(map[string][]driver.Value, len(v.eventIndex))
	for k, r := range v.eventIndex {
		i[k] = append([]driver.Value(nil), r...)
	}
	v.eventIndex = i
	e := make(map[string]string, len(v.eventRecords))
	for k, r := range v.eventRecords {
		e[k] = r
	}
	v.eventRecords = e
	return v
}
func (f *localTextTerminalSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	f.seed.loop.callbackSQL = 0
	f.seed.loop.localWorkerLifecycleSQL.beginPhase("canonical terminal callback")
	f.markerSeen, f.rawCanonical = false, false
	f.rawStatements = 0
	tx, err := f.publicationConn.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	p := make(map[string][][]driver.Value, len(f.physical))
	for k, r := range f.physical {
		p[k] = cloneGenerationRows(r)
	}
	projections := make(map[string]domain.FrontendProjection, len(f.projection))
	for k, r := range f.projection {
		projections[k] = r
	}
	return &localTextTerminalTx{Tx: tx, f: f, state: cloneLocalTextTerminalState(f.state), physical: p, projection: projections, writes: f.state.Mutations, expiry: append([]driver.Value(nil), f.seed.loop.base.expiry...)}, nil
}
func (tx *localTextTerminalTx) Commit() error {
	mutated := tx.f.state.Mutations > tx.writes
	mode := ""
	if mutated && tx.f.completionFault != "" {
		mode = tx.f.completionFault
		tx.f.completionFault = ""
		tx.f.lostACKs++
		tx.f.seed.loop.base.commitModes = []string{mode}
	}
	err := tx.Tx.Commit()
	tx.done = true
	if mode == "rollback" {
		tx.restore()
	} else if err == nil || mode == "ambiguous" {
		if mutated {
			tx.f.state.Commits++
		}
	}
	return err
}
func (tx *localTextTerminalTx) Rollback() error {
	if !tx.done {
		tx.restore()
		tx.done = true
	}
	return tx.Tx.Rollback()
}
func (tx *localTextTerminalTx) restore() {
	tx.f.state = tx.state
	tx.f.physical = tx.physical
	tx.f.projection = tx.projection
	tx.f.seed.loop.base.expiry = append([]driver.Value(nil), tx.expiry...)
}
func (f *localTextTerminalSQL) trace(s string, err error) {
	f.lastSQL = completionSQL(s)
	if len(f.lastSQL) > 180 {
		f.lastSQL = f.lastSQL[:180]
	}
	f.lastError = ""
	if err != nil {
		f.lastError = err.Error()
		if len(f.lastError) > 160 {
			f.lastError = f.lastError[:160]
		}
	}
}
func (f *localTextTerminalSQL) exact(args []driver.NamedValue, want ...driver.Value) bool {
	return f.seed.loop.exactArgs(args, want...)
}
func (f *localTextTerminalSQL) rawCount(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("unbounded canonical helper")
	}
	f.rawStatements++
	f.maxRawStatements = max(f.maxRawStatements, f.rawStatements)
	return nil
}
func (f *localTextTerminalSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Rows, err error) {
	if s == localCompactionEventsSQL {
		if e := f.seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return f.publication.base.compactionSourceSQL.QueryContext(ctx, s, args)
	}
	defer func() { f.trace(s, err) }()
	seed := f.seed
	r := seed.peer.base.request
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	empty := func() (driver.Rows, error) { return &compactionSourceRows{}, nil }
	flat := completionSQL(s)
	// The first raw helper read marks the production's documented SAME-sql.Tx
	// canonical handoff, after its owned-q rejoin. No q reads follow its writes.
	if f.markerSeen && flat == "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2" {
		f.rawCanonical = true
	}
	if f.rawCanonical {
		if e := f.rawCount(ctx); e != nil {
			return nil, e
		}
		return f.rawQuery(ctx, s, args)
	}
	if s == localTerminalMarkerSQL {
		if !f.exact(args, string(r.TenantID), string(r.RunID)) {
			return nil, errors.New("terminal marker exact Run")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		f.markerSeen = true
		if f.state.Finalizations == 0 {
			return empty()
		}
		return row([]driver.Value{f.state.FinalStatus, f.state.FinalDigest})
	}
	if s == localTerminalEvidenceSQL {
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("terminal evidence exact Attempt")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row([]driver.Value{f.state.EvidenceDigest, f.state.EvidenceRecord})
	}
	if len(args) >= 3 && len(args) <= 27 && s == localTerminalReplayEventsSQL(len(args)-2) {
		if args[0].Value != string(r.TenantID) || args[1].Value != string(r.SessionID) {
			return nil, errors.New("terminal replay exact Session")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		seen := map[string]bool{}
		var rows [][]driver.Value
		for _, a := range args[2:] {
			key, ok := a.Value.(string)
			if !ok || seen[key] {
				return nil, errors.New("terminal replay unique keys")
			}
			seen[key] = true
			if i, ok := f.state.eventIndex[key]; ok {
				if record, ok := f.state.eventRecords[i[1].(string)]; ok {
					rows = append(rows, []driver.Value{i[0], i[1], record})
				}
			}
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i][0].(int64) < rows[j][0].(int64) })
		return &compactionSourceRows{rows: rows}, nil
	}
	manifest, _, _ := ownedCanonicalJSONSelect("SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2")
	if s == manifest && len(args) == 2 && f.state.Manifest.ID != "" && args[1].Value == string(f.state.Manifest.ID) {
		if args[0].Value != string(r.TenantID) {
			return nil, errors.New("terminal manifest exact tenant")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		raw, e := json.Marshal(f.state.Manifest)
		if e != nil {
			return nil, e
		}
		return row([]driver.Value{string(raw)})
	}
	if flat == "SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2" {
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("terminal deletion exact Session")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return empty()
	}
	if s == boundedRunPayloadSQL && f.publication.commits > 0 {
		f.publication.readbacks++
	}
	return f.seed.loop.QueryContext(ctx, s, args)
}

func (f *localTextTerminalSQL) rawQuery(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	seed := f.seed
	r := seed.peer.base.request
	flat := completionSQL(s)
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	empty := func() (driver.Rows, error) { return &compactionSourceRows{}, nil }
	switch flat {
	case completionSQL(localTerminalEvidenceSQL):
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical original evidence key")
		}
		if f.state.EvidenceRecord == "" {
			return empty()
		}
		return row([]driver.Value{f.state.EvidenceDigest, f.state.EvidenceRecord})
	case "SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.RunID)) {
			return nil, errors.New("canonical real lease key")
		}
		head := seed.peer.base.rows["lease_heads"]
		if len(head) == 0 {
			return empty()
		}
		if len(head) != 8 {
			return nil, errors.New("canonical lease head shape")
		}
		return row([]driver.Value{head[2], head[5], head[6]})
	case "SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical Session key")
		}
		raw, e := json.Marshal(f.state.Session)
		if e != nil {
			return nil, e
		}
		return row([]driver.Value{string(raw)})
	case "SELECT record FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.BindingID)) {
			return nil, errors.New("canonical original binding key")
		}
		return row(seed.peer.base.rows["frontend_bindings"])
	case "SELECT record FROM session_displays WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical original display key")
		}
		return row(seed.peer.base.rows["session_displays"])
	case "SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical deletion key")
		}
		return empty()
	case "SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2":
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical deletion record key")
		}
		return row(seed.peer.base.rows["session_deletions"])
	case "SELECT binding_id FROM frontend_bindings_by_session WHERE tenant_id = $1 AND session_id = $2 LIMIT $3":
		if !f.exact(args, string(r.TenantID), string(r.SessionID), int64(maxSessionDeletionRows+1)) {
			return nil, errors.New("canonical original binding range")
		}
		// Original setup retained the legacy binding row, not this index.
		return empty()
	case "SELECT completed_at FROM session_lifecycle_backfill_state WHERE backfill_id = $1":
		if !f.exact(args, sessionLifecycleBackfillID) {
			return nil, errors.New("canonical exact lifecycle marker")
		}
		return empty()
	case "SELECT binding_id FROM frontend_bindings WHERE tenant_id = $1 AND session_id = $2 LIMIT $3":
		if !f.exact(args, string(r.TenantID), string(r.SessionID), int64(maxSessionDeletionRows+1)) {
			return nil, errors.New("canonical original legacy binding range")
		}
		var b domain.FrontendBinding
		if json.Unmarshal([]byte(seed.peer.base.rows["frontend_bindings"][0].(string)), &b) != nil {
			return nil, errors.New("canonical original binding")
		}
		return row([]driver.Value{string(b.ID)})
	case "SELECT queue_depth, active_runs FROM tenant_scheduler_counters WHERE tenant_id = $1":
		if !f.exact(args, string(r.TenantID)) {
			return nil, errors.New("canonical counter key")
		}
		return row(seed.peer.base.counters)
	case "SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at, SUBSTRING(local_capacity_association,0,32769), SUBSTRING(CAST(local_capacity_digest AS String),0,65) FROM subscription_scheduler_slots WHERE tenant_id = $1 AND subscription_connection_id = $2":
		physical, e := encodeCapacityStorageV1(seed.Loaded.Run.TenantID, "", seed.Loaded.Run.LocalCapacityAssociation)
		if e != nil || !f.exact(args, string(r.TenantID), physical.key) {
			return nil, errors.New("canonical original local slot key")
		}
		return row(seed.peer.base.slot)
	case completionSQL(sessionMaterialCurrentSQL):
		if !f.exact(args, string(r.TenantID), string(r.SessionID)) {
			return nil, errors.New("canonical material current key")
		}
		return row(f.state.material["current"])
	case completionSQL(sessionMaterialBoundarySQL):
		if len(args) != 3 || args[0].Value != string(r.TenantID) || args[1].Value != string(r.SessionID) {
			return nil, errors.New("canonical material boundary key")
		}
		return row(f.state.material[fmt.Sprint(args[2].Value)])
	}
	// Every remaining helper query must already be in the exact original
	// canonical/lifecycle inventory. This is not a table-name SQL fallback.
	if _, ok := seed.peer.base.reads[flat]; ok {
		return seed.peer.base.QueryContext(ctx, s, args)
	}
	if s == quotaReservationStorageSQLV1 || s == quotaExpiryStorageSQLV1 {
		return seed.peer.QueryContext(ctx, s, args)
	}
	for statement := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(statement)
		if s == bounded {
			return seed.peer.QueryContext(ctx, s, args)
		}
	}
	return nil, fmt.Errorf("unregistered canonical terminal read: %s", flat)
}

const localTextTerminalSlotSQL = `UPSERT INTO subscription_scheduler_slots (tenant_id, subscription_connection_id, state, active_run_id, active_reservation_id, blocked_until, updated_at, local_capacity_association, local_capacity_digest) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

var localTextTerminalProjectionSQL = map[string]int{
	"INSERT INTO frontend_projection_outbox (tenant_id, frontend_projection_id, session_id, event_id, event_sequence, binding_id, binding_revision, frontend, status, created_at, updated_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS JsonDocument))": 12,
	"INSERT INTO frontend_projections_by_session (tenant_id, session_id, frontend_projection_id) VALUES ($1, $2, $3)":                                                                                                                                                                   3,
	"INSERT INTO frontend_projections_by_run (tenant_id, run_id, frontend, frontend_projection_id) VALUES ($1, $2, $3, $4)":                                                                                                                                                             4,
	"INSERT INTO frontend_projection_ready_v1 (frontend, shard_bucket, created_at, tenant_id, frontend_projection_id, run_id) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                                                          6,
}

func (f *localTextTerminalSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func() { f.trace(s, err) }()
	seed := f.seed
	if s == localRetainedRunUpdateSQL {
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return f.publicationConn.ExecContext(ctx, s, args)
	}
	if !f.rawCanonical {
		return f.seed.loop.ExecContext(ctx, s, args)
	}
	if e := f.rawCount(ctx); e != nil {
		return nil, e
	}
	flat := completionSQL(s)
	values := make([]driver.Value, len(args))
	for i, a := range args {
		values[i] = a.Value
	}
	if flat == localTextTerminalSlotSQL {
		previous := seed.peer.base.slot
		if len(args) != 9 || len(previous) != 7 || args[0].Value != string(seed.Event.TenantID) || args[7].Value != previous[5] || args[8].Value != previous[6] {
			return nil, errors.New("terminal local slot exact original association")
		}
		physical, e := encodeCapacityStorageV1(seed.Loaded.Run.TenantID, "", seed.Loaded.Run.LocalCapacityAssociation)
		if e != nil || args[1].Value != physical.key {
			return nil, errors.New("terminal local slot physical key")
		}
		seed.peer.base.slot = []driver.Value{values[2], values[3], values[4], values[5], values[6], values[7], values[8]}
		f.state.ActiveRun = values[3].(string)
		f.state.ActiveReservation = values[4].(string)
		f.state.Mutations++
	} else if n, ok := localTextTerminalProjectionSQL[flat]; ok {
		if len(args) != n {
			return nil, errors.New("terminal projection exact write shape")
		}
		if strings.HasPrefix(flat, "INSERT INTO frontend_projection_outbox ") {
			var p domain.FrontendProjection
			var b domain.FrontendBinding
			if json.Unmarshal([]byte(values[11].(string)), &p) != nil || json.Unmarshal([]byte(seed.peer.base.rows["frontend_bindings"][0].(string)), &b) != nil {
				return nil, errors.New("terminal projection body")
			}
			var event domain.SessionEvent
			for _, v := range f.state.Events {
				if v.ID == p.EventID {
					event = v
				}
			}
			if p.ValidateFor(event, b) != nil {
				return nil, errors.New("terminal projection original binding/event")
			}
			if _, ok := f.projection[string(p.ID)]; ok {
				return nil, errors.New("duplicate terminal projection")
			}
			f.projection[string(p.ID)] = p
		}
		f.state.Mutations++
	} else {
		format := terminalCompletionLocalV2
		// Only the actual private F1 writer's failed Run postimage selects its
		// storage discriminator. Existing success/cancellation remain V2.
		var run domain.Run
		if len(seed.peer.base.rows["runs"]) == 1 && json.Unmarshal([]byte(seed.peer.base.rows["runs"][0].(string)), &run) == nil && run.TerminalFailure != nil {
			format = terminalCompletionLocalF1
		}
		if e := applyTerminalCompletionWrite(&f.state, seed.peer.base.rows, &seed.peer.base.counters, format, s, args); e != nil {
			return nil, e
		}
	}
	// Store the exact physical mutation arguments as well as typed rows. All
	// rollback snapshots include these indexes/audit/activity projection effects.
	f.physical[flat] = append(f.physical[flat], values)
	switch {
	case strings.HasPrefix(flat, "DELETE FROM quota_expiry_v2 "):
		old := seed.peer.base.rows["quota_expiry_v2"]
		if len(old) != 8 || !f.exact(args, old[:4]...) {
			return nil, errors.New("terminal original quota expiry deletion")
		}
		seed.peer.base.rows["quota_expiry_v2"] = nil
		seed.peer.base.expiry = nil
	case strings.HasPrefix(flat, "DELETE FROM lease_heads "):
		seed.peer.base.rows["lease_heads"] = nil
	case strings.HasPrefix(flat, "DELETE FROM lease_expiry_v2 "):
		seed.peer.base.rows["lease_expiry_v2"] = nil
	case strings.HasPrefix(flat, "UPDATE sessions "):
		seed.peer.base.rows["sessions"] = []driver.Value{values[4]}
	case strings.HasPrefix(flat, "UPSERT INTO session_displays "):
		seed.peer.base.rows["session_displays"] = []driver.Value{values[3]}
	}
	return driver.RowsAffected(1), nil
}

// DATA-only exact two-head SQL shape. Both absent-service point reads continue
// through the original physical projection fixture, including all scalars.
func (f *localTextWebSQL) registerCanonicalReads(r ports.CanonicalUserEventCommit) {
	tenant, session, user := string(r.TenantID), string(r.SessionID), string(r.UserID)
	bucket, _ := ydbpartition.BucketV1(user)
	var sessionHead domain.Session
	if len(f.rows["sessions"]) == 1 {
		_ = json.Unmarshal([]byte(f.rows["sessions"][0].(string)), &sessionHead)
	}
	sequence := int64(sessionHead.LastEventSequence + 1)
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
			key = []driver.Value{tenant, session, sequence}
		case "dispatch_outbox":
			key = []driver.Value{tenant, string(r.DispatchID)}
		}
		if key != nil {
			f.addRead(bounded, table, key...)
		}
	}
	f.addRead(ownedCanonicalIngressIdempotencySQL, "frontend_ingress_idempotency", tenant, string(r.BindingID), string(r.IdempotencyKey))
	f.addRead(sessionMaterialCurrentSQL, "session_material_current_v1", tenant, session)
	f.addRead(sessionMaterialBoundarySQL, "session_material_boundaries_v1", tenant, session, int64(sessionHead.LastEventSequence))
	f.addRead(ownedActiveSessionParticipantsSQL, "active_session_participants", tenant, session, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	f.addRead("SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4", "session_participants", tenant, session, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	f.addRead("SELECT SUBSTRING(CAST(run_id AS String),0,161) FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2", "run_idempotency", tenant, string(r.IdempotencyKey))
	f.addRead("SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2", "run_explanation_heads_v1", tenant, string(r.RunID))
	f.addRead("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_displays WHERE tenant_id = $1 AND session_id = $2", "session_displays", tenant, session)
	f.addRead("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3", "session_events", tenant, session, sequence)
	f.addRead("SELECT SUBSTRING(CAST(payload AS String),0,131073) FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2", "dispatch_outbox", tenant, string(r.DispatchID))
	f.addRead("SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", "runs", tenant, string(r.RunID))
	f.addRead("SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2", "worker_jobs", tenant, string(r.RunID))
	f.addRead("SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2", "attempts", tenant, string(r.AttemptID))
	f.addRead("SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2", "artifact_manifests", tenant, string(r.ManifestID))
	f.addRead("SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1", "checkpoints", tenant, string(r.AttemptID))
}

type localTextManagerSQL struct {
	*localTextTerminalSQL
	leaseWrites, runningRunWrites, runningAttemptWrites, preleasePrincipals int
}

func (f *localTextManagerSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localTextManagerSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	seed := f.seed
	r := seed.peer.base.request
	flat := completionSQL(s)
	if s == quotaExpiryStorageSQLV1 && len(seed.peer.base.rows["quota_expiry_v2"]) == 0 {
		bucket, e := ydbpartition.BucketV1(string(seed.Loaded.Reservation.ID))
		if e != nil || !f.exact(args, int64(bucket), seed.Loaded.Reservation.ExpiresAt, string(r.TenantID), string(seed.Loaded.Reservation.ID)) {
			return nil, errors.New("canonical Manager absent original terminal expiry key")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if flat == "SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1" {
		if !f.exact(args, string(r.TenantID), string(r.AttemptID)) {
			return nil, errors.New("canonical Manager exact checkpoint scope")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if flat == "SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2" && len(seed.peer.base.rows["leases"]) == 0 {
		if len(args) != 2 || args[0].Value != string(r.TenantID) {
			return nil, errors.New("canonical Manager absent lease exact tenant")
		}
		id, ok := args[1].Value.(string)
		if !ok || domain.LeaseID(id).Validate() != nil {
			return nil, errors.New("canonical Manager absent lease key")
		}
		if e := seed.loop.countStatement(ctx); e != nil {
			return nil, e
		}
		return &compactionSourceRows{}, nil
	}
	if s == ownedAdmissionPrincipalSQL && len(seed.peer.base.rows["lease_heads"]) == 0 {
		f.preleasePrincipals++
	}
	return f.localTextTerminalSQL.QueryContext(ctx, s, args)
}
func (f *localTextManagerSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	seed := f.seed
	flat := completionSQL(s)
	if flat == localLifecycleLeaseWrite {
		var lease domain.Lease
		if len(args) != 10 || json.Unmarshal([]byte(args[9].Value.(string)), &lease) != nil || lease.TenantID != seed.Loaded.Run.TenantID || lease.RunID != seed.Loaded.Run.ID || lease.AttemptID != seed.Loaded.Attempt.ID || lease.WorkerID != seed.peer.worker || lease.FenceToken != 1 || lease.ValidateForAttempt(seed.Loaded.Run, seed.Loaded.Attempt) != nil {
			return nil, errors.New("canonical Manager actual lease projection")
		}
		if seed.peer.leaseID != "" && seed.peer.leaseID != lease.ID {
			return nil, errors.New("canonical Manager replaced original lease identity")
		}
		// Fixture routing learns only the ID already supplied to an actual
		// physical UPSERT. No domain Lease/authority is created by this assignment.
		seed.peer.leaseID = lease.ID
	}
	result, err := f.localTextTerminalSQL.ExecContext(ctx, s, args)
	if err == nil {
		switch flat {
		case localLifecycleLeaseWrite:
			f.leaseWrites++
		}
		if !f.rawCanonical {
			if entry, ok := localCanonicalJoinWrites[flat]; ok {
				if entry.table == "runs" {
					var r domain.Run
					if json.Unmarshal([]byte(args[entry.payload].Value.(string)), &r) == nil && r.Status == domain.RunRunning {
						f.runningRunWrites++
					}
				}
				if entry.table == "attempts" {
					var a domain.Attempt
					if json.Unmarshal([]byte(args[entry.payload].Value.(string)), &a) == nil && a.Status == domain.AttemptRunning {
						f.runningAttemptWrites++
					}
				}
			}
		}
	}
	return result, err
}

type LocalTextWebSeed struct {
	Source  *localoperator.Source
	Store   *Store
	DB      *sql.DB
	Options Options
	Tenant  domain.TenantID
	Owner   domain.UserID
	Session domain.SessionID
	peer    *localTextWebSQL
}

type localTextBinder struct {
	ports.HarnessBinder
	f *localTextWebSQL
}

func (b localTextBinder) BindHarness(ctx context.Context, r ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	textOnlyRegisterAttemptKeys(b.f.install, domain.AttemptExecutionSourceScopeV1{TenantID: r.TenantID, AttemptID: r.AttemptID})
	result, err := b.HarnessBinder.BindHarness(ctx, r)
	if err != nil {
		b.f.lastError = "text actual binder: " + err.Error()
	}
	return result, err
}
func (s *LocalTextWebSeed) Binder(t *testing.T) ports.HarnessBinder {
	t.Helper()
	b, err := NewLocalHarnessBinder(s.Store, s.Source, s.peer.install.receipt)
	if err != nil {
		t.Fatal(err)
	}
	return localTextBinder{b, s.peer}
}
func (s *LocalTextWebSeed) IngressStore() ports.CanonicalIngressStore {
	return localTextIngress{s.Store, s.peer}
}
func (s *LocalTextWebSeed) Resources(t *testing.T) ports.WebResourceStore {
	t.Helper()
	r, err := NewLocalWebResourceStore(s.Store, s.Source, s.peer.install.receipt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (s *LocalTextWebSeed) Empty() bool {
	f := s.peer
	return len(f.install.sql.fundingHeads) == 0 && len(f.install.sql.fundingAudits) == 0 && len(f.rows["runs"]) == 0 && len(f.rows["attempts"]) == 0 && len(f.rows["dispatch_outbox"]) == 0 && len(f.rows["worker_jobs"]) == 0 && len(f.rows["leases"]) == 0 && len(f.rows["frontend_bindings"]) == 0 && f.bindings == 0
}
func (s *LocalTextWebSeed) FundingCounts() (int, int) {
	return len(s.peer.install.sql.fundingHeads), len(s.peer.install.sql.fundingAudits)
}
func (s *LocalTextWebSeed) LastError() string {
	counts := ""
	if b := s.peer.budget; b != nil {
		b.mu.Lock()
		counts = fmt.Sprintf(" statements=%d/%d callbacks=%d failed=%t bytes=%d", b.callbackStatements, b.statements, b.callbacks, b.failed, b.bytes)
		b.mu.Unlock()
	}
	return s.peer.lastError + " lastSQL=" + s.peer.lastSQL + counts
}

type LocalTextManagerSeed struct {
	*LocalCanonicalLoopSeed
	manager *localTextManagerSQL
}

func (s *LocalTextWebSeed) OriginalReceipt() ports.OwnedProfilePublicationReceiptV1 {
	r := s.peer.install.receipt
	r.Profile = r.Profile.Clone()
	return r
}
func (s *LocalTextManagerSeed) TerminalSnapshot(t *testing.T) LocalTerminalCompletionSnapshot {
	t.Helper()
	v := cloneLocalTextTerminalState(s.manager.state)
	base := s.peer.base
	for table, target := range map[string]any{"runs": &v.Run, "attempts": &v.Attempt, "quota_reservations": &v.Reservation} {
		if len(base.rows[table]) != 1 || json.Unmarshal([]byte(base.rows[table][0].(string)), target) != nil {
			t.Fatal("text original terminal row missing", table)
		}
	}
	v.LeasePresent = len(base.rows["lease_heads"]) != 0
	if len(base.counters) == 2 {
		v.ActiveRuns = uint32(base.counters[1].(int64))
		v.CountersPresent = true
	}
	return v
}
func (s *LocalTextManagerSeed) StorageReleased() bool {
	base := s.peer.base
	return len(base.rows["quota_expiry_v2"]) == 0 && len(base.rows["lease_heads"]) == 0 && len(base.rows["lease_expiry_v2"]) == 0 && len(base.slot) == 7 && base.slot[1] == "" && base.slot[2] == "" && reflect.DeepEqual(base.counters, []driver.Value{int64(0), int64(0)})
}
func (s *LocalTextManagerSeed) Trace() string { return s.manager.lastSQL + " / " + s.manager.lastError }
func (s *LocalTextManagerSeed) ManagerLifecycle() (int, int, int, int) {
	return s.manager.preleasePrincipals, s.manager.leaseWrites, s.manager.runningRunWrites, s.manager.runningAttemptWrites
}

// Adversarial current-row DATA after the real MODEL request body has been
// produced. No capability, Source grant or terminal writer is supplied here.
// The normal runtime must rejoin these actual rows before reserving/sending.
func (s *LocalTextWebSeed) InvalidatePreparedModelCurrent(t *testing.T, kind string) {
	t.Helper()
	f := s.peer
	if f.manager == nil || f.manager.seed.loop.operations.operation != nil {
		t.Fatal("current-row mutation must follow preparation but precede MODEL reserve")
	}
	switch kind {
	case "stale_fence":
		head := f.rows["lease_heads"]
		var lease domain.Lease
		if len(head) != 8 || len(f.rows["leases"]) != 1 || json.Unmarshal([]byte(f.rows["leases"][0].(string)), &lease) != nil || head[5] != int64(lease.FenceToken) {
			t.Fatal("actual original lease projections missing")
		}
		lease.FenceToken++
		if lease.ValidateForAttempt(f.manager.seed.Loaded.Run, f.manager.seed.Loaded.Attempt) != nil {
			t.Fatal("coherent newer lease DATA")
		}
		raw, err := json.Marshal(lease)
		if err != nil {
			t.Fatal(err)
		}
		f.rows["leases"][0] = string(raw)
		head[5] = int64(lease.FenceToken)
	case "cancel_requested":
		var run domain.Run
		if len(f.rows["runs"]) != 1 || json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || run.Status != domain.RunRunning {
			t.Fatal("actual running Run missing")
		}
		at := run.UpdatedAt
		run.CancellationRequestedAt = &at
		if run.Validate() != nil {
			t.Fatal("coherent cancelled current Run DATA")
		}
		ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
	case "revoked_current":
		p := f.install.receipt.Profile.Clone()
		p.State, p.RevocationGeneration = domain.OwnedAgentCurrentRevokedV1, p.RevocationGeneration+1
		raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
		if err != nil {
			t.Fatal("coherent current profile tombstone DATA", err)
		}
		digest, err := p.Digest()
		if err != nil {
			t.Fatal(err)
		}
		row := f.install.sql.heads[localInstallationHeadKey(p)]
		if len(row) != 11 {
			t.Fatal("actual published profile row missing")
		}
		row[0], row[3], row[4], row[5], row[10] = string(raw), int64(p.RevocationGeneration), string(p.State), digest, p.UpdatedAt
	default:
		t.Fatal("unknown current-row DATA mutation", kind)
	}
}

func (s *LocalTextWebSeed) AdmitManager(t *testing.T) *LocalTextManagerSeed {
	t.Helper()
	f := s.peer
	r := f.request
	priorCount := 0
	if f.priorHistory != nil {
		priorCount = len(f.priorHistory.runs)
	}
	if r.PreparedMaterial == nil || len(f.rows["runs"]) != 1 || len(f.rows["attempts"]) != 1 || len(f.install.sql.fundingHeads) != 2*(priorCount+1) || len(f.install.sql.fundingAudits) != 2*(priorCount+1) || f.bindings != 1 {
		t.Fatal("text Web failed to create actual original request/two heads")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admission := ports.DispatchAdmissionRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, OutboxID: r.DispatchID, ReservationID: domain.QuotaReservationID("reservation-" + string(r.AttemptID)), Now: f.at, HoldUntil: f.at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if result, err := s.Store.AdmitDispatch(ctx, admission); err != nil || !result.Admitted {
		t.Fatalf("actual text admission admitted=%t err=%v SQL=%s", result.Admitted, err, f.lastError)
	}
	f.addRead("SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", "quota_reservations", string(r.TenantID), string(admission.ReservationID))
	parent := &ownedAttemptEffectParentSQLFixture{base: f.ownedFinalScript, graph: f.managedAdmissionGraphFixture}
	alloc := &allocationSQLFixture{parent: parent}
	peer := &canonicalSessionSQL{localWorkerLifecycleSQL: &localWorkerLifecycleSQL{localCanonicalJoinSQL: f.localCanonicalJoinSQL, worker: "worker-local-text"}, base: f.localCanonicalJoinSQL, parent: parent, alloc: alloc}
	f.sessionPeer = peer
	operations := &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{alloc}}}
	loop := &canonicalLoopSQL{canonicalSessionSQL: peer, operations: operations}
	prepare, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{19}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{23}, 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	var event domain.SessionEvent
	if json.Unmarshal([]byte(f.rows["session_events"][0].(string)), &event) != nil || event.ID != r.EventID {
		t.Fatal("text original canonical event absent")
	}
	loaded, found, err := s.Store.LoadWorkerJob(ctx, r.TenantID, r.RunID)
	if err != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatalf("text actual queued readback found=%t err=%v SQL=%s", found, err, f.lastError)
	}
	seed := &LocalCanonicalSessionSeed{Source: s.Source, DB: s.DB, Options: s.Options, Prepare: prepare, Operate: operate, Backend: loaded.Job.HarnessBinding.Backend, Substrate: *loaded.Job.SubstrateBinding, Loaded: loaded, Claim: ports.WorkerLeaseRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, WorkerID: peer.worker}, peer: peer, owner: t}
	loopSeed := &LocalCanonicalLoopSeed{LocalCanonicalSessionSeed: seed, Payload: event.Payload, Event: event, loop: loop}
	eventRows := [][]driver.Value{{f.rows["session_events"][0]}}
	for _, raw := range f.priorRecords {
		eventRows = append(eventRows, []driver.Value{raw})
	}
	sort.Slice(eventRows, func(i, j int) bool {
		var a, b domain.SessionEvent
		_ = json.Unmarshal([]byte(eventRows[i][0].(string)), &a)
		_ = json.Unmarshal([]byte(eventRows[j][0].(string)), &b)
		return a.Sequence < b.Sequence
	})
	rows := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: operations}, events: eventRows}
	effects := &compactionEffectSQL{compactionSourceSQL: rows}
	publication := &terminalPublicationSQL{base: &compactionPublicationSQL{compactionEffectSQL: effects}}
	terminal := &localTextTerminalSQL{seed: loopSeed, publication: publication, publicationConn: &terminalPublicationConn{Conn: publication.base, f: publication}, projection: map[string]domain.FrontendProjection{}, physical: map[string][][]driver.Value{}, state: LocalTerminalCompletionSnapshot{material: map[string][]driver.Value{}, eventIndex: map[string][]driver.Value{}, eventRecords: map[string]string{}}}
	if json.Unmarshal([]byte(f.rows["sessions"][0].(string)), &terminal.state.Session) != nil || terminal.state.Session.LastEventSequence != event.Sequence {
		t.Fatal("text original Session frontier")
	}
	terminal.state.material["current"] = append([]driver.Value(nil), f.rows["session_material_current_v1"]...)
	for key, row := range f.priorMaterial {
		if key != "current" {
			terminal.state.material[key] = append([]driver.Value(nil), row...)
		}
	}
	for id, raw := range f.priorRecords {
		var e domain.SessionEvent
		if json.Unmarshal([]byte(raw), &e) != nil || e.Validate() != nil {
			t.Fatal("actual previous canonical event")
		}
		terminal.state.eventRecords[id] = raw
		terminal.state.eventIndex[string(e.IdempotencyKey)] = []driver.Value{int64(e.Sequence), string(e.ID)}
	}
	terminal.state.material[fmt.Sprint(event.Sequence)] = append([]driver.Value(nil), f.rows["session_material_boundaries_v1"]...)
	terminal.state.eventIndex[string(event.IdempotencyKey)] = []driver.Value{int64(event.Sequence), string(event.ID)}
	terminal.state.eventRecords[string(event.ID)] = f.rows["session_events"][0].(string)
	manager := &localTextManagerSQL{localTextTerminalSQL: terminal}
	if len(f.rows["leases"]) != 0 || len(f.rows["lease_heads"]) != 0 || parent.parentRow != nil || alloc.row != nil || operations.operation != nil || operations.row != nil {
		t.Fatal("text queued setup fabricated invocation effects")
	}
	f.manager = manager
	return &LocalTextManagerSeed{LocalCanonicalLoopSeed: loopSeed, manager: manager}
}

// Switch the bounded storage peer to the next actual Web-created Run only after
// the original owner completed and released. Retain the exact durable history
// join bytes and canonical prefix, never a legacy or answer-only substitute.
func (s *LocalTextWebSeed) BeginNextAfterSuccess(t *testing.T, m *LocalTextManagerSeed) {
	t.Helper()
	f := s.peer
	state := m.TerminalSnapshot(t)
	if f.priorHistory != nil || f.manager != m.manager || !m.StorageReleased() || state.Run.Status != domain.RunSucceeded || state.Finalizations != 1 || state.Run.TerminalCustody == nil {
		t.Fatal("next text message before actual original terminal release")
	}
	p := f.sessionPeer.parent
	if len(p.parentRow) != 15 || len(p.accountingRow) != 4 || len(f.final) != 7 {
		t.Fatal("actual prior immutable terminal owners absent")
	}
	rawRun, err := json.Marshal(state.Run)
	if err != nil {
		t.Fatal(err)
	}
	rawManifest, err := json.Marshal(state.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	h := &LocalFreshRunHistorySQLFixture{tenant: s.Tenant, session: s.Session, runs: map[domain.RunID]string{state.Run.ID: string(rawRun)}, markers: map[domain.RunID][2]string{state.Run.ID: {state.FinalStatus, state.FinalDigest}}, details: map[domain.AttemptID][]driver.Value{}, identities: map[domain.AttemptID]freshRunHistoryIdentity{}, DetailReads: map[domain.RunID]int{}}
	h.details[state.Attempt.ID] = []driver.Value{p.parentRow[13], p.parentRow[14], f.final[0], f.final[1], state.EvidenceRecord, state.EvidenceDigest, state.FinalStatus, state.FinalDigest, string(rawManifest), p.accountingRow[2], p.accountingRow[3]}
	h.identities[state.Attempt.ID] = freshRunHistoryIdentity{run: state.Run.ID, manifest: state.Manifest.ID}
	f.priorHistory = h
	f.priorRecords = make(map[string]string, len(state.eventRecords))
	for id, raw := range state.eventRecords {
		f.priorRecords[id] = raw
	}
	f.priorMaterial = make(map[string][]driver.Value, len(state.material))
	for key, row := range state.material {
		f.priorMaterial[key] = append([]driver.Value(nil), row...)
	}
	f.rows["session_material_current_v1"] = append([]driver.Value(nil), state.material["current"]...)
	f.rows["session_material_boundaries_v1"] = append([]driver.Value(nil), state.material[fmt.Sprint(state.Session.LastEventSequence)]...)
	for _, table := range []string{"frontend_ingress_idempotency", "run_idempotency", "runs", "attempts", "run_explanation_heads_v1", "artifact_manifests", "leases", "worker_jobs", "dispatch_outbox", "quota_reservations"} {
		f.rows[table] = nil
	}
	f.final, f.index = nil, nil
	f.manager, f.sessionPeer = nil, nil
	f.canonical = true
}

func (s *LocalTextWebSeed) PriorHistoryReads() int {
	if s.peer.priorHistory == nil {
		return 0
	}
	return s.peer.priorHistory.HistoryReads
}

// Canonical historical DATA only, before the first actual user request/funding.
// The real preparation/measurement/fold establishes the complete prefix; no
// model, tool outcome, terminal receipt or serving grant is invented.
func (s *LocalTextWebSeed) SeedPriorCanonicalContext(t *testing.T, event domain.SessionEvent, body []byte, uploads []domain.Artifact) {
	t.Helper()
	f := s.peer
	if !s.Empty() || len(f.priorRecords) != 0 || event.TenantID != s.Tenant || event.SessionID != s.Session {
		t.Fatal("historical DATA must precede original Web request")
	}
	event.Sequence, event.CreatedAt = 1, f.at
	prepared, err := sessionmaterial.PrepareV1(event, body, uploads)
	if err != nil {
		t.Fatal("valid unsupported historical DATA envelope", err)
	}
	defer prepared.Release()
	measure, err := sessioncontext.MeasurePreparedRecordV1(event, prepared)
	if err != nil || !measure.Matches(event) {
		t.Fatal("actual historical record measurement", err)
	}
	genesis, err := sessioncontext.NewSessionMaterialGenesisV1(s.Tenant, s.Session)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := sessioncontext.FoldSessionMaterialV1(genesis, event, &measure)
	if err != nil {
		t.Fatal("actual historical prefix fold", err)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	f.priorRecords = map[string]string{string(event.ID): string(raw)}
	if event.RunID != nil {
		f.priorHistory = &LocalFreshRunHistorySQLFixture{tenant: s.Tenant, session: s.Session, runs: map[domain.RunID]string{}, markers: map[domain.RunID][2]string{}, DetailReads: map[domain.RunID]int{}}
	}
	f.priorMaterial = map[string][]driver.Value{"0": materialRow(t, genesis), "1": materialRow(t, prefix)}
	f.rows["session_material_current_v1"] = materialRow(t, prefix)
	f.rows["session_material_boundaries_v1"] = materialRow(t, prefix)
	var session domain.Session
	if json.Unmarshal([]byte(f.rows["sessions"][0].(string)), &session) != nil {
		t.Fatal("original Session DATA")
	}
	session.LastEventSequence = 1
	ratedFixturePut(t, f.ratedSQLFixture, "sessions", session)
}

// Structurally valid unsupported retained-head DATA, not a published
// compaction or authenticated operation. The normal original/current capture
// must deny it; this deliberately makes no claim about the later parent guard.
func (s *LocalTextWebSeed) SetUnsupportedRetainedHeadData(t *testing.T) {
	t.Helper()
	f := s.peer
	if f.manager == nil || len(f.rows["runs"]) != 1 {
		t.Fatal("actual queued text Run required")
	}
	var run domain.Run
	if json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || run.Status != domain.RunQueued {
		t.Fatal("actual queued Run DATA")
	}
	h := strings.Repeat("a", 64)
	policy, err := f.manager.seed.Loaded.Job.HarnessBinding.AgentLoopPolicy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := f.manager.seed.Loaded.Job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	scope := domain.AttemptOperationScopeV1{TenantID: s.Tenant, SessionID: s.Session, OwnerUserID: s.Owner, RunID: run.ID, AttemptID: f.request.AttemptID, LeaseID: "unsupported-retained-lease", FenceToken: 1, PhysicalInvocationClaimID: "unsupported-retained-claim", EffectReservationDigest: domain.AttemptEffectReservationDigestV1(h), InvocationAuthorityDigest: domain.ServerlessInvocationAuthorityDigestV1(h), HarnessBindingDigest: binding, AgentLoopPolicyDigest: policy}
	plan := domain.DerivedContextPlanV1{Version: 1, Scope: scope, Generation: 1, OperationSequence: 1, ReservationDigest: domain.AttemptOperationReservationDigestV1(h), ObservationDigest: domain.AttemptOperationObservationDigestV1(h), NormalizedResponseDigest: h, SourceViewDigest: h, Summary: domain.BlobRef{TenantID: s.Tenant, Size: 1, SHA256: h}, Manifest: domain.BlobRef{TenantID: s.Tenant, Size: 1, SHA256: h}}
	plan.Summary.Key, err = domain.DerivedContextObjectKeyV1(scope, 1, "summary", h)
	if err != nil {
		t.Fatal(err)
	}
	plan.Manifest.Key, err = domain.DerivedContextObjectKeyV1(scope, 1, "manifest", h)
	if err != nil || plan.Validate() != nil {
		t.Fatal("typed valid unsupported retained DATA", err)
	}
	digest, err := plan.Digest()
	if err != nil {
		t.Fatal(err)
	}
	run.DerivedContext = &domain.RunDerivedContextV1{Version: 1, Inventory: []domain.DerivedContextPlanV1{plan}, Head: &domain.DerivedContextHeadV1{Version: 1, PlanDigest: digest}}
	if run.Validate() != nil {
		t.Fatal("typed valid retained-head Run DATA")
	}
	ratedFixturePut(t, f.ratedSQLFixture, "runs", run)
}

func NewLocalTextWebSeed(t *testing.T, endpoint string, configure ...func(*localoperator.ManifestV1)) *LocalTextWebSeed {
	t.Helper()
	original := newLocalTextInstallation(t, endpoint, configure...)
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
	f := &localTextWebSQL{localCanonicalJoinSQL: base, install: original, scope: scope}
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
	return &LocalTextWebSeed{Source: original.source, Store: original.store, DB: db, Options: Options{OwnedProfilePublicationVerifier: original.source, AttemptSpendingPublicationVerifier: original.source, OwnedAttemptEffectSourceVerifier: original.source}, Tenant: scope.TenantID, Owner: scope.OwnerUserID, Session: scope.SessionID, peer: f}
}

type localTextSourcesSQL struct {
	*localInstallationSQLFixture
	absenceOverride []driver.Value // adversarial read-view only, never source/publication DATA
}

func (f *localTextSourcesSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *localTextSourcesSQL) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query != ownedTextAdmissionSourcesJoinSQL {
		return f.localInstallationSQLFixture.QueryContext(ctx, query, args)
	}
	values := localInstallationNamedValues(args)
	if len(values) != 17 {
		return nil, errors.New("text source join requires original profile and two exact keys")
	}
	profile := f.pendingHeads[localInstallationSQLKey(values[:9])]
	if len(profile) != 11 {
		return &explanationFixtureRows{}, nil
	}
	p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(profile[0].(string)))
	if err != nil || p.Locator.Profile != domain.AgentLoopOwnedLocalTextV1 {
		return nil, errors.New("text source join has no original text profile")
	}
	rows := append([]driver.Value(nil), profile...)
	for i, kind := range ownedSpendingKinds[:2] {
		key := values[9+4*i : 13+4*i]
		if !reflect.DeepEqual(key, []driver.Value{string(p.Locator.TenantID), key[1], string(kind), string(p.BillingOwnerUserID)}) {
			return nil, errors.New("text source join crossed actual component/payer")
		}
		row := f.pendingFundingHeads[localInstallationSQLKey(key)]
		if len(row) == 0 {
			return &explanationFixtureRows{}, nil
		}
		if len(row) != 11 {
			return nil, errors.New("text source physical head shape")
		}
		rows = append(rows, row...)
	}
	var absence []driver.Value
	for _, kind := range ownedSpendingKinds[2:] {
		key := localInstallationSQLKey([]driver.Value{values[9], values[10], string(kind), values[12]})
		stored, present := f.pendingFundingHeads[key]
		if !present {
			absence = append(absence, make([]driver.Value, 12)...)
			continue
		}
		absence = append(absence, string(kind))
		if len(stored) != 11 {
			return nil, errors.New("text absence physical row shape")
		}
		caps := [11]int{32769, 21, 21, 17, 65, 161, 161, 161, 65, 65, 65}
		for i, value := range stored {
			if value == nil {
				absence = append(absence, nil)
				continue
			}
			var scalar string
			switch v := value.(type) {
			case time.Time:
				scalar = v.UTC().Format(time.RFC3339Nano)
			case []byte:
				scalar = string(v)
			default:
				scalar = fmt.Sprint(v)
			}
			if len(scalar) > caps[i] {
				scalar = scalar[:caps[i]]
			}
			absence = append(absence, scalar)
		}
	}
	if f.absenceOverride != nil {
		if len(f.absenceOverride) != 24 {
			return nil, errors.New("text absence override fixed projection shape")
		}
		absence = append([]driver.Value(nil), f.absenceOverride...)
	}
	rows = append(rows, absence...)
	f.statements = append(f.statements, query)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	return &explanationFixtureRows{values: rows}, nil
}
