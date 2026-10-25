package ydbstore

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// TEST ONLY: raw constructor dependencies, not an existing Store/bootstrap,
// parent reservation, body adapter or approval. Funding/authentication use the
// original Source and Store; SQL and service declarations remain synthetic,
// not normal activation or live service proof.
type LocalSessionConnectedSeed struct {
	Source                       *localoperator.Source
	DB                           *sql.DB
	Options                      Options
	Prepare                      *serverlessharness.CapabilityIssuer
	Operate                      *serverlessharness.OperationCapabilityIssuer
	Backend                      domain.HarnessBackendDescriptorV1
	Substrate                    domain.SubstrateBindingV1
	Request                      ports.ReserveAttemptEffectRequestV1
	Profile                      domain.ConfiguredInferenceProfileV1
	Scope                        domain.AttemptExecutionSourceScopeV1
	parent                       *ownedAttemptEffectParentFixture
	sql                          *loopbackOperationSQL
	compaction                   *compactionPublicationSQL
	installationRoot, executable string
	receipt                      ports.OwnedProfilePublicationReceiptV1
}

func NewLocalSessionConnectedSeed(t *testing.T, endpoint string) *LocalSessionConnectedSeed {
	return newLocalSessionConnectedSeed(t, endpoint, false, LocalCompactionConnectedLimits{})
}

func newLocalSessionConnectedSeed(t *testing.T, endpoint string, compiled bool, limits LocalCompactionConnectedLimits) *LocalSessionConnectedSeed {
	return newLocalSessionConnectedSeedWithAuth(t, endpoint, compiled, limits, domain.ConfiguredInferenceAuthNoneV1)
}

// NewCredentialedLocalSessionConnectedSeed retains the same genuine local
// Source/Store/worker graph as the no-auth seed, but seals a synthetic Bearer
// resource generation before Source intake. It is not profile activation or
// live provider evidence.
func NewCredentialedLocalSessionConnectedSeed(t *testing.T, endpoint string) *LocalSessionConnectedSeed {
	return newLocalSessionConnectedSeedWithAuth(t, endpoint, false, LocalCompactionConnectedLimits{}, domain.ConfiguredInferenceAuthBearerV1)
}

func newLocalSessionConnectedSeedWithAuth(t *testing.T, endpoint string, compiled bool, limits LocalCompactionConnectedLimits, auth domain.ConfiguredInferenceAuthV1) *LocalSessionConnectedSeed {
	t.Helper()
	state := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, OwnerUserID: state.Job.CredentialOwnerUserID,
		SessionID: state.Run.SessionID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{authenticFunding: true, scope: scope, endpoint: endpoint, compiledCompaction: compiled, mcpModelDescription: limits.MCPModelDescription, mcpSyntheticPriceMicrounits: limits.MCPSyntheticPriceMicrounits, inputTokens: limits.InputTokens, outputTokens: limits.OutputTokens, modelWindowTokens: limits.ModelWindowTokens, networkOperation: limits.NetworkOperation, cleanupTimeout: limits.CleanupTimeout, auth: auth})
	parent := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: actual.at,
		sources: &ownedFinalSources{profile: actual.profile, spending: actual.execution.Spending}})
	operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, strings.NewReader(strings.Repeat("S", 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{&allocationSQLFixture{parent: parent.sql}}}, sampleWallClock: true}
	db := sql.OpenDB(loopbackOperationConnector{fixture})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return &LocalSessionConnectedSeed{installationRoot: actual.installationRoot, executable: actual.executable, receipt: actual.receipt, Source: actual.source, DB: db,
		Options: Options{OwnedAttemptEffectSourceVerifier: actual.source, OwnedProfilePublicationVerifier: actual.source},
		Prepare: parent.issuer.issuer, Operate: operate, Backend: parent.parent.Authority.HarnessBinding.Backend,
		Substrate: parent.parent.Authority.SubstrateBinding, Request: parent.request, Profile: actual.config.Clone(), Scope: scope,
		parent: parent, sql: fixture}
}

// InstallProviderCredentialBinding is an exact SQL-row fixture hook for the
// credentialed worker path. It only accepts a canonical binding matching this
// seed's original Source-published profile; it never creates permission by
// changing a resource or profile tuple.
func (s *LocalSessionConnectedSeed) InstallProviderCredentialBinding(t *testing.T, binding domain.ProviderCredentialBindingV1) {
	t.Helper()
	if s == nil || s.Source == nil || s.sql == nil {
		t.Fatal("credential binding requires the original connected seed")
	}
	configuration, err := s.Source.Configuration()
	resource, resourceErr := binding.ResourceBinding()
	if err != nil || resourceErr != nil || binding.Validate() != nil || configuration.Auth != domain.ConfiguredInferenceAuthBearerV1 ||
		resource != configuration.Resource || binding.TenantID != s.Scope.TenantID || binding.OwnerUserID != s.Scope.OwnerUserID ||
		binding.UpdatedAt.After(s.parent.parent.Authority.InvocationDeadline) {
		t.Fatal("credential binding does not match the original Bearer source/profile/scope")
	}
	copy := binding
	s.sql.providerCredential = &copy
}

// RevokeProviderCredentialBinding changes only the exact synthetic row already
// installed for this seed. It models a canonical generation revocation after
// an invocation lifecycle read; it does not change Source/profile authority.
func (s *LocalSessionConnectedSeed) RevokeProviderCredentialBinding(t *testing.T) {
	t.Helper()
	if s == nil || s.Source == nil || s.sql == nil || s.sql.providerCredential == nil {
		t.Fatal("credential revocation requires the installed connected row")
	}
	current := *s.sql.providerCredential
	configuration, err := s.Source.Configuration()
	resource, resourceErr := current.ResourceBinding()
	if err != nil || resourceErr != nil || configuration.Auth != domain.ConfiguredInferenceAuthBearerV1 || resource != configuration.Resource || current.Validate() != nil || current.State != domain.ProviderCredentialActiveV1 {
		t.Fatal("credential revocation requires the exact current synthetic binding")
	}
	current.State = domain.ProviderCredentialRevokedV1
	current.SecretRef = domain.CredentialSecretRef{}
	current.SecretFingerprint = ""
	current.UpdatedAt = current.UpdatedAt.Add(time.Microsecond)
	if err := current.Validate(); err != nil {
		t.Fatal("construct canonical revoked synthetic credential row")
	}
	s.sql.providerCredential = &current
}

func (s *LocalSessionConnectedSeed) ProviderCredentialReadCount() int {
	if s == nil || s.sql == nil {
		return 0
	}
	return s.sql.providerCredentialReads
}

// ArmProviderCredentialRevocationAfterOperationReservationCommit changes only
// the exact synthetic binding after the operation and call-index transaction
// has successfully committed, before the following BeforeSend transaction.
func (s *LocalSessionConnectedSeed) ArmProviderCredentialRevocationAfterOperationReservationCommit(t *testing.T, afterRevoke func()) {
	t.Helper()
	if s == nil || s.sql == nil || s.sql.providerCredential == nil || s.compaction == nil ||
		s.sql.afterOperationReservationCommit != nil {
		t.Fatal("post-reservation credential revocation requires a connected compaction seed with one installed binding")
	}
	s.sql.afterOperationReservationCommit = func() {
		operations := s.compaction.compactionEffectSQL.operations
		if len(operations) != 1 || len(operations[0]) != 11 || operations[0][5] != uint64(0) || operations[0][6] != uint64(0) ||
			operations[0][9] != operations[0][0] || operations[0][10] != operations[0][3] {
			t.Error("credential revocation hook did not follow committed operation and call-index rows")
			return
		}
		s.RevokeProviderCredentialBinding(t)
		if afterRevoke != nil {
			afterRevoke()
		}
	}
}

// TEST ONLY raw constructor inputs. The caller's actual NewLocalV1 must create
// its own original Store/bootstrap/entry/keeper. This installs no private owner,
// proxy, capability, head, fit result or transport approval into the session.
type LocalCompactionConnectedLimits struct {
	InputTokens, OutputTokens, ModelWindowTokens uint64
	NetworkOperation, CleanupTimeout             time.Duration
	MCPModelDescription                          string  // bounded original grant declaration, never patched after Source intake
	MCPSyntheticPriceMicrounits                  *uint64 // independent finite test-only price, before Source intake; nil preserves paid30 fixture
}

// LocalCompactionFixtureRequests is calibration DATA only, never a source,
// preparation, reservation, fit override or operation capability. The fixed
// source hash is deliberately NOT an authority commitment. The test must
// independently exercise the original Store/Source and actual received wires.
func (s *LocalSessionConnectedSeed) LocalCompactionFixtureRequests(t *testing.T, blobs ports.BlobStore) (ordinary, generation, rebuilt *configuredinference.FrozenRequestV1) {
	t.Helper()
	if s == nil || s.compaction == nil || blobs == nil || s.parent.parent.Authority.HarnessBinding.AgentLoopPolicy == nil {
		t.Fatal("original installed compaction fixture required")
	}
	var records []sessioncontext.EventPayload
	for _, row := range s.compaction.compactionEffectSQL.compactionSourceSQL.events {
		var event domain.SessionEvent
		if len(row) != 1 || json.Unmarshal([]byte(row[0].(string)), &event) != nil || event.Validate() != nil {
			t.Fatal("original canonical fixture event")
		}
		r, e := blobs.Open(t.Context(), event.TenantID, event.Payload)
		if e != nil {
			t.Fatal("original immutable event", e)
		}
		body, e := io.ReadAll(io.LimitReader(r, 65537))
		closeErr := r.Close()
		if e != nil || closeErr != nil || len(body) > 65536 {
			t.Fatal("bounded canonical fixture body", e, closeErr)
		}
		records = append(records, sessioncontext.EventPayload{Event: event, Payload: body})
	}
	defer func() {
		for _, record := range records {
			clear(record.Payload)
		}
	}()
	p := s.parent.parent.Authority.HarnessBinding.AgentLoopPolicy.Clone()
	grant, e := s.Source.ReadMCPToolGrant(t.Context(), domain.OwnedAgentProfileReferenceV1{Locator: s.receipt.Profile.Locator, RevocationGeneration: s.receipt.Profile.RevocationGeneration, Digest: s.receipt.Digest}, time.Now().UTC().Truncate(time.Microsecond))
	if e != nil {
		t.Fatal("original retained installed grant", e)
	}
	d, e := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if e != nil {
		t.Fatal(e)
	}
	tools := []configuredinference.ToolV1{{Name: d.Name(), Description: grant.ModelDescription, Parameters: d.InputSchema()}}
	ordinary, e = compactioncontext.BuildOrdinaryV1(s.Profile, s.Profile.Limits, p, strings.Repeat("a", 64), records, nil, nil, tools)
	if e != nil {
		t.Fatal("ordinary fixture DATA", e)
	}
	program, e := compactioncontext.BuildGenerationV1(s.Profile, s.Profile.Limits, p, strings.Repeat("a", 64), records, nil, nil, tools)
	if e != nil {
		t.Fatal("generation fixture DATA", e)
	}
	generation = program.FrozenRequest()
	var wire struct {
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	var history struct {
		Protected []struct {
			SourceID string `json:"source_id"`
		} `json:"protected_originals"`
		Units []struct {
			SourceID string `json:"source_id"`
		} `json:"original_units"`
	}
	if json.Unmarshal(generation.Body(), &wire) != nil || len(wire.Messages) != 2 || json.Unmarshal([]byte(wire.Messages[1].Content), &history) != nil || len(history.Protected) == 0 || len(history.Units) == 0 {
		t.Fatal("bounded actual generation DATA framing")
	}
	summary, e := json.Marshal(map[string]any{"schema": "sessionless.compaction-summary.v1", "task": []any{map[string]any{"text": "Preserve original task and constraints", "source_ids": []string{history.Protected[0].SourceID}}}, "constraints": []any{}, "facts": []any{map[string]any{"text": "Completed original history", "source_ids": []string{history.Units[0].SourceID}}}, "unresolved": []any{}, "artifacts": []any{}, "omissions": []any{}})
	if e != nil {
		t.Fatal(e)
	}
	rebuilt, e = program.Rebuild(summary)
	clear(summary)
	if e != nil {
		t.Fatal("rebuilt fixture DATA", e)
	}
	if bytes.Equal(ordinary.Body(), generation.Body()) {
		t.Fatal("ordinary and summarizer DATA must be distinct")
	}
	return ordinary, generation, rebuilt
}

func NewLocalCompactionSessionConnectedSeed(t *testing.T, endpoint string, blobs ports.BlobStore, limits ...LocalCompactionConnectedLimits) *LocalSessionConnectedSeed {
	t.Helper()
	if len(limits) > 1 {
		t.Fatal("one pre-intake limit declaration")
	}
	narrow := LocalCompactionConnectedLimits{}
	if len(limits) == 1 {
		narrow = limits[0]
	}
	s := newLocalSessionConnectedSeed(t, endpoint, true, narrow)
	return installLocalCompactionConnectedHistory(t, s, blobs)
}

// NewCredentialedLocalCompactionSessionConnectedSeed uses the same canonical
// retained-history fixture as the ordinary compaction seed, while sealing the
// Bearer resource before Source intake. The callback creates BlobStore only
// after the genuine scope is known.
func NewCredentialedLocalCompactionSessionConnectedSeed(t *testing.T, endpoint string, blobsForScope func(domain.AttemptExecutionSourceScopeV1) ports.BlobStore, limits ...LocalCompactionConnectedLimits) *LocalSessionConnectedSeed {
	t.Helper()
	if len(limits) > 1 || blobsForScope == nil {
		t.Fatal("credentialed compaction seed requires one bounded limit declaration and a scope-bound BlobStore factory")
	}
	narrow := LocalCompactionConnectedLimits{}
	if len(limits) == 1 {
		narrow = limits[0]
	}
	s := newLocalSessionConnectedSeedWithAuth(t, endpoint, true, narrow, domain.ConfiguredInferenceAuthBearerV1)
	blobs := blobsForScope(s.Scope)
	if blobs == nil {
		t.Fatal("credentialed compaction seed requires its original BlobStore")
	}
	return installLocalCompactionConnectedHistory(t, s, blobs)
}

// NewCredentialedLargeHistoryCompactionSessionConnectedSeed varies only an
// old assistant summary that the compaction may summarize. The protected
// latest trigger remains unchanged and small enough for the successor MODEL.
func NewCredentialedLargeHistoryCompactionSessionConnectedSeed(t *testing.T, endpoint string, blobsForScope func(domain.AttemptExecutionSourceScopeV1) ports.BlobStore, historicalSummary string, limits ...LocalCompactionConnectedLimits) *LocalSessionConnectedSeed {
	t.Helper()
	if len(limits) > 1 || blobsForScope == nil || historicalSummary == "" || len(historicalSummary) > 1<<16 {
		t.Fatal("large-history credentialed seed requires bounded historical summary and one scope-bound BlobStore factory")
	}
	narrow := LocalCompactionConnectedLimits{}
	if len(limits) == 1 {
		narrow = limits[0]
	}
	s := newLocalSessionConnectedSeedWithAuth(t, endpoint, true, narrow, domain.ConfiguredInferenceAuthBearerV1)
	blobs := blobsForScope(s.Scope)
	if blobs == nil {
		t.Fatal("credentialed compaction seed requires its original BlobStore")
	}
	return installLocalCompactionConnectedHistoryWithHistoricalSummary(t, s, blobs, historicalSummary)
}

func installLocalCompactionConnectedHistory(t *testing.T, s *LocalSessionConnectedSeed, blobs ports.BlobStore) *LocalSessionConnectedSeed {
	return installLocalCompactionConnectedHistoryWithHistoricalSummary(t, s, blobs, "")
}

func installLocalCompactionConnectedHistoryWithHistoricalSummary(t *testing.T, s *LocalSessionConnectedSeed, blobs ports.BlobStore, historicalSummary string) *LocalSessionConnectedSeed {
	t.Helper()
	if blobs == nil {
		t.Fatal("compaction seed requires its original BlobStore")
	}
	queries := &compactionSourceSQL{retainedEntrySQL: &retainedEntrySQL{loopbackOperationSQL: s.sql}}
	engine := &compactionPublicationSQL{compactionEffectSQL: &compactionEffectSQL{compactionSourceSQL: queries}}
	db := sql.OpenDB(compactionPublicationConnector{engine})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s.DB, s.compaction = db, engine
	var run domain.Run
	var job domain.WorkerJob
	if json.Unmarshal([]byte(s.parent.graph.rows["runs"][0].(string)), &run) != nil || json.Unmarshal([]byte(s.parent.graph.rows["worker_jobs"][0].(string)), &job) != nil {
		t.Fatal("connected original canonical rows")
	}
	job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 7}
	ratedFixturePut(t, s.parent.graph.ratedSQLFixture, "worker_jobs", job)
	var historyRecords []sessioncontext.EventPayload
	for i := 1; i <= 7; i++ {
		kind := domain.SessionEventUserMessage
		body := `{"version":1,"text":"original task with constraints"}`
		switch i {
		case 1:
			kind = domain.SessionEventSystemNotice
			body = `{"schema":"sessionless.run-terminal-notice.v1","code":"historical_notice","cancelled":false}`
		case 2, 3, 4, 5:
			kind = domain.SessionEventToolCall
			if i >= 4 {
				kind = domain.SessionEventToolResult
			}
			call := "a"
			if i == 3 || i == 5 {
				call = "b"
			}
			body = fmt.Sprintf(`{"schema":"sessionless.tool-event.v1","call_id":%q,"tool_name":"history_tool","content":{"data":"exact history bytes"}}`, call)
		case 6:
			kind = domain.SessionEventAssistantMessage
			summary := "historical answer"
			if historicalSummary != "" {
				summary = historicalSummary
			}
			encoded, err := json.Marshal(map[string]any{"schema": "sessionless.assistant-message.v1", "summary": summary, "artifact_manifest_id": "historical-manifest"})
			if err != nil {
				t.Fatal("encode bounded historical assistant summary")
			}
			body = string(encoded)
		}
		id := domain.SessionEventID(fmt.Sprintf("connected-history-%d", i))
		if i == 7 {
			id = run.TriggerEventID
		}
		event := domain.SessionEvent{ID: id, TenantID: run.TenantID, SessionID: run.SessionID, Sequence: uint64(i), Kind: kind, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("connected-history-key-%d", i)), CreatedAt: s.parent.graph.at}
		if kind == domain.SessionEventUserMessage {
			author := job.CredentialOwnerUserID
			event.AuthorUserID = &author
		}
		if kind == domain.SessionEventToolCall || kind == domain.SessionEventToolResult || kind == domain.SessionEventAssistantMessage {
			producing := domain.RunID("historical-run")
			event.RunID = &producing
		}
		ref, err := blobs.Put(t.Context(), run.TenantID, domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, id)+"message.json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		event.Payload = ref
		if err := event.Validate(); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(event)
		queries.events = append(queries.events, []driver.Value{string(raw)})
		historyRecords = append(historyRecords, sessioncontext.EventPayload{Event: event, Payload: []byte(body)})
	}
	seedCanonicalHistoryRuns(t, queries, run, historyRecords)
	return s
}

type LocalSessionConnectedStats struct{ Parent, Burn, Allocation, Proxy, Operation, Call, Observation int }

func (s *LocalSessionConnectedSeed) Stats() LocalSessionConnectedStats {
	if s.compaction != nil {
		return LocalSessionConnectedStats{Parent: s.parent.sql.inserts, Burn: s.parent.sql.burnUpdates, Allocation: s.sql.f.writes, Proxy: s.sql.writes + len(s.compaction.proxies), Operation: s.compaction.writes, Call: s.compaction.writes, Observation: s.compaction.observationWrites}
	}
	return LocalSessionConnectedStats{Parent: s.parent.sql.inserts, Burn: s.parent.sql.burnUpdates, Allocation: s.sql.f.writes,
		Proxy: s.sql.writes, Operation: s.sql.operationWrites, Call: s.sql.callWrites, Observation: s.sql.observationWrites}
}
func (s *LocalSessionConnectedSeed) PersistedObservation(t *testing.T) domain.AttemptOperationObservationV1 {
	t.Helper()
	row := s.sql.operation
	if s.compaction != nil && len(s.compaction.operations) > 0 {
		last := s.compaction.operations[len(s.compaction.operations)-1]
		row = last[1:9]
	}
	var o domain.AttemptOperationObservationV1
	if len(row) != 8 || row[4] != uint64(1) || row[5] != uint64(1) {
		t.Fatal("factory did not commit an observation row")
	}
	raw, ok := row[6].(string)
	if !ok || json.Unmarshal([]byte(raw), &o) != nil {
		t.Fatal("invalid committed observation encoding")
	}
	digest, err := o.Digest()
	if err != nil || row[7] != string(digest) {
		t.Fatal("committed observation digest mismatch")
	}
	return o.Clone()
}

// PersistedCompactionObservations is read-only scripted SQL DATA. The caller
// separately verifies each original issuer MAC and reservation/observation join.
func (s *LocalSessionConnectedSeed) PersistedCompactionObservations(t *testing.T) []domain.AttemptOperationObservationV1 {
	t.Helper()
	if s == nil || s.compaction == nil {
		t.Fatal("original compaction SQL fixture required")
	}
	var out []domain.AttemptOperationObservationV1
	for _, operation := range s.compaction.operations {
		if len(operation) != 11 || operation[5] != uint64(1) || operation[6] != uint64(1) {
			t.Fatalf("original committed observation row required: columns=%d want=11", len(operation))
		}
		var o domain.AttemptOperationObservationV1
		raw, ok := operation[7].(string)
		if !ok || json.Unmarshal([]byte(raw), &o) != nil {
			t.Fatal("original observation encoding")
		}
		d, e := o.Digest()
		if e != nil || operation[8] != string(d) {
			t.Fatal("original observation digest", e)
		}
		out = append(out, o.Clone())
	}
	return out
}

// TEST ONLY canonical DATA; no private entry, grant or approval is returned.
// Declare the canonical frontend projection BEFORE NewLocalV1. This does not
// assert a real Web ingress delivery and changes no source/funding seal.
func (s *LocalSessionConnectedSeed) DeclareWorkerPreflightWebOrigin(t *testing.T) {
	t.Helper()
	v := s.WorkerPreflightState(t)
	v.Job.Origin = &domain.FrontendEventOrigin{BindingID: "preflight-web-binding", BindingRevision: 1, Frontend: domain.FrontendWeb,
		ExternalConversationID: string(v.Run.SessionID), ExternalEventID: string(v.Run.TriggerEventID)}
	v.Job.DeliveryChat, v.Job.ReplyToMessageID = domain.TelegramChatRef{}, 0
	if err := validateLoadedWorkerJob(v); err != nil {
		t.Fatal("canonical Web-origin job projection", err)
	}
	ratedFixturePut(t, s.parent.graph.ratedSQLFixture, "worker_jobs", v.Job)
}

func (s *LocalSessionConnectedSeed) WorkerPreflightState(t *testing.T) ports.WorkerJobState {
	t.Helper()
	f := s.parent.graph.ratedSQLFixture
	v := ports.WorkerJobState{Run: ratedFixtureRecord[domain.Run](t, f, "runs"), Job: ratedFixtureRecord[domain.WorkerJob](t, f, "worker_jobs"),
		Attempt: ratedFixtureRecord[domain.Attempt](t, f, "attempts"), Reservation: ratedFixtureRecord[domain.QuotaReservation](t, f, "quota_reservations"),
		InputManifest: ratedFixtureRecord[domain.ArtifactManifest](t, f, "artifact_manifests")}
	if err := validateLoadedWorkerJob(v); err != nil {
		t.Fatal("original preflight rows", err)
	}
	return v
}

type LocalWorkerPreflightReads struct{ Final, Principal, Sources, Lease, Writes int }

func (s *LocalSessionConnectedSeed) WorkerPreflightReads() LocalWorkerPreflightReads {
	v := LocalWorkerPreflightReads{Lease: s.parent.sql.leaseReads, Writes: s.parent.graph.writes}
	for _, statement := range s.parent.graph.statements {
		switch statement {
		case ownedAdmissionSQL:
			v.Final++
		case ownedAdmissionPrincipalSQL:
			v.Principal++
		case ownedAdmissionSourcesJoinSQL:
			v.Sources++
		}
	}
	return v
}

// Freeze only the scripted DATABASE clock after the real Source publications.
// This does not alter Source/caller time or lifetime. Tests use a relative
// current-row timestamp against this exact authority sample, without sleeps.
func (s *LocalSessionConnectedSeed) FreezeWorkerPreflightDBClock() time.Time {
	s.sql.sampleWallClock = false
	s.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	return s.parent.graph.at
}

func (s *LocalSessionConnectedSeed) SetWorkerPreflightTimestamp(t *testing.T, kind string, at time.Time) {
	t.Helper()
	v := s.WorkerPreflightState(t)
	var table string
	var record any
	switch kind {
	case "run":
		v.Run.UpdatedAt, table, record = at, "runs", &v.Run
	case "attempt":
		v.Attempt.UpdatedAt, table, record = at, "attempts", &v.Attempt
	case "reservation":
		v.Reservation.UpdatedAt, table, record = at, "quota_reservations", &v.Reservation
	default:
		t.Fatal("unknown preflight timestamp mutant", kind)
	}
	if err := validateLoadedWorkerJob(v); err != nil {
		t.Fatal("timestamp mutant must remain structurally valid", kind, err)
	}
	ratedFixturePut(t, s.parent.graph.ratedSQLFixture, table, record)
}
