package ydbstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// TEST ONLY: raw constructor dependencies, not an existing Store/bootstrap,
// parent reservation, body adapter or approval. SQL, external funding and the
// complete effect-source verifier remain SCRIPTED, not normal activation proof.
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
	t.Helper()
	state := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, OwnerUserID: state.Job.CredentialOwnerUserID,
		SessionID: state.Run.SessionID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{scope: scope, endpoint: endpoint, compiledCompaction: compiled, inputTokens: limits.InputTokens, outputTokens: limits.OutputTokens, modelWindowTokens: limits.ModelWindowTokens, networkOperation: limits.NetworkOperation})
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
		Options: Options{OwnedAttemptEffectSourceVerifier: parent.verifier, OwnedProfilePublicationVerifier: actual.source},
		Prepare: parent.issuer.issuer, Operate: operate, Backend: parent.parent.Authority.HarnessBinding.Backend,
		Substrate: parent.parent.Authority.SubstrateBinding, Request: parent.request, Profile: actual.config.Clone(), Scope: scope,
		parent: parent, sql: fixture}
}

// TEST ONLY raw constructor inputs. The caller's actual NewLocalV1 must create
// its own original Store/bootstrap/entry/keeper. This installs no private owner,
// proxy, capability, head, fit result or transport approval into the session.
type LocalCompactionConnectedLimits struct {
	InputTokens, OutputTokens, ModelWindowTokens uint64
	NetworkOperation                             time.Duration
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
			body = `{"schema":"sessionless.assistant-message.v1","summary":"historical answer","artifact_manifest_id":"historical-manifest"}`
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
