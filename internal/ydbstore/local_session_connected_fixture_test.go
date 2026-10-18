package ydbstore

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// TEST ONLY: raw constructor dependencies, not an existing Store/bootstrap,
// parent reservation, body adapter or approval. SQL, external funding and the
// complete effect-source verifier remain SCRIPTED, not normal activation proof.
type LocalSessionConnectedSeed struct {
	Source    *localoperator.Source
	DB        *sql.DB
	Options   Options
	Prepare   *serverlessharness.CapabilityIssuer
	Operate   *serverlessharness.OperationCapabilityIssuer
	Backend   domain.HarnessBackendDescriptorV1
	Substrate domain.SubstrateBindingV1
	Request   ports.ReserveAttemptEffectRequestV1
	Profile   domain.ConfiguredInferenceProfileV1
	Scope     domain.AttemptExecutionSourceScopeV1
	parent    *ownedAttemptEffectParentFixture
	sql       *loopbackOperationSQL
}

func NewLocalSessionConnectedSeed(t *testing.T, endpoint string) *LocalSessionConnectedSeed {
	t.Helper()
	state := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, OwnerUserID: state.Job.CredentialOwnerUserID,
		SessionID: state.Run.SessionID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{scope: scope, endpoint: endpoint})
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
	return &LocalSessionConnectedSeed{Source: actual.source, DB: db,
		Options: Options{OwnedAttemptEffectSourceVerifier: parent.verifier, OwnedProfilePublicationVerifier: actual.source},
		Prepare: parent.issuer.issuer, Operate: operate, Backend: parent.parent.Authority.HarnessBinding.Backend,
		Substrate: parent.parent.Authority.SubstrateBinding, Request: parent.request, Profile: actual.config.Clone(), Scope: scope,
		parent: parent, sql: fixture}
}

type LocalSessionConnectedStats struct{ Parent, Burn, Allocation, Proxy, Operation, Call, Observation int }

func (s *LocalSessionConnectedSeed) Stats() LocalSessionConnectedStats {
	return LocalSessionConnectedStats{Parent: s.parent.sql.inserts, Burn: s.parent.sql.burnUpdates, Allocation: s.sql.f.writes,
		Proxy: s.sql.writes, Operation: s.sql.operationWrites, Call: s.sql.callWrites, Observation: s.sql.observationWrites}
}
func (s *LocalSessionConnectedSeed) PersistedObservation(t *testing.T) domain.AttemptOperationObservationV1 {
	t.Helper()
	row := s.sql.operation
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
