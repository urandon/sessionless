package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// The actual installed four-head Source is independent of this SQL peer. The
// peer begins with canonical Session/membership DATA only: normal Web/Store
// transitions must create every Run, Attempt, funding, lease, MODEL and consent.
// The embedded peers are unchanged generic ingress/terminal storage models;
// their historical text names confer no profile or authority on this fixture.
type LocalSearchWebSeed struct {
	Installation *LocalSearchSourceFixture
	Source       *localoperator.Source
	Store        *Store
	DB           *sql.DB
	Options      Options
	Tenant       domain.TenantID
	Owner        domain.UserID
	Session      domain.SessionID
	peer         *localSearchWebSQL
}

type LocalSearchManagerSeed struct {
	*LocalCanonicalLoopSeed
	manager *localTextManagerSQL
	effects *compactionEffectSQL
	owner   *localSearchWebSQL
}

type localSearchWebSQL struct {
	*localTextWebSQL
	mu                           sync.Mutex
	effects                      *compactionEffectSQL
	web                          domain.WebSession
	credential                   domain.ProviderCredentialBindingV1
	credentialReads              int
	consentRecord, consentDigest string
	pending                      chan domain.WebSearchConsentRequestV1
	credentialRotated            bool
	credentialRotationError      error
	searchTransportStopped       bool
	searchTransportStopError     error
	pendingBoundary              chan time.Time
	pendingBoundaryCandidate     time.Time
}

func (f *localSearchWebSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }

type localSearchWebTx struct {
	driver.Tx
	f                            *localSearchWebSQL
	consentRecord, consentDigest string
	done                         bool
}

func (f *localSearchWebSQL) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	f.mu.Lock()
	f.pendingBoundaryCandidate = time.Time{}
	tx, err := f.localTextWebSQL.BeginTx(ctx, options)
	if err != nil {
		f.mu.Unlock()
		return nil, err
	}
	return &localSearchWebTx{Tx: tx, f: f, consentRecord: f.consentRecord, consentDigest: f.consentDigest}, nil
}
func (tx *localSearchWebTx) Commit() error {
	err := tx.Tx.Commit()
	tx.done = true
	if err == nil && tx.f.consentRecord != tx.consentRecord {
		var old, current ownedConsentStates
		_ = json.Unmarshal([]byte(tx.consentRecord), &old)
		if json.Unmarshal([]byte(tx.f.consentRecord), &current) != nil {
			tx.f.mu.Unlock()
			return errors.New("committed consent bytes must remain readable DATA")
		}
		if len(current.States) == len(old.States)+1 && current.States[len(current.States)-1].Status == domain.WebSearchConsentPendingV1 {
			tx.f.pending <- current.States[len(current.States)-1].Request
		}
	}
	var boundary chan time.Time
	observed := tx.f.pendingBoundaryCandidate
	if err == nil && !observed.IsZero() {
		boundary, tx.f.pendingBoundary = tx.f.pendingBoundary, nil
	}
	tx.f.pendingBoundaryCandidate = time.Time{}
	tx.f.mu.Unlock()
	// Notify only AFTER successful healthy read COMMIT and outside the SQL
	// peer's mutex, so the independent BFF can take its actual transaction.
	if boundary != nil {
		select {
		case boundary <- observed:
		default:
			return errors.New("bounded pending boundary observer already full")
		}
	}
	return err
}
func (tx *localSearchWebTx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	tx.f.consentRecord, tx.f.consentDigest = tx.consentRecord, tx.consentDigest
	tx.f.pendingBoundaryCandidate = time.Time{}
	err := tx.Tx.Rollback()
	tx.f.mu.Unlock()
	return err
}

func (f *localSearchWebSQL) count(ctx context.Context) error {
	if f.manager != nil {
		return f.manager.seed.loop.countStatement(ctx)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (f *localSearchWebSQL) parentData() (domain.OwnedAttemptEffectParentV1, error) {
	var parent domain.OwnedAttemptEffectParentV1
	if f.effects == nil || len(f.effects.f.parent.parentRow) != 15 || json.Unmarshal([]byte(f.effects.f.parent.parentRow[13].(string)), &parent) != nil || parent.Validate() != nil {
		return parent, errors.New("no original physically persisted parent DATA")
	}
	return parent, nil
}

func (f *localSearchWebSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Rows, err error) {
	defer func() {
		if err != nil {
			f.lastSQL, f.lastError = completionSQL(s), err.Error()
		}
	}()
	row := func(v []driver.Value) (driver.Rows, error) {
		return &explanationFixtureRows{values: append([]driver.Value(nil), v...)}, nil
	}
	values := localInstallationNamedValues(args)
	// The negative-only Stop transaction reads the actual canonical actor
	// records, not the current permission/scalar projection used by admission.
	// Return only the SAME exact keys and bounded physical String prefix.
	actorTable := ""
	switch s {
	case `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`:
		bucket, err := webBucket(string(f.scope.OwnerUserID))
		if err != nil || !reflect.DeepEqual(values, []driver.Value{int64(bucket), string(f.scope.OwnerUserID), string(f.scope.TenantID)}) {
			return nil, errors.New("SEARCH stop exact canonical membership key")
		}
		actorTable = "tenant_memberships"
	case `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND user_id = $3`:
		if !reflect.DeepEqual(values, []driver.Value{string(f.scope.TenantID), string(f.scope.SessionID), string(f.scope.OwnerUserID)}) {
			return nil, errors.New("SEARCH stop exact canonical participant key")
		}
		actorTable = "session_participants"
	}
	if actorTable != "" {
		if f.count(ctx) != nil {
			return nil, errors.New("SEARCH stop canonical actor callback bound")
		}
		stored := f.rows[actorTable]
		if len(stored) == 0 {
			return row(nil)
		}
		if len(stored) != 1 {
			return nil, errors.New("SEARCH stop canonical actor row count")
		}
		raw, ok := stored[0].(string)
		if !ok {
			return nil, errors.New("SEARCH stop canonical actor String DATA")
		}
		return row([]driver.Value{raw[:min(len(raw), ownedAuthorityHeadBytes+1)]})
	}
	if s == providerCredentialReadSQL {
		b := f.credential
		if !reflect.DeepEqual(values, []driver.Value{string(b.TenantID), string(b.OwnerUserID), string(b.ResourceKind), b.ResourceID}) {
			return row(nil)
		}
		raw, err := json.Marshal(b)
		if err != nil || b.Validate() != nil {
			return nil, errors.New("SEARCH credential current row DATA")
		}
		f.credentialReads++
		return row([]driver.Value{int64(b.ResourceRevision), int64(b.CredentialGeneration), b.CandidateMutationID, string(b.State), b.SecretRef.StorageValue(), string(b.SecretFingerprint), b.UpdatedAt, string(raw)})
	}
	if s == ownedAdmissionSourcesJoinSQL {
		if f.count(ctx) != nil || len(values) != 25 {
			return nil, errors.New("SEARCH full original profile/four-head exact join")
		}
		profile := f.install.sql.pendingHeads[localInstallationSQLKey(values[:9])]
		if len(profile) == 0 {
			return row(nil)
		}
		if len(profile) != 11 {
			return nil, errors.New("SEARCH actual profile scalar count")
		}
		p, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(profile[0].(string)))
		if err != nil || p.Locator.Profile != domain.AgentLoopOwnedPublicV1 {
			return nil, errors.New("SEARCH full profile must be actually installed")
		}
		joined := append([]driver.Value(nil), profile...)
		for i, kind := range domain.OwnedSpendingComponentsV1(p.Locator.Profile) {
			key := values[9+4*i : 13+4*i]
			if !reflect.DeepEqual(key, []driver.Value{string(p.Locator.TenantID), key[1], string(kind), string(p.BillingOwnerUserID)}) {
				return nil, errors.New("SEARCH full join crossed original component/payer")
			}
			stored := f.install.sql.pendingFundingHeads[localInstallationSQLKey(key)]
			if len(stored) == 0 {
				return row(nil)
			}
			if len(stored) != 11 {
				return nil, errors.New("SEARCH actual spending scalar count")
			}
			joined = append(joined, stored...)
		}
		return row(joined)
	}
	if s == privateConsentWebSessionSQL {
		if f.count(ctx) != nil {
			return nil, errors.New("SEARCH Web session callback bound")
		}
		bucket, _ := webBucket(string(f.web.SessionDigest))
		if !reflect.DeepEqual(values, []driver.Value{int64(bucket), string(f.web.SessionDigest)}) {
			return row(nil)
		}
		raw, err := json.Marshal(f.web)
		if err != nil {
			return nil, err
		}
		return row([]driver.Value{string(raw)})
	}
	if s == ownedConsentReadSQL {
		p, err := f.parentData()
		if err != nil || f.count(ctx) != nil || !reflect.DeepEqual(values, []driver.Value{string(p.Scope.TenantID), string(p.Scope.AttemptID), string(p.Reservation.Kind)}) {
			return nil, errors.New("SEARCH original consent parent exact key")
		}
		if f.consentRecord == "" && f.consentDigest == "" {
			return row([]driver.Value{uint64(0), uint64(0), "", ""})
		}
		if f.pendingBoundary != nil {
			states, err := decodeOwnedConsentStates(f.consentRecord, f.consentDigest, p, f.at)
			if err != nil {
				return nil, err
			}
			if len(states.States) == 1 && states.States[0].Status == domain.WebSearchConsentPendingV1 {
				remaining := states.States[0].Request.ExpiresAt.Sub(f.at)
				if remaining > 0 && remaining < 5*time.Second {
					f.pendingBoundaryCandidate = f.at
				}
			}
		}
		return row([]driver.Value{uint64(1), uint64(1), f.consentRecord, f.consentDigest})
	}
	if f.effects != nil {
		switch s {
		case privateConsentTriggerSQL:
			if f.count(ctx) != nil || len(values) != 3 || values[0] != string(f.scope.TenantID) || values[1] != string(f.scope.SessionID) {
				return nil, errors.New("SEARCH canonical actor exact committed event key")
			}
			for _, raw := range f.manager.state.eventRecords {
				var event domain.SessionEvent
				if json.Unmarshal([]byte(raw), &event) != nil || event.Validate() != nil {
					return nil, errors.New("SEARCH canonical actor stored event DATA")
				}
				if values[2] == int64(event.Sequence) {
					return row([]driver.Value{raw[:min(len(raw), ownedAuthorityHeadBytes+1)]})
				}
			}
			return row(nil)
		case `SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM leases WHERE tenant_id=$1 AND lease_id=$2`:
			stored := f.rows["leases"]
			var lease domain.Lease
			if f.count(ctx) != nil || len(stored) != 1 || json.Unmarshal([]byte(stored[0].(string)), &lease) != nil || !reflect.DeepEqual(values, []driver.Value{string(lease.TenantID), string(lease.ID)}) {
				return nil, errors.New("SEARCH retained lease exact existing payload")
			}
			return row(stored)
		case `SELECT SUBSTRING(CAST(lease_id AS String),0,161),SUBSTRING(CAST(attempt_id AS String),0,161),SUBSTRING(CAST(worker_id AS String),0,161),fence_token,expires_at,updated_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`:
			stored := f.rows["lease_heads"]
			if f.count(ctx) != nil || len(stored) != 8 || !reflect.DeepEqual(values, stored[:2]) {
				return nil, errors.New("SEARCH retained current lease exact head")
			}
			return row(stored[2:])
		}
		switch s {
		case ownedOperationReadSQL, ownedOperationCallSQL, ownedOperationProxyRegistrationSQL, localCompactionOperationsSQL, localCompactionProxiesSQL:
			if f.count(ctx) != nil {
				return nil, errors.New("SEARCH operation inventory callback bound")
			}
			return f.effects.QueryContext(ctx, s, args)
		}
	}
	return f.localTextWebSQL.QueryContext(ctx, s, args)
}

func (f *localSearchWebSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (_ driver.Result, err error) {
	defer func() {
		if err != nil {
			f.lastSQL, f.lastError = completionSQL(s), err.Error()
		}
	}()
	if s == privateConsentUpdateSQL {
		p, err := f.parentData()
		pd, _ := p.Digest()
		values := localInstallationNamedValues(args)
		if err != nil || f.count(ctx) != nil || len(values) != 6 || !reflect.DeepEqual(values[2:], []driver.Value{string(p.Scope.TenantID), string(p.Scope.AttemptID), string(p.Reservation.Kind), pd}) {
			return nil, errors.New("SEARCH consent UPDATE original parent key")
		}
		raw, rawOK := values[0].(string)
		digest, digestOK := values[1].(string)
		if _, err := decodeOwnedConsentStates(raw, digest, p, f.at); !rawOK || !digestOK || err != nil {
			return nil, errors.New("SEARCH consent UPDATE must be exact canonical current DATA")
		}
		f.consentRecord, f.consentDigest = raw, digest
		return driver.RowsAffected(1), nil
	}
	if f.effects != nil {
		switch s {
		case loopbackOperationInsertSQL, loopbackCallInsertSQL, loopbackObservationUpdateSQL, `INSERT INTO attempt_operation_proxies_v1 (tenant_id,attempt_id,sequence,service_kind,original_record,original_digest,current_record,current_digest,policy_body,attestation_body) VALUES ($1,$2,$3,$4,$5,$6,$5,$6,$7,$8)`:
			if f.count(ctx) != nil {
				return nil, errors.New("SEARCH operation mutation callback bound")
			}
			return f.effects.ExecContext(ctx, s, args)
		}
	}
	return f.localTextWebSQL.ExecContext(ctx, s, args)
}

type localSearchBinder struct {
	ports.HarnessBinder
	f *localSearchWebSQL
}

func (b localSearchBinder) BindHarness(ctx context.Context, r ports.HarnessBindingRequest) (ports.ExecutionAuthorityV2, error) {
	for _, kind := range domain.OwnedSpendingComponentsV1(b.f.install.receipt.Profile.Locator.Profile) {
		b.f.install.sql.fundingKeys[localInstallationSQLKey([]driver.Value{string(r.TenantID), string(r.AttemptID), string(kind), string(b.f.install.receipt.Profile.BillingOwnerUserID)})] = true
	}
	return b.HarnessBinder.BindHarness(ctx, r)
}

func (s *LocalSearchWebSeed) Binder(t *testing.T) ports.HarnessBinder {
	t.Helper()
	b, err := NewLocalHarnessBinder(s.Store, s.Source, s.Installation.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	return localSearchBinder{b, s.peer}
}
func (s *LocalSearchWebSeed) IngressStore() ports.CanonicalIngressStore {
	return localTextIngress{s.Store, s.peer.localTextWebSQL}
}
func (s *LocalSearchWebSeed) Resources(t *testing.T) ports.WebResourceStore {
	t.Helper()
	r, err := NewLocalWebResourceStore(s.Store, s.Source, s.Installation.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (s *LocalSearchWebSeed) OriginalReceipt() ports.OwnedProfilePublicationReceiptV1 {
	r := s.Installation.Receipt
	r.Profile = r.Profile.Clone()
	return r
}
func (s *LocalSearchWebSeed) FundingCounts() (int, int) {
	return len(s.peer.install.sql.fundingHeads), len(s.peer.install.sql.fundingAudits)
}
func (s *LocalSearchWebSeed) LastError() string {
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	status := f.lastSQL + " / " + f.lastError
	if b := f.budget; b != nil {
		b.mu.Lock()
		status += fmt.Sprintf(" budget{callbacks=%d statements=%d callback=%d bytes=%d failed=%t}", b.callbacks, b.statements, b.callbackStatements, b.bytes, b.failed)
		b.mu.Unlock()
	}
	if f.effects != nil {
		for _, row := range f.effects.operations {
			var r domain.AttemptOperationReservationV1
			var o domain.AttemptOperationObservationV1
			if len(row) == 11 && json.Unmarshal([]byte(row[1].(string)), &r) == nil {
				_ = json.Unmarshal([]byte(row[7].(string)), &o)
				status += fmt.Sprintf(" operation{sequence=%d kind=%s outcome=%s reason=%s}", r.Operation.Sequence, r.Operation.Kind, o.Outcome, o.Reason)
			}
		}
	}
	var consents ownedConsentStates
	if json.Unmarshal([]byte(f.consentRecord), &consents) == nil {
		for _, state := range consents.States {
			status += fmt.Sprintf(" consent{revision=%d status=%s}", state.Revision, state.Status)
		}
	}
	return status
}
func (s *LocalSearchWebSeed) Empty() bool {
	f := s.peer
	return len(f.install.sql.fundingHeads) == 0 && len(f.install.sql.fundingAudits) == 0 && len(f.rows["runs"]) == 0 && len(f.rows["attempts"]) == 0 && len(f.rows["dispatch_outbox"]) == 0 && len(f.rows["worker_jobs"]) == 0 && len(f.rows["leases"]) == 0 && f.effects == nil && f.consentRecord == ""
}

func NewLocalSearchWebSeed(t *testing.T, endpoint string, grantExpiry time.Time) *LocalSearchWebSeed {
	t.Helper()
	installation := NewLocalSearchSourceFixture(t, endpoint, grantExpiry)
	original, p := installation.installation, installation.Receipt.Profile
	at := original.sql.at
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: p.Locator.TenantID, OwnerUserID: p.Locator.OwnerUserID, SessionID: "session-web-search"}
	rated := &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}
	graph := &managedAdmissionGraphFixture{canonicalBudgetFixture: &canonicalBudgetFixture{ratedSQLFixture: rated, at: at}}
	script := &ownedFinalScript{managedAdmissionGraphFixture: graph}
	base := &localCanonicalJoinSQL{localCancellationSQL: &localCancellationSQL{ownedFinalScript: script}, canonical: true, reads: make(map[string]struct {
		table string
		args  []driver.Value
	})}
	f := &localSearchWebSQL{localTextWebSQL: &localTextWebSQL{localCanonicalJoinSQL: base, install: original, scope: scope}, pending: make(chan domain.WebSearchConsentRequestV1, 2)}
	session := domain.Session{ID: scope.SessionID, TenantID: scope.TenantID, CreatedBy: scope.OwnerUserID, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	member := domain.TenantMembership{TenantID: scope.TenantID, UserID: scope.OwnerUserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
	participant := domain.SessionParticipant{TenantID: scope.TenantID, SessionID: scope.SessionID, UserID: scope.OwnerUserID, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	for table, v := range map[string]any{"sessions": session, "tenant_memberships": member, "session_participants": participant} {
		ratedFixturePut(t, rated, table, v)
	}
	script.membership = []driver.Value{rated.rows["tenant_memberships"][0], string(member.Role), string(member.Status), int64(member.SecurityVersion)}
	script.participant = []driver.Value{rated.rows["session_participants"][0], string(participant.Role), string(participant.Status), int64(0)}
	rated.rows["active_session_participants"] = []driver.Value{rated.rows["session_participants"][0], string(scope.TenantID), string(scope.SessionID), string(scope.OwnerUserID), string(participant.Status)}
	genesis, err := sessioncontext.NewSessionMaterialGenesisV1(scope.TenantID, scope.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sessioncontext.EncodeSessionMaterialPrefixV1(genesis)
	if err != nil {
		t.Fatal(err)
	}
	rated.rows["session_material_current_v1"] = []driver.Value{string(raw), int64(1), int64(0), string(scope.TenantID), string(scope.SessionID), "", genesis.PrefixDigest}
	base.request.TenantID, base.request.SessionID, base.request.UserID = scope.TenantID, scope.SessionID, scope.OwnerUserID
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	options := Options{OwnedProfilePublicationVerifier: installation.Source, AttemptSpendingPublicationVerifier: installation.Source, OwnedAttemptEffectSourceVerifier: installation.Source, OwnedWebSearchResourceGrantCustody: installation.Source}
	store, err := New(db, options)
	if err != nil {
		t.Fatal(err)
	}
	return &LocalSearchWebSeed{Installation: installation, Source: installation.Source, Store: store, DB: db, Options: options, Tenant: scope.TenantID, Owner: scope.OwnerUserID, Session: scope.SessionID, peer: f}
}

func (s *LocalSearchWebSeed) AdmitManager(t *testing.T) *LocalSearchManagerSeed {
	t.Helper()
	f, r := s.peer, s.peer.request
	priorRuns := 0
	if f.priorHistory != nil {
		priorRuns = len(f.priorHistory.runs)
	}
	if r.PreparedMaterial == nil || len(f.rows["runs"]) != 1 || len(f.rows["attempts"]) != 1 || f.bindings != 1 || len(f.install.sql.fundingHeads) != (priorRuns+1)*4 || len(f.install.sql.fundingAudits) != (priorRuns+1)*4 {
		t.Fatal("SEARCH Web must create original request/four actual Source heads")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admission := ports.DispatchAdmissionRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, OutboxID: r.DispatchID, ReservationID: domain.QuotaReservationID("reservation-" + string(r.AttemptID)), Now: f.at, HoldUntil: f.at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 8, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if result, err := s.Store.AdmitDispatch(ctx, admission); err != nil || !result.Admitted {
		t.Fatalf("actual SEARCH admission admitted=%t err=%v SQL=%s", result.Admitted, err, f.lastError)
	}
	f.addRead("SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", "quota_reservations", string(r.TenantID), string(admission.ReservationID))
	parent := &ownedAttemptEffectParentSQLFixture{base: f.ownedFinalScript, graph: f.managedAdmissionGraphFixture}
	alloc := &allocationSQLFixture{parent: parent}
	peer := &canonicalSessionSQL{localWorkerLifecycleSQL: &localWorkerLifecycleSQL{localCanonicalJoinSQL: f.localCanonicalJoinSQL, worker: "worker-local-search"}, base: f.localCanonicalJoinSQL, parent: parent, alloc: alloc}
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
		t.Fatal("SEARCH original canonical event absent")
	}
	loaded, found, err := s.Store.LoadWorkerJob(ctx, r.TenantID, r.RunID)
	if err != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatalf("SEARCH actual queued readback found=%t err=%v SQL=%s", found, err, f.lastError)
	}
	seed := &LocalCanonicalSessionSeed{Source: s.Source, DB: s.DB, Options: s.Options, Prepare: prepare, Operate: operate, Backend: loaded.Job.HarnessBinding.Backend, Substrate: *loaded.Job.SubstrateBinding, Loaded: loaded, Claim: ports.WorkerLeaseRequest{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, WorkerID: peer.worker}, peer: peer}
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
		t.Fatal("SEARCH original Session frontier")
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
			t.Fatal("actual previous canonical SEARCH event")
		}
		terminal.state.eventRecords[id] = raw
		terminal.state.eventIndex[string(e.IdempotencyKey)] = []driver.Value{int64(e.Sequence), string(e.ID)}
	}
	terminal.state.material[fmt.Sprint(event.Sequence)] = append([]driver.Value(nil), f.rows["session_material_boundaries_v1"]...)
	terminal.state.eventIndex[string(event.IdempotencyKey)] = []driver.Value{int64(event.Sequence), string(event.ID)}
	terminal.state.eventRecords[string(event.ID)] = f.rows["session_events"][0].(string)
	manager := &localTextManagerSQL{localTextTerminalSQL: terminal}
	if len(f.rows["leases"]) != 0 || len(f.rows["lease_heads"]) != 0 || parent.parentRow != nil || alloc.row != nil || operations.operation != nil || operations.row != nil || len(effects.operations) != 0 {
		t.Fatal("SEARCH queued setup fabricated invocation effects")
	}
	f.manager, f.effects = manager, effects
	return &LocalSearchManagerSeed{LocalCanonicalLoopSeed: loopSeed, manager: manager, effects: effects, owner: f}
}

func (s *LocalSearchManagerSeed) TerminalSnapshot(t *testing.T) LocalTerminalCompletionSnapshot {
	t.Helper()
	v := cloneLocalTextTerminalState(s.manager.state)
	base := s.peer.base
	for table, target := range map[string]any{"runs": &v.Run, "attempts": &v.Attempt, "quota_reservations": &v.Reservation} {
		if len(base.rows[table]) != 1 || json.Unmarshal([]byte(base.rows[table][0].(string)), target) != nil {
			t.Fatal("SEARCH original terminal row missing", table)
		}
	}
	v.LeasePresent = len(base.rows["lease_heads"]) != 0
	if len(base.counters) == 2 {
		v.ActiveRuns = uint32(base.counters[1].(int64))
		v.CountersPresent = true
	}
	return v
}

func (s *LocalSearchManagerSeed) StorageReleased() bool {
	base := s.peer.base
	return len(base.rows["quota_expiry_v2"]) == 0 && len(base.rows["lease_heads"]) == 0 && len(base.rows["lease_expiry_v2"]) == 0 && len(base.slot) == 7 && base.slot[1] == "" && base.slot[2] == "" && reflect.DeepEqual(base.counters, []driver.Value{int64(0), int64(0)})
}
func (s *LocalSearchManagerSeed) Trace() string {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	return s.manager.lastSQL + " / " + s.manager.lastError
}

func (s *LocalSearchWebSeed) SetCredentialBindingData(t *testing.T, binding domain.ProviderCredentialBindingV1) {
	t.Helper()
	r := s.Installation.Declaration.Resource
	if binding.Validate() != nil || binding.TenantID != s.Tenant || binding.OwnerUserID != s.Owner || binding.ResourceKind != r.Kind || binding.ResourceID != r.ResourceID || binding.ResourceRevision != r.Revision || binding.CredentialGeneration != r.CredentialGeneration || binding.State != domain.ProviderCredentialActiveV1 {
		t.Fatal("credential metadata must match the exact Source declaration DATA")
	}
	s.peer.mu.Lock()
	s.peer.credential = binding
	s.peer.mu.Unlock()
}
func (s *LocalSearchWebSeed) CredentialReadCount() int { return s.peer.credentialReads }

// Existing SQL-peer action after the ACTUAL next reservation commits. This
// changes current credential DATA only; it is not a transport/permission hook.
func (s *LocalSearchWebSeed) RotateCredentialAfterSearchReservation() error {
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.effects == nil || len(f.effects.operations) != 1 || f.effects.loopbackOperationSQL.afterOperationReservationCommit != nil || f.credential.Validate() != nil {
		return errors.New("rotation lacks original MODEL/current credential baseline")
	}
	old := f.credential.Clone()
	f.effects.loopbackOperationSQL.afterOperationReservationCommit = func() {
		// Called inside this SQL peer's serialized commit, before its lock is
		// released. Do not reenter the DB or manufacture a reservation here.
		var reserved domain.AttemptOperationReservationV1
		if len(f.effects.operations) != 2 || json.Unmarshal([]byte(f.effects.operations[1][1].(string)), &reserved) != nil || reserved.Validate() != nil || reserved.Operation.Kind != domain.AttemptOperationWebSearchV1 {
			f.credentialRotationError = errors.New("rotation did not follow actual SEARCH reservation")
			return
		}
		next := old.Clone()
		ref, err := domain.NewCredentialSecretRef("synthetic-current-rotation-never-materialized")
		if err != nil {
			f.credentialRotationError = err
			return
		}
		next.CredentialGeneration++
		next.CandidateMutationID, next.SecretRef, next.SecretFingerprint, next.UpdatedAt = "synthetic-controlled-search-rotation", ref, domain.FingerprintCredential([]byte("synthetic replacement never materialized")), time.Now().UTC().Truncate(time.Microsecond)
		if next.Validate() != nil || next.CredentialGeneration == old.CredentialGeneration || next.ResourceID != old.ResourceID || next.ResourceRevision != old.ResourceRevision || next.SecretRef == old.SecretRef {
			f.credentialRotationError = errors.New("current credential rotation DATA is invalid or vacuous")
			return
		}
		f.credential, f.credentialRotated = next, true
	}
	return nil
}

func (s *LocalSearchWebSeed) CredentialRotationReadback() (bool, error) {
	s.peer.mu.Lock()
	defer s.peer.mu.Unlock()
	return s.peer.credentialRotated, s.peer.credentialRotationError
}

func (s *LocalSearchWebSeed) StopTransportAfterSearchReservation(stop func() error) error {
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	if stop == nil || f.effects == nil || len(f.effects.operations) != 1 || f.effects.loopbackOperationSQL.afterOperationReservationCommit != nil {
		return errors.New("transport stop lacks original MODEL/current owned transport")
	}
	f.effects.loopbackOperationSQL.afterOperationReservationCommit = func() {
		var reserved domain.AttemptOperationReservationV1
		if len(f.effects.operations) != 2 || json.Unmarshal([]byte(f.effects.operations[1][1].(string)), &reserved) != nil || reserved.Validate() != nil || reserved.Operation.Kind != domain.AttemptOperationWebSearchV1 || f.effects.operations[1][9] != reserved.Operation.Sequence || f.effects.operations[1][10] != string(reserved.OperationDigest) {
			f.searchTransportStopError = errors.New("transport stop did not follow actual SEARCH reservation/index commit")
			return
		}
		f.searchTransportStopError = stop() // shutdown only, never a send/observation DTO
		f.searchTransportStopped = f.searchTransportStopError == nil
	}
	return nil
}

func (s *LocalSearchWebSeed) TransportStopReadback() (bool, error) {
	s.peer.mu.Lock()
	defer s.peer.mu.Unlock()
	return s.peer.searchTransportStopped, s.peer.searchTransportStopError
}

// Reuse only the existing SQL-storage history handoff, not its text profile or
// Source factory. Both invocations use this SAME actual four-head installation.
func (s *LocalSearchWebSeed) BeginNextAfterSuccess(t *testing.T, m *LocalSearchManagerSeed) {
	t.Helper()
	storage := &LocalTextWebSeed{Tenant: s.Tenant, Session: s.Session, peer: s.peer.localTextWebSQL}
	prior := &LocalTextManagerSeed{LocalCanonicalLoopSeed: m.LocalCanonicalLoopSeed, manager: m.manager}
	storage.BeginNextAfterSuccess(t, prior)
	s.peer.effects = nil
	// These cells now model the new Attempt's initially absent SQL row. The
	// original immutable Run/parent/accounting/seal remain in the handoff above.
	s.peer.consentRecord, s.peer.consentDigest = "", ""
}

func (s *LocalSearchWebSeed) PriorHistoryReads() int {
	if s.peer.priorHistory == nil {
		return 0
	}
	return s.peer.priorHistory.HistoryReads
}

func (s *LocalSearchWebSeed) WaitPending(ctx context.Context) (domain.WebSearchConsentRequestV1, error) {
	select {
	case request := <-s.peer.pending:
		return request, nil
	case <-ctx.Done():
		select {
		case request := <-s.peer.pending:
			return request, nil
		default:
			return domain.WebSearchConsentRequestV1{}, ctx.Err()
		}
	}
}

func (s *LocalSearchWebSeed) SetWebSessionData(t *testing.T, web domain.WebSession) {
	t.Helper()
	if web.Validate() != nil || web.UserID != s.Owner || web.ActiveTenantID != s.Tenant {
		t.Fatal("only actual authenticated owner Web-session DATA")
	}
	s.peer.mu.Lock()
	s.peer.web = web
	s.peer.mu.Unlock()
}

func (s *LocalSearchWebSeed) ConsentStateData(t *testing.T) domain.WebSearchConsentStateV1 {
	t.Helper()
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	parent, err := f.parentData()
	if err != nil {
		t.Fatal("original consent parent DATA", err)
	}
	states, err := decodeOwnedConsentStates(f.consentRecord, f.consentDigest, parent, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil || len(states.States) != 1 {
		t.Fatal("actual stored consent DATA", err)
	}
	return states.States[0].Clone()
}

// Observe stored Pending in the final healthy cadence interval. No authority,
// request, clock, permission or SQL result is changed by this bounded observer.
func (s *LocalSearchWebSeed) ObserveFinalPendingPoll(t *testing.T) <-chan time.Time {
	t.Helper()
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.effects != nil || f.pendingBoundary != nil {
		t.Fatal("pending boundary observer must precede the original invocation")
	}
	f.pendingBoundary = make(chan time.Time, 1)
	return f.pendingBoundary
}

// A late controlled closure must be nonvacuous: real MODEL completion and an
// actual authenticated approved decision already exist, but no SEARCH row.
// This inspects stored DATA only and never manufactures serving authority.
func (s *LocalSearchWebSeed) RequireApprovedSearchBorrowData() error {
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	var states ownedConsentStates
	if json.Unmarshal([]byte(f.consentRecord), &states) != nil || len(states.States) != 1 || states.States[0].Status != domain.WebSearchConsentApprovedV1 || f.effects == nil || len(f.effects.operations) != 1 {
		return errors.New("late closure lacks actual approved pre-SEARCH frontier")
	}
	for _, row := range f.effects.operations {
		var reservation domain.AttemptOperationReservationV1
		var observation domain.AttemptOperationObservationV1
		if len(row) != 11 || row[5] != uint64(1) || row[6] != uint64(1) || json.Unmarshal([]byte(row[1].(string)), &reservation) != nil || json.Unmarshal([]byte(row[7].(string)), &observation) != nil || observation.ValidateForReservation(reservation) != nil || reservation.Operation.Kind != domain.AttemptOperationModelV1 || observation.Outcome != domain.AttemptOperationCompletedV1 || reservation.Operation.Scope != ownedConsentOperationScope(states.States[0].Request.Scope) {
			return errors.New("late closure lacks original completed MODEL correspondence")
		}
	}
	return nil
}

// Controlled current SQL DATA changes only AFTER the real MODEL/pending/Web
// decision and prepared holder exist. No consent/grant/claim is seeded here.
func (s *LocalSearchWebSeed) InvalidateCurrentSearchAuthorityData(mode string) error {
	f := s.peer
	f.mu.Lock()
	defer f.mu.Unlock()
	var member domain.TenantMembership
	var states ownedConsentStates
	if len(f.rows["tenant_memberships"]) != 1 || json.Unmarshal([]byte(f.rows["tenant_memberships"][0].(string)), &member) != nil || member.Validate() != nil || f.web.Authorize(member, domain.TenantPermissionWrite, time.Now()) != nil || json.Unmarshal([]byte(f.consentRecord), &states) != nil || len(states.States) != 1 || states.States[0].Status != domain.WebSearchConsentApprovedV1 || f.effects == nil || len(f.effects.operations) != 1 {
		return errors.New("current-authority mutation lacks genuine valid approved baseline")
	}
	switch mode {
	case "membership":
		member.Status, member.UpdatedAt = domain.TenantMembershipSuspended, time.Now().UTC().Truncate(time.Microsecond)
		if member.Validate() != nil || member.Authorize(s.Owner, s.Tenant, domain.TenantPermissionWrite) == nil {
			return errors.New("membership mutation must be typed-valid and nonvacuous")
		}
		raw, err := json.Marshal(member)
		if err != nil {
			return err
		}
		f.rows["tenant_memberships"] = []driver.Value{string(raw)}
		f.membership = []driver.Value{string(raw), string(member.Role), string(member.Status), int64(member.SecurityVersion)}
	case "session":
		at := time.Now().UTC().Truncate(time.Microsecond)
		f.web.RevokedAt = &at
		if f.web.Validate() != nil || f.web.Authorize(member, domain.TenantPermissionWrite, at) == nil {
			return errors.New("session mutation must be typed-valid and nonvacuous")
		}
	case "claim_fence":
		var lease domain.Lease
		var run domain.Run
		var attempt domain.Attempt
		head := f.rows["lease_heads"]
		if len(head) != 8 || len(f.rows["leases"]) != 1 || json.Unmarshal([]byte(f.rows["leases"][0].(string)), &lease) != nil || json.Unmarshal([]byte(f.rows["runs"][0].(string)), &run) != nil || json.Unmarshal([]byte(f.rows["attempts"][0].(string)), &attempt) != nil || lease.ValidateForAttempt(run, attempt) != nil || head[5] != int64(lease.FenceToken) || lease.FenceToken != states.States[0].Request.Scope.FenceToken {
			return errors.New("current claim fence lacks coherent active original lease")
		}
		lease.FenceToken++
		if lease.ValidateForAttempt(run, attempt) != nil || lease.FenceToken == states.States[0].Request.Scope.FenceToken {
			return errors.New("fenced claim must remain typed-valid and nonvacuous")
		}
		raw, err := json.Marshal(lease)
		if err != nil {
			return err
		}
		f.rows["leases"][0], head[5] = string(raw), int64(lease.FenceToken)
	case "search_authority_revoked":
		request := states.States[0].Request
		key := localInstallationSQLKey([]driver.Value{string(s.Tenant), string(request.Scope.AttemptID), string(domain.AttemptSpendingWebSearchV1), string(s.Installation.Receipt.Profile.BillingOwnerUserID)})
		row := f.install.sql.fundingHeads[key]
		if len(row) != 11 {
			return errors.New("original SEARCH funding head absent")
		}
		old, err := domain.DecodeAttemptSpendingAuthorizationCurrentV1([]byte(row[0].(string)))
		if err != nil || old.ValidateAt(time.Now()) != nil || old.Locator.AttemptID != request.Scope.AttemptID || old.Locator.ComponentKind != domain.AttemptSpendingWebSearchV1 || old.ScopedGrantDigest != request.GrantDigest {
			return errors.New("SEARCH authority revoke lacks valid exact original grant baseline")
		}
		next := old.Clone()
		next.State, next.RevocationGeneration, next.UpdatedAt, next.MutationID = domain.OwnedAgentCurrentRevokedV1, old.RevocationGeneration+1, time.Now().UTC().Truncate(time.Microsecond), "controlled-search-authority-revoked"
		if next.ValidateTransitionFrom(old) != nil || next.ValidateAt(time.Now()) == nil {
			return errors.New("SEARCH authority revoke must be valid closed transition and nonvacuous")
		}
		raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(next)
		digest, de := next.Digest()
		if err != nil || de != nil {
			return errors.New("SEARCH authority revoke canonical DATA")
		}
		// Recompute EVERY physical scalar, not a malformed record/digest pair.
		f.install.sql.fundingHeads[key] = []driver.Value{string(raw), uint64(next.Version), next.RevocationGeneration, string(next.State), digest, string(next.RunID), string(next.SessionID), string(next.OwnerUserID), next.Window.PublishedAt, next.Window.ExpiresAt, next.UpdatedAt}
	default:
		return errors.New("unknown current authority mutation")
	}
	return nil
}

func (s *LocalSearchWebSeed) NewBFFDatabase(t *testing.T) *sql.DB {
	t.Helper()
	db := sql.OpenDB(s.peer)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func (s *LocalSearchManagerSeed) OperationReadback(t *testing.T) ([]domain.AttemptOperationReservationV1, []domain.AttemptOperationObservationV1) {
	t.Helper()
	var reservations []domain.AttemptOperationReservationV1
	var observations []domain.AttemptOperationObservationV1
	for _, row := range s.effects.operations {
		var r domain.AttemptOperationReservationV1
		var o domain.AttemptOperationObservationV1
		if len(row) != 11 || row[5] != uint64(1) || row[6] != uint64(1) || json.Unmarshal([]byte(row[1].(string)), &r) != nil || json.Unmarshal([]byte(row[7].(string)), &o) != nil || o.ValidateForReservation(r) != nil {
			t.Fatal("actual stored operation/observation row incomplete")
		}
		reservations, observations = append(reservations, r), append(observations, o)
	}
	return reservations, observations
}
