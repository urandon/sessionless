package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This exercises actual Confirm -> stored R/M -> actual phase-authorizer code.
// SQL snapshots, native catalog, identity bytes and protocol snapshot are
// synthetic DATA, not native YDB isolation, signed-machine replay, installed
// tariff/native qualification, or permission to send a provider request.
const (
	piUseWorkerSQL     = `SELECT record FROM attached_workers WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`
	piUseConnectionSQL = `SELECT record FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`
	piUseHeadSQL       = `SELECT payload FROM attached_worker_attempt_heads WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`
	piUseRunSQL        = `SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`
	piUseJobSQL        = `SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`
	piUseAttemptSQL    = `SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2`
	piUseQuotaSQL      = `SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`
	piUseManifestSQL   = `SELECT payload FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`
	piUseLeaseHeadSQL  = `SELECT lease_id,attempt_id,worker_id,fence_token,expires_at FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`
	piUseLeaseSQL      = `SELECT payload FROM leases WHERE tenant_id=$1 AND lease_id=$2`
	piUseDeletionSQL   = `SELECT SUBSTRING(CAST(session_id AS String),0,161) FROM session_deletions WHERE tenant_id=$1 AND session_id=$2`
)

type piUseContextKey struct{}

// Only whitespace is normalized; names, columns, predicates and parameter
// order remain exact. No range or fallback query is supplied by this driver.
func piUseSQL(s string) string { return strings.Join(strings.Fields(s), " ") }

type piUseSQLSource struct {
	*piPolicySQLFixture
	rows                        map[string][]driver.Value
	args                        map[string][]driver.Value
	namespace                   []driver.Value
	decisionKey                 string
	marker                      *struct{}
	active, writesAllowed       bool
	transactions, writeAttempts int
	transactionQueries          []string
}

func (f *piUseSQLSource) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *piUseSQLSource) bounded(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil || ctx.Value(piUseContextKey{}) != f.marker {
		return errors.New("source fixture requires original bounded caller context")
	}
	return nil
}
func (f *piUseSQLSource) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if err := f.bounded(ctx); err != nil {
		return nil, err
	}
	if f.active {
		return nil, errors.New("nested source transaction")
	}
	tx, err := f.piPolicySQLFixture.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	f.active = true
	f.transactions++
	f.transactionQueries = nil
	return piUseSourceTx{Tx: tx, f: f}, nil
}

type piUseSourceTx struct {
	driver.Tx
	f *piUseSQLSource
}

func (tx piUseSourceTx) Commit() error { defer func() { tx.f.active = false }(); return tx.Tx.Commit() }
func (tx piUseSourceTx) Rollback() error {
	defer func() { tx.f.active = false }()
	return tx.Tx.Rollback()
}
func (f *piUseSQLSource) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := f.bounded(ctx); err != nil {
		return nil, err
	}
	if !f.active {
		return nil, errors.New("source read outside serializable callback")
	}
	q := piUseSQL(query)
	actual := make([]driver.Value, len(args))
	for i := range args {
		actual[i] = args[i].Value
	}
	if q == piUseSQL(piPolicyReadSQL) {
		if len(actual) != 4 || !reflect.DeepEqual(actual[:3], f.namespace) || (actual[3] != "head" && actual[3] != f.decisionKey) {
			return nil, errors.New("foreign policy namespace or decision key")
		}
		f.transactionQueries = append(f.transactionQueries, q)
		return f.piPolicySQLFixture.QueryContext(ctx, query, args)
	}
	want, ok := f.args[q]
	if !ok || !reflect.DeepEqual(actual, want) {
		return nil, errors.New("unexpected source point selector")
	}
	row, ok := f.rows[q]
	if !ok {
		return nil, errors.New("unregistered source point query")
	}
	f.transactionQueries = append(f.transactionQueries, q)
	return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
}
func (f *piUseSQLSource) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	f.writeAttempts++
	if err := f.bounded(ctx); err != nil {
		return nil, err
	}
	if !f.active || !f.writesAllowed || len(args) != 9 {
		return nil, errors.New("phase source must not write")
	}
	values := []driver.Value{args[0].Value, args[1].Value, args[2].Value}
	if !reflect.DeepEqual(values, f.namespace) || (args[3].Value != "head" && args[3].Value != f.decisionKey) {
		return nil, errors.New("foreign decision write")
	}
	return f.piPolicySQLFixture.ExecContext(ctx, q, args)
}

type piUseSourceFixture struct {
	store      *Store
	sql        *piUseSQLSource
	ctx        context.Context
	selection  ports.AttachedPiAPIPolicySelectionV1
	request    ports.ConfirmAttachedPiAPIPolicyV1
	decision   ports.AttachedPiAPIPolicyDecisionV1
	loaded     ports.WorkerJobState
	lease      domain.Lease
	head       domain.AttachedWorkerAttemptV1
	worker     domain.AttachedWorker
	connection domain.AttachedWorkerConnection
	auth       ports.AttachedWorkerSealedInputAuthorization
	binding    domain.HarnessBindingDigestV2
	at         time.Time
}

func newPiUseSourceFixture(t *testing.T) *piUseSourceFixture {
	t.Helper()
	s, base, selection, request, at := newPiPolicyFixture(t)
	marker := &struct{}{}
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), piUseContextKey{}, marker), 10*time.Second)
	t.Cleanup(cancel)
	locator, err := request.Locator.Digest()
	if err != nil {
		t.Fatal(err)
	}
	key, err := piDecisionKey(request.MutationID)
	if err != nil {
		t.Fatal(err)
	}
	f := &piUseSQLSource{piPolicySQLFixture: base, rows: map[string][]driver.Value{}, args: map[string][]driver.Value{}, namespace: []driver.Value{string(request.Locator.TenantID), string(request.Locator.OwnerUserID), locator}, decisionKey: key, marker: marker, writesAllowed: true}
	for q, row := range base.rows {
		f.rows[piUseSQL(q)] = append([]driver.Value(nil), row...)
	}
	webBucketID, err := webBucket(string(request.WebSessionDigest))
	if err != nil {
		t.Fatal(err)
	}
	memberBucket, err := webBucket(string(request.Locator.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	f.args[piUseSQL(`SELECT CurrentUtcTimestamp()`)] = []driver.Value{}
	f.args[piUseSQL(piPolicyWebSessionSQL)] = []driver.Value{int64(webBucketID), string(request.WebSessionDigest)}
	f.args[piUseSQL(computeChoiceMembershipSQL)] = []driver.Value{int64(memberBucket), string(request.Locator.OwnerUserID), string(request.Locator.TenantID)}
	f.args[piUseSQL(providerCredentialReadSQL)] = []driver.Value{string(request.Locator.TenantID), string(request.Locator.OwnerUserID), string(domain.ProviderResourceAPIAccountV1), request.Locator.ResourceID}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s.db = db
	decision, err := s.ConfirmAttachedPiAPIPolicy(ctx, request)
	if err != nil || decision.Generation != 1 || decision.Replayed || base.writes != 2 {
		t.Fatalf("actual confirmation setup: generation=%d writes=%d err=%v", decision.Generation, base.writes, err)
	}
	g := &piUseSourceFixture{store: s, sql: f, ctx: ctx, selection: selection, request: request, decision: decision, at: at}
	g.loaded, g.lease, g.head, g.auth, g.binding, _ = piAPIOriginalJobData(t)
	l := &g.loaded
	tenant, owner := request.Locator.TenantID, request.Locator.OwnerUserID
	l.Run.TenantID, l.Run.CreatedAt, l.Run.UpdatedAt = tenant, at, at
	l.Run.StartedAt = &g.at
	l.Job.TenantID, l.Job.CredentialOwnerUserID, l.Job.CreatedAt = tenant, owner, at
	l.Job.ContextSnapshot.TenantID, l.Job.ContextSnapshot.Key = tenant, domain.SessionObjectPrefix(tenant, l.Run.SessionID)+"context"
	l.Job.DeliveryChat.TenantID = tenant
	l.Job.ExecutionPlacementV2.OwnerUserID = owner
	l.Attempt.TenantID, l.Attempt.CreatedAt, l.Attempt.UpdatedAt = tenant, at, at
	l.Reservation.TenantID, l.Reservation.HeldAt, l.Reservation.UpdatedAt, l.Reservation.ExpiresAt = tenant, at, at, at.Add(time.Hour)
	l.InputManifest.TenantID, l.InputManifest.CreatedAt = tenant, at
	b := &l.Job.HarnessBinding
	b.TenantID, b.OwnerUserID, b.Backend, b.Resource = tenant, owner, selection.Terms.Scope.Backend, selection.Terms.Scope.Resource
	b.ModelVendorID, b.ModelID, b.InputDataClass = selection.Terms.ModelVendorID, selection.Terms.ModelID, selection.Terms.InputDataClass
	cd, err := selection.Evidence.Catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rd, err := selection.Evidence.Route.Digest()
	if err != nil {
		t.Fatal(err)
	}
	pd, err := selection.Evidence.Privacy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	capd, err := selection.Evidence.Capability.Digest()
	if err != nil {
		t.Fatal(err)
	}
	b.ProviderCatalogDigest, b.ProviderRouteDigest, b.PrivacyPolicyDigest, b.CapabilityEvidenceDigest, b.EffectivePolicyDigest = string(cd), string(rd), string(pd), string(capd), selection.Terms.EffectivePolicyDigest
	expiry := selection.Terms.ExpiresAt
	b.EvidenceExpiresAt = &expiry
	b.AttachedPiAPIInvocationPolicyDigest = decision.MappingDigest
	placement, err := domain.ExecutionPlacementDigest(l.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	b.ExecutionPlacementDigest = string(placement)
	g.binding, err = b.Digest()
	if err != nil {
		t.Fatal(err)
	}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(l.Job, l.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	g.lease.TenantID, g.lease.AcquiredAt, g.lease.ExpiresAt = tenant, at, at.Add(10*time.Minute)
	g.head.TenantID, g.head.OwnerUserID, g.head.CreatedAt, g.head.UpdatedAt, g.head.LeaseExpiresAt = tenant, owner, at, at, g.lease.ExpiresAt
	g.head.Version, g.head.PlatformAttemptSequence, g.head.WorkerAttemptSequence = 1, 1, 1
	g.head.ExecutionConnectionID, g.head.ExecutionConnectionGeneration = g.head.ConnectionID, g.head.ConnectionGeneration
	g.head.ContextDigest = contextDigest
	g.head.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(tenant, owner, g.head.WorkerID, l.Run.ID, l.Attempt.ID, g.lease.ID, g.lease.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	g.auth.TenantID, g.auth.OwnerUserID, g.auth.ContextDigest, g.auth.FenceToken, g.auth.LeaseExpiresAtUnixMicro = tenant, owner, contextDigest, g.head.FenceToken, g.lease.ExpiresAt.UnixMicro()
	g.worker = domain.AttachedWorker{TenantID: tenant, OwnerUserID: owner, ID: g.head.WorkerID, DisplayName: "Synthetic Pi phase fixture", IdentityPublicKey: []byte(strings.Repeat("p", 32)), EnrollmentGeneration: g.head.EnrollmentGeneration, ConnectionGeneration: g.head.ConnectionGeneration, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOnline, Revision: 1, CreatedAt: at, UpdatedAt: at}
	g.connection = domain.AttachedWorkerConnection{TenantID: tenant, OwnerUserID: owner, WorkerID: g.head.WorkerID, ID: g.head.ConnectionID, ActivationChallengeID: "synthetic-challenge", EnrollmentGeneration: g.head.EnrollmentGeneration, ConnectionGeneration: g.head.ConnectionGeneration, ProtocolVersion: 1, CapabilityDigest: g.head.CapabilityDigest, SecretDigest: g.auth.PresentedSecretDigest, ChannelBinding: domain.AttachedWorkerChannelBinding(strings.Repeat("a", 64)), ManifestRevision: 1, ManifestIdentityKey: domain.AttachedWorkerIdentityKeyDigest(strings.Repeat("b", 64)), ManifestSignature: []byte(strings.Repeat("s", 64)), ManifestObservedAt: at, State: domain.AttachedWorkerConnectionOnline, ProtocolSnapshot: []byte(`{"synthetic":"not-replay-machine-proof"}`), ConnectedAt: at, LastCheckpointAt: at, PresenceExpiresAt: at.Add(5 * time.Minute), AuthExpiresAt: at.Add(5 * time.Minute), Revision: 1}
	for name, e := range map[string]error{"worker": g.worker.Validate(), "connection": g.connection.Validate(), "head": g.head.Validate(), "aggregate": validateLoadedWorkerJob(*l)} {
		if e != nil {
			t.Fatalf("%s setup: %v", name, e)
		}
	}
	if !piAPIOriginalJobMatches(*l, g.lease, g.head, g.auth, g.binding, decision.MappingDigest, at) {
		t.Fatal("original aggregate setup mismatch")
	}
	if _, err := domain.ValidateAttachedPiAPIInvocationPolicyV1(selection.Profile, selection.Evidence, selection.Terms, decision.Receipt, decision.Mapping, *b, at); err != nil {
		t.Fatalf("full policy setup: %v", err)
	}
	g.syncOriginalRows(t)
	f.writesAllowed = false
	f.writeAttempts = 0
	return g
}

func (g *piUseSourceFixture) setRow(t *testing.T, q string, args []driver.Value, values ...driver.Value) {
	t.Helper()
	g.sql.rows[piUseSQL(q)] = values
	g.sql.args[piUseSQL(q)] = args
}
func (g *piUseSourceFixture) setJSON(t *testing.T, q string, args []driver.Value, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	g.setRow(t, q, args, string(raw))
}
func (g *piUseSourceFixture) syncOriginalRows(t *testing.T) {
	t.Helper()
	s := g.loaded
	tenant, owner, worker := string(s.Run.TenantID), string(s.Job.CredentialOwnerUserID), string(g.head.WorkerID)
	workerKey := []driver.Value{tenant, owner, worker}
	runKey := []driver.Value{tenant, string(s.Run.ID)}
	g.setJSON(t, piUseWorkerSQL, workerKey, g.worker)
	g.setJSON(t, piUseConnectionSQL, workerKey, g.connection)
	g.setJSON(t, piUseHeadSQL, workerKey, g.head)
	g.setJSON(t, piUseRunSQL, runKey, s.Run)
	g.setJSON(t, piUseJobSQL, runKey, s.Job)
	g.setJSON(t, piUseAttemptSQL, []driver.Value{tenant, string(s.Attempt.ID)}, s.Attempt)
	g.setJSON(t, piUseQuotaSQL, []driver.Value{tenant, string(s.Reservation.ID)}, s.Reservation)
	g.setJSON(t, piUseManifestSQL, []driver.Value{tenant, string(s.InputManifest.ID)}, s.InputManifest)
	g.setRow(t, piUseLeaseHeadSQL, runKey, string(g.lease.ID), string(g.lease.AttemptID), g.lease.WorkerID, int64(g.lease.FenceToken), g.lease.ExpiresAt)
	g.setJSON(t, piUseLeaseSQL, []driver.Value{tenant, string(g.lease.ID)}, g.lease)
	session := domain.Session{ID: s.Run.SessionID, TenantID: s.Run.TenantID, CreatedBy: s.Job.CredentialOwnerUserID, Status: domain.SessionActive, LastEventSequence: 1, CreatedAt: g.at, UpdatedAt: g.at}
	participant := domain.SessionParticipant{TenantID: session.TenantID, SessionID: session.ID, UserID: session.CreatedBy, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: g.at, UpdatedAt: g.at}
	g.setRow(t, computeChoiceSessionSQL, []driver.Value{tenant, string(session.ID)}, choiceFixtureJSON(t, session), owner, string(session.Status), int64(session.LastEventSequence), nil)
	g.setRow(t, computeChoiceParticipantSQL, []driver.Value{tenant, string(session.ID), owner}, choiceFixtureJSON(t, participant), string(participant.Role), string(participant.Status))
	g.setRow(t, piUseDeletionSQL, []driver.Value{tenant, string(session.ID)})
}

func (g *piUseSourceFixture) use(phase string) (ports.AttachedWorkerPiAPIUseResultV1, error) {
	return g.store.AuthorizeAttachedWorkerPiAPIUse(g.ctx, g.auth, g.binding, g.decision.MappingDigest, uint64(2*time.Minute), phase)
}
func (g *piUseSourceFixture) requireApplied(t *testing.T, phase string) {
	t.Helper()
	beforeTx, beforeWrites := g.sql.transactions, g.sql.writes
	got, err := g.use(phase)
	wantExpiry := g.at.Add(time.Minute)
	if phase == "immutable_finalize" {
		wantExpiry = g.at.Add(2 * time.Minute)
	}
	if err != nil || got.Status != ports.AttachedWorkerExecutionApplied || got.AcceptedAttempt != g.head || got.AcceptedAttemptRevision != g.head.Revision || got.HarnessBindingDigest != g.binding || got.AttachedPiAPIInvocationPolicyDigest != g.decision.MappingDigest || !got.CheckedAt.Equal(g.at) || !got.UseExpiresAt.Equal(wantExpiry) {
		t.Fatalf("phase=%s original source not applied: status=%s err=%v", phase, got.Status, err)
	}
	if g.sql.transactions != beforeTx+1 || g.sql.active || g.sql.writes != beforeWrites || g.sql.writeAttempts != 0 {
		t.Fatalf("phase=%s must use exactly one read-only callback", phase)
	}
	counts := map[string]int{}
	for _, q := range g.sql.transactionQueries {
		counts[q]++
	}
	for _, q := range []string{piUseWorkerSQL, piUseConnectionSQL, piUseHeadSQL, piUseRunSQL, piUseJobSQL, piUseAttemptSQL, piUseQuotaSQL, piUseManifestSQL, piUseLeaseHeadSQL, piUseLeaseSQL, computeChoiceSessionSQL, computeChoiceParticipantSQL, piUseDeletionSQL, providerCredentialReadSQL} {
		if counts[piUseSQL(q)] != 1 {
			t.Fatalf("phase=%s missing/repeated original point read %s", phase, q)
		}
	}
	if counts[piUseSQL(piPolicyReadSQL)] != 2 || counts[piUseSQL(computeChoiceMembershipSQL)] != 2 || counts[piUseSQL(`SELECT CurrentUtcTimestamp()`)] != 2 {
		t.Fatalf("phase=%s missing same-transaction policy/principal/final-clock checks", phase)
	}
}
func piUseRequireDenied(t *testing.T, got ports.AttachedWorkerPiAPIUseResultV1) {
	t.Helper()
	want := ports.AttachedWorkerPiAPIUseResultV1{Status: ports.AttachedWorkerExecutionDenied}
	if got != want {
		t.Fatal("denied source leaked speculative attempt, binding or times")
	}
}

func TestPiAPIUseSourceConfirmThenAllThreePhases(t *testing.T) {
	g := newPiUseSourceFixture(t)
	for _, phase := range []string{"pre_issue", "physical_send", "immutable_finalize"} {
		g.requireApplied(t, phase)
	}
}

func TestPiAPIUseSourceReconnectCannotReplaceOriginalExecutionConnection(t *testing.T) {
	g := newPiUseSourceFixture(t)
	phases := []string{"pre_issue", "physical_send", "immutable_finalize"}
	for _, phase := range phases {
		g.requireApplied(t, phase)
	}
	originalWorker, originalConnection, originalHead, originalAuth := g.worker, g.connection, g.head, g.auth
	// A coherent current reconnect advances transport/auth authority, not the
	// accepted attempt's original execution-connection identity. The original
	// job, lease, fence, binding, context, caller allowance and expiry remain.
	g.worker.ConnectionGeneration++
	g.worker.Revision++
	g.connection.ID = "synthetic-reconnected-connection"
	g.connection.ConnectionGeneration = g.worker.ConnectionGeneration
	g.connection.ActivationChallengeID = "synthetic-reconnect-challenge"
	g.connection.Revision++
	g.connection.SecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("synthetic-reconnected-phase"))
	g.head.ConnectionID = g.connection.ID
	g.head.ConnectionGeneration = g.connection.ConnectionGeneration
	g.head.Revision++
	g.auth.ConnectionID = g.connection.ID
	g.auth.ConnectionGeneration = g.connection.ConnectionGeneration
	g.auth.PresentedSecretDigest = g.connection.SecretDigest
	g.auth.ExpectedAttemptRevision = g.head.Revision
	g.syncOriginalRows(t)
	for name, err := range map[string]error{
		"worker": g.worker.Validate(), "connection": g.connection.Validate(), "head": g.head.Validate(),
		"authorization": validateAttachedWorkerSealedInputAuthorization(g.auth),
	} {
		if err != nil {
			t.Fatalf("coherent reconnect %s: %v", name, err)
		}
	}
	if !attachedWorkerSealedInputAuthorized(g.auth, g.at, g.worker, g.connection, g.head) ||
		!piAPIOriginalJobMatches(g.loaded, g.lease, g.head, g.auth, g.binding, g.decision.MappingDigest, g.at) ||
		g.connection.CapabilityDigest != g.auth.CapabilityDigest {
		t.Fatal("reconnect must pass the broad current transport/original-job predicates")
	}
	if g.head.ExecutionConnectionID != originalHead.ExecutionConnectionID ||
		g.head.ExecutionConnectionGeneration != originalHead.ExecutionConnectionGeneration ||
		g.head.ExecutionConnectionID == g.auth.ConnectionID || g.head.ExecutionConnectionGeneration == g.auth.ConnectionGeneration {
		t.Fatal("reconnect did not preserve a distinct original execution connection")
	}
	beforeWrites := g.sql.writes
	for _, phase := range phases {
		beforeTx := g.sql.transactions
		got, err := g.use(phase)
		if err != nil || g.sql.transactions != beforeTx+1 || g.sql.active {
			t.Fatalf("phase=%s reconnect denial must use the actual source callback: %v", phase, err)
		}
		piUseRequireDenied(t, got)
	}
	if g.sql.writes != beforeWrites || g.sql.writeAttempts != 0 {
		t.Fatal("reconnected source attempted mutation")
	}
	// Restore the synthetic original snapshot as a positive control, without
	// rewriting any production decision or reconstructing execution authority.
	g.worker, g.connection, g.head, g.auth = originalWorker, originalConnection, originalHead, originalAuth
	g.syncOriginalRows(t)
	for _, phase := range phases {
		g.requireApplied(t, phase)
	}
}

func TestPiAPIUseSourceRejectsChangedCanonicalAuthority(t *testing.T) {
	for _, mode := range []string{"api_generation", "membership_version", "session_deletion", "participant_removed", "original_context", "head_revision", "head_cancel", "run_cancel", "attempt_worker", "connection_capability", "future_worker", "future_checkpoint", "missing_catalog"} {
		t.Run(mode, func(t *testing.T) {
			g := newPiUseSourceFixture(t)
			g.requireApplied(t, "physical_send")
			saved := map[string][]driver.Value{}
			for q, row := range g.sql.rows {
				saved[q] = append([]driver.Value(nil), row...)
			}
			catalog := g.store.attachedPiAPIPolicyCatalog
			switch mode {
			case "api_generation":
				row := g.sql.rows[piUseSQL(providerCredentialReadSQL)]
				var credential domain.ProviderCredentialBindingV1
				if err := json.Unmarshal([]byte(row[7].(string)), &credential); err != nil {
					t.Fatal(err)
				}
				credential.CredentialGeneration++
				row[1], row[7] = int64(credential.CredentialGeneration), choiceFixtureJSON(t, credential)
			case "membership_version":
				row := g.sql.rows[piUseSQL(computeChoiceMembershipSQL)]
				var m domain.TenantMembership
				if err := json.Unmarshal([]byte(row[0].(string)), &m); err != nil {
					t.Fatal(err)
				}
				m.SecurityVersion++
				row[0], row[3] = choiceFixtureJSON(t, m), int64(m.SecurityVersion)
			case "session_deletion":
				g.sql.rows[piUseSQL(piUseDeletionSQL)] = []driver.Value{string(g.loaded.Run.SessionID)}
			case "participant_removed":
				row := g.sql.rows[piUseSQL(computeChoiceParticipantSQL)]
				var p domain.SessionParticipant
				if err := json.Unmarshal([]byte(row[0].(string)), &p); err != nil {
					t.Fatal(err)
				}
				p.Status = domain.SessionParticipantRemoved
				row[0], row[2] = choiceFixtureJSON(t, p), string(p.Status)
			case "original_context":
				g.loaded.Job.ContextSnapshot.Size++
				g.syncOriginalRows(t)
			case "head_revision":
				g.head.Revision++
				g.syncOriginalRows(t)
			case "head_cancel":
				g.head.CancelRevision = 1
				g.head.CancelDeadline = g.at.Add(time.Second)
				g.syncOriginalRows(t)
			case "run_cancel":
				g.loaded.Run.CancellationRequestedAt = &g.at
				g.syncOriginalRows(t)
			case "attempt_worker":
				g.loaded.Attempt.WorkerID = "foreign-worker"
				g.syncOriginalRows(t)
			case "connection_capability":
				g.connection.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("d", 64))
				g.syncOriginalRows(t)
			case "future_worker":
				g.worker.UpdatedAt = g.at.Add(time.Microsecond)
				g.syncOriginalRows(t)
			case "future_checkpoint":
				g.connection.LastCheckpointAt = g.at.Add(time.Microsecond)
				g.syncOriginalRows(t)
			case "missing_catalog":
				g.store.attachedPiAPIPolicyCatalog = nil
			}
			beforeWrites := g.sql.writes
			for _, phase := range []string{"pre_issue", "physical_send", "immutable_finalize"} {
				got, _ := g.use(phase)
				piUseRequireDenied(t, got)
			}
			if g.sql.writes != beforeWrites || g.sql.writeAttempts != 0 {
				t.Fatal("denied phase attempted mutation")
			}
			// Restore only synthetic row snapshots, never a revoked real decision.
			g.sql.rows = saved
			g.store.attachedPiAPIPolicyCatalog = catalog
			if err := json.Unmarshal([]byte(saved[piUseSQL(piUseHeadSQL)][0].(string)), &g.head); err != nil {
				t.Fatal(err)
			}
			g.requireApplied(t, "physical_send")
		})
	}
}

func TestPiAPIUseSourceRevocationAndReplacementNeverAuthorizeOldMapping(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoke", true: "replace"}[replace], func(t *testing.T) {
			g := newPiUseSourceFixture(t)
			g.requireApplied(t, "physical_send")
			g.sql.writesAllowed = true
			if replace {
				r := g.request
				r.MutationID = "synthetic-new-owner-decision"
				r.ExpectedGeneration = g.decision.Generation
				g.sql.decisionKey, _ = piDecisionKey(r.MutationID)
				if next, err := g.store.ConfirmAttachedPiAPIPolicy(g.ctx, r); err != nil || next.MappingDigest == g.decision.MappingDigest {
					t.Fatalf("replacement setup: %v", err)
				}
			} else {
				r := ports.RevokeAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: g.request.ReadAttachedPiAPIPolicyV1, CSRFTokenDigest: g.request.CSRFTokenDigest, ExpectedGeneration: g.decision.Generation, ExpectedMapping: g.decision.MappingDigest}
				if err := g.store.RevokeAttachedPiAPIPolicy(g.ctx, r); err != nil {
					t.Fatal(err)
				}
			}
			g.sql.writesAllowed = false
			g.sql.writeAttempts = 0
			before := g.sql.writes
			for _, phase := range []string{"pre_issue", "physical_send", "immutable_finalize"} {
				got, _ := g.use(phase)
				piUseRequireDenied(t, got)
			}
			if g.sql.writeAttempts != 0 || g.sql.writes != before {
				t.Fatal("old mapping phase mutated current owner decision")
			}
		})
	}
}

func TestPiAPIUseSourceUnknownCommitAndCancelledCallerReturnNoSuccess(t *testing.T) {
	g := newPiUseSourceFixture(t)
	g.requireApplied(t, "physical_send")
	g.sql.unknownCommit = true
	before := g.sql.commits
	got, err := g.use("physical_send")
	if err == nil || g.sql.commits != before+1 {
		t.Fatal("unknown commit control did not reach actual commit")
	}
	piUseRequireDenied(t, got)
	if g.sql.writeAttempts != 0 {
		t.Fatal("unknown commit path attempted write")
	}
	g.sql.unknownCommit = false
	g.requireApplied(t, "physical_send")
	ctx, cancel := context.WithCancel(g.ctx)
	cancel()
	beforeTx := g.sql.transactions
	got, err = g.store.AuthorizeAttachedWorkerPiAPIUse(ctx, g.auth, g.binding, g.decision.MappingDigest, 1, "physical_send")
	if err == nil || g.sql.transactions != beforeTx {
		t.Fatal("cancelled caller entered source transaction")
	}
	piUseRequireDenied(t, got)
}
