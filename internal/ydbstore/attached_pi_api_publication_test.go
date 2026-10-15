package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const piPublicationReceiptSQL = `SELECT canonical_digest,candidate_fingerprint,created_at,payload FROM attached_worker_output_receipts
	 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`

const piPublicationUpdateSQL = `UPDATE attached_worker_output_receipts SET payload=CAST($7 AS JsonDocument)
	 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`

// SQL and catalog are synthetic; actual Store Confirm/current/READY and actual
// canonical output Materialize are composed here. This does not certify native
// YDB ordering, genuine native/tariff provenance, a model call or activation.
type piPublicationSQL struct {
	*piUseSQLSource
	record, pending         AttachedWorkerOutputReceiptV1
	key                     []driver.Value
	readyWrites, copyWrites int
}

func (f *piPublicationSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *piPublicationSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, err := f.piUseSQLSource.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	f.pending = f.record
	return piPublicationTx{Tx: tx, f: f}, nil
}

type piPublicationTx struct {
	driver.Tx
	f *piPublicationSQL
}

func (tx piPublicationTx) Commit() error {
	// The fixture's unknown-commit mode commits, then loses its response.
	// It must remain reconcilable while no speculative result escapes.
	if err := tx.f.storeReceipt(tx.f.pending); err != nil {
		return err
	}
	return tx.Tx.Commit()
}

func (f *piPublicationSQL) storeReceipt(record AttachedWorkerOutputReceiptV1) error {
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	f.record = record
	f.rows[piUseSQL(piPublicationReceiptSQL)] = []driver.Value{
		string(record.CanonicalDigest), record.CandidateFingerprint, record.CreatedAt, string(body),
	}
	return nil
}

func (f *piPublicationSQL) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if piUseSQL(query) != piUseSQL(piPublicationUpdateSQL) {
		return f.piUseSQLSource.ExecContext(ctx, query, args)
	}
	f.writeAttempts++
	if err := f.bounded(ctx); err != nil || !f.active || len(args) != 7 {
		return nil, errors.New("READY write outside exact original callback")
	}
	key := make([]driver.Value, 6)
	for i := range key {
		key[i] = args[i].Value
	}
	if !reflect.DeepEqual(key, f.key) {
		return nil, errors.New("foreign READY write selector")
	}
	var next AttachedWorkerOutputReceiptV1
	if json.Unmarshal([]byte(args[6].Value.(string)), &next) != nil || next.Ready == next.CopyInProgress ||
		!sameOutputReceiptPlan(next, f.record) {
		return nil, errors.New("READY write replaced original plan")
	}
	f.pending = next
	if next.Ready {
		f.readyWrites++
	} else {
		f.copyWrites++
	}
	return driver.RowsAffected(1), nil
}

func newPiPublicationFixture(t *testing.T) (*piUseSourceFixture, *piPublicationSQL, AttachedWorkerOutputReceiptRequest, AttachedWorkerOutputReceiptV1) {
	t.Helper()
	g := newPiUseSourceFixture(t)
	piNewWorkReadyProtocol(t, g, attachedworkerprotocol.FeatureOutputReceipt)
	auth := g.auth
	request := AttachedWorkerOutputReceiptRequest{Authorization: auth, Nonce: "synthetic-original-output",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "original bounded answer"},
		Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true,
			CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true}}
	fingerprint, err := attachedworkeroutput.CandidateFingerprint(request.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := request.Observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Genuine canonical construction, but a test-owned planning sink: no Put
	// reaches a remote service and this fixture does not claim a completed copy.
	planned := &plannedReceiptBlobs{upstream: &receiptEmptyObjectTestStore{}}
	materialization, err := attachedworkeroutput.Materialize(g.ctx, planned, g.loaded, auth.LeaseID, auth.LeaseGeneration, request.Candidate, g.at)
	if err != nil || materialization.Completion == nil {
		t.Fatalf("actual canonical output preparation: %v", err)
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(request.Candidate.Status, materialization)
	if err != nil {
		t.Fatal(err)
	}
	materialization.EvidenceDigest = digest
	record := AttachedWorkerOutputReceiptV1{Version: 1, CopyInProgress: true, Binding: receiptBindingWithoutBearer(auth),
		Nonce: request.Nonce, CandidateFingerprint: fingerprint, ObservationDigest: observation, Observation: request.Observation,
		CredentialRequired: true, HarnessBindingDigest: g.binding, Status: request.Candidate.Status,
		CanonicalDigest: digest, Materialization: materialization, CreatedAt: g.at}
	if err := validateAttachedWorkerReceiptRecord(record, auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration); err != nil {
		t.Fatalf("original canonical receipt: %v", err)
	}
	key := []driver.Value{string(auth.TenantID), string(auth.RunID), string(auth.OwnerUserID), string(auth.WorkerID), string(auth.AttemptID), int64(auth.LeaseGeneration)}
	f := &piPublicationSQL{piUseSQLSource: g.sql, key: key}
	if err := f.storeReceipt(record); err != nil {
		t.Fatal(err)
	}
	f.args[piUseSQL(piPublicationReceiptSQL)] = key
	bucket, err := webBucket(string(auth.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	memberRow := g.sql.rows[piUseSQL(computeChoiceMembershipSQL)]
	g.setRow(t, `SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`,
		[]driver.Value{int64(bucket), string(auth.OwnerUserID), string(auth.TenantID)}, memberRow[0])
	g.setRow(t, `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`,
		[]driver.Value{string(auth.TenantID), string(g.loaded.Run.SessionID)})
	g.setRow(t, `SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`,
		[]driver.Value{string(auth.TenantID), string(g.loaded.Run.SessionID)}, g.sql.rows[piUseSQL(computeChoiceSessionSQL)][0])
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	g.store.db = db
	return g, f, request, record
}

func TestPiAPIPublicationActualREADYRequiresCurrentOriginalPolicy(t *testing.T) {
	for _, mode := range []string{"missing_catalog", "resource_generation", "membership_version", "participant_removed", "deletion", "cancel", "original_context", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			g, f, request, planned := newPiPublicationFixture(t)
			g.requireApplied(t, "physical_send")
			originalRows := make(map[string][]driver.Value, len(f.rows))
			for q, row := range f.rows {
				originalRows[q] = append([]driver.Value(nil), row...)
			}
			originalCatalog := g.store.attachedPiAPIPolicyCatalog
			originalRecords := make(map[string][]driver.Value, len(f.records))
			for q, row := range f.records {
				originalRecords[q] = append([]driver.Value(nil), row...)
			}
			switch mode {
			case "missing_catalog":
				g.store.attachedPiAPIPolicyCatalog = nil
			case "resource_generation":
				row := f.rows[piUseSQL(providerCredentialReadSQL)]
				var credential domain.ProviderCredentialBindingV1
				if err := json.Unmarshal([]byte(row[7].(string)), &credential); err != nil {
					t.Fatal(err)
				}
				credential.CredentialGeneration++
				row[1], row[7] = int64(credential.CredentialGeneration), choiceFixtureJSON(t, credential)
			case "membership_version":
				row := f.rows[piUseSQL(computeChoiceMembershipSQL)]
				var member domain.TenantMembership
				if err := json.Unmarshal([]byte(row[0].(string)), &member); err != nil {
					t.Fatal(err)
				}
				member.SecurityVersion++
				row[0], row[3] = choiceFixtureJSON(t, member), int64(member.SecurityVersion)
				f.rows[piUseSQL(`SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`)][0] = row[0]
			case "participant_removed":
				f.rows[piUseSQL(computeChoiceParticipantSQL)] = nil
			case "deletion":
				f.rows[piUseSQL(piUseDeletionSQL)] = []driver.Value{string(g.loaded.Run.SessionID)}
			case "cancel":
				run := g.loaded.Run
				run.CancellationRequestedAt = &g.at
				g.setJSON(t, piUseRunSQL, []driver.Value{string(run.TenantID), string(run.ID)}, run)
			case "original_context":
				job := g.loaded.Job
				job.ContextSnapshot.SHA256 = string(domain.DigestAttachedWorkerCapability([]byte("foreign-context")))
				g.setJSON(t, piUseJobSQL, []driver.Value{string(job.TenantID), string(job.RunID)}, job)
			case "revoked":
				f.writesAllowed = true
				err := g.store.RevokeAttachedPiAPIPolicy(g.ctx, ports.RevokeAttachedPiAPIPolicyV1{
					ReadAttachedPiAPIPolicyV1: g.request.ReadAttachedPiAPIPolicyV1, CSRFTokenDigest: g.request.CSRFTokenDigest,
					ExpectedGeneration: g.decision.Generation, ExpectedMapping: g.decision.MappingDigest})
				f.writesAllowed = false
				if err != nil {
					t.Fatal("actual revoke:", err)
				}
			}
			f.writeAttempts = 0
			before := f.transactions
			got, err := g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
			if err != nil || got.Status != ports.AttachedWorkerExecutionFenced || !reflect.DeepEqual(got.Receipt, AttachedWorkerOutputReceiptV1{}) ||
				f.readyWrites != 0 || f.writeAttempts != 0 || f.transactions != before+1 || f.active || f.record.Ready {
				t.Fatalf("mode=%s stale original must deny actual READY with zero writes: status=%s writes=%d attempts=%d err=%v", mode, got.Status, f.readyWrites, f.writeAttempts, err)
			}
			// Restore only synthetic source snapshots, not a production decision.
			// The same original planned receipt can now legitimately become READY.
			f.rows, f.records = originalRows, originalRecords
			g.store.attachedPiAPIPolicyCatalog = originalCatalog
			got, err = g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
			if err != nil || got.Status != ports.AttachedWorkerExecutionApplied || !got.Receipt.Ready || got.Receipt.CopyInProgress || f.readyWrites != 1 || f.writeAttempts != 1 {
				t.Fatalf("mode=%s restored original READY control: status=%s writes=%d err=%v", mode, got.Status, f.readyWrites, err)
			}
			// Losing catalog authority after committed READY cannot strand exact
			// history or create a second write/copy/model permission.
			g.store.attachedPiAPIPolicyCatalog = nil
			got, err = g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
			if err != nil || got.Status != ports.AttachedWorkerExecutionReplayed || !got.Receipt.Ready || f.readyWrites != 1 {
				t.Fatalf("mode=%s historical READY must replay without current policy or new writes: status=%s err=%v", mode, got.Status, err)
			}
		})
	}
}

func TestPiAPIPublicationCopyStartRequiresCurrentSourceButREADYHistoryDoesNot(t *testing.T) {
	for _, mode := range []string{"current", "nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion"} {
		t.Run(mode, func(t *testing.T) {
			g, f, request, planned := newPiPublicationFixture(t)
			prior := planned
			prior.CopyInProgress = false
			if err := f.storeReceipt(prior); err != nil {
				t.Fatal(err)
			}
			if mode != "current" {
				piNewWorkChangeAuthority(t, g, mode)
			}
			var existing AttachedWorkerOutputReceiptV1
			var found, ownsCopy bool
			var prepareAt time.Time
			f.writeAttempts = 0
			status, err := g.store.outputReceiptHeadTransaction(g.ctx, request, planned.CandidateFingerprint, planned.ObservationDigest,
				&planned, &existing, &found, &prepareAt, &ownsCopy)
			if mode == "current" {
				if err != nil || status != ports.AttachedWorkerExecutionApplied || !found || !ownsCopy || f.copyWrites != 1 || f.readyWrites != 0 {
					t.Fatalf("actual copy-start original control: status=%s found=%t owns=%t copies=%d err=%v", status, found, ownsCopy, f.copyWrites, err)
				}
			} else if err != nil || status != ports.AttachedWorkerExecutionFenced || found || ownsCopy || f.writeAttempts != 0 || f.copyWrites != 0 {
				t.Fatalf("mode=%s stale source started/resumed copy: status=%s copies=%d err=%v", mode, status, f.copyWrites, err)
			}
			// An immutable READY postimage is a historical fixture, not a new
			// write justified by stale authority. The callback must read it only.
			history := planned
			history.CopyInProgress, history.Ready = false, true
			if err := f.storeReceipt(history); err != nil {
				t.Fatal(err)
			}
			g.store.attachedPiAPIPolicyCatalog = nil
			before := f.writeAttempts
			status, err = g.store.outputReceiptHeadTransaction(g.ctx, request, planned.CandidateFingerprint, planned.ObservationDigest,
				nil, &existing, &found, &prepareAt, &ownsCopy)
			if err != nil || status != ports.AttachedWorkerExecutionReplayed || !found || !existing.Ready || ownsCopy || f.writeAttempts != before {
				t.Fatalf("mode=%s exact READY history acquired new authority/write: status=%s found=%t err=%v", mode, status, found, err)
			}
		})
	}
}

func TestPiAPIPublicationFailureCleanupIsNotNewModelPolicyUse(t *testing.T) {
	g, f, request, planned := newPiPublicationFixture(t)
	request.Candidate = attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalFailed, FailureCode: "synthetic-failed"}
	request.Observation.RunnerFailed = true
	request.Observation.InvocationFailureCode = "synthetic-failed"
	var err error
	planned.CandidateFingerprint, err = attachedworkeroutput.CandidateFingerprint(request.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	planned.Observation, planned.Status = request.Observation, request.Candidate.Status
	planned.ObservationDigest, err = request.Observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	planned.Materialization, err = attachedworkeroutput.Materialize(g.ctx, &plannedReceiptBlobs{upstream: &receiptEmptyObjectTestStore{}},
		g.loaded, g.auth.LeaseID, g.auth.LeaseGeneration, request.Candidate, g.at)
	if err != nil {
		t.Fatal(err)
	}
	planned.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(planned.Status, planned.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	planned.Materialization.EvidenceDigest = planned.CanonicalDigest
	if err := f.storeReceipt(planned); err != nil {
		t.Fatal(err)
	}
	g.store.attachedPiAPIPolicyCatalog = nil
	got, err := g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
	if err != nil || got.Status != ports.AttachedWorkerExecutionApplied || !got.Receipt.Ready || got.Receipt.Status != domain.AttachedWorkerTerminalFailed || f.readyWrites != 1 {
		t.Fatalf("original failure publication incorrectly depended on new model permission: status=%s err=%v", got.Status, err)
	}
}

func TestPiAPIPublicationUnknownREADYCommitReturnsNoSpeculativeReceipt(t *testing.T) {
	g, f, request, planned := newPiPublicationFixture(t)
	f.unknownCommit = true
	got, err := g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
	if err == nil || !reflect.DeepEqual(got, AttachedWorkerOutputReceiptResult{}) || f.readyWrites != 1 || !f.record.Ready {
		t.Fatalf("unknown READY commit leaked receipt or lost committed fixture: writes=%d ready=%t err=%v", f.readyWrites, f.record.Ready, err)
	}
	f.unknownCommit = false
	g.store.attachedPiAPIPolicyCatalog = nil
	got, err = g.store.finishAttachedWorkerOutputReceipt(g.ctx, request, planned)
	if err != nil || got.Status != ports.AttachedWorkerExecutionReplayed || !got.Receipt.Ready || f.readyWrites != 1 {
		t.Fatalf("unknown committed READY history must reconcile, not republish: status=%s err=%v", got.Status, err)
	}
}
