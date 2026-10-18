//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// A terminal-index-only mutation is not a terminal canonical Run. Temporarily
// use the original canonical writer for both representations so these tests
// reach the pending-receipt barrier rather than an earlier consistency guard.
// Restore the exact original before the caller releases any blocked writer.
func aw07PendingReceiptDeletionBarrier(
	ctx context.Context, store *ydbstore.Store, db *sql.DB,
	runID domain.RunID, status domain.RunStatus, deletion domain.SessionDeletion,
) (err error) {
	var at time.Time
	if err := db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return fmt.Errorf("sample terminal fixture database clock: %w", err)
	}
	if at.IsZero() {
		return errors.New("terminal fixture database clock is zero")
	}
	return aw07PendingReceiptDeletionBarrierAt(ctx, store, runID, status, deletion, at.UTC())
}

// TEST ONLY seam for deterministic transaction-fault coverage. Native callers
// above still obtain the timestamp from their actual database. No permission,
// receipt, state implementation or hook is added to production.
type aw07DeletionFixtureStore interface {
	ports.StateStore
	RequestSessionDeletion(context.Context, domain.SessionDeletion) (domain.SessionDeletion, error)
}

func aw07PendingReceiptDeletionBarrierAt(ctx context.Context, store aw07DeletionFixtureStore, runID domain.RunID, status domain.RunStatus, deletion domain.SessionDeletion, at time.Time) (err error) {
	var original, terminal domain.Run
	captured := false
	// A Put may commit before its ACK is lost. Arm restoration before the
	// transaction, retaining its FIRST exact images across every callback retry.
	defer func() {
		if !captured {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		restoreErr := store.Transact(cleanup, deletion.TenantID, func(tx ports.StateTx) error {
			current, found, err := tx.GetRun(cleanup, runID)
			if err != nil {
				return err
			}
			if !found {
				return errors.New("temporary canonical Run changed; refusing to overwrite another writer")
			}
			switch {
			case reflect.DeepEqual(current, original):
				return nil // Includes a restore whose ACK was lost.
			case reflect.DeepEqual(current, terminal):
				return tx.PutRun(cleanup, original)
			default:
				return errors.New("temporary canonical Run changed; refusing to overwrite another writer")
			}
		})
		if restoreErr != nil {
			err = errors.Join(err, fmt.Errorf("restore original canonical Run and deletion index: %w", restoreErr))
		}
	}()
	if err := store.Transact(ctx, deletion.TenantID, func(tx ports.StateTx) error {
		run, found, err := tx.GetRun(ctx, runID)
		if err != nil {
			return err
		}
		if !found || run.ID != runID || run.TenantID != deletion.TenantID || run.SessionID != deletion.SessionID {
			return errors.New("terminal deletion fixture requires the original canonical Run scope")
		}
		if !captured {
			if run.Status != domain.RunRunning {
				return errors.New("terminal deletion fixture requires the original running canonical Run")
			}
			candidate := run.Clone()
			if err := candidate.Transition(status, at); err != nil {
				return err
			}
			original, terminal, captured = run.Clone(), candidate, true
		}
		switch {
		case reflect.DeepEqual(run, original):
			return tx.PutRun(ctx, terminal)
		case reflect.DeepEqual(run, terminal):
			return nil // Exact committed temporary postimage on callback retry.
		default:
			return errors.New("temporary canonical Run changed during setup; refusing to overwrite another writer")
		}
	}); err != nil {
		return fmt.Errorf("temporarily mark canonical Run and deletion index terminal: %w", err)
	}
	if _, err := store.RequestSessionDeletion(ctx, deletion); err == nil {
		return errors.New("deletion unexpectedly accepted despite a pending attached-worker output receipt")
	} else if !strings.Contains(err.Error(), "pending attached-worker output receipt") {
		return fmt.Errorf("deletion did not reach the pending attached-worker output receipt barrier: %w", err)
	}
	return nil
}

// These tests execute the actual native fixture core without a database. The
// narrow fake models transaction commit/ACK outcomes, not runtime authority or
// attached-worker evidence. Unsupported StateTx methods are never provided.
type aw07DeletionFixtureFaultStore struct {
	run                           domain.Run
	index                         domain.RunStatus
	expectedTerminal              domain.Run
	mode                          string
	tenant                        domain.TenantID
	transactions, puts, deletions int
	cleanupContexts               []context.Context
	foreign                       *domain.Run
	cancel                        context.CancelFunc
	cleanupWasLive                bool
}

type aw07DeletionFixtureFaultTx struct {
	ports.StateTx
	store   *aw07DeletionFixtureFaultStore
	pending *domain.Run
}

func (tx *aw07DeletionFixtureFaultTx) GetRun(ctx context.Context, id domain.RunID) (domain.Run, bool, error) {
	if ctx.Err() != nil {
		return domain.Run{}, false, ctx.Err()
	}
	if id != tx.store.run.ID {
		return domain.Run{}, false, nil
	}
	if tx.pending != nil {
		return tx.pending.Clone(), true, nil
	}
	return tx.store.run.Clone(), true, nil
}
func (tx *aw07DeletionFixtureFaultTx) PutRun(ctx context.Context, run domain.Run) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if run.Validate() != nil || run.ID != tx.store.run.ID || run.TenantID != tx.store.tenant {
		return errors.New("fixture core attempted invalid/foreign Run write")
	}
	tx.store.puts++
	value := run.Clone()
	tx.pending = &value
	return nil
}
func (f *aw07DeletionFixtureFaultStore) Transact(ctx context.Context, tenant domain.TenantID, fn func(ports.StateTx) error) error {
	f.transactions++
	if tenant != f.tenant || ctx == nil || ctx.Err() != nil {
		return errors.New("fixture core transaction context/scope")
	}
	if f.transactions > 1 {
		f.cleanupContexts = append(f.cleanupContexts, ctx)
		f.cleanupWasLive = ctx.Err() == nil
	}
	invoke := func() error {
		tx := &aw07DeletionFixtureFaultTx{store: f}
		if err := fn(tx); err != nil {
			return err
		}
		if tx.pending != nil {
			f.run = tx.pending.Clone()
			f.index = tx.pending.Status
		}
		return nil
	}
	if f.transactions == 1 && f.mode == "pre_commit_failure" {
		tx := &aw07DeletionFixtureFaultTx{store: f}
		if err := fn(tx); err != nil {
			return err
		}
		return errors.New("scripted transaction failed without persistence")
	}
	if err := invoke(); err != nil {
		return err
	}
	if f.transactions == 1 {
		switch f.mode {
		case "lost_ack_persisted":
			return errors.New("scripted persisted commit ACK lost")
		case "lost_ack_callback_retry":
			return invoke()
		case "cancel_after_persist":
			f.cancel()
			return ctx.Err()
		case "foreign_setup_retry":
			f.run = f.foreign.Clone()
			f.index = f.foreign.Status
			return invoke()
		}
	}
	if f.transactions == 2 && f.mode == "restore_ack_callback_retry" {
		return invoke()
	}
	return nil
}
func (f *aw07DeletionFixtureFaultStore) RequestSessionDeletion(ctx context.Context, d domain.SessionDeletion) (domain.SessionDeletion, error) {
	if ctx.Err() != nil {
		return domain.SessionDeletion{}, ctx.Err()
	}
	f.deletions++
	if !reflect.DeepEqual(f.run, f.expectedTerminal) || f.index != f.expectedTerminal.Status {
		return domain.SessionDeletion{}, errors.New("fixture deletion did not see matching terminal canonical Run and index")
	}
	if f.foreign != nil {
		f.run = f.foreign.Clone()
		f.index = f.foreign.Status
	}
	switch f.mode {
	case "deletion_success":
		return d, nil
	case "wrong_barrier":
		return domain.SessionDeletion{}, errors.New("different canonical admission failure")
	case "deletion_error":
		return domain.SessionDeletion{}, errors.New("scripted deletion storage error")
	}
	return domain.SessionDeletion{}, errors.New("pending attached-worker output receipt")
}

func TestAW07PendingReceiptDeletionBarrierFaults(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		wantErr   bool
		deletions int
	}{
		{name: "success", deletions: 1},
		{name: "pre_commit_failure", wantErr: true},
		{name: "lost_ack_persisted", wantErr: true},
		{name: "lost_ack_callback_retry", deletions: 1},
		{name: "restore_ack_callback_retry", deletions: 1},
		{name: "foreign_postimage", wantErr: true, deletions: 1},
		{name: "foreign_setup_retry", wantErr: true},
		{name: "foreign_setup", wantErr: true},
		{name: "cancel_after_persist", wantErr: true},
		{name: "deletion_success", wantErr: true, deletions: 1},
		{name: "wrong_barrier", wantErr: true, deletions: 1},
		{name: "deletion_error", wantErr: true, deletions: 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			at := time.Date(2026, time.October, 18, 12, 0, 0, 0, time.UTC)
			original := domain.Run{ID: "fixture-run", TenantID: "fixture-tenant", SessionID: "fixture-session", TriggerEventID: "fixture-event", SubscriptionConnectionID: "fixture-subscription", Status: domain.RunRunning, IdempotencyKey: "fixture-idempotency", CreatedAt: at.Add(-time.Minute), UpdatedAt: at.Add(-time.Second)}
			if err := original.Validate(); err != nil {
				t.Fatal(err)
			}
			f := &aw07DeletionFixtureFaultStore{run: original.Clone(), index: original.Status, mode: scenario.name, tenant: original.TenantID}
			f.expectedTerminal = original.Clone()
			if err := f.expectedTerminal.Transition(domain.RunSucceeded, at); err != nil {
				t.Fatal(err)
			}
			want := original.Clone()
			if scenario.name == "foreign_postimage" || scenario.name == "foreign_setup_retry" || scenario.name == "foreign_setup" {
				foreign := original.Clone()
				foreign.UpdatedAt = at.Add(time.Second)
				if err := foreign.Transition(domain.RunFailed, at.Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				f.foreign = &foreign
				want = foreign.Clone()
				if scenario.name == "foreign_setup" {
					f.run = foreign.Clone()
					f.index = foreign.Status
				}
			}
			call, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			f.cancel = cancel
			deletion := domain.SessionDeletion{TenantID: original.TenantID, SessionID: original.SessionID, RequestedBy: "fixture-owner", Reason: "pending receipt barrier", State: domain.SessionDeletionRequested, RequestedAt: at}
			err := aw07PendingReceiptDeletionBarrierAt(call, f, original.ID, domain.RunSucceeded, deletion, at)
			if (err != nil) != scenario.wantErr {
				t.Errorf("transaction scenario=%s err=%v wantErr=%t", scenario.name, err, scenario.wantErr)
			}
			if !reflect.DeepEqual(f.run, want) {
				t.Errorf("fixture left or overwrote canonical Run: scenario=%s gotStatus=%s gotUpdated=%s wantStatus=%s wantUpdated=%s transactions=%d puts=%d err=%v", scenario.name, f.run.Status, f.run.UpdatedAt, want.Status, want.UpdatedAt, f.transactions, f.puts, err)
			}
			if f.index != want.Status {
				t.Errorf("deletion index=%s want=%s scenario=%s", f.index, want.Status, scenario.name)
			}
			if f.deletions != scenario.deletions {
				t.Errorf("deletion calls=%d want=%d scenario=%s", f.deletions, scenario.deletions, scenario.name)
			}
			if scenario.name == "foreign_setup" && f.puts != 0 {
				t.Errorf("foreign setup was overwritten: puts=%d", f.puts)
			}
			if (scenario.name == "foreign_postimage" || scenario.name == "foreign_setup_retry") && f.puts != 1 {
				t.Errorf("foreign postimage was overwritten: puts=%d want=1", f.puts)
			}
			if scenario.name == "cancel_after_persist" && (!f.cleanupWasLive || call.Err() == nil) {
				t.Error("cancelled caller did not use independent live restoration")
			}
			for _, ctx := range f.cleanupContexts {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("restoration context lacks finite deadline")
				}
			}
		})
	}
}

func setAW07MembershipStatus(t *testing.T, db *sql.DB, tenant domain.TenantID, owner domain.UserID,
	status domain.TenantMembershipStatus, version uint64, createdAt time.Time,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	membership := domain.TenantMembership{
		TenantID: tenant, UserID: owner, Role: domain.TenantMembershipOwner,
		Status: status, SecurityVersion: version, CreatedAt: createdAt,
		UpdatedAt: createdAt.Add(time.Duration(version) * time.Second),
	}
	if err := membership.Validate(); err != nil {
		t.Fatalf("membership fixture for owner %s: %v", owner, err)
	}
	payload, err := json.Marshal(membership)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := ydbpartition.BucketV1(string(owner))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx,
		`UPDATE tenant_memberships SET status=$1, security_version=$2, updated_at=$3,
		 record=CAST($4 AS JsonDocument) WHERE user_bucket=$5 AND user_id=$6 AND tenant_id=$7`,
		status, version, membership.UpdatedAt, string(payload), bucket, owner, tenant); err != nil {
		t.Fatalf("set owner %s membership to %s: %v", owner, status, err)
	}
	var got domain.TenantMembershipStatus
	if err := db.QueryRowContext(ctx,
		`SELECT status FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`,
		bucket, owner, tenant).Scan(&got); err != nil || got != status {
		t.Fatalf("read owner %s membership after update: got=%s want=%s err=%v", owner, got, status, err)
	}
}

type aw07ReceiptBinder struct {
	bearer []byte
	auth   ports.AttachedWorkerSealedInputAuthorization
}

type failReceiptBlobs struct {
	ports.BlobStore
	failures      int
	notDispatched bool
}

type lateReceiptBlobs struct {
	ports.BlobStore
	release   chan struct{}
	done      chan error
	started   chan struct{}
	startOnce sync.Once
}

func (blobs *lateReceiptBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	content, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
	blobs.startOnce.Do(func() { close(blobs.started) })
	go func() {
		<-blobs.release
		_, putErr := blobs.BlobStore.Put(context.WithoutCancel(ctx), tenant, key, bytes.NewReader(content))
		blobs.done <- putErr
	}()
	return domain.BlobRef{}, errors.New("simulated lost response while remote put is still in flight")
}

type blockingReceiptBlobs struct {
	ports.BlobStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (blobs *blockingReceiptBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	blobs.once.Do(func() { close(blobs.entered) })
	select {
	case <-blobs.release:
		return blobs.BlobStore.Put(ctx, tenant, key, body)
	case <-ctx.Done():
		return domain.BlobRef{}, ctx.Err()
	}
}

func (blobs *failReceiptBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	if blobs.failures > 0 {
		blobs.failures--
		if blobs.notDispatched {
			return domain.BlobRef{}, ydbstore.ErrAttachedWorkerReceiptPutNotDispatched
		}
		return domain.BlobRef{}, errors.New("simulated canonical copy outage")
	}
	return blobs.BlobStore.Put(ctx, tenant, key, body)
}

func (binder aw07ReceiptBinder) BindOutputReceiptBearer(bearer []byte, auth ports.AttachedWorkerSealedInputAuthorization) (ports.AttachedWorkerSealedInputAuthorization, error) {
	if string(bearer) != string(binder.bearer) || auth.TenantID != binder.auth.TenantID ||
		auth.OwnerUserID != binder.auth.OwnerUserID || auth.WorkerID != binder.auth.WorkerID ||
		auth.ConnectionID != binder.auth.ConnectionID || auth.PresentedSecretDigest != "" {
		return ports.AttachedWorkerSealedInputAuthorization{}, errors.New("unauthorized receipt bearer")
	}
	auth.PresentedSecretDigest = binder.auth.PresentedSecretDigest
	return auth, nil
}

// The provider is test-only: its resource and credential generation are
// owner-specific authority facts, not a real key or an enabled backend.
func aw07TestProviderBinding(owner domain.UserID, suffix string, generation uint64, now time.Time) func(*domain.HarnessBindingV2) {
	return func(binding *domain.HarnessBindingV2) {
		binding.Backend.BackendKind = domain.HarnessBackendDirectOpenRouterV1
		binding.Backend.ProviderContractKind = domain.ProviderContractInvocationV1
		binding.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryDirectV1
		binding.Resource = domain.ProviderResourceBindingV1{
			Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-" + suffix,
			OwnerUserID: owner, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1,
			CredentialGeneration: generation,
		}
		expires := now.Add(time.Hour)
		binding.EvidenceExpiresAt = &expires
	}
}

func TestAW07ReceiptTwoOwnerCanonicalTerminalAndReplay(t *testing.T) {
	aStore, aClient, aWorker, aConnection, aSecret, aKey, aManifest, aNow := readyAttachedWorkerForDrainWithIdentity(t,
		"receipt-a", "", "", attachedworkerprotocol.FeatureOutputReceipt)
	bStore, bClient, bWorker, bConnection, bSecret, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"receipt-b", aWorker.TenantID, aWorker.ID, attachedworkerprotocol.FeatureOutputReceipt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	aSuffix := attachedWorkerDrainTestSuffix(t, "receipt-a")
	bSuffix := attachedWorkerDrainTestSuffix(t, "receipt-b")
	a := aw07ClaimedInputWithPayloadAndBinding(t, aStore, aClient, aWorker, aConnection, aSecret, aNow,
		aSuffix, nil, nil, aw07TestProviderBinding(aWorker.OwnerUserID, aSuffix, 3, aNow))
	b := aw07ClaimedInputWithPayloadAndBinding(t, bStore, bClient, bWorker, bConnection, bSecret, bNow,
		bSuffix, nil, nil, aw07TestProviderBinding(bWorker.OwnerUserID, bSuffix, 7, bNow))
	if a.request.OwnerUserID == b.request.OwnerUserID || a.request.WorkerID != b.request.WorkerID {
		t.Fatal("receipt gate requires colliding worker locator under distinct owners")
	}
	blobs := newSessionAPITestBlobs()
	loaded, found, err := aStore.LoadWorkerJob(ctx, a.request.TenantID, a.request.RunID)
	if err != nil || !found {
		t.Fatalf("load pinned job: found=%t err=%v", found, err)
	}
	peer, found, err := bStore.LoadWorkerJob(ctx, b.request.TenantID, b.request.RunID)
	if err != nil || !found {
		t.Fatalf("load peer job: found=%t err=%v", found, err)
	}
	if loaded.Job.HarnessBinding.Resource.ResourceID == peer.Job.HarnessBinding.Resource.ResourceID ||
		loaded.Job.HarnessBinding.Resource.CredentialGeneration != 3 ||
		peer.Job.HarnessBinding.Resource.CredentialGeneration != 7 ||
		loaded.Job.HarnessBinding.Resource.OwnerUserID != a.request.OwnerUserID ||
		peer.Job.HarnessBinding.Resource.OwnerUserID != b.request.OwnerUserID {
		t.Fatalf("test provider authority not owner-distinct: A=%+v B=%+v",
			loaded.Job.HarnessBinding.Resource, peer.Job.HarnessBinding.Resource)
	}
	credentialRequired := loaded.Job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
	request := ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: a.request, Nonce: "receipt-nonce-a",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "owner A answer"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
			CredentialReleaseRequired: credentialRequired, CredentialReleased: credentialRequired,
		},
	}
	setAW07MembershipStatus(t, aClient.DB, a.request.TenantID, a.request.OwnerUserID,
		domain.TenantMembershipSuspended, 2, aNow)
	if denied, err := aStore.OfferAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptOffer{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		RunID: a.request.RunID, AttemptID: a.request.AttemptID,
		ReservationID: a.attempt.ReservationID, LeaseID: a.request.LeaseID, LeaseTTL: 20 * time.Minute,
	}); err != nil || denied.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("suspended owner received offer: result=%+v err=%v", denied, err)
	}
	if denied, err := aStore.PollAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptPoll{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		ConnectionID: a.request.ConnectionID, PresentedSecretDigest: a.request.PresentedSecretDigest,
	}); err != nil || denied.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("suspended owner polled attempt: result=%+v err=%v", denied, err)
	}
	if denied, err := aStore.PollAttachedWorkerControl(ctx, ports.AttachedWorkerControlPoll{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		ConnectionID: a.request.ConnectionID, PresentedSecretDigest: a.request.PresentedSecretDigest,
	}); err != nil || denied.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("suspended owner polled control: result=%+v err=%v", denied, err)
	}
	if denied, err := aStore.AuthorizeAttachedWorkerSealedInput(ctx, a.request); err != nil ||
		denied.Status != ports.AttachedWorkerExecutionDenied {
		t.Fatalf("suspended owner read sealed input: result=%+v err=%v", denied, err)
	}
	if denied, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); err != nil ||
		denied.Status != ports.AttachedWorkerExecutionFenced || denied.Receipt.Version != 0 {
		t.Fatalf("suspended owner published receipt: result=%+v err=%v", denied, err)
	}
	aw07Authorized(t, bStore, ctx, b)
	setAW07MembershipStatus(t, aClient.DB, a.request.TenantID, a.request.OwnerUserID,
		domain.TenantMembershipActive, 3, aNow)
	for _, scenario := range []struct {
		name   string
		mutate func(*attachedworkeroutput.ProcessObservationV1)
	}{
		{name: "credential release", mutate: func(observation *attachedworkeroutput.ProcessObservationV1) {
			observation.CredentialReleased = false
		}},
		{name: "descendant reap", mutate: func(observation *attachedworkeroutput.ProcessObservationV1) {
			observation.DescendantsReaped = false
		}},
		{name: "boundary release", mutate: func(observation *attachedworkeroutput.ProcessObservationV1) {
			observation.BoundaryReleased = false
		}},
		{name: "attempt root cleanup", mutate: func(observation *attachedworkeroutput.ProcessObservationV1) {
			observation.CleanupSucceeded = false
		}},
	} {
		t.Run("missing "+scenario.name, func(t *testing.T) {
			unsafe := request
			scenario.mutate(&unsafe.Observation)
			if result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, unsafe); err != nil ||
				result.Status != ports.AttachedWorkerExecutionFenced || result.Receipt.Version != 0 {
				t.Fatalf("unsafe process observation was not fenced: result=%+v err=%v", result, err)
			}
		})
	}
	foreign := request
	foreign.Authorization.OwnerUserID = b.request.OwnerUserID
	foreign.Authorization.ConnectionID = b.request.ConnectionID
	foreign.Authorization.PresentedSecretDigest = b.request.PresentedSecretDigest
	if result, err := bStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, foreign); err != nil ||
		result.Status == ports.AttachedWorkerExecutionApplied || result.Status == ports.AttachedWorkerExecutionReplayed {
		t.Fatalf("cross-owner output admitted: result=%+v err=%v", result, err)
	}
	if result, err := aStore.CommitAttachedWorkerTerminal(ctx, ports.AttachedWorkerTerminalCommit{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration,
		Materialization: ports.AttachedWorkerTerminalMaterialization{EvidenceDigest: domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(make([]byte, 32)))},
	}); err == nil && result.Outbound != nil {
		t.Fatalf("process-only terminal produced ACK without receipt: %+v", result)
	}
	// Both bounded HTTPS attempts fail, leaving the prepared YDB receipt
	// visible while no canonical object has been copied.
	unstableBlobs := &failReceiptBlobs{BlobStore: blobs, failures: 2, notDispatched: true}
	service, err := attachedworkerreceipt.NewService(aw07ReceiptBinder{bearer: []byte("receipt-bearer-a"), auth: a.request}, aStore, unstableBlobs)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(attachedworkerreceipt.Handler(service))
	defer server.Close()
	publisher, err := attachedworkerreceipt.NewClientPublisher(server.URL+attachedworkerreceipt.PathV1, server.Client(), []byte("receipt-bearer-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	submission := attachedworkerdaemontransport.ReceiptSubmissionV1{
		Request: attachedworkerdaemontransport.MaterializationRequestV1{
			TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
			ConnectionID: a.request.ConnectionID, EnrollmentGeneration: a.request.EnrollmentGeneration,
			ConnectionGeneration: a.request.ConnectionGeneration, Attempt: a.binding,
			AttemptSequence: a.request.AttemptSequence,
		},
		Nonce: request.Nonce, Candidate: request.Candidate, Observation: request.Observation,
	}
	if _, err := publisher.Publish(ctx, submission); !errors.Is(err, attachedworkerreceipt.ErrUnavailable) {
		t.Fatalf("copy outage did not fail closed: %v", err)
	}
	var pendingPayload string
	if err := aClient.DB.QueryRowContext(ctx,
		`SELECT payload FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
		a.request.TenantID, a.request.RunID, a.request.OwnerUserID, a.request.WorkerID, a.request.AttemptID, a.request.LeaseGeneration,
	).Scan(&pendingPayload); err != nil {
		t.Fatalf("pending receipt was not durably staged before object copy: %v", err)
	}
	var pendingReceipt ydbstore.AttachedWorkerOutputReceiptV1
	if err := json.Unmarshal([]byte(pendingPayload), &pendingReceipt); err != nil || pendingReceipt.Ready ||
		pendingReceipt.Materialization.Completion == nil || len(pendingReceipt.Materialization.Completion.Events) != 1 {
		t.Fatalf("invalid pending receipt: %+v err=%v", pendingReceipt, err)
	}
	if _, copied := blobs.values[pendingReceipt.Materialization.Completion.Events[0].Payload.Key]; copied {
		t.Fatal("canonical object was written before pending receipt")
	}
	// Retry may take over only after the previous synchronous writer has
	// released ownership. A second writer while Put is held could otherwise
	// race deletion after the first writer marks the receipt ready.
	blocked := &blockingReceiptBlobs{BlobStore: blobs, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseCopyOnce sync.Once
	releaseCopy := func() { releaseCopyOnce.Do(func() { close(blocked.release) }) }
	type copyOutcome struct {
		result ydbstore.AttachedWorkerOutputReceiptResult
		err    error
	}
	copyDone := make(chan copyOutcome, 1)
	copyCtx, cancelCopy := context.WithCancel(ctx)
	copyJoined := false
	defer func() {
		cancelCopy()
		releaseCopy()
		if copyJoined {
			return
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		select {
		case <-copyDone:
		case <-cleanup.Done():
			select {
			case <-copyDone:
			default:
				t.Error("blocked receipt writer did not join during bounded cleanup")
			}
		}
	}()
	go func() {
		result, err := aStore.CreateAttachedWorkerOutputReceipt(copyCtx, blocked, request)
		copyDone <- copyOutcome{result: result, err: err}
	}()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		select {
		case <-blocked.entered:
		default:
			t.Fatalf("canonical copy never started: %v", ctx.Err())
		}
	}
	if _, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); !errors.Is(err, ydbstore.ErrAttachedWorkerReceiptCopyInProgress) {
		t.Fatalf("concurrent exact retry was allowed to copy: %v", err)
	}
	if _, copied := blobs.values[pendingReceipt.Materialization.Completion.Events[0].Payload.Key]; copied {
		t.Fatal("duplicate request copied canonical object before the active writer settled")
	}
	// Model canonical terminal state while the non-transactional copy is
	// blocked, then restore both Run and index before the writer can finish.
	deletion := domain.SessionDeletion{
		TenantID: a.request.TenantID, SessionID: loaded.Job.SessionID,
		RequestedBy: a.request.OwnerUserID, Reason: "receipt copy race",
		State: domain.SessionDeletionRequested, RequestedAt: aNow.Add(time.Second),
	}
	if err := aw07PendingReceiptDeletionBarrier(ctx, aStore, aClient.DB, a.request.RunID, domain.RunSucceeded, deletion); err != nil {
		t.Fatalf("deletion escaped the in-flight receipt barrier: %v", err)
	}
	releaseCopy()
	var outcome copyOutcome
	select {
	case outcome = <-copyDone:
	case <-ctx.Done():
		select {
		case outcome = <-copyDone:
		default:
			t.Fatalf("single copy writer did not settle: %v", ctx.Err())
		}
	}
	copyJoined = true
	if outcome.err != nil || outcome.result.Status != ports.AttachedWorkerExecutionApplied || !outcome.result.Receipt.Ready {
		t.Fatalf("single copy writer did not commit ready receipt: %+v err=%v", outcome.result, outcome.err)
	}
	commitment, err := publisher.Publish(ctx, submission)
	if err != nil || commitment.Status != domain.AttachedWorkerTerminalSucceeded {
		t.Fatalf("authenticated HTTPS receipt: commitment=%+v err=%v", commitment, err)
	}
	created, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request)
	if err != nil || created.Status != ports.AttachedWorkerExecutionReplayed ||
		created.Receipt.Materialization.Completion == nil {
		t.Fatalf("read back HTTPS-created receipt: result=%+v err=%v", created, err)
	}
	foreignReplay := request
	foreignReplay.Authorization = b.request
	foreignReplay.Authorization.RunID = a.request.RunID
	foreignReplay.Authorization.AttemptID = a.request.AttemptID
	foreignReplay.Authorization.LeaseID = a.request.LeaseID
	foreignReplay.Authorization.LeaseGeneration = a.request.LeaseGeneration
	if result, err := bStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, foreignReplay); err != nil ||
		result.Status == ports.AttachedWorkerExecutionApplied || result.Status == ports.AttachedWorkerExecutionReplayed ||
		result.Receipt.Version != 0 {
		t.Fatalf("owner B replayed owner A's ready receipt: result=%+v err=%v", result, err)
	}
	for _, scenario := range []struct {
		name   string
		mutate func(*ydbstore.AttachedWorkerOutputReceiptRequest)
	}{
		{name: "nonce", mutate: func(candidate *ydbstore.AttachedWorkerOutputReceiptRequest) {
			candidate.Nonce = "receipt-nonce-other"
		}},
		{name: "observation", mutate: func(candidate *ydbstore.AttachedWorkerOutputReceiptRequest) {
			candidate.Observation.ExitCode++
		}},
	} {
		t.Run("divergent "+scenario.name, func(t *testing.T) {
			changed := request
			scenario.mutate(&changed)
			if result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, changed); err != nil ||
				result.Status != ports.AttachedWorkerExecutionConflict || result.Receipt.Version != 0 {
				t.Fatalf("divergent %s replay was not rejected: result=%+v err=%v", scenario.name, result, err)
			}
		})
	}
	request.Candidate.Summary = "divergent"
	if result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); err != nil || result.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("divergent same-key candidate escaped: result=%+v err=%v", result, err)
	}
	request.Candidate.Summary = "owner A answer"
	if result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); err != nil ||
		result.Status != ports.AttachedWorkerExecutionReplayed || result.Receipt.CanonicalDigest != created.Receipt.CanonicalDigest {
		t.Fatalf("lost-response replay changed receipt: result=%+v err=%v", result, err)
	}
	digest, err := hex.DecodeString(string(created.Receipt.CanonicalDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminal := attachedworkerprotocol.FrameV1{
		Version:   a.accepted.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, a.nextWorkerSequence),
		WorkerID:  string(a.request.WorkerID), EnrollmentGeneration: a.request.EnrollmentGeneration,
		ConnectionGeneration: a.request.ConnectionGeneration, Sequence: a.nextWorkerSequence,
		Ack: a.accepted.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{Binding: a.binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: digest},
	}
	pending, err := aStore.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		ConnectionID: a.request.ConnectionID, AttemptID: a.request.AttemptID,
		LeaseGeneration: a.request.LeaseGeneration, PresentedSecretDigest: a.request.PresentedSecretDigest,
		InboundFrame: terminal,
	})
	if err != nil || pending.Status != ports.AttachedWorkerExecutionApplied ||
		pending.Attempt.State != domain.AttachedWorkerAttemptTerminalPending {
		t.Fatalf("receipt terminal pending: result=%+v err=%v", pending, err)
	}
	// A transport reconnect may rotate the bearer after Terminal evidence is
	// persisted. The immutable execution receipt must remain recoverable only
	// through the current owner-scoped head, without re-running the provider.
	previous, found, err := aStore.LoadAttachedWorkerConnection(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("load receipt owner's terminal-pending connection: found=%t err=%v", found, err)
	}
	currentWorker, found, err := aStore.LoadAttachedWorker(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("load receipt owner before reconnect: found=%t err=%v", found, err)
	}
	challengeCreate := attachedWorkerChallengeCreateFixture(currentWorker, "reconnect")
	challengeCreate.Lifetime = 10 * time.Minute
	challengeCreate.Purpose = domain.AttachedWorkerAttachReconnect
	challengeCreate.ExpectedConnectionID = previous.ID
	challengeCreate.ExpectedConnectionRevision = previous.Revision
	challengeCreate.ExpectedCapabilityDigest = previous.CapabilityDigest
	challengeCreate.ExpectedProtocolSnapshot = append([]byte(nil), previous.ProtocolSnapshot...)
	challenge, err := aStore.CreateAttachedWorkerAttachChallenge(ctx, challengeCreate)
	if err != nil {
		t.Fatalf("create receipt reconnect challenge: %v", err)
	}
	channel := bytes.Repeat([]byte{0x7b}, 32)
	attachedSnapshot, readySnapshot, manifestSignature := attachedWorkerReconnectProtocolSnapshotFixtureWithManifest(
		t, currentWorker, previous, challenge, aKey, channel, aManifest)
	rotatedSecret := domain.DigestAttachedWorkerConnectionSecret([]byte("receipt-owner-a-reconnected"))
	activated, err := aStore.ActivateAttachedWorkerConnection(ctx, ports.AttachedWorkerConnectionActivation{
		TenantID: currentWorker.TenantID, OwnerUserID: currentWorker.OwnerUserID, WorkerID: currentWorker.ID,
		ChallengeID: challenge.ID, Purpose: challenge.Purpose, ExpectedChallengeRevision: challenge.Revision,
		ExpectedWorkerRevision: currentWorker.Revision, ExpectedEnrollmentGeneration: currentWorker.EnrollmentGeneration,
		ExpectedConnectionGeneration: currentWorker.ConnectionGeneration,
		ExpectedConnectionID:         previous.ID, ExpectedConnectionRevision: previous.Revision,
		ExpectedPreviousCapabilityDigest: previous.CapabilityDigest,
		ExpectedPreviousProtocolSnapshot: append([]byte(nil), previous.ProtocolSnapshot...),
		PresentedWorkerNonceDigest:       challenge.WorkerNonceDigest, PresentedPlatformNonceDigest: challenge.PlatformNonceDigest,
		ConnectionSecretDigest: rotatedSecret, ChannelBinding: domain.NewAttachedWorkerChannelBinding(channel),
		ExpectedCapabilityDigest: previous.CapabilityDigest, ProtocolSnapshot: attachedSnapshot, AuthTTL: time.Hour,
	})
	if err != nil || activated.Status != ports.AttachedWorkerConnectionActivated ||
		activated.Connection.ID == previous.ID ||
		activated.Connection.ConnectionGeneration != previous.ConnectionGeneration+1 {
		t.Fatalf("activate receipt reconnect: result=%+v err=%v", activated, err)
	}
	currentWorker, found, err = aStore.LoadAttachedWorker(ctx, aWorker.TenantID, aWorker.OwnerUserID, aWorker.ID)
	if err != nil || !found {
		t.Fatalf("load receipt owner after activation: found=%t err=%v", found, err)
	}
	acceptedReconnect, err := aStore.AcceptAttachedWorkerManifest(ctx, ports.AttachedWorkerManifestAcceptance{
		TenantID: currentWorker.TenantID, OwnerUserID: currentWorker.OwnerUserID, WorkerID: currentWorker.ID,
		ConnectionID: activated.Connection.ID, ConnectionGeneration: activated.Connection.ConnectionGeneration,
		ExpectedConnectionRevision: activated.Connection.Revision, ExpectedWorkerRevision: currentWorker.Revision,
		PresentedSecretDigest: rotatedSecret,
		Capability: ports.AttachedWorkerCapabilityTarget{
			ManifestRevision: 1, Digest: previous.CapabilityDigest, ProtocolVersion: challenge.SelectedProtocolVersion,
			IdentityKeyDigest: domain.DigestAttachedWorkerIdentityKey(currentWorker.IdentityPublicKey),
			CanonicalManifest: aManifest, ManifestPayload: attachedWorkerManifestPayloadFixture(t, readySnapshot),
			Signature: manifestSignature,
		},
		PlatformSequence: 2, WorkerSequence: 3, PlatformAck: 2, WorkerAck: 2,
		ProtocolSnapshot: readySnapshot, PresenceTTL: 10 * time.Minute,
	})
	if err != nil || acceptedReconnect.Status != ports.AttachedWorkerConnectionAuthorized {
		t.Fatalf("authorize receipt reconnect: result=%+v err=%v", acceptedReconnect, err)
	}
	if stale, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); err != nil ||
		stale.Status == ports.AttachedWorkerExecutionApplied || stale.Status == ports.AttachedWorkerExecutionReplayed {
		t.Fatalf("old bearer recovered ready receipt: result=%+v err=%v", stale, err)
	}
	if stale, err := aStore.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		ConnectionID: a.request.ConnectionID, AttemptID: a.request.AttemptID,
		LeaseGeneration: a.request.LeaseGeneration, PresentedSecretDigest: a.request.PresentedSecretDigest,
		InboundFrame: terminal,
	}); err != nil || stale.Status != ports.AttachedWorkerExecutionDenied || stale.Outbound != nil {
		t.Fatalf("old terminal envelope was not denied after reconnect: result=%+v err=%v", stale, err)
	}
	rotated := request
	rotated.Authorization.ConnectionID = acceptedReconnect.Connection.ID
	rotated.Authorization.ConnectionGeneration = acceptedReconnect.Connection.ConnectionGeneration
	rotated.Authorization.PresentedSecretDigest = rotatedSecret
	rotated.Authorization.ExpectedAttemptRevision = 0
	recovered, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, rotated)
	if err != nil || recovered.Status != ports.AttachedWorkerExecutionReplayed ||
		recovered.Receipt.CanonicalDigest != created.Receipt.CanonicalDigest ||
		recovered.Receipt.Nonce != created.Receipt.Nonce ||
		!reflect.DeepEqual(recovered.Receipt.Materialization, created.Receipt.Materialization) {
		t.Fatalf("current bearer did not recover exact ready receipt: result=%+v err=%v", recovered, err)
	}
	aw07Authorized(t, bStore, ctx, b)
	commit := ports.AttachedWorkerTerminalCommit{
		TenantID: a.request.TenantID, OwnerUserID: a.request.OwnerUserID, WorkerID: a.request.WorkerID,
		AttemptID: a.request.AttemptID, LeaseGeneration: a.request.LeaseGeneration,
		Materialization: ports.AttachedWorkerTerminalMaterialization{EvidenceDigest: created.Receipt.CanonicalDigest},
	}
	withCallerOutput := commit
	withCallerOutput.Materialization.Completion = created.Receipt.Materialization.Completion
	if result, err := aStore.CommitAttachedWorkerTerminal(ctx, withCallerOutput); err == nil && result.Outbound != nil {
		t.Fatalf("receipt profile accepted caller output: result=%+v", result)
	}
	// A ready receipt commits the exact admitted provider resource and
	// credential generation, not just the fact that some credential is needed.
	// Model a changed durable job between receipt publication and TerminalAck.
	originalJob, err := json.Marshal(loaded.Job)
	if err != nil {
		t.Fatal(err)
	}
	writeJob := func(payload []byte) {
		t.Helper()
		if _, err := aClient.DB.ExecContext(ctx,
			`UPDATE worker_jobs SET payload=CAST($3 AS JsonDocument) WHERE tenant_id=$1 AND run_id=$2`,
			a.request.TenantID, a.request.RunID, string(payload)); err != nil {
			t.Fatalf("replace test-owned worker job: %v", err)
		}
	}
	defer writeJob(originalJob)
	for _, scenario := range []struct {
		name   string
		mutate func(*domain.WorkerJob)
	}{
		{"credential generation", func(job *domain.WorkerJob) { job.HarnessBinding.Resource.CredentialGeneration++ }},
		{"provider resource", func(job *domain.WorkerJob) { job.HarnessBinding.Resource.ResourceID += "-other" }},
		{"backend profile", func(job *domain.WorkerJob) {
			digest := job.HarnessBinding.Backend.BackendProfileDigest
			if digest[0] == 'a' {
				job.HarnessBinding.Backend.BackendProfileDigest = "b" + digest[1:]
			} else {
				job.HarnessBinding.Backend.BackendProfileDigest = "a" + digest[1:]
			}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			changedJob := loaded.Job
			scenario.mutate(&changedJob)
			if err := changedJob.HarnessBinding.ValidateForScope(changedJob.TenantID, changedJob.CredentialOwnerUserID,
				changedJob.RunID, changedJob.AttemptID, changedJob.ExecutionPlacementV2); err != nil {
				t.Fatalf("changed %s binding is invalid fixture: %v", scenario.name, err)
			}
			changedPayload, err := json.Marshal(changedJob)
			if err != nil {
				t.Fatal(err)
			}
			writeJob(changedPayload)
			if result, err := aStore.CommitAttachedWorkerTerminal(ctx, commit); err == nil && result.Outbound != nil {
				t.Fatalf("changed %s accepted receipt: result=%+v", scenario.name, result)
			}
			if status := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.request.RunID); status.Terminal() {
				t.Fatalf("changed %s finalized canonical run as %s", scenario.name, status)
			}
			writeJob(originalJob)
		})
	}
	setAW07MembershipStatus(t, aClient.DB, a.request.TenantID, a.request.OwnerUserID,
		domain.TenantMembershipSuspended, 4, aNow)
	if denied, err := aStore.CommitAttachedWorkerTerminal(ctx, commit); err != nil ||
		denied.Status != ports.AttachedWorkerExecutionFenced || denied.Outbound != nil {
		t.Fatalf("suspended owner finalized ready receipt: result=%+v err=%v", denied, err)
	}
	if status := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.request.RunID); status.Terminal() {
		t.Fatalf("suspended owner changed canonical run to %s", status)
	}
	aw07Authorized(t, bStore, ctx, b)
	setAW07MembershipStatus(t, aClient.DB, a.request.TenantID, a.request.OwnerUserID,
		domain.TenantMembershipActive, 5, aNow)
	committed, err := aStore.CommitAttachedWorkerTerminal(ctx, commit)
	if err != nil || committed.Status != ports.AttachedWorkerExecutionApplied || committed.Outbound == nil ||
		aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.request.RunID) != domain.RunSucceeded {
		t.Fatalf("receipt finalization: result=%+v err=%v", committed, err)
	}
	if replay, err := aStore.CommitAttachedWorkerTerminal(ctx, commit); err != nil || replay.Status != ports.AttachedWorkerExecutionReplayed || replay.Outbound == nil {
		t.Fatalf("terminal ACK replay: result=%+v err=%v", replay, err)
	}
	// Owner B has the same worker locator but a distinct ready receipt. A
	// revocation between Terminal evidence and canonical finalization must
	// strand B's receipt without ACK or product mutation, while A stays done.
	bCredentialRequired := peer.Job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
	bReceiptRequest := ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: b.request, Nonce: "receipt-nonce-b",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "owner B answer"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
			CredentialReleaseRequired: bCredentialRequired, CredentialReleased: bCredentialRequired,
		},
	}
	bReceipt, err := bStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, bReceiptRequest)
	if err != nil || bReceipt.Status != ports.AttachedWorkerExecutionApplied || !bReceipt.Receipt.Ready {
		t.Fatalf("owner B receipt before revocation: %+v err=%v", bReceipt, err)
	}
	bDigest, err := hex.DecodeString(string(bReceipt.Receipt.CanonicalDigest))
	if err != nil {
		t.Fatal(err)
	}
	bTerminal := attachedworkerprotocol.FrameV1{
		Version:   b.accepted.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, b.nextWorkerSequence),
		WorkerID:  string(b.request.WorkerID), EnrollmentGeneration: b.request.EnrollmentGeneration,
		ConnectionGeneration: b.request.ConnectionGeneration, Sequence: b.nextWorkerSequence,
		Ack: b.accepted.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{Binding: b.binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted,
			EvidenceDigest: bDigest},
	}
	bPending, err := bStore.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{
		TenantID: b.request.TenantID, OwnerUserID: b.request.OwnerUserID, WorkerID: b.request.WorkerID,
		ConnectionID: b.request.ConnectionID, AttemptID: b.request.AttemptID,
		LeaseGeneration: b.request.LeaseGeneration, PresentedSecretDigest: b.request.PresentedSecretDigest,
		InboundFrame: bTerminal,
	})
	if err != nil || bPending.Status != ports.AttachedWorkerExecutionApplied ||
		bPending.Attempt.State != domain.AttachedWorkerAttemptTerminalPending {
		t.Fatalf("owner B terminal was not pending before revocation: %+v err=%v", bPending, err)
	}
	var bEventsBefore int64
	if err := bClient.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_events WHERE tenant_id=$1 AND session_id=$2`,
		b.request.TenantID, peer.Job.SessionID,
	).Scan(&bEventsBefore); err != nil {
		t.Fatalf("count owner B canonical events before revocation: %v", err)
	}
	aw07Revoke(t, bStore, ctx, bWorker)
	bCommit := ports.AttachedWorkerTerminalCommit{
		TenantID: b.request.TenantID, OwnerUserID: b.request.OwnerUserID, WorkerID: b.request.WorkerID,
		AttemptID: b.request.AttemptID, LeaseGeneration: b.request.LeaseGeneration,
		Materialization: ports.AttachedWorkerTerminalMaterialization{EvidenceDigest: bReceipt.Receipt.CanonicalDigest},
	}
	if result, err := bStore.CommitAttachedWorkerTerminal(ctx, bCommit); err != nil ||
		result.Status != ports.AttachedWorkerExecutionFenced || result.Outbound != nil {
		t.Fatalf("revoked ready receipt produced ACK: %+v err=%v", result, err)
	}
	bAttempt, found, err := bStore.LoadAttachedWorkerAttempt(ctx, b.request.TenantID, b.request.OwnerUserID, b.request.WorkerID)
	if err != nil || !found || bAttempt.State == domain.AttachedWorkerAttemptTerminalCommitted {
		t.Fatalf("revoked owner B attempt committed Terminal: found=%t attempt=%+v err=%v", found, bAttempt, err)
	}
	var bACKCount int64
	if err := bClient.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM attached_worker_attempt_messages
		 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4
		 AND direction=$5 AND kind=$6`,
		b.request.TenantID, b.request.OwnerUserID, b.request.WorkerID, b.request.AttemptID,
		string(domain.AttachedWorkerAttemptPlatformToWorker), string(domain.AttachedWorkerAttemptMessageTerminalCommitted),
	).Scan(&bACKCount); err != nil || bACKCount != 0 {
		t.Fatalf("revoked owner B durable TerminalAck count=%d err=%v", bACKCount, err)
	}
	if got := aw07RunStatus(t, bStore, ctx, b.request.TenantID, b.request.RunID); got != domain.RunRunning {
		t.Fatalf("owner B revoked receipt mutated canonical run: %s", got)
	}
	var bEventsAfter int64
	if err := bClient.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM session_events WHERE tenant_id=$1 AND session_id=$2`,
		b.request.TenantID, peer.Job.SessionID,
	).Scan(&bEventsAfter); err != nil || bEventsAfter != bEventsBefore {
		t.Fatalf("revoked receipt changed owner B Session events: before=%d after=%d err=%v",
			bEventsBefore, bEventsAfter, err)
	}
	if got := aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.request.RunID); got != domain.RunSucceeded {
		t.Fatalf("owner B revoke changed owner A canonical run: %s", got)
	}
	if replay, err := aStore.CommitAttachedWorkerTerminal(ctx, commit); err != nil ||
		replay.Status != ports.AttachedWorkerExecutionReplayed || replay.Outbound == nil {
		t.Fatalf("owner A committed receipt was not replayable after B revoke: %+v err=%v", replay, err)
	}
}

func TestAW07AmbiguousReceiptCopyKeepsDeletionFailClosed(t *testing.T) {
	store, client, worker, connection, secret, _, _, now := readyAttachedWorkerForDrainWithIdentity(t,
		"receipt-copy-failure", "", "", attachedworkerprotocol.FeatureOutputReceipt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	claimed := aw07ClaimedInput(t, store, client, worker, connection, secret, now,
		attachedWorkerDrainTestSuffix(t, "receipt-copy-failure"))
	loaded, found, err := store.LoadWorkerJob(ctx, claimed.request.TenantID, claimed.request.RunID)
	if err != nil || !found {
		t.Fatalf("load failed-copy job: found=%t err=%v", found, err)
	}
	blobs := newSessionAPITestBlobs()
	late := &lateReceiptBlobs{BlobStore: blobs, release: make(chan struct{}), done: make(chan error, 1), started: make(chan struct{})}
	var releaseLateOnce sync.Once
	releaseLate := func() { releaseLateOnce.Do(func() { close(late.release) }) }
	lateJoined := false
	defer func() {
		releaseLate()
		if lateJoined {
			return
		}
		select {
		case <-late.started:
		default:
			return // No remote writer was acquired.
		}
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var putErr error
		select {
		case putErr = <-late.done:
		case <-cleanup.Done():
			select {
			case putErr = <-late.done:
			default:
				t.Error("late remote receipt writer did not join during bounded cleanup")
				return
			}
		}
		if putErr != nil {
			t.Errorf("late remote receipt writer failed during cleanup: %v", putErr)
		}
	}()
	request := ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: claimed.request, Nonce: "receipt-copy-failure",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "copy failed"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
		},
	}
	if _, err := store.CreateAttachedWorkerOutputReceipt(ctx, late, request); err == nil {
		t.Fatal("lost remote-write response unexpectedly produced a ready receipt")
	}
	var payload string
	if err := client.DB.QueryRowContext(ctx,
		`SELECT payload FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
		claimed.request.TenantID, claimed.request.RunID, claimed.request.OwnerUserID, claimed.request.WorkerID,
		claimed.request.AttemptID, claimed.request.LeaseGeneration,
	).Scan(&payload); err != nil {
		t.Fatalf("read pending failed-copy receipt: %v", err)
	}
	var pending ydbstore.AttachedWorkerOutputReceiptV1
	if err := json.Unmarshal([]byte(payload), &pending); err != nil || pending.Ready || !pending.CopyInProgress ||
		pending.Materialization.Completion == nil {
		t.Fatalf("ambiguous write did not retain the pending copy barrier: %+v err=%v", pending, err)
	}
	// Revocation deterministically fences the same retry path as an expired
	// lease, without making this test depend on the database wall clock.
	aw07Revoke(t, store, ctx, worker)
	if result, err := store.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); err != nil ||
		result.Status != ports.AttachedWorkerExecutionFenced {
		t.Fatalf("revoked exact retry was not fenced: result=%+v err=%v", result, err)
	}
	deletion := domain.SessionDeletion{
		TenantID: claimed.request.TenantID, SessionID: loaded.Job.SessionID,
		RequestedBy: claimed.request.OwnerUserID, Reason: "failed receipt copy after revocation",
		State: domain.SessionDeletionRequested, RequestedAt: now.Add(time.Second),
	}
	if err := aw07PendingReceiptDeletionBarrier(ctx, store, client.DB, claimed.request.RunID, domain.RunFailed, deletion); err != nil {
		t.Fatalf("deletion escaped an ambiguous remote write: %v", err)
	}
	releaseLate()
	var lateErr error
	select {
	case lateErr = <-late.done:
	case <-ctx.Done():
		select {
		case lateErr = <-late.done:
		default:
			t.Fatalf("late remote write did not settle: %v", ctx.Err())
		}
	}
	lateJoined = true
	if lateErr != nil {
		t.Fatalf("late remote write failed: %v", lateErr)
	}
	if _, copied := blobs.values[pending.Materialization.Completion.Events[0].Payload.Key]; !copied {
		t.Fatal("fixture did not complete the remote write after the local failure")
	}
	if err := aw07PendingReceiptDeletionBarrier(ctx, store, client.DB, claimed.request.RunID, domain.RunFailed, deletion); err != nil {
		t.Fatalf("late write released the deletion barrier without a quiescence proof: %v", err)
	}
}
