//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
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
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

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
	release chan struct{}
	done    chan error
}

func (blobs *lateReceiptBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	content, err := io.ReadAll(body)
	if err != nil {
		return domain.BlobRef{}, err
	}
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
func aw07TestProviderBinding(owner domain.UserID, suffix string, generation uint64, now time.Time) func(*domain.HarnessBindingV1) {
	return func(binding *domain.HarnessBindingV1) {
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
	aStore, aClient, aWorker, aConnection, aSecret, _, _, aNow := readyAttachedWorkerForDrainWithIdentity(t,
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
	seedCanonicalMembership(t, aClient.DB, a.request.TenantID, a.request.OwnerUserID, aNow)
	credentialRequired := loaded.Job.HarnessBinding.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1
	request := ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: a.request, Nonce: "receipt-nonce-a",
		Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "owner A answer"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
			CredentialReleaseRequired: credentialRequired, CredentialReleased: credentialRequired,
		},
	}
	withoutCredentialRelease := request
	withoutCredentialRelease.Observation.CredentialReleased = false
	if result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, withoutCredentialRelease); err != nil ||
		result.Status != ports.AttachedWorkerExecutionFenced || result.Receipt.Version != 0 {
		t.Fatalf("provider receipt without credential release was not fenced: result=%+v err=%v", result, err)
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
	defer func() {
		select {
		case <-blocked.release:
		default:
			close(blocked.release)
		}
	}()
	type copyOutcome struct {
		result ydbstore.AttachedWorkerOutputReceiptResult
		err    error
	}
	copyDone := make(chan copyOutcome, 1)
	go func() {
		result, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blocked, request)
		copyDone <- copyOutcome{result: result, err: err}
	}()
	select {
	case <-blocked.entered:
	case <-ctx.Done():
		close(blocked.release)
		t.Fatalf("canonical copy never started: %v", ctx.Err())
	}
	if _, err := aStore.CreateAttachedWorkerOutputReceipt(ctx, blobs, request); !errors.Is(err, ydbstore.ErrAttachedWorkerReceiptCopyInProgress) {
		close(blocked.release)
		t.Fatalf("concurrent exact retry was allowed to copy: %v", err)
	}
	if _, copied := blobs.values[pendingReceipt.Materialization.Completion.Events[0].Payload.Key]; copied {
		close(blocked.release)
		t.Fatal("duplicate request copied canonical object before the active writer settled")
	}
	// Model a run that has just become terminal while the non-transactional
	// copy is still blocked. Deletion must inspect the pending receipt, not
	// merely rely on the normal active-run guard. Restore the test fixture's
	// run index before allowing the receipt writer to finish.
	if _, err := aClient.DB.ExecContext(ctx,
		`UPDATE runs_by_session SET status=$1 WHERE tenant_id=$2 AND session_id=$3 AND run_id=$4`,
		domain.RunSucceeded, a.request.TenantID, loaded.Job.SessionID, a.request.RunID,
	); err != nil {
		close(blocked.release)
		t.Fatalf("mark run terminal for deletion race: %v", err)
	}
	deletion := domain.SessionDeletion{
		TenantID: a.request.TenantID, SessionID: loaded.Job.SessionID,
		RequestedBy: a.request.OwnerUserID, Reason: "receipt copy race",
		State: domain.SessionDeletionRequested, RequestedAt: aNow.Add(time.Second),
	}
	if _, err := aStore.RequestSessionDeletion(ctx, deletion); err == nil ||
		!strings.Contains(err.Error(), "pending attached-worker output receipt") {
		close(blocked.release)
		t.Fatalf("deletion escaped the in-flight receipt barrier: %v", err)
	}
	if _, err := aClient.DB.ExecContext(ctx,
		`UPDATE runs_by_session SET status=$1 WHERE tenant_id=$2 AND session_id=$3 AND run_id=$4`,
		domain.RunRunning, a.request.TenantID, loaded.Job.SessionID, a.request.RunID,
	); err != nil {
		close(blocked.release)
		t.Fatalf("restore active run after deletion race: %v", err)
	}
	close(blocked.release)
	select {
	case outcome := <-copyDone:
		if outcome.err != nil || outcome.result.Status != ports.AttachedWorkerExecutionApplied || !outcome.result.Receipt.Ready {
			t.Fatalf("single copy writer did not commit ready receipt: %+v err=%v", outcome.result, outcome.err)
		}
	case <-ctx.Done():
		t.Fatalf("single copy writer did not settle: %v", ctx.Err())
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
	committed, err := aStore.CommitAttachedWorkerTerminal(ctx, commit)
	if err != nil || committed.Status != ports.AttachedWorkerExecutionApplied || committed.Outbound == nil ||
		aw07RunStatus(t, aStore, ctx, a.request.TenantID, a.request.RunID) != domain.RunSucceeded {
		t.Fatalf("receipt finalization: result=%+v err=%v", committed, err)
	}
	if replay, err := aStore.CommitAttachedWorkerTerminal(ctx, commit); err != nil || replay.Status != ports.AttachedWorkerExecutionReplayed || replay.Outbound == nil {
		t.Fatalf("terminal ACK replay: result=%+v err=%v", replay, err)
	}
	if got := aw07RunStatus(t, bStore, ctx, b.request.TenantID, b.request.RunID); got != domain.RunRunning {
		t.Fatalf("owner B run mutated by owner A receipt: %s", got)
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
	seedCanonicalMembership(t, client.DB, claimed.request.TenantID, claimed.request.OwnerUserID, now)
	blobs := newSessionAPITestBlobs()
	late := &lateReceiptBlobs{BlobStore: blobs, release: make(chan struct{}), done: make(chan error, 1)}
	defer func() {
		select {
		case <-late.release:
		default:
			close(late.release)
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
	if _, err := client.DB.ExecContext(ctx,
		`UPDATE runs_by_session SET status=$1 WHERE tenant_id=$2 AND session_id=$3 AND run_id=$4`,
		domain.RunFailed, claimed.request.TenantID, loaded.Job.SessionID, claimed.request.RunID,
	); err != nil {
		t.Fatalf("mark abandoned run terminal for deletion: %v", err)
	}
	deletion := domain.SessionDeletion{
		TenantID: claimed.request.TenantID, SessionID: loaded.Job.SessionID,
		RequestedBy: claimed.request.OwnerUserID, Reason: "failed receipt copy after revocation",
		State: domain.SessionDeletionRequested, RequestedAt: now.Add(time.Second),
	}
	if _, err := store.RequestSessionDeletion(ctx, deletion); err == nil ||
		!strings.Contains(err.Error(), "pending attached-worker output receipt") {
		t.Fatalf("deletion escaped an ambiguous remote write: %v", err)
	}
	close(late.release)
	select {
	case err := <-late.done:
		if err != nil {
			t.Fatalf("late remote write failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("late remote write did not settle: %v", ctx.Err())
	}
	if _, copied := blobs.values[pending.Materialization.Completion.Events[0].Payload.Key]; !copied {
		t.Fatal("fixture did not complete the remote write after the local failure")
	}
	if _, err := store.RequestSessionDeletion(ctx, deletion); err == nil ||
		!strings.Contains(err.Error(), "pending attached-worker output receipt") {
		t.Fatalf("late write released the deletion barrier without a quiescence proof: %v", err)
	}
}
