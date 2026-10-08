//go:build ydbintegration && (darwin || linux)

package ydbintegration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type aw07HTTPStatusRecorder struct {
	mu       sync.Mutex
	statuses []string
}

// Hold both owner submissions before YDB can create a receipt. This proves
// that neither activated daemon can finalize merely from process success.
type aw07ReceiptBarrier struct {
	store    *ydbstore.Store
	arrivals chan domain.UserID
	release  chan struct{}
}

func (barrier *aw07ReceiptBarrier) CreateAttachedWorkerOutputReceipt(ctx context.Context, blobs ports.BlobStore,
	request ydbstore.AttachedWorkerOutputReceiptRequest,
) (ydbstore.AttachedWorkerOutputReceiptResult, error) {
	select {
	case barrier.arrivals <- request.Authorization.OwnerUserID:
	case <-ctx.Done():
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ctx.Err()
	}
	select {
	case <-barrier.release:
	case <-ctx.Done():
		return ydbstore.AttachedWorkerOutputReceiptResult{}, ctx.Err()
	}
	return barrier.store.CreateAttachedWorkerOutputReceipt(ctx, blobs, request)
}

type aw07StatusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *aw07StatusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (recorder *aw07HTTPStatusRecorder) wrap(path string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		observed := &aw07StatusWriter{ResponseWriter: writer, status: http.StatusOK}
		handler.ServeHTTP(observed, request)
		recorder.mu.Lock()
		recorder.statuses = append(recorder.statuses, fmt.Sprintf("%s:%d:%s", path, observed.status,
			time.Since(started).Round(time.Millisecond)))
		recorder.mu.Unlock()
	})
}

func (recorder *aw07HTTPStatusRecorder) snapshot() string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return strings.Join(recorder.statuses, ",")
}

// The serving adapter deliberately hides backend details from the worker. Keep
// them in this test-only recorder so a sanitized 503 still identifies the YDB
// operation that failed without changing the production response contract.
type aw07BackendRecorder struct {
	*ydbstore.Store
	probeDB     *sql.DB
	probeCtx    context.Context
	probeOnce   sync.Once
	probeMu     sync.Mutex
	probeClosed bool
	probeWG     sync.WaitGroup
	mu          sync.Mutex
	failures    []string
	probes      []string
	probePhase  string
	probeAt     time.Time
	watchdogs   uint64
}

func (recorder *aw07BackendRecorder) record(operation string, err error) {
	if err == nil {
		return
	}
	recorder.mu.Lock()
	recorder.failures = append(recorder.failures, fmt.Sprintf("%s: %v", operation, err))
	recorder.mu.Unlock()
}

func (recorder *aw07BackendRecorder) recordSlow(operation string, started time.Time, ctx context.Context) {
	duration := time.Since(started)
	if duration < 100*time.Millisecond {
		return
	}
	recorder.mu.Lock()
	recorder.probes = append(recorder.probes, fmt.Sprintf("slow %s duration=%s context-at-end=%v",
		operation, duration.Round(time.Millisecond), ctx.Err()))
	recorder.mu.Unlock()
}

func (recorder *aw07BackendRecorder) snapshot() string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	parts := append(append([]string(nil), recorder.failures...), recorder.probes...)
	if recorder.watchdogs > 0 {
		stats := recorder.probeDB.Stats()
		parts = append(parts, fmt.Sprintf("watchdogs-fired=%d probe-phase=%s probe-age=%s pool-open=%d pool-in-use=%d pool-waits=%d",
			recorder.watchdogs, recorder.probePhase, time.Since(recorder.probeAt).Round(time.Millisecond),
			stats.OpenConnections, stats.InUse, stats.WaitCount))
	}
	return strings.Join(parts, "; ")
}

func (recorder *aw07BackendRecorder) markProbePhase(phase string) {
	recorder.mu.Lock()
	recorder.probePhase = phase
	recorder.mu.Unlock()
}

// A single independent point-read probe distinguishes a stalled interactive
// authorization transaction from an unavailable YDB Table service. It never
// changes the authorization result and records no row, bearer, or payload.
func (recorder *aw07BackendRecorder) probeExchangeRead(request ports.AttachedWorkerExchangeAuthorization) {
	started := time.Now()
	recorder.markProbePhase("connection-read")
	ctx, cancel := context.WithTimeout(recorder.probeCtx, 2*time.Second)
	defer cancel()
	readCtx := ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl())
	stage := "connection"
	var raw string
	err := recorder.probeDB.QueryRowContext(readCtx,
		`SELECT record FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`,
		request.TenantID, request.OwnerUserID, request.WorkerID,
	).Scan(&raw)
	if err == nil {
		var connection domain.AttachedWorkerConnection
		err = json.Unmarshal([]byte(raw), &connection)
		if err == nil {
			stage = "manifest"
			recorder.markProbePhase("manifest-read")
			err = recorder.probeDB.QueryRowContext(readCtx,
				`SELECT record FROM attached_worker_capability_manifests
				 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND capability_digest=$4`,
				request.TenantID, request.OwnerUserID, request.WorkerID, connection.CapabilityDigest,
			).Scan(&raw)
		}
	}
	stats := recorder.probeDB.Stats()
	recorder.mu.Lock()
	recorder.probes = append(recorder.probes,
		fmt.Sprintf("independent online-RO probe stage=%s duration=%s error-type=%T deadline=%t pool-open=%d pool-in-use=%d pool-waits=%d",
			stage, time.Since(started).Round(time.Millisecond), err, errors.Is(err, context.DeadlineExceeded),
			stats.OpenConnections, stats.InUse, stats.WaitCount))
	recorder.probePhase = "completed"
	recorder.mu.Unlock()
}

func (recorder *aw07BackendRecorder) LoadAttachedWorkerConnection(ctx context.Context, tenant domain.TenantID,
	owner domain.UserID, worker domain.AttachedWorkerID,
) (domain.AttachedWorkerConnection, bool, error) {
	started := time.Now()
	connection, found, err := recorder.Store.LoadAttachedWorkerConnection(ctx, tenant, owner, worker)
	recorder.recordSlow("load connection", started, ctx)
	recorder.record("load connection", err)
	return connection, found, err
}

func (recorder *aw07BackendRecorder) LoadAttachedWorker(ctx context.Context, tenant domain.TenantID,
	owner domain.UserID, worker domain.AttachedWorkerID,
) (domain.AttachedWorker, bool, error) {
	started := time.Now()
	value, found, err := recorder.Store.LoadAttachedWorker(ctx, tenant, owner, worker)
	recorder.recordSlow("load worker", started, ctx)
	recorder.record("load worker", err)
	return value, found, err
}

func (recorder *aw07BackendRecorder) AuthorizeAttachedWorkerExchange(ctx context.Context,
	request ports.AttachedWorkerExchangeAuthorization,
) (ports.AttachedWorkerAuthorizationResult, error) {
	started := time.Now()
	startContextErr := ctx.Err()
	startDeadline := "none"
	if deadline, ok := ctx.Deadline(); ok {
		startDeadline = time.Until(deadline).Round(time.Millisecond).String()
	}
	finished := make(chan struct{})
	recorder.probeMu.Lock()
	if !recorder.probeClosed {
		recorder.probeWG.Add(1)
		go func() {
			defer recorder.probeWG.Done()
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-finished:
				return
			case <-timer.C:
			}
			select {
			case <-finished:
				return
			default:
			}
			recorder.mu.Lock()
			recorder.watchdogs++
			if recorder.watchdogs == 1 {
				recorder.probeAt = time.Now()
				recorder.probePhase = "scheduled"
			}
			recorder.mu.Unlock()
			recorder.probeOnce.Do(func() { recorder.probeExchangeRead(request) })
		}()
	}
	recorder.probeMu.Unlock()
	result, err := recorder.Store.AuthorizeAttachedWorkerExchange(ctx, request)
	close(finished)
	recorder.record(fmt.Sprintf("authorize exchange duration=%s context-at-start=%v context-at-end=%v deadline-remaining-at-start=%s",
		time.Since(started).Round(time.Millisecond), startContextErr, ctx.Err(), startDeadline), err)
	return result, err
}

func (recorder *aw07BackendRecorder) PollAttachedWorkerControl(ctx context.Context,
	request ports.AttachedWorkerControlPoll,
) (ports.AttachedWorkerDrainResult, error) {
	result, err := recorder.Store.PollAttachedWorkerControl(ctx, request)
	recorder.record("poll control", err)
	return result, err
}

func (recorder *aw07BackendRecorder) PollAttachedWorkerAttempt(ctx context.Context,
	request ports.AttachedWorkerAttemptPoll,
) (ports.AttachedWorkerAttemptResult, error) {
	result, err := recorder.Store.PollAttachedWorkerAttempt(ctx, request)
	recorder.record("poll attempt", err)
	return result, err
}

func (recorder *aw07BackendRecorder) ExchangeAttachedWorkerAttempt(ctx context.Context,
	request ports.AttachedWorkerAttemptExchange,
) (ports.AttachedWorkerAttemptResult, error) {
	result, err := recorder.Store.ExchangeAttachedWorkerAttempt(ctx, request)
	recorder.record("exchange attempt", err)
	return result, err
}

func (recorder *aw07BackendRecorder) CommitAttachedWorkerTerminal(ctx context.Context,
	request ports.AttachedWorkerTerminalCommit,
) (ports.AttachedWorkerAttemptResult, error) {
	result, err := recorder.Store.CommitAttachedWorkerTerminal(ctx, request)
	recorder.record("commit terminal", err)
	if err == nil && result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed {
		recorder.record("commit terminal", fmt.Errorf("status=%s", result.Status))
	}
	return result, err
}

// Poll the canonical row without starting a second serializable transaction
// while two activated daemons are exchanging and committing their own work.
func aw07JoinedRunStatus(t *testing.T, db *sql.DB, ctx context.Context,
	tenant domain.TenantID, runID domain.RunID,
) domain.RunStatus {
	t.Helper()
	readCtx := ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl())
	var payload string
	if err := db.QueryRowContext(readCtx,
		`SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2`, tenant, runID,
	).Scan(&payload); err != nil {
		t.Fatalf("read canonical run %s for tenant %s: %v", runID, tenant, err)
	}
	var run domain.Run
	if err := json.Unmarshal([]byte(payload), &run); err != nil {
		t.Fatalf("decode canonical run %s for tenant %s: %v", runID, tenant, err)
	}
	if run.ID != runID || run.TenantID != tenant {
		t.Fatalf("canonical run identity: got %s/%s, want %s/%s", run.TenantID, run.ID, tenant, runID)
	}
	return run.Status
}

// A real activated daemon and HTTPS exchange must take two independently
// authorized provider-shaped turns through credential issue/release, canonical
// receipt publication, and TerminalAck. The provider and OCI engine are test
// doubles; both are compiled only into this YDB gate.
func TestAW07TwoActivatedProviderDaemonReceipts(t *testing.T) {
	store, client := openStore(t)
	var now time.Time
	clockCtx, clockCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer clockCancel()
	if err := client.DB.QueryRowContext(clockCtx, `SELECT CurrentUtcTimestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	now = now.UTC().Truncate(time.Microsecond)
	// The subprocess advances its injected clock by sixteen minutes on RUN.
	// Keep its test-only credential gate on that same YDB-derived timeline;
	// sampling before the lease is claimed would otherwise reject a future
	// lease start even though the authoritative YDB transaction is healthy.
	clock := func() time.Time { return now.Add(16 * time.Minute) }
	suffix := attachedWorkerDrainTestSuffix(t, "aw07-joined-provider")
	tenant := domain.TenantID(uniqueID("tenant-" + suffix))
	workerID := domain.AttachedWorkerID(uniqueID("worker-" + suffix))
	aWorker, aPrivate := aw07CreateDaemonEnrollment(t, store, tenant, domain.UserID(uniqueID("owner-a-"+suffix)), workerID, suffix+"-a", now)
	bWorker, bPrivate := aw07CreateDaemonEnrollment(t, store, tenant, domain.UserID(uniqueID("owner-b-"+suffix)), workerID, suffix+"-b", now)
	blobs := &aw07ArtifactBlobs{objects: make(map[string][]byte), opens: make(map[string]int)}
	probeCtx, cancelProbe := context.WithCancel(context.Background())
	backend := &aw07BackendRecorder{Store: store, probeDB: client.DB, probeCtx: probeCtx}
	t.Cleanup(func() {
		backend.probeMu.Lock()
		backend.probeClosed = true
		backend.probeMu.Unlock()
		cancelProbe()
		finished := make(chan struct{})
		go func() {
			backend.probeWG.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("joined YDB diagnostic probe did not stop after cancellation")
		}
	})
	service, err := attachedworkertransport.NewTestReceiptFinalizingService(attachedworkertransport.ServiceConfig{
		IDs: testkit.NewSequenceIDGenerator("aw07-provider-"), Audience: "sessionless:attached-worker:v1",
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, backend, backend)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := attachedworkerhttp.NewBootstrapHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := attachedworkerhttp.NewCoreExchangeAdapter(service)
	if err != nil {
		t.Fatal(err)
	}
	exchange, err := attachedworkerhttp.NewHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := attachedworkersealedinput.NewTestProviderService(service, store, blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	receiptBarrier := &aw07ReceiptBarrier{store: store, arrivals: make(chan domain.UserID, 4), release: make(chan struct{})}
	var releaseReceipts sync.Once
	release := func() { releaseReceipts.Do(func() { close(receiptBarrier.release) }) }
	defer release()
	receipts, err := attachedworkerreceipt.NewService(service, receiptBarrier, blobs)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	statusRecorder := &aw07HTTPStatusRecorder{}
	mux.Handle(attachedworkerhttp.ChallengePathV1, bootstrap)
	mux.Handle(attachedworkerhttp.AttachPathV1, bootstrap)
	mux.Handle(attachedworkerhttp.ExchangePathV1, statusRecorder.wrap("exchange", exchange))
	mux.Handle(attachedworkersealedinput.PathV1, statusRecorder.wrap("sealed", attachedworkersealedinput.Handler(sealed)))
	mux.Handle(attachedworkerreceipt.PathV1, statusRecorder.wrap("receipt", attachedworkerreceipt.Handler(receipts)))
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	trust := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	root := t.TempDir()
	a := aw07PrepareDaemonInstallation(t, filepath.Join(root, "a"), server.URL, trust, aWorker, aPrivate, now, true)
	b := aw07PrepareDaemonInstallation(t, filepath.Join(root, "b"), server.URL, trust, bWorker, bPrivate, now, true)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	aProcess := aw07StartDaemonProcess(t, aw07DaemonChildInput{StateRoot: a.stateRoot, ProfilePath: a.profilePath,
		ClockNanos: now.UnixNano(), TestProvider: true, ProviderResource: "subscription-" + suffix + "-a", ProviderGeneration: 3})
	bProcess := aw07StartDaemonProcess(t, aw07DaemonChildInput{StateRoot: b.stateRoot, ProfilePath: b.profilePath,
		ClockNanos: now.UnixNano(), TestProvider: true, ProviderResource: "subscription-" + suffix + "-b", ProviderGeneration: 7})
	for _, process := range []*aw07DaemonProcess{aProcess, bProcess} {
		process.await(t, ctx, "AW07_DAEMON_CONNECTED")
	}
	owners := []struct {
		name       string
		worker     domain.AttachedWorker
		install    aw07DaemonInstallation
		process    *aw07DaemonProcess
		generation uint64
		context    []byte
		artifact   []byte
		suffix     string
		offer      ports.AttachedWorkerAttemptResult
	}{
		{name: "a", worker: aWorker, install: a, process: aProcess, generation: 3,
			context: []byte("owner A canonical context"), artifact: []byte("owner A private artifact"), suffix: suffix + "-a"},
		{name: "b", worker: bWorker, install: b, process: bProcess, generation: 7,
			context: []byte("owner B canonical context"), artifact: []byte("owner B private artifact"), suffix: suffix + "-b"},
	}
	for index := range owners {
		owner := &owners[index]
		connection, found, err := store.LoadAttachedWorkerConnection(ctx, tenant, owner.worker.OwnerUserID, workerID)
		if err != nil || !found {
			t.Fatalf("%s active connection: found=%t err=%v", owner.name, found, err)
		}
		binding := aw07TestProviderBinding(owner.worker.OwnerUserID, owner.suffix, owner.generation, now)
		owner.offer = attachedWorkerOfferForDrainWithPayloadAndBinding(t, store, client, owner.worker, connection, now,
			owner.suffix, owner.context, owner.artifact, func(value *domain.HarnessBindingV1) {
				binding(value)
				value.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryFileV1
			}, 20*time.Minute)
		job, found, err := store.LoadWorkerJob(ctx, tenant, owner.offer.Attempt.RunID)
		if err != nil || !found || len(job.InputManifest.Artifacts) != 1 {
			t.Fatalf("%s worker job: found=%t err=%v", owner.name, found, err)
		}
		seedCanonicalMembership(t, client.DB, tenant, owner.worker.OwnerUserID, now)
		blobs.mu.Lock()
		blobs.objects[job.Job.ContextSnapshot.Key] = owner.context
		blobs.objects[job.InputManifest.Artifacts[0].Blob.Key] = owner.artifact
		blobs.mu.Unlock()
	}
	for index := range owners {
		owners[index].process.commandLine(t, "RUN")
		owners[index].process.await(t, ctx, "AW07_DAEMON_RUNNING")
	}
	seen := make(map[domain.UserID]bool, len(owners))
	for len(seen) < len(owners) {
		select {
		case owner := <-receiptBarrier.arrivals:
			seen[owner] = true
		case <-ctx.Done():
			t.Fatalf("both owners did not reach receipt publication: seen=%v HTTP=%s backend=%s: %v",
				seen, statusRecorder.snapshot(), backend.snapshot(), ctx.Err())
		}
	}
	for index := range owners {
		owner := &owners[index]
		if !seen[owner.worker.OwnerUserID] {
			t.Fatalf("%s did not reach receipt publication: seen=%v", owner.name, seen)
		}
		if status := aw07JoinedRunStatus(t, client.DB, ctx, tenant, owner.offer.Attempt.RunID); status == domain.RunSucceeded {
			t.Fatalf("%s finalized before its receipt was stored", owner.name)
		}
		attempt, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, owner.worker.OwnerUserID, workerID)
		if err != nil || !found || attempt.State == domain.AttachedWorkerAttemptTerminalCommitted {
			t.Fatalf("%s terminal ACK before receipt: found=%t attempt=%+v err=%v", owner.name, found, attempt, err)
		}
	}
	release()
	for index := range owners {
		owner := &owners[index]
		deadline := time.Now().Add(90 * time.Second)
		for {
			status := aw07JoinedRunStatus(t, client.DB, ctx, tenant, owner.offer.Attempt.RunID)
			if status == domain.RunSucceeded {
				break
			}
			if status.Terminal() || strings.Contains(owner.process.stdout.String(), "AW07_DAEMON_RUN_EXIT=") &&
				strings.Contains(owner.process.stdout.String(), "materialization failed") || time.Now().After(deadline) {
				lifecycle, _ := os.ReadFile(filepath.Join(filepath.Dir(owner.install.stateRoot), "credentials", "lifecycle.log"))
				t.Fatalf("%s did not commit successful canonical receipt: status=%s stdout=%s stderr=%s HTTP=%s backend=%s blob-opens=%d credential-lifecycle=%q OCI=%s",
					owner.name, status, owner.process.stdout.String(), owner.process.stderr.String(),
					statusRecorder.snapshot(), backend.snapshot(), blobs.totalOpens(), lifecycle, aw07ReadOCICommands(t, owner.install.commandLog))
			}
			time.Sleep(250 * time.Millisecond)
		}
		attempt, found, err := store.LoadAttachedWorkerAttempt(ctx, tenant, owner.worker.OwnerUserID, workerID)
		if err != nil || !found || attempt.State != domain.AttachedWorkerAttemptTerminalCommitted {
			t.Fatalf("%s terminal ACK not committed: found=%t state=%s err=%v", owner.name, found, attempt.State, err)
		}
		var receiptPayload string
		if err := client.DB.QueryRowContext(ctx,
			`SELECT payload FROM attached_worker_output_receipts WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
			tenant, attempt.RunID, owner.worker.OwnerUserID, workerID, attempt.AttemptID, attempt.LeaseGeneration,
		).Scan(&receiptPayload); err != nil {
			t.Fatalf("%s receipt not durably published: %v", owner.name, err)
		}
		var receipt ydbstore.AttachedWorkerOutputReceiptV1
		if err := json.Unmarshal([]byte(receiptPayload), &receipt); err != nil || !receipt.Ready ||
			receipt.Status != domain.AttachedWorkerTerminalSucceeded || !receipt.CredentialRequired ||
			!receipt.Observation.CredentialReleased || receipt.Materialization.Completion == nil ||
			len(receipt.Materialization.Completion.Events) != 1 ||
			receipt.Materialization.Completion.Events[0].DisplayText != "test provider result for "+string(owner.worker.OwnerUserID) ||
			receipt.Binding.OwnerUserID != owner.worker.OwnerUserID ||
			receipt.Binding.RunID != owner.offer.Attempt.RunID {
			t.Fatalf("%s receipt not owner-bound and ready: receipt=%+v err=%v", owner.name, receipt, err)
		}
		payloadRef := receipt.Materialization.Completion.Events[0].Payload
		blobs.mu.Lock()
		payload := append([]byte(nil), blobs.objects[payloadRef.Key]...)
		blobs.mu.Unlock()
		if !bytes.Contains(payload, []byte(string(owner.worker.OwnerUserID))) ||
			bytes.Contains(payload, []byte(string(owners[1-index].worker.OwnerUserID))) {
			t.Fatalf("%s canonical result crossed owner boundary: %q", owner.name, payload)
		}
		logPath := filepath.Join(filepath.Dir(owner.install.stateRoot), "credentials", "lifecycle.log")
		log, err := os.ReadFile(logPath)
		if err != nil || string(log) != "issue\nmaterialize\nwriteback\nrelease\n" {
			t.Fatalf("%s credential lifecycle = %q, err=%v", owner.name, log, err)
		}
		credentialsRoot := filepath.Join(filepath.Dir(owner.install.stateRoot), "credentials")
		entries, err := os.ReadDir(credentialsRoot)
		if err != nil || len(entries) != 1 || entries[0].Name() != "lifecycle.log" {
			t.Fatalf("%s credential files remain: %v, err=%v", owner.name, entries, err)
		}
		commands := aw07ReadOCICommands(t, owner.install.commandLog)
		if !strings.Contains(commands, "container start --attach --interactive") ||
			!strings.Contains(commands, "--network none") || !strings.Contains(commands, "SESSIONLESS_PROVIDER_HOME") {
			t.Fatalf("%s provider boundary was not launched: %s", owner.name, commands)
		}
	}
	for index := range owners {
		owner := &owners[index]
		owner.process.commandLine(t, "STOP")
		owner.process.await(t, ctx, "AW07_DAEMON_STOPPED")
		for _, path := range []string{owner.install.materializationRoot, owner.install.scratchRoot} {
			entries, err := os.ReadDir(path)
			if err != nil || len(entries) != 0 {
				t.Fatalf("%s attempt root not cleaned: path=%s entries=%v err=%v", owner.name, path, entries, err)
			}
		}
		body, err := os.ReadFile(owner.install.sentinel)
		if err != nil || !bytes.Equal(body, owner.install.sentinelBody) {
			t.Fatalf("%s external sentinel changed: %q, err=%v", owner.name, body, err)
		}
	}
	if owners[0].worker.OwnerUserID == owners[1].worker.OwnerUserID || owners[0].worker.ID != owners[1].worker.ID {
		t.Fatal("joined gate did not exercise colliding worker IDs under two owners")
	}
}
