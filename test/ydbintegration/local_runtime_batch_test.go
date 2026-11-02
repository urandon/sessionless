//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
	"gitcode.com/urandon/sessionless/internal/yandextriggers"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const localRuntimeWebOrigin = "https://runtime-web.example.invalid"

// Return without consulting any other environment variable when disabled.
// This is a test-run guard, never product/provider authorization.
func localRuntimeYDBConnection(getenv func(string) string) (string, bool, error) {
	if getenv("SESSIONLESS_LOCAL_RUNTIME_YDB_GATE") != "1" {
		return "", false, nil
	}
	if getenv("GITHUB_ACTIONS") != "true" || getenv("SESSIONLESS_LOCAL_RUNTIME_YDB_DISPOSABLE") != "1" {
		return "", true, errors.New("local runtime proof requires ordinary disposable GitHub CI")
	}
	dsn := getenv("YDB_CONNECTION_STRING")
	u, err := url.Parse(dsn)
	if err != nil || u == nil || u.User != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		return "", true, errors.New("local runtime proof requires the disposable loopback database")
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(query["go_query_mode"]) != 1 || query.Get("go_query_mode") != "data" || query.Has("go_fake_tx") {
		return "", true, errors.New("local runtime proof requires true data transactions, never scripting/fake transactions")
	}
	return dsn, true, nil
}

func TestLocalRuntimeYDBGuardBeforeEnvironmentAndTrueTransactions(t *testing.T) {
	if dsn, enabled, err := localRuntimeYDBConnection(func(key string) string {
		if key != "SESSIONLESS_LOCAL_RUNTIME_YDB_GATE" {
			t.Fatalf("disabled guard read %s before returning", key)
		}
		return ""
	}); dsn != "" || enabled || err != nil {
		t.Fatal("disabled gate consulted operator environment", enabled, err)
	}
	const good = "grpc://127.0.0.1:2136/local?go_query_mode=data&go_query_bind=declare,numeric"
	for _, check := range []struct {
		name, connection, actions, disposable string
		accepted                              bool
	}{
		{name: "real-data", connection: good, actions: "true", disposable: "1", accepted: true},
		{name: "non-ci", connection: good, disposable: "1"},
		{name: "non-disposable", connection: good, actions: "true"},
		{name: "missing-dsn", actions: "true", disposable: "1"},
		{name: "remote-host", connection: "grpc://db.example.invalid:2136/local?go_query_mode=data", actions: "true", disposable: "1"},
		{name: "scripting", connection: "grpc://localhost:2136/local?go_query_mode=scripting", actions: "true", disposable: "1"},
		{name: "fake-data", connection: good + "&go_fake_tx=scripting", actions: "true", disposable: "1"},
		{name: "duplicate-mode", connection: good + "&go_query_mode=data", actions: "true", disposable: "1"},
		{name: "url-credentials", connection: "grpc://synthetic@localhost:2136/local?go_query_mode=data", actions: "true", disposable: "1"},
	} {
		t.Run(check.name, func(t *testing.T) {
			values := map[string]string{"SESSIONLESS_LOCAL_RUNTIME_YDB_GATE": "1", "GITHUB_ACTIONS": check.actions, "SESSIONLESS_LOCAL_RUNTIME_YDB_DISPOSABLE": check.disposable, "YDB_CONNECTION_STRING": check.connection}
			dsn, enabled, err := localRuntimeYDBConnection(func(key string) string { return values[key] })
			if !enabled || (err == nil) != check.accepted || (check.accepted && dsn != check.connection) || (!check.accepted && dsn != "") {
				t.Fatalf("guard accepted=%t want=%t enabled=%t err=%v", err == nil, check.accepted, enabled, err)
			}
		})
	}
}

// Actual serializable Store and the public runtime own every Run, hold,
// invocation, operation and terminal transition. Only the two HTTP providers
// and immutable object storage are controlled peers. This is not a live model,
// browser, MCP/search, cloud or no-user-host qualification.
func TestLocalRuntimeYDBWebTwoMessagesAndTriggerBatch(t *testing.T) {
	// This guard precedes DSN, private files, client construction and probes.
	dsn, enabled, err := localRuntimeYDBConnection(os.Getenv)
	if !enabled {
		t.Skip("explicit disposable remote local-runtime gate not requested")
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	client, err := ydbclient.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closing, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if err := client.Close(closing); err != nil {
			t.Error(err)
		}
	})
	base, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	// Use the existing real startup predecessor, not another test's side
	// effects or raw marker writes. It verifies existing receipts or establishes
	// them only after its original empty-backlog checks; it never resets data.
	establishIntegrationCutovers(t, dsn, client)
	if err := ctx.Err(); err != nil {
		t.Fatal("original startup exceeded enclosing scenario", err)
	}
	for _, require := range []func(context.Context) error{base.RequireExecutionPlacementCutover, base.RequireHarnessBindingCutover, base.RequireManagedExecutionAuthorityV2Cutover, base.RequireAdmissionCostV2Cutover} {
		if err := require(ctx); err != nil {
			t.Fatal("original serving predecessor missing", err)
		}
	}
	clock := onboardingDatabaseClock{t: t, ctx: ctx, db: client.DB}
	suffix := uniqueID("local-runtime")
	tenant, owner := domain.TenantID("tenant-"+suffix), domain.UserID("owner-"+suffix)
	otherTenant, otherOwner := domain.TenantID("other-"+suffix), domain.UserID("other-"+suffix)
	auth := &localRuntimeAuthStore{Store: base}
	// Arm the complete fresh-scope cleanup before the first identity/profile write.
	t.Cleanup(func() { localRuntimeCleanup(t, client.DB, auth, []domain.TenantID{tenant, otherTenant}) })
	for _, identity := range []struct {
		tenant domain.TenantID
		owner  domain.UserID
	}{{tenant, owner}, {tenant, otherOwner}, {otherTenant, domain.UserID("foreign-" + suffix)}} {
		subjectHash := sha256.Sum256([]byte(identity.owner))
		subject := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: fmt.Sprintf("%d", binary.BigEndian.Uint64(subjectHash[:8])|1)}
		auth.identities = append(auth.identities, localRuntimeIdentity{subject: subject, user: identity.owner})
		resolved, fresh, err := base.ResolveOrCreateExternalIdentity(ctx, subject, identity.owner, clock.Now())
		if err != nil || !fresh || resolved.UserID != identity.owner {
			t.Fatal("fresh actual identity", fresh, err)
		}
		if _, err := base.BootstrapDevelopmentMembership(ctx, domain.DevelopmentBootstrapGrant{TenantID: identity.tenant, UserID: identity.owner, Role: domain.TenantMembershipOwner, Environment: domain.DevelopmentEnvironment, Operator: "local-runtime-ci", Reason: "bounded original canonical runtime proof", GrantedAt: clock.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	model := newLocalRuntimeModel(t)
	root, executable, receiptPath := newLocalRuntimeOperatorFiles(t, ctx, tenant, owner, model.server.URL+"/v1", clock.Now())
	if err := ydbstore.InstallLocalProfile(ctx, client.DB, root, executable, receiptPath); err != nil {
		t.Fatal("actual trusted local installation", err)
	}
	receipt, err := localoperator.ReadProfileReceipt(ctx, receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	source, err := localoperator.OpenExisting(ctx, root, executable, receipt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	model.mu.Lock()
	model.model = receipt.Profile.Locator.ModelID
	model.mu.Unlock()
	store, err := ydbstore.New(client.DB, ydbstore.Options{OwnedProfilePublicationVerifier: source, AttemptSpendingPublicationVerifier: source, OwnedAttemptEffectSourceVerifier: source})
	if err != nil {
		t.Fatal(err)
	}
	// Authentication persistence is the same actual Store; this wrapper only
	// records exact cleanup keys before forwarding unchanged writes.
	auth.Store = store
	binder, err := ydbstore.NewLocalHarnessBinder(store, source, receipt)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := ydbstore.NewLocalWebResourceStore(store, source, receipt)
	if err != nil {
		t.Fatal(err)
	}
	blobs := &localRuntimeBlobs{values: make(map[string][]byte), deleted: make(map[string]bool), absent: make(map[string]bool)}
	sessions, err := sessionapi.New(sessionapi.Config{IDKey: bytes.Repeat([]byte{61}, 32), CursorKey: bytes.Repeat([]byte{62}, 32)}, store, blobs, clock)
	if err != nil {
		t.Fatal(err)
	}
	wakes := &localRuntimeWakes{tenant: tenant, projections: make(map[domain.RunID]int)}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{63}, 32), HarnessBinder: binder, DispatchWakePublisher: wakes}, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	api, err := webapi.New(webapi.Config{IDKey: bytes.Repeat([]byte{64}, 32)}, sessions, ingress, store, resources, aw169UnusedWebObjects{}, clock)
	if err != nil {
		t.Fatal(err)
	}
	login := newLocalRuntimeLogin(t, ctx, auth, sessions, api, clock)
	sessionCookie, csrf := login.signIn(t, auth.identities[0].subject.Subject)
	created := localRuntimeRequest(ctx, login.handler, http.MethodPost, "/api/web/v1/sessions", `{"idempotency_key":"runtime-session"}`, sessionCookie, csrf)
	if created.Code != http.StatusCreated {
		t.Fatal("actual Web session creation", created.Code, created.Body.String())
	}
	var session webcontract.SessionSummary
	if err := json.Unmarshal(created.Body.Bytes(), &session); err != nil || session.SessionID == "" {
		t.Fatal("Web session identity", err)
	}
	messagePath := "/api/web/v1/sessions/" + string(session.SessionID) + "/messages"
	historyPath := "/api/web/v1/sessions/" + string(session.SessionID) + "/events?limit=10"
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 0)
	localRuntimeCount(t, ctx, client.DB, "runs", tenant, 0)
	localRuntimeCount(t, ctx, client.DB, "subscription_connections", tenant, 0)
	// Both foreign identities have valid persisted login/membership; neither
	// has authority for this original canonical Session or local installation.
	var foreignCookies []*http.Cookie
	for _, foreign := range auth.identities[1:] {
		cookie, token := login.signIn(t, foreign.subject.Subject)
		foreignCookies = append(foreignCookies, cookie)
		for _, request := range []struct{ method, path, body string }{{http.MethodGet, historyPath, ""}, {http.MethodPost, messagePath, `{"idempotency_key":"foreign","text":"must not execute"}`}} {
			response := localRuntimeRequest(ctx, login.handler, request.method, request.path, request.body, cookie, token)
			if response.Code != http.StatusNotFound {
				t.Fatal("foreign persisted actor reached canonical Session", response.Code, response.Body.String())
			}
		}
	}
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 0)
	localRuntimeCount(t, ctx, client.DB, "runs", tenant, 0)
	var retryErrors []error
	var retryMu sync.Mutex
	scratch := t.TempDir()
	runtime, err := worker.NewLocalRuntime(worker.LocalRuntimeConfig{Source: source, Receipt: receipt, DB: client.DB, Blobs: blobs, Worker: worker.Config{ScratchRoot: scratch, WorkerID: "runtime-" + suffix, LeaseTTL: 5 * time.Minute, LeaseWatchdogInterval: time.Minute, ProjectionWakePublisher: wakes, DeliveryWakePublisher: wakes, RetryObserver: func(err error) { retryMu.Lock(); defer retryMu.Unlock(); retryErrors = append(retryErrors, err) }}}, clock)
	if err != nil {
		t.Fatal(err)
	}
	submit := func(key, text string) (webcontract.CreateMessageResponse, ports.WorkerJobState, queuecontract.Envelope) {
		t.Helper()
		raw, _ := json.Marshal(webcontract.CreateMessageRequest{IdempotencyKey: domain.IdempotencyKey(key), Text: text})
		response := localRuntimeRequest(ctx, login.handler, http.MethodPost, messagePath, string(raw), sessionCookie, csrf)
		if response.Code != http.StatusCreated {
			t.Fatal("actual Web message", response.Code, response.Body.String())
		}
		var accepted webcontract.CreateMessageResponse
		if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil {
			t.Fatal(err)
		}
		if _, found, err := store.LoadWorkerJob(ctx, tenant, accepted.RunID); err != nil || found {
			t.Fatal("Web fabricated admitted/running job", found, err)
		}
		outboxID := wakes.lastDispatch()
		candidate, status, found, err := store.GetDispatch(ctx, tenant, outboxID)
		if err != nil || !found || status != domain.DispatchPending || candidate.RunID != accepted.RunID {
			t.Fatal("original canonical dispatch", status, found, err)
		}
		at := clock.Now()
		admitted, err := store.AdmitDispatch(ctx, ports.DispatchAdmissionRequest{TenantID: tenant, OutboxID: outboxID, RunID: candidate.RunID, AttemptID: candidate.AttemptID, ReservationID: domain.QuotaReservationID("hold-" + string(outboxID)), Now: at, HoldUntil: at.Add(5 * time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 4, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 64, MaxArtifacts: 8, MaxToolEvents: 16, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Minute, Turns: 1, InputBytes: 4096, ContextBytes: 4096, Artifacts: 1}})
		if err != nil || !admitted.Admitted || admitted.Delivery != ports.DispatchDeliveryManagedQueue {
			t.Fatal("actual queued local admission", admitted.Code, err)
		}
		loaded, found, err := store.LoadWorkerJob(ctx, tenant, accepted.RunID)
		if err != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated || loaded.Run.SubscriptionConnectionID != "" || loaded.Run.LocalCapacityAssociation == nil || loaded.Reservation.Status != domain.ReservationHeld {
			t.Fatal("original queued state", found, err)
		}
		localRuntimeCount(t, ctx, client.DB, "lease_heads", tenant, 0)
		localRuntimeCount(t, ctx, client.DB, "attempt_effect_reservations", tenant, uint64(model.calls()))
		return accepted, loaded, queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: domain.MessageID("message-" + string(accepted.RunID)), Kind: queuecontract.KindDispatchRun, TenantID: tenant, SubjectID: string(accepted.RunID), EnqueuedAt: loaded.Job.CreatedAt}
	}
	first, firstJob, firstEnvelope := submit("message-a", "first public runtime message")
	model.expectHistory(t, ctx, store, blobs, tenant, owner, session.SessionID, []localRuntimeExpectedEvent{{id: first.EventID, run: first.RunID, kind: domain.SessionEventUserMessage, text: "first public runtime message"}})
	firstQueue := localRuntimeTriggerQueue(t, firstEnvelope)
	firstContext, closeFirstCaller := context.WithCancel(ctx)
	t.Cleanup(closeFirstCaller)
	firstCaller := &localRuntimeLifetimeCaller{Context: firstContext}
	firstObserved := &localRuntimeQueue{Queue: firstQueue, onAck: func() error {
		if active, started := firstCaller.counts(); active != 0 || started == 0 {
			return fmt.Errorf("productive ACK before original caller hooks joined: active=%d started=%d", active, started)
		}
		return localRuntimeReleased(ctx, client.DB, store, blobs, tenant, owner, firstJob, 1)
	}}
	outcome, err := runtime.RunOnce(firstCaller, firstObserved)
	if active, started := firstCaller.counts(); active != 0 || started == 0 {
		t.Fatal("first caller not joined at runtime return", active, started)
	}
	closeFirstCaller()
	if err != nil || outcome != worker.OutcomeCompleted || firstObserved.acks != 1 || firstQueue.Remaining() != 0 || model.calls() != 1 {
		t.Fatal("first public runtime", outcome, firstObserved.acks, model.calls(), err)
	}
	firstSnapshot := localRuntimeStoredSnapshot(t, ctx, client.DB, tenant, firstJob)
	firstHistory := localRuntimeHistory(t, ctx, login.handler, historyPath, sessionCookie)
	localRuntimeAssertHistory(t, firstHistory, []webcontract.CreateMessageResponse{first}, []string{"runtime-answer-1"})
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 4)
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_publication_audit_v1", tenant, 4)
	// The same borrowed Source survives caller closure, without reopen, receipt
	// replacement, funding renewal, active-row mutation or a second installer.
	if _, err := source.Configuration(); err != nil {
		t.Fatal("completed caller closed borrowed installation", err)
	}
	second, secondJob, secondEnvelope := submit("message-b", "second public runtime message")
	model.expectHistory(t, ctx, store, blobs, tenant, owner, session.SessionID, []localRuntimeExpectedEvent{
		{id: first.EventID, run: first.RunID, kind: domain.SessionEventUserMessage, text: "first public runtime message"},
		{id: firstHistory.Items[1].EventID, run: first.RunID, kind: domain.SessionEventAssistantMessage, text: "runtime-answer-1"},
		{id: second.EventID, run: second.RunID, kind: domain.SessionEventUserMessage, text: "second public runtime message"},
	})
	if second.RunID == first.RunID || secondJob.Attempt.ID == firstJob.Attempt.ID || secondJob.Reservation.ID == firstJob.Reservation.ID {
		t.Fatal("second Web submission reused original invocation identities")
	}
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 8)
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_publication_audit_v1", tenant, 8)
	batch := localRuntimeTriggerQueue(t, firstEnvelope, secondEnvelope)
	beforeDuplicate := blobs.counts()
	var batchCaller *localRuntimeLifetimeCaller
	observed := &localRuntimeQueue{Queue: batch, onAck: func() error {
		if batch.Remaining() == 1 {
			if model.calls() != 1 || blobs.counts() != beforeDuplicate {
				return errors.New("terminal duplicate replayed a model/storage effect")
			}
			return nil
		}
		if active, started := batchCaller.counts(); active != 0 || started == 0 {
			return fmt.Errorf("second productive ACK before original caller hooks joined: active=%d started=%d", active, started)
		}
		return localRuntimeReleased(ctx, client.DB, store, blobs, tenant, owner, secondJob, 2)
	}}
	var outcomes []worker.Outcome
	for batch.Remaining() > 0 {
		invocation, closeCaller := context.WithCancel(ctx)
		t.Cleanup(closeCaller)
		batchCaller = &localRuntimeLifetimeCaller{Context: invocation}
		result, err := runtime.RunOnce(batchCaller, observed)
		if active, started := batchCaller.counts(); active != 0 || started == 0 {
			t.Fatal("batch message caller not joined at runtime return", active, started)
		}
		closeCaller()
		if err != nil {
			t.Fatal("actual YMQ batch runtime", result, err)
		}
		outcomes = append(outcomes, result)
	}
	if !reflect.DeepEqual(outcomes, []worker.Outcome{worker.OutcomeDuplicate, worker.OutcomeCompleted}) || observed.acks != 2 || model.calls() != 2 {
		t.Fatal("actual trigger batch outcomes", outcomes, observed.acks, model.calls())
	}
	if next := localRuntimeStoredSnapshot(t, ctx, client.DB, tenant, firstJob); !reflect.DeepEqual(firstSnapshot, next) {
		t.Fatal("second invocation/duplicate rewrote original terminal/funding/registration bytes")
	}
	secondSnapshot := localRuntimeStoredSnapshot(t, ctx, client.DB, tenant, secondJob)
	if firstSnapshot.claim == "" || secondSnapshot.claim == "" || firstSnapshot.claim == secondSnapshot.claim || firstSnapshot.lease == secondSnapshot.lease {
		t.Fatal("fresh per-message runtime did not retain distinct physical claims/leases")
	}
	history := localRuntimeHistory(t, ctx, login.handler, historyPath, sessionCookie)
	localRuntimeAssertHistory(t, history, []webcontract.CreateMessageResponse{first, second}, []string{"runtime-answer-1", "runtime-answer-2"})
	if !reflect.DeepEqual(history.Items[:len(firstHistory.Items)], firstHistory.Items) {
		t.Fatal("second message rewrote earlier canonical public history")
	}
	for _, cookie := range foreignCookies {
		if response := localRuntimeRequest(ctx, login.handler, http.MethodGet, historyPath, "", cookie, nil); response.Code != http.StatusNotFound {
			t.Fatal("foreign persisted actor read durable answers", response.Code)
		}
	}
	logout := localRuntimeRequest(ctx, login.handler, http.MethodPost, webcontract.RouteLogout, `{}`, sessionCookie, csrf)
	if logout.Code != http.StatusNoContent {
		t.Fatal("actual logout", logout.Code, logout.Body.String())
	}
	if response := localRuntimeRequest(ctx, login.handler, http.MethodGet, historyPath, "", sessionCookie, nil); response.Code != http.StatusUnauthorized {
		t.Fatal("revoked login retained history access", response.Code)
	}
	newCookie, _ := login.signIn(t, auth.identities[0].subject.Subject)
	if newCookie.Value == sessionCookie.Value || !reflect.DeepEqual(localRuntimeHistory(t, ctx, login.handler, historyPath, newCookie), history) {
		t.Fatal("fresh persisted login did not preserve exact canonical answer history")
	}
	localRuntimeCount(t, ctx, client.DB, "attempt_effect_reservations", tenant, 2)
	localRuntimeCount(t, ctx, client.DB, "attempt_operations_v1", tenant, 2)
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_authorization_current_v1", tenant, 8)
	localRuntimeCount(t, ctx, client.DB, "attempt_spending_publication_audit_v1", tenant, 8)
	localRuntimeCount(t, ctx, client.DB, "subscription_connections", tenant, 0)
	if _, err := source.Configuration(); err != nil {
		t.Fatal("batch return closed borrowed original Source", err)
	}
	retryMu.Lock()
	defer retryMu.Unlock()
	// Terminal redelivery legitimately re-wakes the existing projection; this
	// is not another model execution, canonical finalization or history write.
	if len(retryErrors) != 0 || wakes.projectionCount(first.RunID) != 2 || wakes.projectionCount(second.RunID) != 1 {
		t.Fatal("runtime hid retry or duplicated completion wake", retryErrors)
	}
	if arrivals, invalid := model.arrivalCounts(); arrivals != 2 || invalid != 0 {
		t.Fatal("model oracle missed an HTTP attempt", arrivals, invalid)
	}
	if err := filepath.WalkDir(scratch, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSocket != 0 {
			return errors.New("retained invocation socket after batch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type localRuntimeBlobCounts struct{ puts, deletes, absent int }
type localRuntimeBlobs struct {
	mu              sync.Mutex
	values          map[string][]byte
	deleted, absent map[string]bool
	totals          localRuntimeBlobCounts
}

func (b *localRuntimeBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, reader io.Reader) (domain.BlobRef, error) {
	if ctx.Err() != nil || tenant.Validate() != nil || !strings.HasPrefix(key, "tenants/"+string(tenant)+"/") {
		return domain.BlobRef{}, errors.New("blob scope denied")
	}
	raw, err := io.ReadAll(io.LimitReader(reader, (4<<20)+1))
	if err != nil || len(raw) == 0 || len(raw) > 4<<20 {
		return domain.BlobRef{}, errors.New("blob bound denied")
	}
	hash := sha256.Sum256(raw)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: hex.EncodeToString(hash[:])}
	if err := ref.Validate(); err != nil {
		return domain.BlobRef{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	k := string(tenant) + "\x00" + key
	if old, ok := b.values[k]; ok && !bytes.Equal(old, raw) {
		return domain.BlobRef{}, errors.New("immutable blob overwrite")
	}
	b.values[k] = bytes.Clone(raw)
	b.totals.puts++
	return ref, nil
}
func (b *localRuntimeBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return nil, errors.New("blob scope denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, ok := b.values[string(tenant)+"\x00"+ref.Key]
	if !ok {
		return nil, errors.New("blob absent")
	}
	hash := sha256.Sum256(raw)
	if int64(len(raw)) != ref.Size || hex.EncodeToString(hash[:]) != ref.SHA256 {
		return nil, errors.New("blob identity changed")
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(raw))), nil
}
func (b *localRuntimeBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("blob delete scope denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	k := string(tenant) + "\x00" + ref.Key
	if raw, ok := b.values[k]; ok {
		hash := sha256.Sum256(raw)
		if int64(len(raw)) != ref.Size || hex.EncodeToString(hash[:]) != ref.SHA256 {
			return errors.New("blob delete identity changed")
		}
		delete(b.values, k)
	}
	b.deleted[k] = true
	b.totals.deletes++
	return nil
}
func (b *localRuntimeBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("blob absence scope denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	k := string(tenant) + "\x00" + ref.Key
	if _, ok := b.values[k]; ok || !b.deleted[k] {
		return errors.New("exact blob deletion not observed")
	}
	b.absent[k] = true
	b.totals.absent++
	return nil
}
func (b *localRuntimeBlobs) counts() localRuntimeBlobCounts {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.totals
}

func TestLocalRuntimeYDBBlobPeerExactDeleteAndAbsence(t *testing.T) {
	blobs := &localRuntimeBlobs{values: make(map[string][]byte), deleted: make(map[string]bool), absent: make(map[string]bool)}
	tenant := domain.TenantID("tenant-runtime-peer")
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	ref, err := blobs.Put(ctx, tenant, domain.SessionRunObjectPrefix(tenant, "session-peer", "run-peer")+"operation-body", strings.NewReader("public immutable body"))
	if err != nil {
		t.Fatal(err)
	}
	before := blobs.counts()
	if err := blobs.RequireBlobAbsent(ctx, tenant, ref); err == nil {
		t.Fatal("present object accepted as absent")
	}
	if err := blobs.Delete(ctx, "foreign-tenant", ref); err == nil || blobs.counts() != before {
		t.Fatal("foreign delete altered original object")
	}
	wrong := ref
	wrong.SHA256 = strings.Repeat("0", 64)
	if _, err := blobs.Open(ctx, tenant, wrong); err == nil {
		t.Fatal("substituted object identity opened")
	}
	if err := blobs.Delete(ctx, tenant, wrong); err == nil || blobs.counts() != before {
		t.Fatal("substituted object identity deleted")
	}
	if _, err := blobs.Put(ctx, tenant, ref.Key, strings.NewReader("changed immutable body")); err == nil || blobs.counts() != before {
		t.Fatal("immutable object overwritten")
	}
	if err := blobs.Delete(ctx, tenant, ref); err != nil {
		t.Fatal(err)
	}
	if err := blobs.RequireBlobAbsent(ctx, tenant, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := blobs.Open(ctx, tenant, ref); err == nil {
		t.Fatal("deleted object remained readable")
	}
	before = blobs.counts()
	cancel()
	if err := blobs.RequireBlobAbsent(ctx, tenant, ref); err == nil || blobs.counts() != before {
		t.Fatal("cancellation became successful absence observation")
	}
}

type localRuntimeModel struct {
	server            *httptest.Server
	mu                sync.Mutex
	model             string
	requests          [][]byte
	expected          [][]localRuntimePromptFrame
	arrivals, invalid int
}

type localRuntimeExpectedEvent struct {
	id   domain.SessionEventID
	run  domain.RunID
	kind domain.SessionEventKind
	text string
}

type localRuntimePromptFrame struct {
	Kind     string `json:"kind"`
	SourceID string `json:"source_id"`
	Role     string `json:"original_role"`
	EventID  string `json:"event_id,omitempty"`
	RunID    string `json:"run_id,omitempty"`
	Sequence uint64 `json:"sequence"`
	Payload  string `json:"original_payload"`
	Digest   string `json:"original_digest"`
}

// Freeze the expectation from the authorized canonical Store and immutable
// payloads BEFORE running the worker. Never derive the oracle from its request.
func (p *localRuntimeModel) expectHistory(t *testing.T, ctx context.Context, store *ydbstore.Store, blobs *localRuntimeBlobs, tenant domain.TenantID, owner domain.UserID, session domain.SessionID, expected []localRuntimeExpectedEvent) {
	t.Helper()
	events, err := store.ListSessionHistoryForUser(ctx, tenant, owner, session, 0, 4)
	if err != nil || len(events) != len(expected) {
		t.Fatal("model oracle canonical frontier", len(events), len(expected), err)
	}
	frames := make([]localRuntimePromptFrame, len(events))
	for i, event := range events {
		want := expected[i]
		if event.TenantID != tenant || event.SessionID != session || event.ID != want.id || event.RunID == nil || *event.RunID != want.run || event.Kind != want.kind || event.Sequence != uint64(i+1) {
			t.Fatal("model oracle canonical event identity", i)
		}
		body, err := blobs.Open(ctx, tenant, event.Payload)
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(body, (1<<20)+1))
		err = errors.Join(readErr, body.Close())
		text, payloadErr := localRuntimeCanonicalText(event.Kind, raw)
		if err != nil || len(raw) > 1<<20 || payloadErr != nil || text != want.text {
			t.Fatal("model oracle canonical payload", i, errors.Join(err, payloadErr))
		}
		key, err := compactioncontext.CanonicalSourceKeyV1(event)
		if err != nil {
			t.Fatal(err)
		}
		frames[i] = localRuntimePromptFrame{Kind: "canonical", SourceID: key, Role: string(event.Kind), EventID: string(event.ID), RunID: string(want.run), Sequence: event.Sequence, Payload: string(raw), Digest: event.Payload.SHA256}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expected = append(p.expected, frames)
}

func localRuntimeCanonicalText(kind domain.SessionEventKind, raw []byte) (string, error) {
	switch kind {
	case domain.SessionEventUserMessage:
		var payload struct {
			Version uint32 `json:"version"`
			Text    string `json:"text"`
		}
		if json.Unmarshal(raw, &payload) == nil && payload.Version == 1 && payload.Text != "" {
			return payload.Text, nil
		}
	case domain.SessionEventAssistantMessage:
		var payload struct {
			Schema  string `json:"schema"`
			Summary string `json:"summary"`
		}
		if json.Unmarshal(raw, &payload) == nil && payload.Schema == "sessionless.assistant-message.v1" && payload.Summary != "" {
			return payload.Summary, nil
		}
	}
	return "", errors.New("unexpected canonical message envelope")
}

func TestLocalRuntimeYDBModelOracleCanonicalEnvelopes(t *testing.T) {
	plan, err := canonicaloutput.EncodeCompletionV1("run-oracle", domain.ProductLimits{}, ports.ExecutionResult{Summary: "runtime-answer-1"}, "manifest-oracle")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Release)
	assistant := plan.Events()[0]
	user := []byte(`{"version":1,"text":"first public runtime message"}`)
	for _, check := range []struct {
		name string
		kind domain.SessionEventKind
		raw  []byte
		text string
	}{
		{name: "actual-assistant-encoder", kind: assistant.Kind, raw: assistant.Payload, text: "runtime-answer-1"},
		{name: "canonical-user", kind: domain.SessionEventUserMessage, raw: user, text: "first public runtime message"},
	} {
		t.Run(check.name, func(t *testing.T) {
			if text, err := localRuntimeCanonicalText(check.kind, check.raw); err != nil || text != check.text {
				t.Fatalf("canonical envelope text=%q want=%q err=%v", text, check.text, err)
			}
		})
	}
	for _, check := range []struct {
		name string
		kind domain.SessionEventKind
		raw  []byte
	}{
		{name: "assistant-as-user", kind: domain.SessionEventUserMessage, raw: assistant.Payload},
		{name: "user-as-assistant", kind: domain.SessionEventAssistantMessage, raw: user},
		{name: "wrong-assistant-schema", kind: domain.SessionEventAssistantMessage, raw: []byte(`{"schema":"foreign","summary":"runtime-answer-1"}`)},
		{name: "wrong-user-version", kind: domain.SessionEventUserMessage, raw: []byte(`{"version":2,"text":"first"}`)},
	} {
		t.Run(check.name, func(t *testing.T) {
			if _, err := localRuntimeCanonicalText(check.kind, check.raw); err == nil {
				t.Fatal("substituted canonical envelope accepted")
			}
		})
	}
}

func localRuntimeValidatePrompt(raw []byte, model string, expected []localRuntimePromptFrame) error {
	var input struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(raw, &input) != nil || input.Model != model || len(input.Messages) != 2 || input.Messages[0].Role != "system" || input.Messages[0].Content == "" || input.Messages[1].Role != "user" {
		return errors.New("model request roles/selection")
	}
	var history struct {
		Schema    string                    `json:"schema"`
		Protected []localRuntimePromptFrame `json:"protected_originals"`
		Units     []localRuntimePromptFrame `json:"original_units"`
		Summary   string                    `json:"derived_summary"`
	}
	if json.Unmarshal([]byte(input.Messages[1].Content), &history) != nil || history.Schema != "sessionless.history.v1" || len(history.Protected) != 0 || history.Summary != "" || len(expected) == 0 || !reflect.DeepEqual(history.Units, expected) {
		return errors.New("model request canonical history mismatch")
	}
	return nil
}

func TestLocalRuntimeYDBModelOracleRejectsStaleAndForeignHistory(t *testing.T) {
	a := localRuntimePromptFrame{Kind: "canonical", SourceID: strings.Repeat("a", 64), Role: "user_message", EventID: "event-a", RunID: "run-a", Sequence: 1, Payload: `{"text":"first"}`, Digest: strings.Repeat("b", 64)}
	answer := localRuntimePromptFrame{Kind: "canonical", SourceID: strings.Repeat("c", 64), Role: "assistant_message", EventID: "answer-a", RunID: "run-a", Sequence: 2, Payload: `{"text":"answer"}`, Digest: strings.Repeat("d", 64)}
	b := localRuntimePromptFrame{Kind: "canonical", SourceID: strings.Repeat("e", 64), Role: "user_message", EventID: "event-b", RunID: "run-b", Sequence: 3, Payload: `{"text":"second"}`, Digest: strings.Repeat("f", 64)}
	first, second := []localRuntimePromptFrame{a}, []localRuntimePromptFrame{a, answer, b}
	wire := func(frames []localRuntimePromptFrame) []byte {
		history, err := json.Marshal(map[string]any{"schema": "sessionless.history.v1", "protected_originals": nil, "original_units": frames, "derived_summary": ""})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(map[string]any{"model": "runtime-model", "messages": []map[string]string{{"role": "system", "content": "synthetic policy"}, {"role": "user", "content": string(history)}}})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, frames := range [][]localRuntimePromptFrame{first, second} {
		if err := localRuntimeValidatePrompt(wire(frames), "runtime-model", frames); err != nil {
			t.Fatal("original canonical history denied", err)
		}
	}
	foreign := b
	foreign.SourceID = strings.Repeat("0", 64) // source key binds tenant/session too
	wrongPayload := b
	wrongPayload.Payload = `{"text":"foreign"}`
	for _, check := range []struct {
		name   string
		frames []localRuntimePromptFrame
	}{
		{name: "old-A-as-B", frames: first}, {name: "missing-assistant", frames: []localRuntimePromptFrame{a, b}},
		{name: "reordered", frames: []localRuntimePromptFrame{b, answer, a}}, {name: "duplicate-A", frames: []localRuntimePromptFrame{a, answer, a}},
		{name: "foreign-session-source", frames: []localRuntimePromptFrame{a, answer, foreign}}, {name: "wrong-payload", frames: []localRuntimePromptFrame{a, answer, wrongPayload}},
	} {
		t.Run(check.name, func(t *testing.T) {
			if err := localRuntimeValidatePrompt(wire(check.frames), "runtime-model", second); err == nil {
				t.Fatal("substituted second-message history accepted")
			}
		})
	}
}

func newLocalRuntimeModel(t *testing.T) *localRuntimeModel {
	t.Helper()
	p := new(localRuntimeModel)
	p.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.arrivals++
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			p.invalid++
			t.Error("unexpected model route/auth")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(raw) > 1<<20 {
			p.invalid++
			t.Error("model body bound")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(p.requests) >= 2 || len(p.requests) >= len(p.expected) {
			p.invalid++
			t.Error("unexpected model selection/replay")
			w.WriteHeader(http.StatusConflict)
			return
		}
		if err := localRuntimeValidatePrompt(raw, p.model, p.expected[len(p.requests)]); err != nil {
			p.invalid++
			t.Error("actual model request lost original canonical history", err)
			w.WriteHeader(http.StatusConflict)
			return
		}
		p.requests = append(p.requests, bytes.Clone(raw))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": fmt.Sprintf("runtime-response-%d", len(p.requests)), "object": "chat.completion", "created": 1, "model": p.model, "choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": fmt.Sprintf("runtime-answer-%d", len(p.requests))}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}})
	}))
	p.server.Config.ReadHeaderTimeout = 5 * time.Second
	p.server.Config.ReadTimeout = 10 * time.Second
	p.server.Config.WriteTimeout = 10 * time.Second
	p.server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { p.server.CloseClientConnections(); p.server.Close() })
	p.server.Start()
	return p
}
func (p *localRuntimeModel) calls() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.requests) }
func (p *localRuntimeModel) arrivalCounts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.arrivals, p.invalid
}

type localRuntimeIdentity struct {
	subject domain.ExternalSubject
	user    domain.UserID
}
type localRuntimeAuthStore struct {
	*ydbstore.Store
	mu         sync.Mutex
	identities []localRuntimeIdentity
	challenges []domain.OIDCLoginChallenge
	sessions   []domain.WebSession
	audits     []domain.WebSecurityAuditEvent
}

func (s *localRuntimeAuthStore) CreateLoginChallenge(ctx context.Context, c domain.OIDCLoginChallenge) error {
	s.mu.Lock()
	s.challenges = append(s.challenges, c)
	s.mu.Unlock()
	return s.Store.CreateLoginChallenge(ctx, c)
}
func (s *localRuntimeAuthStore) CreateWebSession(ctx context.Context, v domain.WebSession) error {
	s.mu.Lock()
	s.sessions = append(s.sessions, v)
	s.mu.Unlock()
	return s.Store.CreateWebSession(ctx, v)
}
func (s *localRuntimeAuthStore) RecordWebSecurityEvent(ctx context.Context, v domain.WebSecurityAuditEvent) error {
	s.mu.Lock()
	s.audits = append(s.audits, v)
	s.mu.Unlock()
	return s.Store.RecordWebSecurityEvent(ctx, v)
}

type localRuntimeLogin struct {
	handler            http.Handler
	ctx                context.Context
	subject, challenge string
	mu                 sync.Mutex
}

func newLocalRuntimeLogin(t *testing.T, ctx context.Context, store *localRuntimeAuthStore, sessions *sessionapi.Service, api *webapi.Service, clock ports.Clock) *localRuntimeLogin {
	t.Helper()
	l := &localRuntimeLogin{ctx: ctx}
	peer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		l.mu.Lock()
		defer l.mu.Unlock()
		switch r.URL.Path {
		case "/token":
			client, secret, ok := r.BasicAuth()
			if r.Method != http.MethodPost || !ok || client != "runtime-client" || secret != "synthetic-no-real-key" || r.ParseForm() != nil {
				t.Error("Yandex peer authentication")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			hash := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if r.PostForm.Get("code") != "runtime-code" || r.PostForm.Get("redirect_uri") != localRuntimeWebOrigin+webcontract.RouteLoginCallback || r.PostForm.Get("grant_type") != "authorization_code" || base64.RawURLEncoding.EncodeToString(hash[:]) != l.challenge {
				t.Error("Yandex peer PKCE/code binding")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			fmt.Fprint(w, `{"access_token":"synthetic-runtime-token","token_type":"bearer","expires_in":3600}`)
		case "/info":
			if r.Header.Get("Authorization") != "OAuth synthetic-runtime-token" || r.URL.RawQuery != "format=json" {
				t.Error("Yandex peer account binding")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"id": l.subject, "client_id": "runtime-client"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	peer.Config.ReadHeaderTimeout = 5 * time.Second
	peer.Config.ReadTimeout = 10 * time.Second
	peer.Config.WriteTimeout = 10 * time.Second
	t.Cleanup(func() { peer.CloseClientConnections(); peer.Close() })
	peer.Start()
	provider, err := yandexoauth.New(yandexoauth.Config{ClientID: "runtime-client", ClientSecret: "synthetic-no-real-key", RedirectURI: localRuntimeWebOrigin + webcontract.RouteLoginCallback, AuthorizationEndpoint: peer.URL + "/authorize", TokenEndpoint: peer.URL + "/token", InfoEndpoint: peer.URL + "/info", AllowLoopbackProvider: true, HTTPClient: peer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	// API construction always requires an exact browser capability origin,
	// including this text-only proof. The reserved synthetic origin is policy
	// DATA, not a contacted object service or qualification of uploads/media.
	l.handler, err = webbff.New(webbff.Config{BaseURL: localRuntimeWebOrigin, RedirectURI: localRuntimeWebOrigin + webcontract.RouteLoginCallback, ObjectStorageOrigin: "https://runtime-objects.example.invalid", OAuthProvider: provider, OAuthClientID: "runtime-client", Store: store, Sessions: sessions, API: api, IDs: idgen.New(), Clock: clock, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// Constructor/response policy only. Empty service DATA is never invoked: this
// isolates the same BFF configuration used by the real-SQL scenario without
// opening SQL, installing an operator or executing a model. Health is public
// and has no persistence dependency. The synthetic OAuth peer is never called.
type localRuntimeConfigurationClock struct{}

func (localRuntimeConfigurationClock) Now() time.Time {
	return time.Unix(1_700_000_000, 0).UTC()
}

func TestLocalRuntimeLoginConfigurationHasObjectStorageOrigin(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	login := newLocalRuntimeLogin(t, ctx, &localRuntimeAuthStore{}, nil, &webapi.Service{}, localRuntimeConfigurationClock{})
	response := localRuntimeRequest(ctx, login.handler, http.MethodGet, "/healthz", "", nil, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("same native fixture BFF health: got=%d want=%d", response.Code, http.StatusOK)
	}
	policy := response.Header().Get("Content-Security-Policy")
	if !strings.Contains(policy, "connect-src 'self' https://runtime-objects.example.invalid") || strings.Contains(policy, "connect-src *") {
		t.Fatalf("same native fixture exact object origin missing from CSP: %q", policy)
	}
}

func (l *localRuntimeLogin) signIn(t *testing.T, subject string) (*http.Cookie, *http.Cookie) {
	t.Helper()
	l.mu.Lock()
	l.subject = subject
	l.mu.Unlock()
	start := localRuntimeRequest(l.ctx, l.handler, http.MethodGet, webcontract.RouteLoginStart+"?return_to=/sessions", "", nil, nil)
	if start.Code != http.StatusSeeOther {
		t.Fatal("actual Yandex login start", start.Code, start.Body.String())
	}
	u, err := url.Parse(start.Header().Get("Location"))
	if err != nil || u.Query().Get("code_challenge_method") != "S256" || u.Query().Get("state") == "" {
		t.Fatal("actual Yandex login binding", err)
	}
	l.mu.Lock()
	l.challenge = u.Query().Get("code_challenge")
	l.mu.Unlock()
	callback := localRuntimeRequest(l.ctx, l.handler, http.MethodGet, webcontract.RouteLoginCallback+"?code=runtime-code&state="+url.QueryEscape(u.Query().Get("state")), "", localRuntimeCookie(t, start, webbff.LoginBindingCookieName), nil)
	if callback.Code != http.StatusSeeOther || callback.Header().Get("Location") != "/sessions" {
		t.Fatal("actual Yandex login callback", callback.Code, callback.Header().Get("Location"), callback.Body.String())
	}
	return localRuntimeCookie(t, callback, webcontract.SessionCookieName), localRuntimeCookie(t, callback, webcontract.CSRFCookieName)
}
func localRuntimeCookie(t *testing.T, response *httptest.ResponseRecorder, name string) *http.Cookie {
	t.Helper()
	for _, c := range response.Result().Cookies() {
		if c.Name == name && c.Value != "" {
			return c
		}
	}
	t.Fatalf("missing %s cookie", name)
	return nil
}
func localRuntimeRequest(ctx context.Context, h http.Handler, method, path, body string, cookie, csrf *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, localRuntimeWebOrigin+path, strings.NewReader(body)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	if cookie != nil {
		r.AddCookie(cookie)
	}
	if csrf != nil {
		r.AddCookie(csrf)
		r.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
		r.Header.Set("Origin", localRuntimeWebOrigin)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func localRuntimeHistory(t *testing.T, ctx context.Context, h http.Handler, path string, cookie *http.Cookie) webcontract.Page[webcontract.SessionEvent] {
	t.Helper()
	r := localRuntimeRequest(ctx, h, http.MethodGet, path, "", cookie, nil)
	if r.Code != http.StatusOK {
		t.Fatal("actual public history", r.Code, r.Body.String())
	}
	var p webcontract.Page[webcontract.SessionEvent]
	if err := json.Unmarshal(r.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func localRuntimeAssertHistory(t *testing.T, p webcontract.Page[webcontract.SessionEvent], messages []webcontract.CreateMessageResponse, answers []string) {
	t.Helper()
	if len(p.Items) != len(messages)*2 || p.NextCursor != "" {
		t.Fatal("canonical history frontier", len(p.Items), p.NextCursor)
	}
	for i, message := range messages {
		input, answer := p.Items[2*i], p.Items[2*i+1]
		if input.EventID != message.EventID || input.Sequence != uint64(2*i+1) || answer.Sequence != uint64(2*i+2) || input.RunID == nil || answer.RunID == nil || *input.RunID != message.RunID || *answer.RunID != message.RunID || answer.Kind != domain.SessionEventAssistantMessage || answer.Content.Text != answers[i] {
			t.Fatal("canonical answer correlation", i)
		}
	}
}

type localRuntimeWakes struct {
	mu          sync.Mutex
	tenant      domain.TenantID
	dispatch    domain.DispatchOutboxID
	projections map[domain.RunID]int
}

func (w *localRuntimeWakes) PublishDispatchWake(ctx context.Context, tenant domain.TenantID, id domain.DispatchOutboxID, _ time.Time) error {
	if ctx.Err() != nil || tenant != w.tenant {
		return errors.New("dispatch wake scope")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dispatch = id
	return nil
}
func (w *localRuntimeWakes) lastDispatch() domain.DispatchOutboxID {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dispatch
}
func (w *localRuntimeWakes) PublishFrontendProjectionWake(ctx context.Context, tenant domain.TenantID, run domain.RunID, _ time.Time) error {
	if ctx.Err() != nil || tenant != w.tenant {
		return errors.New("projection wake scope")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projections[run]++
	return nil
}
func (*localRuntimeWakes) PublishTelegramDeliveryWake(context.Context, domain.TenantID, domain.TelegramDeliveryID, time.Time) error {
	return errors.New("local Web proof cannot publish Telegram")
}
func (w *localRuntimeWakes) projectionCount(run domain.RunID) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.projections[run]
}

type localRuntimeQueue struct {
	ports.Queue
	onAck func() error
	acks  int
}

// Observe the public Context.AfterFunc contract, not a capability or a new
// lifecycle owner. Hide cancelCtx's private Value shortcut so each original
// hook is observable; cancellation itself remains the real caller's context.
type localRuntimeLifetimeCaller struct {
	context.Context
	mu              sync.Mutex
	active, started int
}

func (*localRuntimeLifetimeCaller) Value(any) any { return nil }
func (c *localRuntimeLifetimeCaller) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	c.active++
	c.started++
	c.mu.Unlock()
	var once sync.Once
	finish := func() { once.Do(func() { c.mu.Lock(); c.active--; c.mu.Unlock() }) }
	stop := context.AfterFunc(c.Context, func() { defer finish(); f() })
	return func() bool {
		stopped := stop()
		if stopped {
			finish()
		}
		return stopped
	}
}
func (c *localRuntimeLifetimeCaller) counts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active, c.started
}

func TestLocalRuntimeYDBCallerHookObservationJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		caller := &localRuntimeLifetimeCaller{Context: ctx}
		var calls int
		stop := context.AfterFunc(caller, func() { calls++ })
		if active, started := caller.counts(); active != 1 || started != 1 {
			t.Fatal("original hook not observed", active, started)
		}
		if !stop() {
			t.Fatal("live hook did not stop")
		}
		stop()
		if active, started := caller.counts(); active != 0 || started != 1 {
			t.Fatal("stop did not join exact original hook", active, started)
		}
		context.AfterFunc(caller, func() { calls++ })
		cancel()
		synctest.Wait()
		if active, started := caller.counts(); active != 0 || started != 2 || calls != 1 {
			t.Fatal("cancellation hook observation mismatch", active, started, calls)
		}
	})
}

func (q *localRuntimeQueue) Ack(ctx context.Context, receipt string) error {
	if err := q.onAck(); err != nil {
		return err
	}
	if err := q.Queue.Ack(ctx, receipt); err != nil {
		return err
	}
	q.acks++
	return nil
}
func localRuntimeTriggerQueue(t *testing.T, envelopes ...queuecontract.Envelope) *yandextriggers.Queue {
	t.Helper()
	messages := make([]any, 0, len(envelopes))
	for i, envelope := range envelopes {
		raw, err := queuecontract.Encode(envelope)
		if err != nil {
			t.Fatal(err)
		}
		messages = append(messages, map[string]any{"event_metadata": map[string]any{"event_id": fmt.Sprintf("runtime-delivery-%d", i)}, "details": map[string]any{"message": map[string]any{"message_id": envelope.MessageID, "body": string(raw)}}})
	}
	raw, err := json.Marshal(map[string]any{"messages": messages})
	if err != nil {
		t.Fatal(err)
	}
	q, err := yandextriggers.NewYMQQueue(bytes.NewReader(raw), 2)
	if err != nil {
		t.Fatal(err)
	}
	return q
}
func localRuntimeCount(t *testing.T, ctx context.Context, db *sql.DB, table string, tenant domain.TenantID, want uint64) {
	t.Helper()
	var count uint64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1", tenant).Scan(&count); err != nil || count != want {
		t.Fatalf("%s count=%d want=%d err=%v", table, count, want, err)
	}
}
func localRuntimeReleased(ctx context.Context, db *sql.DB, store *ydbstore.Store, blobs *localRuntimeBlobs, tenant domain.TenantID, owner domain.UserID, job ports.WorkerJobState, ordinal int) error {
	run, found, err := store.GetRunForUser(ctx, tenant, owner, job.Run.ID)
	if err != nil || !found || run.Run.Status != domain.RunSucceeded || run.Run.TerminalCustody == nil {
		return fmt.Errorf("ACK before canonical success: found=%t err=%v", found, err)
	}
	var raw string
	if err := db.QueryRowContext(ctx, "SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", tenant, job.Reservation.ID).Scan(&raw); err != nil {
		return err
	}
	var quota domain.QuotaReservation
	if json.Unmarshal([]byte(raw), &quota) != nil || quota.Status != domain.ReservationCommitted || !quota.ExpiresAt.Equal(job.Reservation.ExpiresAt) {
		return errors.New("ACK before exact original held quota committed")
	}
	var queue, active uint32
	if err := db.QueryRowContext(ctx, "SELECT queue_depth,active_runs FROM tenant_scheduler_counters WHERE tenant_id=$1", tenant).Scan(&queue, &active); err != nil || queue != 0 || active != 0 {
		return fmt.Errorf("ACK before scheduling release queue=%d active=%d err=%v", queue, active, err)
	}
	for _, table := range []string{"quota_expiry_v2", "lease_heads"} {
		var count uint64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1", tenant).Scan(&count); err != nil || count != 0 {
			return fmt.Errorf("ACK retained %s count=%d err=%v", table, count, err)
		}
	}
	c := blobs.counts()
	if c.deletes < ordinal || c.absent != c.deletes {
		return errors.New("ACK before explicit original blob absence verification")
	}
	if _, _, _, _, err := localRuntimeRetired(ctx, db, tenant, job.Attempt.ID); err != nil {
		return err
	}
	return nil
}

type localRuntimeSnapshot struct {
	run, job, quota, parent, allocation, proxy, claim, lease string
	terminalStatus, contentDigest                            string
	finalizedAt                                              time.Time
	spending, publications                                   []string
}

func localRuntimeStoredSnapshot(t *testing.T, ctx context.Context, db *sql.DB, tenant domain.TenantID, job ports.WorkerJobState) localRuntimeSnapshot {
	t.Helper()
	var s localRuntimeSnapshot
	if err := db.QueryRowContext(ctx, "SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2", tenant, job.Run.ID).Scan(&s.run); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT payload FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2", tenant, job.Run.ID).Scan(&s.job); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT payload FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2", tenant, job.Reservation.ID).Scan(&s.quota); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT terminal_status,content_digest,created_at FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2", tenant, job.Run.ID).Scan(&s.terminalStatus, &s.contentDigest, &s.finalizedAt); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT owned_record FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3", tenant, job.Attempt.ID, domain.ProviderEffectTurnV1).Scan(&s.parent); err != nil {
		t.Fatal(err)
	}
	var err error
	s.allocation, s.proxy, s.claim, s.lease, err = localRuntimeRetired(ctx, db, tenant, job.Attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []struct {
		query       string
		destination *[]string
	}{
		{"SELECT record FROM attempt_spending_authorization_current_v1 WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY component_kind,billing_owner_user_id LIMIT 5", &s.spending},
		{"SELECT record FROM attempt_spending_publication_audit_v1 WHERE tenant_id=$1 AND attempt_id=$2 ORDER BY component_kind,billing_owner_user_id,mutation_id LIMIT 5", &s.publications},
	} {
		rows, err := db.QueryContext(ctx, read.query, tenant, job.Attempt.ID)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			*read.destination = append(*read.destination, raw)
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil || len(*read.destination) != 4 {
			t.Fatal("original four spending/publication records", err)
		}
	}
	return s
}

func localRuntimeRetired(ctx context.Context, db *sql.DB, tenant domain.TenantID, attempt domain.AttemptID) (allocationRaw, proxyRaw, claim, lease string, resultErr error) {
	if err := db.QueryRowContext(ctx, "SELECT allocation_current_record,proxy_current_record,physical_invocation_claim_id,lease_id FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3", tenant, attempt, domain.ProviderEffectTurnV1).Scan(&allocationRaw, &proxyRaw, &claim, &lease); err != nil {
		return "", "", "", "", err
	}
	allocation, err := domain.DecodeRegisteredAllocationV2([]byte(allocationRaw))
	if err != nil || (allocation.State != domain.RegisteredPreparationRetiredV1 && allocation.State != domain.RegisteredPreparationRevokedV1) {
		return "", "", "", "", errors.New("ACK before original allocation retirement")
	}
	proxy, err := domain.DecodeRegisteredProxyAttestationV2([]byte(proxyRaw))
	if err != nil || (proxy.State != domain.RegisteredPreparationRetiredV1 && proxy.State != domain.RegisteredPreparationRevokedV1) {
		return "", "", "", "", errors.New("ACK before original proxy retirement")
	}
	if allocation.Scope.TenantID != tenant || allocation.Scope.AttemptID != attempt || proxy.Scope != allocation.Scope || allocation.ParentReservation.PhysicalInvocationClaimID != claim || allocation.Lease.ID != domain.LeaseID(lease) || proxy.PhysicalInvocationClaimID != claim {
		return "", "", "", "", errors.New("original retirement identity mismatch")
	}
	return allocationRaw, proxyRaw, claim, lease, nil
}

func localRuntimeCleanup(t *testing.T, db *sql.DB, auth *localRuntimeAuthStore, tenants []domain.TenantID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// These are only newly generated tenant namespaces. There is no shared
	// marker reset, table drop, scan repair, profile/funding rewrite or user data.
	for _, tenant := range tenants {
		for _, table := range []string{
			"attempt_operation_proxies_v1", "attempt_operation_calls_v1", "attempt_operations_v1",
			"attempt_effect_reconciliation_evidence", "substrate_execution_evidence", "attempt_effect_reservations",
			"attempt_execution_admissions_by_session_v1", "attempt_execution_admissions_v1",
			"attempt_spending_publication_audit_v1", "attempt_spending_authorization_current_v1",
			"owned_profile_publication_audit_v1", "owned_agent_profile_current_v1",
			"run_finalizations", "run_explanation_heads_v1", "worker_jobs",
			"quota_expiry_v2", "quota_reservations", "subscription_scheduler_slots", "tenant_scheduler_counters",
			"dispatch_ready_v2", "dispatch_outbox", "usage_observations", "checkpoint_objects_by_run", "checkpoints",
			"artifact_manifests_by_run", "artifact_manifests", "lease_expiry_v2", "lease_heads", "leases",
			"attempts", "runs_by_session", "run_idempotency", "runs",
			"session_event_idempotency", "session_events", "session_material_boundaries_v1", "session_material_current_v1",
			"session_displays", "session_activity", "session_participants", "session_api_idempotency", "session_api_mutations", "sessions",
			"frontend_binding_keys", "frontend_bindings_by_session", "frontend_bindings", "frontend_ingress_idempotency",
			"frontend_projection_ready_v1", "frontend_projection_outbox", "frontend_projections_by_session", "frontend_projections_by_run",
			"audit_events", "development_bootstrap_grants", "tenant_memberships", "tenants",
		} {
			if _, err := db.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Errorf("owned tenant cleanup %s: %v", table, err)
			}
			switch table {
			case "substrate_execution_evidence", "attempt_execution_admissions_by_session_v1", "attempt_execution_admissions_v1":
				var remaining uint64
				if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE tenant_id=$1", tenant).Scan(&remaining); err != nil || remaining != 0 {
					t.Errorf("owned authority cleanup %s: remaining=%d err=%v", table, remaining, err)
				}
			}
		}
	}
	auth.mu.Lock()
	defer auth.mu.Unlock()
	for _, identity := range auth.identities {
		bucket, err := ydbpartition.BucketV1(identity.subject.String())
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3", bucket, identity.subject.Provider, identity.subject.Subject); err != nil {
			t.Error(err)
		}
		userBucket, err := ydbpartition.BucketV1(string(identity.user))
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4", userBucket, identity.user, identity.subject.Provider, identity.subject.Subject); err != nil {
			t.Error(err)
		}
	}
	for _, challenge := range auth.challenges {
		bucket, err := ydbpartition.BucketV1(string(challenge.StateDigest))
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2", bucket, challenge.StateDigest); err != nil {
			t.Error(err)
		}
	}
	for _, session := range auth.sessions {
		bucket, err := ydbpartition.BucketV1(string(session.SessionDigest))
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM web_sessions WHERE shard_bucket=$1 AND session_digest=$2", bucket, session.SessionDigest); err != nil {
			t.Error(err)
		}
	}
	for _, audit := range auth.audits {
		bucket, err := ydbpartition.BucketV1(audit.RequestID)
		if err != nil {
			t.Error(err)
			continue
		}
		if _, err := db.ExecContext(ctx, "DELETE FROM web_security_audit_events WHERE shard_bucket=$1 AND occurred_at=$2 AND request_id=$3", bucket, audit.OccurredAt, audit.RequestID); err != nil {
			t.Error(err)
		}
	}
}
