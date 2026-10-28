//go:build ydbintegration && (darwin || linux)

package attachedworkerstack_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	transport "gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkeronboarding"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkerreceipt"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/testkit"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"gitcode.com/urandon/sessionless/test/support/aw07native"
)

// The object peer stores bounded immutable bytes, not admission, credentials,
// a terminal declaration or a canonical-material producer. Production ingress,
// sealed-input and inline materialization own all those decisions.
type joinedGenerationBlobs struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (b *joinedGenerationBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, source io.Reader) (domain.BlobRef, error) {
	if ctx.Err() != nil || tenant.Validate() != nil || !strings.HasPrefix(key, "tenants/"+string(tenant)+"/") {
		return domain.BlobRef{}, errors.New("synthetic object scope denied")
	}
	raw, err := io.ReadAll(io.LimitReader(source, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 || ctx.Err() != nil {
		return domain.BlobRef{}, errors.New("synthetic object bound exceeded")
	}
	sum := sha256.Sum256(raw)
	b.mu.Lock()
	defer b.mu.Unlock()
	var retained int
	for _, object := range b.objects {
		retained += len(object)
	}
	if len(b.objects) >= 128 || retained+len(raw) > 8<<20 {
		return domain.BlobRef{}, errors.New("synthetic object aggregate bound exceeded")
	}
	if prior, found := b.objects[key]; found && !bytes.Equal(prior, raw) {
		return domain.BlobRef{}, errors.New("immutable object replacement denied")
	}
	b.objects[key] = bytes.Clone(raw)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(raw)), SHA256: hex.EncodeToString(sum[:])}, nil
}

func (b *joinedGenerationBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return nil, errors.New("synthetic object read denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	raw, found := b.objects[ref.Key]
	sum := sha256.Sum256(raw)
	if !found || int64(len(raw)) != ref.Size || hex.EncodeToString(sum[:]) != ref.SHA256 {
		return nil, errors.New("synthetic object absent or changed")
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(raw))), nil
}

func (b *joinedGenerationBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || tenant != ref.TenantID || ref.Validate() != nil {
		return errors.New("synthetic object deletion denied")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	clear(b.objects[ref.Key])
	delete(b.objects, ref.Key)
	return nil
}

// A controlled in-process wire, never a listener or replacement service. It
// records deadline controls required by the real Inline HTTP handler; request
// contexts still bound all DB/body operations. This does NOT qualify net/http's
// physical stream tail or a deployed ingress. Those have separate tests/gates.
type joinedGenerationWriter struct {
	*httptest.ResponseRecorder
	readDeadline, writeDeadline time.Time
	ctx                         context.Context
}

func (w *joinedGenerationWriter) SetReadDeadline(at time.Time) error {
	w.readDeadline = at
	return w.ctx.Err()
}
func (w *joinedGenerationWriter) SetWriteDeadline(at time.Time) error {
	w.writeDeadline = at
	return w.ctx.Err()
}
func (w *joinedGenerationWriter) FlushError() error {
	if w.readDeadline.IsZero() || w.writeDeadline.IsZero() {
		return errors.New("missing physical-control declaration")
	}
	w.ResponseRecorder.Flush()
	return w.ctx.Err()
}

type joinedGenerationWire struct {
	mu           sync.Mutex
	handler      http.Handler
	db           *sql.DB
	installation *attachedworkerstack.NativeGenerationJoinedInstallationV3ForTest
	paths        []string
	lostBodies   uint64
	sealed       []transport.SealedInputV1
	ready        []attachedworkeroutput.AttachedWorkerInlineResultV2
	acks         []attachedworkerprotocol.TerminalAckV1
	prepares     []attachedworkeroutput.AttachedWorkerInlinePrepareV2
	bodies       []attachedworkeroutput.AttachedWorkerInlineBodyV2
	reconciles   []attachedworkeroutput.AttachedWorkerInlineReconcileV2
	lostMetadata uint64
	traffic      chan struct{}
	fault        joinedGenerationFault
	gateRemoved  bool
	gateInline   http.Handler
	gateCalls    uint64
}

// A closed peer selector, never a production flag or supplied callback. The
// replacement handler is preconstructed from the SAME core/DB/body peer.
type joinedGenerationFault uint8

const (
	joinedGenerationNoFault joinedGenerationFault = iota
	joinedGenerationRemoveGateAfterAcceptedInput
)

func (w *joinedGenerationWire) removeGateAfterAcceptedInput() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fault != joinedGenerationNoFault || w.gateRemoved || len(w.sealed) != 0 || len(w.prepares) != 0 || w.gateInline == nil {
		return errors.New("joined gate fault cannot be armed")
	}
	w.fault = joinedGenerationRemoveGateAfterAcceptedInput
	return nil
}

func (w *joinedGenerationWire) sample(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sampleLocked(ctx)
}

func (w *joinedGenerationWire) sampleLocked(ctx context.Context) error {
	var at time.Time
	if err := w.db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		return err
	}
	return w.installation.ObserveDBTime(at)
}

func (w *joinedGenerationWire) RoundTrip(r *http.Request) (*http.Response, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	defer func() {
		select {
		case w.traffic <- struct{}{}:
		default:
		}
	}()
	if r.URL.Scheme != "https" || r.URL.Host != "generation-joined.invalid" || r.URL.RawQuery != "" || r.Context().Err() != nil {
		return nil, errors.New("joined fixed origin denied")
	}
	if err := w.sampleLocked(r.Context()); err != nil {
		return nil, err
	}
	copy := r.Clone(r.Context())
	var controlBody bytes.Buffer
	if r.URL.Path == attachedworkerreceipt.PathInlinePrepareV2 || r.URL.Path == attachedworkerreceipt.PathInlineReconcileV2 {
		if r.ContentLength < 0 || r.ContentLength > 256<<10 {
			return nil, errors.New("joined control capture bound exceeded")
		}
		copy.Body = io.NopCloser(io.TeeReader(r.Body, &controlBody))
	}
	// net/http's real transport materializes this framing field from
	// ContentLength; the controlled wire must do the same, never relax it.
	if copy.ContentLength >= 0 {
		copy.Header.Set("Content-Length", strconv.FormatInt(copy.ContentLength, 10))
	}
	w.paths = append(w.paths, r.URL.Path)
	response := &joinedGenerationWriter{ResponseRecorder: httptest.NewRecorder(), ctx: r.Context()}
	handler := w.handler
	if w.gateRemoved && (r.URL.Path == attachedworkerreceipt.PathInlinePrepareV2 || r.URL.Path == attachedworkerreceipt.PathInlineBodyV2 || r.URL.Path == attachedworkerreceipt.PathInlineReconcileV2) {
		if w.gateInline == nil {
			return nil, errors.New("withdrawn gate has no actual inline handler")
		}
		handler = w.gateInline
		w.gateCalls++
	}
	handler.ServeHTTP(response, copy)
	if r.Body != nil {
		_ = r.Body.Close()
	}
	if err := w.sampleLocked(r.Context()); err != nil {
		return nil, err
	}
	actual := response.Result()
	switch r.URL.Path {
	case attachedworkerreceipt.PathInlinePrepareV2:
		var prepare attachedworkeroutput.AttachedWorkerInlinePrepareV2
		if err := json.Unmarshal(controlBody.Bytes(), &prepare); err != nil {
			return nil, err
		}
		w.prepares = append(w.prepares, prepare)
	case attachedworkerreceipt.PathInlineBodyV2:
		// This header is the actual immutable wire plan, not body authority.
		var body attachedworkeroutput.AttachedWorkerInlineBodyV2
		encoded, err := base64.RawStdEncoding.Strict().DecodeString(r.Header.Get(attachedworkerreceipt.InlineRequestHeaderV2))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(encoded, &body); err != nil {
			return nil, err
		}
		w.bodies = append(w.bodies, body)
	case attachedworkerreceipt.PathInlineReconcileV2:
		var reconcile attachedworkeroutput.AttachedWorkerInlineReconcileV2
		if err := json.Unmarshal(controlBody.Bytes(), &reconcile); err != nil {
			return nil, err
		}
		w.reconciles = append(w.reconciles, reconcile)
	}
	if actual.StatusCode == http.StatusOK {
		switch r.URL.Path {
		case attachedworkersealedinput.PathV1:
			var input transport.SealedInputV1
			if err := json.Unmarshal(response.Body.Bytes(), &input); err != nil {
				return nil, err
			}
			w.sealed = append(w.sealed, input)
			if w.fault == joinedGenerationRemoveGateAfterAcceptedInput {
				if len(w.sealed) != 1 || input.AcceptedAttempt.RunID != input.Job.RunID || input.AcceptedAttempt.AttemptID != input.Job.AttemptID || input.AcceptedAttempt.State != domain.AttachedWorkerAttemptClaimed || input.Credential == nil {
					return nil, errors.New("gate cutover preceded genuine accepted input")
				}
				w.gateRemoved = true
			}
		case attachedworkerhttp.ExchangePathV1:
			var batch attachedworkerprotocol.BatchV1
			if err := json.Unmarshal(response.Body.Bytes(), &batch); err != nil {
				return nil, err
			}
			for _, frame := range batch.Frames {
				if frame.TerminalAck != nil {
					w.acks = append(w.acks, *frame.TerminalAck)
				}
			}
		case attachedworkerreceipt.PathInlineBodyV2:
			var result attachedworkeroutput.AttachedWorkerInlineResultV2
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				return nil, err
			}
			if result.Stage != attachedworkeroutput.InlineReadyV2 || result.CanonicalDigest.Validate() != nil {
				return nil, errors.New("body returned success without actual READY")
			}
			w.ready = append(w.ready, result)
		}
	}
	if r.URL.Path == attachedworkerhttp.ResourceGenerationPublishPathV1 && actual.StatusCode == http.StatusOK && w.lostMetadata == 0 {
		w.lostMetadata++
		_ = actual.Body.Close()
		return nil, errors.New("controlled publication ACK loss after actual commit")
	}
	if r.URL.Path == attachedworkerreceipt.PathInlineBodyV2 && actual.StatusCode == http.StatusOK {
		// The actual handler/store has already returned READY. Lose only the
		// response, never request bytes or the committed durable transition.
		w.lostBodies++
		_ = actual.Body.Close()
		return nil, errors.New("controlled READY response loss after actual commit")
	}
	return actual, nil
}

type joinedGenerationObservation struct {
	lostBodies, lostMetadata uint64
	inputs                   []transport.SealedInputV1
	ready                    []attachedworkeroutput.AttachedWorkerInlineResultV2
	acks                     []attachedworkerprotocol.TerminalAckV1
	prepares                 []attachedworkeroutput.AttachedWorkerInlinePrepareV2
	bodies                   []attachedworkeroutput.AttachedWorkerInlineBodyV2
	reconciles               []attachedworkeroutput.AttachedWorkerInlineReconcileV2
	paths                    []string
	gateRemoved              bool
	gateCalls                uint64
}

func (w *joinedGenerationWire) committedSnapshot() joinedGenerationObservation {
	w.mu.Lock()
	defer w.mu.Unlock()
	return joinedGenerationObservation{lostBodies: w.lostBodies, lostMetadata: w.lostMetadata, inputs: append([]transport.SealedInputV1(nil), w.sealed...), ready: append([]attachedworkeroutput.AttachedWorkerInlineResultV2(nil), w.ready...), acks: append([]attachedworkerprotocol.TerminalAckV1(nil), w.acks...), prepares: append([]attachedworkeroutput.AttachedWorkerInlinePrepareV2(nil), w.prepares...), bodies: append([]attachedworkeroutput.AttachedWorkerInlineBodyV2(nil), w.bodies...), reconciles: append([]attachedworkeroutput.AttachedWorkerInlineReconcileV2(nil), w.reconciles...), paths: append([]string(nil), w.paths...), gateRemoved: w.gateRemoved, gateCalls: w.gateCalls}
}

// The factory only constructs the original public exchange client from the
// borrowed connection bearer; it cannot supply a frame, grant or fake Session.
type joinedGenerationExchange struct{ client *http.Client }

func (f joinedGenerationExchange) Open(_ attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	return attachedworkerhttp.NewClientFromBearerBytes(attachedworkerhttp.ClientBytesConfig{BaseURL: "https://generation-joined.invalid", Bearer: bearer, HTTPClient: f.client, RequestTimeout: 10 * time.Second})
}

type joinedGenerationScenario struct {
	t                *testing.T
	ctx              context.Context
	db               *sql.DB
	store            *ydbstore.Store
	worker           domain.AttachedWorker
	resource         domain.SubscriptionConnectionID
	baseline         domain.AttachedResourceGenerationBaselineV1
	installation     *attachedworkerstack.NativeGenerationJoinedInstallationV3ForTest
	wire             *joinedGenerationWire
	session          *attachedworkersession.Session
	stack            *attachedworkerstack.Stack
	adapter          *transport.Adapter
	localRoot        string
	ingress          *sessioningress.Service
	blobs            *joinedGenerationBlobs
	credentialStore  *ownerlocalcredential.Store
	metadataBefore   joinedGenerationResourceMetadata
	gateRemovedStore *ydbstore.Store
}

type joinedGenerationResourceMetadata struct {
	provider, credential, entitlement, quota string
	observed                                 time.Time
}

func joinedGenerationReadResourceMetadata(ctx context.Context, db *sql.DB, tenant domain.TenantID, resource domain.SubscriptionConnectionID) (joinedGenerationResourceMetadata, error) {
	var value joinedGenerationResourceMetadata
	err := db.QueryRowContext(ctx, `SELECT provider,credential_ref,entitlement_state,quota_state,observed_at FROM subscription_connections WHERE tenant_id=$1 AND subscription_connection_id=$2`, tenant, resource).Scan(&value.provider, &value.credential, &value.entitlement, &value.quota, &value.observed)
	value.observed = value.observed.UTC()
	return value, err
}

func newJoinedGenerationScenario(t *testing.T) *joinedGenerationScenario {
	t.Helper()
	// SKIP before DSN/auth/file reads. Local YDB is not authorized, even if a
	// developer happens to have a disposable daemon on loopback.
	if os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_GATE") != "1" {
		t.Skip("explicit remote disposable joined generation gate not requested")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("SESSIONLESS_OWNED_CURRENT_YDB_DISPOSABLE") != "1" {
		t.Fatal("joined generation proof requires ordinary disposable remote CI")
	}
	u, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil || u == nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("joined generation gate requires disposable loopback DB")
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil || len(q["go_query_mode"]) != 1 || q.Get("go_query_mode") != "data" || q.Has("go_fake_tx") {
		t.Fatal("joined generation proof forbids fake/scripting transactions")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	t.Cleanup(cancel)
	client, err := ydbclient.Open(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if err := client.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	store, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var at time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	at = at.UTC()
	suffix := aw07native.UniqueSuffix(t, "generation-joined")
	tenant, owner := domain.TenantID("tenant-"+suffix), domain.UserID("owner-"+suffix)
	// Every deletion below is confined to this fresh, generated tenant and
	// pre-armed before its first write, including all canonical-ingress indexes.
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		for _, table := range []string{
			"attached_worker_output_receipts", "attached_worker_attempt_messages", "attached_worker_attempt_heads", "attached_worker_attempt_deadlines_v1", "attached_worker_control_messages", "attached_worker_presence_expiry_v1", "attached_worker_capability_manifests", "attached_worker_attach_challenges", "attached_worker_connections", "attached_worker_audit_events", "attached_worker_enrollments", "attached_workers",
			"audit_events", "run_finalizations", "run_explanation_heads_v1", "worker_jobs", "quota_expiry_v2", "quota_reservations", "subscription_scheduler_slots", "tenant_scheduler_counters", "dispatch_ready_v2", "dispatch_outbox", "artifact_manifests_by_run", "artifact_manifests", "lease_expiry_v2", "lease_heads", "leases", "attempts", "runs_by_session", "run_idempotency", "runs", "subscription_connections_by_user", "subscription_connections", "actors", "session_event_idempotency", "session_events", "session_material_boundaries_v1", "session_material_current_v1", "session_displays", "session_activity", "session_participants", "sessions", "frontend_binding_keys", "frontend_bindings_by_session", "frontend_bindings", "frontend_ingress_idempotency", "frontend_projection_ready_v1", "frontend_projection_outbox", "frontend_projections_by_session", "frontend_projections_by_run", "tenant_memberships", "tenants",
		} {
			if _, err := client.DB.ExecContext(cleanup, "DELETE FROM "+table+" WHERE tenant_id=$1", tenant); err != nil {
				t.Errorf("exact synthetic tenant cleanup %s: %v", table, err)
			}
		}
	})
	aw07native.SeedMembership(t, ctx, client.DB, tenant, owner, at)
	enrollment, audit := aw07native.Enrollment(suffix, tenant, owner, at)
	enrollment.ExpiresAt = at.Add(20 * time.Minute)
	if err := store.CreateAttachedWorkerEnrollment(ctx, enrollment, audit); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x57}, ed25519.SeedSize))
	claim := aw07native.EnrollmentClaim(enrollment, 0x57)
	claim.IdentityPublicKey = bytes.Clone(private.Public().(ed25519.PublicKey))
	claimed, err := store.ClaimAttachedWorkerEnrollment(ctx, claim)
	if err != nil || claimed.Status != ports.AttachedWorkerClaimed {
		t.Fatalf("actual enrollment claim: status=%s err=%v", claimed.Status, err)
	}
	worker := claimed.Worker
	resource := attachedworkeronboarding.ResourceIDForWorker(tenant, owner, worker.ID)
	registered, err := store.OnboardingStore().RegisterAttachedComputeResource(ctx, ports.AttachedComputeRegistration{Worker: worker, ResourceID: resource, ActorID: attachedworkeronboarding.ActorIDForOwner(tenant, owner)})
	if err != nil || registered.Entitlement != domain.EntitlementUnknown || registered.Quota != domain.ProviderQuotaUnknown {
		t.Fatalf("normal unknown registration err=%v", err)
	}
	// Separate explicit fixture authority, NOT a property granted by metadata
	// or local auth. Publication must preserve these observations unchanged.
	if _, err := client.DB.ExecContext(ctx, `UPDATE subscription_connections SET entitlement_state=$1,quota_state=$2 WHERE tenant_id=$3 AND subscription_connection_id=$4`, domain.EntitlementActive, domain.ProviderQuotaAvailable, tenant, resource); err != nil {
		t.Fatal(err)
	}
	metadataBefore, err := joinedGenerationReadResourceMetadata(ctx, client.DB, tenant, resource)
	if err != nil || metadataBefore.provider != "codex" || metadataBefore.credential != "" || metadataBefore.entitlement != string(domain.EntitlementActive) || metadataBefore.quota != string(domain.ProviderQuotaAvailable) {
		t.Fatalf("independent operational fixture authority invalid: %v", err)
	}
	installation, err := attachedworkerstack.NewNativeGenerationJoinedInstallationV3ForTest(t, at)
	if err != nil {
		t.Fatal(err)
	}
	settings := installation.Settings()
	capability := aw07native.CapabilityManifest(worker, attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}})
	capability.OperatingSystem, capability.Architecture, capability.HarnessVersion = settings.Profile.OperatingSystem, settings.Profile.Architecture, settings.Profile.Backend.ExecutableVersion
	capability.HarnessName = "codex"
	capability.HarnessExecutableDigest = bytes.Clone(settings.Profile.Backend.ExecutableDigest[:])
	capability.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureInlineOutputV2, attachedworkerprotocol.FeatureOutputReceipt, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}
	baseline := joinedGenerationBaseline(t, worker, resource, capability, settings.Profile, at)
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{baseline})
	if err != nil {
		t.Fatal(err)
	}
	store, err = ydbstore.New(client.DB, ydbstore.Options{AttachedResourceGenerations: registry})
	if err != nil {
		t.Fatal(err)
	}
	blobs := &joinedGenerationBlobs{objects: make(map[string][]byte)}
	core, err := attachedworkertransport.NewReceiptFinalizingService(attachedworkertransport.ServiceConfig{IDs: testkit.NewSequenceIDGenerator(suffix), Audience: enrollment.Audience, PlatformOffer: capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: time.Minute, ChallengeRetention: time.Hour, PresenceTTL: 10 * time.Minute, AuthTTL: time.Hour, CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := attachedworkerhttp.NewBootstrapHandler(core)
	if err != nil {
		t.Fatal(err)
	}
	exchangeCore, err := attachedworkerhttp.NewCoreExchangeAdapter(core)
	if err != nil {
		t.Fatal(err)
	}
	exchange, err := attachedworkerhttp.NewHandler(exchangeCore)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := attachedworkerhttp.NewResourceGenerationHandlerV1(core)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := attachedworkersealedinput.NewNativeSubscriptionServiceV1(core, store, blobs, installation.Now)
	if err != nil {
		t.Fatal(err)
	}
	v1, err := attachedworkerreceipt.NewService(core, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	inline, err := attachedworkerreceipt.NewInlineServiceV2(core, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	gateRemovedStore, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil || gateRemovedStore.AttachedResourceGenerationEnabledV1() {
		t.Fatalf("withdrawn gate construction: %v", err)
	}
	gateRemovedInline, err := attachedworkerreceipt.NewInlineServiceV2(core, gateRemovedStore, blobs)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(attachedworkerhttp.ChallengePathV1, bootstrap)
	mux.Handle(attachedworkerhttp.AttachPathV1, bootstrap)
	mux.Handle(attachedworkerhttp.ExchangePathV1, exchange)
	// Register by fixed production paths; no direct Service call substitutes
	// for authenticated/framed requests made by the original bearer clients.
	mux.Handle("/attached-worker/v1/resource-generation/", metadata)
	mux.Handle(attachedworkersealedinput.PathV1, attachedworkersealedinput.Handler(sealed))
	mux.Handle(attachedworkerreceipt.PathV1, attachedworkerreceipt.Handler(v1))
	for _, path := range []string{attachedworkerreceipt.PathInlinePrepareV2, attachedworkerreceipt.PathInlineBodyV2, attachedworkerreceipt.PathInlineReconcileV2} {
		mux.Handle(path, attachedworkerreceipt.HandlerInlineV2(inline))
	}
	wire := &joinedGenerationWire{handler: mux, db: client.DB, installation: installation, traffic: make(chan struct{}, 1), gateInline: attachedworkerreceipt.HandlerInlineV2(gateRemovedInline)}
	httpClient := &http.Client{Transport: wire}
	source, err := attachedworkersealedinput.NewInlineSessionSourceFactoryV2("https://generation-joined.invalid", httpClient, joinedGenerationExchange{client: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	ownerRoot := filepath.Join(t.TempDir(), "owner")
	credentialStore, err := ownerlocalcredential.New(ctx, ownerlocalcredential.Config{Root: ownerRoot, Scope: ownerlocalcredential.Scope{TenantID: tenant, OwnerUserID: owner, SubscriptionConnectionID: resource}, KeyEpoch: "synthetic-joined-only"}, bytes.Repeat([]byte{0x63}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := credentialStore.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := credentialStore.Initialize(ctx, ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: at}, []byte(`{"synthetic_generation_joined":"original"}`)); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := ownerlocalcredential.NewLifecycle(credentialStore, credentiallifecycle.Config{ScratchRoot: settings.CredentialScratchRoot, Clock: installation, IDs: testkit.NewSequenceIDGenerator("joined-credential-"), MaxAuthBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := lifecycle.Close(); err != nil {
			t.Error(err)
		}
	})
	metadataOwner, err := attachedresourcegenerationsession.NewV1("https://generation-joined.invalid", httpClient, source, lifecycle, baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := metadataOwner.Close(); err != nil {
			t.Error(err)
		}
	})
	localRoot := filepath.Join(t.TempDir(), "worker")
	local, err := attachedworkerlocal.NewStore(localRoot, installation.Now)
	if err != nil {
		t.Fatal(err)
	}
	manifest := attachedworkerlocal.ManifestV1{Version: 1, Revision: 1, ControlPlaneOrigin: "https://generation-joined.invalid", TenantID: tenant, OwnerUserID: owner, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration, IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(private.Public().(ed25519.PublicKey))), OCI: settings.OCI, Harness: settings.Harness, Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: at, UpdatedAt: at}
	secret := attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: 1, TenantID: tenant, OwnerUserID: owner, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: worker.ConnectionGeneration, IdentityPrivateKey: bytes.Clone(private)}
	if err := local.Initialize(ctx, manifest, secret); err != nil {
		t.Fatal(err)
	}
	bootClient, err := attachedworkerhttp.NewBootstrapClient(attachedworkerhttp.BootstrapClientConfig{BaseURL: manifest.ControlPlaneOrigin, HTTPClient: httpClient, RequestTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var stack *attachedworkerstack.Stack
	connector, err := attachedworkersession.New(local, bootClient, metadataOwner, attachedworkersession.Config{Audience: enrollment.Audience, WorkerOffer: capability.ProtocolOffer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, OperationTimeout: 15 * time.Second, Now: installation.Now, RuntimePreflight: func(ctx context.Context, m attachedworkerlocal.ManifestV1) error {
		var err error
		stack, err = installation.BuildStack(ctx, m, baseline, metadataOwner, lifecycle)
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := connector.Connect(ctx, attachedworkersession.ConnectInputV1{ExpectedWorkerRevision: worker.Revision, CapabilityManifest: capability})
	if err != nil {
		t.Fatal("genuine Connector", err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if err := session.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	materializer, err := transport.NewNativeSubscriptionBoundMaterializerV1(source, 1<<20, installation.Now)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := transport.NewNativeGenerationPreparedAdapterV3(ctx, session, materializer, transport.Config{Profile: transport.LocalProfileV1{Name: "joined-generation", CapabilityDigest: baseline.CapabilityDigest, Executable: settings.Harness.Executable, ExecutableDigest: settings.Profile.Backend.ExecutableDigest, Arguments: settings.Harness.Arguments}, CapabilityManifest: &capability, ReceiptPublisher: source, InlineReceiptPublisher: source, MaterializationRoot: t.TempDir(), MaxInputBytes: 1 << 20, Now: installation.Now}, stack, attachedworkersession.ResourceGenerationInitialV1)
	if err != nil {
		t.Fatal("initial metadata ACK and adapter", err)
	}
	binder, err := sessionlessharness.NewAttachedResourceBinder(sessionlessharness.AttachedResourceBinderConfig{Enabled: true, Generations: registry}, store)
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := sessioningress.New(sessioningress.Config{IDKey: bytes.Repeat([]byte{0x38}, 32), HarnessBinder: binder}, store, blobs)
	if err != nil {
		t.Fatal(err)
	}
	metadataAfter, err := joinedGenerationReadResourceMetadata(ctx, client.DB, tenant, resource)
	if err != nil || metadataAfter != metadataBefore {
		t.Fatalf("initial publication changed independent operational observation: %v", err)
	}
	return &joinedGenerationScenario{t: t, ctx: ctx, db: client.DB, store: store, worker: worker, resource: resource, baseline: baseline, installation: installation, wire: wire, session: session, stack: stack, adapter: adapter, localRoot: localRoot, ingress: ingress, blobs: blobs, credentialStore: credentialStore, metadataBefore: metadataBefore, gateRemovedStore: gateRemovedStore}
}

func joinedGenerationBaseline(t *testing.T, worker domain.AttachedWorker, resource domain.SubscriptionConnectionID, capability attachedworkerprotocol.CapabilityManifestV1, profile codexexec.NativeProfileV3, at time.Time) domain.AttachedResourceGenerationBaselineV1 {
	t.Helper()
	descriptor, err := profile.DescriptorV3()
	if err != nil {
		t.Fatal(err)
	}
	capDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	observed, expires := at.Add(-time.Minute), at.Add(time.Hour)
	binding := domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, Backend: descriptor, Resource: domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: string(resource), OwnerUserID: worker.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}, ModelVendorID: "openai", ModelID: profile.Backend.Model, InputDataClass: domain.ProviderDataPrivateV1, EvidenceExpiresAt: &expires}
	scope := domain.ProviderEvidenceScopeV1{TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, Resource: binding.Resource, Backend: binding.Backend}
	bundle := domain.ProviderEvidenceBundleV1{
		Catalog:    domain.ProviderCatalogObservationV1{Version: 1, Scope: scope, CatalogRevision: "synthetic-joined-catalog", Models: []domain.ProviderCatalogModelV1{{ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, CatalogDigest: strings.Repeat("a", 64)}}, ObservedAt: observed, ExpiresAt: expires},
		Route:      domain.ProviderRoutePolicyV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, FallbackPolicy: domain.ProviderFallbackDenyV1, ObservedAt: observed, ExpiresAt: expires},
		Capability: domain.ProviderCapabilityEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, MaxContextTokens: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, ToolCalling: domain.ProviderEvidenceUnsupportedV1, StructuredOutput: domain.ProviderEvidenceUnknownV1, ObservedAt: observed, ExpiresAt: expires},
		Privacy:    domain.ProviderPrivacyEvidenceV1{Version: 1, Scope: scope, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, AllowedDataClasses: []domain.ProviderDataClassV1{binding.InputDataClass}, TrainingUse: domain.ProviderEvidenceUnknownV1, RetentionHours: domain.ProviderEvidenceQuantityV1{State: domain.ProviderEvidenceUnknownV1}, PolicyRevision: "synthetic-joined-privacy", ObservedAt: observed, ExpiresAt: expires},
		Price:      domain.ProviderPriceObservationV1{Version: 1, Scope: scope, State: domain.ProviderEvidenceUnknownV1, ModelVendorID: binding.ModelVendorID, ModelID: binding.ModelID, ObservedAt: observed, ExpiresAt: expires},
		Policy:     domain.ProviderPolicyEvidenceV1{Version: 1, Scope: scope, PolicyID: codexexec.NativeEnvelopePolicyIDV3, Revision: codexexec.NativeEnvelopePolicyRevisionV3, DecisionOwner: "sessionless-policy", EvidenceSource: "reviewed-native-envelope-v3", Verdict: domain.ProviderPolicyConditionalV1, AllowedDataClasses: []domain.ProviderDataClassV1{binding.InputDataClass}, ObservedAt: observed, ExpiresAt: expires},
	}
	digest := func(v interface {
		Digest() (domain.ProviderEvidenceDigestV1, error)
	}) string {
		value, err := v.Digest()
		if err != nil {
			t.Fatal(err)
		}
		return string(value)
	}
	binding.ProviderCatalogDigest = digest(bundle.Catalog)
	binding.ProviderRouteDigest = digest(bundle.Route)
	binding.CapabilityEvidenceDigest = digest(bundle.Capability)
	binding.PrivacyPolicyDigest = digest(bundle.Privacy)
	bundle.Policy.RoutePolicyDigest = binding.ProviderRouteDigest
	bundle.Policy.CapabilityEvidenceDigest = binding.CapabilityEvidenceDigest
	bundle.Policy.PrivacyEvidenceDigest = binding.PrivacyPolicyDigest
	bundle.Policy.PriceObservationDigest = digest(bundle.Price)
	binding.EffectivePolicyDigest = digest(bundle.Policy)
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, SubscriptionConnectionID: resource, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, BaselineRevision: 1, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capDigest)), GenerationOnlyDelegation: true, Binding: binding, Evidence: bundle}
	if err := baseline.Validate(); err != nil {
		t.Fatal(err)
	}
	return baseline
}

func (f *joinedGenerationScenario) canonicalOutbox(generation uint64) domain.DispatchOutbox {
	f.t.Helper()
	if err := f.wire.sample(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	var pngBody, jpegBody bytes.Buffer
	if err := png.Encode(&pngBody, image.NewRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		f.t.Fatal(err)
	}
	if err := jpeg.Encode(&jpegBody, image.NewRGBA(image.Rect(0, 0, 1, 1)), &jpeg.Options{Quality: 80}); err != nil {
		f.t.Fatal(err)
	}
	result, err := f.ingress.Ingest(f.ctx, sessioningress.UserInput{Actor: sessioningress.Actor{TenantID: f.worker.TenantID, UserID: f.worker.OwnerUserID, Frontend: domain.FrontendWeb, ExternalConversationID: "joined-owned-conversation"}, ExternalEventID: fmt.Sprintf("generation-%d", generation), ReceivedAt: f.installation.Now(), Text: "Return the two original requested files", Attachments: []sessioningress.Attachment{
		{Name: "request.txt", MediaType: "text/plain", Body: strings.NewReader("original bounded request")},
		{Name: "request.json", MediaType: "application/json", Body: strings.NewReader(` { "n": 9007199254740993123456789 } `)},
		{Name: "request.png", MediaType: "image/png", Body: bytes.NewReader(pngBody.Bytes())},
		{Name: "request.jpg", MediaType: "image/jpeg", Body: bytes.NewReader(jpegBody.Bytes())},
	}, SubscriptionConnectionID: f.resource})
	if err != nil || !result.Created {
		f.t.Fatalf("canonical generation%d ingress: created=%t err=%v", generation, result.Created, err)
	}
	var raw string
	if err := f.db.QueryRowContext(f.ctx, `SELECT payload FROM dispatch_outbox WHERE tenant_id=$1 AND run_id=$2`, f.worker.TenantID, result.RunID).Scan(&raw); err != nil {
		f.t.Fatal(err)
	}
	var outbox domain.DispatchOutbox
	if err := json.Unmarshal([]byte(raw), &outbox); err != nil {
		f.t.Fatal(err)
	}
	static, staticErr := f.installation.Settings().Profile.InstallationPolicyDigestV3()
	if staticErr != nil || outbox.HarnessBinding.Resource.CredentialGeneration != generation || outbox.HarnessBinding.EffectivePolicyDigest == string(static) {
		f.t.Fatal("canonical binder substituted generation or static policy")
	}
	return outbox
}

func (f *joinedGenerationScenario) admitOutbox(store *ydbstore.Store, outbox domain.DispatchOutbox, generation uint64) (ports.DispatchAdmissionResult, error) {
	f.t.Helper()
	if err := f.wire.sample(f.ctx); err != nil {
		f.t.Fatal(err)
	}
	at := f.installation.Now()
	return store.AdmitDispatch(f.ctx, ports.DispatchAdmissionRequest{TenantID: f.worker.TenantID, OutboxID: outbox.ID, RunID: outbox.RunID, AttemptID: outbox.AttemptID, ReservationID: domain.QuotaReservationID(fmt.Sprintf("joined-reservation-%d", generation)), Now: at, HoldUntil: at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 16, MaxActiveRuns: 2, MaxRuntime: 30 * time.Second, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 16, MaxArtifacts: 16, MaxToolEvents: 1, MaxToolEventBytes: 1024}, Workload: domain.WorkloadShape{Runtime: 30 * time.Second, Turns: 1, InputBytes: 4096, ContextBytes: 4096, Artifacts: 5}})
}

func (f *joinedGenerationScenario) ingest(generation uint64) ports.WorkerJobState {
	f.t.Helper()
	outbox := f.canonicalOutbox(generation)
	admitted, err := f.admitOutbox(f.store, outbox, generation)
	if err != nil || !admitted.Admitted || admitted.Delivery != ports.DispatchDeliveryAttachedOffer {
		f.t.Fatalf("actual same-Tx admission generation%d: admitted=%t code=%s err=%v", generation, admitted.Admitted, admitted.Code, err)
	}
	loaded, found, err := f.store.LoadWorkerJob(f.ctx, f.worker.TenantID, outbox.RunID)
	if err != nil || !found {
		f.t.Fatalf("actual generated job found=%t err=%v", found, err)
	}
	return loaded
}

// Read the canonical result, not just the daemon's counters or a synthetic
// inline declaration. Compare exact original typed bytes through BlobRefs.
func (f *joinedGenerationScenario) assertCanonicalResult(original ports.WorkerJobState, ordinal, total int) {
	f.t.Helper()
	if _, err := os.Stat(filepath.Join(f.localRoot, attachedworkerlocal.ReceiptCheckpointFileName)); !errors.Is(err, os.ErrNotExist) {
		f.t.Fatalf("exact terminal ACK did not retire original journal: %v", err)
	}
	seen := f.wire.committedSnapshot()
	if seen.lostBodies != uint64(total) || len(seen.inputs) != total || len(seen.ready) != total || len(seen.acks) != total || len(seen.prepares) != total || len(seen.bodies) != total || len(seen.reconciles) != total {
		f.t.Fatalf("missing genuine input/READY/lostreply/ACK or repeated export: inputs=%d READY=%d ACK=%d Prepare=%d Body=%d Reconcile=%d want%d", len(seen.inputs), len(seen.ready), len(seen.acks), len(seen.prepares), len(seen.bodies), len(seen.reconciles), total)
	}
	input, ack := seen.inputs[ordinal-1], seen.acks[ordinal-1]
	if !reflect.DeepEqual(input.Job, original.Job) || input.Credential == nil || input.CanonicalContext == nil || input.Job.HarnessBinding.Resource.CredentialGeneration != uint64(ordinal) || input.AcceptedAttempt.RunID != original.Job.RunID || ack.Binding.RunID != string(original.Job.RunID) || ack.Binding.AttemptID != string(original.Job.AttemptID) || ack.Status != attachedworkerprotocol.TerminalSucceeded || hex.EncodeToString(ack.EvidenceDigest) != string(seen.ready[ordinal-1].CanonicalDigest) {
		f.t.Fatal("original input/producer/terminal ACK correspondence changed")
	}
	prepare, inlineBody, reconcile, ready := seen.prepares[ordinal-1], seen.bodies[ordinal-1], seen.reconciles[ordinal-1], seen.ready[ordinal-1]
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(prepare)
	harness, harnessErr := input.Job.HarnessBinding.Digest()
	if err != nil || harnessErr != nil || prepare.Nonce != inlineBody.Nonce || prepare.Nonce != reconcile.Nonce || prepare.Nonce != ready.Nonce || inlineBody.Fingerprint != fingerprint || reconcile.Fingerprint != fingerprint || ready.Fingerprint != fingerprint || inlineBody.Authorization != prepare.Authorization || reconcile.Authorization != prepare.Authorization || !inlineBody.ExpiresAt.Equal(prepare.ExpiresAt) || !ready.ExpiresAt.Equal(prepare.ExpiresAt) || prepare.Producer.HarnessBindingDigest != harness || prepare.Producer.SessionID != input.Job.SessionID || prepare.Producer.ReservationID != input.Job.ReservationID || !prepare.Producer.AcceptedAt.Equal(input.AcceptedAttempt.CreatedAt) {
		f.t.Fatal("original producer/nonce/fingerprint/deadline was replaced across loss")
	}
	for _, path := range []string{attachedworkerreceipt.PathInlinePrepareV2, attachedworkerreceipt.PathInlineBodyV2, attachedworkerreceipt.PathInlineReconcileV2} {
		count := 0
		for _, observed := range seen.paths {
			if path == observed {
				count++
			}
		}
		if count != total {
			f.t.Fatal("extra denied or ambiguous physical export request")
		}
	}
	projected, refs, err := sessioncontext.ProjectCanonical(input.Job, input.CanonicalContext, input.Job.Limits.MaxContextBytes)
	if err != nil || !bytes.Equal(projected, input.Context) || !bytes.Contains(projected, []byte("Return the two original requested files")) || len(refs) != 4*ordinal || len(input.CanonicalContext.Attachments) != 4*ordinal {
		f.t.Fatalf("canonical text/history/media projection lost: refs=%d ordinal=%d err=%v", len(refs), ordinal, err)
	}
	var originalPNG, originalJPEG bytes.Buffer
	if err := png.Encode(&originalPNG, image.NewRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		f.t.Fatal(err)
	}
	if err := jpeg.Encode(&originalJPEG, image.NewRGBA(image.Rect(0, 0, 1, 1)), &jpeg.Options{Quality: 80}); err != nil {
		f.t.Fatal(err)
	}
	for i, attachment := range input.CanonicalContext.Attachments {
		name, media := "", ""
		switch i % 4 {
		case 0:
			name, media = "request.txt", "text/plain"
		case 1:
			name, media = "request.json", "application/json"
		case 2:
			name, media = "request.png", "image/png"
		case 3:
			name, media = "request.jpg", "image/jpeg"
		}
		if attachment.Name != name || attachment.MediaType != media {
			f.t.Fatal("canonical original attachment name/media changed")
		}
		sum := sha256.Sum256(attachment.Body)
		if attachment.Blob.Size != int64(len(attachment.Body)) || attachment.Blob.SHA256 != hex.EncodeToString(sum[:]) || refs[i] != attachment.AttachmentRef {
			f.t.Fatal("canonical original attachment hash/bytes changed")
		}
		if i%4 == 0 && string(attachment.Body) != "original bounded request" || i%4 == 1 && string(attachment.Body) != ` { "n": 9007199254740993123456789 } ` {
			f.t.Fatal("canonical original text/JSON normalized")
		}
		if i%4 == 2 {
			if _, err := png.Decode(bytes.NewReader(attachment.Body)); err != nil {
				f.t.Fatal("original PNG missing", err)
			}
		}
		if i%4 == 3 {
			if _, err := jpeg.Decode(bytes.NewReader(attachment.Body)); err != nil {
				f.t.Fatal("original JPEG missing", err)
			}
		}
		if i%4 == 2 && !bytes.Equal(attachment.Body, originalPNG.Bytes()) || i%4 == 3 && !bytes.Equal(attachment.Body, originalJPEG.Bytes()) {
			f.t.Fatal("original image encoding bytes changed")
		}
	}
	var raw string
	if err := f.db.QueryRowContext(f.ctx, `SELECT record FROM session_events WHERE tenant_id=$1 AND session_id=$2 AND run_id=$3 AND kind=$4`, f.worker.TenantID, original.Job.SessionID, original.Job.RunID, domain.SessionEventAssistantMessage).Scan(&raw); err != nil {
		f.t.Fatal(err)
	}
	var event domain.SessionEvent
	if err := json.Unmarshal([]byte(raw), &event); err != nil {
		f.t.Fatal(err)
	}
	r, err := f.blobs.Open(f.ctx, f.worker.TenantID, event.Payload)
	if err != nil {
		f.t.Fatal(err)
	}
	body, err := io.ReadAll(io.LimitReader(r, 4097))
	closeErr := r.Close()
	if err != nil || closeErr != nil || len(body) > 4096 {
		f.t.Fatal("canonical answer bytes unavailable")
	}
	var answer struct {
		Schema     string                    `json:"schema"`
		Summary    string                    `json:"summary"`
		ManifestID domain.ArtifactManifestID `json:"artifact_manifest_id"`
	}
	if json.Unmarshal(body, &answer) != nil || answer.Schema != "sessionless.assistant-message.v1" || answer.Summary != "two requested files" {
		f.t.Fatal("original canonical answer changed")
	}
	manifest, found, err := f.store.GetArtifactManifest(f.ctx, f.worker.TenantID, answer.ManifestID)
	if err != nil || !found || len(manifest.Artifacts) != 2 || manifest.RunID != original.Job.RunID {
		f.t.Fatalf("actual durable typed manifest: found=%t err=%v", found, err)
	}
	for _, artifact := range manifest.Artifacts {
		wantBody, wantMedia := "", ""
		switch artifact.Name {
		case "data.json":
			wantBody, wantMedia = ` { "n": 9007199254740993123456789 } `, "application/json"
		case "note.txt":
			wantBody, wantMedia = "original text\n", "text/plain"
		default:
			f.t.Fatal("unexpected durable original output name")
		}
		r, err := f.blobs.Open(f.ctx, f.worker.TenantID, artifact.Blob)
		if err != nil {
			f.t.Fatal(err)
		}
		raw, err := io.ReadAll(io.LimitReader(r, 4097))
		closeErr := r.Close()
		if err != nil || closeErr != nil || string(raw) != wantBody || artifact.MediaType != wantMedia {
			f.t.Fatal("original typed output bytes/media changed")
		}
	}
}

// Local Committed is not the platform's durable retirement. Only the next
// ORIGINAL fresh poll conveys the worker's ACK; read its resulting exact head.
// Traffic notifications are observations, never an alternate protocol action.
func (f *joinedGenerationScenario) waitRetired(original ports.WorkerJobState, done <-chan struct{}) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	defer cancel()
	for {
		attempt, found, err := f.store.LoadAttachedWorkerAttempt(ctx, f.worker.TenantID, f.worker.OwnerUserID, f.worker.ID)
		if err != nil || !found {
			f.t.Fatalf("original retirement head found=%t err=%v", found, err)
		}
		if attempt.RunID != original.Job.RunID || attempt.AttemptID != original.Job.AttemptID {
			f.t.Fatal("retirement observed a different attempt")
		}
		if attempt.State == domain.AttachedWorkerAttemptRetired {
			return
		}
		if attempt.State != domain.AttachedWorkerAttemptTerminalCommitted {
			f.t.Fatalf("unexpected original custody state %s", attempt.State)
		}
		select {
		case <-f.wire.traffic:
		case <-done:
			f.t.Fatal("daemon stopped before original durable retirement")
		case <-ctx.Done():
			f.t.Fatal("original durable retirement deadline", ctx.Err())
		}
	}
}

// WIP acceptance target: actual two canonical jobs, not two hand-built DTOs.
// Tagged compilation/skip locally is NOT evidence this remote-only test passed.
func TestAttachedResourceGenerationYDBJoinedTwoCanonicalJobs(t *testing.T) {
	f := newJoinedGenerationScenario(t)
	originalBaseline := f.baseline.Clone()
	first := f.ingest(1)
	daemon, err := attachedworkerdaemon.NewDaemon(attachedworkerdaemon.DaemonConfig{IdleBackoff: 10 * time.Millisecond, ShutdownGrace: 5 * time.Second, ReportGrace: 10 * time.Second}, f.adapter, f.stack, f.adapter)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan struct{})
	var runErr error
	go func() { defer close(done); runErr = daemon.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("joined daemon shutdown: %v", runErr)
			}
		case <-time.After(10 * time.Second):
			t.Error("joined daemon did not stop")
		}
	})
	var second ports.WorkerJobState
	for {
		select {
		case status := <-daemon.Updates():
			if status.Failed != 0 {
				t.Fatalf("original daemon failed code=%s", status.LastFailureCode)
			}
			if status.Committed == 1 && second.Job.RunID == "" {
				f.waitRetired(first, done)
				f.assertCanonicalResult(first, 1, 1)
				// The next original fresh poll synchronizes N+1 BEFORE the binder
				// sees a new canonical job. Explicit sync uses the same owner.
				if err := f.stack.SynchronizePreparedGenerationV3(f.ctx, f.session); err != nil {
					t.Fatal(err)
				}
				second = f.ingest(2)
			}
			if status.Committed == 2 {
				f.waitRetired(second, done)
				f.assertCanonicalResult(second, 2, 2)
				f.assertCanonicalResult(first, 1, 2)
				drainCtx, drainCancel := context.WithTimeout(f.ctx, 5*time.Second)
				drainErr := daemon.Drain(drainCtx)
				drainCancel()
				if drainErr != nil {
					t.Fatal(drainErr)
				}
				if f.installation.LaunchCount() != 2 || !reflect.DeepEqual(originalBaseline, f.baseline) || first.Job.RunID == second.Job.RunID || first.Job.HarnessBinding.Resource.CredentialGeneration != 1 || second.Job.HarnessBinding.Resource.CredentialGeneration != 2 {
					t.Fatal("two-generation execution/custody/source identity mismatch")
				}
				binding, found, err := f.credentialStore.LoadCredentialBinding(f.ctx, f.worker.TenantID, f.resource)
				if err != nil || !found || binding.Generation != 2 {
					t.Fatalf("genuine first WB increment/second unchanged WB: found=%t err=%v", found, err)
				}
				head, found, err := f.store.LoadAttachedResourceGenerationHeadV1(f.ctx, f.worker.TenantID, f.worker.OwnerUserID, f.resource)
				if err != nil || !found || head.CredentialGeneration != 2 || head.Revoked {
					t.Fatalf("actual durable successor head: found=%t err=%v", found, err)
				}
				metadataAfter, err := joinedGenerationReadResourceMetadata(f.ctx, f.db, f.worker.TenantID, f.resource)
				if err != nil || metadataAfter != f.metadataBefore {
					t.Fatalf("successor publication renewed independent observations: %v", err)
				}
				seen := f.wire.committedSnapshot()
				publishes, reads := 0, 0
				for _, path := range seen.paths {
					if path == attachedworkerhttp.ResourceGenerationPublishPathV1 {
						publishes++
					}
					if path == attachedworkerhttp.ResourceGenerationReadPathV1 {
						reads++
					}
				}
				if seen.lostMetadata != 1 || publishes != 2 || reads != 1 {
					t.Fatalf("lost initial publication ACK repeated a mutation or unchanged WB republished: loss=%d writes=%d reads=%d", seen.lostMetadata, publishes, reads)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("joined two-job deadline", ctx.Err())
		case <-done:
			t.Fatalf("original daemon stopped before two committed/retired jobs: err=%v", runErr)
		}
	}
}
