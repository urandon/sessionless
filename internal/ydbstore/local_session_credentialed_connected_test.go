package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const localSessionFixtureCredential = "synthetic-sessionless-bearer"

type connectedCredentialClock struct{ at time.Time }

func (c connectedCredentialClock) Now() time.Time { return c.at }

type connectedCredentialIDs struct{ next int }

func (g *connectedCredentialIDs) NewID(_ context.Context, _ ports.IDKind) (string, error) {
	g.next++
	return "credential-handle-" + string(rune('a'+g.next-1)), nil
}

type connectedCredentialPlanner struct{}

func (connectedCredentialPlanner) PlanProviderCredentialDelivery(_ context.Context, _ domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}, nil
}

type credentialedSessionBlobs struct {
	operations      *connectedOperationBlobs
	history         *sessionCompactionBlobs
	afterCompaction func()
}

func (b *credentialedSessionBlobs) operationKey(key string) bool {
	return strings.HasPrefix(key, b.operations.prefix)
}
func (b *credentialedSessionBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	if b.operationKey(key) {
		return b.operations.Put(ctx, tenant, key, body)
	}
	ref, err := b.history.Put(ctx, tenant, key, body)
	if err == nil && b.afterCompaction != nil && strings.Contains(key, "/derived-context-v1/") {
		b.afterCompaction()
	}
	return ref, err
}
func (b *credentialedSessionBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if b.operationKey(ref.Key) {
		return b.operations.Open(ctx, tenant, ref)
	}
	return b.history.Open(ctx, tenant, ref)
}
func (b *credentialedSessionBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if b.operationKey(ref.Key) {
		return b.operations.Delete(ctx, tenant, ref)
	}
	return b.history.Delete(ctx, tenant, ref)
}

type connectedCredentialSecretStore struct {
	ref    domain.CredentialSecretRef
	secret []byte
}

func (s *connectedCredentialSecretStore) ReadProviderCredentialSecret(_ context.Context, binding domain.ProviderCredentialBindingV1, maxBytes int64) ([]byte, error) {
	if binding.SecretRef != s.ref || int64(len(s.secret)) > maxBytes {
		return nil, errors.New("credential fixture reference mismatch")
	}
	return bytes.Clone(s.secret), nil
}
func (*connectedCredentialSecretStore) PutProviderCredentialCandidate(context.Context, ports.ProviderCredentialCandidateScopeV1, []byte) (ports.ProviderCredentialSecretCandidateV1, error) {
	return ports.ProviderCredentialSecretCandidateV1{}, errors.New("unexpected candidate write in connected invocation test")
}
func (*connectedCredentialSecretStore) CommitProviderCredentialCandidate(context.Context, ports.ProviderCredentialSecretCandidateV1) error {
	return errors.New("unexpected candidate commit in connected invocation test")
}
func (*connectedCredentialSecretStore) DeleteProviderCredentialCandidate(context.Context, ports.ProviderCredentialSecretCandidateV1) error {
	return errors.New("unexpected candidate delete in connected invocation test")
}
func (*connectedCredentialSecretStore) DeleteProviderCredentialSecret(context.Context, ports.ProviderCredentialCleanupV1) error {
	return errors.New("unexpected secret delete in connected invocation test")
}
func (*connectedCredentialSecretStore) ListUncommittedProviderCredentialCandidates(context.Context, ports.ProviderCredentialLocatorV1, uint64) ([]ports.ProviderCredentialSecretCandidateV1, error) {
	return nil, errors.New("unexpected candidate listing in connected invocation test")
}
func (*connectedCredentialSecretStore) ListAbandonedProviderCredentialCandidates(context.Context, time.Time, ports.ProviderCredentialCandidateCursorV1, uint64) (ports.ProviderCredentialCandidatePageV1, error) {
	return ports.ProviderCredentialCandidatePageV1{}, errors.New("unexpected abandoned candidate listing in connected invocation test")
}
func (s *connectedCredentialSecretStore) RecoverProviderCredentialCandidate(_ context.Context, binding domain.ProviderCredentialBindingV1) (bool, error) {
	return binding.SecretRef == s.ref && domain.FingerprintCredential(s.secret) == binding.SecretFingerprint, nil
}

type countedCredentialLifecycle struct {
	inner                                         ports.ProviderResourceCredentialLifecycleV1
	issues, materializations, callbacks, releases int
	callbackError                                 error
	beforeConsume                                 func()
	issueFailure                                  bool
	foreignHandle, zeroExpiry                     bool
	noCallback, doubleCallback, invalidDelivery   bool
	lateCallback                                  bool
	releaseFailure                                bool
	waitForReleaseDeadline                        bool
	issued                                        ports.ProviderInvocationCredentialV1
	lateConsume                                   ports.ProviderCredentialConsumerV1
	materializeReturned                           bool
	borrowedSecret                                []byte
	borrowedZeroed                                bool
	releaseDeadline                               time.Time
	releaseHasDeadline                            bool
	releaseSawMaterializeReturned                 bool
	releaseSawBorrowedZeroed                      bool
}

func (c *countedCredentialLifecycle) IssueProviderCredential(ctx context.Context, request ports.ProviderCredentialIssueRequestV1) (ports.ProviderInvocationCredentialV1, error) {
	c.issues++
	handle, err := c.inner.IssueProviderCredential(ctx, request)
	if err != nil {
		return handle, err
	}
	c.issued = handle
	if c.issueFailure {
		return handle, errors.New("injected issue failure after owned handle")
	}
	if c.foreignHandle {
		handle.OwnerUserID = "foreign-owner"
	}
	if c.zeroExpiry {
		handle.ExpiresAt = time.Time{}
	}
	return handle, nil
}
func (c *countedCredentialLifecycle) MaterializeProviderCredential(ctx context.Context, handle ports.ProviderInvocationCredentialV1, consume ports.ProviderCredentialConsumerV1) error {
	c.materializations++
	if c.noCallback {
		c.materializeReturned = true
		return nil
	}
	if c.lateCallback {
		c.lateConsume = consume
		c.materializeReturned = true
		return nil
	}
	err := c.inner.MaterializeProviderCredential(ctx, handle, func(plan ports.ProviderCredentialMaterializationV1, secret []byte) error {
		c.borrowedSecret = secret
		invoke := func(next ports.ProviderCredentialMaterializationV1) error {
			c.callbacks++
			if c.invalidDelivery {
				next = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryEnvironmentV1, EnvironmentName: "INVALID_TEST_ENV"}
			}
			if plan.Kind != domain.ProviderCredentialDeliveryDirectV1 || len(plan.FilePath) != 0 || len(plan.EnvironmentName) != 0 || len(secret) == 0 {
				return errors.New("connected credential did not use bounded direct delivery")
			}
			if c.beforeConsume != nil {
				c.beforeConsume()
			}
			return consume(next, secret)
		}
		c.callbackError = invoke(plan)
		if c.doubleCallback {
			c.callbackError = invoke(plan)
			if c.callbackError != nil {
				return c.callbackError
			}
		}
		return c.callbackError
	})
	c.materializeReturned = true
	c.borrowedZeroed = len(c.borrowedSecret) > 0
	for _, b := range c.borrowedSecret {
		if b != 0 {
			c.borrowedZeroed = false
			break
		}
	}
	return err
}
func (c *countedCredentialLifecycle) ReleaseProviderCredential(ctx context.Context, handle ports.ProviderInvocationCredentialV1) error {
	c.releases++
	c.releaseDeadline, c.releaseHasDeadline = ctx.Deadline()
	c.releaseSawMaterializeReturned = c.materializeReturned
	c.releaseSawBorrowedZeroed = c.borrowedZeroed
	if c.waitForReleaseDeadline {
		<-ctx.Done()
	}
	err := c.inner.ReleaseProviderCredential(ctx, handle)
	if c.releaseFailure {
		return errors.New("injected release failure")
	}
	return err
}

func (c *countedCredentialLifecycle) cleanupUnreleasedHandle(t *testing.T) {
	t.Helper()
	if c.issued.HandleID == "" {
		return
	}
	if err := c.inner.ReleaseProviderCredential(t.Context(), c.issued); err != nil {
		t.Error("clean up synthetic provider invocation handle")
	}
}

func newConnectedCredentialLifecycle(t *testing.T, seed *ydbstore.LocalSessionConnectedSeed) (*countedCredentialLifecycle, []byte) {
	t.Helper()
	secret := []byte(localSessionFixtureCredential)
	ref, err := domain.NewCredentialSecretRef("fixture/sessionless/local-endpoint/generation-1")
	if err != nil {
		t.Fatal("construct synthetic credential reference")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	binding := domain.ProviderCredentialBindingV1{
		Version: domain.ProviderCredentialBindingVersionV1, TenantID: seed.Scope.TenantID, OwnerUserID: seed.Scope.OwnerUserID,
		ResourceKind: seed.Profile.Resource.Kind, ResourceID: seed.Profile.Resource.ResourceID,
		ResourceRevision: seed.Profile.Resource.Revision, CredentialGeneration: seed.Profile.Resource.CredentialGeneration,
		CandidateMutationID: "synthetic-connected-mutation", State: domain.ProviderCredentialActiveV1,
		SecretRef: ref, SecretFingerprint: domain.FingerprintCredential(secret), UpdatedAt: now,
	}
	if err := binding.Validate(); err != nil {
		t.Fatal("validate synthetic canonical credential binding")
	}
	seed.InstallProviderCredentialBinding(t, binding)
	bindings, err := ydbstore.New(seed.DB, ydbstore.Options{})
	if err != nil {
		t.Fatal("construct actual connected YDB credential binding store")
	}
	secrets := &connectedCredentialSecretStore{ref: ref, secret: bytes.Clone(secret)}
	service, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: connectedCredentialClock{at: now}, IDs: &connectedCredentialIDs{}}, connectedCredentialPlanner{}, bindings, secrets)
	if err != nil {
		for i := range secrets.secret {
			secrets.secret[i] = 0
		}
		t.Fatal("construct actual provider credential invocation service")
	}
	t.Cleanup(func() {
		for i := range secrets.secret {
			secrets.secret[i] = 0
		}
	})
	return &countedCredentialLifecycle{inner: service}, secret
}

func newCredentialedConnectedSessionAt(t *testing.T, endpoint string, limits ...ydbstore.LocalCompactionConnectedLimits) (*ydbstore.LocalSessionConnectedSeed, *credentialedSessionBlobs, *countedCredentialLifecycle, []byte, *providerloopsession.CredentialedV1) {
	return newCredentialedConnectedSessionConfiguredAt(t, endpoint, "", limits...)
}

func newCredentialedConnectedSessionConfiguredAt(t *testing.T, endpoint, historicalSummary string, limits ...ydbstore.LocalCompactionConnectedLimits) (*ydbstore.LocalSessionConnectedSeed, *credentialedSessionBlobs, *countedCredentialLifecycle, []byte, *providerloopsession.CredentialedV1) {
	t.Helper()
	var blobs *credentialedSessionBlobs
	blobsForScope := func(scope domain.AttemptExecutionSourceScopeV1) ports.BlobStore {
		blobs = &credentialedSessionBlobs{operations: newConnectedOperationBlobs(scope), history: newSessionCompactionBlobs()}
		return blobs
	}
	var seed *ydbstore.LocalSessionConnectedSeed
	if historicalSummary == "" {
		seed = ydbstore.NewCredentialedLocalCompactionSessionConnectedSeed(t, endpoint, blobsForScope, limits...)
	} else {
		seed = ydbstore.NewCredentialedLargeHistoryCompactionSessionConnectedSeed(t, endpoint, blobsForScope, historicalSummary, limits...)
	}
	credentials, secret := newConnectedCredentialLifecycle(t, seed)
	session, err := providerloopsession.NewCredentialedV1(t.Context(), providerloopsession.CredentialedConfigV1{
		Local: providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare,
			Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs},
		Credentials: credentials,
	})
	if err != nil {
		t.Fatal("construct credentialed session from original Source/Store owners")
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("close credentialed session")
		}
	})
	return seed, blobs, credentials, secret, session
}

func newCredentialedConnectedSession(t *testing.T) (*localLoopbackPeer, *ydbstore.LocalSessionConnectedSeed, *credentialedSessionBlobs, *countedCredentialLifecycle, []byte, *providerloopsession.CredentialedV1) {
	t.Helper()
	peer := newLocalLoopbackPeer(t, true)
	seed, blobs, credentials, secret, session := newCredentialedConnectedSessionAt(t, peer.endpoint())
	peer.setResponse([]byte(`{"model":"` + seed.Profile.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"synthetic worker result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	return peer, seed, blobs, credentials, secret, session
}

func TestLocalSessionCredentialedConnectedOneBearerModel(t *testing.T) {
	peer, seed, _, credentials, secret, session := newCredentialedConnectedSession(t)
	if seed.Profile.Auth != domain.ConfiguredInferenceAuthBearerV1 || seed.Profile.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 || seed.Profile.Resource.CredentialGeneration != 1 {
		t.Fatal("credentialed seed did not seal the exact Bearer resource generation")
	}
	before := seed.Stats()
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err != nil || terminal == nil || !result.Model.SendAttempted || result.Compaction != nil {
		stats := seed.Stats()
		t.Fatalf("credentialed Run did not return one bounded durable model step and original terminal owner: err=%v terminal=%t modelSent=%t compaction=%t stats=%+v lifecycle=(issue=%d materialize=%d callback=%d release=%d) credentialReads=%d callbackSession=%t callbackIncomplete=%t",
			err, terminal != nil, result.Model.SendAttempted, result.Compaction != nil, stats,
			credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases, seed.ProviderCredentialReadCount(), errors.Is(credentials.callbackError, providerloopsession.ErrSession), errors.Is(credentials.callbackError, providerloopsession.ErrIncomplete))
	}
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 {
		t.Fatal("credential lifecycle did not use exactly one Issue/Materialize/callback/Release")
	}
	if !credentials.materializeReturned || !credentials.borrowedZeroed || !credentials.releaseHasDeadline || !credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed {
		t.Fatal("Release did not follow completed materialization, secret zeroing, and a bounded cleanup deadline")
	}
	if got := seed.Stats(); got.Parent != before.Parent+1 || got.Burn != before.Burn+1 || got.Allocation != before.Allocation+1 || got.Operation != before.Operation+1 || got.Call != before.Call+1 || got.Observation != before.Observation+1 {
		t.Fatal("credentialed model did not consume one original reservation and durable operation")
	}
	if got := peer.waitResult(t); got.err != nil || got.request == nil || got.request.Header.Get("Authorization") != "Bearer "+string(secret) || len(got.extra) != 0 {
		t.Fatal("owned local endpoint did not observe the one synthetic Bearer-authorized request")
	}
	if seed.ProviderCredentialReadCount() < 2 {
		t.Fatalf("Issue and Materialize did not each reread the exact current credential binding row: reads=%d", seed.ProviderCredentialReadCount())
	}
	if err := session.Close(); err != nil {
		t.Fatal("close credentialed session after terminal preparation")
	}
	terminalDeadline, err := terminal.Deadline()
	if err != nil || !terminalDeadline.Equal(credentials.releaseDeadline) {
		t.Fatal("prepared terminal deadline differs from captured Release cleanup deadline")
	}
	if _, err := seed.Source.Configuration(); err != nil {
		t.Fatal("credentialed session closed the borrowed original Source")
	}
	if err := seed.DB.PingContext(t.Context()); err != nil {
		t.Fatal("credentialed session closed the borrowed original DB")
	}
}

func TestLocalSessionCredentialedConnectedRevocationAfterMaterializeBlocksModel(t *testing.T) {
	peer, seed, _, credentials, _, session := newCredentialedConnectedSession(t)
	readsBeforeConsume := 0
	credentials.beforeConsume = func() {
		seed.RevokeProviderCredentialBinding(t)
		readsBeforeConsume = seed.ProviderCredentialReadCount()
	}
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err == nil || terminal != nil || result.Model.SendAttempted || result.Compaction != nil {
		t.Fatalf("revocation after lifecycle read must block model before physical dispatch: err=%v terminal=%t model=%t compaction=%t stats=%+v",
			err, terminal != nil, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
	}
	if got := seed.Stats(); got.Operation != 0 || got.Call != 0 || got.Observation != 0 {
		t.Fatalf("revoked credential reached a durable model operation: %+v", got)
	}
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 {
		t.Fatalf("revoked credential lifecycle was not bounded: issue=%d materialize=%d callback=%d release=%d",
			credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases)
	}
	if seed.ProviderCredentialReadCount() <= readsBeforeConsume {
		t.Fatalf("revoked credential did not trigger another current-row read after lifecycle materialization: before=%d after=%d", readsBeforeConsume, seed.ProviderCredentialReadCount())
	}
	select {
	case got := <-peer.result:
		t.Fatalf("revoked credential reached the owned HTTP peer: request=%t err=%v", got.request != nil, got.err)
	default:
	}
}

func TestLocalSessionCredentialedConnectedRevocationAfterReservationBlocksBeforeSend(t *testing.T) {
	peer, seed, _, credentials, _, session := newCredentialedConnectedSession(t)
	readsAtRevocation := 0
	revokedAfterReservation := false
	seed.ArmProviderCredentialRevocationAfterOperationReservationCommit(t, func() {
		readsAtRevocation = seed.ProviderCredentialReadCount()
		revokedAfterReservation = true
	})
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if !revokedAfterReservation {
		t.Fatalf("credential row was not revoked after reservation and call-index commit: err=%v terminal=%t model=%t stats=%+v lifecycle=(%d,%d,%d,%d) reads=%d",
			err, terminal != nil, result.Model.SendAttempted, seed.Stats(), credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases, seed.ProviderCredentialReadCount())
	}
	if err == nil || terminal != nil || result.Model.SendAttempted || result.Compaction != nil {
		t.Fatalf("revocation after reservation must block BeforeSend without preparing terminal: err=%v terminal=%t model=%t compaction=%t stats=%+v",
			err, terminal != nil, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
	}
	if got := seed.Stats(); got.Proxy != 1 || got.Operation != 1 || got.Call != 1 || got.Observation != 1 {
		t.Fatalf("post-reservation revocation must preserve one proxy, committed operation/call, and authenticated rejection observation: %+v", got)
	}
	observation := seed.PersistedObservation(t)
	if observation.Outcome != domain.AttemptOperationRejectedBeforeSendV1 || observation.Reason != domain.AttemptOperationReasonTransportUnknownV1 {
		t.Fatalf("post-reservation denial persisted an unexpected outcome/reason: outcome=%s reason=%s", observation.Outcome, observation.Reason)
	}
	if seed.Operate.VerifyAttemptOperationObservation(result.Model.Observation) != nil ||
		!reflect.DeepEqual(result.Model.Observation.Observation, observation) {
		t.Fatal("post-reservation rejection was not issuer-authenticated as the persisted observation")
	}
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 {
		t.Fatalf("post-reservation revocation lifecycle was not bounded exactly once: issue=%d materialize=%d callback=%d release=%d",
			credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases)
	}
	if seed.ProviderCredentialReadCount() <= readsAtRevocation {
		t.Fatalf("BeforeSend did not re-read the revoked binding after reservation: atRevoke=%d afterRun=%d", readsAtRevocation, seed.ProviderCredentialReadCount())
	}
	got := peer.waitResult(t)
	if got.request != nil || len(got.body) != 0 || !errors.Is(got.err, io.EOF) {
		t.Fatalf("post-reservation revocation must close the preflight socket before any HTTP request bytes: request=%t bytes=%d err=%v", got.request != nil, len(got.body), got.err)
	}
	beforeReplay := seed.Stats()
	issues, materializations, callbacks, releases := credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases
	copySession := *session
	again, nextTerminal, replayErr := copySession.Run(t.Context(), seed.Request)
	if replayErr == nil || nextTerminal != nil || again.Model.SendAttempted || again.Compaction != nil || seed.Stats() != beforeReplay ||
		credentials.issues != issues || credentials.materializations != materializations || credentials.callbacks != callbacks || credentials.releases != releases {
		t.Fatalf("copied credentialed session replayed after authenticated rejection: err=%v terminal=%t model=%t stats=%+v before=%+v lifecycle=(%d,%d,%d,%d)",
			replayErr, nextTerminal != nil, again.Model.SendAttempted, seed.Stats(), beforeReplay, credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases)
	}
}

func TestLocalSessionCredentialedConnectedLargeHistoryNoFitDeniesWithoutEffect(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	seed, _, credentials, _, session := newCredentialedConnectedSessionConfiguredAt(t, peer.endpoint(), strings.Repeat("x", 16384), ydbstore.LocalCompactionConnectedLimits{InputTokens: 8192, NetworkOperation: 5 * time.Second})
	peer.setResponse([]byte(`{"model":"` + seed.Profile.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"synthetic worker result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err == nil || terminal != nil || result.Model.SendAttempted || result.Compaction != nil {
		t.Fatalf("canonical history that fits neither bounded request must deny without terminal/effect: err=%v terminal=%t model=%t compaction=%t stats=%+v",
			err, terminal != nil, result.Model.SendAttempted, result.Compaction != nil, seed.Stats())
	}
	if got := seed.Stats(); got.Proxy != 0 || got.Operation != 0 || got.Call != 0 || got.Observation != 0 {
		t.Fatalf("no-fit denial created a durable effect: %+v", got)
	}
	if credentials.issues != 1 || credentials.materializations != 1 || credentials.callbacks != 1 || credentials.releases != 1 {
		t.Fatalf("no-fit denial did not release the one materialized credential: issue=%d materialize=%d callback=%d release=%d",
			credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases)
	}
	select {
	case got := <-peer.result:
		t.Fatalf("no-fit denial reached the owned HTTP peer: request=%t err=%v", got.request != nil, got.err)
	default:
	}
}

func TestLocalSessionCredentialedConnectedLifecycleFaultMatrix(t *testing.T) {
	tests := []struct {
		name            string
		configure       func(*countedCredentialLifecycle)
		wantRelease     int
		wantMaterialize int
		wantCallbacks   int
		wantOperations  int
	}{
		{name: "owned issue error", configure: func(c *countedCredentialLifecycle) { c.issueFailure = true }, wantRelease: 1},
		{name: "foreign handle", configure: func(c *countedCredentialLifecycle) { c.foreignHandle = true }},
		{name: "owned zero expiry", configure: func(c *countedCredentialLifecycle) { c.zeroExpiry = true }, wantRelease: 1},
		{name: "zero callbacks", configure: func(c *countedCredentialLifecycle) { c.noCallback = true }, wantRelease: 1, wantMaterialize: 1},
		{name: "invalid delivery", configure: func(c *countedCredentialLifecycle) { c.invalidDelivery = true }, wantRelease: 1, wantMaterialize: 1, wantCallbacks: 1},
		{name: "late callback", configure: func(c *countedCredentialLifecycle) { c.lateCallback = true }, wantRelease: 1, wantMaterialize: 1},
		{name: "double callback", configure: func(c *countedCredentialLifecycle) { c.doubleCallback = true }, wantRelease: 1, wantMaterialize: 1, wantCallbacks: 2, wantOperations: 1},
		{name: "release error", configure: func(c *countedCredentialLifecycle) { c.releaseFailure = true }, wantRelease: 1, wantMaterialize: 1, wantCallbacks: 1, wantOperations: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer, seed, _, credentials, secret, session := newCredentialedConnectedSession(t)
			tt.configure(credentials)
			result, terminal, err := session.Run(t.Context(), seed.Request)
			if err == nil || terminal != nil || result.Model.SendAttempted != (tt.wantOperations == 1) || seed.Stats().Operation != tt.wantOperations {
				t.Fatalf("lifecycle fault escaped bounded failure: err=%v terminal=%t model=%t stats=%+v", err, terminal != nil, result.Model.SendAttempted, seed.Stats())
			}
			if credentials.issues != 1 || credentials.releases != tt.wantRelease || credentials.materializations != tt.wantMaterialize || credentials.callbacks != tt.wantCallbacks {
				t.Fatalf("unexpected lifecycle calls: issue=%d materialize=%d callbacks=%d release=%d", credentials.issues, credentials.materializations, credentials.callbacks, credentials.releases)
			}
			if tt.wantRelease == 1 && (!credentials.releaseHasDeadline || (tt.wantCallbacks > 0 && (!credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed))) {
				t.Fatal("Release did not follow completed materialization, zeroed borrowed bytes, and bounded cleanup")
			}
			if tt.name == "late callback" {
				if credentials.lateConsume == nil {
					t.Fatal("late callback fixture did not retain the callback")
				}
				if err := credentials.lateConsume(ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryDirectV1}, secret); err == nil || seed.Stats().Operation != 0 {
					t.Fatal("late callback was accepted or created an operation")
				}
			}
			if tt.wantOperations == 1 {
				got := peer.waitResult(t)
				if got.err != nil || got.request == nil || got.request.Header.Get("Authorization") != "Bearer "+string(secret) {
					t.Fatal("one bounded physical request was not observed on the owned peer")
				}
				before := seed.Stats()
				if again, next, err := session.Run(t.Context(), seed.Request); err == nil || next != nil || again.Model.SendAttempted || seed.Stats() != before {
					t.Fatal("failed credentialed invocation replayed after callback or release failure")
				}
			} else if credentials.foreignHandle || credentials.zeroExpiry {
				credentials.cleanupUnreleasedHandle(t)
			}
		})
	}
}

func TestLocalSessionCredentialedConnectedExpiredReleaseCannotPrepareTerminal(t *testing.T) {
	peer := newLocalLoopbackPeer(t, true)
	seed, _, credentials, secret, session := newCredentialedConnectedSessionAt(t, peer.endpoint(), ydbstore.LocalCompactionConnectedLimits{CleanupTimeout: 2 * time.Second})
	peer.setResponse([]byte(`{"model":"` + seed.Profile.ModelID + `","choices":[{"index":0,"message":{"role":"assistant","content":"synthetic worker result"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	credentials.waitForReleaseDeadline = true
	result, terminal, err := session.Run(t.Context(), seed.Request)
	if err == nil || terminal != nil || !result.Model.SendAttempted || seed.Stats().Operation != 1 {
		t.Fatalf("expired Release deadline did not fail after the one model effect and before terminal preparation: err=%v terminal=%t model=%t stats=%+v",
			err, terminal != nil, result.Model.SendAttempted, seed.Stats())
	}
	if credentials.releases != 1 || !credentials.releaseHasDeadline || credentials.releaseDeadline.After(time.Now()) || !credentials.releaseSawMaterializeReturned || !credentials.releaseSawBorrowedZeroed {
		t.Fatal("Release did not observe the original bounded cleanup deadline after Materialize returned and zeroed its bytes")
	}
	got := peer.waitResult(t)
	if got.err != nil || got.request == nil || got.request.Header.Get("Authorization") != "Bearer "+string(secret) {
		t.Fatal("bounded synthetic MODEL request was not observed before expired Release")
	}
	before := seed.Stats()
	if again, next, err := session.Run(t.Context(), seed.Request); err == nil || next != nil || again.Model.SendAttempted || seed.Stats() != before {
		t.Fatal("expired Release failure permitted a second model dispatch")
	}
	credentials.cleanupUnreleasedHandle(t)
}
