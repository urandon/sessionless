package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// The real Connector, Session exchange reducer and accepted claim are used;
// bootstrap/transport peers are owned in-process fixtures, with no sockets,
// provider calls, credential lifecycle or executable involved.
func nativeClaimedSession(t *testing.T) (*Session, *sessionFixture, domain.AttachedWorkerAttemptV1, domain.WorkerJob, domain.ArtifactManifest) {
	t.Helper()
	f := newSessionFixture(t)
	s := mustReadySession(t, f)
	b := s.Snapshot()
	expires := sessionTestTime.Add(time.Minute)
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "native-subscription",
		OwnerUserID: b.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied,
		OwnerUserID: b.OwnerUserID, WorkerID: b.WorkerID, CapabilityDigest: b.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	pd, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	job := domain.WorkerJob{TenantID: b.TenantID, RunID: "native-run", SessionID: "native-session", TriggerEventID: "native-trigger",
		AttemptID: "native-attempt", ReservationID: "native-reservation", InputManifestID: "native-manifest", CredentialOwnerUserID: b.OwnerUserID,
		ExecutionPlacementV2: placement, Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Minute,
			MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 1, MaxToolEvents: 1, MaxToolEventBytes: 4096}}
	job.HarnessBinding = domain.HarnessBindingV2{Version: 2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: b.TenantID, OwnerUserID: b.OwnerUserID,
		RunID: job.RunID, AttemptID: job.AttemptID, Resource: resource, ExecutionPlacementDigest: string(pd), EvidenceExpiresAt: &expires,
		Backend: domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendCodexExecV1,
			ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "codex-jsonl.v1", BackendProfileDigest: strings.Repeat("2", 64),
			ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryFileV1}, ModelVendorID: "openai", ModelID: "fixture",
		InputDataClass: domain.ProviderDataPrivateV1, ProviderCatalogDigest: strings.Repeat("1", 64), ProviderRouteDigest: strings.Repeat("2", 64),
		PrivacyPolicyDigest: strings.Repeat("3", 64), CapabilityEvidenceDigest: strings.Repeat("4", 64), EffectivePolicyDigest: strings.Repeat("5", 64)}
	body := []byte("owned native context")
	h := sha256.Sum256(body)
	job.ContextSnapshot = domain.BlobRef{TenantID: b.TenantID, Key: "tenants/" + string(b.TenantID) + "/native-context", Size: int64(len(body)), SHA256: hex.EncodeToString(h[:])}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: b.TenantID, RunID: job.RunID, CreatedAt: sessionTestTime}
	d, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(b.TenantID, b.OwnerUserID, b.WorkerID, job.RunID, job.AttemptID, "native-lease", 1)
	if err != nil {
		t.Fatal(err)
	}
	a := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, WorkerID: b.WorkerID, ConnectionID: b.ConnectionID,
		EnrollmentGeneration: b.EnrollmentGeneration, ConnectionGeneration: b.ConnectionGeneration, RunID: job.RunID, AttemptID: job.AttemptID, ReservationID: job.ReservationID,
		LeaseID: "native-lease", LeaseGeneration: 1, FenceToken: fence, ContextDigest: d, CapabilityDigest: b.CapabilityDigest, PolicyDigest: placement.PolicyDigest,
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1, Revision: 1, CreatedAt: sessionTestTime, UpdatedAt: sessionTestTime, LeaseExpiresAt: expires}
	binding := attachedworkerprotocol.AttemptBindingV1{RunID: string(a.RunID), AttemptID: string(a.AttemptID), LeaseID: string(a.LeaseID), LeaseGeneration: a.LeaseGeneration,
		FenceToken: string(a.FenceToken), ExpiresAtUnixMicro: expires.UnixMicro(), ContextDigest: mustDecodeHex(t, string(d)), CapabilityDigest: mustDecodeHex(t, string(b.CapabilityDigest)), PolicyDigest: mustDecodeHex(t, string(a.PolicyDigest))}
	step := 0
	f.exchange.setHandler(func(_ context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
		step++
		response := sessionPlatformBatch(b, uint64(step+2), batch.Frames[0].Sequence, attachedworkerprotocol.MessageLeaseOffer)
		switch step {
		case 1:
			response.Frames[0].LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: binding, AttemptSequence: 1}
		case 2:
			response.Frames[0].Kind = attachedworkerprotocol.MessageLeaseAccepted
			response.Frames[0].LeaseOffer = nil
			response.Frames[0].LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: binding, AttemptSequence: 2}
		case 3:
			response.Frames[0].Kind = attachedworkerprotocol.MessageCancel
			response.Frames[0].LeaseOffer = nil
			response.Frames[0].Cancel = &attachedworkerprotocol.CancelV1{Binding: binding, AttemptSequence: 3, CancelRevision: 1, Code: attachedworkerprotocol.CancelRequested}
		default:
			t.Fatalf("unexpected native fixture exchange %d", step)
		}
		return response, nil
	})
	if _, err := s.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: sessionTestTime.UnixMicro(), Available: true}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExchangeAction(context.Background(), ActionV1{LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: binding, AttemptSequence: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := a.Validate(); err != nil {
		t.Fatal(err)
	}
	return s, f, a, job, manifest
}

func TestNativeConduitSessionOriginalBorrowAndAcceptedCancel(t *testing.T) {
	s, _, a, job, manifest := nativeClaimedSession(t)
	v, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	if same, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest); err != nil || same != v {
		t.Fatalf("original borrow replaced: same=%t err=%v", same == v, err)
	}
	life, err := v.Lifetime()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExchangeAction(context.Background(), ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: sessionTestTime.UnixMicro(), ActiveAttempts: 1}}); err != nil {
		t.Fatal(err)
	}
	if life.Err() == nil || v.Current() == nil {
		t.Fatal("accepted Cancel left original child alive")
	}
}

func TestNativeConduitSessionRejectsDTOCopyAndRemint(t *testing.T) {
	s, _, a, job, manifest := nativeClaimedSession(t)
	foreign := job
	foreign.HarnessBinding = job.HarnessBinding.Clone()
	foreign.HarnessBinding.Resource.ResourceID = "foreign"
	if _, err := s.BorrowNativeAttemptV2(context.Background(), a, foreign, manifest); err == nil {
		t.Fatal("resource DTO bypassed actual accepted context digest")
	}
	// Deliberate copy counterexample without invoking Go's prohibited mutex-copy
	// assignment: every original-identity method must reject the copied receiver.
	copied := reflect.New(reflect.TypeOf(s).Elem())
	copied.Elem().Set(reflect.ValueOf(s).Elem())
	if _, err := copied.Interface().(*Session).BorrowNativeAttemptV2(context.Background(), a, job, manifest); err == nil {
		t.Fatal("copied Session minted child")
	}
	v, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	alias := *v
	if alias.Current() == nil {
		t.Fatal("copied child authenticated")
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest); err == nil {
		t.Fatal("retired child recreated for same claim")
	}
}

func TestNativeConduitSessionLossAndReconnectAreNotNewClaims(t *testing.T) {
	for _, kind := range []string{"close", "fence"} {
		t.Run(kind, func(t *testing.T) {
			s, _, a, job, manifest := nativeClaimedSession(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			v, err := s.BorrowNativeAttemptV2(ctx, a, job, manifest)
			if err != nil {
				t.Fatal(err)
			}
			life, err := v.Lifetime()
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "close":
				if err := s.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			case "fence":
				s.fail(StateFenced, "owned-test-fence")
			}
			if life.Err() == nil || v.Current() == nil {
				t.Fatal("original lifetime outlived lost authority")
			}
		})
	}
}

func TestNativeConduitSessionBorrowCancellationCannotRetireClaim(t *testing.T) {
	s, _, a, job, manifest := nativeClaimedSession(t)
	borrow, cancel := context.WithCancel(context.Background())
	child, err := s.BorrowNativeAttemptV2(borrow, a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Close() })
	life, err := child.Lifetime()
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if child.Current() != nil || life.Err() != nil {
		t.Fatal("borrow cancellation retired genuine Session claim")
	}
	if again, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest); err != nil || again != child {
		t.Fatalf("borrow cancellation changed original owner: same=%t err=%v", again == child, err)
	}
	if _, err := s.BorrowNativeAttemptV2(borrow, a, job, manifest); err == nil {
		t.Fatal("already canceled preflight context accepted")
	}
}

func TestNativeConduitSessionActualReconnectCannotReviveClaim(t *testing.T) {
	s, f, a, job, manifest := nativeClaimedSession(t)
	child, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	previousConfig, previousSnapshot := sessionProtocolState(t, s)
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	bootstrap := &reconnectBootstrap{now: f.bootstrap.now,
		publicKey:      ed25519.PrivateKey(f.secret.IdentityPrivateKey).Public().(ed25519.PublicKey),
		previousConfig: previousConfig, previousSnapshot: previousSnapshot}
	config := f.config
	config.Random = bytes.NewReader(append(bytes.Repeat([]byte{0x63}, 32), bytes.Repeat([]byte{0x64}, 32)...))
	connector, err := New(f.store, bootstrap, &fakeFactory{exchange: &fakeExchange{}}, config)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := connector.Reconnect(context.Background(), ReconnectInputV1(f.input))
	if err != nil {
		t.Fatal(err)
	}
	registerSessionCleanup(t, resumed)
	snapshot := resumed.Snapshot()
	_, recovered := sessionProtocolState(t, resumed)
	if snapshot.ConnectionGeneration != a.ConnectionGeneration+1 || recovered.Attempt.Summary.State != attachedworkerprotocol.AttemptClaimed {
		t.Fatalf("not a genuine recovered claim: snapshot=%+v attempt=%+v", snapshot, recovered.Attempt.Summary)
	}
	a.ConnectionID, a.ConnectionGeneration = snapshot.ConnectionID, snapshot.ConnectionGeneration
	if _, err := resumed.BorrowNativeAttemptV2(context.Background(), a, job, manifest); err == nil {
		t.Fatal("actual reconnect reminted a native owner for recovered claim")
	}
	if child.Current() == nil {
		t.Fatal("old owner survived actual close/reconnect")
	}
}

func TestNativeConduitSessionOriginalLeaseExpiry(t *testing.T) {
	s, _, a, job, manifest := nativeClaimedSession(t)
	child, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	life, err := child.Lifetime()
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.now = func() time.Time { return a.LeaseExpiresAt }
	s.mu.Unlock()
	if child.Current() == nil || life.Err() == nil {
		t.Fatal("original lease expiry did not retire owner")
	}
	if _, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest); err == nil {
		t.Fatal("expired original claim reminted owner")
	}
}

type passiveNativeEntry struct{ n int }

func (*passiveNativeEntry) NativePreparedEntryIdentityV2() { panic("marker must never be invoked") }

func TestNativeConduitSessionOriginalPreparedEntryAssociation(t *testing.T) {
	s, _, a, job, manifest := nativeClaimedSession(t)
	child, err := s.BorrowNativeAttemptV2(context.Background(), a, job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Close() })
	var typedNil *passiveNativeEntry
	if _, err := child.BindPreparedEntryV2(typedNil); err == nil {
		t.Fatal("typed nil accepted")
	}
	original, loser := &passiveNativeEntry{}, &passiveNativeEntry{}
	got, err := child.BindPreparedEntryV2(original)
	if err != nil || got != original {
		t.Fatalf("original association got=%p err=%v", got, err)
	}
	got, err = child.BindPreparedEntryV2(loser)
	if err != nil || got != original {
		t.Fatalf("loser replaced original got=%p err=%v", got, err)
	}
	foreignSession, _, fa, fj, fm := nativeClaimedSession(t)
	foreign, err := foreignSession.BorrowNativeAttemptV2(context.Background(), fa, fj, fm)
	if err != nil {
		t.Fatal(err)
	}
	_ = foreign.Close()
	if _, err := foreign.BindPreparedEntryV2(original); err == nil {
		t.Fatal("retired foreign owner attached entry")
	}
	if child.Current() != nil {
		t.Fatal("loser/foreign entry retired winner child")
	}
}
