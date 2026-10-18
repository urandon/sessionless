package ydbstore_test

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type cleanupSessionBlobs struct {
	*terminalSessionBlobs
	cleanupMu        sync.Mutex
	deletes, absence int
	mode             string
	beforeDelete     func()
	sql              *ydbstore.LocalTerminalCleanupSQLFixture
}

func (b *cleanupSessionBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil || !strings.Contains(ref.Key, "/attempt-operations-v1/") {
		return errors.New("unexpected cleanup target")
	}
	if b.beforeDelete != nil {
		b.beforeDelete()
	}
	b.cleanupMu.Lock()
	defer b.cleanupMu.Unlock()
	b.deletes++
	if b.mode == "delete_failure" {
		return errors.New("owned delete failure")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.objects, string(tenant)+"\x00"+ref.Key)
	return nil
}
func (b *cleanupSessionBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("invalid exact absence")
	}
	b.cleanupMu.Lock()
	defer b.cleanupMu.Unlock()
	b.absence++
	if b.mode == "absence_failure" {
		return errors.New("owned absence unknown")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.objects[string(tenant)+"\x00"+ref.Key]; exists {
		return errors.New("object still present")
	}
	return nil
}
func newCleanupSession(t *testing.T, mode, text string, limits ...ydbstore.LocalCompactionConnectedLimits) (*providerloopsession.LocalV1, *ydbstore.LocalTerminalPublicationSessionConnectedSeed, *sessionCompactionPeer, *cleanupSessionBlobs) {
	t.Helper()
	peer := &sessionCompactionPeer{modelText: text}
	server := httptest.NewUnstartedServer(peer)
	server.Config.ReadHeaderTimeout = 10 * time.Second
	server.Config.ReadTimeout = 20 * time.Second
	server.Config.WriteTimeout = 20 * time.Second
	server.Config.MaxHeaderBytes = 16 << 10
	t.Cleanup(func() { server.CloseClientConnections(); server.Close() })
	server.Start()
	b := &cleanupSessionBlobs{terminalSessionBlobs: &terminalSessionBlobs{sessionCompactionBlobs: newSessionCompactionBlobs()}, mode: mode}
	seed := ydbstore.NewLocalTerminalPublicationSessionConnectedSeed(t, server.URL+"/v1", b, limits...)
	b.sql = seed.InstallTerminalCleanupSQL(t)
	b.seed = seed
	session, err := providerloopsession.NewLocalV1(t.Context(), providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: b})
	if err != nil {
		t.Fatal("original cleanup factory", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error("cleanup original session join", err)
		}
	})
	if _, err := session.Start(t.Context(), seed.Request); err != nil {
		t.Fatal("original cleanup invocation", err)
	}
	return session, seed, peer, b
}

// Actual NewLocalV1 keeper/bridge/lease/Store/physical-body-owner chain and
// owned HTTP peer; SCRIPTED SQL/funding and in-memory storage. No native YDB,
// real model, completed canonical finalizer, service activation or cloud proof.
func TestLocalSessionTerminalCleanupConnected(t *testing.T) {
	for _, mode := range []string{"fitting", "two_compactions", "source_cancelled"} {
		t.Run(mode, func(t *testing.T) {
			text, steps := "original cleanup answer", 1
			var limits []ydbstore.LocalCompactionConnectedLimits
			if mode == "two_compactions" {
				text, steps = strings.Repeat("x", 4096), 3
				limits = []ydbstore.LocalCompactionConnectedLimits{{InputTokens: 8192, NetworkOperation: 5 * time.Second}}
			}
			session, seed, peer, blobs := newCleanupSession(t, "", text, limits...)
			for i := 0; i < steps; i++ {
				if result, err := session.RunModelStep(t.Context()); err != nil || !result.Model.SendAttempted {
					t.Fatalf("original step%d err=%v attempted=%t stats=%+v", i, err, result.Model.SendAttempted, seed.Stats())
				}
			}
			h, p := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal("original confirmed publication", err)
			}
			before, stats := seed.TerminalPublicationRun(t), seed.Stats()
			blobs.mu.Lock()
			retained := make(map[string][]byte)
			ordinary := 0
			for key, body := range blobs.objects {
				if strings.Contains(key, "/attempt-operations-v1/") {
					ordinary++
				} else {
					retained[key] = bytes.Clone(body)
				}
			}
			blobs.mu.Unlock()
			if ordinary == 0 {
				t.Fatal("no original physical bodies")
			}
			blobs.beforeDelete = func() {
				original, current := seed.TerminalCleanupAllocation(t)
				if original.State != domain.RegisteredPreparationActiveV1 || current.State != domain.RegisteredPreparationRetiredV1 || current.ValidateTransitionFrom(original) != nil {
					t.Error("Delete before original confirmed retirement")
				}
			}
			if mode == "source_cancelled" {
				if err := seed.Source.Close(); err != nil {
					t.Fatal(err)
				}
			}
			result, err := h.FinishCleanup(t.Context())
			if err != nil || result == nil {
				original, current := seed.TerminalCleanupAllocation(t)
				t.Fatalf("original joined cleanup err=%v record=%t allocation=%s/%s writes=%d delete=%d absence=%d", err, result != nil, original.State, current.State, seed.Stats().Allocation, blobs.deletes, blobs.absence)
			}
			if !reflect.DeepEqual(before, seed.TerminalPublicationRun(t)) {
				t.Fatal("cleanup committed or changed canonical Run")
			}
			got := seed.Stats()
			want := stats
			want.Allocation++
			if got != want {
				t.Fatal("cleanup extra reservation/provider/observation effects", got, want)
			}
			blobs.mu.Lock()
			after := make(map[string][]byte)
			for key, body := range blobs.objects {
				after[key] = bytes.Clone(body)
			}
			blobs.mu.Unlock()
			if !reflect.DeepEqual(after, retained) || blobs.deletes != ordinary || blobs.absence != ordinary {
				t.Fatal("exact namespace erasure or retained preservation", ordinary, blobs.deletes, blobs.absence, len(after), len(retained))
			}
			copyHandle := *h
			if _, err := copyHandle.FinishCleanup(t.Context()); err == nil {
				t.Error("copied cleanup owner")
			}
			if _, err := h.FinishCleanup(t.Context()); err == nil {
				t.Error("cleanup replay")
			}
			if _, err := p.FinishCleanup(t.Context()); err == nil {
				t.Error("direct cleanup replay")
			}
			peer.mu.Lock()
			count, failure := len(peer.kinds), peer.failure
			peer.mu.Unlock()
			expected := 1
			if steps == 3 {
				expected = 5
			}
			if count != expected || failure != "" {
				t.Fatal("cleanup added model effect", count, expected, failure)
			}
		})
	}
}

func TestLocalSessionTerminalCleanupStorageFaultsConnected(t *testing.T) {
	for _, mode := range []string{"missing_capsule", "changed_canonical", "missing_derived", "delete_failure", "absence_failure", "deletion_registered", "deletion_read_failure", "deletion_close_failure", "changed_historical_run", "retirement_lost_ack"} {
		t.Run(mode, func(t *testing.T) {
			text, steps := "original cleanup answer", 1
			var limits []ydbstore.LocalCompactionConnectedLimits
			if mode == "missing_derived" {
				text, steps = strings.Repeat("x", 4096), 3
				limits = []ydbstore.LocalCompactionConnectedLimits{{InputTokens: 8192, NetworkOperation: 5 * time.Second}}
			}
			session, seed, _, blobs := newCleanupSession(t, mode, text, limits...)
			for i := 0; i < steps; i++ {
				if _, err := session.RunModelStep(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			h, p := prepareTerminalPublication(t, session, blobs.terminalSessionBlobs)
			if err := h.PublishOutput(t.Context()); err != nil {
				t.Fatal(err)
			}
			blobs.sql.SetFault(mode)
			if mode == "missing_derived" {
				run := seed.TerminalPublicationRun(t)
				if run.DerivedContext == nil || len(run.DerivedContext.Inventory) != 2 {
					t.Fatal("missing original two-pair derived closure")
				}
				ref := run.DerivedContext.Inventory[0].Summary
				blobs.mu.Lock()
				delete(blobs.objects, string(ref.TenantID)+"\x00"+ref.Key)
				blobs.mu.Unlock()
			}
			if mode == "missing_capsule" {
				blobs.mu.Lock()
				delete(blobs.objects, string(blobs.plan.Capsule.TenantID)+"\x00"+blobs.plan.Capsule.Key)
				blobs.mu.Unlock()
			}
			if mode == "changed_canonical" {
				drafts, err := p.Drafts()
				if err != nil {
					t.Fatal(err)
				}
				blobs.mu.Lock()
				blobs.objects[string(drafts[0].Payload.TenantID)+"\x00"+drafts[0].Payload.Key] = []byte("corrupt canonical payload")
				blobs.mu.Unlock()
			}
			result, err := h.FinishCleanup(t.Context())
			if err == nil || result != nil {
				t.Fatal("failed cleanup became original receipt", mode)
			}
			if mode != "delete_failure" && mode != "absence_failure" {
				if blobs.deletes != 0 || blobs.absence != 0 {
					t.Fatal("missing retained custody reached Delete", mode)
				}
			}
			_, current := seed.TerminalCleanupAllocation(t)
			if current.State != domain.RegisteredPreparationRetiredV1 {
				t.Fatal("failed cleanup restored allocation", mode)
			}
			before := blobs.deletes
			if _, err := h.FinishCleanup(t.Context()); err == nil || blobs.deletes != before {
				t.Error("failed cleanup retry", mode)
			}
		})
	}
}
