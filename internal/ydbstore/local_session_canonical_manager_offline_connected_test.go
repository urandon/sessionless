//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/queuecontract"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Audit storage against the ACTUAL committed plan/capsule at its PUT boundary.
// No prepared handle, final event, evidence or owner is injected into Manager.
type canonicalManagerBlobs struct {
	*canonicalTerminalBlobs
	puts, eventWrites int
}

func (b *canonicalManagerBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	b.puts++
	if strings.Contains(key, "/terminal-custody-v1/") {
		run := b.seed.TerminalRun(b.owner)
		if run.TerminalCustody == nil || run.TerminalCustody.Plan.Capsule.Key != key {
			return domain.BlobRef{}, errors.New("Manager PUT before original registered custody")
		}
		b.plan = run.TerminalCustody.Plan
	} else if b.plan.Capsule.Key != "" {
		b.mu.Lock()
		raw := bytes.Clone(b.objects[string(tenant)+"\x00"+b.plan.Capsule.Key])
		b.mu.Unlock()
		var capsule struct {
			Events []struct {
				Payload domain.BlobRef `json:"payload"`
			} `json:"events"`
		}
		if json.Unmarshal(raw, &capsule) != nil {
			return domain.BlobRef{}, errors.New("Manager event before durable actual capsule")
		}
		clear(raw)
		for _, event := range capsule.Events {
			if event.Payload.Key == key {
				if !b.capsuleClosed {
					return domain.BlobRef{}, errors.New("Manager event before actual capsule verification close")
				}
				b.eventWrites++
				break
			}
		}
	}
	return b.canonicalTerminalBlobs.Put(ctx, tenant, key, r)
}

type canonicalManagerQueue struct {
	message                          ports.ReceivedMessage
	receives, ack, retry, dead, wake int
	currentReceipt                   string
	order                            []string
	beforeAck                        func() error
}

func (q *canonicalManagerQueue) Receive(ctx context.Context) (ports.ReceivedMessage, error) {
	if ctx.Err() != nil {
		return ports.ReceivedMessage{}, ctx.Err()
	}
	q.receives++
	q.currentReceipt = fmt.Sprintf("canonical-receipt-%d", q.receives)
	q.order = append(q.order, "receive")
	m := q.message
	m.ReceiptHandle = q.currentReceipt
	m.DeliveryCount = uint32(q.receives)
	return m, nil
}
func (q *canonicalManagerQueue) Ack(ctx context.Context, receipt string) error {
	if ctx.Err() != nil || receipt != q.currentReceipt {
		return errors.New("Manager ACK exact delivery")
	}
	if q.beforeAck != nil {
		if e := q.beforeAck(); e != nil {
			return e
		}
	}
	q.ack++
	q.order = append(q.order, "ack")
	return nil
}
func (q *canonicalManagerQueue) Retry(_ context.Context, receipt string, _ time.Duration) error {
	if receipt != q.currentReceipt {
		return errors.New("Manager retry exact receipt")
	}
	q.retry++
	return nil
}
func (q *canonicalManagerQueue) DeadLetter(_ context.Context, receipt, _ string) error {
	if receipt != q.currentReceipt {
		return errors.New("Manager dead-letter exact receipt")
	}
	q.dead++
	return nil
}
func (*canonicalManagerQueue) Publish(context.Context, queuecontract.Envelope) error {
	return errors.New("unexpected queue publication")
}
func (q *canonicalManagerQueue) PublishFrontendProjectionWake(ctx context.Context, tenant domain.TenantID, run domain.RunID, _ time.Time) error {
	if ctx.Err() != nil || tenant != q.message.Envelope.TenantID || string(run) != q.message.Envelope.SubjectID {
		return errors.New("Manager wake exact canonical Run")
	}
	q.wake++
	q.order = append(q.order, "wake")
	return nil
}
func (*canonicalManagerQueue) PublishTelegramDeliveryWake(context.Context, domain.TenantID, domain.TelegramDeliveryID, time.Time) error {
	return errors.New("canonical Web must not wake Telegram")
}

// Genuine empty canonical commit/admission -> QUEUED -> original Manager
// load/preflight/claim/start/fresh entry/loop/terminal/wake/ACK. SQL, blobs and
// numeric HTTP/TLS/DNS/IP observation are controlled, not native/Web/model proof.
func TestLocalSessionCredentialedMCPOfflineCanonicalQueuedManager(t *testing.T) {
	peer := newOfflineModelChainPeer(t)
	seed := ydbstore.NewLocalCanonicalManagerSeed(t, peer.endpoint())
	profile, e := seed.Source.Configuration()
	if e != nil {
		t.Fatal(e)
	}
	peer.configure(t, profile.ModelID)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	blobs := &canonicalManagerBlobs{canonicalTerminalBlobs: &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), owner: t, seed: seed.LocalCanonicalTerminalSeed}}
	ref, e := blobs.Put(ctx, seed.Payload.TenantID, seed.Payload.Key, bytes.NewReader(seed.Body))
	if e != nil || ref != seed.Payload {
		t.Fatal("actual committed canonical body", e)
	}
	session, e := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := session.Close(); e != nil {
			t.Error(e)
		}
	})
	mcp := newOfflineMCPPeer(t, seed.Grant.Descriptor, 4, nil, false)
	if e := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e != nil {
		t.Fatal(e)
	}
	state, e := session.WorkerState()
	if e != nil {
		t.Fatal(e)
	}
	initial := seed.Stats()
	if seed.Loaded.Run.Status != domain.RunQueued || seed.Loaded.Attempt.Status != domain.AttemptCreated || initial.LeasePresent || initial.Parents != 0 || initial.Burns != 0 || initial.Allocations != 0 {
		t.Fatal("Manager was supplied a seeded running invocation")
	}
	queue := &canonicalManagerQueue{message: ports.ReceivedMessage{Envelope: queuecontract.Envelope{Schema: queuecontract.SchemaV1, MessageID: "canonical-manager-message", Kind: queuecontract.KindDispatchRun, TenantID: seed.Loaded.Run.TenantID, SubjectID: string(seed.Loaded.Run.ID), EnqueuedAt: seed.Loaded.Job.CreatedAt}}}
	signals := &localManagerOpsSink{}
	var retryError error
	// PhysicalClaimGenerator is deliberately omitted: production Manager owns
	// its fresh claim. Lease identity also comes from its actual normal claim.
	manager, e := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: seed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: session, ProjectionWakePublisher: queue, DeliveryWakePublisher: queue, OpsSignals: signals, RetryObserver: func(e error) { retryError = e }}, localManagerClock{}, queue, state, blobs, nil)
	if e != nil {
		t.Fatal(e)
	}
	queue.beforeAck = func() error {
		got := seed.TerminalSnapshot(t)
		if got.Finalizations != 1 || got.Run.Status != domain.RunSucceeded || !seed.TerminalStorageReleased() || queue.wake != queue.ack+1 || blobs.deletes != 6 || blobs.absences != 6 {
			return errors.New("ACK before original canonical/cleanup/wake")
		}
		// The productive invocation owns physical effects and must close before
		// ACK. An INERT duplicate is caller-owned, as LocalRuntime.RunOnce's
		// defer closes it after Manager returns, before the next delivery.
		if queue.ack == 0 {
			if _, e := session.WorkerState(); e == nil {
				return errors.New("productive ACK before original session close")
			}
		}
		return nil
	}
	outcome, e := manager.RunOnce(ctx)
	if e != nil || outcome != worker.OutcomeCompleted || retryError != nil || queue.ack != 1 || queue.wake != 1 || queue.retry != 0 || queue.dead != 0 {
		t.Fatalf("actual queued Manager: outcome=%s err=%v retry=%v trace=%s", outcome, e, retryError, seed.TerminalTrace())
	}
	mcp.wait(t)
	wires := peer.wait(t, false)
	principals, leases, runs, attempts := seed.ManagerLifecycle()
	if principals < 1 || leases != 1 || runs != 1 || attempts != 1 {
		t.Fatal("Manager missed genuine prelease/claim/queued start", principals, leases, runs, attempts)
	}
	parent := seed.ManagerParent(t)
	if parent.Reservation.PhysicalInvocationClaimID == "" || parent.Authority.Lease.ID == seed.Claim.LeaseID || parent.Authority.Lease.FenceToken != 1 || parent.Authority.Lease.WorkerID != seed.Claim.WorkerID {
		t.Fatal("Manager did not retain its actual fresh claim/lease")
	}
	reservations, observed := seed.OperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 || observed[2].Outcome != domain.AttemptOperationCompletedV1 {
		t.Fatal("Manager original durable frontier absent")
	}
	for i, r := range reservations {
		if r.Operation.Scope.PhysicalInvocationClaimID != parent.Reservation.PhysicalInvocationClaimID || r.Operation.Scope.LeaseID != parent.Authority.Lease.ID {
			t.Fatal("Manager operation changed original claimed invocation")
		}
		if i > 0 {
			d, _ := observed[i-1].Digest()
			if r.Operation.PredecessorObservationDigest != d {
				t.Fatal("Manager successor lost durable predecessor")
			}
		}
	}
	assertOfflinePairOnce(t, wires[0].body, 0, string(seed.Loaded.Run.ID))
	assertOfflinePairOnce(t, wires[1].body, 1, string(seed.Loaded.Run.ID))
	got := seed.TerminalSnapshot(t)
	if got.Run.Status != domain.RunSucceeded || got.Attempt.Status != domain.AttemptSucceeded || got.Reservation.Status != domain.ReservationCommitted || len(got.Events) != 3 || got.Session.LastEventSequence != seed.Event.Sequence+3 || got.Finalizations != 1 || got.Commits != 1 {
		t.Fatal("Manager did not durably finalize actual canonical lineage")
	}
	if blobs.puts != 12 || blobs.capsulePuts != 1 || blobs.eventWrites != 3 || blobs.deletes != 6 || blobs.absences != 6 {
		t.Fatal("Manager repeated/missed physical output or cleanup", blobs.puts, blobs.capsulePuts, blobs.eventWrites, blobs.deletes, blobs.absences)
	}
	if seed.Stats().Parents != 1 || seed.Stats().Burns != 1 || seed.Stats().Allocations != 2 || signals.count(opssignal.ManagedCleanupSuccess) != 1 || signals.count(opssignal.ManagedCanonicalSuccess) != 1 {
		t.Fatal("Manager original entry/retirement/observation counts changed")
	}
	beforeStats, puts, deletes := seed.Stats(), blobs.puts, blobs.deletes
	// A redelivered message gets fresh INERT invocation owners/issuers over
	// the borrowed original installation and durable Store, not a reused entry.
	prepare, e := serverlessharness.NewCapabilityIssuer(localManagerClock{}.Now, rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	operate, e := serverlessharness.NewOperationCapabilityIssuer(localManagerClock{}.Now, rand.Reader, 16)
	if e != nil {
		t.Fatal(e)
	}
	duplicateSession, e := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: prepare, Operate: operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := duplicateSession.Close(); e != nil {
			t.Error(e)
		}
	})
	if e := duplicateSession.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); e != nil {
		t.Fatal("duplicate inert owner offline transport", e)
	}
	duplicateState, e := duplicateSession.WorkerState()
	if e != nil {
		t.Fatal(e)
	}
	duplicateManager, e := worker.New(worker.Config{ScratchRoot: t.TempDir(), WorkerID: seed.Claim.WorkerID, LeaseTTL: time.Hour, LeaseWatchdogInterval: 20 * time.Minute, LocalOwnedSession: duplicateSession, ProjectionWakePublisher: queue, DeliveryWakePublisher: queue, RetryObserver: func(e error) { retryError = e }}, localManagerClock{}, queue, duplicateState, blobs, nil)
	if e != nil {
		t.Fatal(e)
	}
	if seed.Stats() != beforeStats || puts != blobs.puts || deletes != blobs.deletes {
		t.Fatal("duplicate factory minted effects before load")
	}
	outcome, e = duplicateManager.RunOnce(ctx)
	if e != nil || outcome != worker.OutcomeDuplicate || queue.ack != 2 || queue.wake != 2 || queue.retry != 0 || queue.dead != 0 || seed.Stats() != beforeStats || puts != blobs.puts || deletes != blobs.deletes || !reflect.DeepEqual(got, seed.TerminalSnapshot(t)) {
		t.Fatal("normal duplicate delivery repeated canonical/provider/cleanup effects", outcome, e)
	}
	if e := duplicateSession.Close(); e != nil {
		t.Fatal("duplicate inert owner close", e)
	}
	if _, e := duplicateSession.WorkerState(); e == nil {
		t.Fatal("caller did not close inert duplicate before another delivery")
	}
	if !reflect.DeepEqual(queue.order, []string{"receive", "wake", "ack", "receive", "wake", "ack"}) {
		t.Fatal("normal queue completion ordering", queue.order)
	}
	life, e := seed.Source.ExecutionLifetime()
	if e != nil || life.Err() != nil {
		t.Fatal("Manager closed the borrowed installation Source", e)
	}
	t.Logf("actual canonical queued Manager: prelease principal=%d, lease=%d, start=%d/%d; authority max SQL=%d; canonical raw=%d", principals, leases, runs, attempts, seed.MaxCallbackSQL(), seed.TerminalRawStatements())
}
