package attachedworkertransport

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type opsAttachedSink struct {
	records  []opssignal.Record
	contexts []context.Context
	err      error
	observe  func(context.Context, opssignal.Record)
}

func (sink *opsAttachedSink) Observe(ctx context.Context, record opssignal.Record) error {
	sink.records = append(sink.records, record)
	sink.contexts = append(sink.contexts, ctx)
	if sink.observe != nil {
		sink.observe(ctx, record)
	}
	return sink.err
}

type opsCommitBroker struct {
	transportAttemptBroker
	commit func(context.Context, ports.AttachedWorkerTerminalCommit) (ports.AttachedWorkerAttemptResult, error)
}

func (broker opsCommitBroker) CommitAttachedWorkerTerminal(ctx context.Context, request ports.AttachedWorkerTerminalCommit) (ports.AttachedWorkerAttemptResult, error) {
	return broker.commit(ctx, request)
}

func opsTerminalResult(t *testing.T, fixture readyTransportFixture, status attachedworkerprotocol.TerminalStatus) (attachedworkerprotocol.FrameV1, ports.AttachedWorkerAttemptResult) {
	t.Helper()
	committed := fixture.pollResult(t, attachedworkerprotocol.MessageTerminalAck)
	batch, err := attachedworkerprotocol.DecodeBatchV1(committed.Outbound.Payload)
	if err != nil {
		t.Fatal(err)
	}
	ack := batch.Frames[0].TerminalAck
	ack.Status = status
	switch status {
	case attachedworkerprotocol.TerminalFailed:
		ack.Result = attachedworkerprotocol.TerminalResultFailed
	case attachedworkerprotocol.TerminalCancelled:
		ack.Result = attachedworkerprotocol.TerminalResultCancelled
	}
	batch.Frames[0].TerminalAck = ack
	committed.Outbound.Payload, err = attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		t.Fatal(err)
	}
	committed.Attempt.TerminalStatus = domain.AttachedWorkerTerminalStatus(status)
	committed.Attempt.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(hex.EncodeToString(ack.EvidenceDigest))
	sequence := fixture.connection.WorkerSequence + 1
	terminal := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence), WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration, ConnectionGeneration: fixture.connection.ConnectionGeneration, Sequence: sequence, Ack: fixture.connection.PlatformSequence, Kind: attachedworkerprotocol.MessageTerminal, Terminal: &attachedworkerprotocol.TerminalV1{Binding: ack.Binding, AttemptSequence: 2, TerminalSequence: 1, Status: ack.Status, Result: ack.Result, EvidenceDigest: ack.EvidenceDigest}}
	if err := terminal.Validate(); err != nil {
		t.Fatalf("terminal fixture: %v", err)
	}
	if err := committed.Attempt.Validate(); err != nil {
		t.Fatalf("committed fixture: %v", err)
	}
	return terminal, committed
}

func TestOpsAttachedCanonicalOutcomeRequiresExactFinalizerReadback(t *testing.T) {
	for _, tc := range []struct {
		name           string
		terminalStatus attachedworkerprotocol.TerminalStatus
		want           opssignal.SignalID
		mutate         func(*ports.AttachedWorkerAttemptResult)
		commitErr      error
		lostAck        bool
		plain          bool
	}{
		{name: "committed", terminalStatus: attachedworkerprotocol.TerminalSucceeded, want: opssignal.AttachedCanonicalSuccess},
		{name: "replayed", terminalStatus: attachedworkerprotocol.TerminalSucceeded, want: opssignal.AttachedCanonicalSuccess, mutate: func(r *ports.AttachedWorkerAttemptResult) { r.Status = ports.AttachedWorkerExecutionReplayed }},
		{name: "execution-failed", terminalStatus: attachedworkerprotocol.TerminalFailed, want: opssignal.AttachedExecutionFailure},
		{name: "cancelled", terminalStatus: attachedworkerprotocol.TerminalCancelled},
		{name: "ack-without-commit", terminalStatus: attachedworkerprotocol.TerminalSucceeded, plain: true},
		{name: "foreign-readback", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) { r.Attempt.RunID = "foreign-run" }},
		{name: "foreign-enrollment", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) { r.Attempt.EnrollmentGeneration++ }},
		{name: "pending-readback", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) {
			r.Attempt.State = domain.AttachedWorkerAttemptTerminalPending
		}},
		{name: "wrong-digest", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) {
			r.Attempt.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("a", 64))
		}},
		{name: "historical-no-head", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) {
			r.Historical = true
			r.Attempt = domain.AttachedWorkerAttemptV1{}
		}},
		{name: "denied-finalizer", terminalStatus: attachedworkerprotocol.TerminalSucceeded, mutate: func(r *ports.AttachedWorkerAttemptResult) { r.Status = ports.AttachedWorkerExecutionFenced }},
		{name: "ambiguous-commit", terminalStatus: attachedworkerprotocol.TerminalSucceeded, commitErr: errors.New("secret-provider-body private-run"), want: opssignal.AttachedCanonicalUnknown},
		{name: "commit-canceled", terminalStatus: attachedworkerprotocol.TerminalSucceeded, commitErr: context.Canceled},
		{name: "commit-conflict", terminalStatus: attachedworkerprotocol.TerminalSucceeded, commitErr: ydbstore.ErrAttachedWorkerAttemptConflict},
		{name: "commit-validation", terminalStatus: attachedworkerprotocol.TerminalSucceeded, commitErr: domain.ValidationError{Field: "private-run", Reason: "secret-body"}},
		{name: "lost-ack-after-commit", terminalStatus: attachedworkerprotocol.TerminalSucceeded, lostAck: true, want: opssignal.AttachedCanonicalSuccess},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := opsReceiptReadyFixture(t)
			fixture.makeClaimed(t)
			frame, truth := opsTerminalResult(t, fixture, tc.terminalStatus)
			accepted := truth
			accepted.Attempt.State = domain.AttachedWorkerAttemptTerminalPending
			accepted.Outbound = nil
			committed := truth
			if tc.mutate != nil {
				tc.mutate(&committed)
			}
			commits, exchanges, polls := 0, 0, 0
			ctx, cancel := context.WithDeadline(context.WithValue(context.Background(), struct{}{}, "private-context"), time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
			t.Cleanup(cancel)
			broker := opsCommitBroker{transportAttemptBroker: transportAttemptBroker{
				exchange: func(got context.Context, request ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
					exchanges++
					if got != ctx {
						t.Error("exchange replaced original context")
					}
					return accepted, nil
				},
				poll: func(got context.Context, request ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
					polls++
					if got != ctx {
						t.Error("poll replaced original context")
					}
					if tc.lostAck {
						return ports.AttachedWorkerAttemptResult{}, errors.New("private lost ACK")
					}
					return truth, nil
				},
			}, commit: func(got context.Context, request ports.AttachedWorkerTerminalCommit) (ports.AttachedWorkerAttemptResult, error) {
				commits++
				if got != ctx {
					t.Error("commit replaced original context")
				}
				return committed, tc.commitErr
			}}
			sink := &opsAttachedSink{err: errors.New("dropped observation")}
			config := receiptServiceConfig()
			config.OpsSignals = sink
			var service *Service
			var err error
			if tc.plain {
				service, err = NewService(config, fixture.store, broker)
			} else {
				service, err = NewReceiptFinalizingService(config, fixture.store, broker)
			}
			if err != nil {
				t.Fatal(err)
			}
			response, err := service.Exchange(ctx, fixture.bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}})
			wantErr := tc.commitErr != nil || committed.Status == ports.AttachedWorkerExecutionFenced || tc.lostAck
			if wantErr && err != ErrTransportBackend || !wantErr && (err != nil || response == nil) {
				t.Fatalf("public result response=%t err=%v wantBackend=%t", response != nil, err, wantErr)
			}
			wantCommits := 1
			if tc.plain {
				wantCommits = 0
			}
			if commits != wantCommits || exchanges != 1 || polls > 1 {
				t.Fatalf("effect calls commits=%d exchanges=%d polls=%d", commits, exchanges, polls)
			}
			wantRecords := 0
			if tc.want != "" {
				wantRecords = 1
			}
			if len(sink.records) != wantRecords {
				t.Fatalf("records=%+v want=%s", sink.records, tc.want)
			}
			if wantRecords == 1 {
				record := sink.records[0]
				if record.SignalID != tc.want || sink.contexts[0] != ctx {
					t.Fatalf("record=%+v contextPreserved=%t", record, sink.contexts[0] == ctx)
				}
				encoded, _ := json.Marshal(record)
				for _, secret := range []string{"private-run", "private-context", "secret-provider-body", "run-1", "attempt-1", "owner-a", "lease-1"} {
					if strings.Contains(string(encoded), secret) {
						t.Fatalf("record leaked %q", secret)
					}
				}
			}
		})
	}
}

func TestOpsAttachedAttemptBrokerFailureIsTransportNotExecution(t *testing.T) {
	for _, tc := range []struct {
		failure error
		want    int
	}{
		{errors.New("secret-broker-body"), 1},
		{domain.ErrMembershipDenied, 0},
		{domain.ValidationError{Field: "private", Reason: "deny"}, 0},
		{&domain.ValidationError{Field: "private", Reason: "deny"}, 0},
		{context.Canceled, 0},
	} {
		failure := tc.failure
		fixture := opsReceiptReadyFixture(t)
		fixture.makeClaimed(t)
		frame, truth := opsTerminalResult(t, fixture, attachedworkerprotocol.TerminalSucceeded)
		calls := 0
		sink := &opsAttachedSink{err: errors.New("drop")}
		config := receiptServiceConfig()
		config.OpsSignals = sink
		service, err := NewService(config, fixture.store, transportAttemptBroker{exchange: func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
			calls++
			return truth, failure
		}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = service.Exchange(context.Background(), fixture.bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}})
		if err != ErrTransportBackend || calls != 1 {
			t.Fatalf("error=%v calls=%d", err, calls)
		}
		want := tc.want
		if len(sink.records) != want || want == 1 && sink.records[0].SignalID != opssignal.AttachedTransportFailure {
			t.Fatalf("failure=%v records=%+v", failure, sink.records)
		}
	}
}

func TestOpsAttachedDelayedObservationPreservesCommittedTruthAndOriginalContext(t *testing.T) {
	fixture := opsReceiptReadyFixture(t)
	fixture.makeClaimed(t)
	frame, truth := opsTerminalResult(t, fixture, attachedworkerprotocol.TerminalSucceeded)
	accepted := truth
	accepted.Attempt.State = domain.AttachedWorkerAttemptTerminalPending
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	commits, polls := 0, 0
	durable := false
	broker := opsCommitBroker{transportAttemptBroker: transportAttemptBroker{
		exchange: func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
			return accepted, nil
		},
		poll: func(got context.Context, request ports.AttachedWorkerAttemptPoll) (ports.AttachedWorkerAttemptResult, error) {
			polls++
			if got != ctx || got.Err() != context.Canceled {
				t.Error("downstream read got renewed context")
			}
			return ports.AttachedWorkerAttemptResult{}, got.Err()
		},
	}, commit: func(got context.Context, request ports.AttachedWorkerTerminalCommit) (ports.AttachedWorkerAttemptResult, error) {
		commits++
		durable = true
		return truth, nil
	}}
	sink := &opsAttachedSink{err: errors.New("drop"), observe: func(got context.Context, record opssignal.Record) {
		if !durable || got != ctx {
			t.Error("observation preceded commit or detached context")
		}
		cancel()
	}}
	config := receiptServiceConfig()
	config.OpsSignals = sink
	service, err := NewReceiptFinalizingService(config, fixture.store, broker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Exchange(ctx, fixture.bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{frame}})
	if err != ErrTransportBackend || !durable || commits != 1 || polls != 1 || len(sink.records) != 1 || sink.records[0].SignalID != opssignal.AttachedCanonicalSuccess {
		t.Fatalf("err=%v durable=%t commits=%d polls=%d records=%+v", err, durable, commits, polls, sink.records)
	}
}

func opsReceiptReadyFixture(t *testing.T) readyTransportFixture {
	service, store, worker, privateKey := newTransportFixture(t)
	secretByte := byte(0x63)
	t.Helper()
	request := signedChallengeRequest(t, worker, privateKey)
	grant, err := service.IssueChallenge(context.Background(), worker.TenantID, worker.OwnerUserID, request)
	if err != nil {
		t.Fatal(err)
	}
	secret, _ := ParseConnectionSecret(bytes.Repeat([]byte{secretByte}, connectionSecretBytes))
	manifest := testCapabilityManifest(worker)
	manifest.Features = []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureOutputReceipt, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	channel := ConnectionChannelBinding(grant.Challenge.ID, grant.Challenge.WorkerNonceDigest, grant.Challenge.PlatformNonceDigest, secret.Digest())
	channelBytes, _ := decodeChannelBinding(channel)
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(worker.TenantID), OwnerUserID: string(worker.OwnerUserID), WorkerID: string(worker.ID),
		IdentityPublicKey: worker.IdentityPublicKey, EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Version: 1, ChannelBinding: channelBytes}
	attach := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Sequence: 2, Ack: 1, Kind: attachedworkerprotocol.MessageAttach,
		Attach: &attachedworkerprotocol.AttachV1{WorkerOffer: request.Hello.Hello.Offer, PlatformOffer: grant.Frame.Challenge.PlatformOffer,
			SelectedVersion: 1, WorkerNonce: request.Hello.Hello.WorkerNonce, PlatformNonce: grant.Frame.Challenge.PlatformNonce, CapabilityDigest: capabilityDigest}}
	if err := attachedworkerprotocol.SignAttachV1(privateKey, auth, &attach); err != nil {
		t.Fatal(err)
	}
	activation, err := service.Activate(context.Background(), worker.TenantID, worker.OwnerUserID, ActivateRequest{ChallengeID: grant.Challenge.ID, ConnectionSecretDigest: secret.Digest(), Attach: attach})
	if err != nil {
		t.Fatal(err)
	}
	manifestFrame := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(worker.ID), EnrollmentGeneration: worker.EnrollmentGeneration, ConnectionGeneration: grant.Challenge.TargetConnectionGeneration,
		Sequence: 3, Ack: 2, Kind: attachedworkerprotocol.MessageManifest, Manifest: &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: capabilityDigest}}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	bearer, _ := NewConnectionBearer(worker.TenantID, worker.OwnerUserID, worker.ID, activation.Connection.ID, secret)
	if response, err := service.Exchange(context.Background(), bearer, attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{manifestFrame}}); err != nil || response != nil {
		t.Fatalf("manifest setup response=%#v err=%v", response, err)
	}
	return readyTransportFixture{service: service, store: store, worker: worker, privateKey: privateKey, bearer: bearer, connection: store.connection, secret: secret}
}
