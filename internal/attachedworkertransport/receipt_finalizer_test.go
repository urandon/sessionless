package attachedworkertransport

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type receiptTestBroker struct {
	transportAttemptBroker
	commits int
}

func (broker *receiptTestBroker) CommitAttachedWorkerTerminal(context.Context, ports.AttachedWorkerTerminalCommit) (ports.AttachedWorkerAttemptResult, error) {
	broker.commits++
	return ports.AttachedWorkerAttemptResult{}, errors.New("unexpected terminal commit")
}

func receiptServiceConfig() ServiceConfig {
	return ServiceConfig{
		IDs: transportIDs{}, Audience: "sessionless:attached-worker:v1", PlatformOffer: testOffer(),
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}, ChallengeLifetime: 5 * time.Minute,
		ChallengeRetention: time.Hour, PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: MinimumHeartbeatInterval,
	}
}

func TestReceiptFinalizingServiceRequiresCanonicalCommitBroker(t *testing.T) {
	_, store, _, _ := newTransportFixture(t)
	if _, err := NewReceiptFinalizingService(receiptServiceConfig(), store, transportAttemptBroker{}); !errors.Is(err, ErrTransportConfig) {
		t.Fatalf("broker without commit: got %v, want invalid config", err)
	}
	broker := &receiptTestBroker{}
	service, err := NewReceiptFinalizingService(receiptServiceConfig(), store, broker)
	if err != nil || service == nil || service.receiptFinalizer != broker {
		t.Fatalf("canonical composition: service=%v error=%v", service, err)
	}
	plain, err := NewService(receiptServiceConfig(), store, broker)
	if err != nil || plain.receiptFinalizer != nil {
		t.Fatalf("default constructor enabled finalization: error=%v", err)
	}
}

func receiptTerminalFixture(t *testing.T) (readyTransportFixture, attachedworkerprotocol.FrameV1) {
	fixture := newReadyTransportFixture(t)
	fixture.makeClaimed(t)
	ack := fixture.pollResult(t, attachedworkerprotocol.MessageTerminalAck)
	batch, err := attachedworkerprotocol.DecodeBatchV1(ack.Outbound.Payload)
	if err != nil {
		t.Fatal(err)
	}
	evidence := batch.Frames[0].TerminalAck
	sequence := fixture.connection.WorkerSequence + 1
	terminal := attachedworkerprotocol.FrameV1{
		Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
		WorkerID: string(fixture.worker.ID), EnrollmentGeneration: fixture.connection.EnrollmentGeneration,
		ConnectionGeneration: fixture.connection.ConnectionGeneration, Sequence: sequence, Ack: fixture.connection.PlatformSequence,
		Kind: attachedworkerprotocol.MessageTerminal, Terminal: &attachedworkerprotocol.TerminalV1{
			Binding: evidence.Binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: evidence.Status, Result: evidence.Result, EvidenceDigest: evidence.EvidenceDigest,
		},
	}
	if err := terminal.Validate(); err != nil {
		t.Fatalf("terminal fixture is invalid: %v", err)
	}
	return fixture, terminal
}

func TestReceiptFinalizingServiceRejectsLegacyTerminalBeforeMutation(t *testing.T) {
	fixture, terminal := receiptTerminalFixture(t)
	exchanges := 0
	broker := &receiptTestBroker{transportAttemptBroker: transportAttemptBroker{
		exchange: func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
			exchanges++
			return ports.AttachedWorkerAttemptResult{}, nil
		},
	}}
	service, err := NewReceiptFinalizingService(receiptServiceConfig(), fixture.store, broker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.exchangeAttemptFrame(context.Background(), fixture.bearer, fixture.connection, terminal)
	if !errors.Is(err, ErrTransportUnauthorized) || exchanges != 0 || broker.commits != 0 {
		t.Fatalf("legacy terminal: error=%v exchanges=%d commits=%d; want unauthorized and zero mutations", err, exchanges, broker.commits)
	}
}

type receiptBackendFailureStore struct {
	ports.AttachedWorkerTransportStore
}

func (store receiptBackendFailureStore) LoadAttachedWorker(context.Context, domain.TenantID, domain.UserID, domain.AttachedWorkerID) (domain.AttachedWorker, bool, error) {
	return domain.AttachedWorker{}, false, errors.New("temporary backend failure")
}

func TestReceiptFinalizingServicePreservesRetryableBackendFailure(t *testing.T) {
	fixture, terminal := receiptTerminalFixture(t)
	exchanges := 0
	broker := &receiptTestBroker{transportAttemptBroker: transportAttemptBroker{
		exchange: func(context.Context, ports.AttachedWorkerAttemptExchange) (ports.AttachedWorkerAttemptResult, error) {
			exchanges++
			return ports.AttachedWorkerAttemptResult{}, nil
		},
	}}
	service, err := NewReceiptFinalizingService(receiptServiceConfig(), receiptBackendFailureStore{fixture.store}, broker)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.exchangeAttemptFrame(context.Background(), fixture.bearer, fixture.connection, terminal)
	if !errors.Is(err, ErrTransportBackend) || exchanges != 0 || broker.commits != 0 {
		t.Fatalf("backend failure: error=%v exchanges=%d commits=%d; want retryable backend and zero mutations", err, exchanges, broker.commits)
	}
}
