//go:build darwin || linux

package attachedworkerstack_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	transport "gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type preparedReceiptPeer struct {
	snapshot  attachedworkersession.SnapshotV1
	binding   attachedworkerprotocol.AttemptBindingV1
	sealed    []byte
	retired   bool
	terminals int
	loseAck   bool
}

func (peer *preparedReceiptPeer) Snapshot() attachedworkersession.SnapshotV1 { return peer.snapshot }
func (peer *preparedReceiptPeer) SealReceiptSubmission(_ context.Context, body []byte) error {
	if len(peer.sealed) != 0 && !bytes.Equal(peer.sealed, body) {
		return errors.New("divergent original receipt")
	}
	peer.sealed = bytes.Clone(body)
	return nil
}
func (peer *preparedReceiptPeer) RetireReceiptSubmission(_ context.Context, body []byte) error {
	if !bytes.Equal(peer.sealed, body) {
		return errors.New("wrong original receipt retirement")
	}
	peer.retired = true
	return nil
}
func (peer *preparedReceiptPeer) ExchangeAction(_ context.Context, action attachedworkersession.ActionV1) (*attachedworkerprotocol.FrameV1, error) {
	frame := &attachedworkerprotocol.FrameV1{Version: 1, WorkerID: string(peer.snapshot.WorkerID), EnrollmentGeneration: peer.snapshot.EnrollmentGeneration, ConnectionGeneration: peer.snapshot.ConnectionGeneration}
	switch {
	case action.Heartbeat != nil:
		frame.Sequence = 3
		frame.Kind = attachedworkerprotocol.MessageLeaseOffer
		frame.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: peer.binding, AttemptSequence: 1}
	case action.LeaseClaim != nil:
		frame.Sequence = 4
		frame.Kind = attachedworkerprotocol.MessageLeaseAccepted
		frame.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: peer.binding, AttemptSequence: 2}
	case action.Terminal != nil:
		peer.terminals++
		if peer.loseAck {
			return nil, errors.New("fixture lost original ACK")
		}
		frame.Sequence = 6
		frame.Kind = attachedworkerprotocol.MessageTerminalAck
		frame.TerminalAck = &attachedworkerprotocol.TerminalAckV1{Binding: peer.binding, AttemptSequence: 3, TerminalSequence: action.Terminal.TerminalSequence, Status: action.Terminal.Status, Result: action.Terminal.Result, EvidenceDigest: bytes.Clone(action.Terminal.EvidenceDigest)}
	default:
		return nil, errors.New("unexpected original-peer action")
	}
	frame.Ack = frame.Sequence
	frame.MessageID = attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, frame.Sequence)
	return frame, nil
}

type preparedReceiptMaterializer struct {
	invocation attachedworkerdaemon.Invocation
}

func (m *preparedReceiptMaterializer) Materialize(_ context.Context, request transport.MaterializationRequestV1) (transport.MaterializedInputV1, error) {
	if request.Attempt.RunID != string(m.invocation.Identity.RunID) || request.Attempt.AttemptID != string(m.invocation.Identity.AttemptID) {
		return transport.MaterializedInputV1{}, errors.New("foreign materialization")
	}
	input, err := m.invocation.VerifiedInput.Clone()
	if err != nil {
		return transport.MaterializedInputV1{}, err
	}
	return transport.MaterializedInputV1{Stdin: []byte("synthetic envelope must not become authority"), Credential: m.invocation.Credential, VerifiedInput: input}, nil
}

// This is a bounded receipt/server-peer fixture, not native YDB finalization
// or actual platform isolation. Process and lifecycle observations are genuine.
type preparedReceiptPublisher struct {
	first        []byte
	calls        int
	loseResponse bool
}

func (publisher *preparedReceiptPublisher) Publish(_ context.Context, submission transport.ReceiptSubmissionV1) (transport.ReceiptCommitmentV1, error) {
	publisher.calls++
	body, err := json.Marshal(submission)
	if err != nil {
		return transport.ReceiptCommitmentV1{}, err
	}
	if len(publisher.first) == 0 {
		publisher.first = bytes.Clone(body)
	} else if !bytes.Equal(publisher.first, body) {
		return transport.ReceiptCommitmentV1{}, errors.New("divergent same-run submission")
	}
	if submission.Observation.ValidateFor(submission.Candidate.Status, true) != nil {
		return transport.ReceiptCommitmentV1{}, errors.New("invalid actual process/finalization observation")
	}
	if publisher.loseResponse {
		return transport.ReceiptCommitmentV1{}, errors.New("fixture lost immutable receipt response")
	}
	digest := sha256.Sum256(body)
	return transport.ReceiptCommitmentV1{Status: submission.Candidate.Status, CanonicalDigest: digest[:]}, nil
}

func TestNormalPreparedStackJoinsOriginalReceiptAndACKWithoutResend(t *testing.T) {
	stack, original, capability := attachedworkerstack.PreparedCodexFixtureForTest(t)
	head := original.VerifiedInput.AcceptedAttempt
	decode := func(value string) []byte {
		body, err := hex.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	capabilityDigest, err := attachedworkerprotocol.ManifestDigestV1(capability)
	if err != nil {
		t.Fatal(err)
	}
	// This is the capability authenticated by the original Connector, not a
	// second authority installed by the receipt peer.
	input := original.VerifiedInput
	input.Job.ExecutionPlacementV2.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest))
	placementDigest, err := domain.ExecutionPlacementDigest(input.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	input.Job.HarnessBinding.ExecutionPlacementDigest = string(placementDigest)
	input.AcceptedAttempt.CapabilityDigest = input.Job.ExecutionPlacementV2.CapabilityDigest
	input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	head = input.AcceptedAttempt
	now := head.CreatedAt
	expires := head.LeaseExpiresAt
	peer := &preparedReceiptPeer{snapshot: attachedworkersession.SnapshotV1{Version: 1, State: attachedworkersession.StateReady, TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, EnrollmentGeneration: head.EnrollmentGeneration, ConnectionGeneration: head.ConnectionGeneration, ProtocolVersion: 1, ConnectionID: head.ConnectionID, CapabilityDigest: head.CapabilityDigest, AuthenticationExpires: &expires},
		binding: attachedworkerprotocol.AttemptBindingV1{RunID: string(head.RunID), AttemptID: string(head.AttemptID), LeaseID: string(head.LeaseID), LeaseGeneration: head.LeaseGeneration, FenceToken: string(head.FenceToken), ExpiresAtUnixMicro: expires.UnixMicro(), ContextDigest: decode(string(head.ContextDigest)), CapabilityDigest: capabilityDigest, PolicyDigest: decode(string(head.PolicyDigest))}}
	publisher := &preparedReceiptPublisher{loseResponse: true}
	materializationRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := transport.New(peer, &preparedReceiptMaterializer{invocation: original}, transport.Config{Profile: transport.LocalProfileV1{Name: "prepared-codex-fixture", CapabilityDigest: head.CapabilityDigest, Executable: original.Process.Executable, ExecutableDigest: original.Process.ExecutableDigest, Arguments: original.Process.Arguments},
		CapabilityManifest: &capability, ReceiptPublisher: publisher, ReceiptCandidate: stack.ReceiptCandidate, MaterializationRoot: materializationRoot, MaxInputBytes: 1 << 20, ReportTimeout: 5 * time.Second, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	invocation, available, err := adapter.Next(context.Background())
	if err != nil || !available {
		t.Fatalf("accepted Next available=%t err=%v", available, err)
	}
	defer invocation.VerifiedInput.Clear()
	result, runErr := stack.Run(context.Background(), invocation)
	if !result.Succeeded(runErr) {
		t.Fatalf("real prepared result=%+v err=%v", result, runErr)
	}
	if err := adapter.Complete(context.Background(), invocation.Identity, result, runErr); !errors.Is(err, transport.ErrReconciliationRequired) || peer.terminals != 0 || peer.retired {
		t.Fatalf("lost receipt Complete=%v terminals=%d retired=%t", err, peer.terminals, peer.retired)
	}
	publisher.loseResponse = false
	peer.loseAck = true
	if err := adapter.Complete(context.Background(), invocation.Identity, result, runErr); !errors.Is(err, transport.ErrReconciliationRequired) || peer.retired {
		t.Fatalf("lost ACK Complete=%v retired=%t", err, peer.retired)
	}
	peer.loseAck = false
	if err := adapter.Complete(context.Background(), invocation.Identity, result, runErr); err != nil {
		t.Fatalf("exact original receipt/ACK replay: %v", err)
	}
	if !peer.retired || publisher.calls != 3 || peer.terminals != 2 {
		t.Fatalf("same original receipt recovery retired=%t publications=%d terminals=%d", peer.retired, publisher.calls, peer.terminals)
	}
	if _, err := stack.Run(context.Background(), invocation); err == nil {
		t.Fatal("receipt/ACK recovery granted provider resend")
	}
	if err := stack.RetireReceipt(invocation.Identity); err != nil {
		t.Fatal(err)
	}
}
