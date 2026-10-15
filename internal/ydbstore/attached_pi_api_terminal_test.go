package ydbstore

import (
	"bytes"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const piTerminalMembershipSQL = `SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`
const piTerminalFinalizationSQL = `SELECT terminal_status, content_digest FROM run_finalizations WHERE tenant_id = $1 AND run_id = $2`
const piTerminalLeaseSQL = `SELECT lease_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2`
const piTerminalManifestSQL = `SELECT record FROM attached_worker_capability_manifests WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND capability_digest=$4`
const piTerminalMessageSQL = `SELECT payload FROM attached_worker_attempt_messages WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4 AND direction=$5 AND attempt_sequence=$6`

// Drive the real public terminal method's committed-transport/absent-semantic
// repair path. A transport ACK is not itself canonical success. This path must
// recheck current API authority before creating previously absent finalization.
// The synthetic manifest deliberately lacks output_receipt, so these source
// tests do not claim the genuine Pi receipt/producer/profile integration.
func piTerminalRequest(t *testing.T, g *piUseSourceFixture, status domain.AttachedWorkerTerminalStatus) ports.AttachedWorkerTerminalCommit {
	t.Helper()
	manifest := attachedworkerprotocol.CapabilityManifestV1{WorkerID: string(g.head.WorkerID), EnrollmentGeneration: g.head.EnrollmentGeneration, Revision: 1, ProtocolOffer: attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}, OperatingSystem: "synthetic", Architecture: "synthetic", BuildID: "synthetic", HarnessName: "sessionless", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: bytes.Repeat([]byte{0x41}, 32), IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationProcessBoundary}, Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
	capability, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	g.head.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability))
	g.auth.CapabilityDigest = g.head.CapabilityDigest
	g.loaded.Job.ExecutionPlacementV2.CapabilityDigest = g.head.CapabilityDigest
	placement, err := domain.ExecutionPlacementDigest(g.loaded.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	g.loaded.Job.HarnessBinding.ExecutionPlacementDigest = string(placement)
	g.binding, err = g.loaded.Job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	g.head.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(g.loaded.Job, g.loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	g.auth.ContextDigest = g.head.ContextDigest
	g.connection.CapabilityDigest = g.head.CapabilityDigest
	g.head.State = domain.AttachedWorkerAttemptTerminalCommitted
	g.head.TerminalStatus = status
	g.head.TerminalSequence = 1
	g.head.WorkerAttemptSequence = 2
	g.head.PlatformAttemptSequence = 3
	eventID := domain.SessionEventID("synthetic-pi-terminal-event")
	event := domain.SessionEventDraft{ID: eventID, Kind: domain.SessionEventAssistantMessage, IdempotencyKey: "synthetic-pi-terminal-event-key", Payload: domain.BlobRef{TenantID: g.loaded.Run.TenantID, Key: domain.SessionEventObjectPrefix(g.loaded.Run.TenantID, g.loaded.Run.SessionID, eventID) + "payload.json", SHA256: strings.Repeat("a", 64), Size: 1}, CreatedAt: g.at}
	var materialization ports.AttachedWorkerTerminalMaterialization
	if status == domain.AttachedWorkerTerminalSucceeded {
		resultManifest := g.loaded.InputManifest
		resultManifest.ID = "synthetic-pi-output-manifest"
		materialization.Completion = &ports.WorkerCompletion{TenantID: g.head.TenantID, RunID: g.head.RunID, AttemptID: g.head.AttemptID, ReservationID: g.head.ReservationID, LeaseID: g.head.LeaseID, Fence: g.head.LeaseGeneration, At: g.at, Manifest: resultManifest, Events: []domain.SessionEventDraft{event}}
	} else {
		event.Kind = domain.SessionEventSystemNotice
		materialization.Failure = &ports.WorkerFailure{TenantID: g.head.TenantID, RunID: g.head.RunID, AttemptID: g.head.AttemptID, ReservationID: g.head.ReservationID, LeaseID: g.head.LeaseID, Fence: g.head.LeaseGeneration, At: g.at, Code: "synthetic_failure", Cancelled: status == domain.AttachedWorkerTerminalCancelled, Events: []domain.SessionEventDraft{event}}
	}
	digest, err := attachedWorkerTerminalMaterializationDigest(status, materialization)
	if err != nil {
		t.Fatal(err)
	}
	materialization.EvidenceDigest = digest
	g.head.TerminalEvidenceDigest = digest
	if err := g.head.Validate(); err != nil {
		t.Fatalf("coherent committed transport head: %v", err)
	}
	g.syncOriginalRows(t)
	bucket, err := webBucket(string(g.head.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	member := g.sql.rows[piUseSQL(computeChoiceMembershipSQL)][0]
	g.setRow(t, piTerminalMembershipSQL, []driver.Value{int64(bucket), string(g.head.OwnerUserID), string(g.head.TenantID)}, member)
	g.setRow(t, piTerminalFinalizationSQL, []driver.Value{string(g.head.TenantID), string(g.head.RunID)})
	g.setRow(t, piTerminalLeaseSQL, []driver.Value{string(g.head.TenantID), string(g.head.RunID)}, string(g.lease.ID), int64(g.lease.FenceToken), g.lease.ExpiresAt)
	g.setRow(t, `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, []driver.Value{string(g.head.TenantID), string(g.loaded.Run.SessionID)})
	g.setRow(t, `SELECT run_id FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2`, []driver.Value{string(g.head.TenantID), string(g.loaded.Run.IdempotencyKey)}, string(g.head.RunID))
	g.setRow(t, `SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, []driver.Value{string(g.head.TenantID), string(g.head.RunID)})
	// loadWorkerTerminalState uses its original spacing around predicates.
	g.setJSON(t, `SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2`, []driver.Value{string(g.head.TenantID), string(g.head.ReservationID)}, g.loaded.Reservation)
	record := domain.AttachedWorkerCapabilityManifest{Version: 1, TenantID: g.head.TenantID, OwnerUserID: g.head.OwnerUserID, WorkerID: g.head.WorkerID, EnrollmentGeneration: g.head.EnrollmentGeneration, ManifestRevision: 1, Digest: g.head.CapabilityDigest, ProtocolVersion: 1, IdentityKeyDigest: g.connection.ManifestIdentityKey, ManifestPayload: []byte(choiceFixtureJSON(t, manifest))}
	if err := record.Validate(); err != nil {
		t.Fatal(err)
	}
	g.setJSON(t, piTerminalManifestSQL, []driver.Value{string(g.head.TenantID), string(g.head.OwnerUserID), string(g.head.WorkerID), string(g.head.CapabilityDigest)}, record)
	contextDigest, _ := hex.DecodeString(string(g.head.ContextDigest))
	policy, _ := hex.DecodeString(string(g.head.PolicyDigest))
	evidence, _ := hex.DecodeString(string(digest))
	frame := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 4), WorkerID: string(g.head.WorkerID), EnrollmentGeneration: g.head.EnrollmentGeneration, ConnectionGeneration: g.head.ConnectionGeneration, Sequence: 4, Ack: 5, Kind: attachedworkerprotocol.MessageTerminalAck, TerminalAck: &attachedworkerprotocol.TerminalAckV1{Binding: attachedworkerprotocol.AttemptBindingV1{RunID: string(g.head.RunID), AttemptID: string(g.head.AttemptID), LeaseID: string(g.head.LeaseID), LeaseGeneration: g.head.LeaseGeneration, FenceToken: string(g.head.FenceToken), ExpiresAtUnixMicro: g.head.LeaseExpiresAt.UnixMicro(), ContextDigest: contextDigest, CapabilityDigest: capability, PolicyDigest: policy}, AttemptSequence: 3, TerminalSequence: 1, Status: attachedworkerprotocol.TerminalStatus(status), Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: evidence}}
	if status == domain.AttachedWorkerTerminalFailed {
		frame.TerminalAck.Result = attachedworkerprotocol.TerminalResultFailed
	}
	if status == domain.AttachedWorkerTerminalCancelled {
		frame.TerminalAck.Result = attachedworkerprotocol.TerminalResultCancelled
	}
	message, err := attachedWorkerAttemptMessageFromFrame(g.head, attachedworkerprotocol.DirectionPlatformToWorker, frame, g.at, time.Time{})
	if err != nil {
		t.Fatalf("canonical ACK fixture: %v", err)
	}
	g.setJSON(t, piTerminalMessageSQL, []driver.Value{string(g.head.TenantID), string(g.head.OwnerUserID), string(g.head.WorkerID), string(g.head.AttemptID), string(domain.AttachedWorkerAttemptPlatformToWorker), int64(g.head.PlatformAttemptSequence)}, message)
	r := ports.AttachedWorkerTerminalCommit{TenantID: g.head.TenantID, OwnerUserID: g.head.OwnerUserID, WorkerID: g.head.WorkerID, AttemptID: g.head.AttemptID, LeaseGeneration: g.head.LeaseGeneration, Materialization: materialization}
	if err := validateAttachedWorkerTerminalCommit(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPiAPITerminalActualNewSuccessRechecksSourceBeforeCanonicalWrite(t *testing.T) {
	for _, mode := range []string{"current", "nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion"} {
		t.Run(mode, func(t *testing.T) {
			g, f := piNewWorkSource(t)
			r := piTerminalRequest(t, g, domain.AttachedWorkerTerminalSucceeded)
			if mode != "current" {
				piNewWorkChangeAuthority(t, g, mode)
			}
			beforeTx, beforeCommits := g.sql.transactions, g.sql.commits
			got, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r)
			if g.sql.transactions != beforeTx+1 || g.sql.active || g.sql.commits != beforeCommits || got.Status == ports.AttachedWorkerExecutionApplied || got.Status == ports.AttachedWorkerExecutionReplayed {
				t.Fatalf("mode=%s speculative semantic result: status=%s err=%v", mode, got.Status, err)
			}
			if mode == "current" {
				if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 || !strings.Contains(f.writeSQL, "INTO runs ") {
					t.Fatalf("current source must reach original Run write: writes=%d query=%s err=%v", f.firstWrites, f.writeSQL, err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || f.firstWrites != 0 {
				t.Fatalf("mode=%s stale success reached canonical write: writes=%d err=%v", mode, f.firstWrites, err)
			}
		})
	}
}

func TestPiAPITerminalCommittedHistoryAndFailureDoNotRequireNewAPIPermission(t *testing.T) {
	for _, status := range []domain.AttachedWorkerTerminalStatus{domain.AttachedWorkerTerminalSucceeded, domain.AttachedWorkerTerminalFailed, domain.AttachedWorkerTerminalCancelled} {
		t.Run(string(status), func(t *testing.T) {
			g, f := piNewWorkSource(t)
			r := piTerminalRequest(t, g, status)
			piNewWorkChangeAuthority(t, g, "revoked")
			g.store.attachedPiAPIPolicyCatalog = nil
			if status == domain.AttachedWorkerTerminalSucceeded {
				g.setRow(t, piTerminalFinalizationSQL, []driver.Value{string(g.head.TenantID), string(g.head.RunID)}, string(domain.RunSucceeded), string(r.Materialization.EvidenceDigest))
				got, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r)
				if err != nil || got.Status != ports.AttachedWorkerExecutionReplayed || got.Outbound == nil || f.firstWrites != 0 {
					t.Fatalf("committed history lost ACK: status=%s writes=%d err=%v", got.Status, f.firstWrites, err)
				}
				for _, q := range g.sql.transactionQueries {
					if q == piUseSQL(piPolicyReadSQL) {
						t.Fatal("historical ACK consulted today's API decision")
					}
				}
			} else {
				_, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r)
				if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 {
					t.Fatalf("failure/cancellation stranded by revoked API: writes=%d err=%v", f.firstWrites, err)
				}
			}
		})
	}
}

func TestPiAPITerminalRestoredCurrentSourceReachesOriginalEffect(t *testing.T) {
	g, f := piNewWorkSource(t)
	r := piTerminalRequest(t, g, domain.AttachedWorkerTerminalSucceeded)
	row := append([]driver.Value(nil), g.sql.rows[piUseSQL(providerCredentialReadSQL)]...)
	piNewWorkChangeAuthority(t, g, "api_generation")
	if _, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r); !errors.Is(err, domain.ErrMembershipDenied) || f.firstWrites != 0 {
		t.Fatalf("rotated current source: %v", err)
	}
	g.sql.rows[piUseSQL(providerCredentialReadSQL)] = row
	if _, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r); !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 {
		t.Fatalf("restored lawful source: %v", err)
	}
}

func TestPiAPITerminalUnknownCommitDoesNotReturnSpeculativeACK(t *testing.T) {
	g, f := piNewWorkSource(t)
	r := piTerminalRequest(t, g, domain.AttachedWorkerTerminalSucceeded)
	g.setRow(t, piTerminalFinalizationSQL, []driver.Value{string(g.head.TenantID), string(g.head.RunID)},
		string(domain.RunSucceeded), string(r.Materialization.EvidenceDigest))
	g.store.attachedPiAPIPolicyCatalog = nil
	g.sql.unknownCommit = true
	got, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r)
	if err == nil || !reflect.DeepEqual(got, ports.AttachedWorkerAttemptResult{}) || f.firstWrites != 0 {
		t.Fatalf("unknown commit leaked speculative historical ACK: status=%s err=%v", got.Status, err)
	}
	g.sql.unknownCommit = false
	got, err = g.store.CommitAttachedWorkerTerminal(g.ctx, r)
	if err != nil || got.Status != ports.AttachedWorkerExecutionReplayed || got.Outbound == nil || f.firstWrites != 0 {
		t.Fatalf("exact original history lost after unknown read-only commit: status=%s err=%v", got.Status, err)
	}
}

const piPendingTerminalMessagesSQL = `SELECT payload FROM attached_worker_attempt_messages
 WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4
 AND direction=$5 AND kind=$6 ORDER BY attempt_sequence LIMIT 65`

// Build the actual protocol/provenance chain consumed by the OTHER public
// terminal entry: ready -> offer -> claim -> accepted -> worker Terminal.
// Neither a terminal enum nor a caller-supplied summary replaces that chain.
func piPendingTerminalRequest(t *testing.T, g *piUseSourceFixture) ports.AttachedWorkerTerminalCommit {
	t.Helper()
	r := piTerminalRequest(t, g, domain.AttachedWorkerTerminalSucceeded)
	config, ready := piNewWorkReadyProtocol(t, g)
	contextDigest, err := hex.DecodeString(string(g.head.ContextDigest))
	if err != nil {
		t.Fatal(err)
	}
	policyDigest, err := hex.DecodeString(string(g.head.PolicyDigest))
	if err != nil {
		t.Fatal(err)
	}
	offer, offered, err := attachedworkerprotocol.BuildLeaseOfferTransitionV1(config, ready, attachedworkerprotocol.LeaseOfferAuthorityV1{
		RunID: string(g.head.RunID), AttemptID: string(g.head.AttemptID), LeaseID: string(g.head.LeaseID),
		LeaseGeneration: g.head.LeaseGeneration, NowUnixMicro: g.at.UnixMicro(), ExpiresAtUnixMicro: g.head.LeaseExpiresAt.UnixMicro(),
		ContextDigest: contextDigest, PolicyDigest: policyDigest,
	})
	if err != nil || offer.LeaseOffer == nil || offer.LeaseOffer.Binding.FenceToken != string(g.head.FenceToken) {
		t.Fatalf("genuine synthetic offer transition: %v", err)
	}
	claim := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, offered.Worker.Sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration, ConnectionGeneration: config.Auth.ConnectionGeneration,
		Sequence: offered.Worker.Sequence + 1, Ack: offered.Platform.Sequence, Kind: attachedworkerprotocol.MessageLeaseClaim,
		LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: offer.LeaseOffer.Binding, AttemptSequence: 1}}
	claimed, err := attachedworkerprotocol.ApplyMachineFrameV1(config, offered, attachedworkerprotocol.DirectionWorkerToPlatform, claim, g.at.UnixMicro())
	if err != nil {
		t.Fatalf("genuine synthetic claim transition: %v", err)
	}
	_, accepted, err := attachedworkerprotocol.BuildLeaseAcceptedTransitionV1(config, claimed, attachedworkerprotocol.LeaseAcceptedAuthorityV1{NowUnixMicro: g.at.UnixMicro()})
	if err != nil {
		t.Fatalf("genuine synthetic accepted transition: %v", err)
	}
	evidence, err := hex.DecodeString(string(r.Materialization.EvidenceDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminal := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, accepted.Worker.Sequence+1),
		WorkerID: config.Auth.WorkerID, EnrollmentGeneration: config.Auth.EnrollmentGeneration, ConnectionGeneration: config.Auth.ConnectionGeneration,
		Sequence: accepted.Worker.Sequence + 1, Ack: accepted.Platform.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{Binding: offer.LeaseOffer.Binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: evidence}}
	pending, err := attachedworkerprotocol.ApplyMachineFrameV1(config, accepted, attachedworkerprotocol.DirectionWorkerToPlatform, terminal, g.at.UnixMicro())
	if err != nil || pending.Attempt.Summary.State != attachedworkerprotocol.AttemptTerminalPending {
		t.Fatalf("genuine synthetic pending-terminal transition: %v", err)
	}
	g.head.State = domain.AttachedWorkerAttemptTerminalPending
	g.head.PlatformAttemptSequence = pending.Attempt.Platform.Sequence
	g.head.WorkerAttemptSequence = pending.Attempt.Worker.Sequence
	g.head.TerminalSequence = pending.Attempt.Summary.TerminalSequence
	g.head.Revision++
	g.connection.ProtocolSnapshot, err = attachedworkerprotocol.EncodeMachineSnapshotV1(pending)
	if err != nil {
		t.Fatal(err)
	}
	g.connection.PlatformSequence, g.connection.PlatformAck = pending.Platform.Sequence, pending.Platform.Ack
	g.connection.WorkerSequence, g.connection.WorkerAck = pending.Worker.Sequence, pending.Worker.Ack
	if err := g.head.Validate(); err != nil {
		t.Fatalf("pending original head: %v", err)
	}
	if err := g.connection.Validate(); err != nil {
		t.Fatalf("pending original connection: %v", err)
	}
	g.syncOriginalRows(t)
	manifest := domain.AttachedWorkerCapabilityManifest{Version: 1, TenantID: g.head.TenantID, OwnerUserID: g.head.OwnerUserID, WorkerID: g.head.WorkerID,
		EnrollmentGeneration: g.head.EnrollmentGeneration, ManifestRevision: pending.Manifest.Revision, Digest: g.head.CapabilityDigest,
		ProtocolVersion: 1, IdentityKeyDigest: g.connection.ManifestIdentityKey, ManifestPayload: []byte(choiceFixtureJSON(t, pending.Manifest))}
	g.setJSON(t, piTerminalManifestSQL, []driver.Value{string(g.head.TenantID), string(g.head.OwnerUserID), string(g.head.WorkerID), string(g.head.CapabilityDigest)}, manifest)
	message, err := attachedWorkerAttemptMessageFromFrame(g.head, attachedworkerprotocol.DirectionWorkerToPlatform, terminal, g.at, time.Time{})
	if err != nil {
		t.Fatalf("stored original worker Terminal: %v", err)
	}
	reservation, execution, err := selectAttachedWorkerTerminalProvenance(g.head, pending.Attempt.Summary, []domain.AttachedWorkerAttemptMessageV1{message})
	if err != nil || reservation != g.head.ReservationID || execution != g.head.ExecutionConnectionID {
		t.Fatalf("exact original terminal provenance: %v", err)
	}
	g.setJSON(t, piPendingTerminalMessagesSQL, []driver.Value{string(g.head.TenantID), string(g.head.OwnerUserID), string(g.head.WorkerID), string(g.head.AttemptID),
		string(domain.AttachedWorkerAttemptWorkerToPlatform), string(domain.AttachedWorkerAttemptMessageTerminal)}, message)
	return r
}

func TestPiAPITerminalActualPendingProtocolRechecksCurrentSourceBeforeCanonicalWrite(t *testing.T) {
	for _, mode := range []string{"current", "revoked", "replaced", "api_generation"} {
		t.Run(mode, func(t *testing.T) {
			g, f := piNewWorkSource(t)
			r := piPendingTerminalRequest(t, g)
			if mode != "current" {
				piNewWorkChangeAuthority(t, g, mode)
			}
			beforeTx, beforeCommits := g.sql.transactions, g.sql.commits
			got, err := g.store.CommitAttachedWorkerTerminal(g.ctx, r)
			if g.sql.transactions != beforeTx+1 || g.sql.active || g.sql.commits != beforeCommits ||
				got.Status == ports.AttachedWorkerExecutionApplied || got.Status == ports.AttachedWorkerExecutionReplayed {
				t.Fatalf("mode=%s pending transaction escaped first-write rollback: status=%s err=%v", mode, got.Status, err)
			}
			provenanceReads := 0
			for _, q := range g.sql.transactionQueries {
				if q == piUseSQL(piPendingTerminalMessagesSQL) {
					provenanceReads++
				}
			}
			if provenanceReads != 1 {
				t.Fatalf("mode=%s did not traverse actual pending protocol/provenance entry: reads=%d err=%v", mode, provenanceReads, err)
			}
			if mode == "current" {
				if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 || !strings.Contains(f.writeSQL, "INTO runs ") {
					t.Fatalf("lawful pending source must reach original canonical Run write: writes=%d query=%s err=%v", f.firstWrites, f.writeSQL, err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || f.firstWrites != 0 {
				t.Fatalf("mode=%s pending stale source reached canonical effect: writes=%d err=%v", mode, f.firstWrites, err)
			}
		})
	}
}
