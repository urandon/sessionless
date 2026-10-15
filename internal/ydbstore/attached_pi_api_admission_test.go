package ydbstore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var errPiNewWorkFirstWrite = errors.New("synthetic source reached original first write")

// A passing current-source snapshot reaches the real method's original SQL
// effect, where this driver stops and rolls back. This is NOT a committed
// scheduler/terminal result or a native YDB isolation/qualification proof.
type piNewWorkSQLSource struct {
	*piUseSQLSource
	firstWrites int
	writeSQL    string
	writeArgs   []driver.NamedValue
}

func (f *piNewWorkSQLSource) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *piNewWorkSQLSource) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	if f.writesAllowed && strings.Contains(q, " INTO attached_pi_api_policy_v1 ") {
		return f.piUseSQLSource.ExecContext(ctx, q, args)
	}
	if err := f.bounded(ctx); err != nil {
		return nil, err
	}
	if !f.active {
		return nil, errors.New("new work escaped original transaction")
	}
	f.firstWrites++
	f.writeSQL = piUseSQL(q)
	f.writeArgs = append([]driver.NamedValue(nil), args...)
	return nil, errPiNewWorkFirstWrite
}
func piNewWorkSource(t *testing.T) (*piUseSourceFixture, *piNewWorkSQLSource) {
	t.Helper()
	g := newPiUseSourceFixture(t)
	f := &piNewWorkSQLSource{piUseSQLSource: g.sql}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	g.store.db = db
	return g, f
}

// A real conformance-machine handshake with test-owned deterministic signing
// bytes, never user keys. Rebinds only compute capability/placement/context
// projections; P/E/T/R/M and the original Run/attempt/lease remain unchanged.
// The ready machine has an idle protocol attempt, suitable for actual Offer
// or receipt feature checks, not proof of a claimed native execution.
func piNewWorkReadyProtocol(t *testing.T, g *piUseSourceFixture, features ...attachedworkerprotocol.ProtocolFeatureV1) (attachedworkerprotocol.MachineConfig, attachedworkerprotocol.MachineSnapshotV1) {
	t.Helper()
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x17}, ed25519.SeedSize))
	offer := attachedworkerprotocol.VersionOfferV1{Window: attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []attachedworkerprotocol.ProtocolVersion{1}}
	channel, err := hex.DecodeString(string(g.connection.ChannelBinding))
	if err != nil {
		t.Fatal(err)
	}
	auth := attachedworkerprotocol.AuthContextV1{TenantID: string(g.head.TenantID), OwnerUserID: string(g.head.OwnerUserID), WorkerID: string(g.head.WorkerID), IdentityPublicKey: private.Public().(ed25519.PublicKey), EnrollmentGeneration: g.head.EnrollmentGeneration, ConnectionGeneration: g.head.ConnectionGeneration, Version: 1, ChannelBinding: channel}
	manifest := attachedworkerprotocol.CapabilityManifestV1{WorkerID: auth.WorkerID, EnrollmentGeneration: auth.EnrollmentGeneration, Revision: 1, ProtocolOffer: offer, OperatingSystem: "synthetic", Architecture: "synthetic", BuildID: "synthetic-ready", HarnessName: "sessionless", HarnessVersion: "1", HarnessSurface: attachedworkerprotocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: bytes.Repeat([]byte{0x41}, 32), IsolationEvidence: []attachedworkerprotocol.IsolationEvidenceV1{attachedworkerprotocol.IsolationFilesystemBoundary, attachedworkerprotocol.IsolationProcessBoundary}, Features: append([]attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureCancellation, attachedworkerprotocol.FeatureProgress, attachedworkerprotocol.FeatureReconnect}, features...), MaxConcurrentAttempts: 1}
	slices.Sort(manifest.Features)
	digest, err := attachedworkerprotocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	config := attachedworkerprotocol.MachineConfig{Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{1}}
	machine, err := attachedworkerprotocol.NewConformanceMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	var w, p uint64
	frame := func(direction attachedworkerprotocol.Direction, kind attachedworkerprotocol.MessageKind) attachedworkerprotocol.FrameV1 {
		seq, ack := &w, p
		if direction == attachedworkerprotocol.DirectionPlatformToWorker {
			seq, ack = &p, w
		}
		*seq++
		return attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(direction, *seq), WorkerID: auth.WorkerID, EnrollmentGeneration: auth.EnrollmentGeneration, ConnectionGeneration: auth.ConnectionGeneration, Sequence: *seq, Ack: ack, Kind: kind}
	}
	accept := func(direction attachedworkerprotocol.Direction, f attachedworkerprotocol.FrameV1) {
		t.Helper()
		if err := machine.Accept(direction, f, attachedworkerprotocol.AcceptanceContextV1{ChannelBinding: channel, NowUnixMicro: g.at.UnixMicro()}); err != nil {
			t.Fatalf("real synthetic handshake %s: %v", f.Kind, err)
		}
	}
	wn, pn := bytes.Repeat([]byte{0x51}, 32), bytes.Repeat([]byte{0x61}, 32)
	f := frame(attachedworkerprotocol.DirectionWorkerToPlatform, attachedworkerprotocol.MessageHello)
	f.Hello = &attachedworkerprotocol.HelloV1{Offer: offer, WorkerNonce: wn}
	accept(attachedworkerprotocol.DirectionWorkerToPlatform, f)
	f = frame(attachedworkerprotocol.DirectionPlatformToWorker, attachedworkerprotocol.MessageChallenge)
	f.Challenge = &attachedworkerprotocol.ChallengeV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: wn, PlatformNonce: pn}
	accept(attachedworkerprotocol.DirectionPlatformToWorker, f)
	f = frame(attachedworkerprotocol.DirectionWorkerToPlatform, attachedworkerprotocol.MessageAttach)
	f.Attach = &attachedworkerprotocol.AttachV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: wn, PlatformNonce: pn, CapabilityDigest: digest}
	if err := attachedworkerprotocol.SignAttachV1(private, auth, &f); err != nil {
		t.Fatal(err)
	}
	accept(attachedworkerprotocol.DirectionWorkerToPlatform, f)
	f = frame(attachedworkerprotocol.DirectionPlatformToWorker, attachedworkerprotocol.MessageAttachAccepted)
	f.AttachAccepted = &attachedworkerprotocol.AttachAcceptedV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: wn, PlatformNonce: pn, CapabilityDigest: digest}
	accept(attachedworkerprotocol.DirectionPlatformToWorker, f)
	f = frame(attachedworkerprotocol.DirectionWorkerToPlatform, attachedworkerprotocol.MessageManifest)
	f.Manifest = &attachedworkerprotocol.ManifestV1{Manifest: manifest, Digest: digest}
	if err := attachedworkerprotocol.SignManifestV1(private, auth, &f); err != nil {
		t.Fatal(err)
	}
	accept(attachedworkerprotocol.DirectionWorkerToPlatform, f)
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := attachedworkerprotocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	g.worker.IdentityPublicKey = append([]byte(nil), auth.IdentityPublicKey...)
	g.connection.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest))
	g.connection.ProtocolSnapshot = encoded
	g.connection.PlatformSequence, g.connection.PlatformAck = snapshot.Platform.Sequence, snapshot.Platform.Ack
	g.connection.WorkerSequence, g.connection.WorkerAck = snapshot.Worker.Sequence, snapshot.Worker.Ack
	g.head.CapabilityDigest, g.auth.CapabilityDigest = g.connection.CapabilityDigest, g.connection.CapabilityDigest
	g.loaded.Job.ExecutionPlacementV2.CapabilityDigest = g.connection.CapabilityDigest
	pd, err := domain.ExecutionPlacementDigest(g.loaded.Job.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	g.loaded.Job.HarnessBinding.ExecutionPlacementDigest = string(pd)
	g.binding, err = g.loaded.Job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	g.head.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(g.loaded.Job, g.loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	g.auth.ContextDigest = g.head.ContextDigest
	g.syncOriginalRows(t)
	return config, snapshot
}

func piNewWorkChangeAuthority(t *testing.T, g *piUseSourceFixture, mode string) {
	t.Helper()
	switch mode {
	case "nil_catalog":
		g.store.attachedPiAPIPolicyCatalog = nil
	case "revoked", "replaced":
		g.sql.writesAllowed = true
		if mode == "revoked" {
			r := ports.RevokeAttachedPiAPIPolicyV1{ReadAttachedPiAPIPolicyV1: g.request.ReadAttachedPiAPIPolicyV1, CSRFTokenDigest: g.request.CSRFTokenDigest, ExpectedGeneration: g.decision.Generation, ExpectedMapping: g.decision.MappingDigest}
			if err := g.store.RevokeAttachedPiAPIPolicy(g.ctx, r); err != nil {
				t.Fatal(err)
			}
		} else {
			r := g.request
			r.MutationID = "synthetic-replacement-before-new-work"
			r.ExpectedGeneration = g.decision.Generation
			g.sql.decisionKey, _ = piDecisionKey(r.MutationID)
			if next, err := g.store.ConfirmAttachedPiAPIPolicy(g.ctx, r); err != nil || next.MappingDigest == g.decision.MappingDigest {
				t.Fatalf("replacement: %v", err)
			}
		}
		g.sql.writesAllowed = false
		g.sql.writeAttempts = 0
	case "api_generation":
		row := g.sql.rows[piUseSQL(providerCredentialReadSQL)]
		var v domain.ProviderCredentialBindingV1
		if err := json.Unmarshal([]byte(row[7].(string)), &v); err != nil {
			t.Fatal(err)
		}
		v.CredentialGeneration++
		row[1], row[7] = int64(v.CredentialGeneration), choiceFixtureJSON(t, v)
	case "membership_version":
		row := g.sql.rows[piUseSQL(computeChoiceMembershipSQL)]
		var m domain.TenantMembership
		if err := json.Unmarshal([]byte(row[0].(string)), &m); err != nil {
			t.Fatal(err)
		}
		m.SecurityVersion++
		row[0], row[3] = choiceFixtureJSON(t, m), int64(m.SecurityVersion)
	case "session_deletion":
		g.sql.rows[piUseSQL(piUseDeletionSQL)] = []driver.Value{string(g.loaded.Run.SessionID)}
	case "expired_at_database_time":
		g.sql.rows[piUseSQL(`SELECT CurrentUtcTimestamp()`)] = []driver.Value{g.at.Add(2 * time.Hour)}
	default:
		t.Fatalf("unknown authority mutation %s", mode)
	}
}

func piAdmissionRequest(t *testing.T, g *piUseSourceFixture) ports.DispatchAdmissionRequest {
	t.Helper()
	g.loaded.Run.Status = domain.RunCreated
	g.loaded.Run.StartedAt = nil
	g.loaded.Attempt.Status = domain.AttemptCreated
	g.syncOriginalRows(t)
	j := g.loaded.Job
	o := domain.DispatchOutbox{ID: "synthetic-pi-dispatch", TenantID: j.TenantID, RunID: j.RunID, AttemptID: j.AttemptID, InputManifestID: j.InputManifestID, ContextSnapshot: j.ContextSnapshot, CredentialOwnerUserID: j.CredentialOwnerUserID, ExecutionPlacementV2: j.ExecutionPlacementV2, HarnessBinding: j.HarnessBinding, DeliveryChat: j.DeliveryChat, ReplyToMessageID: j.ReplyToMessageID, Status: domain.DispatchPending, IdempotencyKey: "synthetic-pi-dispatch-key", CreatedAt: g.at, UpdatedAt: g.at}
	if err := o.ValidateForAttempt(g.loaded.Run, g.loaded.Attempt); err != nil {
		t.Fatal(err)
	}
	g.setJSON(t, `SELECT payload FROM dispatch_outbox WHERE tenant_id = $1 AND dispatch_outbox_id = $2`, []driver.Value{string(j.TenantID), string(o.ID)}, o)
	connectionKey := []driver.Value{string(j.TenantID), string(g.loaded.Run.SubscriptionConnectionID)}
	g.setRow(t, `SELECT entitlement_state, quota_state FROM subscription_connections WHERE tenant_id = $1 AND subscription_connection_id = $2`, connectionKey, string(domain.EntitlementActive), string(domain.ProviderQuotaAvailable))
	g.setRow(t, `SELECT state, active_run_id, active_reservation_id, blocked_until, updated_at FROM subscription_scheduler_slots WHERE tenant_id = $1 AND subscription_connection_id = $2`, connectionKey)
	// Deliberately stale queue time: the API branch must use the DB sample.
	r := ports.DispatchAdmissionRequest{TenantID: j.TenantID, OutboxID: o.ID, RunID: j.RunID, AttemptID: j.AttemptID, ReservationID: j.ReservationID, Now: g.at.Add(-time.Second), HoldUntil: g.at.Add(4 * time.Hour), Limits: j.Limits, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if err := validateAdmissionRequest(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestPiAPIAdmissionActualDispatchChecksCurrentSourceBeforeColdSlotWrite(t *testing.T) {
	for _, mode := range []string{"current", "nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion", "expired_at_database_time", "foreign_stored_run", "cancelled_run"} {
		t.Run(mode, func(t *testing.T) {
			g, f := piNewWorkSource(t)
			r := piAdmissionRequest(t, g)
			if mode == "foreign_stored_run" {
				g.loaded.Run.TenantID = "foreign-tenant"
				g.setJSON(t, piUseRunSQL, []driver.Value{string(r.TenantID), string(r.RunID)}, g.loaded.Run)
			} else if mode == "cancelled_run" {
				g.loaded.Run.CancellationRequestedAt = &g.at
				g.setJSON(t, piUseRunSQL, []driver.Value{string(r.TenantID), string(r.RunID)}, g.loaded.Run)
			} else if mode != "current" {
				piNewWorkChangeAuthority(t, g, mode)
			}
			beforeTx, beforeCommits := g.sql.transactions, g.sql.commits
			got, err := g.store.AdmitDispatch(g.ctx, r)
			if got.Admitted || g.sql.transactions != beforeTx+1 || g.sql.active || g.sql.commits != beforeCommits {
				t.Fatalf("mode=%s speculative admission/escaped transaction: admitted=%t err=%v", mode, got.Admitted, err)
			}
			if mode == "current" {
				if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 || !strings.HasPrefix(f.writeSQL, "UPSERT INTO subscription_scheduler_slots ") || len(f.writeArgs) != 7 || f.writeArgs[6].Value != g.at {
					t.Fatalf("lawful original source must reach cold slot with DB time: writes=%d query=%s err=%v", f.firstWrites, f.writeSQL, err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || f.firstWrites != 0 {
				t.Fatalf("mode=%s stale API admission reached effect: writes=%d err=%v", mode, f.firstWrites, err)
			}
		})
	}
}

func TestPiAPIAdmissionRestoredCurrentRowsReachOriginalEffect(t *testing.T) {
	g, f := piNewWorkSource(t)
	r := piAdmissionRequest(t, g)
	saved := append([]driver.Value(nil), g.sql.rows[piUseSQL(providerCredentialReadSQL)]...)
	piNewWorkChangeAuthority(t, g, "api_generation")
	if got, err := g.store.AdmitDispatch(g.ctx, r); !errors.Is(err, domain.ErrMembershipDenied) || got.Admitted || f.firstWrites != 0 {
		t.Fatalf("rotated source: writes=%d err=%v", f.firstWrites, err)
	}
	g.sql.rows[piUseSQL(providerCredentialReadSQL)] = saved
	if _, err := g.store.AdmitDispatch(g.ctx, r); !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 {
		t.Fatalf("restored synthetic current source did not reach first effect: %v", err)
	}
}

func TestPiAPIAdmissionDirectOfferCannotBypassCurrentPolicy(t *testing.T) {
	for _, mode := range []string{"current", "nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion"} {
		t.Run(mode, func(t *testing.T) {
			g, f := piNewWorkSource(t)
			piNewWorkReadyProtocol(t, g)
			g.loaded.Run.Status = domain.RunQueued
			g.loaded.Run.StartedAt = nil
			g.loaded.Attempt.Status = domain.AttemptCreated
			g.connection.AuthExpiresAt = g.at.Add(4 * time.Hour)
			g.syncOriginalRows(t)
			g.sql.rows[piUseSQL(piUseHeadSQL)] = nil
			g.sql.rows[piUseSQL(piUseLeaseHeadSQL)] = nil
			bucket, err := webBucket(string(g.head.OwnerUserID))
			if err != nil {
				t.Fatal(err)
			}
			g.setRow(t, piTerminalMembershipSQL, []driver.Value{int64(bucket), string(g.head.OwnerUserID), string(g.head.TenantID)}, g.sql.rows[piUseSQL(computeChoiceMembershipSQL)][0])
			leaseID, err := domain.NewAttachedWorkerLeaseIDV1(g.head.TenantID, g.head.RunID, g.head.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			g.setRow(t, `SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2`, []driver.Value{string(g.head.TenantID), string(leaseID)})
			g.setRow(t, `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, []driver.Value{string(g.head.TenantID), string(g.loaded.Run.SessionID)})
			ttl, err := domain.AttachedWorkerLeaseTTLForLimitsV1(g.loaded.Job.Limits)
			if err != nil {
				t.Fatal(err)
			}
			r := ports.AttachedWorkerAttemptOffer{TenantID: g.head.TenantID, OwnerUserID: g.head.OwnerUserID, WorkerID: g.head.WorkerID, RunID: g.head.RunID, AttemptID: g.head.AttemptID, ReservationID: g.head.ReservationID, LeaseID: leaseID, LeaseTTL: ttl}
			if err := validateAttachedWorkerAttemptOffer(r); err != nil {
				t.Fatal(err)
			}
			if mode != "current" {
				piNewWorkChangeAuthority(t, g, mode)
			}
			beforeTx, beforeCommits := g.sql.transactions, g.sql.commits
			got, err := g.store.OfferAttachedWorkerAttempt(g.ctx, r)
			if g.sql.transactions != beforeTx+1 || g.sql.active || g.sql.commits != beforeCommits || got.Status == ports.AttachedWorkerExecutionApplied {
				t.Fatalf("mode=%s offer escaped rollback: status=%s err=%v", mode, got.Status, err)
			}
			if mode == "current" {
				if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 || !strings.Contains(f.writeSQL, "INTO leases ") {
					t.Fatalf("lawful offer must reach first canonical lease write: writes=%d query=%s err=%v", f.firstWrites, f.writeSQL, err)
				}
			} else if !errors.Is(err, domain.ErrMembershipDenied) || f.firstWrites != 0 {
				t.Fatalf("mode=%s direct offer bypassed current source: writes=%d err=%v", mode, f.firstWrites, err)
			}
		})
	}
}
