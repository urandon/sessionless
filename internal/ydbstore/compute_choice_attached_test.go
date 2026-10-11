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
	"reflect"
	"strings"
	"testing"
	"time"

	protocol "gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type computeChoiceAttachedFixture struct {
	source       *computeChoiceSourceFixture
	pin          computeChoiceAttachedPin
	worker       domain.AttachedWorker
	connection   domain.AttachedWorkerConnection
	subscription computeChoiceAttachedSubscription
}

func newComputeChoiceAttachedFixture(t *testing.T) *computeChoiceAttachedFixture {
	return newComputeChoiceAttachedPhaseFixture(t, false)
}

func newComputeChoiceAttachedPhaseFixture(t *testing.T, attaching bool) *computeChoiceAttachedFixture {
	t.Helper()
	f := &computeChoiceAttachedFixture{source: newComputeChoiceSourceFixture(t)}
	at := f.source.at
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x12}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	f.worker = domain.AttachedWorker{TenantID: f.source.cookie.ActiveTenantID, OwnerUserID: f.source.cookie.UserID, ID: "attached-choice-worker", DisplayName: "Private worker", IdentityPublicKey: public, EnrollmentGeneration: 2, ConnectionGeneration: 3, DesiredState: domain.AttachedWorkerDesiredActive, ObservedState: domain.AttachedWorkerObservedOnline, Revision: 4, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	offer := protocol.VersionOfferV1{Window: protocol.VersionWindow{Minimum: 1, Maximum: 1}, Supported: []protocol.ProtocolVersion{1}}
	auth := protocol.AuthContextV1{TenantID: string(f.worker.TenantID), OwnerUserID: string(f.worker.OwnerUserID), WorkerID: string(f.worker.ID), IdentityPublicKey: public, EnrollmentGeneration: 2, ConnectionGeneration: 3, Version: 1, ChannelBinding: bytes.Repeat([]byte{0x31}, 32)}
	manifest := protocol.CapabilityManifestV1{WorkerID: auth.WorkerID, EnrollmentGeneration: 2, Revision: 1, ProtocolOffer: offer, OperatingSystem: "linux", Architecture: "arm64", BuildID: "choice-test-build", HarnessName: "sessionless", HarnessVersion: "1.0.0", HarnessSurface: protocol.HarnessSurfaceSessionTurn, HarnessExecutableDigest: bytes.Repeat([]byte{0x41}, 32), IsolationEvidence: []protocol.IsolationEvidenceV1{protocol.IsolationFilesystemBoundary, protocol.IsolationProcessBoundary}, Features: []protocol.ProtocolFeatureV1{protocol.FeatureCancellation, protocol.FeatureProgress, protocol.FeatureReconnect}, MaxConcurrentAttempts: 1}
	capability, err := protocol.ManifestDigestV1(manifest)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := protocol.NewConformanceMachine(protocol.MachineConfig{Auth: auth, WorkerOffer: offer, PlatformOffer: offer, ImplementedVersions: []protocol.ProtocolVersion{1}})
	if err != nil {
		t.Fatal(err)
	}
	frame := func(direction protocol.Direction, sequence, ack uint64, kind protocol.MessageKind) protocol.FrameV1 {
		return protocol.FrameV1{Version: 1, MessageID: protocol.MessageIDV1(direction, sequence), WorkerID: auth.WorkerID, EnrollmentGeneration: 2, ConnectionGeneration: 3, Sequence: sequence, Ack: ack, Kind: kind}
	}
	accept := func(direction protocol.Direction, frame protocol.FrameV1) {
		t.Helper()
		if err := machine.Accept(direction, frame, protocol.AcceptanceContextV1{ChannelBinding: auth.ChannelBinding, NowUnixMicro: at.UnixMicro()}); err != nil {
			t.Fatalf("fixture frame%s: %v", frame.Kind, err)
		}
	}
	workerNonce, platformNonce := bytes.Repeat([]byte{0x51}, 32), bytes.Repeat([]byte{0x61}, 32)
	hello := frame(protocol.DirectionWorkerToPlatform, 1, 0, protocol.MessageHello)
	hello.Hello = &protocol.HelloV1{Offer: offer, WorkerNonce: workerNonce}
	accept(protocol.DirectionWorkerToPlatform, hello)
	challenge := frame(protocol.DirectionPlatformToWorker, 1, 1, protocol.MessageChallenge)
	challenge.Challenge = &protocol.ChallengeV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce}
	accept(protocol.DirectionPlatformToWorker, challenge)
	attach := frame(protocol.DirectionWorkerToPlatform, 2, 1, protocol.MessageAttach)
	attach.Attach = &protocol.AttachV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: capability}
	if err := protocol.SignAttachV1(private, auth, &attach); err != nil {
		t.Fatal(err)
	}
	accept(protocol.DirectionWorkerToPlatform, attach)
	accepted := frame(protocol.DirectionPlatformToWorker, 2, 2, protocol.MessageAttachAccepted)
	accepted.AttachAccepted = &protocol.AttachAcceptedV1{WorkerOffer: offer, PlatformOffer: offer, SelectedVersion: 1, WorkerNonce: workerNonce, PlatformNonce: platformNonce, CapabilityDigest: capability}
	accept(protocol.DirectionPlatformToWorker, accepted)
	manifestFrame := frame(protocol.DirectionWorkerToPlatform, 3, 2, protocol.MessageManifest)
	manifestFrame.Manifest = &protocol.ManifestV1{Manifest: manifest, Digest: capability}
	if err := protocol.SignManifestV1(private, auth, &manifestFrame); err != nil {
		t.Fatal(err)
	}
	if !attaching {
		accept(protocol.DirectionWorkerToPlatform, manifestFrame)
	} else if machine.ConnectionState() != protocol.ConnectionAttached {
		t.Fatal("pending fixture did not reach canonical pre-manifest attached state")
	}
	snapshot, err := machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := protocol.EncodeMachineSnapshotV1(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	f.connection = domain.AttachedWorkerConnection{TenantID: f.worker.TenantID, OwnerUserID: f.worker.OwnerUserID, WorkerID: f.worker.ID, ID: "attached-choice-connection", ActivationChallengeID: "choice-challenge", EnrollmentGeneration: 2, ConnectionGeneration: 3, ProtocolVersion: 1, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability)), SecretDigest: domain.DigestAttachedWorkerConnectionSecret([]byte("test-only-secret")), ChannelBinding: domain.AttachedWorkerChannelBinding(hex.EncodeToString(auth.ChannelBinding)), ManifestRevision: 1, ManifestIdentityKey: domain.AttachedWorkerIdentityKeyDigest(strings.Repeat("a", 64)), ManifestSignature: manifestFrame.Manifest.Signature, ManifestObservedAt: at.Add(-time.Minute), State: domain.AttachedWorkerConnectionOnline, PlatformSequence: snapshot.Platform.Sequence, WorkerSequence: snapshot.Worker.Sequence, PlatformAck: snapshot.Platform.Ack, WorkerAck: snapshot.Worker.Ack, ProtocolSnapshot: encoded, ConnectedAt: at.Add(-2 * time.Minute), LastCheckpointAt: at.Add(-time.Minute), PresenceExpiresAt: at.Add(time.Hour), AuthExpiresAt: at.Add(2 * time.Hour), Revision: 5}
	if attaching {
		f.worker.ObservedState = domain.AttachedWorkerObservedPending
		f.connection.State = domain.AttachedWorkerConnectionAttaching
		f.connection.ManifestRevision = 0
		f.connection.ManifestIdentityKey = ""
		f.connection.ManifestSignature = nil
		f.connection.ManifestObservedAt = time.Time{}
		f.connection.LastCheckpointAt = time.Time{}
		f.connection.PresenceExpiresAt = time.Time{}
	}
	f.pin = computeChoiceAttachedPin{tenantID: f.worker.TenantID, ownerUserID: f.worker.OwnerUserID, subscriptionID: "choice-subscription", actorID: "choice-actor", provider: "subscription-provider", workerID: f.worker.ID, connectionID: f.connection.ID, enrollmentGeneration: 2, connectionGeneration: 3, workerRevision: f.worker.Revision, connectionRevision: f.connection.Revision, capabilityDigest: f.connection.CapabilityDigest}
	f.subscription = computeChoiceAttachedSubscription{id: f.pin.subscriptionID, actorID: f.pin.actorID, provider: f.pin.provider, entitlement: domain.EntitlementActive, quota: domain.ProviderQuotaAvailable, observedAt: at.Add(-time.Minute)}
	if f.worker.Validate() != nil || f.connection.Validate() != nil {
		t.Fatalf("invalid fixture worker%v connection%v", f.worker.Validate(), f.connection.Validate())
	}
	f.syncRows(t)
	return f
}

func (f *computeChoiceAttachedFixture) syncRows(t *testing.T) {
	t.Helper()
	f.source.syncRows(t)
	w, c, s := f.worker, f.connection, f.subscription
	optional := func(value time.Time) driver.Value {
		if value.IsZero() {
			return nil
		}
		return value.UTC().Truncate(time.Microsecond)
	}
	f.source.driver.rows[computeChoiceAttachedSubscriptionSQL] = []driver.Value{string(s.id), string(s.actorID), s.provider, string(s.entitlement), string(s.quota), s.observedAt}
	f.source.driver.rows[computeChoiceAttachedActorSQL] = []driver.Value{string(f.source.cookie.UserID)}
	f.source.driver.rows[computeChoiceAttachedWorkerSQL] = []driver.Value{choiceFixtureJSON(t, w), string(w.TenantID), string(w.OwnerUserID), string(w.ID), []byte(w.IdentityPublicKey), int64(w.EnrollmentGeneration), int64(w.ConnectionGeneration), string(w.DesiredState), string(w.ObservedState), int64(w.Revision), w.CreatedAt.UTC().Truncate(time.Microsecond), w.UpdatedAt.UTC().Truncate(time.Microsecond), optional(w.RevokedAt)}
	f.source.driver.rows[computeChoiceAttachedConnectionSQL] = []driver.Value{choiceFixtureJSON(t, c), string(c.TenantID), string(c.OwnerUserID), string(c.WorkerID), string(c.ID), string(c.ActivationChallengeID), int64(c.EnrollmentGeneration), int64(c.ConnectionGeneration), int64(c.ProtocolVersion), string(c.CapabilityDigest), string(c.ChannelBinding), string(c.SecretDigest), int64(c.ManifestRevision), string(c.ManifestIdentityKey), []byte(c.ManifestSignature), optional(c.ManifestObservedAt), string(c.State), int64(c.PlatformSequence), int64(c.WorkerSequence), int64(c.PlatformAck), int64(c.WorkerAck), c.ConnectedAt.UTC().Truncate(time.Microsecond), optional(c.LastCheckpointAt), optional(c.PresenceExpiresAt), c.AuthExpiresAt.UTC().Truncate(time.Microsecond), int64(c.Revision)}
	f.source.driver.rows[computeChoiceAttachedSlotSQL] = []driver.Value{string(domain.SchedulerReady), "", "", time.Unix(0, 0).UTC(), f.source.at.Add(-time.Minute)}
	f.source.driver.rows[computeChoiceAttachedCountersSQL] = []driver.Value{int64(3), int64(2), f.source.at.Add(-time.Minute)}
	f.source.driver.expected[computeChoiceAttachedSubscriptionSQL] = []driver.Value{string(f.pin.tenantID), string(f.pin.subscriptionID)}
	f.source.driver.expected[computeChoiceAttachedActorSQL] = []driver.Value{string(f.pin.tenantID), string(f.pin.actorID)}
	f.source.driver.expected[computeChoiceAttachedWorkerSQL] = []driver.Value{string(f.pin.tenantID), string(f.pin.ownerUserID), string(f.pin.workerID)}
	f.source.driver.expected[computeChoiceAttachedConnectionSQL] = f.source.driver.expected[computeChoiceAttachedWorkerSQL]
	f.source.driver.expected[computeChoiceAttachedSlotSQL] = []driver.Value{string(f.pin.tenantID), string(f.pin.subscriptionID)}
	f.source.driver.expected[computeChoiceAttachedCountersSQL] = []driver.Value{string(f.pin.tenantID)}
}

func (f *computeChoiceAttachedFixture) read(budget *computechoice.ResourceBudget) (computeChoiceAttachedSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
	defer cancel()
	var pending computeChoiceAttachedSnapshot
	err := f.source.store.authTx(ctx, "compute_choice.attached_fixture", func(tx *sql.Tx) error {
		pending = computeChoiceAttachedSnapshot{}
		attempt, err := budget.NewAttempt()
		if err != nil {
			return err
		}
		defer attempt.Finish()
		query := &computeChoiceQuery{tx: tx, budget: attempt}
		auth, err := readComputeChoiceAuth(ctx, query, f.source.cookie.SessionDigest, f.source.session.ID)
		if err != nil {
			return err
		}
		pending, err = readComputeChoiceAttached(ctx, query, auth, f.pin)
		return err
	})
	if err != nil {
		return computeChoiceAttachedSnapshot{}, err
	}
	return pending, nil
}

func assertComputeChoiceAttachedCorrupt(t *testing.T, f *computeChoiceAttachedFixture) {
	t.Helper()
	got, err := f.read(computechoice.NewResourceBudget())
	if !reflect.DeepEqual(got, computeChoiceAttachedSnapshot{}) || err != errComputeChoiceSource || err.Error() != "compute choice source unavailable" {
		t.Fatalf("corrupt source published partial facts: %+v %v", got, err)
	}
}

func TestComputeChoiceAttachedSixExactSourcesNoEffects(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	budget := computechoice.NewResourceBudget()
	got, err := f.read(budget)
	if err != nil || got.state != computeChoiceAttachedObserved || got.subscription == nil || got.worker == nil || got.connection == nil || got.slot == nil || got.counters == nil {
		t.Fatalf("six-source snapshot: %+v %v", got, err)
	}
	if got.subscription.quota != domain.ProviderQuotaAvailable || got.counters.queueDepth != 3 || got.counters.activeRuns != 2 || got.worker.OwnerUserID != f.source.cookie.UserID {
		t.Fatalf("facts changed: %+v", got)
	}
	if snapshot := budget.Snapshot(); snapshot.TotalStatements != 11 || snapshot.Failed || snapshot.TotalBytes == 0 {
		t.Fatalf("expected5auth+6source counted reads: %+v", snapshot)
	}
	// Auth charges 64 fixed-width bytes as well as selected variable prefixes.
	// This reader charges binary/string prefixes and fixed YDB scalar widths:
	// subscription8 + worker48 + connection108 + slot16 + counters16.
	wantBytes := uint64(64 + 196)
	for _, statement := range f.source.driver.statements {
		for _, value := range f.source.driver.rows[statement] {
			switch value := value.(type) {
			case string:
				wantBytes += uint64(len(value))
			case []byte:
				wantBytes += uint64(len(value))
			}
		}
	}
	if gotBytes := budget.Snapshot().TotalBytes; gotBytes != wantBytes {
		t.Fatalf("materialized prefix/scalar bytes: got%d want%d", gotBytes, wantBytes)
	}
	for _, statement := range []string{computeChoiceAttachedSubscriptionSQL, computeChoiceAttachedActorSQL, computeChoiceAttachedWorkerSQL, computeChoiceAttachedConnectionSQL, computeChoiceAttachedSlotSQL, computeChoiceAttachedCountersSQL} {
		upper := strings.ToUpper(statement)
		for _, token := range strings.Fields(upper) {
			for _, forbidden := range []string{"INSERT", "UPDATE", "UPSERT", "DELETE", "JOIN", "CREDENTIAL_REF"} {
				if token == forbidden {
					t.Errorf("source SQL includes%s: %s", forbidden, statement)
				}
			}
		}
		if !strings.HasPrefix(upper, "SELECT ") || !strings.Contains(upper, "WHERE") {
			t.Errorf("not exact point SELECT: %s", statement)
		}
	}
	encoded, err := json.Marshal(got)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("private facts serialized: %s %v", encoded, err)
	}
	got.worker.IdentityPublicKey[0] ^= 1
	got.connection.ProtocolSnapshot[0] ^= 1
	if bytes.Equal(got.worker.IdentityPublicKey, f.worker.IdentityPublicKey) || bytes.Equal(got.connection.ProtocolSnapshot, f.connection.ProtocolSnapshot) {
		t.Fatal("returned decoded facts alias fixture source")
	}
}

func TestComputeChoiceAttachedMissingSourcesRemainUnknown(t *testing.T) {
	for _, statement := range []string{computeChoiceAttachedSubscriptionSQL, computeChoiceAttachedActorSQL, computeChoiceAttachedWorkerSQL, computeChoiceAttachedConnectionSQL, computeChoiceAttachedSlotSQL, computeChoiceAttachedCountersSQL} {
		t.Run(strings.Fields(statement)[len(strings.Fields(statement))-1], func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			f.source.driver.rows[statement] = nil
			budget := computechoice.NewResourceBudget()
			got, err := f.read(budget)
			if err != nil || got.state != computeChoiceAttachedMissing || budget.Snapshot().TotalStatements != 11 {
				t.Fatalf("missing source invented fact/skipped reads: %+v %v budget%+v", got, err, budget.Snapshot())
			}
			if statement == computeChoiceAttachedSlotSQL && got.slot != nil {
				t.Fatal("missing slot became ready")
			}
			if statement == computeChoiceAttachedCountersSQL && got.counters != nil {
				t.Fatal("missing counters became zero")
			}
		})
	}
}

func TestComputeChoiceAttachedPendingBeforeManifestUnavailable(t *testing.T) {
	f := newComputeChoiceAttachedPhaseFixture(t, true)
	encoded := f.source.driver.rows[computeChoiceAttachedConnectionSQL][0].(string)
	if !strings.Contains(encoded, `"manifest_signature":null`) {
		t.Fatal("fixture does not reproduce canonical mandatory null signature")
	}
	budget := computechoice.NewResourceBudget()
	got, err := f.read(budget)
	if err != nil || got.state != computeChoiceAttachedUnavailable || got.worker == nil || got.connection == nil || got.worker.ObservedState != domain.AttachedWorkerObservedPending || got.connection.State != domain.AttachedWorkerConnectionAttaching || budget.Snapshot().TotalStatements != 11 {
		t.Fatalf("canonical pending source not unavailable after six reads: %+v err%v budget%+v", got, err, budget.Snapshot())
	}
	if f.source.driver.writes != 0 {
		t.Fatal("pending source reader mutated state")
	}
}

func TestComputeChoiceAttachedManifestSignatureStrictLifecycle(t *testing.T) {
	for _, test := range []struct {
		name      string
		attaching bool
		mutate    func(*computeChoiceAttachedFixture)
	}{
		{"online_null", false, func(f *computeChoiceAttachedFixture) { f.connection.ManifestSignature = nil; f.syncRows(t) }},
		{"attaching_omitted", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"manifest_signature":null,`, "", 1)
		}},
		{"attaching_duplicate", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), "{", `{"manifest_signature":null,`, 1)
		}},
		{"attaching_case_alias", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"manifest_signature"`, `"Manifest_Signature"`, 1)
		}},
		{"attaching_object", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"manifest_signature":null`, `"manifest_signature":{}`, 1)
		}},
		{"attaching_array", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"manifest_signature":null`, `"manifest_signature":[]`, 1)
		}},
		{"attaching_other_required_null", true, func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"revision":5`, `"revision":null`, 1)
		}},
		{"attaching_scalar_disagreement", true, func(f *computeChoiceAttachedFixture) {
			f.source.driver.rows[computeChoiceAttachedConnectionSQL][14] = []byte{1}
		}},
		{"attaching_manifest_metadata", true, func(f *computeChoiceAttachedFixture) { f.connection.ManifestRevision = 1; f.syncRows(t) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceAttachedPhaseFixture(t, test.attaching)
			test.mutate(f)
			assertComputeChoiceAttachedCorrupt(t, f)
		})
	}
}

func TestComputeChoiceAttachedCoherentUnavailableAndChangedPins(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*computeChoiceAttachedFixture)
	}{
		{"subscription_actor", func(f *computeChoiceAttachedFixture) { f.subscription.actorID = "replacement-actor" }},
		{"subscription_provider", func(f *computeChoiceAttachedFixture) { f.subscription.provider = "replacement-provider" }},
		{"entitlement", func(f *computeChoiceAttachedFixture) { f.subscription.entitlement = domain.EntitlementReauthRequired }},
		{"quota_unknown", func(f *computeChoiceAttachedFixture) { f.subscription.quota = domain.ProviderQuotaUnknown }},
		{"quota_exhausted", func(f *computeChoiceAttachedFixture) { f.subscription.quota = domain.ProviderQuotaExhausted }},
		{"worker_revision", func(f *computeChoiceAttachedFixture) { f.pin.workerRevision++ }},
		{"connection_revision", func(f *computeChoiceAttachedFixture) { f.pin.connectionRevision++ }},
		{"enrollment_pin", func(f *computeChoiceAttachedFixture) { f.pin.enrollmentGeneration++ }},
		{"generation_pin", func(f *computeChoiceAttachedFixture) { f.pin.connectionGeneration++ }},
		{"capability_pin", func(f *computeChoiceAttachedFixture) {
			f.pin.capabilityDigest = domain.DigestAttachedWorkerCapability([]byte("other"))
		}},
		{"connection_pin", func(f *computeChoiceAttachedFixture) { f.pin.connectionID = "replacement-connection" }},
		{"worker_offline", func(f *computeChoiceAttachedFixture) { f.worker.ObservedState = domain.AttachedWorkerObservedOffline }},
		{"worker_revoked", func(f *computeChoiceAttachedFixture) {
			f.worker.DesiredState = domain.AttachedWorkerDesiredRevoked
			f.worker.RevokedAt = f.worker.UpdatedAt
		}},
		{"connection_offline", func(f *computeChoiceAttachedFixture) { f.connection.State = domain.AttachedWorkerConnectionOffline }},
		{"auth_exact_expiry", func(f *computeChoiceAttachedFixture) { f.connection.AuthExpiresAt = f.source.at }},
		{"presence_exact_expiry", func(f *computeChoiceAttachedFixture) { f.connection.PresenceExpiresAt = f.source.at }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			test.mutate(f)
			f.syncRows(t)
			got, err := f.read(computechoice.NewResourceBudget())
			if err != nil || got.state != computeChoiceAttachedUnavailable {
				t.Fatalf("coherent own source not unavailable: %+v %v", got, err)
			}
		})
	}
}

func TestComputeChoiceAttachedForeignPinRejectedBeforeSources(t *testing.T) {
	for _, mutate := range []func(*computeChoiceAttachedPin){func(p *computeChoiceAttachedPin) { p.tenantID = "foreign" }, func(p *computeChoiceAttachedPin) { p.ownerUserID = "foreign" }, func(p *computeChoiceAttachedPin) { p.workerID = "bad/worker" }, func(p *computeChoiceAttachedPin) { p.actorID = "" }, func(p *computeChoiceAttachedPin) { p.connectionRevision = 0 }} {
		f := newComputeChoiceAttachedFixture(t)
		mutate(&f.pin)
		budget := computechoice.NewResourceBudget()
		got, err := f.read(budget)
		if !reflect.DeepEqual(got, computeChoiceAttachedSnapshot{}) || err != errComputeChoiceSource || budget.Snapshot().TotalStatements != 5 {
			t.Fatalf("foreign/invalid pin reached source SQL: %+v %v budget%+v", got, err, budget.Snapshot())
		}
	}
}

func TestComputeChoiceAttachedScalarAndEncodedCorruption(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		column    int
		value     driver.Value
	}{
		{"subscription_id", computeChoiceAttachedSubscriptionSQL, 0, "foreign"},
		{"subscription_enum", computeChoiceAttachedSubscriptionSQL, 4, "invented-free"},
		{"subscription_future", computeChoiceAttachedSubscriptionSQL, 5, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"actor_owner", computeChoiceAttachedActorSQL, 0, "foreign"},
		{"worker_scalar_owner", computeChoiceAttachedWorkerSQL, 2, "foreign"},
		{"worker_scalar_revision", computeChoiceAttachedWorkerSQL, 9, int64(99)},
		{"worker_scalar_key", computeChoiceAttachedWorkerSQL, 4, bytes.Repeat([]byte{7}, 32)},
		{"worker_oversize_key", computeChoiceAttachedWorkerSQL, 4, bytes.Repeat([]byte{7}, 33)},
		{"worker_future", computeChoiceAttachedWorkerSQL, 11, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"connection_scalar_owner", computeChoiceAttachedConnectionSQL, 2, "foreign"},
		{"connection_scalar_revision", computeChoiceAttachedConnectionSQL, 25, int64(99)},
		{"connection_scalar_watermark", computeChoiceAttachedConnectionSQL, 17, int64(99)},
		{"connection_scalar_signature", computeChoiceAttachedConnectionSQL, 14, bytes.Repeat([]byte{7}, 64)},
		{"connection_oversize_signature", computeChoiceAttachedConnectionSQL, 14, bytes.Repeat([]byte{7}, 65)},
		{"slot_enum", computeChoiceAttachedSlotSQL, 0, "invented-ready"},
		{"slot_half_pair", computeChoiceAttachedSlotSQL, 1, "run-present"},
		{"slot_unexpected_block", computeChoiceAttachedSlotSQL, 3, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"counter_overflow", computeChoiceAttachedCountersSQL, 0, int64(-1)},
		{"counter_future", computeChoiceAttachedCountersSQL, 2, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			f.source.driver.rows[test.statement][test.column] = test.value
			assertComputeChoiceAttachedCorrupt(t, f)
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*computeChoiceAttachedFixture)
	}{
		{"worker_encoded_owner", func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedWorkerSQL]
			row[0] = strings.Replace(row[0].(string), `"owner_user_id":"choice-user"`, `"owner_user_id":"foreign"`, 1)
		}},
		{"connection_encoded_owner", func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), `"owner_user_id":"choice-user"`, `"owner_user_id":"foreign"`, 1)
		}},
		{"worker_unknown_field", func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedWorkerSQL]
			row[0] = strings.Replace(row[0].(string), "{", `{"approved":true,`, 1)
		}},
		{"connection_duplicate_field", func(f *computeChoiceAttachedFixture) {
			row := f.source.driver.rows[computeChoiceAttachedConnectionSQL]
			row[0] = strings.Replace(row[0].(string), "{", `{"revision":5,`, 1)
		}},
		{"connection_protocol_corrupt", func(f *computeChoiceAttachedFixture) {
			f.connection.ProtocolSnapshot = []byte("private-malformed")
			f.syncRows(t)
		}},
		{"connection_impossible_generations", func(f *computeChoiceAttachedFixture) { f.connection.ConnectionGeneration++; f.syncRows(t) }},
		{"protocol_scalar_coherent_but_wrong_watermark", func(f *computeChoiceAttachedFixture) { f.connection.PlatformSequence++; f.syncRows(t) }},
		{"unavailable_does_not_hide_corruption", func(f *computeChoiceAttachedFixture) {
			f.subscription.entitlement = domain.EntitlementInactive
			f.syncRows(t)
			f.source.driver.rows[computeChoiceAttachedCountersSQL][0] = int64(-1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			test.mutate(f)
			assertComputeChoiceAttachedCorrupt(t, f)
		})
	}
}

func TestComputeChoiceAttachedStatementBudgetBeforeDriverCall(t *testing.T) {
	f := newComputeChoiceAttachedFixture(t)
	budget := computechoice.NewResourceBudget()
	ctx, cancel := context.WithTimeout(context.Background(), computechoice.ResourceDeadline)
	defer cancel()
	var pending computeChoiceAttachedSnapshot
	err := f.source.store.authTx(ctx, "compute_choice.attached_budget_fixture", func(tx *sql.Tx) error {
		pending = computeChoiceAttachedSnapshot{}
		attempt, err := budget.NewAttempt()
		if err != nil {
			return err
		}
		defer attempt.Finish()
		query := &computeChoiceQuery{tx: tx, budget: attempt}
		auth, err := readComputeChoiceAuth(ctx, query, f.source.cookie.SessionDigest, f.source.session.ID)
		if err != nil {
			return err
		}
		// Account for prior caller-owned work in the same callback, leaving
		// exactly one SQL statement for the source reader.
		for range 58 {
			if err := attempt.BeforeStatement(); err != nil {
				return err
			}
		}
		pending, err = readComputeChoiceAttached(ctx, query, auth, f.pin)
		return err
	})
	if !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || !reflect.DeepEqual(pending, computeChoiceAttachedSnapshot{}) || budget.Snapshot().TotalStatements != 64 || len(f.source.driver.statements) != 6 || f.source.driver.commits != 0 || f.source.driver.writes != 0 {
		t.Fatalf("denied source reached driver/published snapshot: %+v err%v budget%+v SQL%d commits%d writes%d", pending, err, budget.Snapshot(), len(f.source.driver.statements), f.source.driver.commits, f.source.driver.writes)
	}
}

func TestComputeChoiceAttachedPrefixesChargedBeforeParsing(t *testing.T) {
	for _, test := range []struct {
		name      string
		statement string
		size      int
		kind      computechoice.SourceRecordKind
	}{
		{"worker", computeChoiceAttachedWorkerSQL, computechoice.MaxOrdinaryRecordBytes + 1, computechoice.SourceOrdinary},
		{"connection", computeChoiceAttachedConnectionSQL, computechoice.MaxConnectionRecordBytes + 1, computechoice.SourceConnection},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newComputeChoiceAttachedFixture(t)
			f.source.driver.rows[test.statement][0] = strings.Repeat("x", test.size)
			budget := computechoice.NewResourceBudget()
			got, err := f.read(budget)
			if !reflect.DeepEqual(got, computeChoiceAttachedSnapshot{}) || !errors.Is(err, computechoice.ErrResourceBudgetExceeded) || budget.Snapshot().TotalBytes < uint64(test.size) {
				t.Fatalf("oversize prefix decoded/uncharged: %+v %v budget%+v", got, err, budget.Snapshot())
			}
		})
	}
	t.Run("protocol64KiB", func(t *testing.T) {
		f := newComputeChoiceAttachedFixture(t)
		f.connection.ProtocolSnapshot = bytes.Repeat([]byte{7}, (64<<10)+1)
		f.syncRows(t)
		assertComputeChoiceAttachedCorrupt(t, f)
	})
	t.Run("scalar_prefix", func(t *testing.T) {
		f := newComputeChoiceAttachedFixture(t)
		f.source.driver.rows[computeChoiceAttachedSubscriptionSQL][2] = strings.Repeat("x", 161)
		assertComputeChoiceAttachedCorrupt(t, f)
	})
}
