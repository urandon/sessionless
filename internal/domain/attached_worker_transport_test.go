package domain

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAttachedWorkerTransportGenerationAndWatermarkFences(t *testing.T) {
	challenge := validTransportChallenge()
	challenge.ExpectedConnectionGeneration = math.MaxUint64
	challenge.TargetConnectionGeneration = 0
	if err := challenge.Validate(); err == nil {
		t.Fatal("connection generation overflow accepted")
	}

	connection := validTransportConnection(AttachedWorkerConnectionOnline)
	connection.PlatformAck = connection.WorkerSequence + 1
	if err := connection.Validate(); err == nil {
		t.Fatal("platform acknowledgement beyond worker sequence accepted")
	}
	connection = validTransportConnection(AttachedWorkerConnectionOnline)
	connection.WorkerAck = connection.PlatformSequence + 1
	if err := connection.Validate(); err == nil {
		t.Fatal("worker acknowledgement beyond platform sequence accepted")
	}
}

func TestAttachingConnectionHasChannelBindingButNoPresenceLease(t *testing.T) {
	connection := validTransportConnection(AttachedWorkerConnectionAttaching)
	if err := connection.Validate(); err != nil {
		t.Fatalf("valid attaching connection rejected: %v", err)
	}
	connection.PresenceExpiresAt = connection.ConnectedAt.Add(time.Minute)
	if err := connection.Validate(); err == nil {
		t.Fatal("attaching connection with presence lease accepted")
	}
	connection = validTransportConnection(AttachedWorkerConnectionOnline)
	connection.PresenceExpiresAt = time.Time{}
	if err := connection.Validate(); err == nil {
		t.Fatal("online connection without presence lease accepted")
	}
}

func TestAttachedWorkerConnectionCanonicalJSONIncludesChannelBinding(t *testing.T) {
	encoded, err := json.Marshal(validTransportConnection(AttachedWorkerConnectionAttaching))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"channel_binding"`) || strings.Contains(string(encoded), `"ChannelBinding"`) {
		t.Fatalf("non-canonical connection JSON: %s", encoded)
	}
}

func TestCapabilityContentExcludesConnectionBoundObservation(t *testing.T) {
	typeOf := reflect.TypeOf(AttachedWorkerCapabilityManifest{})
	for _, forbidden := range []string{"ConnectionGeneration", "Signature", "ObservedAt"} {
		if _, found := typeOf.FieldByName(forbidden); found {
			t.Fatalf("immutable capability content contains connection-bound %s", forbidden)
		}
	}
	connectionType := reflect.TypeOf(AttachedWorkerConnection{})
	for _, required := range []string{"ManifestRevision", "ManifestIdentityKey", "ManifestSignature", "ManifestObservedAt"} {
		if _, found := connectionType.FieldByName(required); !found {
			t.Fatalf("connection head lacks signed observation field %s", required)
		}
	}
}

func validTransportChallenge() AttachedWorkerAttachChallenge {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	return AttachedWorkerAttachChallenge{
		TenantID: "tenant-a", OwnerUserID: "owner-a", ID: "wch-test", WorkerID: "wrk-test", ConnectionID: "wcn-test",
		Purpose: AttachedWorkerAttachInitial, Audience: "sessionless:attached-worker:v1",
		ExpectedWorkerRevision: 1, ExpectedEnrollmentGeneration: 1, TargetConnectionGeneration: 1,
		WorkerProtocolMinimum: 1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
		PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
		WorkerNonceDigest: DigestAttachedWorkerChallenge([]byte("worker")), PlatformNonceDigest: DigestAttachedWorkerChallenge([]byte("platform")),
		CreatedAt: now, ExpiresAt: now.Add(time.Minute), RetainUntil: now.Add(time.Hour), Revision: 1,
	}
}

func validTransportConnection(state AttachedWorkerConnectionState) AttachedWorkerConnection {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	connection := AttachedWorkerConnection{
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "wrk-test", ID: "wcn-test", ActivationChallengeID: "wch-test",
		EnrollmentGeneration: 1, ConnectionGeneration: 1, ProtocolVersion: 1,
		CapabilityDigest: DigestAttachedWorkerCapability([]byte("capability")), SecretDigest: DigestAttachedWorkerConnectionSecret([]byte("secret")),
		ChannelBinding: NewAttachedWorkerChannelBinding(make([]byte, 32)), State: state,
		PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
		ConnectedAt: now, AuthExpiresAt: now.Add(time.Hour), Revision: 1,
	}
	// A zero binding is invalid by contract; set a deterministic non-zero byte.
	binding := make([]byte, 32)
	binding[0] = 1
	connection.ChannelBinding = NewAttachedWorkerChannelBinding(binding)
	if state != AttachedWorkerConnectionAttaching {
		connection.ManifestRevision = 1
		connection.ManifestIdentityKey = DigestAttachedWorkerIdentityKey([]byte("identity"))
		connection.ManifestSignature = make([]byte, 64)
		connection.ManifestObservedAt = now
		connection.LastCheckpointAt = now
		connection.PresenceExpiresAt = now.Add(time.Minute)
	}
	return connection
}
