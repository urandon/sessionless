package attachedworkerlocal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"
)

func TestReceiptCheckpointSealsExactPayloadAcrossRuntimeRestart(t *testing.T) {
	store, manifest, secret := initializeFixture(t)
	next := manifest
	next.Revision = 2
	next.ConnectionGeneration = 1
	next.UpdatedAt = testTime.Add(time.Second)
	nextSecret := secret
	nextSecret.ManifestRevision = 2
	nextSecret.ConnectionGeneration = 1
	nextSecret.ConnectionSecret = bytes.Repeat([]byte{0x41}, 32)
	if err := store.Update(context.Background(), manifest.Revision, next, nextSecret); err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"nonce":"receipt-a","candidate":{"summary":"private result"}}`)
	digest := sha256.Sum256(payload)
	checkpoint := ReceiptCheckpointV1{
		Version: ReceiptCheckpointVersionV1, ManifestRevision: next.Revision,
		TenantID: next.TenantID, OwnerUserID: next.OwnerUserID, WorkerID: next.WorkerID,
		EnrollmentGeneration: next.EnrollmentGeneration, ConnectionGeneration: next.ConnectionGeneration,
		ConnectionID: "connection-a", PayloadSHA256: hex.EncodeToString(digest[:]), Payload: payload,
	}
	lease, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.PersistReceiptCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if err := lease.PersistReceiptCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	mutated := checkpoint
	mutated.Payload = []byte(`{"nonce":"receipt-b"}`)
	mutatedDigest := sha256.Sum256(mutated.Payload)
	mutated.PayloadSHA256 = hex.EncodeToString(mutatedDigest[:])
	if err := lease.PersistReceiptCheckpoint(context.Background(), mutated); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("divergent overwrite: %v", err)
	}
	info, err := os.Stat(store.path(ReceiptCheckpointFileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("private checkpoint mode=%v", info.Mode())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.AcquireRuntime(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	loaded, err := restarted.LoadReceiptCheckpoint(context.Background())
	if err != nil || loaded.PayloadSHA256 != checkpoint.PayloadSHA256 || !bytes.Equal(loaded.Payload, payload) {
		t.Fatalf("restart checkpoint=%+v err=%v", loaded, err)
	}
	if err := restarted.RetireReceiptCheckpoint(context.Background(), mutated); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("foreign retirement: %v", err)
	}
	if err := restarted.RetireReceiptCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.LoadReceiptCheckpoint(context.Background()); !errors.Is(err, ErrStateMissing) {
		t.Fatalf("retired checkpoint: %v", err)
	}
}
