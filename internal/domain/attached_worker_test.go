package domain

import (
	"crypto/ed25519"
	"reflect"
	"testing"
	"time"
)

func TestAttachedWorkerEnrollmentRequiresRetentionBeyondBootstrapExpiry(t *testing.T) {
	now := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	enrollment := AttachedWorkerEnrollment{
		TenantID: "tenant-a", OwnerUserID: "owner-a", ID: "wen-a", WorkerID: "wrk-a",
		DisplayName: "laptop", Audience: "sessionless:attached-worker:v1",
		BootstrapDigest: DigestWorkerBootstrap([]byte("secret")),
		CreatedAt:       now, ExpiresAt: now.Add(5 * time.Minute), RetainUntil: now.Add(time.Hour), Revision: 1,
	}
	if err := enrollment.Validate(); err != nil {
		t.Fatalf("valid enrollment rejected: %v", err)
	}
	enrollment.RetainUntil = enrollment.ExpiresAt
	if err := enrollment.Validate(); err == nil {
		t.Fatal("enrollment TTL equal to bootstrap expiry accepted")
	}
}

func TestAttachedWorkerAllowsDenyFirstRevocationWithoutFalseObservation(t *testing.T) {
	now := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	worker := AttachedWorker{
		TenantID: "tenant-a", OwnerUserID: "owner-a", ID: "wrk-a", DisplayName: "laptop",
		IdentityPublicKey:    make([]byte, ed25519.PublicKeySize),
		EnrollmentGeneration: 2, ConnectionGeneration: 1,
		DesiredState: AttachedWorkerDesiredRevoked, ObservedState: AttachedWorkerObservedOffline,
		Revision: 2, CreatedAt: now, UpdatedAt: now.Add(time.Minute), RevokedAt: now.Add(time.Minute),
	}
	if err := worker.Validate(); err != nil {
		t.Fatalf("deny-first worker rejected: %v", err)
	}
	worker.RevokedAt = time.Time{}
	if err := worker.Validate(); err == nil {
		t.Fatal("revoked desired state without revocation time accepted")
	}
}

func TestAttachedWorkerAuditV1IsContentFreeAndVersioned(t *testing.T) {
	wantFields := []string{
		"Version", "TenantID", "OwnerUserID", "WorkerID", "EnrollmentID", "Action",
		"WorkerRevision", "EnrollmentGeneration", "ConnectionGeneration", "OccurredAt",
	}
	eventType := reflect.TypeOf(AttachedWorkerAuditEvent{})
	if eventType.NumField() != len(wantFields) {
		t.Fatalf("audit fields = %d, want exact content-free V1 shape %v", eventType.NumField(), wantFields)
	}
	for index, name := range wantFields {
		if eventType.Field(index).Name != name {
			t.Fatalf("audit field %d = %s, want %s", index, eventType.Field(index).Name, name)
		}
	}

	now := time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC)
	created := AttachedWorkerAuditEvent{
		Version:  AttachedWorkerAuditEventVersionV1,
		TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "wrk-a", EnrollmentID: "wen-a",
		Action: AttachedWorkerAuditEnrollmentCreated, OccurredAt: now,
	}
	if err := created.Validate(); err != nil {
		t.Fatalf("valid revision-zero creation audit rejected: %v", err)
	}
	created.Version++
	if err := created.Validate(); err == nil {
		t.Fatal("unknown audit version accepted")
	}
}

func TestAttachedWorkerCoreShapesExcludeLaterProtocolConcerns(t *testing.T) {
	for _, value := range []any{AttachedWorker{}, AttachedWorkerEnrollment{}} {
		typeOf := reflect.TypeOf(value)
		for index := 0; index < typeOf.NumField(); index++ {
			name := typeOf.Field(index).Name
			switch name {
			case "Transport", "Capabilities", "CapabilityManifest", "Provider", "Credential", "Dispatch", "Endpoint":
				t.Fatalf("%s contains out-of-scope field %s", typeOf.Name(), name)
			}
		}
	}
}

func TestAttachedWorkerDisplayNameRejectsControlAndInvalidUTF8(t *testing.T) {
	for _, valid := range []string{"Рабочий ноутбук", "桌面 worker", "desk 🚀"} {
		if err := validateAttachedWorkerDisplayName(valid); err != nil {
			t.Fatalf("normal Unicode display name %q rejected: %v", valid, err)
		}
	}
	for _, invalid := range []string{
		"line\nforgery",
		"tab\tforgery",
		"nul\x00forgery",
		string([]byte{'b', 'a', 'd', 0xff}),
	} {
		if err := validateAttachedWorkerDisplayName(invalid); err == nil {
			t.Fatalf("unsafe display name %q accepted", invalid)
		}
	}
}

func TestAttachedWorkerCanonicalJSONTagsAreExplicitSnakeCase(t *testing.T) {
	tests := []struct {
		value any
		tags  []string
	}{
		{value: AttachedWorkerEnrollment{}, tags: []string{
			"tenant_id", "owner_user_id", "enrollment_id", "worker_id", "display_name", "audience",
			"bootstrap_digest", "expires_at", "retain_until", "consumed_at", "created_at", "revision",
		}},
		{value: AttachedWorker{}, tags: []string{
			"tenant_id", "owner_user_id", "worker_id", "display_name", "identity_public_key",
			"enrollment_generation", "connection_generation", "desired_state", "observed_state",
			"revision", "created_at", "updated_at", "revoked_at",
		}},
		{value: AttachedWorkerAuditEvent{}, tags: []string{
			"version", "tenant_id", "owner_user_id", "worker_id", "enrollment_id", "action",
			"worker_revision", "enrollment_generation", "connection_generation", "occurred_at",
		}},
	}
	for _, test := range tests {
		typeOf := reflect.TypeOf(test.value)
		if typeOf.NumField() != len(test.tags) {
			t.Fatalf("%s fields = %d, want %d", typeOf.Name(), typeOf.NumField(), len(test.tags))
		}
		for index, want := range test.tags {
			if got := typeOf.Field(index).Tag.Get("json"); got != want {
				t.Fatalf("%s.%s json tag = %q, want %q", typeOf.Name(), typeOf.Field(index).Name, got, want)
			}
		}
	}
}
