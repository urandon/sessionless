package attachedworkertransport

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type sealedInputAuthorizerFixture struct {
	ports.AttachedWorkerTransportStore
	calls   int
	request ports.AttachedWorkerSealedInputAuthorization
	result  ports.AttachedWorkerSealedInputAuthorizationResult
	err     error
}

func (fixture *sealedInputAuthorizerFixture) AuthorizeAttachedWorkerSealedInput(
	_ context.Context,
	request ports.AttachedWorkerSealedInputAuthorization,
) (ports.AttachedWorkerSealedInputAuthorizationResult, error) {
	fixture.calls++
	fixture.request = request
	return fixture.result, fixture.err
}

func TestSealedInputBearerGateDerivesSecretAndRejectsForeignScope(t *testing.T) {
	secret, err := ParseConnectionSecret([]byte("12345678901234567890123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := NewConnectionBearer("tenant-1", "owner-1", "worker-1", "connection-1", secret)
	if err != nil {
		t.Fatal(err)
	}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", ConnectionID: "connection-1",
		EnrollmentGeneration: 2, ConnectionGeneration: 3, RunID: "run-1", AttemptID: "attempt-1", AttemptSequence: 1,
		LeaseID: "lease-1", LeaseGeneration: 4, LeaseExpiresAtUnixMicro: time.Unix(120, 0).UnixMicro(),
		ContextDigest:    domain.AttachedWorkerContextDigest(strings.Repeat("1", 64)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("2", 64)),
		PolicyDigest:     domain.AttachedWorkerPolicyDigest(strings.Repeat("3", 64)),
	}
	request.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(request.TenantID, request.OwnerUserID, request.WorkerID,
		request.RunID, request.AttemptID, request.LeaseID, request.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	head := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		WorkerID: request.WorkerID, ConnectionID: request.ConnectionID, RunID: request.RunID, AttemptID: request.AttemptID,
		ReservationID: "reservation-1", LeaseID: request.LeaseID, LeaseGeneration: request.LeaseGeneration, FenceToken: request.FenceToken,
		EnrollmentGeneration: request.EnrollmentGeneration, ConnectionGeneration: request.ConnectionGeneration,
		ContextDigest: request.ContextDigest, CapabilityDigest: request.CapabilityDigest, PolicyDigest: request.PolicyDigest,
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 2, WorkerAttemptSequence: 1,
		CreatedAt: time.Unix(20, 0).UTC(), UpdatedAt: time.Unix(20, 0).UTC(), LeaseExpiresAt: time.Unix(120, 0).UTC(), Revision: 7}
	fixture := &sealedInputAuthorizerFixture{result: ports.AttachedWorkerSealedInputAuthorizationResult{
		Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: 7, Attempt: head,
	}}
	service := &Service{store: fixture}
	if result, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); err != nil || result.Attempt != head {
		t.Fatalf("exact bearer gate revision=%d error=%v, want exact head", result.AttemptRevision, err)
	}
	if fixture.calls != 1 || fixture.request.PresentedSecretDigest != secret.Digest() {
		t.Fatalf("store calls=%d presented digest=%s", fixture.calls, fixture.request.PresentedSecretDigest)
	}
	for _, test := range []struct {
		name   string
		change func(*ports.AttachedWorkerSealedInputAuthorization)
	}{
		{name: "foreign tenant", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.TenantID = "tenant-2" }},
		{name: "foreign owner", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.OwnerUserID = "owner-2" }},
		{name: "foreign worker", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.WorkerID = "worker-2" }},
		{name: "rotated connection", change: func(r *ports.AttachedWorkerSealedInputAuthorization) { r.ConnectionID = "connection-2" }},
		{name: "caller secret digest", change: func(r *ports.AttachedWorkerSealedInputAuthorization) {
			r.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("caller value"))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := request
			test.change(&changed)
			if result, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), changed); result.AttemptRevision != 0 || !errors.Is(err, ErrTransportUnauthorized) {
				t.Fatalf("foreign scope revision=%d error=%v, want unauthorized", result.AttemptRevision, err)
			}
			if fixture.calls != 1 {
				t.Fatalf("foreign scope crossed store: calls=%d, want 1", fixture.calls)
			}
		})
	}
	fixture.result.Status = ports.AttachedWorkerExecutionDenied
	if result, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); result.AttemptRevision != 0 || !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("denied head revision=%d error=%v, want unauthorized", result.AttemptRevision, err)
	}
	fixture.err = errors.New("private store failure")
	if result, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); result.AttemptRevision != 0 || !errors.Is(err, ErrTransportBackend) || err.Error() == "private store failure" {
		t.Fatalf("backend failure revision=%d error=%v, want sanitized backend error", result.AttemptRevision, err)
	}
}

func TestOutputReceiptBearerBindingDoesNotTrustCallerSecretOrCrossOwner(t *testing.T) {
	secret, err := ParseConnectionSecret([]byte("12345678901234567890123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := NewConnectionBearer("tenant-1", "owner-1", "worker-1", "connection-1", secret)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{store: &sealedInputAuthorizerFixture{}}
	request := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", ConnectionID: "connection-1",
	}
	bound, err := service.BindOutputReceiptBearer(bearer.Bytes(), request)
	if err != nil || bound.PresentedSecretDigest != secret.Digest() {
		t.Fatalf("bound=%+v error=%v", bound, err)
	}
	for _, changed := range []ports.AttachedWorkerSealedInputAuthorization{
		{TenantID: "tenant-1", OwnerUserID: "owner-2", WorkerID: "worker-1", ConnectionID: "connection-1"},
		{TenantID: "tenant-1", OwnerUserID: "owner-1", WorkerID: "worker-1", ConnectionID: "connection-1", PresentedSecretDigest: secret.Digest()},
	} {
		if _, err := service.BindOutputReceiptBearer(bearer.Bytes(), changed); !errors.Is(err, ErrTransportUnauthorized) {
			t.Fatalf("changed=%+v error=%v, want unauthorized", changed, err)
		}
	}
}
