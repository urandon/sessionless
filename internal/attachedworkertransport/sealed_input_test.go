package attachedworkertransport

import (
	"context"
	"errors"
	"testing"

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
	}
	fixture := &sealedInputAuthorizerFixture{result: ports.AttachedWorkerSealedInputAuthorizationResult{
		Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: 7,
	}}
	service := &Service{store: fixture}
	if revision, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); err != nil || revision != 7 {
		t.Fatalf("exact bearer gate revision=%d error=%v, want 7", revision, err)
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
			if revision, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), changed); revision != 0 || !errors.Is(err, ErrTransportUnauthorized) {
				t.Fatalf("foreign scope revision=%d error=%v, want unauthorized", revision, err)
			}
			if fixture.calls != 1 {
				t.Fatalf("foreign scope crossed store: calls=%d, want 1", fixture.calls)
			}
		})
	}
	fixture.result.Status = ports.AttachedWorkerExecutionDenied
	if revision, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); revision != 0 || !errors.Is(err, ErrTransportUnauthorized) {
		t.Fatalf("denied head revision=%d error=%v, want unauthorized", revision, err)
	}
	fixture.err = errors.New("private store failure")
	if revision, err := service.AuthorizeSealedInputBearer(context.Background(), bearer.Bytes(), request); revision != 0 || !errors.Is(err, ErrTransportBackend) || err.Error() == "private store failure" {
		t.Fatalf("backend failure revision=%d error=%v, want sanitized backend error", revision, err)
	}
}
