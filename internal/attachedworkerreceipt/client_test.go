package attachedworkerreceipt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type joinedReceiptStore struct {
	calls          int
	last           ydbstore.AttachedWorkerOutputReceiptRequest
	lost           bool
	failAlways     bool
	pending        bool
	wrongCandidate bool
}

func (store *joinedReceiptStore) CreateAttachedWorkerOutputReceipt(_ context.Context, _ ports.BlobStore, request ydbstore.AttachedWorkerOutputReceiptRequest) (ydbstore.AttachedWorkerOutputReceiptResult, error) {
	store.calls++
	store.last = request
	if store.lost || store.failAlways {
		store.lost = false
		return ydbstore.AttachedWorkerOutputReceiptResult{}, errors.New("private lost response")
	}
	auth := request.Authorization
	auth.PresentedSecretDigest = ""
	observationDigest, _ := request.Observation.Digest()
	fingerprint, _ := attachedworkeroutput.CandidateFingerprint(request.Candidate)
	if store.wrongCandidate {
		fingerprint = strings.Repeat("0", sha256.Size*2)
	}
	status := ports.AttachedWorkerExecutionApplied
	if store.calls > 1 {
		status = ports.AttachedWorkerExecutionReplayed
	}
	return ydbstore.AttachedWorkerOutputReceiptResult{
		Status: status,
		Receipt: ydbstore.AttachedWorkerOutputReceiptV1{
			Version: 1, Ready: !store.pending, Binding: auth, Nonce: request.Nonce,
			CandidateFingerprint: fingerprint, ObservationDigest: observationDigest,
			Status:          request.Candidate.Status,
			CanonicalDigest: domain.AttachedWorkerTerminalEvidenceDigest(strings.Repeat("ab", sha256.Size)),
		},
	}, nil
}

func TestClientPublisherBoundsAmbiguousRetry(t *testing.T) {
	store := &joinedReceiptStore{failAlways: true}
	service, err := NewService(&receiptBinderFixture{}, store, receiptBlobFixture{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(Handler(service))
	defer server.Close()
	publisher, err := NewClientPublisher(server.URL+PathV1, server.Client(), []byte("bound-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	if _, err := publisher.Publish(context.Background(), receiptSubmissionFixture()); !errors.Is(err, ErrUnavailable) || store.calls != 2 {
		t.Fatalf("ambiguous retry error=%v calls=%d", err, store.calls)
	}
}

func TestClientPublisherRejectsPendingOrDivergentReceipt(t *testing.T) {
	for _, name := range []string{"pending", "wrong_candidate"} {
		t.Run(name, func(t *testing.T) {
			store := &joinedReceiptStore{pending: name == "pending", wrongCandidate: name == "wrong_candidate"}
			service, err := NewService(&receiptBinderFixture{}, store, receiptBlobFixture{})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewTLSServer(Handler(service))
			defer server.Close()
			publisher, err := NewClientPublisher(server.URL+PathV1, server.Client(), []byte("bound-bearer"))
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()
			submission := receiptSubmissionFixture()
			if _, err := publisher.Publish(context.Background(), submission); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("uncommitted or divergent receipt accepted: %v", err)
			}
		})
	}
}

func TestClientPublisherJoinsHTTPSBearerAndReceiptWithExactRetry(t *testing.T) {
	binder := &receiptBinderFixture{}
	store := &joinedReceiptStore{lost: true}
	service, err := NewService(binder, store, receiptBlobFixture{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(Handler(service))
	defer server.Close()
	publisher, err := NewClientPublisher(server.URL+PathV1, server.Client(), []byte("bound-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close()
	submission := receiptSubmissionFixture()
	capability := bytes.Repeat([]byte{0x21}, sha256.Size)
	commitment, err := publisher.Publish(context.Background(), submission)
	if err != nil || commitment.Status != domain.AttachedWorkerTerminalSucceeded ||
		!bytes.Equal(commitment.CanonicalDigest, bytes.Repeat([]byte{0xab}, sha256.Size)) ||
		store.calls != 2 || store.last.Authorization.PresentedSecretDigest == "" ||
		store.last.Authorization.CapabilityDigest != domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability)) ||
		store.last.Nonce != submission.Nonce {
		t.Fatalf("commitment=%+v error=%v calls=%d request=%+v", commitment, err, store.calls, store.last)
	}
	if err := publisher.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.Publish(context.Background(), submission); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("closed publisher error=%v", err)
	}
}

func receiptSubmissionFixture() attachedworkerdaemontransport.ReceiptSubmissionV1 {
	capability := bytes.Repeat([]byte{0x21}, sha256.Size)
	policy := bytes.Repeat([]byte{0x22}, sha256.Size)
	contextDigest := bytes.Repeat([]byte{0x23}, sha256.Size)
	return attachedworkerdaemontransport.ReceiptSubmissionV1{
		Request: attachedworkerdaemontransport.MaterializationRequestV1{
			TenantID: "tenant-a", OwnerUserID: "owner-a", WorkerID: "worker-a",
			ConnectionID: "connection-a", EnrollmentGeneration: 2, ConnectionGeneration: 3,
			AttemptSequence: 1,
			Attempt: attachedworkerprotocol.AttemptBindingV1{
				RunID: "run-a", AttemptID: "attempt-a", LeaseID: "lease-a", LeaseGeneration: 7,
				FenceToken: "fence-a", ExpiresAtUnixMicro: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro(),
				ContextDigest: contextDigest, CapabilityDigest: capability, PolicyDigest: policy,
			},
		},
		Nonce: "receipt-nonce-a", Candidate: attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: "ready"},
		Observation: attachedworkeroutput.ProcessObservationV1{
			Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true,
		},
	}
}
