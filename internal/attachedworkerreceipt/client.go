package attachedworkerreceipt

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// ClientPublisher is the daemon's bounded HTTPS channel. The bearer remains
// local to this client, and no caller-provided terminal digest is transmitted.
type ClientPublisher struct {
	endpoint string
	client   http.Client
	mu       sync.Mutex
	bearer   []byte
	closed   bool
}

func NewClientPublisher(endpoint string, client *http.Client, bearer []byte) (*ClientPublisher, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Path != PathV1 || parsed.RawQuery != "" || parsed.Fragment != "" || len(bearer) == 0 {
		return nil, ErrInvalid
	}
	result := &ClientPublisher{endpoint: endpoint, bearer: bytes.Clone(bearer)}
	if client != nil {
		result.client = *client
	}
	if result.client.Timeout <= 0 || result.client.Timeout > time.Minute {
		result.client.Timeout = time.Minute
	}
	result.client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return result, nil
}

func (publisher *ClientPublisher) Publish(ctx context.Context, submission attachedworkerdaemontransport.ReceiptSubmissionV1) (attachedworkerdaemontransport.ReceiptCommitmentV1, error) {
	if publisher == nil || ctx == nil || ctx.Err() != nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrInvalid
	}
	publisher.mu.Lock()
	if publisher.closed {
		publisher.mu.Unlock()
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	bearer := bytes.Clone(publisher.bearer)
	publisher.mu.Unlock()
	defer clearBytes(bearer)
	request, err := receiptRequest(submission)
	if err != nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrInvalid
	}
	data, err := json.Marshal(request)
	if err != nil || len(data) > maxRequestBytes {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrInvalid
	}
	defer clearBytes(data)
	// A lost first response may have followed a durable server commit. Retry
	// the exact bytes once, never a regenerated candidate or nonce. The store
	// resolves that ambiguity by immutable fingerprint before Terminal.
	var response *http.Response
	for attempt := 0; attempt < 2; attempt++ {
		httpRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, publisher.endpoint, bytes.NewReader(data))
		if requestErr != nil {
			return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrInvalid
		}
		httpRequest.Header.Set("Authorization", "Bearer "+string(bearer))
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Accept", "application/json")
		response, err = publisher.client.Do(httpRequest)
		httpRequest.Header.Del("Authorization")
		if err != nil {
			if response != nil {
				response.Body.Close()
				response = nil
			}
			if ctx.Err() != nil {
				return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
			}
			continue
		}
		if response.StatusCode != http.StatusBadGateway && response.StatusCode != http.StatusServiceUnavailable &&
			response.StatusCode != http.StatusGatewayTimeout {
			break
		}
		if attempt == 1 {
			break
		}
		response.Body.Close()
		response = nil
	}
	if response == nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "application/json" {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		clearBytes(body)
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	defer clearBytes(body)
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var result ydbstore.AttachedWorkerOutputReceiptResult
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF || ctx.Err() != nil {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	digest, err := hex.DecodeString(string(result.Receipt.CanonicalDigest))
	observationDigest, observationErr := submission.Observation.Digest()
	candidateFingerprint, fingerprintErr := attachedworkeroutput.CandidateFingerprint(submission.Candidate)
	if err != nil || len(digest) != sha256.Size || observationErr != nil || fingerprintErr != nil ||
		(result.Status != ports.AttachedWorkerExecutionApplied && result.Status != ports.AttachedWorkerExecutionReplayed) ||
		result.Receipt.Version != 1 || !result.Receipt.Ready || result.Receipt.Nonce != submission.Nonce ||
		result.Receipt.CandidateFingerprint != candidateFingerprint ||
		result.Receipt.Status != submission.Candidate.Status ||
		result.Receipt.ObservationDigest != observationDigest ||
		!sameReceiptSubmissionBinding(result.Receipt.Binding, request.Authorization) {
		return attachedworkerdaemontransport.ReceiptCommitmentV1{}, ErrUnavailable
	}
	return attachedworkerdaemontransport.ReceiptCommitmentV1{Status: result.Receipt.Status, CanonicalDigest: digest}, nil
}

func receiptRequest(submission attachedworkerdaemontransport.ReceiptSubmissionV1) (ydbstore.AttachedWorkerOutputReceiptRequest, error) {
	request := submission.Request
	binding := request.Attempt
	auth := ports.AttachedWorkerSealedInputAuthorization{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID,
		WorkerID: request.WorkerID, ConnectionID: request.ConnectionID,
		EnrollmentGeneration: request.EnrollmentGeneration, ConnectionGeneration: request.ConnectionGeneration,
		RunID: domain.RunID(binding.RunID), AttemptID: domain.AttemptID(binding.AttemptID),
		AttemptSequence: request.AttemptSequence, LeaseID: domain.LeaseID(binding.LeaseID),
		LeaseGeneration: binding.LeaseGeneration, FenceToken: domain.AttachedWorkerFenceToken(binding.FenceToken),
		LeaseExpiresAtUnixMicro: binding.ExpiresAtUnixMicro,
		ContextDigest:           domain.AttachedWorkerContextDigest(hex.EncodeToString(binding.ContextDigest)),
		CapabilityDigest:        domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(binding.CapabilityDigest)),
		PolicyDigest:            domain.AttachedWorkerPolicyDigest(hex.EncodeToString(binding.PolicyDigest)),
	}
	if request.TenantID.Validate() != nil || request.OwnerUserID.Validate() != nil ||
		request.WorkerID.Validate() != nil || request.ConnectionID.Validate() != nil ||
		binding.Validate() != nil || submission.Nonce.Validate() != nil ||
		!submission.Candidate.Status.Valid() || submission.Observation.Version != 1 ||
		strings.TrimSpace(string(auth.FenceToken)) == "" {
		return ydbstore.AttachedWorkerOutputReceiptRequest{}, ErrInvalid
	}
	return ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: auth, Nonce: submission.Nonce, Candidate: submission.Candidate,
		Observation: submission.Observation,
	}, nil
}

func sameReceiptSubmissionBinding(record, submitted ports.AttachedWorkerSealedInputAuthorization) bool {
	return record.TenantID == submitted.TenantID && record.OwnerUserID == submitted.OwnerUserID &&
		record.WorkerID == submitted.WorkerID && record.EnrollmentGeneration == submitted.EnrollmentGeneration &&
		record.RunID == submitted.RunID &&
		record.AttemptID == submitted.AttemptID && record.LeaseID == submitted.LeaseID &&
		record.AttemptSequence == submitted.AttemptSequence &&
		record.LeaseGeneration == submitted.LeaseGeneration && record.FenceToken == submitted.FenceToken &&
		record.LeaseExpiresAtUnixMicro == submitted.LeaseExpiresAtUnixMicro &&
		record.ContextDigest == submitted.ContextDigest && record.CapabilityDigest == submitted.CapabilityDigest &&
		record.PolicyDigest == submitted.PolicyDigest && record.PresentedSecretDigest == ""
}

func (publisher *ClientPublisher) Close() error {
	if publisher == nil {
		return nil
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	clearBytes(publisher.bearer)
	publisher.bearer = nil
	publisher.closed = true
	return nil
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

var _ attachedworkerdaemontransport.ReceiptPublisher = (*ClientPublisher)(nil)
