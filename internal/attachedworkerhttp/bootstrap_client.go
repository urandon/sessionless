package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/http"
	"net/url"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type BootstrapClientConfig struct {
	BaseURL        string
	HTTPClient     *http.Client
	RequestTimeout time.Duration
}

// BootstrapClient is an outbound-only proof bootstrap client. It carries no
// bearer credential and blocks redirects so bootstrap material cannot be
// forwarded to a different origin.
type BootstrapClient struct {
	baseURL *url.URL
	http    http.Client
	timeout time.Duration
}

// ActivateClientInputV1 keeps the raw connection secret out of the wire DTO.
// The client derives only its digest for ActivateRequestV1 and never sends the
// raw bytes. The caller retains the secret for constructing the later bearer.
type ActivateClientInputV1 struct {
	TenantLocator        domain.TenantID                          `json:"tenant_locator"`
	OwnerLocator         domain.UserID                            `json:"owner_locator"`
	ChallengeID          domain.AttachedWorkerChallengeID         `json:"challenge_id"`
	ExpectedConnectionID domain.AttachedWorkerConnectionID        `json:"-"`
	ConnectionSecret     attachedworkertransport.ConnectionSecret `json:"-"`
	Attach               attachedworkerprotocol.FrameV1           `json:"attach"`
}

func NewBootstrapClient(config BootstrapClientConfig) (*BootstrapClient, error) {
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		(base.Path != "" && base.Path != "/") {
		return nil, ErrInvalidRequest
	}
	timeout := config.RequestTimeout
	if timeout == 0 {
		timeout = defaultRequestTimeout
	}
	if timeout <= 0 || timeout > time.Minute {
		return nil, ErrInvalidRequest
	}
	httpClient := http.Client{}
	if config.HTTPClient != nil {
		httpClient = *config.HTTPClient
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	base.Path = ""
	base.RawPath = ""
	return &BootstrapClient{baseURL: base, http: httpClient, timeout: timeout}, nil
}

func (client *BootstrapClient) IssueChallenge(ctx context.Context, input ChallengeRequestV1) (*ChallengeResponseV1, error) {
	if input.Purpose != domain.AttachedWorkerAttachInitial {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	var response ChallengeResponseV1
	if err := client.post(ctx, ChallengePathV1, http.StatusCreated, input, &response); err != nil {
		return nil, err
	}
	if !validChallengeResponse(input, response) {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	return &response, nil
}

func (client *BootstrapClient) Activate(ctx context.Context, input ActivateClientInputV1) (*ActivateResponseV1, error) {
	rawSecret := input.ConnectionSecret.Bytes()
	validated, err := attachedworkertransport.ParseConnectionSecret(rawSecret)
	for index := range rawSecret {
		rawSecret[index] = 0
	}
	if err != nil {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	wire := ActivateRequestV1{
		TenantLocator: input.TenantLocator, OwnerLocator: input.OwnerLocator, ChallengeID: input.ChallengeID,
		ConnectionSecretDigest: validated.Digest(), Attach: input.Attach,
	}
	var response ActivateResponseV1
	if err := client.post(ctx, AttachPathV1, http.StatusOK, wire, &response); err != nil {
		return nil, err
	}
	if !validActivateResponse(wire, input.ExpectedConnectionID, response) {
		return nil, &ExchangeError{Kind: ErrorProtocol}
	}
	return &response, nil
}

func validChallengeResponse(input ChallengeRequestV1, response ChallengeResponseV1) bool {
	hello := input.Hello.Hello
	challenge := response.Challenge
	frame := response.Frame
	message := frame.Challenge
	if hello == nil || input.Hello.Kind != attachedworkerprotocol.MessageHello || challenge.Validate() != nil ||
		validateBootstrapFrame(frame) != nil || message == nil || frame.Kind != attachedworkerprotocol.MessageChallenge {
		return false
	}
	workerID := domain.AttachedWorkerID(input.Hello.WorkerID)
	return challenge.TenantID == input.TenantLocator && challenge.OwnerUserID == input.OwnerLocator &&
		challenge.WorkerID == workerID && challenge.ExpectedWorkerRevision == input.ExpectedWorkerRevision &&
		challenge.Purpose == input.Purpose && challenge.Audience == input.ExpectedAudience &&
		challenge.ExpectedEnrollmentGeneration == input.Hello.EnrollmentGeneration &&
		challenge.TargetConnectionGeneration == input.Hello.ConnectionGeneration &&
		challenge.WorkerNonceDigest == domain.DigestAttachedWorkerChallenge(hello.WorkerNonce) &&
		frame.Version == attachedworkerprotocol.ProtocolVersion(challenge.SelectedProtocolVersion) &&
		frame.MessageID == attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1) &&
		frame.WorkerID == input.Hello.WorkerID && frame.EnrollmentGeneration == input.Hello.EnrollmentGeneration &&
		frame.ConnectionGeneration == input.Hello.ConnectionGeneration && frame.Sequence == 1 && frame.Ack == 1 &&
		sameOffer(message.WorkerOffer, hello.Offer) &&
		sameOffer(message.WorkerOffer, offerFromChallenge(challenge.WorkerProtocolMinimum, challenge.WorkerProtocolMaximum, challenge.WorkerProtocolVersions)) &&
		sameOffer(message.PlatformOffer, offerFromChallenge(challenge.PlatformProtocolMinimum, challenge.PlatformProtocolMaximum, challenge.PlatformProtocolVersions)) &&
		message.SelectedVersion == frame.Version && bytes.Equal(message.WorkerNonce, hello.WorkerNonce) &&
		challenge.PlatformNonceDigest == domain.DigestAttachedWorkerChallenge(message.PlatformNonce)
}

func validActivateResponse(input ActivateRequestV1, expectedConnectionID domain.AttachedWorkerConnectionID, response ActivateResponseV1) bool {
	attach := input.Attach.Attach
	accepted := response.Accepted
	message := accepted.AttachAccepted
	connection := response.Connection
	if attach == nil || input.Attach.Kind != attachedworkerprotocol.MessageAttach || connection.Validate() != nil ||
		validateBootstrapFrame(accepted) != nil || message == nil || accepted.Kind != attachedworkerprotocol.MessageAttachAccepted {
		return false
	}
	capabilityDigest := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(attach.CapabilityDigest))
	expectedBinding := attachedworkertransport.ConnectionChannelBinding(
		input.ChallengeID, domain.DigestAttachedWorkerChallenge(attach.WorkerNonce),
		domain.DigestAttachedWorkerChallenge(attach.PlatformNonce), input.ConnectionSecretDigest,
	)
	return connection.TenantID == input.TenantLocator && connection.OwnerUserID == input.OwnerLocator &&
		connection.WorkerID == domain.AttachedWorkerID(input.Attach.WorkerID) && connection.ActivationChallengeID == input.ChallengeID &&
		connection.ID == expectedConnectionID && connection.ChannelBinding == expectedBinding &&
		connection.EnrollmentGeneration == input.Attach.EnrollmentGeneration && connection.ConnectionGeneration == input.Attach.ConnectionGeneration &&
		connection.ProtocolVersion == uint32(input.Attach.Version) && connection.CapabilityDigest == capabilityDigest &&
		connection.SecretDigest == input.ConnectionSecretDigest && connection.State == domain.AttachedWorkerConnectionAttaching &&
		connection.PlatformSequence == 2 && connection.WorkerSequence == 2 && connection.PlatformAck == 2 && connection.WorkerAck == 1 &&
		accepted.Version == input.Attach.Version && accepted.MessageID == attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2) &&
		accepted.WorkerID == input.Attach.WorkerID && accepted.EnrollmentGeneration == input.Attach.EnrollmentGeneration &&
		accepted.ConnectionGeneration == input.Attach.ConnectionGeneration && accepted.Sequence == 2 && accepted.Ack == 2 &&
		attach.SelectedVersion == input.Attach.Version && sameOffer(message.WorkerOffer, attach.WorkerOffer) && sameOffer(message.PlatformOffer, attach.PlatformOffer) &&
		message.SelectedVersion == attach.SelectedVersion && bytes.Equal(message.WorkerNonce, attach.WorkerNonce) &&
		bytes.Equal(message.PlatformNonce, attach.PlatformNonce) && bytes.Equal(message.CapabilityDigest, attach.CapabilityDigest)
}

func validateBootstrapFrame(frame attachedworkerprotocol.FrameV1) error {
	return attachedworkerprotocol.BatchV1{Version: frame.Version, Frames: []attachedworkerprotocol.FrameV1{frame}}.Validate()
}

func sameOffer(left, right attachedworkerprotocol.VersionOfferV1) bool {
	if left.Window != right.Window || len(left.Supported) != len(right.Supported) {
		return false
	}
	for index := range left.Supported {
		if left.Supported[index] != right.Supported[index] {
			return false
		}
	}
	return true
}

func offerFromChallenge(minimum, maximum uint32, supported []uint32) attachedworkerprotocol.VersionOfferV1 {
	versions := make([]attachedworkerprotocol.ProtocolVersion, len(supported))
	for index, version := range supported {
		versions[index] = attachedworkerprotocol.ProtocolVersion(version)
	}
	return attachedworkerprotocol.VersionOfferV1{
		Window: attachedworkerprotocol.VersionWindow{
			Minimum: attachedworkerprotocol.ProtocolVersion(minimum), Maximum: attachedworkerprotocol.ProtocolVersion(maximum),
		},
		Supported: versions,
	}
}

func (client *BootstrapClient) post(ctx context.Context, path string, successStatus int, input, output any) error {
	if client == nil || client.baseURL == nil {
		return &ExchangeError{Kind: ErrorProtocol}
	}
	encoded, err := encodeStrictJSON(input)
	if err != nil {
		return &ExchangeError{Kind: ErrorProtocol}
	}
	endpoint := *client.baseURL
	endpoint.Path = path
	requestContext, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return &ExchangeError{Kind: ErrorProtocol}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := client.http.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &ExchangeError{Kind: ErrorUnavailable, retryable: true}
	}
	defer response.Body.Close()
	if response.StatusCode != successStatus {
		return statusError(response.StatusCode)
	}
	if !exactJSONMediaType(response.Header.Values("Content-Type")) {
		return &ExchangeError{Kind: ErrorProtocol}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, attachedworkerprotocol.MaxBatchBytes+1))
	if err != nil {
		return &ExchangeError{Kind: ErrorUnavailable, retryable: true}
	}
	if len(body) > attachedworkerprotocol.MaxBatchBytes || decodeStrictJSON(body, output) != nil {
		return &ExchangeError{Kind: ErrorProtocol}
	}
	return nil
}
