package attachedworkerhttp

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestBootstrapClientIssuesChallengeWithoutAuthorization(t *testing.T) {
	input := validChallengeClientInput()
	want := validChallengeClientResponse(input)
	responseBody, _ := encodeStrictJSON(want)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Scheme != "https" || request.URL.Path != ChallengePathV1 || request.URL.RawQuery != "" ||
			len(request.Header.Values("Authorization")) != 0 || !exactJSONMediaType(request.Header.Values("Content-Type")) ||
			!exactJSONMediaType(request.Header.Values("Accept")) {
			t.Fatalf("unsafe bootstrap request url=%s headers=%v", request.URL.Redacted(), request.Header)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var got ChallengeRequestV1
		if err := decodeStrictJSON(body, &got); err != nil || got.TenantLocator != input.TenantLocator || !bytes.Equal(got.Proof, input.Proof) {
			t.Fatalf("challenge body=%q decoded=%+v err=%v", body, got, err)
		}
		return bootstrapHTTPResponse(http.StatusCreated, responseBody), nil
	})
	client := newBootstrapClient(t, transport)
	response, err := client.IssueChallenge(context.Background(), input)
	if err != nil || response == nil || response.Frame.MessageID != want.Frame.MessageID {
		t.Fatalf("challenge response=%+v err=%v", response, err)
	}
}

func TestBootstrapClientSendsOnlyConnectionSecretDigest(t *testing.T) {
	rawSecret := bytes.Repeat([]byte{0x55}, 32)
	secret, err := attachedworkertransport.ParseConnectionSecret(rawSecret)
	if err != nil {
		t.Fatal(err)
	}
	input := validActivateClientInput(secret)
	want := validActivateClientResponse(input, secret.Digest())
	responseBody, _ := encodeStrictJSON(want)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, rawSecret) || bytes.Contains(body, []byte("connection_secret\"")) || len(request.Header.Values("Authorization")) != 0 {
			t.Fatalf("raw secret crossed activation HTTP boundary: headers=%v body=%q", request.Header, body)
		}
		var got ActivateRequestV1
		if err := decodeStrictJSON(body, &got); err != nil || got.ConnectionSecretDigest != secret.Digest() || got.ChallengeID != input.ChallengeID {
			t.Fatalf("activation body=%q decoded=%+v err=%v", body, got, err)
		}
		return bootstrapHTTPResponse(http.StatusOK, responseBody), nil
	})
	client := newBootstrapClient(t, transport)
	response, err := client.Activate(context.Background(), input)
	if err != nil || response == nil || response.Accepted.MessageID != want.Accepted.MessageID ||
		!bytes.Equal(secret.Bytes(), rawSecret) {
		t.Fatalf("activation response=%+v err=%v", response, err)
	}
}

func TestBootstrapClientBlocksRedirectAndSanitizesFailure(t *testing.T) {
	var calls atomic.Int32
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		if len(request.Header.Values("Authorization")) != 0 {
			t.Fatal("bootstrap request carried authorization")
		}
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Header:     http.Header{"Location": []string{"https://capture.example/private"}},
			Body:       io.NopCloser(strings.NewReader("private bootstrap detail")),
		}, nil
	})
	client := newBootstrapClient(t, transport)
	_, err := client.IssueChallenge(context.Background(), validChallengeClientInput())
	var exchangeErr *ExchangeError
	if !errors.As(err, &exchangeErr) || exchangeErr.Kind != ErrorProtocol || calls.Load() != 1 || strings.Contains(err.Error(), "private") {
		t.Fatalf("redirect calls=%d err=%v", calls.Load(), err)
	}
}

func TestBootstrapClientRejectsCrossWorkerChallengeResponse(t *testing.T) {
	input := validChallengeClientInput()
	response := validChallengeClientResponse(input)
	response.Challenge.WorkerID = "worker-other"
	body, err := encodeStrictJSON(response)
	if err != nil {
		t.Fatal(err)
	}
	client := newBootstrapClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return bootstrapHTTPResponse(http.StatusCreated, body), nil
	}))
	_, err = client.IssueChallenge(context.Background(), input)
	var exchangeErr *ExchangeError
	if !errors.As(err, &exchangeErr) || exchangeErr.Kind != ErrorProtocol {
		t.Fatalf("cross-worker challenge error=%v", err)
	}
}

func TestBootstrapClientRejectsMismatchedActivationResponses(t *testing.T) {
	secret, err := attachedworkertransport.ParseConnectionSecret(bytes.Repeat([]byte{0x55}, 32))
	if err != nil {
		t.Fatal(err)
	}
	input := validActivateClientInput(secret)
	for _, test := range []struct {
		name   string
		mutate func(*ActivateResponseV1)
	}{
		{name: "cross worker", mutate: func(response *ActivateResponseV1) { response.Connection.WorkerID = "worker-other" }},
		{name: "wrong connection", mutate: func(response *ActivateResponseV1) { response.Connection.ID = "connection-other" }},
		{name: "wrong challenge", mutate: func(response *ActivateResponseV1) { response.Connection.ActivationChallengeID = "challenge-other" }},
		{name: "accepted digest bait switch", mutate: func(response *ActivateResponseV1) {
			response.Accepted.AttachAccepted.CapabilityDigest = bytes.Repeat([]byte{0x77}, 32)
		}},
		{name: "wrong platform sequence", mutate: func(response *ActivateResponseV1) { response.Connection.PlatformSequence = 3 }},
		{name: "wrong worker sequence", mutate: func(response *ActivateResponseV1) { response.Connection.WorkerSequence = 3 }},
		{name: "wrong platform ack", mutate: func(response *ActivateResponseV1) { response.Connection.PlatformAck = 1 }},
		{name: "wrong worker ack", mutate: func(response *ActivateResponseV1) { response.Connection.WorkerAck = 2 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := validActivateClientResponse(input, secret.Digest())
			test.mutate(&response)
			body, encodeErr := encodeStrictJSON(response)
			if encodeErr != nil {
				t.Fatal(encodeErr)
			}
			client := newBootstrapClient(t, roundTripFunc(func(*http.Request) (*http.Response, error) {
				return bootstrapHTTPResponse(http.StatusOK, body), nil
			}))
			_, exchangeErr := client.Activate(context.Background(), input)
			var classified *ExchangeError
			if !errors.As(exchangeErr, &classified) || classified.Kind != ErrorProtocol {
				t.Fatalf("mismatched response error=%v", exchangeErr)
			}
		})
	}
}

func TestBootstrapClientRejectsPlainHTTP(t *testing.T) {
	if _, err := NewBootstrapClient(BootstrapClientConfig{BaseURL: "http://control.example"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("plain HTTP accepted: %v", err)
	}
}

func newBootstrapClient(t *testing.T, transport http.RoundTripper) *BootstrapClient {
	t.Helper()
	client, err := NewBootstrapClient(BootstrapClientConfig{
		BaseURL: "https://control.example", HTTPClient: &http.Client{Transport: transport},
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func bootstrapHTTPResponse(status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(bytes.NewReader(body)),
	}
}

func validChallengeClientInput() ChallengeRequestV1 {
	offer := attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
	return ChallengeRequestV1{
		TenantLocator: "tenant-a", OwnerLocator: "owner-a", ExpectedAudience: "https://control.example", ExpectedWorkerRevision: 7,
		Purpose: domain.AttachedWorkerAttachInitial,
		Hello: attachedworkerprotocol.FrameV1{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
			WorkerID: "worker-1", EnrollmentGeneration: 1, ConnectionGeneration: 1, Sequence: 1, Ack: 0,
			Kind:  attachedworkerprotocol.MessageHello,
			Hello: &attachedworkerprotocol.HelloV1{Offer: offer, WorkerNonce: bytes.Repeat([]byte{0x11}, 32)},
		},
		Proof: bytes.Repeat([]byte{0x44}, 64),
	}
}

func validChallengeClientResponse(input ChallengeRequestV1) ChallengeResponseV1 {
	platformOffer := attachedworkerprotocol.VersionOfferV1{
		Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
		Supported: []attachedworkerprotocol.ProtocolVersion{1},
	}
	platformNonce := bytes.Repeat([]byte{0x22}, 32)
	created := time.Unix(1_700_000_000, 0).UTC()
	return ChallengeResponseV1{
		Challenge: domain.AttachedWorkerAttachChallenge{
			TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, ID: "challenge-a",
			WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ConnectionID: "connection-a", Purpose: input.Purpose,
			Audience: input.ExpectedAudience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
			ExpectedEnrollmentGeneration: input.Hello.EnrollmentGeneration, ExpectedConnectionGeneration: input.Hello.ConnectionGeneration - 1,
			TargetConnectionGeneration: input.Hello.ConnectionGeneration,
			WorkerProtocolMinimum:      1, WorkerProtocolMaximum: 1, WorkerProtocolVersions: []uint32{1},
			PlatformProtocolMinimum: 1, PlatformProtocolMaximum: 1, PlatformProtocolVersions: []uint32{1}, SelectedProtocolVersion: 1,
			WorkerNonceDigest:   domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce),
			PlatformNonceDigest: domain.DigestAttachedWorkerChallenge(platformNonce),
			CreatedAt:           created, ExpiresAt: created.Add(time.Minute), RetainUntil: created.Add(time.Hour), Revision: 1,
		},
		Frame: attachedworkerprotocol.FrameV1{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1),
			WorkerID: input.Hello.WorkerID, EnrollmentGeneration: input.Hello.EnrollmentGeneration,
			ConnectionGeneration: input.Hello.ConnectionGeneration, Sequence: 1, Ack: 1,
			Kind: attachedworkerprotocol.MessageChallenge,
			Challenge: &attachedworkerprotocol.ChallengeV1{
				WorkerOffer: input.Hello.Hello.Offer, PlatformOffer: platformOffer, SelectedVersion: 1,
				WorkerNonce: append([]byte(nil), input.Hello.Hello.WorkerNonce...), PlatformNonce: platformNonce,
			},
		},
	}
}

func validActivateClientInput(secret attachedworkertransport.ConnectionSecret) ActivateClientInputV1 {
	challengeInput := validChallengeClientInput()
	challenge := validChallengeClientResponse(challengeInput)
	capabilityDigest := bytes.Repeat([]byte{0x33}, 32)
	return ActivateClientInputV1{
		TenantLocator: challengeInput.TenantLocator, OwnerLocator: challengeInput.OwnerLocator,
		ChallengeID: challenge.Challenge.ID, ExpectedConnectionID: challenge.Challenge.ConnectionID, ConnectionSecret: secret,
		Attach: attachedworkerprotocol.FrameV1{
			Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
			WorkerID: challengeInput.Hello.WorkerID, EnrollmentGeneration: challengeInput.Hello.EnrollmentGeneration,
			ConnectionGeneration: challengeInput.Hello.ConnectionGeneration, Sequence: 2, Ack: 1,
			Kind: attachedworkerprotocol.MessageAttach,
			Attach: &attachedworkerprotocol.AttachV1{
				WorkerOffer: challenge.Frame.Challenge.WorkerOffer, PlatformOffer: challenge.Frame.Challenge.PlatformOffer,
				SelectedVersion: 1, WorkerNonce: append([]byte(nil), challenge.Frame.Challenge.WorkerNonce...),
				PlatformNonce: append([]byte(nil), challenge.Frame.Challenge.PlatformNonce...), CapabilityDigest: capabilityDigest,
				Signature: bytes.Repeat([]byte{0x66}, 64),
			},
		},
	}
}

func validActivateClientResponse(input ActivateClientInputV1, secretDigest domain.AttachedWorkerConnectionSecretDigest) ActivateResponseV1 {
	connected := time.Unix(1_700_000_100, 0).UTC()
	capabilityDigest := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(input.Attach.Attach.CapabilityDigest))
	channelBinding := attachedworkertransport.ConnectionChannelBinding(
		input.ChallengeID, domain.DigestAttachedWorkerChallenge(input.Attach.Attach.WorkerNonce),
		domain.DigestAttachedWorkerChallenge(input.Attach.Attach.PlatformNonce), secretDigest,
	)
	return ActivateResponseV1{
		Connection: domain.AttachedWorkerConnection{
			TenantID: input.TenantLocator, OwnerUserID: input.OwnerLocator, WorkerID: domain.AttachedWorkerID(input.Attach.WorkerID),
			ID: input.ExpectedConnectionID, ActivationChallengeID: input.ChallengeID, EnrollmentGeneration: input.Attach.EnrollmentGeneration,
			ConnectionGeneration: input.Attach.ConnectionGeneration, ProtocolVersion: uint32(input.Attach.Version),
			CapabilityDigest: capabilityDigest, SecretDigest: secretDigest,
			ChannelBinding: channelBinding, ManifestSignature: []byte{},
			State: domain.AttachedWorkerConnectionAttaching, PlatformSequence: 2, WorkerSequence: 2, PlatformAck: 2, WorkerAck: 1,
			ConnectedAt: connected, AuthExpiresAt: connected.Add(time.Hour), Revision: 1,
		},
		Accepted: attachedworkerprotocol.FrameV1{
			Version: input.Attach.Version, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 2),
			WorkerID: input.Attach.WorkerID, EnrollmentGeneration: input.Attach.EnrollmentGeneration,
			ConnectionGeneration: input.Attach.ConnectionGeneration, Sequence: 2, Ack: 2,
			Kind: attachedworkerprotocol.MessageAttachAccepted,
			AttachAccepted: &attachedworkerprotocol.AttachAcceptedV1{
				WorkerOffer: input.Attach.Attach.WorkerOffer, PlatformOffer: input.Attach.Attach.PlatformOffer,
				SelectedVersion: input.Attach.Attach.SelectedVersion, WorkerNonce: append([]byte(nil), input.Attach.Attach.WorkerNonce...),
				PlatformNonce:    append([]byte(nil), input.Attach.Attach.PlatformNonce...),
				CapabilityDigest: append([]byte(nil), input.Attach.Attach.CapabilityDigest...),
			},
		},
	}
}
