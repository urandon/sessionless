package attachedworkerhttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	ChallengePathV1 = "/attached-worker/v1/challenges"
	AttachPathV1    = "/attached-worker/v1/attach"
)

// BootstrapCore keeps the proof-based HTTP bootstrap surface independent of a
// concrete transport service.
type BootstrapCore interface {
	IssueChallenge(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.IssueChallengeRequest) (attachedworkertransport.ChallengeGrant, error)
	Activate(context.Context, domain.TenantID, domain.UserID, attachedworkertransport.ActivateRequest) (attachedworkertransport.ActivationGrant, error)
}

// ChallengeRequestV1 carries untrusted owner-scoped lookup hints. TenantLocator
// and OwnerLocator are never principals: the core must load the exact worker
// row and authenticate both locators, the worker, and current generations with
// the Ed25519 challenge-request proof before granting a challenge.
type ChallengeRequestV1 struct {
	TenantLocator          domain.TenantID                    `json:"tenant_locator"`
	OwnerLocator           domain.UserID                      `json:"owner_locator"`
	ExpectedAudience       string                             `json:"expected_audience"`
	ExpectedWorkerRevision uint64                             `json:"expected_worker_revision"`
	Purpose                domain.AttachedWorkerAttachPurpose `json:"purpose"`
	Hello                  attachedworkerprotocol.FrameV1     `json:"hello"`
	Proof                  []byte                             `json:"proof"`
}

type ChallengeResponseV1 struct {
	Challenge domain.AttachedWorkerAttachChallenge `json:"challenge"`
	Frame     attachedworkerprotocol.FrameV1       `json:"frame"`
}

// ActivateRequestV1 repeats the untrusted lookup hints and carries only the
// digest of the worker-generated connection secret. Raw connection secrets and
// manifests are forbidden at this phase; the manifest is sent in the first
// authenticated exchange after Accepted is processed.
type ActivateRequestV1 struct {
	TenantLocator          domain.TenantID                             `json:"tenant_locator"`
	OwnerLocator           domain.UserID                               `json:"owner_locator"`
	ChallengeID            domain.AttachedWorkerChallengeID            `json:"challenge_id"`
	ConnectionSecretDigest domain.AttachedWorkerConnectionSecretDigest `json:"connection_secret_digest"`
	Attach                 attachedworkerprotocol.FrameV1              `json:"attach"`
}

type ActivateResponseV1 struct {
	Connection ActivateConnectionV1           `json:"connection"`
	Accepted   attachedworkerprotocol.FrameV1 `json:"accepted"`
}

// ActivateConnectionV1 is the worker-visible connection grant. The canonical
// MachineSnapshot and its replay fingerprints remain server-side authority and
// are deliberately absent from this wire projection.
type ActivateConnectionV1 struct {
	TenantID              domain.TenantID                             `json:"tenant_id"`
	OwnerUserID           domain.UserID                               `json:"owner_user_id"`
	WorkerID              domain.AttachedWorkerID                     `json:"worker_id"`
	ID                    domain.AttachedWorkerConnectionID           `json:"id"`
	ActivationChallengeID domain.AttachedWorkerChallengeID            `json:"activation_challenge_id"`
	EnrollmentGeneration  uint64                                      `json:"enrollment_generation"`
	ConnectionGeneration  uint64                                      `json:"connection_generation"`
	ProtocolVersion       uint32                                      `json:"protocol_version"`
	CapabilityDigest      domain.AttachedWorkerCapabilityDigest       `json:"capability_digest"`
	SecretDigest          domain.AttachedWorkerConnectionSecretDigest `json:"secret_digest"`
	ChannelBinding        domain.AttachedWorkerChannelBinding         `json:"channel_binding"`
	State                 domain.AttachedWorkerConnectionState        `json:"state"`
	PlatformSequence      uint64                                      `json:"platform_sequence"`
	WorkerSequence        uint64                                      `json:"worker_sequence"`
	PlatformAck           uint64                                      `json:"platform_ack"`
	WorkerAck             uint64                                      `json:"worker_ack"`
	ConnectedAt           time.Time                                   `json:"connected_at"`
	AuthExpiresAt         time.Time                                   `json:"auth_expires_at"`
	Revision              uint64                                      `json:"revision"`
}

type BootstrapHandler struct {
	core BootstrapCore
}

func NewBootstrapHandler(core BootstrapCore) (*BootstrapHandler, error) {
	if core == nil {
		return nil, errCoreExchange
	}
	return &BootstrapHandler{core: core}, nil
}

func (handler *BootstrapHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	setSafeResponseHeaders(writer.Header())
	if handler == nil || handler.core == nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	path := request.URL.Path
	if path != ChallengePathV1 && path != AttachPathV1 {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.URL.RawQuery != "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Content-Type")) {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	if !exactJSONMediaType(request.Header.Values("Accept")) {
		writer.WriteHeader(http.StatusNotAcceptable)
		return
	}
	switch path {
	case ChallengePathV1:
		handler.issueChallenge(writer, request)
	case AttachPathV1:
		handler.activate(writer, request)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (handler *BootstrapHandler) issueChallenge(writer http.ResponseWriter, request *http.Request) {
	var input ChallengeRequestV1
	if !decodeBootstrapBody(writer, request, &input) {
		return
	}
	if input.Purpose != domain.AttachedWorkerAttachInitial {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	grant, err := handler.core.IssueChallenge(request.Context(), input.TenantLocator, input.OwnerLocator, attachedworkertransport.IssueChallengeRequest{
		WorkerID: domain.AttachedWorkerID(input.Hello.WorkerID), ExpectedAudience: input.ExpectedAudience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		Purpose:                input.Purpose, Hello: input.Hello, Proof: append([]byte(nil), input.Proof...),
	})
	if err != nil {
		writeBootstrapCoreStatus(writer, err)
		return
	}
	writeBootstrapJSON(writer, http.StatusCreated, ChallengeResponseV1{Challenge: grant.Challenge, Frame: grant.Frame})
}

func (handler *BootstrapHandler) activate(writer http.ResponseWriter, request *http.Request) {
	var input ActivateRequestV1
	if !decodeBootstrapBody(writer, request, &input) {
		return
	}
	grant, err := handler.core.Activate(request.Context(), input.TenantLocator, input.OwnerLocator, attachedworkertransport.ActivateRequest{
		ChallengeID: input.ChallengeID, ConnectionSecretDigest: input.ConnectionSecretDigest, Attach: input.Attach,
	})
	if err != nil {
		writeBootstrapCoreStatus(writer, err)
		return
	}
	connection := activateConnectionProjection(grant.Connection)
	writeBootstrapJSON(writer, http.StatusOK, ActivateResponseV1{Connection: connection, Accepted: grant.Accepted})
}

func activateConnectionProjection(connection domain.AttachedWorkerConnection) ActivateConnectionV1 {
	return ActivateConnectionV1{
		TenantID: connection.TenantID, OwnerUserID: connection.OwnerUserID, WorkerID: connection.WorkerID,
		ID: connection.ID, ActivationChallengeID: connection.ActivationChallengeID,
		EnrollmentGeneration: connection.EnrollmentGeneration, ConnectionGeneration: connection.ConnectionGeneration,
		ProtocolVersion: connection.ProtocolVersion, CapabilityDigest: connection.CapabilityDigest,
		SecretDigest: connection.SecretDigest, ChannelBinding: connection.ChannelBinding, State: connection.State,
		PlatformSequence: connection.PlatformSequence, WorkerSequence: connection.WorkerSequence,
		PlatformAck: connection.PlatformAck, WorkerAck: connection.WorkerAck,
		ConnectedAt: connection.ConnectedAt, AuthExpiresAt: connection.AuthExpiresAt, Revision: connection.Revision,
	}
}

func decodeBootstrapBody(writer http.ResponseWriter, request *http.Request, target any) bool {
	request.Body = http.MaxBytesReader(writer, request.Body, attachedworkerprotocol.MaxBatchBytes)
	encoded, err := io.ReadAll(request.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			writer.WriteHeader(http.StatusBadRequest)
		}
		return false
	}
	if decodeStrictJSON(encoded, target) != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return false
	}
	return true
}

func writeBootstrapCoreStatus(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, attachedworkertransport.ErrTransportUnauthorized):
		writer.WriteHeader(http.StatusUnauthorized)
	case errors.Is(err, attachedworkertransport.ErrTransportConflict):
		writer.WriteHeader(http.StatusConflict)
	case errors.Is(err, attachedworkertransport.ErrTransportBackend):
		writer.Header().Set("Retry-After", "1")
		writer.WriteHeader(http.StatusServiceUnavailable)
	default:
		writer.WriteHeader(http.StatusInternalServerError)
	}
}

func writeBootstrapJSON(writer http.ResponseWriter, status int, value any) {
	encoded, err := encodeStrictJSON(value)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = writer.Write(encoded)
}
