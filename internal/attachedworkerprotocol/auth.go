package attachedworkerprotocol

import "crypto/ed25519"

type Direction string

const (
	DirectionPlatformToWorker Direction = "platform_to_worker"
	DirectionWorkerToPlatform Direction = "worker_to_platform"
)

// AuthContextV1 is supplied by the verifier from authoritative state. It is
// never decoded from a protocol frame.
type AuthContextV1 struct {
	TenantID             string
	OwnerUserID          string
	WorkerID             string
	IdentityPublicKey    ed25519.PublicKey
	EnrollmentGeneration uint64
	ConnectionGeneration uint64
	Version              ProtocolVersion
	ChannelBinding       []byte
}

func (auth AuthContextV1) Validate() error {
	if !validOpaque(auth.TenantID) || !validOpaque(auth.OwnerUserID) || !validOpaque(auth.WorkerID) ||
		len(auth.IdentityPublicKey) != ed25519.PublicKeySize || auth.EnrollmentGeneration == 0 ||
		auth.ConnectionGeneration == 0 || auth.Version != ProtocolVersionV1 || len(auth.ChannelBinding) != 32 {
		return protocolError(ErrorAuthentication)
	}
	return nil
}

func validDirection(direction Direction) bool {
	return direction == DirectionPlatformToWorker || direction == DirectionWorkerToPlatform
}
