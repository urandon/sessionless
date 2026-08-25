package attachedworkerprotocol

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
)

const (
	attachTranscriptDomainV1    = "sessionless.attached-worker.attach.v1"
	reconnectTranscriptDomainV1 = "sessionless.attached-worker.reconnect.v1"
	manifestTranscriptDomainV1  = "sessionless.attached-worker.manifest.v1"
)

func AttachTranscriptV1(auth AuthContextV1, frame FrameV1) ([]byte, error) {
	if validateSignedFrame(auth, DirectionWorkerToPlatform, frame, MessageAttach) != nil ||
		frame.Attach == nil || validateAttachForTranscript(*frame.Attach) != nil {
		return nil, protocolError(ErrorAuthentication)
	}
	selected, err := highestCommonOfferedVersion(frame.Attach.WorkerOffer, frame.Attach.PlatformOffer)
	if err != nil || selected != auth.Version || frame.Attach.SelectedVersion != auth.Version {
		return nil, protocolError(ErrorAuthentication)
	}
	body := make([]byte, 0, 384)
	body = appendVersionOffer(body, frame.Attach.WorkerOffer)
	body = appendVersionOffer(body, frame.Attach.PlatformOffer)
	body = appendCanonicalUint32(body, uint32(frame.Attach.SelectedVersion))
	body = appendCanonicalField(body, frame.Attach.WorkerNonce)
	body = appendCanonicalField(body, frame.Attach.PlatformNonce)
	body = appendCanonicalField(body, frame.Attach.CapabilityDigest)
	return signedTranscript(attachTranscriptDomainV1, auth, DirectionWorkerToPlatform, frame, body), nil
}

func ReconnectTranscriptV1(auth AuthContextV1, frame FrameV1) ([]byte, error) {
	if validateSignedFrame(auth, DirectionWorkerToPlatform, frame, MessageReconnect) != nil ||
		frame.Reconnect == nil || validateReconnectForTranscript(*frame.Reconnect) != nil {
		return nil, protocolError(ErrorAuthentication)
	}
	selected, err := highestCommonOfferedVersion(frame.Reconnect.WorkerOffer, frame.Reconnect.PlatformOffer)
	if err != nil || selected != auth.Version || frame.Reconnect.SelectedVersion != auth.Version {
		return nil, protocolError(ErrorAuthentication)
	}
	body := make([]byte, 0, 384)
	body = appendVersionOffer(body, frame.Reconnect.WorkerOffer)
	body = appendVersionOffer(body, frame.Reconnect.PlatformOffer)
	body = appendCanonicalUint32(body, uint32(frame.Reconnect.SelectedVersion))
	body = appendCanonicalUint64(body, frame.Reconnect.PreviousConnectionGeneration)
	body = appendCanonicalField(body, frame.Reconnect.WorkerNonce)
	body = appendCanonicalField(body, frame.Reconnect.PlatformNonce)
	body = appendCanonicalField(body, frame.Reconnect.CapabilityDigest)
	body = appendWatermarks(body, frame.Reconnect.PreviousWatermarks)
	body = appendCanonicalField(body, frame.Reconnect.AttemptSummary.Digest)
	return signedTranscript(reconnectTranscriptDomainV1, auth, DirectionWorkerToPlatform, frame, body), nil
}

func appendWatermarks(destination []byte, watermarks ConnectionWatermarksV1) []byte {
	destination = appendCanonicalUint64(destination, watermarks.PlatformSequence)
	destination = appendCanonicalUint64(destination, watermarks.WorkerSequence)
	destination = appendCanonicalUint64(destination, watermarks.PlatformAck)
	return appendCanonicalUint64(destination, watermarks.WorkerAck)
}

func ManifestTranscriptV1(auth AuthContextV1, frame FrameV1) ([]byte, error) {
	if validateSignedFrame(auth, DirectionWorkerToPlatform, frame, MessageManifest) != nil ||
		frame.Manifest == nil || validateManifestForTranscript(*frame.Manifest) != nil {
		return nil, protocolError(ErrorAuthentication)
	}
	digest, err := ManifestDigestV1(frame.Manifest.Manifest)
	if err != nil || !bytes.Equal(digest, frame.Manifest.Digest) {
		return nil, protocolError(ErrorAuthentication)
	}
	body := appendCanonicalField(nil, frame.Manifest.Digest)
	return signedTranscript(manifestTranscriptDomainV1, auth, DirectionWorkerToPlatform, frame, body), nil
}

func SignAttachV1(privateKey ed25519.PrivateKey, auth AuthContextV1, frame *FrameV1) error {
	return signFrame(privateKey, auth, frame, AttachTranscriptV1, func(signature []byte) { frame.Attach.Signature = signature })
}

func SignReconnectV1(privateKey ed25519.PrivateKey, auth AuthContextV1, frame *FrameV1) error {
	return signFrame(privateKey, auth, frame, ReconnectTranscriptV1, func(signature []byte) { frame.Reconnect.Signature = signature })
}

func SignManifestV1(privateKey ed25519.PrivateKey, auth AuthContextV1, frame *FrameV1) error {
	return signFrame(privateKey, auth, frame, ManifestTranscriptV1, func(signature []byte) { frame.Manifest.Signature = signature })
}

func VerifyAttachV1(auth AuthContextV1, frame FrameV1) error {
	return verifyFrame(auth, frame, AttachTranscriptV1, frame.Attach)
}

func VerifyReconnectV1(auth AuthContextV1, frame FrameV1) error {
	return verifyFrame(auth, frame, ReconnectTranscriptV1, frame.Reconnect)
}

func VerifyManifestV1(auth AuthContextV1, frame FrameV1) error {
	return verifyFrame(auth, frame, ManifestTranscriptV1, frame.Manifest)
}

func signFrame(
	privateKey ed25519.PrivateKey,
	auth AuthContextV1,
	frame *FrameV1,
	transcript func(AuthContextV1, FrameV1) ([]byte, error),
	setSignature func([]byte),
) error {
	if frame == nil || len(privateKey) != ed25519.PrivateKeySize || auth.Validate() != nil ||
		!bytes.Equal(privateKey.Public().(ed25519.PublicKey), auth.IdentityPublicKey) {
		return protocolError(ErrorAuthentication)
	}
	encoded, err := transcript(auth, *frame)
	if err != nil {
		return protocolError(ErrorAuthentication)
	}
	setSignature(ed25519.Sign(privateKey, encoded))
	return nil
}

func verifyFrame[T any](
	auth AuthContextV1,
	frame FrameV1,
	transcript func(AuthContextV1, FrameV1) ([]byte, error),
	payload *T,
) error {
	if payload == nil {
		return protocolError(ErrorAuthentication)
	}
	encoded, err := transcript(auth, frame)
	if err != nil {
		return protocolError(ErrorAuthentication)
	}
	var signature []byte
	switch value := any(payload).(type) {
	case *AttachV1:
		signature = value.Signature
	case *ReconnectV1:
		signature = value.Signature
	case *ManifestV1:
		signature = value.Signature
	default:
		return protocolError(ErrorAuthentication)
	}
	if !ed25519.Verify(auth.IdentityPublicKey, encoded, signature) {
		return protocolError(ErrorAuthentication)
	}
	return nil
}

func validateSignedFrame(auth AuthContextV1, direction Direction, frame FrameV1, kind MessageKind) error {
	if auth.Validate() != nil || !validDirection(direction) || validateFrameCommon(frame) != nil || frame.Kind != kind ||
		frame.Version != auth.Version ||
		frame.WorkerID != auth.WorkerID || frame.EnrollmentGeneration != auth.EnrollmentGeneration ||
		frame.ConnectionGeneration != auth.ConnectionGeneration {
		return protocolError(ErrorAuthentication)
	}
	return nil
}

func signedTranscript(domain string, auth AuthContextV1, direction Direction, frame FrameV1, body []byte) []byte {
	bodyDigest := sha256.Sum256(body)
	result := make([]byte, 0, 512)
	result = appendCanonicalField(result, []byte(domain))
	result = appendCanonicalField(result, []byte(direction))
	result = appendCanonicalField(result, []byte(frame.Kind))
	result = appendCanonicalUint32(result, uint32(auth.Version))
	result = appendCanonicalField(result, []byte(auth.TenantID))
	result = appendCanonicalField(result, []byte(auth.OwnerUserID))
	result = appendCanonicalField(result, []byte(auth.WorkerID))
	result = appendCanonicalUint64(result, auth.EnrollmentGeneration)
	result = appendCanonicalUint64(result, auth.ConnectionGeneration)
	result = appendCanonicalField(result, auth.ChannelBinding)
	result = appendCanonicalField(result, []byte(frame.MessageID))
	result = appendCanonicalUint64(result, frame.Sequence)
	result = appendCanonicalUint64(result, frame.Ack)
	result = appendCanonicalField(result, bodyDigest[:])
	return result
}

func appendVersionOffer(destination []byte, offer VersionOfferV1) []byte {
	destination = appendCanonicalUint32(destination, uint32(offer.Window.Minimum))
	destination = appendCanonicalUint32(destination, uint32(offer.Window.Maximum))
	destination = appendCanonicalUint32(destination, uint32(len(offer.Supported)))
	for _, version := range offer.Supported {
		destination = appendCanonicalUint32(destination, uint32(version))
	}
	return destination
}

func validateAttachForTranscript(message AttachV1) error {
	return validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
		message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest)
}

func validateReconnectForTranscript(message ReconnectV1) error {
	if message.PreviousConnectionGeneration == 0 {
		return protocolError(ErrorInvalidFrame)
	}
	return validateNegotiationProof(message.WorkerOffer, message.PlatformOffer, message.SelectedVersion,
		message.WorkerNonce, message.PlatformNonce, message.CapabilityDigest)
}

func validateManifestForTranscript(message ManifestV1) error {
	if message.Manifest.Validate() != nil || !validBytes(message.Digest, sha256.Size) {
		return protocolError(ErrorInvalidFrame)
	}
	return nil
}
