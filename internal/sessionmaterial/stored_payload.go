package sessionmaterial

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var ErrStoredPayloadMismatch = errors.New("canonical stored payload does not match uploaded material")

// ValidateStoredPayloadV1 checks a Put response against the exact uploaded
// bytes, tenant and key, even when the payload cannot be classified by V1.
// It does not read the object, authorize its use or imply prefix coverage.
func ValidateStoredPayloadV1(tenant domain.TenantID, key string, ref domain.BlobRef, body []byte) error {
	if ref.Validate() != nil || ref.TenantID != tenant || ref.Key != key || ref.Size != int64(len(body)) {
		return ErrStoredPayloadMismatch
	}
	digest := sha256.Sum256(body)
	if ref.SHA256 != hex.EncodeToString(digest[:]) {
		return ErrStoredPayloadMismatch
	}
	return nil
}
