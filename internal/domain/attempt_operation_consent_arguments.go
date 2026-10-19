package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// AttemptOperationConsentArgumentsMaxBytesV1 is the original consent argument
// bound, not an additional physical-body allowance or permission to write.
const AttemptOperationConsentArgumentsMaxBytesV1 uint64 = 4 << 10

// AttemptOperationConsentArgumentsRefV1 constructs content DATA in one closed
// arm of an original attempt's body inventory. No caller supplies a path. The
// scope hash uses the existing operation-body scope encoding without changing
// ordinary keys, serialization or digests. This value grants no send/erase right.
func AttemptOperationConsentArgumentsRefV1(scope AttemptOperationScopeV1, size uint64, digest string) (BlobRef, error) {
	if scope.Validate() != nil || size == 0 || size > AttemptOperationConsentArgumentsMaxBytesV1 || len(digest) != 64 {
		return BlobRef{}, ErrMembershipDenied
	}
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return BlobRef{}, ErrMembershipDenied
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return BlobRef{}, ErrMembershipDenied
	}
	namespace := sha256.Sum256(append([]byte("sessionless.operation-body-scope.v1\x00"), raw...))
	key := SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) +
		"attempts/" + string(scope.AttemptID) + "/consent-arguments-v1/" + hex.EncodeToString(namespace[:]) +
		"/consent-args-v1." + strconv.FormatUint(size, 10) + "." + digest
	ref := BlobRef{TenantID: scope.TenantID, Key: key, Size: int64(size), SHA256: digest}
	if ref.Validate() != nil {
		return BlobRef{}, ErrMembershipDenied
	}
	return ref, nil
}

// ParseAttemptOperationConsentArgumentsRefV1 accepts only an exact full key of
// the closed arm, correlated with the entire original scope. It does not accept
// an arbitrary attempts-prefix, normalize paths or authorize materialization.
func ParseAttemptOperationConsentArgumentsRefV1(scope AttemptOperationScopeV1, key string, cap uint64) (BlobRef, error) {
	if len(key) > 2048 || cap == 0 || cap > 1<<20 {
		return BlobRef{}, ErrMembershipDenied
	}
	last := strings.LastIndexByte(key, '/')
	if last < 0 {
		return BlobRef{}, ErrMembershipDenied
	}
	parts := strings.Split(key[last+1:], ".")
	if len(parts) != 3 || parts[0] != "consent-args-v1" {
		return BlobRef{}, ErrMembershipDenied
	}
	size, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || size > cap || strconv.FormatUint(size, 10) != parts[1] {
		return BlobRef{}, ErrMembershipDenied
	}
	ref, err := AttemptOperationConsentArgumentsRefV1(scope, size, parts[2])
	if err != nil || ref.Key != key {
		return BlobRef{}, ErrMembershipDenied
	}
	return ref, nil
}
