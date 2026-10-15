package ports

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	AttachedWorkerOutputObjectVersionV1 = uint32(1)
	AttachedWorkerOutputArtifactBytesV1 = int64(32 << 10)
	AttachedWorkerOutputTotalBytesV1    = int64(64 << 10)
	AttachedWorkerOutputLifetimeV1      = 15 * time.Second
)

var ErrAttachedWorkerOutputObjectInvalid = errors.New("attached worker output object is invalid")

// AttachedWorkerOutputObjectRequestV1 is server-owned signing DATA from the
// original admitted output plan. Validation does not authenticate a producer,
// acquire pre-Put custody, select current authority, or authorize another body.
// In particular, the durable owner still accounts for the whole two-body/64KiB
// cohort and retains ambiguous signing/upload outcomes.
type AttachedWorkerOutputObjectRequestV1 struct {
	Version                                              uint32
	Authorization                                        AttachedWorkerSealedInputAuthorization
	AcceptedAttempt                                      domain.AttachedWorkerAttemptV1
	SessionID                                            domain.SessionID
	HarnessBindingDigest                                 domain.HarnessBindingDigestV2
	ProfileDigest, SchemaDigest, FinalDigest, PlanDigest string
	Ordinal                                              uint32
	MediaType                                            string
	Size                                                 int64
	SHA256, ContentMD5                                   string
	// ExpiresAt is the SAME original absolute server cutoff, not a duration to
	// restart when signing or retrying. Earlier caller authority remains separate.
	ExpiresAt time.Time
}

func (AttachedWorkerOutputObjectRequestV1) String() string {
	return "AttachedWorkerOutputObjectRequestV1{private:[redacted]}"
}
func (v AttachedWorkerOutputObjectRequestV1) GoString() string { return v.String() }

// ValidateAt checks bounded exact DATA against the signer's server clock. The
// clock and this value are not a substitute for the original source transaction.
func (r AttachedWorkerOutputObjectRequestV1) ValidateAt(now time.Time) error {
	a, h := r.Authorization, r.AcceptedAttempt
	for _, s := range []string{string(a.TenantID), string(a.OwnerUserID), string(a.WorkerID), string(a.ConnectionID), string(a.RunID), string(a.AttemptID), string(a.LeaseID), string(r.SessionID), string(h.TenantID), string(h.OwnerUserID), string(h.WorkerID), string(h.ConnectionID), string(h.ExecutionConnectionID), string(h.RunID), string(h.AttemptID), string(h.ReservationID), string(h.LeaseID)} {
		if len(s) > 160 {
			return ErrAttachedWorkerOutputObjectInvalid
		}
	}
	for _, s := range []string{string(a.FenceToken), string(a.ContextDigest), string(a.CapabilityDigest), string(a.PolicyDigest), string(h.FenceToken), string(h.ContextDigest), string(h.CapabilityDigest), string(h.PolicyDigest), string(r.HarnessBindingDigest), r.ProfileDigest, r.SchemaDigest, r.FinalDigest, r.PlanDigest, r.SHA256} {
		if !workerOutputDigest(s) {
			return ErrAttachedWorkerOutputObjectInvalid
		}
	}
	if r.Version != AttachedWorkerOutputObjectVersionV1 || a.PresentedSecretDigest != "" || a.ExpectedAttemptRevision == 0 || a.ExpectedAttemptRevision != h.Revision ||
		h.ExecutionConnectionID != a.ConnectionID || h.ExecutionConnectionGeneration != a.ConnectionGeneration ||
		h.Validate() != nil || r.SessionID.Validate() != nil || h.CancelRevision != 0 || !h.CancelDeadline.IsZero() || h.UpdatedAt.After(now) ||
		!(AttachedWorkerSealedInputAuthorizationResult{Status: AttachedWorkerExecutionApplied, AttemptRevision: h.Revision, Attempt: h}).Matches(a) ||
		r.Ordinal > 1 || r.Size < 0 || r.Size > AttachedWorkerOutputArtifactBytesV1 ||
		(r.MediaType != "text/plain" && r.MediaType != "application/json") || r.Size == 0 && r.MediaType != "text/plain" ||
		len(r.ContentMD5) != 24 || now.IsZero() || r.ExpiresAt.IsZero() || !now.Before(r.ExpiresAt) ||
		r.ExpiresAt.Sub(now) > AttachedWorkerOutputLifetimeV1 || r.ExpiresAt.After(h.LeaseExpiresAt) {
		return ErrAttachedWorkerOutputObjectInvalid
	}
	m, err := base64.StdEncoding.Strict().DecodeString(r.ContentMD5)
	if err != nil || len(m) != md5.Size || base64.StdEncoding.EncodeToString(m) != r.ContentMD5 {
		return ErrAttachedWorkerOutputObjectInvalid
	}
	if r.Size == 0 {
		s, m := sha256.Sum256(nil), md5.Sum(nil)
		if r.SHA256 != hex.EncodeToString(s[:]) || r.ContentMD5 != base64.StdEncoding.EncodeToString(m[:]) {
			return ErrAttachedWorkerOutputObjectInvalid
		}
	}
	if (domain.BlobRef{TenantID: a.TenantID, Key: r.CandidateKey(), Size: r.Size, SHA256: r.SHA256}).Validate() != nil {
		return ErrAttachedWorkerOutputObjectInvalid
	}
	return nil
}

func workerOutputDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, b := range []byte(s) {
		if b < '0' || b > '9' && b < 'a' || b > 'f' {
			return false
		}
	}
	return true
}

// CandidateKey uses only exact server-selected plan identity, never an artifact
// filename or a caller-selected object locator. Call ValidateAt before use.
func (r AttachedWorkerOutputObjectRequestV1) CandidateKey() string {
	return domain.SessionRunObjectPrefix(r.Authorization.TenantID, r.SessionID, r.Authorization.RunID) +
		"attached-worker/candidates/" + string(r.Authorization.AttemptID) + "/" + r.PlanDigest + "/" + strconv.FormatUint(uint64(r.Ordinal), 10)
}

// AttachedWorkerOutputObjectCapabilityV1 is distinct from browser uploads.
// It is a private exact-object PUT capability, not a producer/currentness proof.
// Transport owners must explicitly encode the selected closed response rather
// than log or generically serialize this credential-bearing value.
type AttachedWorkerOutputObjectCapabilityV1 struct {
	Version     uint32
	Method, URL string
	Headers     map[string]string
	Object      domain.BlobRef
	ExpiresAt   time.Time
}

func (AttachedWorkerOutputObjectCapabilityV1) String() string {
	return "AttachedWorkerOutputObjectCapabilityV1{private:[redacted]}"
}
func (v AttachedWorkerOutputObjectCapabilityV1) GoString() string { return v.String() }
func (AttachedWorkerOutputObjectCapabilityV1) MarshalJSON() ([]byte, error) {
	return []byte(`"[REDACTED]"`), nil
}

type AttachedWorkerOutputObjectSignerV1 interface {
	PresignAttachedWorkerOutputObject(context.Context, AttachedWorkerOutputObjectRequestV1) (AttachedWorkerOutputObjectCapabilityV1, error)
}
