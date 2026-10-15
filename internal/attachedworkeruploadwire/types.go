// Package attachedworkeruploadwire implements closed, bounded output-upload
// metadata. Decoding/comparison is DATA validation, never currentness, private
// producer provenance, a signing grant, native fit or no-future-writer evidence.
package attachedworkeruploadwire

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	Version                        = uint32(1)
	AuthorizationMaxBytes          = 1816
	HeadMaxBytes                   = 2651
	AdmitMaxBytes                  = 4531
	ClaimMaxBytes                  = 2206
	ReconcileMaxBytes              = 2284
	RequestMaxBytes                = AdmitMaxBytes
	ReplyWithoutCapabilityMaxBytes = 3070
	ReplyMaxBytes                  = 19752
	URLMaxBytes                    = 8192
	CapabilityMaxBytes             = 16686
	DeniedReplyBytes               = 19
	PendingReplyBytes              = 20
	AdmitNodes                     = 118
	ClaimNodes                     = 55
	ReconcileNodes                 = 55
	ReplyWithoutCapabilityNodes    = 113
	ReplyNodes                     = 137
	SharedNodes                    = 951
	MaxControlRPCs                 = 5
	SharedRequestBytes             = 13511
	SharedReplyBytes               = 48714
	SharedControlBytes             = SharedRequestBytes + SharedReplyBytes
	// MinimumWireCopyBytes counts two request appearances, two response
	// appearances and two retained capabilities. It is NOT total allocation.
	MinimumWireCopyBytes = 2*SharedRequestBytes + 2*SharedReplyBytes + 2*CapabilityMaxBytes
	// ByteArenaCapacity includes one request, response+sentinel, capability and
	// explicit 4096-byte PUT scratch. Typed storage/parsing/HTTP/TLS/receipt and
	// journal/ACK work must be reserved additionally on the ORIGINAL ledger.
	ByteArenaCapacity = RequestMaxBytes + ReplyMaxBytes + 1 + CapabilityMaxBytes + 4096
)

var ErrInvalid = errors.New("invalid attached-worker upload wire")

type Operation string

const (
	Admit     Operation                           = "admit"
	Claim     Operation                           = "claim"
	Reconcile Operation                           = "reconcile"
	Pending   ports.AttachedWorkerExecutionStatus = "pending"
)

type CallerDeadline struct {
	Version              uint32
	RemainingNanoseconds uint64
}

// Request is a distinct union: fields belonging to another operation MUST be
// zero, not silently ignored. PresentedSecretDigest is never transported.
type Request struct {
	Version                                                                uint32
	Operation                                                              Operation
	Authorization                                                          ports.AttachedWorkerSealedInputAuthorization
	SessionID                                                              domain.SessionID
	HarnessBindingDigest                                                   domain.HarnessBindingDigestV2
	ProfileDigest, SchemaDigest, FinalDigest, AnswerDigest, ProducerDigest string
	Nonce                                                                  domain.IdempotencyKey
	CallerDeadline                                                         CallerDeadline
	Observation                                                            attachedworkeroutput.ProcessObservationV1
	ObservationDigest                                                      string
	Artifacts                                                              []attachedworkeroutput.UploadArtifactMetadataV1
	PlanDigest                                                             string
	Ordinal                                                                uint32
}

type Ordinal struct {
	Ordinal uint32
	State   attachedworkeroutput.UploadOrdinalStateV1
}

// SignedHeaders is closed and ordered; it does not allow an ambient map/header
// extension. A validated signature/origin still belongs to the real signer.
type SignedHeaders struct{ ContentLength, ContentType, ContentMD5, IfNoneMatch, ChecksumSHA256 string }
type Capability struct {
	Version     uint32
	Method, URL string
	Headers     SignedHeaders
	ExpiresAt   time.Time
}
type Reply struct {
	Status                          ports.AttachedWorkerExecutionStatus
	AcceptedAttempt                 domain.AttachedWorkerAttemptV1
	PlanDigest                      string
	CreatedAt, CheckedAt, ExpiresAt time.Time // ExpiresAt is immutable PLAN expiry.
	Ordinals                        []Ordinal
	Capability                      *Capability // its expiry may be EARLIER than immutable plan expiry.
}

func (Request) String() string                     { return "UploadRequest{private:[redacted]}" }
func (v Request) GoString() string                 { return v.String() }
func (Request) MarshalJSON() ([]byte, error)       { return []byte(`"[REDACTED]"`), nil }
func (Reply) String() string                       { return "UploadReply{private:[redacted]}" }
func (v Reply) GoString() string                   { return v.String() }
func (Reply) MarshalJSON() ([]byte, error)         { return []byte(`"[REDACTED]"`), nil }
func (Capability) String() string                  { return "UploadCapability{private:[redacted]}" }
func (v Capability) GoString() string              { return v.String() }
func (Capability) MarshalJSON() ([]byte, error)    { return []byte(`"[REDACTED]"`), nil }
func (SignedHeaders) String() string               { return "UploadHeaders{private:[redacted]}" }
func (v SignedHeaders) GoString() string           { return v.String() }
func (SignedHeaders) MarshalJSON() ([]byte, error) { return []byte(`"[REDACTED]"`), nil }

func id(s string) bool {
	return len(s) > 0 && len(s) <= 160 && domain.ValidateOpaqueID("upload_wire", s) == nil
}
func digest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := range s {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}
func validAuth(a ports.AttachedWorkerSealedInputAuthorization) bool {
	if a.PresentedSecretDigest != "" || !id(string(a.TenantID)) || !id(string(a.OwnerUserID)) || !id(string(a.WorkerID)) || !id(string(a.ConnectionID)) || !id(string(a.RunID)) || !id(string(a.AttemptID)) || !id(string(a.LeaseID)) || a.EnrollmentGeneration == 0 || a.ConnectionGeneration == 0 || a.LeaseGeneration == 0 || a.ExpectedAttemptRevision == 0 || a.AttemptSequence != 1 || a.LeaseExpiresAtUnixMicro <= 0 || !digest(string(a.FenceToken)) || !digest(string(a.ContextDigest)) || !digest(string(a.CapabilityDigest)) || !digest(string(a.PolicyDigest)) {
		return false
	}
	f, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	return err == nil && f == a.FenceToken
}

// ValidateAuthorization is structural comparison data only, not authentication.
func ValidateAuthorization(a ports.AttachedWorkerSealedInputAuthorization) error {
	if !validAuth(a) {
		return ErrInvalid
	}
	return nil
}
func canonicalTime(t time.Time) bool {
	_, offset := t.Zone()
	return !t.IsZero() && offset == 0 && t.Year() >= 1 && t.Year() <= 9999 && t.Nanosecond()%1000 == 0
}
func headTime(t time.Time) bool {
	w := t.UTC().Round(0)
	return !w.IsZero() && w.Nanosecond()%1000 == 0 && time.Unix(w.Unix(), int64(w.Nanosecond())).UTC().Equal(w)
}
func validHead(h domain.AttachedWorkerAttemptV1) bool {
	return h.Validate() == nil && id(string(h.TenantID)) && id(string(h.OwnerUserID)) && id(string(h.WorkerID)) && id(string(h.ConnectionID)) && id(string(h.ExecutionConnectionID)) && id(string(h.RunID)) && id(string(h.AttemptID)) && id(string(h.ReservationID)) && id(string(h.LeaseID)) && h.State == domain.AttachedWorkerAttemptClaimed && h.CancelRevision == 0 && h.CancelDeadline.IsZero() && h.TerminalSequence == 0 && h.TerminalStatus == "" && h.TerminalEvidenceDigest == "" && headTime(h.CreatedAt) && headTime(h.UpdatedAt) && headTime(h.LeaseExpiresAt) && h.LeaseExpiresAt.UnixMicro() > 0 && time.UnixMicro(h.LeaseExpiresAt.UnixMicro()).UTC().Equal(h.LeaseExpiresAt)
}
func validName(s string) bool {
	if len(s) == 0 || len(s) > 255 || !utf8.ValidString(s) || s == "." || s == ".." || strings.ContainsAny(s, "/\\\x00") || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func md5Value(s string) bool {
	if len(s) != 24 || s[22:] != "==" {
		return false
	}
	var b [16]byte
	n, err := base64.StdEncoding.Strict().Decode(b[:], []byte(s))
	return err == nil && n == 16 && base64.StdEncoding.EncodeToString(b[:]) == s
}
func validMetadata(a []attachedworkeroutput.UploadArtifactMetadataV1) bool {
	if len(a) < 1 || len(a) > 2 {
		return false
	}
	var total int64
	for i, v := range a {
		if v.Ordinal != uint32(i) || !validName(v.Name) || (v.MediaType != "text/plain" && v.MediaType != "application/json") || v.Size < 0 || v.Size > 32768 || v.Size > 65536-total || v.Size == 0 && v.MediaType != "text/plain" || !digest(v.SHA256) || !md5Value(v.ContentMD5) {
			return false
		}
		if v.Size == 0 {
			s, m := sha256.Sum256(nil), md5.Sum(nil)
			if v.SHA256 != hex.EncodeToString(s[:]) || v.ContentMD5 != base64.StdEncoding.EncodeToString(m[:]) {
				return false
			}
		}
		for _, p := range a[:i] {
			if strings.EqualFold(p.Name, v.Name) {
				return false
			}
		}
		total += v.Size
	}
	return true
}
func validObservation(o attachedworkeroutput.ProcessObservationV1, d string) bool {
	if !digest(d) || o.ValidateFor(domain.AttachedWorkerTerminalSucceeded, o.CredentialReleaseRequired) != nil {
		return false
	}
	actual, err := o.Digest()
	return err == nil && actual == d
}
func requestLimit(op Operation) int {
	switch op {
	case Admit:
		return AdmitMaxBytes
	case Claim:
		return ClaimMaxBytes
	case Reconcile:
		return ReconcileMaxBytes
	}
	return 0
}
func validRequest(r Request) bool {
	if r.Version != 1 || !validAuth(r.Authorization) || !id(string(r.Nonce)) || r.CallerDeadline.Version != 1 || r.CallerDeadline.RemainingNanoseconds == 0 || r.CallerDeadline.RemainingNanoseconds > math.MaxInt64 {
		return false
	}
	switch r.Operation {
	case Admit:
		return id(string(r.SessionID)) && digest(string(r.HarnessBindingDigest)) && digest(r.ProfileDigest) && digest(r.SchemaDigest) && digest(r.FinalDigest) && digest(r.AnswerDigest) && digest(r.ProducerDigest) && validObservation(r.Observation, r.ObservationDigest) && validMetadata(r.Artifacts) && r.PlanDigest == "" && r.Ordinal == 0
	case Claim, Reconcile:
		if r.SessionID != "" || r.HarnessBindingDigest != "" || r.ProfileDigest != "" || r.SchemaDigest != "" || r.AnswerDigest != "" || r.Observation != (attachedworkeroutput.ProcessObservationV1{}) || r.ObservationDigest != "" || len(r.Artifacts) != 0 {
			return false
		}
		if r.Operation == Claim {
			return digest(r.PlanDigest) && r.Ordinal <= 1 && r.FinalDigest == "" && r.ProducerDigest == ""
		}
		return digest(r.FinalDigest) && digest(r.ProducerDigest) && r.PlanDigest == "" && r.Ordinal == 0
	}
	return false
}

// ValidateRequest performs bounded typed-data validation without encoding a
// wire arena. Its observation digest check has bounded existing hash work.
func ValidateRequest(r Request) error {
	if !validRequest(r) {
		return ErrInvalid
	}
	return nil
}
func validURL(s string) bool {
	if len(s) == 0 || len(s) > URLMaxBytes || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	u, err := url.Parse(s)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.Fragment == "" && u.Opaque == "" && !u.ForceQuery
}
func validHeaders(h SignedHeaders) bool {
	if len(h.ContentLength) < 1 || len(h.ContentLength) > 5 {
		return false
	}
	n, err := strconv.ParseUint(h.ContentLength, 10, 16)
	if err != nil || n > 32768 || strconv.FormatUint(n, 10) != h.ContentLength || (h.ContentType != "text/plain" && h.ContentType != "application/json") || n == 0 && h.ContentType != "text/plain" || !md5Value(h.ContentMD5) || h.IfNoneMatch != "*" || len(h.ChecksumSHA256) != 44 || h.ChecksumSHA256[43] != '=' {
		return false
	}
	var b [32]byte
	k, e := base64.StdEncoding.Strict().Decode(b[:], []byte(h.ChecksumSHA256))
	return e == nil && k == 32 && base64.StdEncoding.EncodeToString(b[:]) == h.ChecksumSHA256
}
func validReply(r Reply) bool {
	if r.Status == ports.AttachedWorkerExecutionDenied || r.Status == Pending {
		return r.AcceptedAttempt == (domain.AttachedWorkerAttemptV1{}) && r.PlanDigest == "" && r.CreatedAt.IsZero() && r.CheckedAt.IsZero() && r.ExpiresAt.IsZero() && len(r.Ordinals) == 0 && r.Capability == nil
	}
	if (r.Status != ports.AttachedWorkerExecutionApplied && r.Status != ports.AttachedWorkerExecutionReplayed) || !validHead(r.AcceptedAttempt) || !digest(r.PlanDigest) || !canonicalTime(r.CreatedAt) || !canonicalTime(r.CheckedAt) || !canonicalTime(r.ExpiresAt) || r.CheckedAt.Before(r.CreatedAt) || r.CheckedAt.Before(r.AcceptedAttempt.UpdatedAt) || !r.CheckedAt.Before(r.ExpiresAt) || r.ExpiresAt.Sub(r.CreatedAt) > 15*time.Second || !r.CreatedAt.Before(r.ExpiresAt) || r.ExpiresAt.After(r.AcceptedAttempt.LeaseExpiresAt) || len(r.Ordinals) < 1 || len(r.Ordinals) > 2 {
		return false
	}
	for i, o := range r.Ordinals {
		if o.Ordinal != uint32(i) {
			return false
		}
		switch o.State {
		case attachedworkeroutput.UploadOrdinalUnissuedV1, attachedworkeroutput.UploadOrdinalWritePendingV1, attachedworkeroutput.UploadOrdinalNotDispatchedV1, attachedworkeroutput.UploadOrdinalSourceVerifiedV1:
		default:
			return false
		}
	}
	if c := r.Capability; c != nil {
		return r.Status == ports.AttachedWorkerExecutionApplied && c.Version == 1 && c.Method == "PUT" && validURL(c.URL) && validHeaders(c.Headers) && canonicalTime(c.ExpiresAt) && c.ExpiresAt.After(r.CheckedAt) && !c.ExpiresAt.After(r.ExpiresAt)
	}
	return true
}

// ValidateReply checks only the closed typed representation, not the source
// of a response, original RPC association or permission to use a capability.
func ValidateReply(r Reply) error {
	if !validReply(r) {
		return ErrInvalid
	}
	return nil
}

// CompareReply validates static association with the complete original admit
// and (for subsequent calls) retained original plan. It does NOT authenticate
// the RPC, establish original clocks/currentness, spend an ordinal, grant PUT,
// or establish no-future-writer. NewAttemptOutputUploadPlanV1 and Validate do
// bounded DATA hashing/allocation, to be charged on the SAME original ledger.
func CompareReply(original, request Request, reply Reply, retained *attachedworkeroutput.AttemptOutputUploadPlanV1) error {
	if !validRequest(original) || original.Operation != Admit || !validRequest(request) || !validReply(reply) || (reply.Status != ports.AttachedWorkerExecutionApplied && reply.Status != ports.AttachedWorkerExecutionReplayed) || request.Authorization != original.Authorization || request.Nonce != original.Nonce {
		return ErrInvalid
	}
	h := reply.AcceptedAttempt
	m := ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionApplied, AttemptRevision: h.Revision, Attempt: h}
	if !m.Matches(original.Authorization) || h.ExecutionConnectionID != original.Authorization.ConnectionID || h.ExecutionConnectionGeneration != original.Authorization.ConnectionGeneration {
		return ErrInvalid
	}
	identity := attachedworkeroutput.UploadPlanIdentityV1{Binding: original.Authorization, SessionID: original.SessionID, HarnessBindingDigest: original.HarnessBindingDigest, ProfileDigest: original.ProfileDigest, SchemaDigest: original.SchemaDigest, FinalDigest: original.FinalDigest, AnswerDigest: original.AnswerDigest, ProducerDigest: original.ProducerDigest, Nonce: original.Nonce, CreatedAt: reply.CreatedAt, ExpiresAt: reply.ExpiresAt}
	plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(identity, original.Artifacts)
	if err != nil || reply.PlanDigest != plan.PlanDigest || len(reply.Ordinals) != len(plan.Ordinals) {
		return ErrInvalid
	}
	if request.Operation == Admit {
		if request.SessionID != original.SessionID || request.HarnessBindingDigest != original.HarnessBindingDigest || request.ProfileDigest != original.ProfileDigest || request.SchemaDigest != original.SchemaDigest || request.FinalDigest != original.FinalDigest || request.AnswerDigest != original.AnswerDigest || request.ProducerDigest != original.ProducerDigest || request.Observation != original.Observation || request.ObservationDigest != original.ObservationDigest || len(request.Artifacts) != len(original.Artifacts) || reply.Capability != nil {
			return ErrInvalid
		}
		for i := range original.Artifacts {
			if original.Artifacts[i] != request.Artifacts[i] {
				return ErrInvalid
			}
		}
	} else {
		if retained == nil || retained.Validate() != nil || !retained.Identity.CreatedAt.Equal(identity.CreatedAt) || !retained.Identity.ExpiresAt.Equal(identity.ExpiresAt) || retained.PlanDigest != plan.PlanDigest || len(retained.Ordinals) != len(plan.Ordinals) {
			return ErrInvalid
		}
		// Wire times preserve instants, not a Go Location pointer/monotonic bit.
		// This normalization is DATA equality, never original-clock provenance.
		retainedIdentity := retained.Identity
		retainedIdentity.CreatedAt, retainedIdentity.ExpiresAt = identity.CreatedAt, identity.ExpiresAt
		if retainedIdentity != identity {
			return ErrInvalid
		}
		for i := range plan.Ordinals {
			if retained.Ordinals[i].Metadata != plan.Ordinals[i].Metadata || retained.Ordinals[i].Key != plan.Ordinals[i].Key {
				return ErrInvalid
			}
		}
		if request.Operation == Claim {
			if request.PlanDigest != reply.PlanDigest || request.Ordinal >= uint32(len(reply.Ordinals)) || reply.Ordinals[request.Ordinal].State != attachedworkeroutput.UploadOrdinalWritePendingV1 {
				return ErrInvalid
			}
		} else if request.FinalDigest != original.FinalDigest || request.ProducerDigest != original.ProducerDigest || reply.Capability != nil {
			return ErrInvalid
		}
	}
	if c := reply.Capability; c != nil {
		if request.Operation != Claim {
			return ErrInvalid
		}
		v := plan.Ordinals[request.Ordinal]
		u, err := url.Parse(c.URL)
		if err != nil || u.Path != "/"+v.Key && !strings.HasSuffix(u.Path, "/"+v.Key) || c.Headers.ContentLength != strconv.FormatInt(v.Metadata.Size, 10) || c.Headers.ContentType != v.Metadata.MediaType || c.Headers.ContentMD5 != v.Metadata.ContentMD5 {
			return ErrInvalid
		}
		var b [32]byte
		n, e := hex.Decode(b[:], []byte(v.Metadata.SHA256))
		if e != nil || n != 32 || c.Headers.ChecksumSHA256 != base64.StdEncoding.EncodeToString(b[:]) {
			return ErrInvalid
		}
	}
	return nil
}
