// Package attachedworkeronboarding owns the private, operator-assisted handoff.
// It never supplies public read DTOs or execution/provider authorization.
package attachedworkeronboarding

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/domain"
	"golang.org/x/sys/unix"
)

const MaxPrivateBytes = 128 << 10

var ErrInvalid = errors.New("private onboarding handoff invalid")
var ErrConflict = errors.New("private onboarding handoff conflicts")
var ErrAmbiguous = errors.New("private onboarding durability ambiguous")

type GrantV1 struct {
	Version            uint32                          `json:"version"`
	Enrollment         domain.AttachedWorkerEnrollment `json:"enrollment"`
	ControlPlaneOrigin string                          `json:"control_plane_origin"`
	BootstrapSecret    []byte                          `json:"bootstrap_secret"`
}
type ClaimV1 struct {
	Version           uint32                          `json:"version"`
	Enrollment        domain.AttachedWorkerEnrollment `json:"enrollment"`
	BootstrapSecret   []byte                          `json:"bootstrap_secret"`
	IdentityPublicKey []byte                          `json:"identity_public_key"`
	Proof             []byte                          `json:"proof"`
}
type ReceiptV1 struct {
	Version     uint32                          `json:"version"`
	Worker      domain.AttachedWorker           `json:"worker"`
	ResourceID  domain.SubscriptionConnectionID `json:"resource_id"`
	ActorID     domain.ActorID                  `json:"actor_id"`
	Entitlement domain.EntitlementState         `json:"entitlement"`
	Quota       domain.ProviderQuotaState       `json:"quota"`
}
type RotationV1 struct {
	Version      uint32                `json:"version"`
	Worker       domain.AttachedWorker `json:"worker"`
	NewPublicKey []byte                `json:"new_public_key"`
	CurrentProof []byte                `json:"current_proof"`
	NewProof     []byte                `json:"new_proof"`
}
type grantDisk GrantV1
type claimDisk ClaimV1
type rotationDisk RotationV1

func (GrantV1) String() string                  { return "GrantV1{[REDACTED]}" }
func (GrantV1) GoString() string                { return "GrantV1{[REDACTED]}" }
func (GrantV1) MarshalJSON() ([]byte, error)    { return []byte(`{"version":1,"redacted":true}`), nil }
func (ClaimV1) String() string                  { return "ClaimV1{[REDACTED]}" }
func (ClaimV1) GoString() string                { return "ClaimV1{[REDACTED]}" }
func (ClaimV1) MarshalJSON() ([]byte, error)    { return []byte(`{"version":1,"redacted":true}`), nil }
func (RotationV1) String() string               { return "RotationV1{[REDACTED]}" }
func (RotationV1) GoString() string             { return "RotationV1{[REDACTED]}" }
func (RotationV1) MarshalJSON() ([]byte, error) { return []byte(`{"version":1,"redacted":true}`), nil }
func scopedID(kind string, parts ...string) string {
	encoded, _ := json.Marshal(append([]string{kind}, parts...))
	digest := sha256.Sum256(encoded)
	return kind + "-" + fmt.Sprintf("%x", digest[:16])
}
func ResourceIDForWorker(tenant domain.TenantID, owner domain.UserID, worker domain.AttachedWorkerID) domain.SubscriptionConnectionID {
	return domain.SubscriptionConnectionID(scopedID("attached", string(tenant), string(owner), string(worker)))
}
func ActorIDForOwner(tenant domain.TenantID, owner domain.UserID) domain.ActorID {
	return domain.ActorID(scopedID("owner", string(tenant), string(owner)))
}
func ValidateOrigin(origin string) error {
	u, e := url.Parse(origin)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.Opaque != "" || u.String() != origin || strings.ToLower(u.Host) != u.Host {
		return ErrInvalid
	}
	return nil
}
func (g GrantV1) Validate() error {
	if g.Version != 1 || g.Enrollment.Validate() != nil || !g.Enrollment.ConsumedAt.IsZero() || ValidateOrigin(g.ControlPlaneOrigin) != nil || len(g.BootstrapSecret) != 32 || domain.DigestWorkerBootstrap(g.BootstrapSecret) != g.Enrollment.BootstrapDigest {
		return ErrInvalid
	}
	return nil
}
func (c ClaimV1) Validate() error {
	if c.Version != 1 || c.Enrollment.Validate() != nil || len(c.BootstrapSecret) != 32 || domain.DigestWorkerBootstrap(c.BootstrapSecret) != c.Enrollment.BootstrapDigest || len(c.IdentityPublicKey) != ed25519.PublicKeySize || len(c.Proof) != ed25519.SignatureSize {
		return ErrInvalid
	}
	transcript, e := attachedworker.ClaimProofTranscript(c.Enrollment, c.Enrollment.Revision, c.IdentityPublicKey)
	if e != nil || !ed25519.Verify(c.IdentityPublicKey, transcript, c.Proof) {
		return ErrInvalid
	}
	return nil
}
func (r ReceiptV1) Validate() error {
	w := r.Worker
	if r.Version != 1 || w.Validate() != nil || w.DesiredState != domain.AttachedWorkerDesiredActive || r.ResourceID != ResourceIDForWorker(w.TenantID, w.OwnerUserID, w.ID) || r.ActorID != ActorIDForOwner(w.TenantID, w.OwnerUserID) || r.Entitlement != domain.EntitlementUnknown || r.Quota != domain.ProviderQuotaUnknown {
		return ErrInvalid
	}
	return nil
}
func (r RotationV1) Validate() error {
	if r.Version != 1 || r.Worker.Validate() != nil || r.Worker.DesiredState != domain.AttachedWorkerDesiredActive || len(r.NewPublicKey) != 32 || bytes.Equal(r.NewPublicKey, r.Worker.IdentityPublicKey) {
		return ErrInvalid
	}
	transcript, e := attachedworker.RotationProofTranscript(r.Worker, r.Worker.Revision, r.NewPublicKey)
	if e != nil || !ed25519.Verify(r.Worker.IdentityPublicKey, transcript, r.CurrentProof) || !ed25519.Verify(r.NewPublicKey, transcript, r.NewProof) {
		return ErrInvalid
	}
	return nil
}

// A pinned parent descriptor and O_NOFOLLOW protect final-component races.
// Existing private directories are mandatory; this API never chmods caller data.
func parentFile(path string) (*os.File, string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Dir(path) == path {
		return nil, "", ErrInvalid
	}
	parent := filepath.Dir(path)
	real, e := filepath.EvalSymlinks(parent)
	if e != nil || real != parent {
		return nil, "", ErrInvalid
	}
	fd, e := unix.Open(parent, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if e != nil {
		return nil, "", ErrInvalid
	}
	f := os.NewFile(uintptr(fd), parent)
	info, e := f.Stat()
	if e != nil || !info.IsDir() || info.Mode().Perm() != 0700 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		f.Close()
		return nil, "", ErrInvalid
	}
	return f, filepath.Base(path), nil
}
func readAt(parent *os.File, name string) ([]byte, error) {
	fd, e := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		if errors.Is(e, unix.ENOENT) {
			return nil, os.ErrNotExist
		}
		return nil, ErrInvalid
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) || info.Size() > MaxPrivateBytes {
		return nil, ErrInvalid
	}
	b, e := io.ReadAll(io.LimitReader(f, MaxPrivateBytes+1))
	if e != nil || len(b) > MaxPrivateBytes {
		return nil, ErrInvalid
	}
	// Replaying a previously ambiguous write must establish file durability
	// before returning a claim or other continuation to a caller.
	if f.Sync() != nil {
		return nil, ErrAmbiguous
	}
	return b, nil
}
func validateJSONTokens(d *json.Decoder, depth int) error {
	if depth > 64 {
		return ErrInvalid
	}
	token, err := d.Token()
	if err != nil {
		return ErrInvalid
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			keyToken, err := d.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok || seen[key] {
				return ErrInvalid
			}
			seen[key] = true
			if validateJSONTokens(d, depth+1) != nil {
				return ErrInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalid
		}
	case '[':
		for d.More() {
			if validateJSONTokens(d, depth+1) != nil {
				return ErrInvalid
			}
		}
		end, err := d.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
func ReadPrivate(path string, out any) error {
	return readPrivate(path, out, nil)
}

// syncParent is a per-call test seam for durability failures; production always
// syncs the pinned descriptor. It is never mutable global process state.
func readPrivate(path string, out any, syncParent func(*os.File) error) error {
	parent, name, e := parentFile(path)
	if e != nil {
		return e
	}
	defer parent.Close()
	b, e := readAt(parent, name)
	if e != nil {
		return e
	}
	check := json.NewDecoder(bytes.NewReader(b))
	if validateJSONTokens(check, 0) != nil {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	if syncParent == nil {
		syncParent = func(parent *os.File) error { return parent.Sync() }
	}
	if syncParent(parent) != nil {
		return ErrAmbiguous
	}
	return nil
}
func WritePrivate(path string, value any) error {
	b, e := json.Marshal(value)
	if e != nil || len(b) > MaxPrivateBytes {
		return ErrInvalid
	}
	parent, name, e := parentFile(path)
	if e != nil {
		return e
	}
	defer parent.Close()
	fd, e := unix.Openat(int(parent.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if errors.Is(e, unix.EEXIST) {
		existing, re := readAt(parent, name)
		if re != nil {
			return re
		}
		if !bytes.Equal(existing, b) {
			return ErrConflict
		}
		if parent.Sync() != nil {
			return ErrAmbiguous
		}
		return nil
	}
	if e != nil {
		return ErrInvalid
	}
	f := os.NewFile(uintptr(fd), name)
	_, e = f.Write(b)
	if e == nil {
		e = f.Sync()
	}
	e = errors.Join(e, f.Close())
	if e != nil {
		return ErrAmbiguous
	}
	if parent.Sync() != nil {
		return ErrAmbiguous
	}
	return nil
}
func ReadGrant(path string) (GrantV1, error) {
	var d grantDisk
	e := ReadPrivate(path, &d)
	g := GrantV1(d)
	if e == nil {
		e = g.Validate()
	}
	return g, e
}
func WriteGrant(path string, g GrantV1) error {
	if e := g.Validate(); e != nil {
		return e
	}
	return WritePrivate(path, grantDisk(g))
}
func ReadClaim(path string) (ClaimV1, error) {
	var d claimDisk
	e := ReadPrivate(path, &d)
	g := ClaimV1(d)
	if e == nil {
		e = g.Validate()
	}
	return g, e
}
func WriteClaim(path string, c ClaimV1) error {
	if e := c.Validate(); e != nil {
		return e
	}
	return WritePrivate(path, claimDisk(c))
}
func ReadReceipt(path string) (ReceiptV1, error) {
	var r ReceiptV1
	e := ReadPrivate(path, &r)
	if e == nil {
		e = r.Validate()
	}
	return r, e
}
func WriteReceipt(path string, r ReceiptV1) error {
	if e := r.Validate(); e != nil {
		return e
	}
	return WritePrivate(path, r)
}
func ReadRotation(path string) (RotationV1, error) {
	var d rotationDisk
	e := ReadPrivate(path, &d)
	r := RotationV1(d)
	if e == nil {
		e = r.Validate()
	}
	return r, e
}
func WriteRotation(path string, r RotationV1) error {
	if e := r.Validate(); e != nil {
		return e
	}
	return WritePrivate(path, rotationDisk(r))
}
func ReadWorkerHead(path string) (domain.AttachedWorker, error) {
	var w domain.AttachedWorker
	e := ReadPrivate(path, &w)
	if e == nil && w.Validate() != nil {
		e = ErrInvalid
	}
	return w, e
}
func WriteWorkerHead(path string, w domain.AttachedWorker) error {
	if w.Validate() != nil {
		return ErrInvalid
	}
	return WritePrivate(path, w)
}
func (GrantV1) Format(s fmt.State, _ rune)    { _, _ = io.WriteString(s, "GrantV1{[REDACTED]}") }
func (ClaimV1) Format(s fmt.State, _ rune)    { _, _ = io.WriteString(s, "ClaimV1{[REDACTED]}") }
func (RotationV1) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "RotationV1{[REDACTED]}") }
