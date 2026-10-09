package attachedworkeronboarding

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"reflect"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/domain"
	"golang.org/x/sys/unix"
)

// SetupV1 is declarative configuration only: it cannot import an identity key,
// server generation, provider credential, or runtime observation.
type SetupV1 struct {
	Version            uint32                              `json:"version"`
	ControlPlaneOrigin string                              `json:"control_plane_origin"`
	OCI                attachedworkerlocal.OCIConfigV1     `json:"oci"`
	Harness            attachedworkerlocal.HarnessConfigV1 `json:"harness"`
}
type enrollmentPending struct {
	Version    uint32    `json:"version"`
	Grant      grantDisk `json:"grant"`
	Setup      SetupV1   `json:"setup"`
	Claim      claimDisk `json:"claim"`
	PrivateKey []byte    `json:"private_key"`
}
type rotationPending struct {
	Version    uint32                         `json:"version"`
	Base       attachedworkerlocal.ManifestV1 `json:"base"`
	Rotation   rotationDisk                   `json:"rotation"`
	PrivateKey []byte                         `json:"private_key"`
}

func ReadSetup(path string) (SetupV1, error) {
	var s SetupV1
	e := ReadPrivate(path, &s)
	if e == nil && (s.Version != 1 || ValidateOrigin(s.ControlPlaneOrigin) != nil) {
		e = ErrInvalid
	}
	return s, e
}
func WriteSetup(path string, s SetupV1) error {
	if s.Version != 1 || ValidateOrigin(s.ControlPlaneOrigin) != nil {
		return ErrInvalid
	}
	return WritePrivate(path, s)
}
func lockPending(ctx context.Context, path string) (func(), error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrInvalid
	}
	parent, name, e := parentFile(path)
	if e != nil {
		return nil, e
	}
	defer parent.Close()
	fd, e := unix.Openat(int(parent.Fd()), name+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0600)
	if e != nil {
		return nil, ErrInvalid
	}
	f := os.NewFile(uintptr(fd), name)
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Sys().(*syscall.Stat_t).Uid != uint32(os.Geteuid()) {
		f.Close()
		return nil, ErrInvalid
	}
	if unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) != nil {
		f.Close()
		return nil, attachedworkerlocal.ErrStateBusy
	}
	return func() { _ = unix.Flock(fd, unix.LOCK_UN); _ = f.Close() }, nil
}
func manifestFor(p enrollmentPending, w domain.AttachedWorker) attachedworkerlocal.ManifestV1 {
	return attachedworkerlocal.ManifestV1{Version: 1, Revision: 1, ControlPlaneOrigin: p.Setup.ControlPlaneOrigin, TenantID: w.TenantID, OwnerUserID: w.OwnerUserID, WorkerID: w.ID, EnrollmentGeneration: w.EnrollmentGeneration, ConnectionGeneration: w.ConnectionGeneration, IdentityKeyFingerprint: string(domain.DigestAttachedWorkerIdentityKey(w.IdentityPublicKey)), OCI: p.Setup.OCI, Harness: p.Setup.Harness, Lifecycle: attachedworkerlocal.LifecycleActive, CreatedAt: w.CreatedAt, UpdatedAt: w.CreatedAt}
}
func secretFor(m attachedworkerlocal.ManifestV1, key []byte) attachedworkerlocal.SecretRecordV1 {
	return attachedworkerlocal.SecretRecordV1{Version: 1, ManifestRevision: m.Revision, TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, WorkerID: m.WorkerID, EnrollmentGeneration: m.EnrollmentGeneration, ConnectionGeneration: m.ConnectionGeneration, IdentityPrivateKey: append([]byte(nil), key...)}
}
func validEnrollmentPending(p enrollmentPending) bool {
	g := GrantV1(p.Grant)
	c := ClaimV1(p.Claim)
	if p.Version != 1 || g.Validate() != nil || c.Validate() != nil || p.Setup.Version != 1 || p.Setup.ControlPlaneOrigin != g.ControlPlaneOrigin || !reflect.DeepEqual(c.Enrollment, g.Enrollment) || !bytes.Equal(c.BootstrapSecret, g.BootstrapSecret) || len(p.PrivateKey) != 64 {
		return false
	}
	key := ed25519.PrivateKey(p.PrivateKey)
	if !bytes.Equal(ed25519.NewKeyFromSeed(key.Seed()), key) || !bytes.Equal(key.Public().(ed25519.PublicKey), c.IdentityPublicKey) {
		return false
	}
	w := domain.AttachedWorker{TenantID: g.Enrollment.TenantID, OwnerUserID: g.Enrollment.OwnerUserID, ID: g.Enrollment.WorkerID, IdentityPublicKey: c.IdentityPublicKey, EnrollmentGeneration: 1, CreatedAt: g.Enrollment.CreatedAt}
	return manifestFor(p, w).Validate() == nil
}

func PrepareEnrollment(ctx context.Context, store *attachedworkerlocal.Store, pendingPath string, grant GrantV1, setup SetupV1, random io.Reader) (ClaimV1, error) {
	if store == nil || grant.Validate() != nil || setup.Version != 1 || setup.ControlPlaneOrigin != grant.ControlPlaneOrigin {
		return ClaimV1{}, ErrInvalid
	}
	unlock, e := lockPending(ctx, pendingPath)
	if e != nil {
		return ClaimV1{}, e
	}
	defer unlock()
	var p enrollmentPending
	e = ReadPrivate(pendingPath, &p)
	if e == nil {
		defer clear(p.PrivateKey)
		if !validEnrollmentPending(p) || !reflect.DeepEqual(p.Grant, grantDisk(grant)) || !reflect.DeepEqual(p.Setup, setup) {
			return ClaimV1{}, ErrConflict
		}
		return ClaimV1(p.Claim), nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return ClaimV1{}, e
	}
	// A partial installation is not permission to generate another identity.
	if _, e = store.LoadSnapshot(ctx); !errors.Is(e, attachedworkerlocal.ErrStateMissing) {
		if e == nil {
			e = ErrConflict
		}
		return ClaimV1{}, e
	}
	if random == nil {
		random = rand.Reader
	}
	public, private, e := ed25519.GenerateKey(random)
	if e != nil {
		return ClaimV1{}, ErrInvalid
	}
	defer clear(private)
	transcript, e := attachedworker.ClaimProofTranscript(grant.Enrollment, grant.Enrollment.Revision, public)
	if e != nil {
		return ClaimV1{}, ErrInvalid
	}
	claim := ClaimV1{Version: 1, Enrollment: grant.Enrollment, BootstrapSecret: append([]byte(nil), grant.BootstrapSecret...), IdentityPublicKey: public, Proof: ed25519.Sign(private, transcript)}
	p = enrollmentPending{Version: 1, Grant: grantDisk(grant), Setup: setup, Claim: claimDisk(claim), PrivateKey: private}
	if !validEnrollmentPending(p) {
		return ClaimV1{}, ErrInvalid
	}
	if e = WritePrivate(pendingPath, p); e != nil {
		return ClaimV1{}, e
	}
	return claim, nil
}
func CompleteEnrollment(ctx context.Context, store *attachedworkerlocal.Store, pendingPath string, receipt ReceiptV1) error {
	if store == nil || receipt.Validate() != nil {
		return ErrInvalid
	}
	unlock, e := lockPending(ctx, pendingPath)
	if e != nil {
		return e
	}
	defer unlock()
	var p enrollmentPending
	if e = ReadPrivate(pendingPath, &p); e != nil {
		return e
	}
	defer clear(p.PrivateKey)
	if !validEnrollmentPending(p) {
		return ErrInvalid
	}
	w := receipt.Worker
	g := p.Grant.Enrollment
	if w.TenantID != g.TenantID || w.OwnerUserID != g.OwnerUserID || w.ID != g.WorkerID || w.DisplayName != g.DisplayName || !bytes.Equal(w.IdentityPublicKey, p.Claim.IdentityPublicKey) || w.Revision != 1 || w.EnrollmentGeneration != 1 || w.ConnectionGeneration != 0 || w.ObservedState != domain.AttachedWorkerObservedOffline || !w.CreatedAt.Equal(w.UpdatedAt) || w.CreatedAt.Before(g.CreatedAt) || !w.CreatedAt.Before(g.ExpiresAt) {
		return ErrConflict
	}
	manifest := manifestFor(p, w)
	secret := secretFor(manifest, p.PrivateKey)
	defer clear(secret.IdentityPrivateKey)
	snapshot, e := store.LoadSnapshot(ctx)
	if e == nil {
		if !reflect.DeepEqual(snapshot.Manifest, manifest) {
			return ErrConflict
		}
		existing, e := store.LoadSecret(ctx)
		if e != nil {
			return e
		}
		defer clear(existing.IdentityPrivateKey)
		if !reflect.DeepEqual(existing, secret) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, attachedworkerlocal.ErrStateMissing) {
		return e
	}
	return store.Initialize(ctx, manifest, secret)
}
func matchesWorker(m attachedworkerlocal.ManifestV1, w domain.AttachedWorker) bool {
	return w.Validate() == nil && w.DesiredState == domain.AttachedWorkerDesiredActive && m.Lifecycle == attachedworkerlocal.LifecycleActive && m.TenantID == w.TenantID && m.OwnerUserID == w.OwnerUserID && m.WorkerID == w.ID && m.EnrollmentGeneration == w.EnrollmentGeneration && m.ConnectionGeneration == w.ConnectionGeneration && m.IdentityKeyFingerprint == string(domain.DigestAttachedWorkerIdentityKey(w.IdentityPublicKey))
}
func validRotationPending(p rotationPending) bool {
	r := RotationV1(p.Rotation)
	return p.Version == 1 && p.Base.Validate() == nil && r.Validate() == nil && matchesWorker(p.Base, r.Worker) && len(p.PrivateKey) == 64 && bytes.Equal(ed25519.NewKeyFromSeed(ed25519.PrivateKey(p.PrivateKey).Seed()), p.PrivateKey) && bytes.Equal(ed25519.PrivateKey(p.PrivateKey).Public().(ed25519.PublicKey), r.NewPublicKey)
}
func PrepareRotation(ctx context.Context, store *attachedworkerlocal.Store, pendingPath string, worker domain.AttachedWorker, random io.Reader) (RotationV1, error) {
	if store == nil {
		return RotationV1{}, ErrInvalid
	}
	unlock, e := lockPending(ctx, pendingPath)
	if e != nil {
		return RotationV1{}, e
	}
	defer unlock()
	lease, e := store.AcquireRuntime(ctx)
	if e != nil {
		return RotationV1{}, e
	}
	defer lease.Close()
	snapshot, e := lease.LoadSnapshot(ctx)
	if e != nil {
		return RotationV1{}, e
	}
	if !matchesWorker(snapshot.Manifest, worker) {
		return RotationV1{}, ErrConflict
	}
	var p rotationPending
	e = ReadPrivate(pendingPath, &p)
	if e == nil {
		if !validRotationPending(p) || !reflect.DeepEqual(p.Base, snapshot.Manifest) || !reflect.DeepEqual(p.Rotation.Worker, worker) {
			return RotationV1{}, ErrConflict
		}
		defer clear(p.PrivateKey)
		return RotationV1(p.Rotation), nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return RotationV1{}, e
	}
	secret, e := lease.LoadSecret(ctx)
	if e != nil {
		return RotationV1{}, e
	}
	defer clear(secret.IdentityPrivateKey)
	if random == nil {
		random = rand.Reader
	}
	public, private, e := ed25519.GenerateKey(random)
	if e != nil {
		return RotationV1{}, ErrInvalid
	}
	defer clear(private)
	transcript, e := attachedworker.RotationProofTranscript(worker, worker.Revision, public)
	if e != nil {
		return RotationV1{}, ErrInvalid
	}
	r := RotationV1{Version: 1, Worker: worker, NewPublicKey: public, CurrentProof: ed25519.Sign(secret.IdentityPrivateKey, transcript), NewProof: ed25519.Sign(private, transcript)}
	p = rotationPending{Version: 1, Base: snapshot.Manifest, Rotation: rotationDisk(r), PrivateKey: private}
	if !validRotationPending(p) {
		return RotationV1{}, ErrInvalid
	}
	if e = WritePrivate(pendingPath, p); e != nil {
		return RotationV1{}, e
	}
	return r, nil
}
func CompleteRotation(ctx context.Context, store *attachedworkerlocal.Store, pendingPath string, receipt ReceiptV1) error {
	if store == nil || receipt.Validate() != nil {
		return ErrInvalid
	}
	unlock, e := lockPending(ctx, pendingPath)
	if e != nil {
		return e
	}
	defer unlock()
	var p rotationPending
	if e = ReadPrivate(pendingPath, &p); e != nil {
		return e
	}
	defer clear(p.PrivateKey)
	if !validRotationPending(p) {
		return ErrInvalid
	}
	old := p.Rotation.Worker
	w := receipt.Worker
	expected := old
	expected.IdentityPublicKey = p.Rotation.NewPublicKey
	expected.EnrollmentGeneration++
	expected.Revision++
	expected.UpdatedAt = w.UpdatedAt
	if !reflect.DeepEqual(w, expected) || !w.UpdatedAt.After(old.UpdatedAt) {
		return ErrConflict
	}
	lease, e := store.AcquireRuntime(ctx)
	if e != nil {
		return e
	}
	defer lease.Close()
	snapshot, e := lease.LoadSnapshot(ctx)
	if e != nil {
		return e
	}
	next := p.Base
	next.Revision++
	next.EnrollmentGeneration = w.EnrollmentGeneration
	next.IdentityKeyFingerprint = string(domain.DigestAttachedWorkerIdentityKey(w.IdentityPublicKey))
	next.UpdatedAt = w.UpdatedAt
	if !next.UpdatedAt.After(p.Base.UpdatedAt) {
		// The installation timestamp is local lifecycle evidence, not server
		// authority. Owner-host and server clocks need not be synchronized.
		// Keep this transition deterministic across receipt replay.
		next.UpdatedAt = p.Base.UpdatedAt.Add(time.Microsecond)
	}
	secret := secretFor(next, p.PrivateKey)
	existing, e := lease.LoadSecret(ctx)
	if e != nil {
		return e
	}
	defer clear(existing.IdentityPrivateKey)
	secret.ConnectionSecret = append([]byte(nil), existing.ConnectionSecret...)
	defer clear(secret.IdentityPrivateKey)
	if reflect.DeepEqual(snapshot.Manifest, next) {
		if !reflect.DeepEqual(existing, secret) {
			return ErrConflict
		}
		return nil
	}
	if !reflect.DeepEqual(snapshot.Manifest, p.Base) {
		return ErrConflict
	}
	return lease.Update(ctx, p.Base.Revision, next, secret)
}
