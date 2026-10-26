// Package ownerlocalcredential persists the existing subscription credential
// lifecycle on one trusted owner's host. It does not publish server resources,
// prove entitlement, discover login files or enable any serving profile.
package ownerlocalcredential

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var (
	ErrDenied      = errors.New("owner-local credential operation denied")
	ErrBusy        = errors.New("owner-local credential owner is busy")
	ErrUnavailable = errors.New("owner-local credential state unavailable")
	ErrUnknown     = errors.New("owner-local credential durability is unknown")
)

const (
	maxAuthBytes      = 1 << 20
	maxHeaderBytes    = 4 << 10
	maxMetadataBytes  = 64 << 10
	maxDiskBytes      = 12 << 20
	maxCandidates     = 4
	maxCleanups       = 4
	maxKeyUses        = uint64(1) << 32
	metadataName      = "state.v1"
	metadataTemporary = ".state.tmp"
	lockName          = ".owner.lock"
	maxEnvelopeBytes  = maxAuthBytes + maxHeaderBytes + 40
)

// Scope is explicit metadata, never inferred from a HOME, keychain or auth file.
type Scope struct {
	TenantID                 domain.TenantID                 `json:"tenant_id"`
	OwnerUserID              domain.UserID                   `json:"owner_user_id"`
	SubscriptionConnectionID domain.SubscriptionConnectionID `json:"subscription_connection_id"`
}

func (v Scope) valid() bool {
	return v.TenantID.Validate() == nil && v.OwnerUserID.Validate() == nil && v.SubscriptionConnectionID.Validate() == nil
}

type Config struct {
	Root     string
	Scope    Scope
	KeyEpoch string
}

// Store owns one root flock until Close. The supplied key must be dedicated to
// this exact root/scope/epoch by its trusted operator. Local code cannot prove
// global key nonreuse or malicious same-UID snapshot rollback. Copy/restore is
// not initialization; unsupported restoration must remain disabled.
type Store struct{ *store }

type store struct {
	mu      sync.Mutex
	config  Config
	fs      *filesystem
	aead    cipher.AEAD
	macKey  []byte
	state   snapshot
	image   []byte
	pending []byte
	unknown bool
	closed  bool
	guard   atomic.Bool
	wrapper bool
}

func New(ctx context.Context, config Config, injectedKey []byte) (*Store, error) {
	return newStore(ctx, config, injectedKey, openFilesystem)
}

// The private opener seam permits faults on real test-owned filesystem FDs;
// public construction always uses the actual pinned filesystem implementation.
func newStore(ctx context.Context, config Config, injectedKey []byte, open func(string) (*filesystem, error)) (*Store, error) {
	if ctx == nil || ctx.Err() != nil || !config.Scope.valid() || !filepath.IsAbs(config.Root) || filepath.Clean(config.Root) != config.Root || config.Root == "/" || len(config.Root) > 4096 || domain.ValidateOpaqueID("key_epoch", config.KeyEpoch) != nil || len(injectedKey) != 32 || bytes.Equal(injectedKey, make([]byte, 32)) {
		return nil, ErrDenied
	}
	fs, err := open(config.Root)
	if err != nil {
		return nil, err
	}
	owned := false
	defer func() {
		if !owned {
			_ = fs.close()
		}
	}()
	identity := config.Root + "\x00" + string(config.Scope.TenantID) + "\x00" + string(config.Scope.OwnerUserID) + "\x00" + string(config.Scope.SubscriptionConnectionID) + "\x00" + config.KeyEpoch
	keys, err := hkdf.Key(sha256.New, injectedKey, nil, "sessionless-owner-local-credentials-v1\x00"+identity, 64)
	if err != nil {
		return nil, ErrDenied
	}
	defer clear(keys)
	block, err := aes.NewCipher(keys[:32])
	if err != nil {
		return nil, ErrDenied
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrDenied
	}
	s := &Store{store: &store{config: config, fs: fs, aead: aead, macKey: bytes.Clone(keys[32:])}}
	maximumHeader := s.header(candidateRecord{Reference: strings.Repeat("f", 64), Expected: math.MaxUint64 - 2, Fingerprint: domain.CredentialFingerprint(strings.Repeat("f", 64)), Size: maxAuthBytes})
	if _, err := headerEncodedSize(maximumHeader); err != nil {
		clear(s.macKey)
		return nil, err
	}
	s.state = snapshot{Version: 1, Scope: config.Scope, Root: config.Root, RootDevice: fs.device, RootInode: fs.inode, KeyEpoch: config.KeyEpoch, Revision: 1, Candidates: []candidateRecord{}, Cleanups: []cleanupRecord{}}
	raw, found, err := fs.read(ctx, metadataName, maxMetadataBytes)
	if err != nil {
		clear(s.macKey)
		return nil, err
	}
	if found {
		state, err := s.decodeSnapshot(raw)
		if err != nil {
			clear(raw)
			clear(s.macKey)
			return nil, err
		}
		s.state, s.image = state, raw
		// Only an authenticated unrenamed next image can be discarded. It
		// never became binding authority, and encryption cannot have followed
		// a reservation whose publication did not finish successfully.
		temporary, present, err := fs.read(ctx, metadataTemporary, maxMetadataBytes)
		if err != nil {
			clear(s.image)
			clear(s.macKey)
			return nil, err
		}
		if present {
			pending, err := s.decodeSnapshot(temporary)
			clear(temporary)
			if err != nil || s.state.Revision == math.MaxUint64 || pending.Revision != s.state.Revision+1 || fs.remove(metadataTemporary) != nil {
				clear(s.image)
				clear(s.macKey)
				return nil, ErrUnknown
			}
		}
	} else {
		temporary, present, err := fs.read(ctx, metadataTemporary, maxMetadataBytes)
		if err != nil {
			clear(s.macKey)
			return nil, err
		}
		if present {
			initial, err := s.decodeSnapshot(temporary)
			clear(temporary)
			entries, listErr := fs.entries()
			if err != nil || listErr != nil || len(entries) != 2 || initial.Revision != 1 || initial.KeyUses != 0 || initial.Binding != nil || len(initial.Candidates) != 0 || len(initial.Cleanups) != 0 || fs.remove(metadataTemporary) != nil {
				clear(s.macKey)
				return nil, ErrUnknown
			}
		}
		if err := fs.onlyLock(); err != nil {
			clear(s.macKey)
			return nil, err
		}
		raw, err = s.encodeSnapshot(s.state)
		if err == nil {
			err = fs.writeAtomic(ctx, raw)
		}
		if err != nil {
			clear(raw)
			clear(s.macKey)
			return nil, err
		}
		s.image = raw
	}
	if err := s.inventoryLocked(ctx); err != nil {
		clear(s.image)
		clear(s.macKey)
		return nil, err
	}
	// Visibility after a previous owner's ambiguous rename is not proof of
	// durability. Establish the exact validated namespace before admitting
	// any read or write, including reopen without an unrenamed temporary.
	if err := fs.syncDirectory(); err != nil {
		clear(s.image)
		clear(s.macKey)
		return nil, ErrUnknown
	}
	owned = true
	return s, nil
}

func (s *Store) readyLocked(ctx context.Context) error {
	if s == nil || s.store == nil || ctx == nil || ctx.Err() != nil || s.closed {
		return ErrDenied
	}
	if s.unknown {
		return ErrUnknown
	}
	if err := s.fs.check(); err != nil {
		return err
	}
	raw, found, err := s.fs.read(ctx, metadataName, maxMetadataBytes)
	if err != nil {
		return err
	}
	if !found {
		return ErrUnavailable
	}
	defer clear(raw)
	if !bytes.Equal(raw, s.image) {
		return ErrDenied
	}
	return s.inventoryLocked(ctx)
}

func (s *Store) saveLocked(ctx context.Context, next snapshot) error {
	if s.state.Revision == math.MaxUint64 {
		return ErrDenied
	}
	next.Revision = s.state.Revision + 1
	raw, err := s.encodeSnapshot(next)
	if err != nil {
		return err
	}
	err = s.fs.writeAtomic(ctx, raw)
	if err == ErrUnknown {
		s.unknown, s.pending = true, raw
		return err
	}
	if err != nil {
		clear(raw)
		return err
	}
	clear(s.image)
	s.state, s.image = next, raw
	return nil
}

// Reconcile establishes durability of the exact attempted local image and
// drains only original bounded cleanup intents with definitive Release proof.
// It never reconstructs an invocation, publishes a new generation or retries a
// provider effect. Seeing an image alone is insufficient: directory sync must
// succeed. A different image remains unknown; there is no rollback fallback.
func (s *Store) Reconcile(ctx context.Context) error {
	if s == nil || s.store == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	if !s.unknown {
		if err := s.readyLocked(ctx); err != nil {
			return err
		}
		return s.drainLocked(ctx)
	}
	if err := s.fs.check(); err != nil {
		return err
	}
	raw, found, err := s.fs.read(ctx, metadataName, maxMetadataBytes)
	if err != nil || !found {
		return ErrUnknown
	}
	defer clear(raw)
	if !bytes.Equal(raw, s.pending) && !bytes.Equal(raw, s.image) {
		return ErrUnknown
	}
	state, err := s.decodeSnapshot(raw)
	if err != nil {
		return ErrUnknown
	}
	temporary, present, err := s.fs.read(ctx, metadataTemporary, maxMetadataBytes)
	if err != nil {
		return ErrUnknown
	}
	if present {
		valid := bytes.Equal(temporary, s.pending)
		clear(temporary)
		if !valid || s.fs.remove(metadataTemporary) != nil {
			return ErrUnknown
		}
	}
	if s.fs.syncDirectory() != nil {
		return ErrUnknown
	}
	if err := s.inventoryFor(ctx, state); err != nil {
		return ErrUnknown
	}
	clear(s.image)
	s.state, s.image = state, bytes.Clone(raw)
	clear(s.pending)
	s.pending, s.unknown = nil, false
	return s.drainLocked(ctx)
}

func (s *Store) Close() error {
	if s == nil || s.store == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.guard.Load() || s.wrapper {
		return ErrBusy
	}
	if s.unknown {
		return ErrUnknown
	}
	if err := s.fs.close(); err != nil {
		return err
	}
	s.closed = true
	clear(s.image)
	clear(s.pending)
	clear(s.macKey)
	s.aead = nil
	return nil
}

func (s *Store) scopeFor(expected uint64) ports.CredentialCandidateScope {
	return ports.CredentialCandidateScope{TenantID: s.config.Scope.TenantID, OwnerUserID: s.config.Scope.OwnerUserID, SubscriptionConnectionID: s.config.Scope.SubscriptionConnectionID, ExpectedGeneration: expected}
}

func (s *Store) scopeMatches(v ports.CredentialCandidateScope) bool {
	return s.scopeFor(v.ExpectedGeneration) == v
}

func (s *Store) bindingMatches(v domain.CredentialBinding) bool {
	return v.Validate() == nil && v.TenantID == s.config.Scope.TenantID && v.OwnerUserID == s.config.Scope.OwnerUserID && v.SubscriptionConnectionID == s.config.Scope.SubscriptionConnectionID && (v.Revoked || v.Generation < math.MaxUint64)
}

func (s *Store) LoadCredentialBinding(ctx context.Context, tenant domain.TenantID, connection domain.SubscriptionConnectionID) (domain.CredentialBinding, bool, error) {
	if s == nil || s.store == nil {
		return domain.CredentialBinding{}, false, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return domain.CredentialBinding{}, false, err
	}
	if tenant != s.config.Scope.TenantID || connection != s.config.Scope.SubscriptionConnectionID {
		return domain.CredentialBinding{}, false, ErrDenied
	}
	if s.state.Binding == nil {
		return domain.CredentialBinding{}, false, nil
	}
	return s.state.Binding.Value, true, nil
}

// Registration is trusted explicit operator input, not proof of provider
// entitlement or server resource eligibility. Initialization never adopts an
// ambient file, overwrites a binding, or upgrades an already sealed job.
type Registration struct {
	Provider    string
	AuthMode    string
	Entitlement domain.EntitlementState
	At          time.Time
}

func (s *Store) Initialize(ctx context.Context, registration Registration, plaintext []byte) error {
	if s == nil || s.store == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return err
	}
	if s.state.Binding != nil || registration.Provider != "openai" || registration.AuthMode != "chatgpt" || registration.Entitlement != domain.EntitlementActive || registration.At.IsZero() {
		return ErrDenied
	}
	candidate, err := s.putLocked(ctx, s.scopeFor(0), plaintext)
	if err != nil {
		return err
	}
	next := cloneSnapshot(s.state)
	binding := domain.CredentialBinding{Version: 1, TenantID: s.config.Scope.TenantID, OwnerUserID: s.config.Scope.OwnerUserID, SubscriptionConnectionID: s.config.Scope.SubscriptionConnectionID, Provider: registration.Provider, AuthMode: registration.AuthMode, Entitlement: registration.Entitlement, SecretRef: candidate.Reference, SecretFingerprint: candidate.Fingerprint, Generation: 1, UpdatedAt: registration.At.UTC()}
	next.Binding = &bindingRecord{Value: binding, Reference: binding.SecretRef.StorageValue()}
	if err := s.saveLocked(ctx, next); err != nil {
		return err
	}
	return s.commitLocked(ctx, candidate)
}

func cloneSnapshot(v snapshot) snapshot {
	v.Candidates = append([]candidateRecord{}, v.Candidates...)
	v.Cleanups = append([]cleanupRecord{}, v.Cleanups...)
	if v.Binding != nil {
		copy := *v.Binding
		v.Binding = &copy
	}
	return v
}

var _ ports.CredentialBindingStore = (*Store)(nil)
var _ ports.CredentialSecretStore = (*Store)(nil)
