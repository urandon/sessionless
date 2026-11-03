package s3store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var errProviderCredentialObject = errors.New("provider credential object store unavailable")

type ProviderCredentialBindingReader interface {
	LoadProviderCredential(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error)
}

type ProviderCredentialObjectConfig struct {
	KeyEpoch       string
	Clock          ports.Clock
	MaxSecretBytes int64
}

// ProviderCredentialObjectStore stores one immutable ciphertext before the
// binding CAS. YDB alone decides which object is authoritative; no object
// operation creates another binding, and Commit/Recover never write bytes.
type ProviderCredentialObjectStore struct {
	objects  *Store
	bindings ProviderCredentialBindingReader
	config   ProviderCredentialObjectConfig
	keyState *providerCredentialKeyState
}

// Keep keys and synchronization behind an opaque pointer: copying the public
// adapter value must neither copy a mutex nor expose key bytes through fmt.
type providerCredentialKeyState struct {
	mu       sync.Mutex
	root     []byte
	active   int
	closing  bool
	joined   chan struct{}
	random   io.Reader
	randomMu sync.Mutex
}

var _ ports.ProviderCredentialSecretStore = (*ProviderCredentialObjectStore)(nil)

// Value receivers redact copies and container elements too. Synchronization and
// keys remain in the shared private state; these receivers copy no mutex.
func (ProviderCredentialObjectStore) String() string {
	return "[redacted provider credential object store]"
}
func (ProviderCredentialObjectStore) GoString() string {
	return "[redacted provider credential object store]"
}
func (ProviderCredentialObjectStore) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, "[redacted provider credential object store]")
}

// NewProviderCredentialSecretStore performs no network/key discovery. It owns
// its independent key copy; the caller remains responsible for its input bytes.
func NewProviderCredentialSecretStore(objects *Store, config ProviderCredentialObjectConfig, bindings ProviderCredentialBindingReader, injectedKey []byte) (*ProviderCredentialObjectStore, error) {
	if config.MaxSecretBytes == 0 {
		config.MaxSecretBytes = 8 << 10
	}
	if objects == nil || objects.bucket == "" || config.Clock == nil || bindings == nil || !validProviderEpoch(config.KeyEpoch) || len(injectedKey) != 32 || bytes.Equal(injectedKey, make([]byte, 32)) || config.MaxSecretBytes < 1 || config.MaxSecretBytes > 64<<10 {
		return nil, errProviderCredentialObject
	}
	if objects.iamClient != nil {
		c := objects.iamClient
		if c.endpoint == nil || c.endpoint.Scheme != "https" || c.endpoint.Host == "" || c.endpoint.User != nil || c.endpoint.RawQuery != "" || c.endpoint.Fragment != "" || c.tokens == nil || c.http == nil {
			return nil, errProviderCredentialObject
		}
	} else {
		client, ok := objects.s3Client.(*s3.Client)
		if !ok {
			return nil, errProviderCredentialObject
		}
		o := client.Options()
		if o.BaseEndpoint != nil {
			u, err := url.Parse(*o.BaseEndpoint)
			if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
				return nil, errProviderCredentialObject
			}
		}
		_, finish, err := exactAbsenceHTTPClient(o.HTTPClient)
		if err != nil {
			return nil, errProviderCredentialObject
		}
		finish()
	}
	owned := *objects
	if objects.iamClient != nil {
		client := *objects.iamClient
		endpoint, httpClient := *client.endpoint, *client.http
		client.endpoint, client.http = &endpoint, &httpClient
		owned.iamClient = &client
	}
	return &ProviderCredentialObjectStore{objects: &owned, config: config, bindings: bindings, keyState: &providerCredentialKeyState{root: append([]byte(nil), injectedKey...), joined: make(chan struct{}), random: rand.Reader}}, nil
}

func finiteProviderContext(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	_, ok := ctx.Deadline()
	return ok
}
func (s *ProviderCredentialObjectStore) begin(ctx context.Context) error {
	if s == nil || s.keyState == nil || !finiteProviderContext(ctx) {
		return errProviderCredentialObject
	}
	s.keyState.mu.Lock()
	defer s.keyState.mu.Unlock()
	if s.keyState.closing {
		return errProviderCredentialObject
	}
	s.keyState.active++
	return nil
}
func (s *ProviderCredentialObjectStore) end() {
	s.keyState.mu.Lock()
	defer s.keyState.mu.Unlock()
	s.keyState.active--
	if s.keyState.closing && s.keyState.active == 0 {
		close(s.keyState.joined)
	}
}

// Close is irreversible admission shutdown. Timeout retains key ownership for
// existing operations; a later Close can join them but cannot renew their work.
func (s *ProviderCredentialObjectStore) Close(ctx context.Context) error {
	if s == nil || s.keyState == nil || !finiteProviderContext(ctx) {
		return errProviderCredentialObject
	}
	s.keyState.mu.Lock()
	if !s.keyState.closing {
		s.keyState.closing = true
		if s.keyState.active == 0 {
			close(s.keyState.joined)
		}
	}
	joined := s.keyState.joined
	s.keyState.mu.Unlock()
	select {
	case <-joined:
	default:
		select {
		case <-joined:
		case <-ctx.Done():
			return errProviderCredentialObject
		}
	}
	s.keyState.mu.Lock()
	clear(s.keyState.root)
	s.keyState.root = nil
	s.keyState.mu.Unlock()
	return nil
}

func providerBindingLocator(b domain.ProviderCredentialBindingV1) ports.ProviderCredentialLocatorV1 {
	return ports.ProviderCredentialLocatorV1{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, ResourceKind: b.ResourceKind, ResourceID: b.ResourceID}
}
func providerCandidateMatches(b domain.ProviderCredentialBindingV1, c ports.ProviderCredentialSecretCandidateV1) bool {
	return b.State == domain.ProviderCredentialActiveV1 && providerBindingLocator(b) == c.Scope.Locator && b.ResourceRevision == c.Scope.ResourceRevision && b.CredentialGeneration == c.Scope.CredentialGeneration && b.CandidateMutationID == c.Scope.MutationID && b.SecretRef == c.Reference && b.SecretFingerprint == c.Fingerprint
}

func (s *ProviderCredentialObjectStore) key(locator ports.ProviderCredentialLocatorV1, ref domain.CredentialSecretRef) (string, error) {
	r, err := parseProviderRef(ref)
	if err != nil || r.epoch != s.config.KeyEpoch {
		return "", errProviderCredentialObject
	}
	return providerObjectKey(locator, ref)
}
func (s *ProviderCredentialObjectStore) current(ctx context.Context, l ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	b, found, err := s.bindings.LoadProviderCredential(ctx, l)
	if err != nil || ctx.Err() != nil || found && (b.Validate() != nil || providerBindingLocator(b) != l) {
		return domain.ProviderCredentialBindingV1{}, false, errProviderCredentialObject
	}
	return b, found, nil
}
func (s *ProviderCredentialObjectStore) inspect(ctx context.Context, key string) (ports.ProviderCredentialSecretCandidateV1, []byte, bool, error) {
	wire, found, err := s.get(ctx, key)
	if err != nil || !found {
		return ports.ProviderCredentialSecretCandidateV1{}, nil, found, err
	}
	c, a, _, err := authenticateProviderObject(s.keyState.root, s.config.KeyEpoch, key, wire)
	clear(a)
	if err != nil {
		return c, nil, false, err
	}
	return c, wire, true, nil
}

func (s *ProviderCredentialObjectStore) PutProviderCredentialCandidate(ctx context.Context, scope ports.ProviderCredentialCandidateScopeV1, secret []byte) (ports.ProviderCredentialSecretCandidateV1, error) {
	var empty ports.ProviderCredentialSecretCandidateV1
	if err := s.begin(ctx); err != nil {
		return empty, err
	}
	defer s.end()
	if !validProviderScope(scope) || len(secret) == 0 || int64(len(secret)) > s.config.MaxSecretBytes {
		return empty, errProviderCredentialObject
	}
	at := s.config.Clock.Now().UTC().Truncate(time.Microsecond)
	if at.Year() < 1970 || at.Year() > 9999 {
		return empty, errProviderCredentialObject
	}
	id := make([]byte, 32)
	s.keyState.randomMu.Lock()
	_, err := io.ReadFull(s.keyState.random, id)
	s.keyState.randomMu.Unlock()
	if err != nil {
		return empty, errProviderCredentialObject
	}
	ref, err := domain.NewCredentialSecretRef(fmt.Sprintf("pcs1.%s.%020d.%s.%s", s.config.KeyEpoch, at.UnixMicro(), providerDigest(scope), hex.EncodeToString(id)))
	if err != nil {
		return empty, errProviderCredentialObject
	}
	c := ports.ProviderCredentialSecretCandidateV1{Scope: scope, Reference: ref, Fingerprint: domain.FingerprintCredential(secret), CreatedAt: at}
	s.keyState.randomMu.Lock()
	wire, err := sealProviderObject(s.keyState.root, c, s.config.KeyEpoch, secret, s.keyState.random)
	s.keyState.randomMu.Unlock()
	if err != nil {
		return empty, err
	}
	key, err := s.key(scope.Locator, ref)
	if err != nil {
		return empty, err
	}
	if err := s.put(ctx, key, wire); err != nil {
		return empty, err
	}
	observed, found, err := s.get(ctx, key)
	if err != nil || !found || !bytes.Equal(wire, observed) {
		return empty, errProviderCredentialObject
	}
	verified, a, _, err := authenticateProviderObject(s.keyState.root, s.config.KeyEpoch, key, observed)
	clear(a)
	if err != nil || verified != c {
		return empty, errProviderCredentialObject
	}
	return c, nil
}

func (s *ProviderCredentialObjectStore) CommitProviderCredentialCandidate(ctx context.Context, c ports.ProviderCredentialSecretCandidateV1) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.end()
	if !validProviderScope(c.Scope) {
		return errProviderCredentialObject
	}
	b, found, err := s.current(ctx, c.Scope.Locator)
	if err != nil || !found || !providerCandidateMatches(b, c) {
		return errProviderCredentialObject
	}
	key, err := s.key(c.Scope.Locator, c.Reference)
	if err != nil {
		return err
	}
	observed, _, found, err := s.inspect(ctx, key)
	if err != nil || !found || observed != c {
		return errProviderCredentialObject
	}
	return nil
}

func (s *ProviderCredentialObjectStore) RecoverProviderCredentialCandidate(ctx context.Context, b domain.ProviderCredentialBindingV1) (bool, error) {
	if err := s.begin(ctx); err != nil {
		return false, err
	}
	defer s.end()
	if b.Validate() != nil || b.State != domain.ProviderCredentialActiveV1 {
		return false, errProviderCredentialObject
	}
	key, err := s.key(providerBindingLocator(b), b.SecretRef)
	if err != nil {
		return false, err
	}
	c, _, found, err := s.inspect(ctx, key)
	if err != nil || !found {
		return false, err
	}
	if !providerCandidateMatches(b, c) {
		return false, errProviderCredentialObject
	}
	return true, nil
}

func (s *ProviderCredentialObjectStore) ReadProviderCredentialSecret(ctx context.Context, b domain.ProviderCredentialBindingV1, limit int64) ([]byte, error) {
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	defer s.end()
	if b.Validate() != nil || b.State != domain.ProviderCredentialActiveV1 || limit < 1 || limit > s.config.MaxSecretBytes {
		return nil, errProviderCredentialObject
	}
	key, err := s.key(providerBindingLocator(b), b.SecretRef)
	if err != nil {
		return nil, err
	}
	wire, found, err := s.get(ctx, key)
	if err != nil || !found {
		return nil, errProviderCredentialObject
	}
	c, secret, err := openProviderObject(s.keyState.root, s.config.KeyEpoch, key, wire)
	if err != nil || !providerCandidateMatches(b, c) || int64(len(secret)) > limit || ctx.Err() != nil {
		clear(secret)
		return nil, errProviderCredentialObject
	}
	return secret, nil
}

func (s *ProviderCredentialObjectStore) DeleteProviderCredentialCandidate(ctx context.Context, c ports.ProviderCredentialSecretCandidateV1) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.end()
	if !validProviderScope(c.Scope) {
		return errProviderCredentialObject
	}
	key, err := s.key(c.Scope.Locator, c.Reference)
	if err != nil {
		return err
	}
	observed, _, found, err := s.inspect(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if observed != c {
		return errProviderCredentialObject
	}
	return s.delete(ctx, key)
}
func (s *ProviderCredentialObjectStore) DeleteProviderCredentialSecret(ctx context.Context, c ports.ProviderCredentialCleanupV1) error {
	if err := s.begin(ctx); err != nil {
		return err
	}
	defer s.end()
	if c.Locator.Validate() != nil || c.CredentialGeneration == 0 {
		return errProviderCredentialObject
	}
	key, err := s.key(c.Locator, c.Reference)
	if err != nil {
		return err
	}
	observed, _, found, err := s.inspect(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	if observed.Scope.Locator != c.Locator || observed.Scope.CredentialGeneration != c.CredentialGeneration {
		return errProviderCredentialObject
	}
	return s.delete(ctx, key)
}

func (s *ProviderCredentialObjectStore) ListUncommittedProviderCredentialCandidates(ctx context.Context, l ports.ProviderCredentialLocatorV1, limit uint64) ([]ports.ProviderCredentialSecretCandidateV1, error) {
	if err := s.begin(ctx); err != nil {
		return nil, err
	}
	defer s.end()
	if l.Validate() != nil || limit == 0 || limit > 64 {
		return nil, errProviderCredentialObject
	}
	b, found, err := s.current(ctx, l)
	if err != nil {
		return nil, err
	}
	prefix := providerObjectRoot + providerDigest(l) + "/"
	after := ""
	scanned := uint64(0)
	result := make([]ports.ProviderCredentialSecretCandidateV1, 0, limit)
	for page := 0; page < 2; page++ {
		keys, more, err := s.list(ctx, prefix, after, limit+1-scanned)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			scanned++
			after = key
			c, _, exists, err := s.inspect(ctx, key)
			if err != nil || !exists || c.Scope.Locator != l {
				return nil, errProviderCredentialObject
			}
			if found && providerCandidateMatches(b, c) {
				continue
			}
			result = append(result, c)
			if uint64(len(result)) == limit {
				return result, nil
			}
		}
		if !more {
			return result, nil
		}
		if len(result) > 0 {
			return result, nil
		}
		if scanned >= limit+1 {
			return nil, errProviderCredentialObject
		}
	}
	return nil, errProviderCredentialObject
}

func providerCursor(c ports.ProviderCredentialSecretCandidateV1) ports.ProviderCredentialCandidateCursorV1 {
	l := c.Scope.Locator
	return ports.ProviderCredentialCandidateCursorV1{Present: true, CreatedAt: c.CreatedAt, TenantID: l.TenantID, OwnerUserID: l.OwnerUserID, ResourceKind: l.ResourceKind, ResourceID: l.ResourceID, ResourceRevision: c.Scope.ResourceRevision, CredentialGeneration: c.Scope.CredentialGeneration, MutationID: c.Scope.MutationID, Reference: c.Reference}
}
func providerCursorKey(c ports.ProviderCredentialCandidateCursorV1) (string, error) {
	scope := ports.ProviderCredentialCandidateScopeV1{Locator: ports.ProviderCredentialLocatorV1{TenantID: c.TenantID, OwnerUserID: c.OwnerUserID, ResourceKind: c.ResourceKind, ResourceID: c.ResourceID}, ResourceRevision: c.ResourceRevision, CredentialGeneration: c.CredentialGeneration, MutationID: c.MutationID}
	r, err := parseProviderRef(c.Reference)
	if err != nil || !validProviderScope(scope) || c.CreatedAt.IsZero() || c.CreatedAt.Location() != time.UTC || c.CreatedAt.Nanosecond()%1000 != 0 || fmt.Sprintf("%020d", c.CreatedAt.UnixMicro()) != r.stamp || providerDigest(scope) != r.scope {
		return "", errProviderCredentialObject
	}
	return providerObjectKey(scope.Locator, c.Reference)
}
func (s *ProviderCredentialObjectStore) ListAbandonedProviderCredentialCandidates(ctx context.Context, before time.Time, cursor ports.ProviderCredentialCandidateCursorV1, limit uint64) (ports.ProviderCredentialCandidatePageV1, error) {
	var result ports.ProviderCredentialCandidatePageV1
	if err := s.begin(ctx); err != nil {
		return result, err
	}
	defer s.end()
	if before.IsZero() || limit == 0 || limit > 64 {
		return result, errProviderCredentialObject
	}
	after := ""
	if cursor.Present {
		var err error
		r, parseErr := parseProviderRef(cursor.Reference)
		if parseErr != nil || r.epoch != s.config.KeyEpoch {
			return result, errProviderCredentialObject
		}
		after, err = providerCursorKey(cursor)
		if err != nil {
			return result, err
		}
	} else if cursor != (ports.ProviderCredentialCandidateCursorV1{}) {
		return result, errProviderCredentialObject
	}
	keys, more, err := s.list(ctx, providerObjectRoot, after, limit)
	if err != nil {
		return result, err
	}
	for _, key := range keys {
		c, _, found, err := s.inspect(ctx, key)
		if err != nil || !found {
			return ports.ProviderCredentialCandidatePageV1{}, errProviderCredentialObject
		}
		result.NextCursor = providerCursor(c)
		if !c.CreatedAt.Before(before) {
			continue
		}
		b, found, err := s.current(ctx, c.Scope.Locator)
		if err != nil {
			return ports.ProviderCredentialCandidatePageV1{}, err
		}
		if found && providerCandidateMatches(b, c) {
			continue
		}
		result.Items = append(result.Items, c)
	}
	result.HasMore = more
	return result, nil
}
