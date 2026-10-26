package ownerlocalcredential

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func bodyName(reference string) string { return "body-" + reference + ".v1" }

func (s *Store) inventoryLocked(ctx context.Context) error { return s.inventoryFor(ctx, s.state) }
func (s *Store) inventoryFor(ctx context.Context, state snapshot) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrDenied
	}
	entries, err := s.fs.entries()
	if err != nil {
		return err
	}
	expected := map[string]bool{lockName: true, metadataName: true}
	if state.Binding != nil && !state.Binding.Value.Revoked {
		name := bodyName(state.Binding.Reference)
		expected[name] = true
		if size, ok := entries[name]; !ok || size == 0 {
			return ErrDenied
		}
	}
	for _, c := range state.Candidates {
		expected[bodyName(c.Reference)] = true
	}
	for _, c := range state.Cleanups {
		expected[bodyName(c.Reference)] = true
	}
	for name := range entries {
		if !expected[name] {
			return ErrDenied
		}
	}
	return nil
}

func (s *Store) PutCredentialCandidate(ctx context.Context, scope ports.CredentialCandidateScope, plaintext []byte) (ports.CredentialSecretCandidate, error) {
	if s == nil || s.store == nil {
		return ports.CredentialSecretCandidate{}, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	return s.putLocked(ctx, scope, plaintext)
}

func (s *Store) putLocked(ctx context.Context, scope ports.CredentialCandidateScope, plaintext []byte) (ports.CredentialSecretCandidate, error) {
	if !s.scopeMatches(scope) || len(plaintext) == 0 || len(plaintext) > maxAuthBytes || len(s.state.Candidates) >= maxCandidates || len(s.state.Cleanups) >= maxCleanups-1 || s.state.KeyUses >= maxKeyUses-1 || scope.ExpectedGeneration >= math.MaxUint64-1 {
		return ports.CredentialSecretCandidate{}, ErrDenied
	}
	if s.state.Binding == nil {
		if scope.ExpectedGeneration != 0 {
			return ports.CredentialSecretCandidate{}, ErrDenied
		}
	} else if s.state.Binding.Value.Revoked || scope.ExpectedGeneration != s.state.Binding.Value.Generation {
		return ports.CredentialSecretCandidate{}, ErrDenied
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return ports.CredentialSecretCandidate{}, ErrUnavailable
	}
	c := candidateRecord{Reference: hex.EncodeToString(random[:]), Expected: scope.ExpectedGeneration, Fingerprint: domain.FingerprintCredential(plaintext), Size: uint64(len(plaintext))}
	headerSize, err := headerEncodedSize(s.header(c))
	if err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	if err := s.fs.space(uint64(12 + headerSize + s.aead.NonceSize() + len(plaintext) + s.aead.Overhead() + 2*maxMetadataBytes)); err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	for _, old := range s.state.Candidates {
		if old.Reference == c.Reference {
			return ports.CredentialSecretCandidate{}, ErrDenied
		}
	}
	if _, found, err := s.fs.read(ctx, bodyName(c.Reference), maxEnvelopeBytes); err != nil || found {
		return ports.CredentialSecretCandidate{}, ErrDenied
	}
	next := cloneSnapshot(s.state)
	next.KeyUses++
	next.Candidates = append(next.Candidates, c)
	if err := s.saveLocked(ctx, next); err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	sealed, err := s.seal(c, plaintext)
	if err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	defer clear(sealed)
	if err := s.fs.create(ctx, bodyName(c.Reference), sealed); err != nil {
		if err == ErrUnknown {
			s.unknown = true
			s.pending = append([]byte(nil), s.image...)
		}
		return ports.CredentialSecretCandidate{}, err
	}
	next = cloneSnapshot(s.state)
	next.Candidates[len(next.Candidates)-1].Ready = true
	if err := s.saveLocked(ctx, next); err != nil {
		return ports.CredentialSecretCandidate{}, err
	}
	return candidatePort(s, c), nil
}

func (s *Store) CompareAndSwapCredentialBinding(ctx context.Context, expected uint64, next domain.CredentialBinding) (bool, error) {
	if s == nil || s.store == nil {
		return false, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return false, err
	}
	if s.state.Binding == nil || s.state.Binding.Value.Revoked || s.state.Binding.Value.Generation != expected {
		return false, nil
	}
	current := s.state.Binding.Value
	if expected >= math.MaxUint64-1 || !s.bindingMatches(next) || next.Revoked || next.Generation != expected+1 || next.Provider != current.Provider || next.AuthMode != current.AuthMode || next.Entitlement != current.Entitlement || next.UpdatedAt.Before(current.UpdatedAt) || next.SecretRef == current.SecretRef || len(s.state.Cleanups) >= maxCleanups-1 {
		return false, ErrDenied
	}
	match := false
	for _, c := range s.state.Candidates {
		if c.Ready && c.Expected == expected && c.Reference == next.SecretRef.StorageValue() && c.Fingerprint == next.SecretFingerprint {
			match = true
		}
	}
	if !match {
		return false, ErrDenied
	}
	state := cloneSnapshot(s.state)
	state.Cleanups = append(state.Cleanups, cleanupRecord{Reference: current.SecretRef.StorageValue(), Generation: current.Generation})
	state.Binding = &bindingRecord{Value: next, Reference: next.SecretRef.StorageValue()}
	if err := s.saveLocked(ctx, state); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) RevokeCredentialBinding(ctx context.Context, request ports.CredentialRevokeRequest, at time.Time) (ports.CredentialRevocationResult, error) {
	if s == nil || s.store == nil {
		return ports.CredentialRevocationResult{}, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return ports.CredentialRevocationResult{}, err
	}
	if request.TenantID != s.config.Scope.TenantID || request.OwnerUserID != s.config.Scope.OwnerUserID || request.SubscriptionConnectionID != s.config.Scope.SubscriptionConnectionID || at.IsZero() || s.state.Binding == nil {
		return ports.CredentialRevocationResult{}, ErrDenied
	}
	current := s.state.Binding.Value
	if current.Revoked {
		result := ports.CredentialRevocationResult{Binding: current}
		// Retry preserves the original revoke's still-pending cleanup owner;
		// older rotation intents are not authority for this revoke. Returning
		// an empty relation would let the original lifecycle skip a failed
		// deletion and incorrectly report cleanup as complete.
		for _, cleanup := range s.state.Cleanups {
			if cleanup.Generation == current.Generation-1 {
				ref, err := domain.NewCredentialSecretRef(cleanup.Reference)
				if err != nil {
					return ports.CredentialRevocationResult{}, ErrDenied
				}
				result.SupersededSecretRef = ref
				result.SupersededGeneration = cleanup.Generation
				break
			}
		}
		return result, nil
	}
	if current.Generation == math.MaxUint64 || len(s.state.Cleanups) >= maxCleanups || at.Before(current.UpdatedAt) {
		return ports.CredentialRevocationResult{}, ErrDenied
	}
	state := cloneSnapshot(s.state)
	// A published-but-uncommitted active candidate belongs to this same
	// revoked cleanup relation, not a second conflicting owner of its body.
	for i := len(state.Candidates) - 1; i >= 0; i-- {
		if state.Candidates[i].Reference == current.SecretRef.StorageValue() {
			state.Candidates = append(state.Candidates[:i], state.Candidates[i+1:]...)
		}
	}
	state.Cleanups = append(state.Cleanups, cleanupRecord{Reference: current.SecretRef.StorageValue(), Generation: current.Generation})
	next := current
	next.Generation++
	next.Revoked = true
	next.Entitlement = domain.EntitlementDisconnected
	next.SecretRef = domain.CredentialSecretRef{}
	next.SecretFingerprint = ""
	next.UpdatedAt = at.UTC()
	state.Binding = &bindingRecord{Value: next}
	if err := s.saveLocked(ctx, state); err != nil {
		return ports.CredentialRevocationResult{}, err
	}
	return ports.CredentialRevocationResult{Binding: next, SupersededSecretRef: current.SecretRef, SupersededGeneration: current.Generation}, nil
}

func (s *Store) ReadCredentialSecret(ctx context.Context, binding domain.CredentialBinding, limit int64) ([]byte, error) {
	if s == nil || s.store == nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return nil, err
	}
	if limit < 1 || limit > maxAuthBytes || s.state.Binding == nil || s.state.Binding.Value != binding || !s.bindingMatches(binding) || binding.Revoked {
		return nil, ErrDenied
	}
	return s.readCurrentLocked(ctx, binding, limit)
}

func (s *Store) readCurrentLocked(ctx context.Context, binding domain.CredentialBinding, limit int64) ([]byte, error) {
	raw, found, err := s.fs.read(ctx, bodyName(binding.SecretRef.StorageValue()), maxEnvelopeBytes)
	if err != nil || !found {
		return nil, ErrDenied
	}
	defer clear(raw)
	// Decode the bounded authenticated header to learn length, then join it
	// to exact current identity before prospective plaintext allocation.
	header, err := s.envelopeRecord(raw)
	if err != nil || header.Expected != binding.Generation-1 || header.Reference != binding.SecretRef.StorageValue() || header.Fingerprint != binding.SecretFingerprint || header.Size > uint64(limit) {
		return nil, ErrDenied
	}
	return s.openEnvelope(raw, header)
}

func (s *Store) CommitCredentialCandidate(ctx context.Context, candidate ports.CredentialSecretCandidate) error {
	if s == nil || s.store == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return err
	}
	return s.commitLocked(ctx, candidate)
}

func (s *Store) commitLocked(ctx context.Context, candidate ports.CredentialSecretCandidate) error {
	if !s.scopeMatches(candidate.Scope) || s.state.Binding == nil || s.state.Binding.Value.Revoked || s.state.Binding.Reference != candidate.Reference.StorageValue() || s.state.Binding.Value.SecretFingerprint != candidate.Fingerprint || candidate.Scope.ExpectedGeneration == math.MaxUint64 || s.state.Binding.Value.Generation != candidate.Scope.ExpectedGeneration+1 {
		return ErrDenied
	}
	plain, err := s.readCurrentLocked(ctx, s.state.Binding.Value, maxAuthBytes)
	if err != nil {
		return err
	}
	clear(plain)
	state := cloneSnapshot(s.state)
	for i, c := range state.Candidates {
		if candidatePort(s, c) == candidate {
			if !c.Ready {
				return ErrDenied
			}
			state.Candidates = append(state.Candidates[:i], state.Candidates[i+1:]...)
			return s.saveLocked(ctx, state)
		}
	}
	return nil // Exact current relation already committed.
}

func (s *Store) RecoverCredentialCandidate(ctx context.Context, binding domain.CredentialBinding) (bool, error) {
	if s == nil || s.store == nil {
		return false, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return false, err
	}
	if s.state.Binding == nil || s.state.Binding.Value != binding || binding.Revoked {
		return false, ErrDenied
	}
	for _, c := range s.state.Candidates {
		if c.Reference == binding.SecretRef.StorageValue() {
			return true, s.commitLocked(ctx, candidatePort(s, c))
		}
	}
	plain, err := s.readCurrentLocked(ctx, binding, maxAuthBytes)
	clear(plain)
	return false, err
}

func (s *Store) ListUncommittedCredentialCandidates(ctx context.Context, scope ports.CredentialCandidateScope, limit uint64) ([]ports.CredentialSecretCandidate, error) {
	if s == nil || s.store == nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return nil, err
	}
	if !s.scopeMatches(scope) || limit == 0 || limit > maxCandidates {
		return nil, ErrDenied
	}
	result := make([]ports.CredentialSecretCandidate, 0, int(limit))
	for _, c := range s.state.Candidates {
		if c.Expected == scope.ExpectedGeneration && uint64(len(result)) < limit {
			result = append(result, candidatePort(s, c))
		}
	}
	return result, nil
}

func (s *Store) DeleteCredentialCandidate(ctx context.Context, candidate ports.CredentialSecretCandidate) error {
	if s == nil || s.store == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return err
	}
	if !s.scopeMatches(candidate.Scope) || !validReference(candidate.Reference.StorageValue()) || s.state.Binding != nil && s.state.Binding.Reference == candidate.Reference.StorageValue() {
		return ErrDenied
	}
	state := cloneSnapshot(s.state)
	for i, c := range state.Candidates {
		if candidatePort(s, c) == candidate {
			if err := s.removeLocked(bodyName(c.Reference)); err != nil {
				return err
			}
			state.Candidates = append(state.Candidates[:i], state.Candidates[i+1:]...)
			return s.saveLocked(ctx, state)
		}
	}
	return ErrDenied
}

func (s *Store) DeleteCredentialSecret(ctx context.Context, scope ports.CredentialCandidateScope, ref domain.CredentialSecretRef) error {
	if s == nil || s.store == nil {
		return ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return err
	}
	if !s.scopeMatches(scope) || !validReference(ref.StorageValue()) || s.state.Binding != nil && s.state.Binding.Reference == ref.StorageValue() {
		return ErrDenied
	}
	state := cloneSnapshot(s.state)
	for i, c := range state.Cleanups {
		if c.Reference == ref.StorageValue() && c.Generation == scope.ExpectedGeneration {
			if err := s.removeLocked(bodyName(c.Reference)); err != nil {
				return err
			}
			state.Cleanups = append(state.Cleanups[:i], state.Cleanups[i+1:]...)
			return s.saveLocked(ctx, state)
		}
	}
	return ErrDenied
}

func (s *Store) removeLocked(name string) error {
	err := s.fs.remove(name)
	if err == ErrUnknown {
		s.unknown = true
		s.pending = append([]byte(nil), s.image...)
	}
	return err
}

// Only the wrapper of the exact original lifecycle calls released, after its
// registered auth-file cleanup succeeds. Absence of an in-memory handle after
// restart is deliberately NOT evidence that an old materialization was erased.
func (s *Store) released(ctx context.Context, handle ports.CredentialHandle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.readyLocked(ctx); err != nil {
		return err
	}
	if handle.Validate() != nil || handle.TenantID != s.config.Scope.TenantID || handle.OwnerUserID != s.config.Scope.OwnerUserID || handle.SubscriptionConnectionID != s.config.Scope.SubscriptionConnectionID || handle.BindingGeneration == math.MaxUint64 || !s.guard.Load() {
		return ErrDenied
	}
	state := cloneSnapshot(s.state)
	changed := false
	for i, c := range state.Cleanups {
		if (c.Generation == handle.BindingGeneration || c.Generation == handle.BindingGeneration+1) && !c.Released {
			state.Cleanups[i].Released = true
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return s.saveLocked(ctx, state)
}

func (s *Store) drainLocked(ctx context.Context) error {
	// The original bounded intent inventory is the only drain source. Never
	// infer release from an absent handle, generation age or a directory scan.
	for count := 0; count < maxCleanups; count++ {
		index := -1
		for i, c := range s.state.Cleanups {
			if c.Released {
				index = i
				break
			}
		}
		if index < 0 {
			return nil
		}
		c := s.state.Cleanups[index]
		if s.state.Binding != nil && s.state.Binding.Reference == c.Reference {
			return ErrDenied
		}
		if err := s.removeLocked(bodyName(c.Reference)); err != nil {
			return err
		}
		state := cloneSnapshot(s.state)
		state.Cleanups = append(state.Cleanups[:index], state.Cleanups[index+1:]...)
		if err := s.saveLocked(ctx, state); err != nil {
			return err
		}
	}
	return nil
}
