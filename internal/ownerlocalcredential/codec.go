package ownerlocalcredential

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

type bindingRecord struct {
	Value     domain.CredentialBinding `json:"binding"`
	Reference string                   `json:"secret_ref"`
}
type candidateRecord struct {
	Reference   string                       `json:"reference"`
	Expected    uint64                       `json:"expected_generation"`
	Fingerprint domain.CredentialFingerprint `json:"fingerprint"`
	Size        uint64                       `json:"plaintext_bytes"`
	Ready       bool                         `json:"ready"`
}
type cleanupRecord struct {
	Reference  string `json:"reference"`
	Generation uint64 `json:"generation"`
	Released   bool   `json:"materialization_released"`
}
type snapshot struct {
	Version    uint32            `json:"version"`
	Scope      Scope             `json:"scope"`
	Root       string            `json:"root"`
	RootDevice uint64            `json:"root_device"`
	RootInode  uint64            `json:"root_inode"`
	KeyEpoch   string            `json:"key_epoch"`
	Revision   uint64            `json:"revision"`
	KeyUses    uint64            `json:"key_uses"`
	Binding    *bindingRecord    `json:"binding"`
	Candidates []candidateRecord `json:"candidates"`
	Cleanups   []cleanupRecord   `json:"cleanups"`
}

func validReference(v string) bool {
	decoded, err := hex.DecodeString(v)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == v
}

func (s *Store) validateSnapshot(v *snapshot) error {
	if v.Version != 1 || v.Scope != s.config.Scope || v.Root != s.config.Root || v.RootDevice != s.fs.device || v.RootInode != s.fs.inode || v.KeyEpoch != s.config.KeyEpoch || v.Revision == 0 || v.KeyUses >= maxKeyUses || v.Candidates == nil || v.Cleanups == nil || len(v.Candidates) > maxCandidates || len(v.Cleanups) > maxCleanups {
		return ErrDenied
	}
	if v.Binding != nil {
		if v.Revision == math.MaxUint64 && !v.Binding.Value.Revoked {
			return ErrDenied
		}
		b := v.Binding.Value
		if b.Revoked {
			if v.Binding.Reference != "" {
				return ErrDenied
			}
		} else {
			if !validReference(v.Binding.Reference) {
				return ErrDenied
			}
			ref, err := domain.NewCredentialSecretRef(v.Binding.Reference)
			if err != nil {
				return ErrDenied
			}
			b.SecretRef = ref
		}
		if !s.bindingMatches(b) || b.Provider != "openai" || b.AuthMode != "chatgpt" {
			return ErrDenied
		}
		v.Binding.Value = b
	}
	seen := make(map[string]bool, maxCandidates+maxCleanups)
	for _, c := range v.Candidates {
		if !validReference(c.Reference) || seen[c.Reference] || c.Expected >= math.MaxUint64-1 || c.Fingerprint.Validate() != nil || c.Size == 0 || c.Size > maxAuthBytes {
			return ErrDenied
		}
		seen[c.Reference] = true
	}
	for _, c := range v.Cleanups {
		if !validReference(c.Reference) || seen[c.Reference] || c.Generation == 0 || c.Generation == math.MaxUint64 || v.Binding != nil && v.Binding.Reference == c.Reference {
			return ErrDenied
		}
		seen[c.Reference] = true
	}
	if v.Binding != nil && !v.Binding.Value.Revoked && len(v.Cleanups) >= maxCleanups {
		return ErrDenied
	}
	return nil
}

func strictDecode(raw []byte, target any, limit int) error {
	if len(raw) == 0 || len(raw) > limit || strictjson.ValidateDocument(raw, uint64(limit), 16, 512) != nil || json.Unmarshal(raw, target) != nil {
		return ErrDenied
	}
	canonical, err := json.Marshal(target)
	defer clear(canonical)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ErrDenied
	}
	return nil
}

func (s *Store) encodeSnapshot(v snapshot) ([]byte, error) {
	if err := s.validateSnapshot(&v); err != nil {
		return nil, err
	}
	// Public CredentialBinding JSON intentionally omits SecretRef. Only this
	// private record separately persists its validated opaque StorageValue.
	raw, err := json.Marshal(v)
	if err != nil || len(raw)+40 > maxMetadataBytes {
		clear(raw)
		return nil, ErrDenied
	}
	defer clear(raw)
	if v.Binding != nil && !v.Binding.Value.Revoked {
		reserved := cloneSnapshot(v)
		b := reserved.Binding.Value
		reserved.Cleanups = append(reserved.Cleanups, cleanupRecord{Reference: reserved.Binding.Reference, Generation: b.Generation})
		b.Generation++
		b.Revoked = true
		b.Entitlement = domain.EntitlementDisconnected
		b.SecretRef = domain.CredentialSecretRef{}
		b.SecretFingerprint = ""
		reserved.Binding = &bindingRecord{Value: b}
		check, err := json.Marshal(reserved)
		defer clear(check)
		if err != nil || len(check)+40 > maxMetadataBytes {
			return nil, ErrDenied
		}
	}
	out := make([]byte, 8+len(raw)+32)
	copy(out, "OLCS\x00\x00\x00\x01")
	copy(out[8:], raw)
	mac := hmac.New(sha256.New, s.macKey)
	_, _ = mac.Write(out[:len(out)-32])
	copy(out[len(out)-32:], mac.Sum(nil))
	return out, nil
}

func (s *Store) decodeSnapshot(raw []byte) (snapshot, error) {
	var v snapshot
	if len(raw) < 40 || len(raw) > maxMetadataBytes || !bytes.Equal(raw[:8], []byte("OLCS\x00\x00\x00\x01")) {
		return v, ErrDenied
	}
	mac := hmac.New(sha256.New, s.macKey)
	_, _ = mac.Write(raw[:len(raw)-32])
	if !hmac.Equal(raw[len(raw)-32:], mac.Sum(nil)) || strictDecode(raw[8:len(raw)-32], &v, maxMetadataBytes) != nil || s.validateSnapshot(&v) != nil {
		return snapshot{}, ErrDenied
	}
	return v, nil
}

type envelopeHeader struct {
	Version     uint32                       `json:"version"`
	Scope       Scope                        `json:"scope"`
	Root        string                       `json:"root"`
	RootDevice  uint64                       `json:"root_device"`
	RootInode   uint64                       `json:"root_inode"`
	KeyEpoch    string                       `json:"key_epoch"`
	Reference   string                       `json:"reference"`
	Expected    uint64                       `json:"expected_generation"`
	Fingerprint domain.CredentialFingerprint `json:"fingerprint"`
	Size        uint64                       `json:"plaintext_bytes"`
}

func (s *Store) header(c candidateRecord) envelopeHeader {
	return envelopeHeader{Version: 1, Scope: s.config.Scope, Root: s.config.Root, RootDevice: s.fs.device, RootInode: s.fs.inode, KeyEpoch: s.config.KeyEpoch, Reference: c.Reference, Expected: c.Expected, Fingerprint: c.Fingerprint, Size: c.Size}
}

func (s *Store) seal(c candidateRecord, plaintext []byte) ([]byte, error) {
	h := s.header(c)
	prospective, err := headerEncodedSize(h)
	if err != nil {
		return nil, err
	}
	header, err := json.Marshal(h)
	if err != nil || len(header) != prospective || uint64(len(plaintext)) != c.Size || domain.FingerprintCredential(plaintext) != c.Fingerprint {
		return nil, ErrDenied
	}
	defer clear(header)
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	size := 12 + len(header) + len(nonce) + len(plaintext) + s.aead.Overhead()
	if size > maxEnvelopeBytes {
		return nil, ErrDenied
	}
	out := make([]byte, 12+len(header)+len(nonce), size)
	copy(out, "OLCB\x00\x00\x00\x01")
	binary.BigEndian.PutUint32(out[8:12], uint32(len(header)))
	copy(out[12:], header)
	copy(out[12+len(header):], nonce)
	return s.aead.Seal(out, nonce, plaintext, header), nil
}

// Count only this fixed header's variable strings before allocating its JSON.
// The empty-string schema encoding below is fixed-size, not a payload encoding.
func headerEncodedSize(v envelopeHeader) (int, error) {
	values := []string{string(v.Scope.TenantID), string(v.Scope.OwnerUserID), string(v.Scope.SubscriptionConnectionID), v.Root, v.KeyEpoch, v.Reference, string(v.Fingerprint)}
	v.Scope = Scope{}
	v.Root = ""
	v.KeyEpoch = ""
	v.Reference = ""
	v.Fingerprint = ""
	fixed, err := json.Marshal(v)
	if err != nil {
		return 0, ErrDenied
	}
	size := len(fixed)
	for _, value := range values {
		if !utf8.ValidString(value) {
			return 0, ErrDenied
		}
		for _, r := range value {
			switch r {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&', '\u2028', '\u2029':
				size += 6
			default:
				if r < 0x20 {
					size += 6
				} else {
					size += utf8.RuneLen(r)
				}
			}
			if size > maxHeaderBytes {
				return 0, ErrDenied
			}
		}
	}
	return size, nil
}

func (s *Store) openEnvelope(raw []byte, c candidateRecord) ([]byte, error) {
	if len(raw) < 12 || len(raw) > maxEnvelopeBytes || !bytes.Equal(raw[:8], []byte("OLCB\x00\x00\x00\x01")) {
		return nil, ErrDenied
	}
	n := int(binary.BigEndian.Uint32(raw[8:12]))
	if n < 1 || n > maxHeaderBytes || n > len(raw)-12-s.aead.NonceSize()-s.aead.Overhead() {
		return nil, ErrDenied
	}
	var header envelopeHeader
	if strictDecode(raw[12:12+n], &header, maxHeaderBytes) != nil || header != s.header(c) {
		return nil, ErrDenied
	}
	nonce := raw[12+n : 12+n+s.aead.NonceSize()]
	if len(raw)-12-n-len(nonce)-s.aead.Overhead() != int(c.Size) {
		return nil, ErrDenied
	}
	plaintext, err := s.aead.Open(make([]byte, 0, int(c.Size)), nonce, raw[12+n+len(nonce):], raw[12:12+n])
	if err != nil || uint64(len(plaintext)) != c.Size || domain.FingerprintCredential(plaintext) != c.Fingerprint {
		clear(plaintext)
		return nil, ErrDenied
	}
	return plaintext, nil
}

func (s *Store) envelopeRecord(raw []byte) (candidateRecord, error) {
	if len(raw) < 12 || len(raw) > maxEnvelopeBytes || !bytes.Equal(raw[:8], []byte("OLCB\x00\x00\x00\x01")) {
		return candidateRecord{}, ErrDenied
	}
	n := int(binary.BigEndian.Uint32(raw[8:12]))
	if n < 1 || n > maxHeaderBytes || n > len(raw)-12-s.aead.NonceSize()-s.aead.Overhead() {
		return candidateRecord{}, ErrDenied
	}
	var h envelopeHeader
	if strictDecode(raw[12:12+n], &h, maxHeaderBytes) != nil || !validReference(h.Reference) || h.Expected >= math.MaxUint64-1 || h.Fingerprint.Validate() != nil || h.Size == 0 || h.Size > maxAuthBytes {
		return candidateRecord{}, ErrDenied
	}
	c := candidateRecord{Reference: h.Reference, Expected: h.Expected, Fingerprint: h.Fingerprint, Size: h.Size, Ready: true}
	if h != s.header(c) {
		return candidateRecord{}, ErrDenied
	}
	return c, nil
}

func candidatePort(s *Store, c candidateRecord) ports.CredentialSecretCandidate {
	ref, _ := domain.NewCredentialSecretRef(c.Reference)
	return ports.CredentialSecretCandidate{Scope: s.scopeFor(c.Expected), Reference: ref, Fingerprint: c.Fingerprint}
}
