package s3store

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const providerObjectRoot = "sessionless-provider-credentials/v1/"
const providerObjectMagic = "SLPCS001"
const providerMetadataMax = 4 << 10
const providerCipherMax = (64 << 10) + 16
const providerEnvelopeMax = 16 + 12 + providerMetadataMax + providerCipherMax + 32
const providerTimeLayout = "2006-01-02T15:04:05.000000Z"

type providerSecretMetadata struct {
	Version              uint32                        `json:"version"`
	Epoch                string                        `json:"epoch"`
	TenantID             domain.TenantID               `json:"tenant_id"`
	OwnerUserID          domain.UserID                 `json:"owner_user_id"`
	ResourceKind         domain.ProviderResourceKindV1 `json:"resource_kind"`
	ResourceID           string                        `json:"resource_id"`
	ResourceRevision     uint64                        `json:"resource_revision"`
	CredentialGeneration uint64                        `json:"credential_generation"`
	MutationID           string                        `json:"mutation_id"`
	Reference            string                        `json:"reference"`
	Fingerprint          domain.CredentialFingerprint  `json:"fingerprint"`
	CreatedAt            string                        `json:"created_at"`
}

type providerObjectRef struct{ epoch, stamp, scope, id string }

func providerDigest(value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// ValidateProviderCredentialKeyEpoch is the canonical nonsecret epoch grammar,
// shared by explicit startup intake and the encrypted object's constructor/wire.
func ValidateProviderCredentialKeyEpoch(epoch string) error {
	if len(epoch) == 0 || len(epoch) > 32 {
		return errProviderCredentialObject
	}
	for _, c := range epoch {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return errProviderCredentialObject
		}
	}
	return nil
}

func validProviderScope(scope ports.ProviderCredentialCandidateScopeV1) bool {
	return scope.Locator.Validate() == nil && scope.ResourceRevision > 0 && scope.CredentialGeneration > 0 && domain.ValidateOpaqueID("mutation", scope.MutationID) == nil
}

func parseProviderRef(ref domain.CredentialSecretRef) (providerObjectRef, error) {
	parts := strings.Split(ref.StorageValue(), ".")
	if ref.Validate() != nil || len(parts) != 5 || parts[0] != "pcs1" || ValidateProviderCredentialKeyEpoch(parts[1]) != nil {
		return providerObjectRef{}, errProviderCredentialObject
	}
	r := providerObjectRef{epoch: parts[1], stamp: parts[2], scope: parts[3], id: parts[4]}
	micros, err := strconv.ParseInt(r.stamp, 10, 64)
	if err != nil || micros < 0 || len(r.stamp) != 20 || fmt.Sprintf("%020d", micros) != r.stamp || domain.ValidateSHA256Digest("scope", r.scope) != nil || domain.ValidateSHA256Digest("object", r.id) != nil {
		return providerObjectRef{}, errProviderCredentialObject
	}
	return r, nil
}

func providerObjectKey(locator ports.ProviderCredentialLocatorV1, ref domain.CredentialSecretRef) (string, error) {
	r, err := parseProviderRef(ref)
	if err != nil || locator.Validate() != nil {
		return "", errProviderCredentialObject
	}
	return providerObjectRoot + providerDigest(locator) + "/" + r.stamp + "." + r.scope + "." + r.id, nil
}

func metadataFor(candidate ports.ProviderCredentialSecretCandidateV1, epoch string) providerSecretMetadata {
	s := candidate.Scope
	return providerSecretMetadata{1, epoch, s.Locator.TenantID, s.Locator.OwnerUserID, s.Locator.ResourceKind, s.Locator.ResourceID, s.ResourceRevision, s.CredentialGeneration, s.MutationID, candidate.Reference.StorageValue(), candidate.Fingerprint, candidate.CreatedAt.Format(providerTimeLayout)}
}

func (m providerSecretMetadata) candidate() (ports.ProviderCredentialSecretCandidateV1, error) {
	ref, err := domain.NewCredentialSecretRef(m.Reference)
	if err != nil {
		return ports.ProviderCredentialSecretCandidateV1{}, errProviderCredentialObject
	}
	r, err := parseProviderRef(ref)
	at, timeErr := time.Parse(providerTimeLayout, m.CreatedAt)
	c := ports.ProviderCredentialSecretCandidateV1{Scope: ports.ProviderCredentialCandidateScopeV1{Locator: ports.ProviderCredentialLocatorV1{TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, ResourceKind: m.ResourceKind, ResourceID: m.ResourceID}, ResourceRevision: m.ResourceRevision, CredentialGeneration: m.CredentialGeneration, MutationID: m.MutationID}, Reference: ref, Fingerprint: m.Fingerprint, CreatedAt: at}
	if err != nil || timeErr != nil || m.Version != 1 || m.Epoch != r.epoch || at.Year() < 1970 || at.Format(providerTimeLayout) != m.CreatedAt || fmt.Sprintf("%020d", at.UnixMicro()) != r.stamp || providerDigest(c.Scope) != r.scope || !validProviderScope(c.Scope) || c.Fingerprint.Validate() != nil {
		return ports.ProviderCredentialSecretCandidateV1{}, errProviderCredentialObject
	}
	return c, nil
}

func providerKeys(root []byte, r providerObjectRef) ([]byte, []byte, error) {
	salt, err := hex.DecodeString(r.id)
	if err != nil {
		return nil, nil, errProviderCredentialObject
	}
	a, err := hkdf.Key(sha256.New, root, salt, "sessionless.provider-credential.object.v1/aead", 32)
	if err != nil {
		return nil, nil, errProviderCredentialObject
	}
	m, err := hkdf.Key(sha256.New, root, salt, "sessionless.provider-credential.object.v1/mac", 32)
	if err != nil {
		clear(a)
		return nil, nil, errProviderCredentialObject
	}
	return a, m, nil
}

func sealProviderObject(root []byte, candidate ports.ProviderCredentialSecretCandidateV1, epoch string, secret []byte, random io.Reader) ([]byte, error) {
	r, err := parseProviderRef(candidate.Reference)
	if err != nil || len(secret) == 0 || len(secret) > 64<<10 {
		return nil, errProviderCredentialObject
	}
	meta := metadataFor(candidate, epoch)
	if _, err := meta.candidate(); err != nil {
		return nil, errProviderCredentialObject
	}
	raw, _ := json.Marshal(meta)
	if len(raw) > providerMetadataMax {
		return nil, errProviderCredentialObject
	}
	a, m, err := providerKeys(root, r)
	if err != nil {
		return nil, err
	}
	defer clear(a)
	defer clear(m)
	block, err := aes.NewCipher(a)
	if err != nil {
		return nil, errProviderCredentialObject
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errProviderCredentialObject
	}
	header := make([]byte, 16)
	copy(header, providerObjectMagic)
	binary.BigEndian.PutUint32(header[8:12], uint32(len(raw)))
	binary.BigEndian.PutUint32(header[12:16], uint32(len(secret)+gcm.Overhead()))
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(random, nonce); err != nil {
		return nil, errProviderCredentialObject
	}
	aad := append(append([]byte(nil), header...), raw...)
	wire := make([]byte, 0, 16+12+len(raw)+len(secret)+16+32)
	wire = append(wire, header...)
	wire = append(wire, nonce...)
	wire = append(wire, raw...)
	wire = gcm.Seal(wire, nonce, secret, aad)
	mac := hmac.New(sha256.New, m)
	_, _ = mac.Write(wire)
	wire = append(wire, mac.Sum(nil)...)
	return wire, nil
}

func authenticateProviderObject(root []byte, epoch, key string, wire []byte) (ports.ProviderCredentialSecretCandidateV1, []byte, []byte, error) {
	bad := func() (ports.ProviderCredentialSecretCandidateV1, []byte, []byte, error) {
		return ports.ProviderCredentialSecretCandidateV1{}, nil, nil, errProviderCredentialObject
	}
	if len(wire) < 16+12+16+32 || len(wire) > providerEnvelopeMax || string(wire[:8]) != providerObjectMagic {
		return bad()
	}
	ml, cl := int(binary.BigEndian.Uint32(wire[8:12])), int(binary.BigEndian.Uint32(wire[12:16]))
	if ml < 1 || ml > providerMetadataMax || cl < 17 || cl > providerCipherMax || len(wire) != 16+12+ml+cl+32 {
		return bad()
	}
	raw := wire[28 : 28+ml]
	var meta providerSecretMetadata
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&meta) != nil {
		return bad()
	}
	canonical, _ := json.Marshal(meta)
	if !bytes.Equal(raw, canonical) {
		return bad()
	}
	c, err := meta.candidate()
	if err != nil || meta.Epoch != epoch {
		return bad()
	}
	exact, err := providerObjectKey(c.Scope.Locator, c.Reference)
	if err != nil || key != exact {
		return bad()
	}
	r, _ := parseProviderRef(c.Reference)
	a, m, err := providerKeys(root, r)
	if err != nil {
		return bad()
	}
	defer clear(m)
	mac := hmac.New(sha256.New, m)
	_, _ = mac.Write(wire[:len(wire)-32])
	if !hmac.Equal(mac.Sum(nil), wire[len(wire)-32:]) {
		clear(a)
		return bad()
	}
	return c, a, append(append([]byte(nil), wire[:16]...), raw...), nil
}

func openProviderObject(root []byte, epoch, key string, wire []byte) (ports.ProviderCredentialSecretCandidateV1, []byte, error) {
	c, a, aad, err := authenticateProviderObject(root, epoch, key, wire)
	if err != nil {
		return c, nil, err
	}
	defer clear(a)
	block, err := aes.NewCipher(a)
	if err != nil {
		return c, nil, errProviderCredentialObject
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return c, nil, errProviderCredentialObject
	}
	ml := int(binary.BigEndian.Uint32(wire[8:12]))
	secret, err := gcm.Open(nil, wire[16:28], wire[28+ml:len(wire)-32], aad)
	if err != nil || domain.FingerprintCredential(secret) != c.Fingerprint {
		clear(secret)
		return c, nil, errProviderCredentialObject
	}
	return c, secret, nil
}
