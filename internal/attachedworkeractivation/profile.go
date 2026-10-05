// Package attachedworkeractivation is the explicit, default-off operator gate
// for a provider-neutral synthetic attached-worker runtime.
package attachedworkeractivation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const maxProfileBytes = 64 << 10

var ErrInvalidProfile = errors.New("attached worker activation profile is invalid")

// ProfileV1 is an operator-authored, private file. It carries no provider
// credential and grants only synthetic/denied-credential execution. Every
// installation and connection identity is pinned before any network action.
type ProfileV1 struct {
	Version                 uint32                                       `json:"version"`
	Mode                    string                                       `json:"mode"`
	ManifestRevision        uint64                                       `json:"manifest_revision"`
	ControlPlaneOrigin      string                                       `json:"control_plane_origin"`
	TenantID                domain.TenantID                              `json:"tenant_id"`
	OwnerUserID             domain.UserID                                `json:"owner_user_id"`
	WorkerID                domain.AttachedWorkerID                      `json:"worker_id"`
	EnrollmentGeneration    uint64                                       `json:"enrollment_generation"`
	ConnectionGeneration    uint64                                       `json:"connection_generation"`
	InstallationSHA256      string                                       `json:"installation_sha256"`
	ExpectedWorkerRevision  uint64                                       `json:"expected_worker_revision"`
	Capability              attachedworkerprotocol.CapabilityManifestV1  `json:"capability"`
	LocalProfile            attachedworkerdaemontransport.LocalProfileV1 `json:"local_profile"`
	TLSRootPEMPath          string                                       `json:"tls_root_pem_path"`
	MaterializationRoot     string                                       `json:"materialization_root"`
	ScratchRoot             string                                       `json:"scratch_root"`
	AllowedReadRoots        []string                                     `json:"allowed_read_roots"`
	AllowedEnvironmentNames []string                                     `json:"allowed_environment_names"`
	MaxInputBytes           int                                          `json:"max_input_bytes"`
	rootPEM                 []byte
}

// ReadProfile accepts only a single owner-owned, non-symlink regular file in
// a private canonical directory. The same restriction applies to the
// optional trust bundle. Paths are never inferred from HOME or environment.
func ReadProfile(path string) (ProfileV1, error) {
	profile, _, err := ReadProfileWithDigest(path)
	return profile, err
}

// ReadProfileWithDigest binds the exact private profile and trust bundle
// bytes to a reviewed service unit. The returned profile retains the trust
// bytes so Connect never rereads a replaceable path after this check.
func ReadProfileWithDigest(path string) (ProfileV1, string, error) {
	data, err := readPrivateFile(path, maxProfileBytes)
	if err != nil {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	defer clear(data)
	var profile ProfileV1
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&profile); err != nil {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	if profile.Version != 1 || profile.Mode != "synthetic-denied" || profile.ManifestRevision == 0 ||
		profile.ExpectedWorkerRevision == 0 || profile.EnrollmentGeneration == 0 ||
		profile.MaxInputBytes <= 0 || profile.MaxInputBytes > 1<<20 ||
		profile.ConnectionGeneration == ^uint64(0) || len(profile.InstallationSHA256) != 64 ||
		profile.InstallationSHA256 != strings.ToLower(profile.InstallationSHA256) ||
		profile.TenantID.Validate() != nil ||
		profile.OwnerUserID.Validate() != nil || profile.WorkerID.Validate() != nil ||
		profile.Capability.Validate() != nil || profile.LocalProfile.CapabilityDigest.Validate() != nil ||
		profile.Capability.OperatingSystem != runtime.GOOS || profile.Capability.Architecture != runtime.GOARCH ||
		len(profile.LocalProfile.Environment) != 0 || len(profile.AllowedEnvironmentNames) != 0 ||
		len(profile.AllowedReadRoots) != 0 {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	if _, err := hex.DecodeString(profile.InstallationSHA256); err != nil {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	digest, err := attachedworkerprotocol.ManifestDigestV1(profile.Capability)
	if err != nil || profile.LocalProfile.CapabilityDigest != domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)) ||
		!slices.Equal(profile.LocalProfile.ExecutableDigest[:], profile.Capability.HarnessExecutableDigest) {
		return ProfileV1{}, "", ErrInvalidProfile
	}
	if profile.TLSRootPEMPath != "" {
		profile.rootPEM, err = readPrivateFile(profile.TLSRootPEMPath, maxProfileBytes)
		if err != nil {
			return ProfileV1{}, "", ErrInvalidProfile
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(profile.rootPEM) {
			clear(profile.rootPEM)
			return ProfileV1{}, "", ErrInvalidProfile
		}
	}
	hasher := sha256.New()
	hasher.Write([]byte("sessionless-activation-profile-v1"))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(data)))
	hasher.Write(length[:])
	hasher.Write(data)
	binary.BigEndian.PutUint64(length[:], uint64(len(profile.rootPEM)))
	hasher.Write(length[:])
	hasher.Write(profile.rootPEM)
	return profile, hex.EncodeToString(hasher.Sum(nil)), nil
}

// Connect validates local installation authority before constructing any
// outbound client. The underlying pinned runtime performs its own lease-held
// preflight and checkpoint reconciliation before an initial or reconnect call.
func Connect(ctx context.Context, store *attachedworkerlocal.Store, profile ProfileV1) (*attachedworkersealedinput.SyntheticRuntime, error) {
	return ConnectWithClock(ctx, store, profile, nil)
}

// ConnectWithClock exists for deterministic command-level fixtures. The
// shipped command always calls Connect, which uses the real clock and the
// immutable 15-minute transport minimum.
func ConnectWithClock(ctx context.Context, store *attachedworkerlocal.Store, profile ProfileV1, now func() time.Time) (*attachedworkersealedinput.SyntheticRuntime, error) {
	if ctx == nil || ctx.Err() != nil || store == nil {
		return nil, ErrInvalidProfile
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || !matchesManifest(profile, snapshot.Manifest) {
		return nil, ErrInvalidProfile
	}
	origin, err := url.Parse(profile.ControlPlaneOrigin)
	if err != nil || origin.Scheme != "https" || origin.Host == "" || origin.User != nil ||
		origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" {
		return nil, ErrInvalidProfile
	}
	var roots *x509.CertPool
	if profile.TLSRootPEMPath != "" {
		if len(profile.rootPEM) == 0 {
			return nil, ErrInvalidProfile
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(profile.rootPEM) {
			return nil, ErrInvalidProfile
		}
	}
	transport := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	bootstrap, err := attachedworkerhttp.NewBootstrapClient(attachedworkerhttp.BootstrapClientConfig{BaseURL: profile.ControlPlaneOrigin, HTTPClient: client})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, ErrInvalidProfile
	}
	exchange, err := attachedworkersession.NewHTTPExchangeFactory(attachedworkersession.HTTPExchangeFactoryConfig{
		BaseURL: profile.ControlPlaneOrigin, RootCAs: roots,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, ErrInvalidProfile
	}
	offer := profile.Capability.ProtocolOffer
	config := attachedworkersealedinput.SyntheticRuntimeConfig{
		Store: store, Bootstrap: bootstrap, Exchange: exchange,
		Session: attachedworkersession.Config{
			Audience: "sessionless:attached-worker:v1", WorkerOffer: offer,
			ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
			OperationTimeout:    15 * time.Second, Now: now,
		},
		Connect: attachedworkersession.ConnectInputV1{
			ExpectedWorkerRevision: profile.ExpectedWorkerRevision,
			CapabilityManifest:     profile.Capability,
		},
		SealedEndpoint: profile.ControlPlaneOrigin + attachedworkersealedinput.PathV1,
		SealedClient:   client, MaxInputBytes: profile.MaxInputBytes, Now: now,
		Adapter: attachedworkerdaemontransport.Config{
			Profile: profile.LocalProfile, MaterializationRoot: profile.MaterializationRoot,
			MaxInputBytes: profile.MaxInputBytes, Now: now,
		},
		Poll: attachedworkertransport.Config{
			Enabled: true, PollInterval: attachedworkertransport.MinimumHeartbeatInterval,
			InitialBackoff: time.Second, MaxBackoff: time.Minute, Now: now,
		},
		Stack: attachedworkerstack.Config{
			ScratchRoot: profile.ScratchRoot, AllowedReadRoots: profile.AllowedReadRoots,
			AllowedEnvironmentNames: profile.AllowedEnvironmentNames,
		},
	}
	var owner *attachedworkersealedinput.SyntheticRuntime
	if snapshot.Manifest.ConnectionGeneration == 0 {
		owner, err = attachedworkersealedinput.ConnectSyntheticPinnedRuntime(ctx, config)
	} else {
		owner, err = attachedworkersealedinput.ReconnectSyntheticPinnedRuntime(ctx, config)
	}
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return owner, nil
}

func matchesManifest(profile ProfileV1, manifest attachedworkerlocal.ManifestV1) bool {
	// A successful connect/reconnect advances both counters once. Keep the
	// operator's original installation pin while allowing only that paired
	// progression; an unrelated manifest mutation must require a new profile.
	if manifest.Revision < profile.ManifestRevision || manifest.ConnectionGeneration < profile.ConnectionGeneration ||
		manifest.Revision-profile.ManifestRevision != manifest.ConnectionGeneration-profile.ConnectionGeneration {
		return false
	}
	installationDigest, err := InstallationDigestV1(manifest)
	if err != nil || installationDigest != profile.InstallationSHA256 {
		return false
	}
	return profile.Version == 1 && profile.Mode == "synthetic-denied" &&
		manifest.Lifecycle == attachedworkerlocal.LifecycleActive &&
		manifest.ControlPlaneOrigin == profile.ControlPlaneOrigin &&
		manifest.TenantID == profile.TenantID && manifest.OwnerUserID == profile.OwnerUserID &&
		manifest.WorkerID == profile.WorkerID && manifest.EnrollmentGeneration == profile.EnrollmentGeneration &&
		profile.ExpectedWorkerRevision != 0 &&
		profile.Capability.WorkerID == string(manifest.WorkerID) &&
		profile.Capability.EnrollmentGeneration == manifest.EnrollmentGeneration
}

// InstallationDigestV1 pins execution-affecting launcher configuration while
// allowing the manifest's connection-only revision/generation progression.
func InstallationDigestV1(manifest attachedworkerlocal.ManifestV1) (string, error) {
	data, err := json.Marshal(struct {
		OCI     attachedworkerlocal.OCIConfigV1     `json:"oci"`
		Harness attachedworkerlocal.HarnessConfigV1 `json:"harness"`
	}{OCI: manifest.OCI, Harness: manifest.Harness})
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	hasher.Write([]byte("sessionless-activation-installation-v1\x00"))
	hasher.Write(data)
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// ValidateForManifest checks the operator's exact installation pin without
// acquiring a lease or contacting the control plane.
func ValidateForManifest(profile ProfileV1, manifest attachedworkerlocal.ManifestV1) error {
	if !matchesManifest(profile, manifest) {
		return ErrInvalidProfile
	}
	return nil
}

func readPrivateFile(path string, limit int64) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || limit <= 0 {
		return nil, ErrInvalidProfile
	}
	parent := filepath.Dir(path)
	canonical, err := filepath.EvalSymlinks(parent)
	if err != nil || canonical != parent {
		return nil, ErrInvalidProfile
	}
	directory, err := os.Stat(parent)
	if err != nil || !directory.IsDir() || directory.Mode().Perm() != 0o700 || !ownedByCurrentUser(directory) {
		return nil, ErrInvalidProfile
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrInvalidProfile
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 ||
		!ownedByCurrentUser(info) || info.Size() <= 0 || info.Size() > limit {
		return nil, ErrInvalidProfile
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(data) == 0 || int64(len(data)) > limit {
		clear(data)
		return nil, ErrInvalidProfile
	}
	return data, nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
