// Package attachedworkerpackage stages exact, default-off service artifacts.
// It does not register, start, or auto-restart an OS service or container.
package attachedworkerpackage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkeractivation"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

const VersionV1 = uint32(1)

var (
	ErrInvalid   = errors.New("attached worker package plan is invalid")
	ErrConflict  = errors.New("attached worker package revision conflicts")
	ErrAmbiguous = errors.New("attached worker package write is ambiguous")
	ErrIO        = errors.New("attached worker package operation failed")
	safePath     = regexp.MustCompile(`^/[A-Za-z0-9_./-]+$`)
	imageDigest  = regexp.MustCompile(`^[A-Za-z0-9._:/-]+@sha256:[0-9a-f]{64}$`)
)

type Mode string

const (
	ModeLaunchd           Mode = "launchd"
	ModeSystemdUser       Mode = "systemd-user"
	ModeRootlessContainer Mode = "rootless-container"
)

// Config is entirely explicit. ContainerImage is a pinned daemon-service
// image, never the harness image from the installation manifest.
type Config struct {
	Mode              Mode
	StateRoot         string
	InstallDir        string
	BinaryPath        string
	BinarySHA256      string
	ContainerImage    string
	ActivationProfile string
}

// PlanV1 is the exact artifact and rollback precondition shown to the
// operator. It contains no secret, but paths remain local operator data.
type PlanV1 struct {
	Version                 uint32 `json:"version"`
	Mode                    Mode   `json:"mode"`
	OwnerUserID             string `json:"owner_user_id"`
	WorkerID                string `json:"worker_id"`
	ManifestRevision        uint64 `json:"manifest_revision"`
	ExpectedInstallRevision uint64 `json:"expected_install_revision"`
	NextInstallRevision     uint64 `json:"next_install_revision"`
	UnitPath                string `json:"unit_path"`
	UnitSHA256              string `json:"unit_sha256"`
	RollbackSHA256          string `json:"rollback_sha256,omitempty"`
	RollbackReceiptSHA256   string `json:"rollback_receipt_sha256,omitempty"`
	BinarySHA256            string `json:"binary_sha256"`
	ContainerImage          string `json:"container_image,omitempty"`
	ActivationProfile       string `json:"activation_profile,omitempty"`
	PlanSHA256              string `json:"plan_sha256"`
}

// ReceiptV1 is local package-stage evidence, not service registration or
// runtime health. A mismatch with the staged unit fails closed.
type ReceiptV1 struct {
	Version               uint32 `json:"version"`
	InstallRevision       uint64 `json:"install_revision"`
	ManifestRevision      uint64 `json:"manifest_revision"`
	OwnerUserID           string `json:"owner_user_id"`
	WorkerID              string `json:"worker_id"`
	Mode                  Mode   `json:"mode"`
	UnitSHA256            string `json:"unit_sha256"`
	BinaryPath            string `json:"binary_path,omitempty"`
	BinarySHA256          string `json:"binary_sha256"`
	ContainerImage        string `json:"container_image,omitempty"`
	ActivationProfile     string `json:"activation_profile,omitempty"`
	PlanSHA256            string `json:"plan_sha256"`
	RollbackSHA256        string `json:"rollback_sha256,omitempty"`
	RollbackReceiptSHA256 string `json:"rollback_receipt_sha256,omitempty"`
	Registration          string `json:"registration"`
}

// Plan reads the exact enrolled installation, pinned binary, prior staged
// receipt and unit. It does not create a directory or mutate service state.
func Plan(ctx context.Context, config Config, expectedInstallRevision uint64) (PlanV1, error) {
	if ctx == nil || ctx.Err() != nil || validateConfig(config) != nil {
		return PlanV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return PlanV1{}, ErrInvalid
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return PlanV1{}, errors.Join(ErrInvalid, err)
	}
	manifest := snapshot.Manifest
	if err := validateActivationProfile(config, manifest); err != nil {
		return PlanV1{}, err
	}
	if err := verifyBinary(config.BinaryPath, config.BinarySHA256); err != nil {
		return PlanV1{}, err
	}
	unit, err := render(config, manifest)
	if err != nil {
		return PlanV1{}, err
	}
	unitPath, receiptPath := paths(config, manifest)
	if err := checkLegacyWorkerOnlyUnit(config, manifest); err != nil {
		return PlanV1{}, err
	}
	if err := checkNoNativePending(unitPath); err != nil {
		return PlanV1{}, err
	}
	registration, err := readNativeReceipt(unitPath)
	if err != nil || registration.Action == NativeRegister {
		return PlanV1{}, errors.Join(ErrConflict, err)
	}
	previous, previousUnit, err := readPrevious(config.InstallDir, unitPath, receiptPath, manifest)
	if err != nil {
		return PlanV1{}, err
	}
	// A terminal unregister can justify one replacement of its exact unit.
	// Staging another replacement before registration would sever that
	// evidence chain and leave the new unit impossible to reconcile safely.
	if registration.Action == NativeUnregister && previous.InstallRevision != registration.InstallRevision {
		return PlanV1{}, ErrConflict
	}
	if previous.InstallRevision != expectedInstallRevision || expectedInstallRevision == ^uint64(0) {
		return PlanV1{}, ErrConflict
	}
	plan := PlanV1{
		Version: VersionV1, Mode: config.Mode, OwnerUserID: string(manifest.OwnerUserID),
		WorkerID: string(manifest.WorkerID), ManifestRevision: manifest.Revision,
		ExpectedInstallRevision: expectedInstallRevision, NextInstallRevision: expectedInstallRevision + 1,
		UnitPath: unitPath, UnitSHA256: digest(unit), BinarySHA256: config.BinarySHA256,
		ContainerImage: config.ContainerImage, ActivationProfile: config.ActivationProfile,
	}
	if len(previousUnit) > 0 {
		plan.RollbackSHA256 = digest(previousUnit)
		plan.RollbackReceiptSHA256 = digest(encodeReceipt(previous))
	}
	plan.PlanSHA256 = planDigest(plan)
	return plan, nil
}

// Apply stages one reviewed exact plan under the installation runtime lock.
// It intentionally does not call launchctl, systemctl or Docker. A caller
// must not infer that a staged artifact is a running service.
func Apply(ctx context.Context, config Config, plan PlanV1) (receiptResult ReceiptV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || validateConfig(config) != nil || plan.Version != VersionV1 ||
		plan.PlanSHA256 == "" || plan.PlanSHA256 != planDigest(plan) {
		return ReceiptV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return ReceiptV1{}, ErrInvalid
	}
	operation, err := acquireOperationLease(config.InstallDir)
	if err != nil {
		return ReceiptV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, operation.Close()) }()
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return ReceiptV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	current, err := Plan(ctx, config, plan.ExpectedInstallRevision)
	if err != nil || current != plan {
		return ReceiptV1{}, errors.Join(ErrConflict, err)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Revision != plan.ManifestRevision {
		return ReceiptV1{}, errors.Join(ErrConflict, err)
	}
	unit, err := render(config, snapshot.Manifest)
	if err != nil || digest(unit) != plan.UnitSHA256 {
		return ReceiptV1{}, ErrConflict
	}
	unitPath, receiptPath := paths(config, snapshot.Manifest)
	if err := checkLegacyWorkerOnlyUnit(config, snapshot.Manifest); err != nil {
		return ReceiptV1{}, err
	}
	previous, oldUnit, err := readPrevious(config.InstallDir, unitPath, receiptPath, snapshot.Manifest)
	if err != nil {
		return ReceiptV1{}, err
	}
	if err := ensurePrivateDir(config.InstallDir, true); err != nil {
		return ReceiptV1{}, err
	}
	if config.Mode == ModeRootlessContainer {
		if err := prepareRootlessDirectories(config); err != nil {
			return ReceiptV1{}, err
		}
	}
	receipt := ReceiptV1{
		Version: VersionV1, InstallRevision: plan.NextInstallRevision,
		ManifestRevision: plan.ManifestRevision, OwnerUserID: plan.OwnerUserID,
		WorkerID: plan.WorkerID, Mode: plan.Mode, UnitSHA256: plan.UnitSHA256,
		BinaryPath: config.BinaryPath, BinarySHA256: plan.BinarySHA256,
		ContainerImage: plan.ContainerImage, ActivationProfile: plan.ActivationProfile, PlanSHA256: plan.PlanSHA256,
		RollbackSHA256:        plan.RollbackSHA256,
		RollbackReceiptSHA256: plan.RollbackReceiptSHA256, Registration: "not_attempted",
	}
	if len(oldUnit) > 0 {
		if digest(encodeReceipt(previous)) != plan.RollbackReceiptSHA256 {
			return ReceiptV1{}, ErrConflict
		}
		if err := archiveBundle(unitPath, oldUnit, previous); err != nil {
			return ReceiptV1{}, err
		}
	}
	if err := archiveBundle(unitPath, unit, receipt); err != nil {
		return ReceiptV1{}, err
	}
	if err := writeAtomic(unitPath, unit); err != nil {
		return ReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	if err := writeAtomic(receiptPath, encodeReceipt(receipt)); err != nil {
		if errors.Is(err, ErrAmbiguous) {
			return ReceiptV1{}, err
		}
		// Best-effort exact rollback; a failed rollback remains unknown and
		// requires operator reconciliation rather than a success receipt.
		rollbackErr := rollback(unitPath, oldUnit)
		return ReceiptV1{}, errors.Join(ErrAmbiguous, err, rollbackErr)
	}
	return receipt, nil
}

func validateConfig(config Config) error {
	if !canonicalPath(config.StateRoot) || !canonicalPath(config.InstallDir) ||
		!canonicalPath(config.BinaryPath) || within(config.StateRoot, config.InstallDir) ||
		len(config.BinarySHA256) != 64 {
		return ErrInvalid
	}
	if _, err := hex.DecodeString(config.BinarySHA256); err != nil {
		return ErrInvalid
	}
	if config.ActivationProfile != "" {
		if !canonicalPath(config.ActivationProfile) || within(config.StateRoot, config.ActivationProfile) {
			return ErrInvalid
		}
		if _, err := attachedworkeractivation.ReadProfile(config.ActivationProfile); err != nil {
			return ErrInvalid
		}
	}
	switch config.Mode {
	case ModeLaunchd:
		if runtime.GOOS != "darwin" || config.ContainerImage != "" {
			return ErrInvalid
		}
	case ModeSystemdUser:
		if runtime.GOOS != "linux" || config.ContainerImage != "" {
			return ErrInvalid
		}
	case ModeRootlessContainer:
		if runtime.GOOS != "linux" {
			return ErrInvalid
		}
		if !imageDigest.MatchString(config.ContainerImage) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

func validateActivationProfile(config Config, manifest attachedworkerlocal.ManifestV1) error {
	if config.ActivationProfile == "" {
		return nil
	}
	profile, err := attachedworkeractivation.ReadProfile(config.ActivationProfile)
	if err != nil || attachedworkeractivation.ValidateForManifest(profile, manifest) != nil {
		return ErrInvalid
	}
	return nil
}

func within(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && (relative == "." || relative != ".." &&
		!filepath.IsAbs(relative) && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func canonicalPath(path string) bool {
	return safePath.MatchString(path) && filepath.Clean(path) == path && filepath.Dir(path) != path
}

func verifyBinary(path, expected string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 ||
		info.Mode().Perm()&0o022 != 0 || info.Size() <= 0 || info.Size() > 128<<20 {
		return ErrInvalid
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || canonical != path {
		return ErrInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return ErrIO
	}
	defer file.Close()
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return ErrIO
	}
	if hex.EncodeToString(hasher.Sum(nil)) != expected {
		return ErrConflict
	}
	return nil
}

// VerifyRunningBinary checks the explicit service path against the executable
// selected by the OS before the process is allowed to acquire runtime state.
func VerifyRunningBinary(path, expected string) error {
	actual, err := os.Executable()
	if err != nil {
		return ErrInvalid
	}
	actual, err = filepath.EvalSymlinks(actual)
	if err != nil || actual != path {
		return ErrConflict
	}
	return verifyBinary(path, expected)
}

func render(config Config, manifest attachedworkerlocal.ManifestV1) ([]byte, error) {
	if manifest.Validate() != nil || manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return nil, ErrInvalid
	}
	activationSHA256 := ""
	if config.ActivationProfile != "" {
		profile, pin, err := attachedworkeractivation.ReadProfileWithDigest(config.ActivationProfile)
		if err != nil || attachedworkeractivation.ValidateForManifest(profile, manifest) != nil {
			return nil, ErrInvalid
		}
		activationSHA256 = pin
		// A service unit keeps the reviewed installation baseline. Successful
		// connections advance the live manifest but do not mutate this unit.
		if config.Mode != ModeRootlessContainer {
			manifest.Revision = profile.ManifestRevision
		}
	}
	switch config.Mode {
	case ModeLaunchd:
		var path, state, activation bytes.Buffer
		_ = xml.EscapeText(&path, []byte(config.BinaryPath))
		_ = xml.EscapeText(&state, []byte(config.StateRoot))
		if config.ActivationProfile != "" {
			var escaped bytes.Buffer
			_ = xml.EscapeText(&escaped, []byte(config.ActivationProfile))
			activation.WriteString("<string>--activation-profile</string><string>")
			activation.Write(escaped.Bytes())
			activation.WriteString("</string><string>--activation-sha256</string><string>")
			activation.WriteString(activationSHA256)
			activation.WriteString("</string>")
		}
		label := "com.sessionless.attached-worker." + shortID(manifest)
		return []byte(fmt.Sprintf("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict><key>Label</key><string>%s</string><key>ProgramArguments</key><array><string>%s</string><string>serve</string><string>--state-dir</string><string>%s</string><string>--expected-revision</string><string>%d</string><string>--binary</string><string>%s</string><string>--binary-sha256</string><string>%s</string>%s</array><key>RunAtLoad</key><false/><key>KeepAlive</key><false/></dict></plist>\n", label, path.String(), state.String(), manifest.Revision, path.String(), config.BinarySHA256, activation.String())), nil
	case ModeSystemdUser:
		activation := ""
		if config.ActivationProfile != "" {
			activation = " --activation-profile " + config.ActivationProfile + " --activation-sha256 " + activationSHA256
		}
		return []byte(fmt.Sprintf("[Unit]\nDescription=Sessionless attached worker %s\n[Service]\nType=exec\nExecStart=%s serve --state-dir %s --expected-revision %d --binary %s --binary-sha256 %s%s\nRestart=no\nUMask=0077\nNoNewPrivileges=yes\n[Install]\nWantedBy=default.target\n", shortID(manifest), config.BinaryPath, config.StateRoot, manifest.Revision, config.BinaryPath, config.BinarySHA256, activation)), nil
	case ModeRootlessContainer:
		return renderRootless(config, manifest)
	default:
		return nil, ErrInvalid
	}
}

// validateStagedActivation prevents same-path profile or trust-bundle edits
// from silently changing the authority of a registered service.
func validateStagedActivation(config Config, manifest attachedworkerlocal.ManifestV1, staged ReceiptV1, unit []byte) error {
	if staged.ActivationProfile != config.ActivationProfile || validateActivationProfile(config, manifest) != nil {
		return ErrConflict
	}
	if config.ActivationProfile == "" && staged.ManifestRevision != manifest.Revision {
		return ErrConflict
	}
	if config.ActivationProfile != "" {
		profile, err := attachedworkeractivation.ReadProfile(config.ActivationProfile)
		if err != nil || staged.ManifestRevision < profile.ManifestRevision || staged.ManifestRevision > manifest.Revision {
			return ErrConflict
		}
	}
	currentUnit, err := render(config, manifest)
	if err != nil || !bytes.Equal(currentUnit, unit) || digest(currentUnit) != staged.UnitSHA256 {
		return ErrConflict
	}
	return nil
}

func validRegistrationManifestRevision(config Config, manifest attachedworkerlocal.ManifestV1, staged ReceiptV1, revision uint64) bool {
	if config.ActivationProfile == "" {
		return revision == manifest.Revision
	}
	return revision >= staged.ManifestRevision && revision <= manifest.Revision
}

// A completed unregister remains valid evidence after a newly staged unit
// replaces exactly that unit, even if the connection advanced meanwhile.
// A registered receipt never crosses that package boundary.
func validRegistrationForStage(config Config, manifest attachedworkerlocal.ManifestV1, staged ReceiptV1, registration NativeReceiptV1) bool {
	if validRegistrationManifestRevision(config, manifest, staged, registration.ManifestRevision) {
		return true
	}
	return registration.Action == NativeUnregister && registration.ManifestRevision < manifest.Revision &&
		registration.InstallRevision != ^uint64(0) && registration.InstallRevision+1 == staged.InstallRevision &&
		registration.UnitSHA256 == staged.RollbackSHA256
}

func shortID(manifest attachedworkerlocal.ManifestV1) string {
	// The same worker locator may be enrolled by different owners (or tenants)
	// on one host. OS unit and container names must follow the authority scope,
	// not the caller-chosen locator alone. Length prefixes make the tuple unique
	// even if an identifier contains a separator.
	var identity []byte
	for _, part := range []string{string(manifest.TenantID), string(manifest.OwnerUserID), string(manifest.WorkerID)} {
		identity = binary.BigEndian.AppendUint32(identity, uint32(len(part)))
		identity = append(identity, part...)
	}
	hash := sha256.Sum256(identity)
	return hex.EncodeToString(hash[:8])
}

// Before the scoped naming change, units were named from WorkerID alone.
// Do not silently stage a second unit while a legacy unit or registration
// receipt may still own the same installation. It must be explicitly stopped
// and unregistered under its old exact name first.
func checkLegacyWorkerOnlyUnit(config Config, manifest attachedworkerlocal.ManifestV1) error {
	oldUnit := legacyWorkerOnlyUnitPath(config, manifest)
	if oldUnit == "" {
		return ErrInvalid
	}
	for _, path := range []string{oldUnit, oldUnit + ".receipt.json", nativeReceiptPath(oldUnit), nativePendingPath(oldUnit)} {
		if _, err := os.Lstat(path); err == nil {
			return ErrConflict
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.Join(ErrConflict, err)
		}
	}
	return nil
}

func legacyWorkerOnlyUnitPath(config Config, manifest attachedworkerlocal.ManifestV1) string {
	oldHash := sha256.Sum256([]byte(string(manifest.WorkerID)))
	oldName := "sessionless-attached-worker-" + hex.EncodeToString(oldHash[:8])
	switch config.Mode {
	case ModeLaunchd:
		oldName += ".plist"
	case ModeSystemdUser, ModeRootlessContainer:
		oldName += ".service"
	default:
		return ""
	}
	return filepath.Join(config.InstallDir, oldName)
}

func paths(config Config, manifest attachedworkerlocal.ManifestV1) (string, string) {
	name := "sessionless-attached-worker-" + shortID(manifest)
	switch config.Mode {
	case ModeLaunchd:
		name += ".plist"
	case ModeSystemdUser, ModeRootlessContainer:
		name += ".service"
	}
	unit := filepath.Join(config.InstallDir, name)
	return unit, unit + ".receipt.json"
}

func readPrevious(directory, unitPath, receiptPath string, manifest attachedworkerlocal.ManifestV1) (ReceiptV1, []byte, error) {
	if err := ensurePrivateDir(directory, false); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ReceiptV1{}, nil, nil
		}
		return ReceiptV1{}, nil, err
	}
	unit, unitErr := readRegular(unitPath)
	receiptBytes, receiptErr := readRegular(receiptPath)
	if errors.Is(unitErr, os.ErrNotExist) && errors.Is(receiptErr, os.ErrNotExist) {
		return ReceiptV1{}, nil, nil
	}
	if unitErr != nil || receiptErr != nil {
		return ReceiptV1{}, nil, ErrConflict
	}
	var receipt ReceiptV1
	decoder := json.NewDecoder(bytes.NewReader(receiptBytes))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF || receipt.Version != VersionV1 ||
		receipt.InstallRevision == 0 || receipt.OwnerUserID != string(manifest.OwnerUserID) ||
		receipt.WorkerID != string(manifest.WorkerID) || receipt.UnitSHA256 != digest(unit) ||
		receipt.Registration != "not_attempted" {
		return ReceiptV1{}, nil, ErrConflict
	}
	return receipt, unit, nil
}

func ensurePrivateDir(directory string, create bool) error {
	if create {
		if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return errors.Join(ErrIO, err)
		}
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return ErrInvalid
	}
	canonical, err := filepath.EvalSymlinks(directory)
	if err != nil || canonical != directory {
		return ErrInvalid
	}
	return nil
}

func readRegular(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) || info.Size() > 64<<10 {
		return nil, ErrInvalid
	}
	return os.ReadFile(path)
}

func writeAtomic(path string, payload []byte) (resultErr error) {
	return writeAtomicWithSync(path, payload, (*os.File).Sync, (*os.File).Sync)
}

// writeAtomicWithSync keeps the pre-rename and post-rename durability points
// separately injectable for deterministic fault tests. A failed directory
// fsync means the new name may already be visible and is always ambiguous.
func writeAtomicWithSync(path string, payload []byte, syncFile, syncDirectory func(*os.File) error) (resultErr error) {
	if syncFile == nil || syncDirectory == nil {
		return ErrInvalid
	}
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, ".sessionless-stage-")
	if err != nil {
		return errors.Join(ErrIO, err)
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return errors.Join(ErrIO, err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return errors.Join(ErrIO, err)
	}
	if err := syncFile(file); err != nil {
		_ = file.Close()
		return errors.Join(ErrIO, err)
	}
	if err := file.Close(); err != nil {
		return errors.Join(ErrIO, err)
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return errors.Join(ErrIO, err)
	}
	dir, err := os.Open(directory)
	if err != nil {
		return ErrAmbiguous
	}
	defer dir.Close()
	if err := syncDirectory(dir); err != nil {
		return ErrAmbiguous
	}
	return nil
}

func rollback(unitPath string, old []byte) error {
	if len(old) > 0 {
		return writeAtomic(unitPath, old)
	}
	if err := os.Remove(unitPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrAmbiguous
	}
	directory, err := os.Open(filepath.Dir(unitPath))
	if err != nil {
		return ErrAmbiguous
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return ErrAmbiguous
	}
	return nil
}

func digest(payload []byte) string {
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:])
}

func planDigest(plan PlanV1) string {
	plan.PlanSHA256 = ""
	encoded, _ := json.Marshal(plan)
	return digest(encoded)
}
