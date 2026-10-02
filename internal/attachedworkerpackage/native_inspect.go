package attachedworkerpackage

import (
	"context"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

// NativeInspectionV1 is OS registration evidence, not worker or server health.
// An active OS service still needs live-status to prove the local owner.
type NativeInspectionV1 struct {
	Version              uint32 `json:"version"`
	Status               string `json:"status"`
	OSActive             bool   `json:"os_active"`
	ManifestRevision     uint64 `json:"manifest_revision"`
	InstallRevision      uint64 `json:"install_revision"`
	RegistrationRevision uint64 `json:"registration_revision"`
}

func NativeInspect(ctx context.Context, config Config) (NativeInspectionV1, error) {
	return nativeInspect(ctx, config, osNativeManager{})
}

func nativeInspect(ctx context.Context, config Config, manager nativeManager) (NativeInspectionV1, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || validateConfig(config) != nil ||
		(config.Mode != ModeLaunchd && config.Mode != ModeSystemdUser) {
		return NativeInspectionV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return NativeInspectionV1{}, ErrInvalid
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return NativeInspectionV1{}, errors.Join(ErrConflict, err)
	}
	manifest := snapshot.Manifest
	if err := verifyBinary(config.BinaryPath, config.BinarySHA256); err != nil {
		return NativeInspectionV1{}, err
	}
	unitPath, receiptPath := paths(config, manifest)
	staged, _, err := readPrevious(config.InstallDir, unitPath, receiptPath, manifest)
	if err != nil || staged.Version != VersionV1 || staged.Mode != config.Mode ||
		staged.ManifestRevision != manifest.Revision || staged.BinaryPath != config.BinaryPath ||
		staged.BinarySHA256 != config.BinarySHA256 {
		return NativeInspectionV1{}, errors.Join(ErrConflict, err)
	}
	registration, err := readNativeReceipt(unitPath)
	if err != nil {
		return NativeInspectionV1{}, err
	}
	result := NativeInspectionV1{Version: VersionV1, Status: "reconciliation_required",
		ManifestRevision: manifest.Revision, InstallRevision: staged.InstallRevision,
		RegistrationRevision: registration.RegistrationRevision}
	if err := checkNoNativePending(unitPath); err != nil {
		return result, nil
	}
	state, err := manager.inspect(ctx, config.Mode, nativeName(manifest), unitPath)
	if err != nil {
		return result, errors.Join(ErrIO, err)
	}
	result.OSActive = state.active
	if registration.Version != 0 && (registration.Mode != config.Mode ||
		registration.OwnerUserID != string(manifest.OwnerUserID) ||
		registration.WorkerID != string(manifest.WorkerID) ||
		registration.ManifestRevision != manifest.Revision) {
		return result, nil
	}
	switch {
	case !state.loaded && registration.Action != NativeRegister:
		result.Status = "unregistered"
	case state.loaded && state.path == unitPath && registration.Action == NativeRegister &&
		registration.InstallRevision == staged.InstallRevision && registration.UnitSHA256 == staged.UnitSHA256:
		result.Status = "registered"
	}
	return result, nil
}

// NativeStart explicitly requests one start of an exact registered user
// service. The command never enables restart; success is only OS readback,
// not proof of local owner or server connection.
func NativeStart(ctx context.Context, config Config, expectedInstallRevision, expectedRegistrationRevision uint64) (NativeInspectionV1, error) {
	return nativeStart(ctx, config, expectedInstallRevision, expectedRegistrationRevision, osNativeManager{})
}

func nativeStart(ctx context.Context, config Config, expectedInstallRevision, expectedRegistrationRevision uint64, manager nativeManager) (NativeInspectionV1, error) {
	if ctx == nil || ctx.Err() != nil || expectedInstallRevision == 0 || expectedRegistrationRevision == 0 ||
		manager == nil {
		return NativeInspectionV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return NativeInspectionV1{}, ErrInvalid
	}
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return NativeInspectionV1{}, err
	}
	inspection, inspectErr := nativeInspect(ctx, config, manager)
	if closeErr := lease.Close(); closeErr != nil {
		return NativeInspectionV1{}, errors.Join(ErrAmbiguous, closeErr)
	}
	if inspectErr != nil || inspection.Status != "registered" || inspection.OSActive ||
		inspection.InstallRevision != expectedInstallRevision ||
		inspection.RegistrationRevision != expectedRegistrationRevision {
		return inspection, errors.Join(ErrConflict, inspectErr)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Revision != inspection.ManifestRevision {
		return inspection, errors.Join(ErrConflict, err)
	}
	unitPath, _ := paths(config, snapshot.Manifest)
	if err := manager.start(ctx, config.Mode, nativeName(snapshot.Manifest), unitPath); err != nil {
		return inspection, errors.Join(ErrAmbiguous, err)
	}
	readbackCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		result, err := nativeInspect(readbackCtx, config, manager)
		if err == nil && result.Status == "registered" && result.OSActive {
			return result, nil
		}
		select {
		case <-readbackCtx.Done():
			return result, errors.Join(ErrAmbiguous, err, readbackCtx.Err())
		case <-ticker.C:
		}
	}
}
