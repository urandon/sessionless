package attachedworkerpackage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

// NativeAction changes only an OS user-service registration. Register does
// not enable or start the service; unregister requires an inactive service.
type NativeAction string

const (
	NativeRegister   NativeAction = "register"
	NativeUnregister NativeAction = "unregister"
)

type NativePlanV1 struct {
	Version                      uint32       `json:"version"`
	Action                       NativeAction `json:"action"`
	Mode                         Mode         `json:"mode"`
	OwnerUserID                  string       `json:"owner_user_id"`
	WorkerID                     string       `json:"worker_id"`
	ManifestRevision             uint64       `json:"manifest_revision"`
	InstallRevision              uint64       `json:"install_revision"`
	ExpectedRegistrationRevision uint64       `json:"expected_registration_revision"`
	NextRegistrationRevision     uint64       `json:"next_registration_revision"`
	UnitPath                     string       `json:"unit_path"`
	UnitSHA256                   string       `json:"unit_sha256"`
	BinarySHA256                 string       `json:"binary_sha256"`
	PlanSHA256                   string       `json:"plan_sha256"`
}

// NativeReceiptV1 is local registration evidence, not running health. A
// mismatch with the OS manager is reconciliation-required, not success.
type NativeReceiptV1 struct {
	Version              uint32       `json:"version"`
	RegistrationRevision uint64       `json:"registration_revision"`
	Action               NativeAction `json:"action"`
	Mode                 Mode         `json:"mode"`
	OwnerUserID          string       `json:"owner_user_id"`
	WorkerID             string       `json:"worker_id"`
	ManifestRevision     uint64       `json:"manifest_revision"`
	InstallRevision      uint64       `json:"install_revision"`
	UnitSHA256           string       `json:"unit_sha256"`
	PlanSHA256           string       `json:"plan_sha256"`
}

type nativeState struct {
	loaded bool
	active bool
	path   string
}

type nativeManager interface {
	inspect(context.Context, Mode, string, string) (nativeState, error)
	register(context.Context, Mode, string, string) error
	unregister(context.Context, Mode, string, string) error
	start(context.Context, Mode, string, string) error
}

// NativePlan is read-only. It checks local staging, the exact registration
// receipt and current OS service state without changing either one.
func NativePlan(ctx context.Context, config Config, action NativeAction, expectedInstallRevision uint64) (NativePlanV1, error) {
	return nativePlan(ctx, config, action, expectedInstallRevision, osNativeManager{})
}

func nativePlan(ctx context.Context, config Config, action NativeAction, expectedInstallRevision uint64, manager nativeManager) (NativePlanV1, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || validateConfig(config) != nil ||
		(config.Mode != ModeLaunchd && config.Mode != ModeSystemdUser) ||
		(action != NativeRegister && action != NativeUnregister) || expectedInstallRevision == 0 ||
		expectedInstallRevision == ^uint64(0) {
		return NativePlanV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return NativePlanV1{}, ErrInvalid
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return NativePlanV1{}, errors.Join(ErrInvalid, err)
	}
	manifest := snapshot.Manifest
	if err := verifyBinary(config.BinaryPath, config.BinarySHA256); err != nil {
		return NativePlanV1{}, err
	}
	unitPath, receiptPath := paths(config, manifest)
	staged, _, err := readPrevious(config.InstallDir, unitPath, receiptPath, manifest)
	if err != nil || staged.InstallRevision != expectedInstallRevision || staged.Mode != config.Mode ||
		staged.ManifestRevision != manifest.Revision || staged.BinaryPath != config.BinaryPath ||
		staged.BinarySHA256 != config.BinarySHA256 {
		return NativePlanV1{}, errors.Join(ErrConflict, err)
	}
	registration, err := readNativeReceipt(unitPath)
	if err != nil {
		return NativePlanV1{}, err
	}
	if err := checkNoNativePending(unitPath); err != nil {
		return NativePlanV1{}, err
	}
	if registration.Version != 0 && (registration.Mode != config.Mode ||
		registration.OwnerUserID != string(manifest.OwnerUserID) ||
		registration.WorkerID != string(manifest.WorkerID) ||
		registration.ManifestRevision != manifest.Revision) {
		return NativePlanV1{}, ErrConflict
	}
	state, err := manager.inspect(ctx, config.Mode, nativeName(manifest), unitPath)
	if err != nil {
		return NativePlanV1{}, errors.Join(ErrIO, err)
	}
	switch action {
	case NativeRegister:
		if state.loaded || registration.Action == NativeRegister {
			return NativePlanV1{}, ErrConflict
		}
	case NativeUnregister:
		if registration.Action != NativeRegister || registration.InstallRevision != staged.InstallRevision ||
			registration.UnitSHA256 != staged.UnitSHA256 || !state.loaded || state.active ||
			state.path != unitPath {
			return NativePlanV1{}, ErrConflict
		}
	}
	if registration.RegistrationRevision == ^uint64(0) {
		return NativePlanV1{}, ErrConflict
	}
	plan := NativePlanV1{Version: VersionV1, Action: action, Mode: config.Mode,
		OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
		ManifestRevision: manifest.Revision, InstallRevision: staged.InstallRevision,
		ExpectedRegistrationRevision: registration.RegistrationRevision,
		NextRegistrationRevision:     registration.RegistrationRevision + 1,
		UnitPath:                     unitPath, UnitSHA256: staged.UnitSHA256, BinarySHA256: config.BinarySHA256}
	plan.PlanSHA256 = nativePlanDigest(plan)
	return plan, nil
}

// ApplyNative executes one reviewed registration change while holding the
// installation runtime lease. A command or readback failure after an attempted
// OS mutation is ambiguous and must not be retried automatically.
func ApplyNative(ctx context.Context, config Config, plan NativePlanV1) (NativeReceiptV1, error) {
	return applyNative(ctx, config, plan, osNativeManager{})
}

func applyNative(ctx context.Context, config Config, plan NativePlanV1, manager nativeManager) (result NativeReceiptV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || validateConfig(config) != nil || plan.Version != VersionV1 || plan.PlanSHA256 == "" ||
		plan.PlanSHA256 != nativePlanDigest(plan) {
		return NativeReceiptV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return NativeReceiptV1{}, ErrInvalid
	}
	operation, err := acquireOperationLease(config.InstallDir)
	if err != nil {
		return NativeReceiptV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, operation.Close()) }()
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return NativeReceiptV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	current, err := nativePlan(ctx, config, plan.Action, plan.InstallRevision, manager)
	if err != nil || current != plan {
		return NativeReceiptV1{}, errors.Join(ErrConflict, err)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Revision != plan.ManifestRevision {
		return NativeReceiptV1{}, errors.Join(ErrConflict, err)
	}
	name := nativeName(snapshot.Manifest)
	if err := writeAtomic(nativePendingPath(plan.UnitPath), encodeNativePlan(plan)); err != nil {
		return NativeReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	if plan.Action == NativeRegister {
		err = manager.register(ctx, plan.Mode, name, plan.UnitPath)
	} else {
		err = manager.unregister(ctx, plan.Mode, name, plan.UnitPath)
	}
	if err != nil {
		return NativeReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	state, err := manager.inspect(ctx, plan.Mode, name, plan.UnitPath)
	if err != nil || (plan.Action == NativeRegister && (!state.loaded || state.active || state.path != plan.UnitPath)) ||
		(plan.Action == NativeUnregister && state.loaded) {
		return NativeReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	receipt := NativeReceiptV1{Version: VersionV1, RegistrationRevision: plan.NextRegistrationRevision,
		Action: plan.Action, Mode: plan.Mode, OwnerUserID: plan.OwnerUserID, WorkerID: plan.WorkerID,
		ManifestRevision: plan.ManifestRevision, InstallRevision: plan.InstallRevision,
		UnitSHA256: plan.UnitSHA256, PlanSHA256: plan.PlanSHA256}
	if err := writeAtomic(nativeReceiptPath(plan.UnitPath), encodeNativeReceipt(receipt)); err != nil {
		return NativeReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	if err := removeNativePending(plan.UnitPath); err != nil {
		return NativeReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	return receipt, nil
}

func nativeName(manifest attachedworkerlocal.ManifestV1) string {
	return "com.sessionless.attached-worker." + shortID(manifest)
}

func nativeReceiptPath(unitPath string) string { return unitPath + ".native-receipt.json" }
func nativePendingPath(unitPath string) string { return unitPath + ".native-pending.json" }

func checkNoNativePending(unitPath string) error {
	_, err := os.Lstat(nativePendingPath(unitPath))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return errors.Join(ErrConflict, err)
}

func removeNativePending(unitPath string) error {
	if err := os.Remove(nativePendingPath(unitPath)); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(unitPath))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func encodeNativePlan(plan NativePlanV1) []byte {
	encoded, _ := json.Marshal(plan)
	return append(encoded, '\n')
}

func readNativeReceipt(unitPath string) (NativeReceiptV1, error) {
	encoded, err := readRegular(nativeReceiptPath(unitPath))
	if errors.Is(err, os.ErrNotExist) {
		return NativeReceiptV1{}, nil
	}
	if err != nil {
		return NativeReceiptV1{}, ErrConflict
	}
	var receipt NativeReceiptV1
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF ||
		receipt.Version != VersionV1 || receipt.RegistrationRevision == 0 ||
		(receipt.Action != NativeRegister && receipt.Action != NativeUnregister) ||
		!bytes.Equal(encoded, encodeNativeReceipt(receipt)) {
		return NativeReceiptV1{}, ErrConflict
	}
	return receipt, nil
}

func encodeNativeReceipt(receipt NativeReceiptV1) []byte {
	encoded, _ := json.Marshal(receipt)
	return append(encoded, '\n')
}

func nativePlanDigest(plan NativePlanV1) string {
	plan.PlanSHA256 = ""
	encoded, _ := json.Marshal(plan)
	return digest(encoded)
}
