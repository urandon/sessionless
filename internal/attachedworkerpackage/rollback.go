package attachedworkerpackage

import (
	"context"
	"encoding/json"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

// RollbackPlanV1 targets only the immediately preceding, exact staged unit.
// It never changes an OS service registration and never decrements the
// monotonic install revision. The target executable/image must still be
// available and match the archived receipt and current enrollment manifest.
type RollbackPlanV1 struct {
	Version                 uint32 `json:"version"`
	Mode                    Mode   `json:"mode"`
	OwnerUserID             string `json:"owner_user_id"`
	WorkerID                string `json:"worker_id"`
	ManifestRevision        uint64 `json:"manifest_revision"`
	ExpectedInstallRevision uint64 `json:"expected_install_revision"`
	NextInstallRevision     uint64 `json:"next_install_revision"`
	UnitPath                string `json:"unit_path"`
	CurrentUnitSHA256       string `json:"current_unit_sha256"`
	TargetUnitSHA256        string `json:"target_unit_sha256"`
	TargetReceiptSHA256     string `json:"target_receipt_sha256"`
	BinarySHA256            string `json:"binary_sha256"`
	ContainerImage          string `json:"container_image,omitempty"`
	PlanSHA256              string `json:"plan_sha256"`
}

// RollbackPlan is read-only and fails closed for legacy or missing archives,
// changed enrollment revisions, or missing/tampered target binaries.
func RollbackPlan(ctx context.Context, config Config, expectedInstallRevision uint64) (RollbackPlanV1, error) {
	if ctx == nil || ctx.Err() != nil || validateConfig(config) != nil || expectedInstallRevision == 0 ||
		expectedInstallRevision == ^uint64(0) {
		return RollbackPlanV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return RollbackPlanV1{}, ErrInvalid
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return RollbackPlanV1{}, errors.Join(ErrInvalid, err)
	}
	manifest := snapshot.Manifest
	if err := verifyBinary(config.BinaryPath, config.BinarySHA256); err != nil {
		return RollbackPlanV1{}, err
	}
	unitPath, receiptPath := paths(config, manifest)
	if err := checkNoNativePending(unitPath); err != nil {
		return RollbackPlanV1{}, err
	}
	registration, err := readNativeReceipt(unitPath)
	if err != nil || registration.Action == NativeRegister {
		return RollbackPlanV1{}, errors.Join(ErrConflict, err)
	}
	current, _, err := readPrevious(config.InstallDir, unitPath, receiptPath, manifest)
	if err != nil || current.InstallRevision != expectedInstallRevision || current.Mode != config.Mode ||
		current.ManifestRevision != manifest.Revision || current.RollbackSHA256 == "" ||
		current.RollbackReceiptSHA256 == "" {
		return RollbackPlanV1{}, errors.Join(ErrConflict, err)
	}
	targetUnit, target, err := loadArchivedBundle(unitPath, current.RollbackSHA256, current.RollbackReceiptSHA256)
	if err != nil {
		return RollbackPlanV1{}, err
	}
	if target.OwnerUserID != string(manifest.OwnerUserID) || target.WorkerID != string(manifest.WorkerID) ||
		target.Mode != config.Mode || target.ManifestRevision != manifest.Revision ||
		target.BinaryPath != config.BinaryPath || target.BinarySHA256 != config.BinarySHA256 ||
		target.ContainerImage != config.ContainerImage || target.InstallRevision >= current.InstallRevision {
		return RollbackPlanV1{}, ErrConflict
	}
	rendered, err := render(config, manifest)
	if err != nil || digest(rendered) != digest(targetUnit) {
		return RollbackPlanV1{}, ErrConflict
	}
	plan := RollbackPlanV1{
		Version: VersionV1, Mode: config.Mode, OwnerUserID: string(manifest.OwnerUserID),
		WorkerID: string(manifest.WorkerID), ManifestRevision: manifest.Revision,
		ExpectedInstallRevision: expectedInstallRevision, NextInstallRevision: expectedInstallRevision + 1,
		UnitPath: unitPath, CurrentUnitSHA256: current.UnitSHA256,
		TargetUnitSHA256: current.RollbackSHA256, TargetReceiptSHA256: current.RollbackReceiptSHA256,
		BinarySHA256: config.BinarySHA256, ContainerImage: config.ContainerImage,
	}
	plan.PlanSHA256 = rollbackPlanDigest(plan)
	return plan, nil
}

// ApplyRollback stages the archived exact unit as a new monotonic revision.
// It does not load, restart, enable, or claim the health of an OS service.
func ApplyRollback(ctx context.Context, config Config, plan RollbackPlanV1) (result ReceiptV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || plan.Version != VersionV1 ||
		plan.PlanSHA256 == "" || plan.PlanSHA256 != rollbackPlanDigest(plan) {
		return ReceiptV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return ReceiptV1{}, ErrInvalid
	}
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return ReceiptV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	currentPlan, err := RollbackPlan(ctx, config, plan.ExpectedInstallRevision)
	if err != nil || currentPlan != plan {
		return ReceiptV1{}, errors.Join(ErrConflict, err)
	}
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Revision != plan.ManifestRevision {
		return ReceiptV1{}, errors.Join(ErrConflict, err)
	}
	unitPath, receiptPath := paths(config, snapshot.Manifest)
	current, currentUnit, err := readPrevious(config.InstallDir, unitPath, receiptPath, snapshot.Manifest)
	if err != nil || current.UnitSHA256 != plan.CurrentUnitSHA256 {
		return ReceiptV1{}, errors.Join(ErrConflict, err)
	}
	targetUnit, _, err := loadArchivedBundle(unitPath, plan.TargetUnitSHA256, plan.TargetReceiptSHA256)
	if err != nil {
		return ReceiptV1{}, err
	}
	receipt := ReceiptV1{
		Version: VersionV1, InstallRevision: plan.NextInstallRevision,
		ManifestRevision: plan.ManifestRevision, OwnerUserID: plan.OwnerUserID,
		WorkerID: plan.WorkerID, Mode: plan.Mode, UnitSHA256: plan.TargetUnitSHA256,
		BinaryPath: config.BinaryPath, BinarySHA256: plan.BinarySHA256,
		ContainerImage: plan.ContainerImage, PlanSHA256: plan.PlanSHA256,
		RollbackSHA256:        plan.CurrentUnitSHA256,
		RollbackReceiptSHA256: digest(encodeReceipt(current)), Registration: "not_attempted",
	}
	if err := archiveBundle(unitPath, currentUnit, current); err != nil {
		return ReceiptV1{}, err
	}
	if err := archiveBundle(unitPath, targetUnit, receipt); err != nil {
		return ReceiptV1{}, err
	}
	if err := writeAtomic(unitPath, targetUnit); err != nil {
		return ReceiptV1{}, errors.Join(ErrAmbiguous, err)
	}
	if err := writeAtomic(receiptPath, encodeReceipt(receipt)); err != nil {
		if errors.Is(err, ErrAmbiguous) {
			return ReceiptV1{}, err
		}
		return ReceiptV1{}, errors.Join(ErrAmbiguous, err, rollback(unitPath, currentUnit))
	}
	return receipt, nil
}

func rollbackPlanDigest(plan RollbackPlanV1) string {
	plan.PlanSHA256 = ""
	encoded, _ := json.Marshal(plan)
	return digest(encoded)
}
