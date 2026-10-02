package attachedworkerpackage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

type NativeReconciliationV1 struct {
	Version uint32           `json:"version"`
	Status  string           `json:"status"`
	Receipt *NativeReceiptV1 `json:"receipt,omitempty"`
}

// ReconcileNative resolves one durable pending operation after an ambiguous
// result. It does not retry a service-manager command. Unknown OS state or
// changed local inputs leave the marker in place for operator intervention.
func ReconcileNative(ctx context.Context, config Config, expectedPlanSHA256 string) (NativeReconciliationV1, error) {
	return reconcileNative(ctx, config, expectedPlanSHA256, osNativeManager{})
}

func reconcileNative(ctx context.Context, config Config, expectedPlanSHA256 string, manager nativeManager) (result NativeReconciliationV1, resultErr error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || validateConfig(config) != nil ||
		(config.Mode != ModeLaunchd && config.Mode != ModeSystemdUser) || len(expectedPlanSHA256) != 64 {
		return NativeReconciliationV1{}, ErrInvalid
	}
	store, err := attachedworkerlocal.NewStore(config.StateRoot, nil)
	if err != nil {
		return NativeReconciliationV1{}, ErrInvalid
	}
	operation, err := acquireOperationLease(config.InstallDir)
	if err != nil {
		return NativeReconciliationV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, operation.Close()) }()
	lease, err := store.AcquireRuntime(ctx)
	if err != nil {
		return NativeReconciliationV1{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, lease.Close()) }()
	snapshot, err := store.LoadSnapshot(ctx)
	if err != nil || snapshot.Manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		return NativeReconciliationV1{}, errors.Join(ErrConflict, err)
	}
	manifest := snapshot.Manifest
	if err := verifyBinary(config.BinaryPath, config.BinarySHA256); err != nil {
		return NativeReconciliationV1{}, err
	}
	unitPath, receiptPath := paths(config, manifest)
	staged, _, err := readPrevious(config.InstallDir, unitPath, receiptPath, manifest)
	if err != nil {
		return NativeReconciliationV1{}, err
	}
	encoded, err := readRegular(nativePendingPath(unitPath))
	if err != nil {
		return NativeReconciliationV1{}, errors.Join(ErrConflict, err)
	}
	var plan NativePlanV1
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&plan) != nil || decoder.Decode(new(any)) != io.EOF ||
		!bytes.Equal(encoded, encodeNativePlan(plan)) || plan.Version != VersionV1 ||
		(plan.Action != NativeRegister && plan.Action != NativeUnregister) ||
		plan.PlanSHA256 != expectedPlanSHA256 || plan.PlanSHA256 != nativePlanDigest(plan) ||
		plan.Mode != config.Mode || plan.OwnerUserID != string(manifest.OwnerUserID) ||
		plan.WorkerID != string(manifest.WorkerID) || plan.ManifestRevision != manifest.Revision ||
		plan.InstallRevision != staged.InstallRevision || plan.UnitPath != unitPath ||
		plan.UnitSHA256 != staged.UnitSHA256 || plan.BinarySHA256 != config.BinarySHA256 ||
		plan.NextRegistrationRevision == 0 || plan.ExpectedRegistrationRevision == ^uint64(0) ||
		plan.NextRegistrationRevision != plan.ExpectedRegistrationRevision+1 {
		return NativeReconciliationV1{}, ErrConflict
	}
	prior, err := readNativeReceipt(unitPath)
	if err != nil {
		return NativeReconciliationV1{}, err
	}
	if prior.RegistrationRevision != plan.ExpectedRegistrationRevision &&
		prior.RegistrationRevision != plan.NextRegistrationRevision {
		return NativeReconciliationV1{}, ErrConflict
	}
	state, err := manager.inspect(ctx, plan.Mode, nativeName(manifest), unitPath)
	if err != nil {
		return NativeReconciliationV1{}, errors.Join(ErrIO, err)
	}
	completed := false
	switch plan.Action {
	case NativeRegister:
		if state.loaded {
			if state.active || state.path != unitPath {
				return NativeReconciliationV1{}, ErrConflict
			}
			completed = true
		}
	case NativeUnregister:
		if !state.loaded {
			completed = true
		} else if state.active || state.path != unitPath {
			return NativeReconciliationV1{}, ErrConflict
		}
	}
	if !completed {
		if prior.RegistrationRevision != plan.ExpectedRegistrationRevision {
			return NativeReconciliationV1{}, ErrConflict
		}
		if err := removeNativePending(unitPath); err != nil {
			return NativeReconciliationV1{}, errors.Join(ErrAmbiguous, err)
		}
		return NativeReconciliationV1{Version: VersionV1, Status: "not_applied"}, nil
	}
	receipt := NativeReceiptV1{Version: VersionV1, RegistrationRevision: plan.NextRegistrationRevision,
		Action: plan.Action, Mode: plan.Mode, OwnerUserID: plan.OwnerUserID, WorkerID: plan.WorkerID,
		ManifestRevision: plan.ManifestRevision, InstallRevision: plan.InstallRevision,
		UnitSHA256: plan.UnitSHA256, PlanSHA256: plan.PlanSHA256}
	if prior.RegistrationRevision == plan.ExpectedRegistrationRevision {
		if err := writeAtomic(nativeReceiptPath(unitPath), encodeNativeReceipt(receipt)); err != nil {
			return NativeReconciliationV1{}, errors.Join(ErrAmbiguous, err)
		}
	} else if prior != receipt {
		return NativeReconciliationV1{}, ErrConflict
	}
	if err := removeNativePending(unitPath); err != nil {
		return NativeReconciliationV1{}, errors.Join(ErrAmbiguous, err)
	}
	return NativeReconciliationV1{Version: VersionV1, Status: "completed", Receipt: &receipt}, nil
}
