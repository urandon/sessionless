package registrygc

import (
	"context"
	"errors"
	"fmt"
)

var ErrImageNotFound = errors.New("registry image not found")

type Cloud interface {
	Discover(context.Context, Inventory) (LiveState, error)
	GetImage(context.Context, string) (CloudImage, error)
	DeleteImage(context.Context, string) error
}

func Run(
	ctx context.Context,
	config PlanConfig,
	inventory Inventory,
	manifests []DeploymentManifest,
	protected ProtectedDigests,
	cloud Cloud,
) (Report, error) {
	if cloud == nil {
		return Report{}, errors.New("cloud adapter is required")
	}
	if err := validateConfig(config); err != nil {
		return Report{}, err
	}
	if err := validateInventory(inventory, config.Mode); err != nil {
		return Report{}, fmt.Errorf("Terraform inventory: %w", err)
	}
	if err := validateManifests(inventory, manifests); err != nil {
		return Report{}, fmt.Errorf("deployment manifests: %w", err)
	}
	protected = normalizeProtected(inventory, protected)
	if err := validateProtected(inventory, protected); err != nil {
		return Report{}, fmt.Errorf("protected digests: %w", err)
	}
	live, err := cloud.Discover(ctx, inventory)
	if err != nil {
		return Report{}, fmt.Errorf("discover live cloud state: %w", err)
	}
	report, err := BuildPlan(config, inventory, live, manifests, protected)
	if err != nil {
		return Report{}, err
	}
	if config.Mode == ModeDryRun {
		return report, nil
	}
	for index := range report.Decisions {
		decision := &report.Decisions[index]
		if decision.Decision != DecisionDelete {
			continue
		}
		image, getErr := cloud.GetImage(ctx, decision.ImageID)
		if errors.Is(getErr, ErrImageNotFound) {
			decision.Execution = "already_absent"
			report.Summary.AlreadyAbsent++
			continue
		}
		if getErr != nil {
			report.Complete = false
			report.Status = "partial_failure"
			return report, fmt.Errorf("recheck image %s: %w", decision.ImageID, getErr)
		}
		if image.ID != decision.ImageID || image.Name != decision.Repository ||
			image.Digest != decision.Digest || !image.CreatedAt.Equal(decision.CreatedAt) {
			report.Complete = false
			report.Status = "partial_failure"
			return report, fmt.Errorf("image %s changed after the immutable cleanup plan", decision.ImageID)
		}
		if deleteErr := cloud.DeleteImage(ctx, decision.ImageID); errors.Is(deleteErr, ErrImageNotFound) {
			decision.Execution = "already_absent"
			report.Summary.AlreadyAbsent++
			continue
		} else if deleteErr != nil {
			report.Complete = false
			report.Status = "partial_failure"
			return report, fmt.Errorf("delete image %s: %w", decision.ImageID, deleteErr)
		}
		decision.Execution = "deleted"
		report.Summary.Deleted++
	}
	return report, nil
}
