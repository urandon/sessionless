package registrygc

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	shaPattern    = regexp.MustCompile(`^[0-9a-f]{40}$`)
	idPattern     = regexp.MustCompile(`^[a-z0-9]+$`)
)

func BuildPlan(
	config PlanConfig,
	inventory Inventory,
	live LiveState,
	manifests []DeploymentManifest,
	protected ProtectedDigests,
) (Report, error) {
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

	keep := make(map[string]map[string][]Reason, len(inventory.Repositories))
	for _, component := range RequiredComponents {
		keep[component] = make(map[string][]Reason)
	}
	if err := addActiveRevisionKeepSet(inventory, live, manifests, keep); err != nil {
		return Report{}, fmt.Errorf("live deployment evidence: %w", err)
	}
	for _, manifest := range manifests {
		for _, component := range RequiredComponents {
			addReason(keep, component, manifest.Images[component].ManifestDigest,
				Reason{Kind: "deployment_manifest", Reference: manifest.Source.SHA})
		}
	}
	for component, digests := range protected.Digests {
		for _, digest := range digests {
			addReason(keep, component, digest,
				Reason{Kind: "explicit_protection", Reference: "protected-digests"})
		}
	}

	report := Report{
		SchemaVersion:       SchemaVersion,
		GeneratedAt:         config.Now.UTC(),
		Mode:                config.Mode,
		Environment:         inventory.Environment,
		FolderID:            inventory.FolderID,
		RegistryID:          inventory.RegistryID,
		TerraformLineage:    inventory.Terraform.StateLineage,
		TerraformSerial:     inventory.Terraform.StateSerial,
		TerraformDigest:     inventory.Terraform.OutputsDigest,
		SafetyWindowSeconds: int64(config.SafetyWindow / time.Second),
		Complete:            true,
		Status:              "complete",
		Source:              config.Source,
	}

	seenImageIDs := make(map[string]struct{})
	for _, component := range RequiredComponents {
		repository, ok := live.Repositories[component]
		if !ok {
			return Report{}, fmt.Errorf("live registry evidence is missing repository %q", component)
		}
		tfRepository := inventory.Repositories[component]
		if repository.ID != tfRepository.ID || repository.Name != tfRepository.Name {
			return Report{}, fmt.Errorf("repository %q identity disagrees with Terraform", component)
		}
		seenDigests := make(map[string]struct{})
		for _, image := range repository.Images {
			if err := validateRegistryImage(config.Now, component, image); err != nil {
				return Report{}, err
			}
			if _, duplicate := seenImageIDs[image.ID]; duplicate {
				return Report{}, fmt.Errorf("registry image id %q appears more than once", image.ID)
			}
			seenImageIDs[image.ID] = struct{}{}
			if _, duplicate := seenDigests[image.Digest]; duplicate {
				return Report{}, fmt.Errorf("repository %q contains duplicate digest %s", component, image.Digest)
			}
			seenDigests[image.Digest] = struct{}{}

			reasons := append([]Reason(nil), keep[component][image.Digest]...)
			if config.Now.Sub(image.CreatedAt) <= config.SafetyWindow {
				reasons = appendReason(reasons, Reason{
					Kind:      "safety_window",
					Reference: config.SafetyWindow.String(),
				})
			}
			sort.Slice(reasons, func(i, j int) bool {
				if reasons[i].Kind != reasons[j].Kind {
					return reasons[i].Kind < reasons[j].Kind
				}
				return reasons[i].Reference < reasons[j].Reference
			})
			decision := DecisionDelete
			execution := "dry_run"
			if config.Mode == ModeDelete {
				execution = "pending"
			}
			if len(reasons) > 0 {
				decision = DecisionRetain
				execution = "not_applicable"
				report.Summary.Retained++
			} else {
				report.Summary.DeleteCandidates++
				report.Summary.EstimatedReclaimBytes += image.CompressedSize
			}
			tags := append([]string(nil), image.Tags...)
			sort.Strings(tags)
			report.Decisions = append(report.Decisions, Decision{
				Component: component, Repository: repository.Name,
				ImageID: image.ID, Digest: image.Digest, CreatedAt: image.CreatedAt.UTC(),
				AgeSeconds:     int64(config.Now.Sub(image.CreatedAt) / time.Second),
				CompressedSize: image.CompressedSize, Tags: tags,
				Decision: decision, Reasons: reasons, Execution: execution,
			})
		}
		for digest := range keep[component] {
			if _, exists := seenDigests[digest]; !exists {
				return Report{}, fmt.Errorf("protected digest %s is absent from repository %q", digest, component)
			}
		}
	}
	if len(live.Repositories) != len(RequiredComponents) {
		return Report{}, fmt.Errorf("live registry returned %d repositories, expected %d", len(live.Repositories), len(RequiredComponents))
	}
	sort.Slice(report.Decisions, func(i, j int) bool {
		if report.Decisions[i].Component != report.Decisions[j].Component {
			return report.Decisions[i].Component < report.Decisions[j].Component
		}
		if !report.Decisions[i].CreatedAt.Equal(report.Decisions[j].CreatedAt) {
			return report.Decisions[i].CreatedAt.Before(report.Decisions[j].CreatedAt)
		}
		return report.Decisions[i].Digest < report.Decisions[j].Digest
	})
	report.Summary.Repositories = len(RequiredComponents)
	report.Summary.Images = len(report.Decisions)
	return report, nil
}

func normalizeProtected(inventory Inventory, protected ProtectedDigests) ProtectedDigests {
	if protected.Digests == nil {
		return ProtectedDigests{
			SchemaVersion: SchemaVersion, Environment: inventory.Environment,
			RegistryID: inventory.RegistryID, Digests: map[string][]string{},
		}
	}
	return protected
}

func validateConfig(config PlanConfig) error {
	if config.Mode != ModeDryRun && config.Mode != ModeDelete {
		return fmt.Errorf("unsupported mode %q", config.Mode)
	}
	if config.Now.IsZero() {
		return fmt.Errorf("current time is required")
	}
	if config.SafetyWindow <= 0 {
		return fmt.Errorf("safety window must be positive")
	}
	return nil
}

func validateInventory(inventory Inventory, mode string) error {
	if inventory.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema_version %d", inventory.SchemaVersion)
	}
	if inventory.Environment == "" || inventory.LockEnvironment != inventory.Environment {
		return fmt.Errorf("environment and lock_environment must be identical and non-empty")
	}
	if !idPattern.MatchString(inventory.FolderID) || !idPattern.MatchString(inventory.RegistryID) {
		return fmt.Errorf("folder_id and registry_id must be lowercase alphanumeric identifiers")
	}
	if inventory.StableSlot != "blue" && inventory.StableSlot != "green" {
		return fmt.Errorf("stable_slot must be blue or green")
	}
	if inventory.CandidateSlot != "blue" && inventory.CandidateSlot != "green" {
		return fmt.Errorf("candidate_slot must be blue or green")
	}
	if inventory.StableSlot == inventory.CandidateSlot {
		return fmt.Errorf("stable_slot and candidate_slot must differ")
	}
	if err := requireExactKeys(inventory.LifecyclePolicyStatus, RequiredComponents, "lifecycle_policy_status"); err != nil {
		return err
	}
	if mode == ModeDelete {
		for component, policyStatus := range inventory.LifecyclePolicyStatus {
			if !strings.EqualFold(policyStatus, "disabled") {
				return fmt.Errorf("live deletion requires disabled native lifecycle policy for %q, got %q", component, policyStatus)
			}
		}
	}
	if inventory.Terraform.StateLineage == "" || inventory.Terraform.StateSerial == 0 ||
		!digestPattern.MatchString(inventory.Terraform.OutputsDigest) {
		return fmt.Errorf("Terraform lineage, positive serial, and SHA-256 outputs digest are required")
	}
	if err := requireExactKeys(inventory.Repositories, RequiredComponents, "repositories"); err != nil {
		return err
	}
	for _, component := range RequiredComponents {
		repository := inventory.Repositories[component]
		if !idPattern.MatchString(repository.ID) || repository.Name != inventory.RegistryID+"/"+component {
			return fmt.Errorf("repository %q has invalid Terraform identity", component)
		}
	}
	if len(inventory.Containers) != 6 {
		return fmt.Errorf("containers must contain the six managed runtime containers, got %d", len(inventory.Containers))
	}
	seenIDs := make(map[string]struct{})
	seenRevisions := make(map[string]struct{})
	componentCounts := make(map[string]int)
	controlSlots := make(map[string]bool)
	for name, container := range inventory.Containers {
		if name == "" || !contains(RequiredComponents, container.Component) || container.Repository != container.Component {
			return fmt.Errorf("container %q has invalid component/repository", name)
		}
		if !idPattern.MatchString(container.ContainerID) || !idPattern.MatchString(container.RevisionID) || !shaPattern.MatchString(container.SourceSHA) {
			return fmt.Errorf("container %q has invalid IDs or source SHA", name)
		}
		if _, duplicate := seenIDs[container.ContainerID]; duplicate {
			return fmt.Errorf("container id %q is duplicated", container.ContainerID)
		}
		seenIDs[container.ContainerID] = struct{}{}
		if _, duplicate := seenRevisions[container.RevisionID]; duplicate {
			return fmt.Errorf("revision id %q is duplicated", container.RevisionID)
		}
		seenRevisions[container.RevisionID] = struct{}{}
		digest, err := parseImageReference(container.ImageRef, inventory.RegistryID, container.Component)
		if err != nil || digest == "" {
			return fmt.Errorf("container %q image_ref: %w", name, err)
		}
		componentCounts[container.Component]++
		if container.Component == "control-api" {
			if container.Slot != "blue" && container.Slot != "green" {
				return fmt.Errorf("control container %q has invalid slot %q", name, container.Slot)
			}
			controlSlots[container.Slot] = true
		} else if container.Slot == "" {
			return fmt.Errorf("container %q slot/role is empty", name)
		}
	}
	for _, component := range RequiredComponents {
		expected := 1
		if component == "control-api" {
			expected = 2
		}
		if componentCounts[component] != expected {
			return fmt.Errorf("component %q has %d containers, expected %d", component, componentCounts[component], expected)
		}
	}
	if !controlSlots["blue"] || !controlSlots["green"] {
		return fmt.Errorf("control blue and green slots are both required")
	}
	return nil
}

func validateManifests(inventory Inventory, manifests []DeploymentManifest) error {
	if len(manifests) != 3 {
		return fmt.Errorf("exactly three manifests are required, got %d", len(manifests))
	}
	seenSHAs := make(map[string]struct{})
	for index, manifest := range manifests {
		if manifest.SchemaVersion != ManifestSchemaVersion || manifest.Build.Platform != "linux/amd64" {
			return fmt.Errorf("manifest %d has unsupported schema or platform", index+1)
		}
		if manifest.Source.Repository != "gitcode.com/urandon/sessionless" {
			return fmt.Errorf("manifest %d has unexpected source repository", index+1)
		}
		if !shaPattern.MatchString(manifest.Source.SHA) || !shaPattern.MatchString(manifest.Source.Tree) {
			return fmt.Errorf("manifest %d has invalid source identity", index+1)
		}
		if _, duplicate := seenSHAs[manifest.Source.SHA]; duplicate {
			return fmt.Errorf("source SHA %s appears in more than one manifest", manifest.Source.SHA)
		}
		seenSHAs[manifest.Source.SHA] = struct{}{}
		committedAt, err := time.Parse(time.RFC3339, manifest.Source.CommittedAt)
		if err != nil {
			return fmt.Errorf("manifest %d committed_at: %w", index+1, err)
		}
		epoch, err := strconv.ParseInt(manifest.Source.SourceDateEpoch, 10, 64)
		if err != nil || epoch <= 0 || committedAt.Unix() != epoch {
			return fmt.Errorf("manifest %d has invalid source_date_epoch", index+1)
		}
		if !digestPattern.MatchString(manifest.Build.ContractDigest) ||
			len(manifest.Build.InputDigests) != len(RequiredComponents) {
			return fmt.Errorf("manifest %d has invalid build contract", index+1)
		}
		if err := requireExactKeys(manifest.Images, RequiredComponents, "manifest images"); err != nil {
			return fmt.Errorf("manifest %d: %w", index+1, err)
		}
		if err := requireExactKeys(manifest.Build.InputDigests, RequiredComponents, "manifest input digests"); err != nil {
			return fmt.Errorf("manifest %d: %w", index+1, err)
		}
		for _, component := range RequiredComponents {
			image := manifest.Images[component]
			if !digestPattern.MatchString(image.ManifestDigest) || !digestPattern.MatchString(image.ConfigDigest) ||
				!digestPattern.MatchString(image.InputDigest) || image.InputDigest != manifest.Build.InputDigests[component] {
				return fmt.Errorf("manifest %d image %q has invalid digest contract", index+1, component)
			}
			digest, err := parseImageReference(image.Reference, inventory.RegistryID, component)
			if err != nil || digest != image.ManifestDigest {
				return fmt.Errorf("manifest %d image %q reference/digest mismatch", index+1, component)
			}
			expectedTag := "cr.yandex/" + inventory.RegistryID + "/" + component + ":" + manifest.Source.SHA
			if image.TaggedReference != expectedTag {
				return fmt.Errorf("manifest %d image %q has unexpected tagged reference", index+1, component)
			}
		}
	}
	return nil
}

func validateProtected(inventory Inventory, protected ProtectedDigests) error {
	if protected.SchemaVersion != SchemaVersion || protected.Environment != inventory.Environment ||
		protected.RegistryID != inventory.RegistryID {
		return fmt.Errorf("schema, environment, or registry does not match inventory")
	}
	for component, digests := range protected.Digests {
		if _, exists := inventory.Repositories[component]; !exists {
			return fmt.Errorf("unknown repository %q", component)
		}
		seen := make(map[string]struct{})
		for _, digest := range digests {
			if !digestPattern.MatchString(digest) {
				return fmt.Errorf("repository %q contains invalid digest %q", component, digest)
			}
			if _, duplicate := seen[digest]; duplicate {
				return fmt.Errorf("repository %q repeats digest %s", component, digest)
			}
			seen[digest] = struct{}{}
		}
	}
	return nil
}

func addActiveRevisionKeepSet(
	inventory Inventory,
	live LiveState,
	manifests []DeploymentManifest,
	keep map[string]map[string][]Reason,
) error {
	if len(live.Containers) != len(inventory.Containers) {
		return fmt.Errorf("live API returned %d managed containers, Terraform has %d", len(live.Containers), len(inventory.Containers))
	}
	byID := make(map[string]LiveContainer, len(live.Containers))
	for _, container := range live.Containers {
		if _, duplicate := byID[container.ID]; duplicate {
			return fmt.Errorf("live container id %q is duplicated", container.ID)
		}
		byID[container.ID] = container
	}
	_ = manifests
	for logicalName, expected := range inventory.Containers {
		container, exists := byID[expected.ContainerID]
		if !exists {
			return fmt.Errorf("Terraform container %q (%s) is absent live", logicalName, expected.ContainerID)
		}
		for key, value := range map[string]string{
			"application": "sessionless", "managed-by": "terraform",
			"component": expected.Component, "slot": expected.Slot,
			"source-commit": expected.SourceSHA,
		} {
			if container.Labels[key] != value {
				return fmt.Errorf("container %q ownership label %s is %q, expected %q", logicalName, key, container.Labels[key], value)
			}
		}
		var active *LiveRevision
		for index := range container.Revisions {
			revision := &container.Revisions[index]
			switch revision.Status {
			case "ACTIVE":
				if active != nil {
					return fmt.Errorf("container %q has more than one ACTIVE revision", logicalName)
				}
				active = revision
			case "OBSOLETE":
			case "CREATING":
				return fmt.Errorf("container %q has a CREATING revision while deployment lock is held", logicalName)
			default:
				return fmt.Errorf("container %q has unknown revision status %q", logicalName, revision.Status)
			}
		}
		if active == nil || active.ID != expected.RevisionID || active.ImageURL != expected.ImageRef {
			return fmt.Errorf("container %q active revision disagrees with Terraform", logicalName)
		}
		digest, err := parseImageReference(active.ImageURL, inventory.RegistryID, expected.Component)
		if err != nil || digest != active.ImageDigest {
			return fmt.Errorf("container %q active image URL/digest mismatch", logicalName)
		}
		role := expected.Slot
		if expected.Component == "control-api" {
			if expected.Slot == inventory.StableSlot {
				role = "stable/" + expected.Slot
			} else if expected.Slot == inventory.CandidateSlot {
				role = "candidate/" + expected.Slot
			}
		}
		addReason(keep, expected.Component, digest,
			Reason{Kind: "active_revision", Reference: logicalName + ":" + role + ":" + active.ID})
	}
	return nil
}

func validateRegistryImage(now time.Time, component string, image RegistryImage) error {
	if !idPattern.MatchString(image.ID) || !digestPattern.MatchString(image.Digest) {
		return fmt.Errorf("repository %q has an image with invalid id or digest", component)
	}
	if image.CreatedAt.IsZero() || image.CreatedAt.After(now) {
		return fmt.Errorf("repository %q image %s has an invalid creation time", component, image.ID)
	}
	if image.CompressedSize < 0 {
		return fmt.Errorf("repository %q image %s has a negative compressed size", component, image.ID)
	}
	return nil
}

func parseImageReference(reference, registryID, component string) (string, error) {
	prefix := "cr.yandex/" + registryID + "/" + component + "@"
	if !strings.HasPrefix(reference, prefix) {
		return "", fmt.Errorf("reference must start with %q", prefix)
	}
	digest := strings.TrimPrefix(reference, prefix)
	if !digestPattern.MatchString(digest) {
		return "", fmt.Errorf("reference has invalid digest")
	}
	return digest, nil
}

func addReason(keep map[string]map[string][]Reason, component, digest string, reason Reason) {
	keep[component][digest] = appendReason(keep[component][digest], reason)
}

func appendReason(reasons []Reason, reason Reason) []Reason {
	for _, existing := range reasons {
		if existing == reason {
			return reasons
		}
	}
	return append(reasons, reason)
}

func requireExactKeys[T any](values map[string]T, expected []string, field string) error {
	if len(values) != len(expected) {
		return fmt.Errorf("%s has %d entries, expected %d", field, len(values), len(expected))
	}
	for _, key := range expected {
		if _, exists := values[key]; !exists {
			return fmt.Errorf("%s is missing %q", field, key)
		}
	}
	return nil
}

func contains(values []string, candidate string) bool {
	for _, value := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
