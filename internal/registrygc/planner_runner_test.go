package registrygc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const testRegistryID = "crptestregistry"

type gcFixture struct {
	now       time.Time
	config    PlanConfig
	inventory Inventory
	live      LiveState
	manifests []DeploymentManifest
	protected ProtectedDigests
	orphans   map[string]RegistryImage
}

func newGCFixture(t *testing.T, mode string) gcFixture {
	t.Helper()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	f := gcFixture{
		now:    now,
		config: PlanConfig{Now: now, SafetyWindow: 48 * time.Hour, Mode: mode},
		inventory: Inventory{
			SchemaVersion:         SchemaVersion,
			Environment:           "cloud-dev",
			FolderID:              "folder123",
			RegistryID:            testRegistryID,
			Repositories:          make(map[string]TerraformRepository),
			Containers:            make(map[string]TerraformContainer),
			StableSlot:            "blue",
			CandidateSlot:         "green",
			LockEnvironment:       "cloud-dev",
			LifecyclePolicyStatus: map[string]string{},
			Terraform: TerraformStateEvidence{
				StateLineage: "lineage-1", StateSerial: 7, OutputsDigest: digest(240),
			},
		},
		live: LiveState{
			Containers:   make(map[string]LiveContainer),
			Repositories: make(map[string]LiveRepository),
		},
		protected: ProtectedDigests{
			SchemaVersion: SchemaVersion, Environment: "cloud-dev", RegistryID: testRegistryID,
			Digests: map[string][]string{},
		},
		orphans: make(map[string]RegistryImage),
	}

	active := map[string][]string{}
	for componentIndex, component := range RequiredComponents {
		f.inventory.Repositories[component] = TerraformRepository{
			ID: "repo" + fmt.Sprint(componentIndex+1), Name: testRegistryID + "/" + component,
		}
		f.inventory.LifecyclePolicyStatus[component] = "disabled"
		active[component] = []string{digest(10 + componentIndex)}
	}
	active["control-api"] = []string{digest(10), digest(11)}

	containerSpecs := []struct {
		name, id, revisionID, component, slot string
		digest                                string
	}{
		{"control-blue", "container1", "revision1", "control-api", "blue", active["control-api"][0]},
		{"control-green", "container2", "revision2", "control-api", "green", active["control-api"][1]},
		{"reconciler", "container3", "revision3", "reconciler", "singleton", active["reconciler"][0]},
		{"telegram-sender", "container4", "revision4", "telegram-sender", "singleton", active["telegram-sender"][0]},
		{"web-bff", "container5", "revision5", "web-bff", "singleton", active["web-bff"][0]},
		{"worker-runtime", "container6", "revision6", "worker-runtime", "singleton", active["worker-runtime"][0]},
	}
	for index, spec := range containerSpecs {
		sha := sha(20 + index)
		imageRef := imageReference(spec.component, spec.digest)
		f.inventory.Containers[spec.name] = TerraformContainer{
			ContainerID: spec.id, RevisionID: spec.revisionID, Component: spec.component,
			Repository: spec.component, Slot: spec.slot, SourceSHA: sha, ImageRef: imageRef,
		}
		f.live.Containers[spec.id] = LiveContainer{
			ID: spec.id, Name: spec.name,
			Labels: map[string]string{
				"application": "sessionless", "managed-by": "terraform", "component": spec.component,
				"slot": spec.slot, "source-commit": sha,
			},
			Revisions: []LiveRevision{{
				ID: spec.revisionID, Status: "ACTIVE", CreatedAt: now.Add(-72 * time.Hour),
				ImageURL: imageRef, ImageDigest: spec.digest,
			}},
		}
	}

	for manifestIndex := 0; manifestIndex < 3; manifestIndex++ {
		manifest := DeploymentManifest{
			SchemaVersion: ManifestSchemaVersion,
			Source: ManifestSource{
				Repository: "gitcode.com/urandon/sessionless", SHA: sha(60 + manifestIndex), Tree: sha(70 + manifestIndex),
				CommittedAt:     now.Add(-time.Duration(manifestIndex+1) * 24 * time.Hour).Format(time.RFC3339),
				SourceDateEpoch: fmt.Sprint(now.Add(-time.Duration(manifestIndex+1) * 24 * time.Hour).Unix()),
			},
			Build: ManifestBuild{
				Platform: "linux/amd64", ContractDigest: digest(80 + manifestIndex),
				InputDigests: make(map[string]string),
			},
			Images: make(map[string]ManifestImage),
		}
		for componentIndex, component := range RequiredComponents {
			manifestDigest := digest(100 + manifestIndex*10 + componentIndex)
			inputDigest := digest(140 + manifestIndex*10 + componentIndex)
			manifest.Build.InputDigests[component] = inputDigest
			manifest.Images[component] = ManifestImage{
				TaggedReference: "cr.yandex/" + testRegistryID + "/" + component + ":" + manifest.Source.SHA,
				Reference:       imageReference(component, manifestDigest), ManifestDigest: manifestDigest,
				ConfigDigest: digest(180 + manifestIndex*10 + componentIndex), InputDigest: inputDigest,
			}
		}
		f.manifests = append(f.manifests, manifest)
	}

	for componentIndex, component := range RequiredComponents {
		images := make([]RegistryImage, 0, 8)
		for activeIndex, activeDigest := range active[component] {
			images = append(images, registryImage(componentIndex*100+activeIndex+1, activeDigest, now.Add(-96*time.Hour)))
		}
		for manifestIndex := range f.manifests {
			images = append(images, registryImage(componentIndex*100+manifestIndex+10,
				f.manifests[manifestIndex].Images[component].ManifestDigest, now.Add(-96*time.Hour)))
		}
		young := registryImage(componentIndex*100+20, digest(210+componentIndex), now.Add(-47*time.Hour))
		images = append(images, young)
		orphan := registryImage(componentIndex*100+30, digest(220+componentIndex), now.Add(-49*time.Hour))
		images = append(images, orphan)
		f.orphans[component] = orphan
		f.live.Repositories[component] = LiveRepository{
			ID: f.inventory.Repositories[component].ID, Name: f.inventory.Repositories[component].Name, Images: images,
		}
	}
	rollbackDigest := digest(230)
	f.protected.Digests["control-api"] = []string{rollbackDigest}
	control := f.live.Repositories["control-api"]
	control.Images = append(control.Images, registryImage(99, rollbackDigest, now.Add(-240*time.Hour)))
	f.live.Repositories["control-api"] = control
	return f
}

func TestBuildPlanKeepsEveryProtectedClassAndDeletesOnlyOldOrphans(t *testing.T) {
	f := newGCFixture(t, ModeDryRun)
	report, err := BuildPlan(f.config, f.inventory, f.live, f.manifests, f.protected)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	if !report.Complete {
		t.Fatal("complete plan marked incomplete")
	}
	if got, want := report.Summary.DeleteCandidates, len(RequiredComponents); got != want {
		t.Fatalf("delete candidates = %d, want %d", got, want)
	}

	for component, orphan := range f.orphans {
		decision := decisionFor(t, report, component, orphan.Digest)
		if decision.Decision != DecisionDelete || len(decision.Reasons) != 0 {
			t.Errorf("old orphan %s/%s = %#v, want unqualified delete", component, orphan.Digest, decision)
		}
	}
	assertReason(t, report, "control-api", f.inventory.Containers["control-blue"].ImageRef, "active_revision", "stable/blue")
	assertReason(t, report, "control-api", f.inventory.Containers["control-green"].ImageRef, "active_revision", "candidate/green")
	for _, component := range RequiredComponents {
		for _, manifest := range f.manifests {
			assertReason(t, report, component, manifest.Images[component].Reference, "deployment_manifest", manifest.Source.SHA)
		}
		assertReason(t, report, component, imageReference(component, digest(210+indexOf(component))), "safety_window", "48h0m0s")
	}
	assertReason(t, report, "control-api", imageReference("control-api", digest(230)), "explicit_protection", "protected-digests")
}

func TestBuildPlanRetainsImageAtExactSafetyWindowBoundary(t *testing.T) {
	f := newGCFixture(t, ModeDryRun)
	repository := f.live.Repositories["web-bff"]
	boundaryDigest := digest(210 + indexOf("web-bff"))
	for index := range repository.Images {
		if repository.Images[index].Digest == boundaryDigest {
			repository.Images[index].CreatedAt = f.now.Add(-48 * time.Hour)
		}
	}
	f.live.Repositories["web-bff"] = repository

	report, err := BuildPlan(f.config, f.inventory, f.live, f.manifests, f.protected)
	if err != nil {
		t.Fatalf("BuildPlan() error = %v", err)
	}
	assertReason(t, report, "web-bff", imageReference("web-bff", boundaryDigest), "safety_window", "48h0m0s")
}

func TestBuildPlanReportIsDeterministic(t *testing.T) {
	f := newGCFixture(t, ModeDryRun)
	var baseline []byte
	for attempt := 0; attempt < 20; attempt++ {
		report, err := BuildPlan(f.config, f.inventory, f.live, f.manifests, f.protected)
		if err != nil {
			t.Fatalf("BuildPlan() attempt %d error = %v", attempt, err)
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		if attempt == 0 {
			baseline = encoded
			continue
		}
		if string(encoded) != string(baseline) {
			t.Fatalf("report changed between identical builds:\nfirst: %s\nlater: %s", baseline, encoded)
		}
	}
}

func TestBuildPlanFailsClosedOnIncompleteOrContradictoryEvidence(t *testing.T) {
	tests := []struct {
		name string
		edit func(*gcFixture)
		want string
	}{
		{
			name: "malformed live digest",
			edit: func(f *gcFixture) {
				repo := f.live.Repositories["worker-runtime"]
				repo.Images[0].Digest = "sha256:not-a-digest"
				f.live.Repositories["worker-runtime"] = repo
			},
			want: "invalid id or digest",
		},
		{
			name: "missing repository",
			edit: func(f *gcFixture) { delete(f.live.Repositories, "web-bff") },
			want: "missing repository",
		},
		{
			name: "active revision disagreement",
			edit: func(f *gcFixture) {
				container := f.live.Containers["container1"]
				container.Revisions[0].ID = "differentrevision"
				f.live.Containers["container1"] = container
			},
			want: "active revision disagrees",
		},
		{
			name: "protected digest absent",
			edit: func(f *gcFixture) { f.protected.Digests["web-bff"] = []string{digest(250)} },
			want: "protected digest",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newGCFixture(t, ModeDryRun)
			test.edit(&f)
			report, err := BuildPlan(f.config, f.inventory, f.live, f.manifests, f.protected)
			if err == nil {
				t.Fatalf("BuildPlan() succeeded with unsafe evidence: %#v", report)
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("BuildPlan() error = %q, want substring %q", err, test.want)
			}
			if len(report.Decisions) != 0 {
				t.Fatalf("unsafe evidence produced %d decisions", len(report.Decisions))
			}
		})
	}
}

type fakeCloud struct {
	live          LiveState
	discoverErr   error
	get           func(string) (CloudImage, error)
	delete        func(string) error
	discoverCalls int
	getCalls      []string
	deleteCalls   []string
}

func (cloud *fakeCloud) Discover(context.Context, Inventory) (LiveState, error) {
	cloud.discoverCalls++
	return cloud.live, cloud.discoverErr
}

func (cloud *fakeCloud) GetImage(_ context.Context, imageID string) (CloudImage, error) {
	cloud.getCalls = append(cloud.getCalls, imageID)
	return cloud.get(imageID)
}

func (cloud *fakeCloud) DeleteImage(_ context.Context, imageID string) error {
	cloud.deleteCalls = append(cloud.deleteCalls, imageID)
	if cloud.delete != nil {
		return cloud.delete(imageID)
	}
	return nil
}

func TestRunRejectsMalformedInventoryBeforeCloudDiscovery(t *testing.T) {
	f := newGCFixture(t, ModeDryRun)
	f.inventory.RegistryID = "INVALID/REGISTRY"
	cloud := &fakeCloud{live: f.live, get: func(string) (CloudImage, error) {
		return CloudImage{}, errors.New("unexpected GetImage")
	}}
	_, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err == nil || !strings.Contains(err.Error(), "Terraform inventory") {
		t.Fatalf("Run() error = %v, want malformed inventory failure", err)
	}
	if cloud.discoverCalls != 0 || len(cloud.getCalls) != 0 || len(cloud.deleteCalls) != 0 {
		t.Fatalf("malformed inventory reached cloud: discover=%d get=%v delete=%v",
			cloud.discoverCalls, cloud.getCalls, cloud.deleteCalls)
	}
}

func TestRunDryRunNeverCallsMutationAPIs(t *testing.T) {
	f := newGCFixture(t, ModeDryRun)
	cloud := &fakeCloud{live: f.live, get: func(string) (CloudImage, error) {
		return CloudImage{}, errors.New("GetImage must not be called in dry-run")
	}}
	report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if report.Mode != ModeDryRun || len(cloud.getCalls) != 0 || len(cloud.deleteCalls) != 0 {
		t.Fatalf("dry-run mutated cloud: mode=%q get=%v delete=%v", report.Mode, cloud.getCalls, cloud.deleteCalls)
	}
}

func TestRunFailsBeforeDeleteWhenImageChangesAfterPlan(t *testing.T) {
	f := newGCFixture(t, ModeDelete)
	images := cloudImages(f.live)
	cloud := &fakeCloud{live: f.live}
	cloud.get = func(imageID string) (CloudImage, error) {
		image := images[imageID]
		image.Digest = digest(255)
		return image, nil
	}
	report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err == nil || !strings.Contains(err.Error(), "changed after") {
		t.Fatalf("Run() error = %v, want immutable-plan mismatch", err)
	}
	if report.Complete {
		t.Fatal("failed pre-delete recheck left report complete")
	}
	if len(cloud.deleteCalls) != 0 {
		t.Fatalf("pre-delete mismatch issued deletes: %v", cloud.deleteCalls)
	}
}

func TestRunDeletesOnlyOldOrphansAndCountsSuccess(t *testing.T) {
	f := newGCFixture(t, ModeDelete)
	images := cloudImages(f.live)
	cloud := &fakeCloud{live: f.live, get: func(imageID string) (CloudImage, error) {
		return images[imageID], nil
	}}
	report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	wantIDs := make(map[string]struct{}, len(f.orphans))
	for _, orphan := range f.orphans {
		wantIDs[orphan.ID] = struct{}{}
	}
	if got, want := len(cloud.deleteCalls), len(wantIDs); got != want {
		t.Fatalf("delete calls = %v (%d), want %d old orphans", cloud.deleteCalls, got, want)
	}
	for _, imageID := range cloud.deleteCalls {
		if _, exists := wantIDs[imageID]; !exists {
			t.Errorf("deleted protected/non-orphan image %q", imageID)
		}
	}
	if got, want := report.Summary.Deleted, len(wantIDs); got != want {
		t.Fatalf("summary.deleted = %d, want %d", got, want)
	}
	if report.Summary.AlreadyAbsent != 0 {
		t.Fatalf("summary.already_absent = %d, want 0", report.Summary.AlreadyAbsent)
	}
}

func TestRunTreatsAlreadyAbsentImagesAsIdempotentSuccess(t *testing.T) {
	f := newGCFixture(t, ModeDelete)
	cloud := &fakeCloud{live: f.live, get: func(string) (CloudImage, error) {
		return CloudImage{}, ErrImageNotFound
	}}
	report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !report.Complete || len(cloud.deleteCalls) != 0 {
		t.Fatalf("idempotent run: complete=%v delete=%v", report.Complete, cloud.deleteCalls)
	}
	if got, want := report.Summary.AlreadyAbsent, report.Summary.DeleteCandidates; got != want {
		t.Fatalf("already_absent = %d, want %d", got, want)
	}
	for _, decision := range report.Decisions {
		if decision.Decision == DecisionDelete && decision.Execution != "already_absent" {
			t.Errorf("delete decision %s execution = %q", decision.ImageID, decision.Execution)
		}
	}
}

func TestRunTreatsDeleteNotFoundAfterSuccessfulRecheckAsIdempotent(t *testing.T) {
	f := newGCFixture(t, ModeDelete)
	images := cloudImages(f.live)
	cloud := &fakeCloud{
		live:   f.live,
		get:    func(imageID string) (CloudImage, error) { return images[imageID], nil },
		delete: func(string) error { return ErrImageNotFound },
	}
	report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got, want := report.Summary.AlreadyAbsent, report.Summary.DeleteCandidates; got != want {
		t.Fatalf("already_absent = %d, want %d", got, want)
	}
	if report.Summary.Deleted != 0 || len(cloud.deleteCalls) != report.Summary.DeleteCandidates {
		t.Fatalf("delete race accounting: deleted=%d calls=%v", report.Summary.Deleted, cloud.deleteCalls)
	}
}

func cloudImages(live LiveState) map[string]CloudImage {
	result := make(map[string]CloudImage)
	for _, repository := range live.Repositories {
		for _, image := range repository.Images {
			result[image.ID] = CloudImage{ID: image.ID, Name: repository.Name, Digest: image.Digest, CreatedAt: image.CreatedAt}
		}
	}
	return result
}

func decisionFor(t *testing.T, report Report, component, digestValue string) Decision {
	t.Helper()
	for _, decision := range report.Decisions {
		if decision.Component == component && decision.Digest == digestValue {
			return decision
		}
	}
	t.Fatalf("decision for %s/%s not found", component, digestValue)
	return Decision{}
}

func assertReason(t *testing.T, report Report, component, reference, kind, containsReference string) {
	t.Helper()
	digestValue, err := parseImageReference(reference, testRegistryID, component)
	if err != nil {
		t.Fatalf("parse test reference: %v", err)
	}
	decision := decisionFor(t, report, component, digestValue)
	if decision.Decision != DecisionRetain {
		t.Fatalf("%s/%s decision = %s, want retain", component, digestValue, decision.Decision)
	}
	for _, reason := range decision.Reasons {
		if reason.Kind == kind && strings.Contains(reason.Reference, containsReference) {
			return
		}
	}
	t.Fatalf("%s/%s reasons = %#v, want %s containing %q", component, digestValue, decision.Reasons, kind, containsReference)
}

func registryImage(id int, digestValue string, createdAt time.Time) RegistryImage {
	return RegistryImage{
		ID: fmt.Sprintf("image%d", id), Digest: digestValue, CreatedAt: createdAt,
		CompressedSize: int64(1000 + id), Tags: []string{fmt.Sprintf("tag%d", id)},
	}
}

func imageReference(component, digestValue string) string {
	return "cr.yandex/" + testRegistryID + "/" + component + "@" + digestValue
}

func digest(seed int) string { return fmt.Sprintf("sha256:%064x", seed) }

func sha(seed int) string { return fmt.Sprintf("%040x", seed) }

func indexOf(component string) int {
	for index, value := range RequiredComponents {
		if value == component {
			return index
		}
	}
	return -1
}
