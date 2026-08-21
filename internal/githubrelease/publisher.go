package githubrelease

import (
	"context"
	"errors"
	"fmt"
)

type Publisher struct {
	api API
}

func NewPublisher(api API) (*Publisher, error) {
	if api == nil {
		return nil, errors.New("GitHub release API must not be nil")
	}
	return &Publisher{api: api}, nil
}

func (p *Publisher) Publish(ctx context.Context, request Request) (Result, error) {
	assets, err := validateRequest(request)
	if err != nil {
		return Result{}, err
	}

	release, err := p.api.FindRelease(ctx, request.Repository, request.Tag)
	writes := 0
	if errors.Is(err, ErrNotFound) {
		release, err = p.api.CreateDraft(ctx, request)
		if err != nil {
			if statusCode(err) != 422 {
				return Result{}, fmt.Errorf("create draft release: %w", err)
			}
			release, err = p.api.FindRelease(ctx, request.Repository, request.Tag)
			if err != nil {
				return Result{}, fmt.Errorf("find concurrently created release: %w", err)
			}
		} else {
			writes++
		}
	} else if err != nil {
		return Result{}, fmt.Errorf("find release: %w", err)
	}

	if err := validateRelease(release, request); err != nil {
		return Result{}, err
	}
	remoteAssets, err := p.api.ListAssets(ctx, request.Repository, release.ID)
	if err != nil {
		return Result{}, fmt.Errorf("list release assets: %w", err)
	}
	plan, err := planAssets(release, assets, remoteAssets)
	if err != nil {
		return Result{}, err
	}
	if !release.Draft {
		return Result{ReleaseID: release.ID, Tag: request.Tag, SourceSHA: request.SourceSHA, Status: "verified_existing", Writes: writes}, nil
	}

	for _, starter := range plan.removeStarters {
		if err := p.api.DeleteAsset(ctx, request.Repository, starter.ID); err != nil {
			return Result{}, fmt.Errorf("delete incomplete starter asset %q: %w", starter.Name, err)
		}
		writes++
	}
	for _, asset := range plan.upload {
		remote, uploadErr := p.api.UploadAsset(ctx, request.Repository, release, asset)
		if uploadErr != nil {
			if statusCode(uploadErr) != 422 {
				return Result{}, fmt.Errorf("upload release asset %q: %w", asset.Name, uploadErr)
			}
			remoteAssets, err = p.api.ListAssets(ctx, request.Repository, release.ID)
			if err != nil {
				return Result{}, fmt.Errorf("re-read raced release asset %q: %w", asset.Name, err)
			}
			remote, err = exactNamedAsset(remoteAssets, asset.Name)
			if err != nil {
				return Result{}, fmt.Errorf("upload race for release asset %q: %w", asset.Name, err)
			}
		} else {
			writes++
		}
		if err := validateRemoteAsset(remote, asset); err != nil {
			return Result{}, err
		}
	}

	remoteAssets, err = p.api.ListAssets(ctx, request.Repository, release.ID)
	if err != nil {
		return Result{}, fmt.Errorf("verify release assets: %w", err)
	}
	finalPlan, err := planAssets(release, assets, remoteAssets)
	if err != nil {
		return Result{}, err
	}
	if len(finalPlan.upload) != 0 || len(finalPlan.removeStarters) != 0 {
		return Result{}, errors.New("draft release asset set is incomplete after upload")
	}

	release, err = p.api.PublishDraft(ctx, request.Repository, release.ID)
	if err != nil {
		return Result{}, fmt.Errorf("publish verified draft release: %w", err)
	}
	writes++
	if err := validateRelease(release, request); err != nil {
		return Result{}, fmt.Errorf("published release response: %w", err)
	}
	if release.Draft {
		return Result{}, errors.New("GitHub kept verified release in draft state")
	}

	release, err = p.api.FindRelease(ctx, request.Repository, request.Tag)
	if err != nil {
		return Result{}, fmt.Errorf("read back published release: %w", err)
	}
	if err := validateRelease(release, request); err != nil {
		return Result{}, fmt.Errorf("published release readback: %w", err)
	}
	if release.Draft {
		return Result{}, errors.New("published release readback is still a draft")
	}
	remoteAssets, err = p.api.ListAssets(ctx, request.Repository, release.ID)
	if err != nil {
		return Result{}, fmt.Errorf("read back published assets: %w", err)
	}
	if _, err := planAssets(release, assets, remoteAssets); err != nil {
		return Result{}, fmt.Errorf("published asset readback: %w", err)
	}
	return Result{ReleaseID: release.ID, Tag: request.Tag, SourceSHA: request.SourceSHA, Status: "published", Writes: writes}, nil
}

type assetPlan struct {
	upload         []Asset
	removeStarters []RemoteAsset
}

func planAssets(release Release, expected []Asset, remote []RemoteAsset) (assetPlan, error) {
	if len(remote) > len(expected) {
		return assetPlan{}, fmt.Errorf("release has %d assets; exactly three are allowed", len(remote))
	}
	expectedByName := make(map[string]Asset, len(expected))
	for _, asset := range expected {
		expectedByName[asset.Name] = asset
	}
	seen := make(map[string]struct{}, len(remote))
	plan := assetPlan{}
	for _, asset := range remote {
		expectedAsset, ok := expectedByName[asset.Name]
		if !ok {
			return assetPlan{}, fmt.Errorf("release contains unexpected asset %q", asset.Name)
		}
		if _, duplicate := seen[asset.Name]; duplicate {
			return assetPlan{}, fmt.Errorf("release contains duplicate asset %q", asset.Name)
		}
		seen[asset.Name] = struct{}{}
		if asset.State == "starter" && release.Draft && asset.ID > 0 && asset.Size == 0 && asset.Digest == "" {
			plan.removeStarters = append(plan.removeStarters, asset)
			plan.upload = append(plan.upload, expectedAsset)
			continue
		}
		if err := validateRemoteAsset(asset, expectedAsset); err != nil {
			return assetPlan{}, err
		}
	}
	for _, asset := range expected {
		if _, ok := seen[asset.Name]; !ok {
			plan.upload = append(plan.upload, asset)
		}
	}
	if !release.Draft && (len(plan.upload) != 0 || len(plan.removeStarters) != 0) {
		return assetPlan{}, errors.New("published release does not contain the exact immutable asset set")
	}
	return plan, nil
}

func validateRelease(release Release, request Request) error {
	if release.ID <= 0 || release.TagName != request.Tag || release.Name != request.Name ||
		release.Body != string(request.Body) || release.Prerelease != request.Prerelease {
		return errors.New("existing release metadata does not match deterministic release inputs")
	}
	if release.Draft && release.Immutable {
		return errors.New("immutable release cannot be resumed as a draft")
	}
	if release.Draft && release.UploadURL == "" {
		return errors.New("draft release does not expose an upload URL")
	}
	return nil
}

func exactNamedAsset(assets []RemoteAsset, name string) (RemoteAsset, error) {
	var found *RemoteAsset
	for _, asset := range assets {
		if asset.Name != name {
			continue
		}
		if found != nil {
			return RemoteAsset{}, errors.New("more than one same-name asset exists")
		}
		copy := asset
		found = &copy
	}
	if found == nil {
		return RemoteAsset{}, errors.New("same-name asset was not found after upload conflict")
	}
	return *found, nil
}
