# Releases

GitCode owns Sessionless commits and tags. GitHub is a push-mirrored automation
and asset surface; creating a GitHub tag is not a release operation.

## Version and provenance contract

Release tags use one of these exact forms:

- `vMAJOR.MINOR.PATCH`
- `vMAJOR.MINOR.PATCH-rc.N`

Numeric components have no leading zeroes. Build metadata and other prerelease
names are intentionally unsupported for the initial contract.

The release workflow checks the raw tag object and the peeled commit through
the public Git transports of both GitCode and GitHub. Annotated tags must have
the same tag-object SHA and peeled commit on both services; lightweight tags
must have the same commit SHA. The checkout, `GITHUB_SHA`, and tag commit must
also agree. The tag must be on the first-parent history of `main`, and mirrored
GitHub `main` must equal GitCode `main`.

This proves that both services currently expose identical Git objects. It
cannot prove which process copied an already-identical object: Git has no
push-mirror attestation. Stronger causal proof would require a GitCode token or
a signed GitCode webhook, neither of which is stored in GitHub for this phase.
GitCode lookup failure therefore blocks release publication.

## One-time authorization setup

1. Protect `v*` tags in GitCode so only maintainers can create, move, or delete
   them. Configure the GitHub mirror so direct human tag writes are denied.
2. Create a GitHub Environment named `release`. Its deployment branch/tag
   policy must allow protected `v*` tags and no branches. Do not add a branch
   exception for manual runs.
3. Select an existing GitCode release tag when manually dispatching
   `.github/workflows/release.yml`. With publication disabled, the workflow
   prints only safe OIDC claims and then stops. Copy the exact `subject` claim
   into the external cloud-dev tfvars as `github_release_oidc_subject`.
4. Apply the reviewed Terraform plan. It creates a dedicated
   `release-publisher` service account and workload-identity credential. The
   service account has `container-registry.images.pusher` only on these five
   repositories: `control-api`, `web-bff`, `reconciler`, `telegram-sender`, and
   `worker-runtime`.
5. Set these non-secret variables on the `release` environment:

   - `YANDEX_OIDC_AUDIENCE`
   - `YANDEX_RELEASE_GITHUB_OIDC_SUBJECT`
   - `YANDEX_RELEASE_PUBLISHER_SERVICE_ACCOUNT_ID`
   - `YANDEX_CONTAINER_REGISTRY_ID`
   - `YANDEX_RELEASE_PUBLISH_ENABLED=true`

Yandex federated credentials match one exact OIDC subject and do not accept a
`refs/tags/v*` wildcard. The constant environment subject plus GitHub's
environment tag policy provides the wildcard gate. The login helper separately
requires the signed JWT `ref`, `sha`, repository, and event to match the
selected release tag. Repository subjects can contain immutable owner and
repository IDs, so copy the observed claim rather than constructing it.

## Publishing

Create and push the tag only to GitCode:

```sh
git switch main
git pull --ff-only
git tag -a v0.1.0 -m v0.1.0
git push origin v0.1.0
```

The mirror propagates the identical tag object to GitHub. The release workflow
then:

1. validates GitCode, GitHub, and checkout provenance without write or OIDC
   permission;
2. runs the clean tagged repository verification contract;
3. rebuilds all five images twice and proves their registry manifests are
   reproducible;
4. uses the dedicated release identity to publish or verify commit-SHA tags in
   Yandex Container Registry;
5. generates first-parent release notes from local Git history;
6. creates one GitHub Release with `deployment-images.json`,
   `deployment-images.sha256`, and `release-notes.md`.

No `latest` image tag is produced. The deployment manifest contains the tagged
source SHA and digest-qualified references for all five images.

## Retry and conflicts

For a retry, dispatch the workflow at the tag ref itself:

```sh
gh workflow run release.yml --repo urandon/sessionless --ref v0.1.0
```

Do not dispatch `main` with a tag string. A branch-ref dispatch fails in the
read-only validation job and cannot enter the protected environment.

Same-tag image publication is a no-op only when the complete remote manifest
and config identities match the reproducible candidates. Existing release
assets are accepted only when their bytes match; missing assets may be added,
while a conflicting asset or release body blocks the retry. Never move a
released tag or use an overwrite/clobber path.

If a first publication is blocked because the Yandex repositories or IAM
bindings have not been applied, leave the GitCode tag unchanged, complete the
infrastructure apply, and retry that same tag.
