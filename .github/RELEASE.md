## release workflow

the `Release` workflow runs only for `v*` tags. it builds the locked release
server binary, Odoo client bundle and version-tag GHCR image from the tagged
source. it extracts the image SBOM into a release asset, writes `SHA256SUMS`,
creates GitHub artifact attestations from that checksum manifest, creates
GitHub image attestation for the version-tag image, then publishes the release.

this does not replace the container image workflow. commit-addressable GHCR
images remain the testing-infrastructure package path for `master` and specific
commits.

the `Release` workflow also publishes the version-tag container image. only
those version-tag image builds get Docker provenance, SBOM and GitHub image
attestations. `master`, commit-addressable `sha-<commit>` images and pull
request smoke-test images are intentionally not attested.

suffixed tags, such as `v0.3.1-rc.1` or `v0.3.1-test.20260605`, are
published as GitHub prereleases and are explicitly not marked as the latest
release

the GitHub release includes the server tarball, Odoo client bundle, image SBOM
and checksum manifest. the release notes include verification commands generated
from `GITHUB_REPOSITORY` and `GITHUB_REF_NAME`, so the owner and version are not
hardcoded in the published release text.

## updating release lockfiles

for a release-only version bump, update the root Cargo version and refresh only
the local workspace package entries in each tracked lockfile:

```bash
cargo update --workspace
cargo update --manifest-path tests/fuzz/Cargo.toml --workspace
```

`cargo update --workspace` keeps already locked third-party dependencies in
place and updates only packages defined by the current workspace unless Cargo
must add a missing package. this is the right default after changing the version
in `Cargo.toml`. Omit it if you update dependencies.

the fuzz targets are a separate Cargo workspace with their own lockfile at
`tests/fuzz/Cargo.lock`, so the fuzz manifest must get the same workspace-only
refresh. otherwise `--locked` CI checks can fail because Cargo would need to
rewrite the fuzz lockfile.

after the lockfiles are refreshed, check that the locked metadata commands do
not need to rewrite either lockfile:

```bash
cargo metadata --locked --format-version 1 >/dev/null
cargo metadata --manifest-path tests/fuzz/Cargo.toml --locked --format-version 1 >/dev/null
```

if the release intentionally includes a full dependency refresh, update both the
root workspace and the fuzz workspace without `--workspace`:

```bash
cargo update
cargo update --manifest-path tests/fuzz/Cargo.toml
```

review third-party dependency changes before merging a full refresh. tooling
dependencies can also affect CI, so keep workflow-installed tool versions in
sync with any locked tool crate updates.

## creating a release tag

merge the release changes through a pull request first. after the PR is merged,
create the tag on the updated `master` branch and push only the tag:

```bash
git checkout master
git pull
git tag -a v0.3.1 -m "v0.3.1"
git push origin v0.3.1
```

`git tag -a` creates an annotated tag that points at the current `HEAD` commit.
it does not create a code commit and does not move `master`.

when GitHub receives the pushed `v*` tag, it starts the `Release` workflow on
the commit the tag points to. avoid creating the GitHub release manually from
the web UI for this flow because the workflow creates the release and uploads
the assets itself.

## formatting

tags must start with the Cargo base version and use Docker-tag-safe suffix
characters because the release tag is also used as the GHCR image tag

`v{major}.{minor}.{patch}`
example: `v0.3.1`

or the same base version with any suffix that starts with `.`, `_` or `-`

`v{major}.{minor}.{patch}<suffix>`
examples: `v0.3.1-rc.1`, `v0.3.1-test.20260605`, `v0.3.1_build42`

only the base `major.minor.patch` is compared to Cargo, so generated suffixes
do not require Cargo version bumps
