<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Build Docker images

- [Automatic publication](#automatic-publication)
- [Registry credentials](#registry-credentials)
- [Testing](#testing)
- [Manual publication and rebuilds](#manual-publication-and-rebuilds)

## Automatic publication

The [Build Docker images workflow](../../.github/workflows/docker-images.yaml) selects changed `.build/docker/*.docker` files after pushes to `trunk`, `cassandra-5.0`, and `cassandra-6.0` in `apache/cassandra`.  The build context is `.build`.

On forks, the same workflow builds changed Dockerfiles after pushes to any branch, always with publication disabled.  Each image builds on native AMD64 and ARM64 runners.  Enable GitHub Actions on the fork and include the workflow on each branch to test.  Pushes without Dockerfile changes do not trigger image builds.

Once triggered, a new branch or an unavailable previous commit selects all tracked Dockerfiles.  A force push can make the previous commit unavailable even with a full-history checkout.  Publication runs still check the registries before building.

Image names match the in-tree build scripts: `apache/cassandra-<dockerfile basename>:<dockerfile md5sum>`.  Images are published to Docker Hub and `apache.jfrog.io/cassan-docker/apache/cassandra-<dockerfile basename>` with provenance and software bill of materials (SBOM) attestations.  There is no `latest` tag.

Publication runs share one concurrency group across branches.  The group covers the registry check, native AMD64 and ARM64 builds, and final publication.  An atomic push to multiple branches therefore queues separate runs; after one run publishes a shared tag, the next run checks the registries and skips that image.

The workflow skips a tag only when both registries contain both required Linux platforms.  A missing tag, incomplete manifest, or missing mirror triggers a build.  Authentication, network, and other unexpected registry errors fail the run instead of triggering a rebuild.

The queue also serializes runs for different tags, while each run builds its selected images and architectures in parallel.  GitHub's [`queue: max`](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency) retains up to 100 pending runs.

Tags depend only on Dockerfile contents.  When changing a file copied into an image, also update each affected Dockerfile so consumers request a new tag.  Helper changes alone do not trigger publication.

## Registry credentials

Ask ASF Infra to provision these repository secrets for `apache/cassandra` before publication:

| Secret | Required value and access |
| --- | --- |
| `DOCKERHUB_USERNAME` | Service account for the Cassandra build image repositories in the `apache` organisation. |
| `DOCKERHUB_TOKEN` | Token with pull and push access to those repositories. |
| `JFROG_USERNAME` | Service account for `apache.jfrog.io`. |
| `JFROG_TOKEN` | Token with read and deploy access to `cassan-docker/apache/cassandra-*`. |

This branch uses `cassandra-almalinux-build`, `cassandra-bullseye-build`, and `cassandra-ubuntu-test`.  Forward ports must also grant access to their Dockerfile basenames, including `cassandra-debian-build` on trunk.

Reference [CASSANDRA-18931](https://issues.apache.org/jira/browse/CASSANDRA-18931) and the precedent in [INFRA-21119](https://issues.apache.org/jira/browse/INFRA-21119).  The names above are those expected by this workflow.  Supply values through GitHub secrets, never through repository files or workflow inputs.

## Testing

Run these commands from the repository root without Docker or registry credentials:

```sh
python3 -m unittest discover -s .build/sh/test -p 'test_docker_image*.py' -v
python3 .build/ci/docker_image_matrix.py --all
python3 .build/ci/docker_image_matrix.py --all --image bullseye-build
```

The tests cover Git history selection and simulated registry responses, including missing platforms, missing mirrors, access failures, and a subsequent branch run seeing a published tag.  They do not verify GitHub scheduling or live registry behavior.

Fork pushes that change Dockerfiles run native image builds automatically.  Build-only runs use a concurrency group per branch, so different branches can build independently.

To run builds manually, install the workflow on the default branch of a fork, such as `thelastpickle/cassandra`.  Also include these changes on the branch selected for testing.  GitHub requires the workflow to exist on the default branch before [manual dispatch](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).

Enable GitHub Actions on the fork.

Run one image on both native architectures:

```sh
gh workflow run docker-images.yaml --repo thelastpickle/cassandra \
  --ref mck/18931/5.0 -f publish=false -f image=bullseye-build
```

Omit `-f image=...` to build all images.  Build-only runs always build the selected images, do not log in to either registry, and do not publish images.  Forks reject `publish=true`.

This checks the Dockerfiles and runner environment without pushing code to `apache/cassandra`.  It does not test registry writes or duplicate suppression under real GitHub scheduling.  Testing those requires a separate copy of the workflow directed at disposable registry repositories with test credentials.

## Manual publication and rebuilds

Once the workflow is on the upstream default branch, select **Actions → Build Docker images → Run workflow**.  Select `trunk`, `cassandra-5.0`, or `cassandra-6.0`.

| Input | Default | Behavior |
| --- | --- | --- |
| `publish` | `false` | Build only; set `true` to publish to both Apache registries. |
| `force` | `false` | Skip complete published tags; set `true` to rebuild, pull base images, and disable Docker build layer reuse. |
| `image` | empty | Select all Dockerfiles on the branch; optionally name one basename without `.docker`. |

Publish all missing or incomplete images on a branch:

```sh
gh workflow run docker-images.yaml --repo apache/cassandra \
  --ref cassandra-5.0 -f publish=true
```

Rebuild and redeploy every image on the branch, including existing MD5 tags:

```sh
gh workflow run docker-images.yaml --repo apache/cassandra \
  --ref cassandra-5.0 -f publish=true -f force=true
```

A forced run replaces shared tags for every branch using the same Dockerfile contents.  It uses the same publication queue as automatic runs.

Registry writes are not atomic across Docker Hub and JFrog.  If publication fails, start a new manual publication run to recheck current registry state.
