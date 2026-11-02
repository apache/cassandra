# Help for `.build/run-ci`

- [Tools](#tools)
- [Credentials](#credentials)
- [Attach to a build](#attach-to-a-build)
- [Build progress](#build-progress)
- [Examples](#examples)

```text
usage: run-ci [-h] [-c KUBECONFIG] [-x KUBECONTEXT] [-i URL] [-u USER]
              [--credentials-file CREDENTIALS_FILE] [--save-credentials]
              [--attach BUILD] [--job JOB] [-r REPOSITORY] [-b BRANCH]
              [-p {packaging,skinny,pre-commit,pre-commit w/ upgrades,post-commit,custom}]
              [-e PROFILE_CUSTOM_REGEXP] [-j JDK] [-d DTEST_REPOSITORY]
              [-k DTEST_BRANCH] [-s] [--only-setup] [-f VALUES_OVERRIDE]
              [--tear-down] [--only-tear-down] [--only-node-cleaner]
              [-o DOWNLOAD_RESULTS]

Run CI pipeline for Cassandra on K8s using Jenkins.

options:
  -h, --help            show this help message and exit
  -c, --kubeconfig KUBECONFIG
                        Path to a different kubeconfig.
  -x, --kubecontext KUBECONTEXT
                        Use a different Kubernetes context.
  -i, --url URL         Jenkins url. Suitable when kubectl access in not
                        available. Can also be specified via the JENKINS_URL
                        environment variable (and in .build/.run-ci.env)
  -u, --user USER       Jenkins user. Can also be specified via the
                        JENKINS_USER environment variable (and in .build/.run-
                        ci.env)
  --credentials-file CREDENTIALS_FILE
                        Credentials file (default:
                        $XDG_CONFIG_HOME/cassandra/run-ci-credentials.json or
                        ~/.config/cassandra/run-ci-credentials.json).
  --save-credentials    Save or replace credentials after authentication
                        succeeds. Prompts for an API token or password without
                        echoing it.
  --attach BUILD        Reconnect to a build number or URL. Infer the job from
                        the repository and branch, as for a new build. Wait
                        for completion and download results.
  --job JOB             Override the inferred Jenkins job name, including
                        folders.
  -r, --repository REPOSITORY
                        Repository URL. Defaults to current tracking remote.
  -b, --branch BRANCH   Repository branch. Defaults to current branch.
  -p, --profile {packaging,skinny,pre-commit,pre-commit w/ upgrades,post-commit,custom}
                        CI pipeline profile. Defaults to skinny.
  -e, --profile-custom-regexp PROFILE_CUSTOM_REGEXP
                        Regexp for stages when using custom profile. See
                        `testSteps` in Jenkinsfile for list of stages.
                        Example: 'stress.*|jvm-dtest.'
  -j, --jdk JDK         Specify JDK version. Defaults to all JDKs the current
                        branch supports.
  -d, --dtest-repository DTEST_REPOSITORY
                        DTest repository URL.
  -k, --dtest-branch DTEST_BRANCH
                        DTest repository branch.
  -s, --setup           Set up Jenkins before the build.
  --only-setup          Only install Jenkins into the k8s cluster.
  -f, --values-override VALUES_OVERRIDE
                        Path to an additional helm values file, applied over
                        .jenkins/k8s/jenkins-deployment.yaml. Required when
                        the target cluster carries site customisations, see
                        .jenkins/k8s/README.md
  --tear-down           Tear down Jenkins after the build.
  --only-tear-down      Only tear down Jenkins.
  --only-node-cleaner   Only run the node cleaner. The node cleaner scans the
                        k8s nodes, eagerly terminating those unused.
  -o, --download-results DOWNLOAD_RESULTS
                        Just download the results for the specificed build
                        number. Naming of local artefacts assumes current
                        tracking remote and branch, use -r and -b otherwise.
```

## Tools

URL connections need neither Helm nor kubectl, and do not load a kubeconfig. Helm is checked when installing or uninstalling Jenkins. Kubectl is checked when an operation invokes it, such as reading the cluster's admin password or copying results. A cluster connection through the Python Kubernetes client alone does not require either CLI. Python dependencies remain listed in `requirements.txt`.

Without an explicit URL, `run-ci` reads the `cassius-jenkins` Service in the selected Kubernetes context.  It prefers the first concrete hostname in the Service's `external-dns.kubernetes.io/hostname` annotation, or its older `external-dns.alpha.kubernetes.io/hostname` equivalent.  For a named host, an HTTPS listener takes precedence over HTTP; custom ports are retained.  Without a usable DNS annotation, the Service's load-balancer address is used, preferring HTTP when available because the site's TLS certificate generally does not cover the load balancer's own hostname.  `--url` and `JENKINS_URL` override this discovery.  Saved credentials are selected by the resulting URL.

New builds started through a Kubernetes context also run a background node cleaner, unless `NODE_CLEANER_DISABLE=1` is set.  The cleaner uses kubectl to list nodes so failed credential refreshes are reported.  Any error listing, inspecting or deleting Kubernetes resources stops the cleaner and its pending deletion steps for this invocation, with one diagnostic; Jenkins monitoring continues. This includes transient errors: cleanup does not retry automatically after a failure.  Restore cluster access before restarting `--only-node-cleaner`.  For AWS login credentials, run `aws login` with the same profile.  An already running client does not pick up code changes: use Ctrl-C, then `--attach` with the build URL to continue monitoring and download results without cluster access.  Ordinary cluster-based runs still need cluster access for post-build file cleanup and downloads.

## Credentials

Add `--save-credentials` to a build, download or attach command to remember the supplied username and API token or password. Input is hidden, and credentials are saved only after Jenkins confirms authentication. Prefer a [Jenkins API token](https://www.jenkins.io/doc/book/system-administration/authenticating-scripted-clients/) and use HTTPS for remote connections.

```shell
.build/run-ci --url https://ci.example.org --user alice --save-credentials
```

Later commands for that Jenkins URL reuse the saved credentials without `--user` or a prompt. Use `--save-credentials` again to replace the token. An explicit different `--user` prompts for that user's credentials.

The default file is `~/.config/cassandra/run-ci-credentials.json`, or `$XDG_CONFIG_HOME/cassandra/run-ci-credentials.json` when that variable names an absolute directory. Use `--credentials-file PATH` to select another file. The file has mode `0600`; a newly created credentials directory has mode `0700`. Writes are atomic, and symbolic links, hard links, foreign ownership and group/world-readable files are rejected. This is a permissions-protected JSON file, not encrypted storage. Keep it outside the checkout.

Credentials are keyed by the complete Jenkins base URL, including scheme, port and context path. A token saved for `https://ci.example.org/jenkins` is not reused for HTTP or a different Jenkins path. Tokens and passwords are never accepted as command-line arguments or embedded in URLs.

## Attach to a build

Reconnect using the build number:

```shell
.build/run-ci --attach 42
```

The job is inferred from the repository and branch's Cassandra version, as for a new build. Repeat any `-r` and `-b` overrides from the initial invocation. `--job` is an optional override for custom job names or folders.

Use the same connection options as before, for example `--url https://ci.example.org`, or set `JENKINS_URL`. Without a URL, the selected Kubernetes context supplies the Jenkins address. Saved credentials are reused; add `--user USER` when credentials have not been saved.

A build URL also supplies the job and server, without a remote branch lookup:

```shell
.build/run-ci --attach https://ci.example.org/job/cassandra/42/
```

Before starting a new build, `run-ci` checks the inferred job for running builds with identical parameters. Matching includes job defaults for parameters the command does not supply, such as architecture. The newest match prompts `Attach? [Y/n]`: Enter or `y` attaches; `n` starts a new build. Completed builds are not offered. If the prompt cannot read an answer, the command stops and prints `--attach BUILD_NUMBER` instructions. This check does not reserve the job against submissions by other clients.

Attaching waits for the existing build to finish, then downloads its summary, results archive and console log through the authenticated Jenkins connection. Completed builds download immediately. Artifact names use the build's recorded repository and branch; a clean working tree is not required. Results are stored under `build/ci/<encoded-server-url>/<encoded-job-name>/<build-number>/`, so different jobs cannot overwrite one another's results.

Attaching starts no build and runs no node cleaner or remote file deletion. Accepting the matching-build prompt preserves `--tear-down`: Jenkins is uninstalled after monitoring and result downloads return. Explicit `--attach` runs no teardown; setup, teardown and `--download-results` cannot be combined with it. Ctrl-C disconnects the client and leaves the Jenkins build running without teardown.

## Build progress

New builds and `--attach` show split progress beside the elapsed waiting time:

```text
Splits: 128/320 finished (3 failed) | 24 running | 168 queued | 12:34 /
```

The counter reads the named parallel branches under `Tests` through the [Pipeline Graph View API](https://github.com/jenkinsci/pipeline-graph-view-plugin/blob/main/openapi.yaml), which the Kubernetes deployment configures.  Each selected task/JDK combination counts separately, including tasks with one split; retries remain part of the same branch.  A split finishes after its whole branch, including result processing and cleanup, ends.  Failed, unstable, aborted and skipped splits count as finished and appear separately in parentheses.

The total follows the branches Jenkins has published for this build and can grow while the parallel test branches are being created.  Before that, the display says `Splits: waiting for test branches`.  The counter refreshes every 15 seconds and once at completion; overall build status is still checked approximately every three seconds.  If the plugin or its data is unavailable, the display says `Splits: unavailable` and continues waiting for the build.  Attaching reconstructs the counter from Jenkins, while the elapsed timer starts at attachment.  Split completion is not an estimate of remaining time.

The final summary labels the Jenkins result and test totals separately, then prints the saved `console_log.txt.gz` path.  Ant's `BUILD FAILED` messages belong to individual commands and can appear in an `UNSTABLE` Jenkins build.  They remain in the full console log; the summary omits excerpts because parallel task output is interleaved.  Missing reports are shown as unavailable totals rather than evidence that no tests ran.

Results use the Jenkins hostname as their server directory, for example `build/ci/astro-cass.ci/cassandra-6.0/58/`.  Non-default ports and encoded context paths, when present, distinguish servers on the same host.  The URL scheme is omitted from this directory name; credential lookup still uses the complete URL.

## Examples
Run the current directory's fork and branch through the default "skinny" pipeline, connecting via your default kubeconfig
```
.build/run-ci
```

Do the same but connecting via a jenkins url
```
.build/run-ci --url pre-ci.cassandra.apache.org --user myuser
```

Run the the specified fork and branch through the "skinny" pipeline restricted to tests on jdk11
```
.build/run-ci -r "https://github.com/jrwest/cassandra.git" -b "jwest/15452-5.0" -p "skinny" -j 11
```

Run the the specified fork and branch through just the "fqltool-test" tests
```
.build/run-ci -r "https://github.com/jrwest/cassandra.git" -b "jwest/15452-5.0" -p "custom" -e "fqltool-test"
```

Setup/Update Jenkins Helm into your current kubeconfig
```
.build/run-ci --only-setup
```

Setup/Update Jenkins Helm into a cluster that carries site customisations, e.g. pre-ci.cassandra.apache.org
```
.build/run-ci --only-setup --values-override ~/.cassandra-ci/pre-ci-overrides.yaml
```

Before any setup, the values already deployed are compared against those about to be applied.  Any value the deployed jenkins holds that the new files lack is listed, and confirmation is asked for before it is dropped; running non-interactively aborts instead.  See `.jenkins/k8s/README.md` for what this can and cannot catch.

Uninstall Jenkins from your current kubeconfig.
```
.build/run-ci --only-tear-down
```
The jenkins-home volume is kept; delete it separately with `kubectl delete pvc cassius-jenkins`
