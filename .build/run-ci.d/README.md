# Help for `.build/run-ci`

```
➤ .build/run-ci --help
usage: run-ci [-h] [-c KUBECONFIG] [-x KUBECONTEXT] [-i URL] [-u USER] [-r REPOSITORY] [-b BRANCH] [-p {packaging,skinny,pre-commit,pre-commit w/ upgrades,post-commit,custom}] [-e PROFILE_CUSTOM_REGEXP] [-j JDK] [-d DTEST_REPOSITORY] [-k DTEST_BRANCH]
              [-s] [--only-setup] [--tear-down] [--only-tear-down] [--only-node-cleaner] [-o DOWNLOAD_RESULTS]

Run CI pipeline for Cassandra on K8s using Jenkins.

options:
  -h, --help            show this help message and exit
  -c KUBECONFIG, --kubeconfig KUBECONFIG
                        Path to a different kubeconfig.
  -x KUBECONTEXT, --kubecontext KUBECONTEXT
                        Use a different Kubernetes context.
  -i URL, --url URL     Jenkins url. Suitable when kubectl access in not available. Can also be specified via the JENKINS_URL environment variable (and in .build/.run-ci.env)
  -u USER, --user USER  Jenkins user. Can also be specified via the JENKINS_USER environment variable (and in .build/.run-ci.env)
  -r REPOSITORY, --repository REPOSITORY
                        Repository URL. Defaults to current tracking remote.
  -b BRANCH, --branch BRANCH
                        Repository branch. Defaults to current branch.
  -p {packaging,skinny,pre-commit,pre-commit w/ upgrades,post-commit,custom}, --profile {packaging,skinny,pre-commit,pre-commit w/ upgrades,post-commit,custom}
                        CI pipeline profile. Defaults to skinny.
  -e PROFILE_CUSTOM_REGEXP, --profile-custom-regexp PROFILE_CUSTOM_REGEXP
                        Regexp for stages when using custom profile. See `testSteps` in Jenkinsfile for list of stages. Example: 'stress.*|jvm-dtest.'
  -j JDK, --jdk JDK     Specify JDK version. Defaults to all JDKs the current branch supports.
  -d DTEST_REPOSITORY, --dtest-repository DTEST_REPOSITORY
                        DTest repository URL.
  -k DTEST_BRANCH, --dtest-branch DTEST_BRANCH
                        DTest repository branch.
  -s, --setup           Set up Jenkins before the build.
  --only-setup          Only install Jenkins into the k8s cluster.
  --tear-down           Tear down Jenkins after the build.
  --only-tear-down      Only tear down Jenkins.
  --only-node-cleaner   Only run the node cleaner. The node cleaner scans the k8s nodes, eagerly terminating those unused.
  -o DOWNLOAD_RESULTS, --download-results DOWNLOAD_RESULTS
                        Just download the results for the specificed build number. Naming of local artefacts assumes current tracking remote and branch, use -r and -b otherwise.
```

## Examples
Run the current directory's fork and branch through the default "pre-commit" pipeline, connecting via your default kubeconfig
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

Uninstall Jenkins from your current kubeconfig
```
.build/run-ci --only-tear-down
```