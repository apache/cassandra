Building and Testing with the helper sripts
-------------------------------------------

Information on building and testing beyond the use of ant.
All scripts also print help if the first argument is `-h`.

`build-jars.sh`, `check-code.sh` and `run-tests.sh` accept `-s` (`--summary`).
That prints a summary of the failures in place of the full ant output, and keeps
the full output in a log file under the build directory.  The scripts name that
file when a run fails.

- [Building JARs](#building-jars)
- [Code checks and lints](#code-checks-and-lints)
- [Building artifacts](#building-artifacts-tarball-and-maven)
- [Building packages](#building-debian-and-redhat-packages)
- [Running tests](#running-tests)
- [Testing the helper scripts](#testing-the-helper-scripts)
- [Sonar analysis](#running-sonar-analysis-experimental)
- [Test configurations](#test-configurations)

Building JARs
-------------

Build in docker:

    .build/docker/build-jars.sh


Build without docker:

    .build/build-jars.sh


Remove the artifacts of a previous build first:

    .build/build-jars.sh --clean


Code Checks and Lints
---------------------

Run in docker:

    .build/docker/check-code.sh


Run without docker:

    .build/check-code.sh


Run in docker with a specific jdk.
The following applies to all build scripts.

    .build/docker/check-code.sh 11


Run in docker with a specific build path.
This permits parallel builds off the same source path.
The following applies to all build scripts.

    build_dir=/tmp/cass_Mtu462n .build/docker/check-code.sh


Building Artifacts (tarball and maven)
-------------------------------------

Build with docker:

    .build/docker/build-artifacts.sh


Build without docker:

    .build/build-artifacts.sh


Build in docker with a specific jdk:

    .build/docker/build-artifacts.sh 11


Building Debian and RedHat packages
-----------------------------------

The packaging scripts are only intended to be used with docker.

Build:

    .build/docker/build-debian.sh
    .build/docker/build-redhat.sh


Build with a specific jdk:

    .build/docker/build-debian.sh 11
    .build/docker/build-redhat.sh rpm 11



Running Tests
-------------

Running unit tests with docker:

    .build/docker/run-tests.sh -a test


Running unittests without docker:

    .build/run-tests.sh -a test


Running only a split of unittests, with docker:

    .build/docker/run-tests.sh -a test -c 1/64


Running unittests with a specific jdk with docker:

    .build/docker/run-tests.sh -a test -c 1/64 -j 11


Running only unit tests matching a regexp, with docker:

    .build/docker/run-tests.sh -a test -t VerifyTest -j 11
    .build/docker/run-tests.sh -a test -t "Compaction*Test$" -j 11


Running other types of tests with docker:

    .build/docker/run-tests.sh -a test
    .build/docker/run-tests.sh -a stress-test
    .build/docker/run-tests.sh -a fqltool-test
    .build/docker/run-tests.sh -a sstableloader-test
    .build/docker/run-tests.sh -a microbench
    .build/docker/run-tests.sh -a test-cdc
    .build/docker/run-tests.sh -a test-compression
    .build/docker/run-tests.sh -a test-oa
    .build/docker/run-tests.sh -a test-system-keyspace-directory
    .build/docker/run-tests.sh -a test-tries
    .build/docker/run-tests.sh -a test-burn
    .build/docker/run-tests.sh -a long-test
    .build/docker/run-tests.sh -a harry-test
    .build/docker/run-tests.sh -a cqlsh-test
    .build/docker/run-tests.sh -a jvm-dtest
    .build/docker/run-tests.sh -a jvm-dtest-upgrade
    .build/docker/run-tests.sh -a dtest
    .build/docker/run-tests.sh -a dtest-novnode
    .build/docker/run-tests.sh -a dtest-offheap
    .build/docker/run-tests.sh -a dtest-large
    .build/docker/run-tests.sh -a dtest-large-novnode
    .build/docker/run-tests.sh -a dtest-upgrade
    .build/docker/run-tests.sh -a dtest-upgrade-large

Repeating tests

Just add the '-repeat' suffix to the test type and pass in the repeat arguments

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2
    .build/docker/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -j 11

Fail fast with repeating tests is done with REPEATED_TESTS_STOP_ON_FAILURE

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -e REPEATED_TESTS_STOP_ON_FAILURE=false

Running python dtests without docker:

    .build/run-python-dtests.sh dtest


Other test types without docker:

    .build/run-tests.sh jvm-test


Other python dtest types without docker:

    .build/run-python-dtests.sh dtest-upgrade-large

Debugging test scripts:

    DEBUG=true .build/docker/run-tests.sh -a test


Testing the helper scripts
--------------------------

The tests of the helper scripts need no JDK build.  A stub `ant` and a stub `docker`
replay canned output, so each suite runs in seconds.

Run them:

    python3 -m unittest discover -s .build/sh/test    # the log summarizers
    .build/sh/test/stub-ant-cases.sh                  # .build/*.sh
    .build/sh/test/stub-docker-cases.sh               # .build/docker/*.sh


Running Sonar analysis (experimental)
-------------------------------------

Run:

    ant sonar

Sonar analysis requires the SonarQube server to be available. If there
is already some SonarQube server, it can be used by setting the
following env variables:

    SONAR_HOST_URL=http://sonar.example.com
    SONAR_CASSANDRA_TOKEN=cassandra-project-analysis-token
    SONAR_PROJECT_KEY=<key of the Cassandra project in SonarQube>

If SonarQube server is not available, one can be started locally in
a Docker container. The following command will create a SonarQube
container and start the server:

    ant sonar-create-server

The server will be available at http://localhost:9000 with admin
credentials admin/password. The Docker container named `sonarqube`
is created and left running. When using this local SonarQube server,
no env variables to configure url, token, or project key are needed,
and the analysis can be run right away with `ant sonar`.

After the analysis, the server remains running so that one can
inspect the results.

To stop the local SonarQube server:

    ant sonar-stop-server

However, this command just stops the Docker container without removing
it. It allows to start the container later with:

    docker container start sonarqube

and access previous analysis results. To drop the container, run:

    docker container rm sonarqube

When `SONAR_HOST_URL` is not provided, the script assumes a dedicated
local instance of the SonarQube server and sets it up automatically,
which includes creating a project, setting up the quality profile, and
quality gate from the configuration stored in
[sonar-quality-profile.xml](sonar%2Fsonar-quality-profile.xml) and
[sonar-quality-gate.json](sonar%2Fsonar-quality-gate.json)
respectively. To run the analysis with a custom quality profile, start
the server using `ant sonar-create-server`, create a project manually,
and set up a desired quality profile for it. Then, create the analysis
token for the project and export the following env variables:

    SONAR_HOST_URL="http://127.0.0.1:9000"
    SONAR_CASSANDRA_TOKEN="<token>"
    SONAR_PROJECT_KEY="<key of the Cassandra project in SonarQube>"

The analysis can be run with `ant sonar`.

Test configurations
-------------------

The `harry-test` type collects the standalone classes in `test/harry/main`. It supports the same class regexp, splits, summaries and `-repeat` suffix as `test`. Jenkins schedules two splits; CircleCI schedules the suite on Java 11 and 17. CircleCI's changed-test detection and `REPEATED_HARRY_TESTS` / `REPEATED_HARRY_TESTS_COUNT` variables also support this source root.

Classes can declare `@TestConfigurations({"default", "latest"})`. The shared Ant launcher reads the compiled annotation without loading the class and runs each configuration in a fresh JVM. This works with existing JUnit runners, including parameterized tests. Unannotated classes run once. Annotations are not inherited: annotate the concrete class that warrants extra executions.

The `test`, `testclasslist` and `testsome` Ant targets use these annotations. Existing explicit targets such as `test-latest` retain their selected configuration. JVM dtests configure their own instances and are not assigned server YAML profiles here.

`default` uses `test/conf/cassandra.yaml`; `latest` appends the existing `latest_diff.yaml` and storage-compatibility overlay. Other names select `test/conf/configurations/<name>.yaml`, appended to the same base. Names must contain lowercase letters, digits or hyphens and start with a letter. Unknown, empty and duplicate declarations fail the build.

To reproduce one configuration, pass `-Dtest.configuration=<name>` to Ant. For example:

```sh
ANT_OPTS=-Dtest.configuration=bti .build/run-tests.sh -s -a harry-test -t HistoryBuilderTest
ant testsome -Dtest.classlistprefix=harry/main -Dtest.name=org.apache.cassandra.harry.test.HistoryBuilderTest -Dtest.configuration=latest
```

A class and all its configurations stay together when splitting the source list. Reports and logs distinguish configurations; repeats rerun each declared configuration. This avoids creating separate test types or repeating model-only tests for server settings they never use.

### Harry profile selection

| Classes | Configurations | Coverage |
| --- | --- | --- |
| HarryCompactionTest, HarryCompactionWithRangeDeletionsTest | default, big-cursor, bti, latest | BIG iterator and cursor compaction, BTI iterator compaction, bulk loading and storage reads under latest defaults |
| HarrySSTableWriterTest | default, bti, latest | SSTable writing, loading and reads under both formats and latest storage defaults |
| HistoryBuilderTest | default, bti, latest | CQL mutations, reads and flushes, including the Trie memtable and offheap allocation in latest |
| EntropySourceTest, ASTSingleTableModelTest, QueryBuilderTest, SimpleBijectionTest, TestStateTest | default | Generators and in-memory models; server configuration adds no coverage |

`bti.yaml` changes only format and storage compatibility, with cursor compaction explicitly disabled. `big-cursor.yaml` enables cursor compaction with its supported BIG format. Latest selects BTI, so its cursor setting does not exercise the BIG cursor implementation.

### YAML settings and additional profiles

The selection considers the complete latest overlay and the existing test YAML files. Add a profile only when a test reaches the affected behavior and checks its result; do not generate a Cartesian product of settings.

| Settings | Decision |
| --- | --- |
| SSTable format, compatibility, cursor compaction | Separate focused BTI and BIG cursor overlays isolate these implementations. |
| Trie memtable and offheap allocation | HistoryBuilderTest exercises writes, reads and flushes under latest. Pure generators do not. |
| Alternate sharded and test memtables | Keep the dedicated memtable tests. These Harry histories do not provide a concurrency or allocator-specific oracle for another global memtable profile. |
| Default UCS, compactor concurrency | Latest exercises compaction with these defaults; the compaction tests disable automatic compaction and explicitly force it, so they do not validate background scheduling. |
| Direct background writes, commitlog access mode | Latest covers storage paths reached by these tests. Bulk-loaded mutations bypass the commitlog; this is not commitlog durability coverage. |
| Key cache, column index size, UUID SSTable identifiers, entire-SSTable streaming | Storage reads and bulk loading warrant latest. Column index size and entire-SSTable streaming already match the base test YAML, so no extra profile is needed for them. |
| Default SAI, default-secondary-index enablement | These Harry classes do not create indexes. Existing SAI fuzz tests are the appropriate consumers, not another standalone-Harry YAML. |
| Batchlog endpoint strategy, authenticator/authorizer defaults | These tests do not validate replica selection or authentication. Latest includes the settings, but a successful run does not establish that coverage. |
| CDC and commitlog compression overlays | Keep dedicated CDC/commitlog tests and existing targets. Harry tables do not enable CDC; bulk-loaded compaction tests bypass the commitlog. |
| System-keyspace directory overlay | Keep the existing dedicated target; these Harry checks validate user data rather than system-keyspace placement. |
| TLS, JMX, authentication, seeds, partitioner and configuration-conversion YAML fixtures | Keep the tests that own their setup and assertions. They are complete or deliberately invalid configuration fixtures, not generic appendable profiles. |
| Encryption and SSTable-loader encryption fixtures | Keep dedicated encryption/loader tests, with their keys and assertions. Applying their YAML alone would not prove encrypted files or transport. |
| Remaining base YAML settings: networking, topology, hints, caches, thresholds, Accord, timeouts and resource limits | Preserve the tested base settings. Add focused overlays with behavioral assertions when testing those features; changing them across every Harry class would add cost and unrelated failures. |

New overlays must preserve the base test directories and ports. Overlays that change nested mappings must supply the required mapping entries: concatenation replaces duplicate YAML keys rather than recursively merging mappings. The existing latest overlay deliberately extends the trailing memtable mapping, so retain its ordering.
