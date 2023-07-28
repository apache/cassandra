Building and Testing with the helper sripts
-------------------------------------------

Information on building and testing beyond the use of ant.
All scripts also print help if the first argument is `-h`.

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


Build with centos7 and a specific jdk:

    .build/docker/build-redhat.sh noboolean 11


Running Tests
-------------

Running unit tests with docker:

    .build/docker/run-tests.sh test


Running unittests without docker:

    .build/run-tests.sh test


Running only a split of unittests, with docker:

    .build/docker/run-tests.sh test 1/64


Running unittests with a specific jdk with docker:

    .build/docker/run-tests.sh test 1/64 11


Running only unit tests matching a regexp, with docker:

    .build/docker/run-tests.sh test VerifyTest 11
    .build/docker/run-tests.sh test "Compaction*Test$" 11


Running other types of tests with docker:

    .build/docker/run-tests.sh test
    .build/docker/run-tests.sh stress-test
    .build/docker/run-tests.sh fqltool-test
    .build/docker/run-tests.sh microbench
    .build/docker/run-tests.sh test-cdc
    .build/docker/run-tests.sh test-compression
    .build/docker/run-tests.sh test-oa
    .build/docker/run-tests.sh test-system-keyspace-directory
    .build/docker/run-tests.sh test-tries
    .build/docker/run-tests.sh test-burn
    .build/docker/run-tests.sh long-test
    .build/docker/run-tests.sh cqlsh-test
    .build/docker/run-tests.sh jvm-dtest
    .build/docker/run-tests.sh jvm-dtest-upgrade
    .build/docker/run-tests.sh dtest
    .build/docker/run-tests.sh dtest-novnode
    .build/docker/run-tests.sh dtest-offheap
    .build/docker/run-tests.sh dtest-large
    .build/docker/run-tests.sh dtest-large-novnode
    .build/docker/run-tests.sh dtest-upgrade
    .build/docker/run-tests.sh dtest-upgrade-large


Running python dtests without docker:

    .build/run-python-dtests.sh dtest


Other test types without docker:

    .build/run-tests.sh jvm-test


Other python dtest types without docker:

    .build/run-python-dtests.sh dtest-upgrade-large

