# Gradle Build

Gradle 9 requires JDK 17, so doesn't work on trunk; need to downgrade to Gradle 8 to pick up JDK 11 support

An alternative build interface for Cassandra. Ant remains the source of truth — this
build parses the existing `.build/` POM files and `build.xml` properties at
configuration time, so no Gradle files need updating when dependencies change.

## Requirements

- JDK 21 (set `JAVA_HOME` accordingly)
- No separate Gradle installation needed (`./gradlew` wrapper is included)

## Quick Reference

```bash
# Compile everything (main + tests)
./gradlew classes testClasses

# Run checkstyle (main + test)
./gradlew checkstyleMain checkstyleTest

# Build the jar
./gradlew jar

# Run a single unit test
./gradlew test --tests org.apache.cassandra.repair.HappyPathFuzzTest

# Re-run a test (force re-execution)
./gradlew test --tests org.apache.cassandra.repair.HappyPathFuzzTest --rerun

# Run a single test method
./gradlew test --tests "org.apache.cassandra.db.SomeTest.testMethod"

# Run all unit tests
./gradlew test

# Run with a variant
./gradlew test -Pvariant=compression

# Run a distributed (in-jvm dtest) test
./gradlew testDistributed --tests org.apache.cassandra.distributed.test.SimpleReadWriteTest

# Re-run a distributed test
./gradlew testDistributed --tests org.apache.cassandra.distributed.test.SimpleReadWriteTest --rerun

# Distributed test with a variant
./gradlew testDistributed -Pvariant=latest
```

## Test Suites

| Task               | Source directory        | Timeout  | Heap | Ant CI equivalent                            |
|--------------------|------------------------|----------|------|----------------------------------------------|
| `test`             | `test/unit`            | 480s     | 1G   | `testclasslist -Dtest.classlistprefix=unit`   |
| `testLong`         | `test/long`            | 600s     | 1G   | `testclasslist -Dtest.classlistprefix=long`   |
| `testBurn`         | `test/burn`            | ~16.7h   | 1G   | `testclasslist -Dtest.classlistprefix=burn`   |
| `testDistributed`  | `test/distributed`     | 900s     | 1G   | `testclasslist -Dtest.classlistprefix=distributed` |
| `testSimulator`    | `test/simulator/test`  | 1800s    | 8G   | `testclasslist-simulator`                     |

All suites accept `--tests` for filtering and `--rerun` to force re-execution.

## Test Variants

Variants overlay extra YAML configuration on top of `test/conf/cassandra.yaml`
and set additional system properties. Pass `-Pvariant=NAME` to any test task.

| Variant                    | Example                                        |
|----------------------------|------------------------------------------------|
| `compression`              | `./gradlew test -Pvariant=compression`         |
| `cdc`                      | `./gradlew test -Pvariant=cdc`                 |
| `latest`                   | `./gradlew test -Pvariant=latest`              |
| `oa`                       | `./gradlew test -Pvariant=oa`                  |
| `system-keyspace-directory` | `./gradlew test -Pvariant=system-keyspace-directory` |

Variants work with all test suites: `./gradlew testDistributed -Pvariant=compression`

## Build Output

Output goes to `build-gradle/` by default (separate from Ant's `build/`).
To change this, edit `cassandraBuildDir` in `gradle.properties`.

## How It Works

Dependencies are resolved by parsing these files every time Gradle configures:

- `build.xml` — version tokens (`jamm.version`, `asm.version`, etc.)
- `.build/parent-maven-pom.xml` — dependency version BOM
- `.build/cassandra-deps-maven-pom.xml` — compile/runtime dependencies
- `.build/cassandra-build-maven-pom.xml` — test/build dependencies

The `modules/accord` module is integrated as a Gradle composite build — it builds
from source automatically, no `~/.m2` install needed.

All build logic lives in `buildSrc/` as a convention plugin.
