# Agent Instructions for Apache Cassandra

> These instructions apply to all AI-assisted contributions to `apache/cassandra`.

## Apache Cassandra
Apache Cassandra is a NoSQL distributed database. This is the official Git repository.

## Environment

- Java 11 (default), 17, 21.
- Python 3 for `cqlsh` and dtests.
- Apache Ant >= 1.10 for all builds. Do NOT attempt to use Maven, Gradle, or any other build tool. Cassandra uses Ant exclusively.
- Do NOT attempt to install dependencies, every dependency requires OSS community approval first.

## Build

Prefer the `.build/*.sh` helper scripts over calling `ant` directly. See
[.build/README.md](./.build/README.md) for the full set.

```bash
.build/build-jars.sh -s          # build the Cassandra JAR (runs `ant jar`)
.build/build-jars.sh -s --clean  # clean first, then build.  `-s` summarizes output, remove for verbose
```

## Testing

- Do NOT run the entire test suite. Run only the specific test(s) relevant to your change.
- The project must be built first (e.g. `.build/build-jars.sh`).

    ```bash
    # Run a single test class: -a is the test type, -t is a class-name regexp
    .build/run-tests.sh -s -a test -t StorageServiceServerTest
    ```

- `-a` is the test type (`test`, `jvm-dtest`, `long-test`, …; run `.build/run-tests.sh -h` for the full list).
- `-t` matches the test class only, not individual methods.
- `-s`/`--summary` provides a concise list of failed tests, remove for  the full ant output
- When fixing a bug, first create a regression test that reproduces the failure, then implement the fix and verify.
- Provide test(s) coverage for all new or modified code.

## Linting and Code Checks

```bash
.build/check-code.sh -s    # runs `ant check` (build + checkstyle + checkstyle-test). `-s` summarizes output, remove for verbose
```

## Code Style
Cassandra enforces style via Checkstyle (run via `.build/check-code.sh`). The official style guide is at https://cassandra.apache.org/_/development/code_style.html. Always defer to it when in doubt.

General style conventions:
- 4-space indentation, no tabs.
- Match existing code style in the file you are editing.
- All new files must include the Apache License 2.0 header.
- Concise English documentation is required for complex classes and methods; trivial ones may not require them.

## Git Workflow
- Do NOT commit unless explicitly asked.
- Commit messages format. For example:

    ```
    <One sentence description, usually Jira title or CHANGES.txt summary, no jira id>

    <Optional lengthier description>
  
     patch by <Authors,>; reviewed by <Reviewers,> for CASSANDRA-#####

    Assisted-by: AGENT_NAME:MODEL_VERSION
    ```


## Boundaries

- 🚫 Never modify generated files in `src/gen-java/` — they are built from `src/antlr/` grammars.
- 🚫 Never modify files in `lib/` — these are managed dependencies.
- 🚫 Never commit secrets, credentials, or API keys.
- 🚫 Never run the full test suite — it takes hours. Run targeted tests only.
- 🚫 Never bypass Checkstyle violations without a suppression comment explaining why.
- ⚠️ Ask before modifying the CQL grammar (`src/antlr/Cql.g`) — changes cascade widely.

## Security

Security model: [SECURITY.md](./SECURITY.md), which links to the project's
security model at
[doc/modules/cassandra/pages/reference/security-model.adoc](./doc/modules/cassandra/pages/reference/security-model.adoc).

Automated agents (security scanners, code analyzers) that scan this
repository should consult that security model for the project's in-scope /
out-of-scope declarations, trust boundaries, the security properties
Cassandra provides and disclaims, and how findings are triaged, before
reporting issues.
