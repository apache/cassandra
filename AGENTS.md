# Agent Instructions for Apache Cassandra

> These instructions apply to all AI-assisted contributions to `apache/cassandra`.

## Apache Cassandra
Apache Cassandra is a NoSQL distributed database. This is the official Git repository

## Environment

- Java 11 (default), 17, 21.
- Python 3 for `cqlsh` and dtests.
- Apache Ant >= 1.10 for all builds. Do NOT attempt to use Maven, Gradle, or any other build tool. Cassandra uses Ant exclusively.
- Do NOT attempt to install dependencies, every dependency requires OSS community approval first

## Build

```bash
.build/sh/ai-build       # clean, build JAR, and run checkstyle (output is summarized)
```

Do NOT call `ant` directly — always use the `ai-*` wrapper scripts which handle log summarization and correct working directory.

## Testing

- Do NOT run the entire test suite. Run only the specific test(s) relevant to your change.

    ```bash
    # Run a single unit test class
    .build/sh/ai-ci-test org.apache.cassandra.service.StorageServiceServerTest
    ```

- `ai-ci-test` does NOT support method-level filtering — it runs the entire test class.
- When fixing a bug, first create a regression test that reproduces the failure, then implement the fix and verify.
- Provide test(s) coverage for all new or modified code.

## Linting and Code Checks

`.build/sh/ai-build` includes checkstyle validation. There is no need to run checkstyle separately.

## Code Style
Cassandra enforces style via Checkstyle (run via `.build/sh/ai-build`). The official style guide is at https://cassandra.apache.org/_/development/code_style.html. Always defer to it when in doubt.

General style conventions:
- 4-space indentation, no tabs.
- Match existing code style in the file you are editing.
- All new files must include the Apache License 2.0 header.
- Concise English documentation is required for complex classes and methods; trivial ones may not require them.

## Git Workflow
- Do NOT commit unless explicitly asked.
- Commit messages format. For example:

    ```
    <One sentence description, usually Jira title or CHANGES.txt summary>

    <Optional lengthier description>
  
    Assisted-by: AGENT_NAME:MODEL_VERSION
    ```


## Boundaries

- 🚫 Never modify generated files in `src/gen-java/` — they are built from `src/antlr/` grammars.
- 🚫 Never modify files in `lib/` — these are managed dependencies.
- 🚫 Never commit secrets, credentials, or API keys.
- 🚫 Never run the full test suite — it takes hours. Run targeted tests only.
- 🚫 Never bypass Checkstyle violations without a suppression comment explaining why.
- ⚠️ Ask before modifying the CQL grammar (`src/antlr/Cql.g`) — changes cascade widely.