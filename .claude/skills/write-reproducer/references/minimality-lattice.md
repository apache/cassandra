# Minimality Lattice

A repro sits on a tier. Pick the lowest tier that reliably exhibits the failure. A 300-line Jepsen scenario is more minimal than 2000 lines of ad-hoc bash if the bug requires a 5-node cluster with partitions.

## Tier 1: Expression / REPL snippet

**What**: a single expression or a few lines in a REPL.
**When**: pure-function semantics bugs, e.g., `assert (a+b)+c == a+(b+c)` for floating-point.
**When not**: anything requiring state, I/O, or multiple operations.

Example:
```python
>>> round(2.675, 2)
2.67  # expected 2.68
```

## Tier 2: Single-file snippet with `main`

**What**: one file with a main function, no test framework, no build system.
**When**: logic bugs in a library that can be triggered by calling a function from main. Compiler/optimizer crashes triggered by a single source file.
**When not**: when the bug needs the project's test infrastructure or external services.

Example: a single `.java` file with `public static void main` that calls the buggy method and prints/asserts.

## Tier 3: Single test in the project's framework

**What**: one test method using the project's existing test framework (JUnit, pytest, cargo test, go test).
**When**: most bugs. This is the default tier. The bug can be triggered by calling code within the project's dependency graph.
**When not**: when the bug requires external services, multiple processes, or cluster topology.

This is the sweet spot for most repros. It runs fast, integrates into CI, and uses familiar conventions.

## Tier 4: Multi-file project fixture

**What**: a small project (pom.xml / Cargo.toml + a few source files) that demonstrates the bug.
**When**: build/tool/classpath bugs, or bugs that require specific project configuration. Bugs in build plugins, code generators, or IDE tooling.
**When not**: when a single test file within the existing project suffices.

Example: a minimal Maven project with one pom.xml and one test class that triggers a Surefire plugin bug.

## Tier 5: Docker Compose + test script

**What**: docker-compose.yml defining services + a test script that runs against them.
**When**: integration bugs requiring multiple processes (app + database, app + message queue), but where the processes don't need to form a cluster or have distributed coordination.
**When not**: when testcontainers within a Tier 3 test can provide the same services, or when the bug is about distributed coordination (use Tier 6).

Pin image digests, not `:latest`. Include a `reproduce.sh` that does `docker-compose up -d && sleep-or-poll && run-test && docker-compose down`.

## Tier 6: Ephemeral cluster

**What**: a multi-node cluster test using project-specific or general-purpose distributed test infrastructure.
**When**: bugs involving replication, leader election, failover, consensus, network partitions, distributed coordination, consistency anomalies.
**When not**: when the bug can be triggered on a single instance (always try Tier 3 first).

Frameworks by ecosystem:
- **Cassandra**: in-JVM dtests (`Cluster.build().withNodes(N)...`), CQLTester for single-node
- **General distributed**: Jepsen (Clojure, controls real/Docker nodes, built-in checkers)
- **Any language**: testcontainers (starts real services in Docker, used from test code)
- **Kafka**: `EmbeddedKafkaCluster` or testcontainers-kafka

## Tier 7: Deterministic simulation

**What**: a simulation environment that provides full determinism (same seed = same execution).
**When**: rare races, liveness bugs, fault-tolerance properties that are hard to trigger reliably even with cluster tests. When you need to explore millions of schedules.
**When not**: when the bug reproduces reliably at Tier 6, or when no simulation framework exists for the system.

Frameworks:
- **Cassandra**: CEP-10 cluster and code simulator (`org.apache.cassandra.simulator.*`)
- **Rust**: turmoil (used by S2), madsim (used by RisingWave)
- **General**: Antithesis (commercial, language-agnostic via Docker)
- **FoundationDB**: Flow (C++ actor model with deterministic simulation)
- **TigerBeetle**: VOPR (Viewstamped Operation Replication simulator)

## Decision flowchart

```
Can the bug be triggered by calling one function with known inputs?
  YES → Tier 1-2 (expression or single file)
  NO ↓

Can the bug be triggered within the project's test framework?
  YES → Tier 3 (single test)
  NO ↓

Does the bug require specific project/build configuration?
  YES → Tier 4 (project fixture)
  NO ↓

Does the bug require multiple processes but NOT distributed coordination?
  YES → Tier 5 (docker-compose)
  NO ↓

Does the bug involve distributed coordination, replication, or consensus?
  YES → Is the bug reliably reproducible?
    YES → Tier 6 (ephemeral cluster)
    NO  → Tier 7 (deterministic simulation)
```

Always try one tier lower first. If the bug disappears, that's valuable information — it means the bug requires the complexity you removed.
