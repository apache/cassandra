# Cassandra Unit Test Docker Setup

This Docker Compose setup allows running Cassandra unit tests in an isolated environment with all required dependencies (Java 11 and Ant).

## Prerequisites

- Docker
- Docker Compose

## Usage

### Run the full test suite

```bash
cd test/docker
docker compose up --build
```

### Run a specific test

```bash
cd test/docker
TEST_NAME_REGEX=GarbageCollectRepairedSSTablesTest docker compose up --build
```

### Run multiple specific tests

```bash
cd test/docker
TEST_NAME_REGEX="GarbageCollectRepairedSSTablesTest,NeverPurgeTest" docker compose up --build
```

### Clean up

```bash
docker compose down
```

## Test for the ConcurrentModificationException Fix

To verify the fix for the ConcurrentModificationException bug in `performGarbageCollection()`:

```bash
TEST_NAME_REGEX=GarbageCollectRepairedSSTablesTest docker compose up --build
```

This test validates that garbage collection works correctly with mixed repaired/unrepaired SSTables when `only_purge_repaired_tombstones=true`.
