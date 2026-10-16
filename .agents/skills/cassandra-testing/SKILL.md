---
name: cassandra-testing
description: Guide for writing tests in Apache Cassandra. Use whenever writing new tests, adding test coverage, or deciding what testing approach to use for a change. Prioritizes property-based and stateful property testing over hand-written example-based tests.
---

# Cassandra Testing Strategy

When writing tests for Apache Cassandra, prefer property-based testing over hand-written example-based tests. Property tests generate thousands of random inputs and catch edge cases that humans miss.

## Decision Flow

Ask these questions about what you're testing:

### 1. Does it involve sequences of operations on mutable state?

Examples: data structures (trees, indexes, caches), CRUD workflows, schema mutations, topology changes, journal lifecycle (flush/compact/restart).

**Use the `cassandra-testing-stateful` skill** - it provides `Property.stateful()`, which gives you:
- Random command sequences with weighted selection
- Conditional commands (only delete when non-empty)
- Model-based verification (compare SUT against a simple oracle)
- Full command history in failure reports for debugging
- Lifecycle hooks (destroyState, destroySut, onSuccess)

### 2. Does it validate a property/invariant?

Examples: serialization round-trips, algebraic laws (idempotency, commutativity), encoding correctness, range/boundary behavior, type validation.

**Use the `cassandra-testing-property` skill** - it provides `Property.qt()` with typed generators, which gives you:
- `forAll(gen).check(value -> ...)` for clean property assertions
- `Serializers.testSerde()` for automated serialization testing
- Rich generator library (Accord `Gens`, Cassandra `Generators`, `AbstractTypeGenerators`, `CassandraGenerators`)
- Meta-randomness (`mixedDistribution`) for bias-aware testing

### 3. Even "simple" tests benefit from property testing

A test that looks simple often hides complexity. A straightforward `INSERT INTO ... SELECT * FROM` test seems trivial with `(pk int, v int)`, but breaks with `(pk varint, ck frozen<udt>, v int, PRIMARY KEY(pk, ck)) WITH CLUSTERING ORDER BY (ck DESC)`. The schema, types, clustering order, and value expressions all interact in ways no human enumerates completely.

Rather than writing a single example and hoping it covers enough, generate the schema and values randomly:

```
// pseudocode
qt().check(rs -> {
    schema = randomSchema(rs)       // random types, clustering order, indexes, etc.
    row    = randomRow(rs, schema)  // random values matching the schema
    insert(schema, row)
    result = select(schema, row.pk)
    assertEqual(result, row)
});
```

This approach finds edge cases across type combinations, encoding paths, and query planning that hand-written examples miss entirely.

## Why Property Tests Over Example Tests

- A single `qt().forAll(gen).check(...)` replaces dozens of hand-written test cases
- Random generation finds edge cases humans don't think of (boundary values, empty inputs, overflow, type interactions)
- Meta-randomness varies the distribution per example, catching bugs that require specific patterns (e.g., mostly-deletes followed by a read)
- Failures are reproducible via seed - just add `withOnlySeed(seed)` to replay
- The generator libraries already exist for most Cassandra types - you just compose them
