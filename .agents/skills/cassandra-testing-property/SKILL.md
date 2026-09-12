---
name: cassandra-testing-property
description: Write property-based tests for Apache Cassandra using the Property.qt() framework. Use when implementing new tests, validating invariants, testing serialization round-trips, or verifying algebraic properties of code.
---

# Property-Based Testing with `qt()`

Write property-based tests for Apache Cassandra using the `Property.qt()` framework. This framework lives in the Accord module but is used extensively across both Accord and Cassandra test suites.

## When to Use

- Writing tests that validate properties/invariants hold across random inputs
- Testing serialization/deserialization round-trips
- Verifying algebraic laws (idempotency, commutativity, associativity)
- Testing with randomly-generated complex types (tables, keyspaces, types)
- Replacing hand-written example-based tests with broader coverage

## Key Concepts

### Anatomy of a Property Test

Every property test has:
1. **Source of Randomness** - A seed-based `RandomSource` for reproducibility
2. **One or more Properties** - Assertions that must hold for all generated values
3. **Multiple Examples** - Default 1000 iterations per test
4. **Reproducibility** - Seeds are reported on failure for exact replay
5. **Data Generation** - `Gen<T>` instances produce random values

### Entry Point: `qt()`

```java
import static accord.utils.Property.qt;
```

Returns a `ForBuilder` for configuring and running property tests.

### Configuration Methods

| Method | Purpose | Default |
|--------|---------|---------|
| `withSeed(long)` | Pin seed for reproducing a failure | random |
| `withExamples(int)` | Number of test iterations | 1000 |
| `withPure(boolean)` | Fresh seed per example (reproducible) | true |
| `withTimeout(Duration)` | Wall-clock timeout for the entire test | none |

### Test Input Generation

- `qt().check(rs -> {...})` - Raw `RandomSource`, drive generation inline
- `qt().forAll(Gen<T>).check(value -> {...})` - Single typed generator
- `qt().forAll(Gen<A>, Gen<B>).check((a, b) -> {...})` - Two generators
- `qt().forAll(Gen<A>, Gen<B>, Gen<C>).check((a, b, c) -> {...})` - Three generators

### Generators (`Gen<T>`)

There are **two generator systems** in Cassandra's test code:

1. **Accord generators** (`accord.utils.Gen<T>`) - The new system, used with `Property.qt()`. Interface: `T next(RandomSource rs)`
2. **QuickTheories generators** (`org.quicktheories.core.Gen<T>`) - The old system. Interface: `T generate(RandomnessSource rs)`

Most Cassandra-specific generators in `test/unit/` still use the QuickTheories `Gen` type for historical reasons, but are fully usable with `Property.qt()` via bridge functions.

#### Bridge between old and new generators

In `test/unit/org/apache/cassandra/utils/Generators.java`:

```java
// Convert QuickTheories Gen to Accord Gen (for use in qt().forAll(...))
accord.utils.Gen<T> accordGen = Generators.toGen(quickTheoriesGen);

// Convert Accord Gen to QuickTheories Gen (for use with old APIs)
org.quicktheories.core.Gen<T> qtGen = Generators.fromGen(accordGen);
```

#### Accord `Gens` utility class (`accord.utils.Gens`)

Common generators:
- `Gens.ints().between(lo, hi)` / `Gens.ints().all()`
- `Gens.longs().between(lo, hi)` / `Gens.longs().all()`
- `Gens.bools().all()`
- `Gens.enums().all(MyEnum.class)`
- `Gens.strings().all().ofLengthBetween(min, max)`
- `Gens.lists(itemGen).ofSizeBetween(min, max)`
- `Gens.lists(itemGen).unique().ofSize(n)`
- `Gens.arrays(Class, itemGen).ofSizeBetween(min, max)`
- `Gens.oneOf(value1, value2, ...)` - Pick from fixed set
- `Gens.constant(value)` - Always same value
- `Gens.pick(collection)` - Pick random element from collection
- `Gens.pick(Map<T, Integer>)` - Weighted random pick
- `Gens.random()` - A `Gen<RandomSource>` (the RandomSource itself)

Meta-randomness (generators of generators):
- `Gens.enums().allMixedDistribution(MyEnum.class)` - Returns `Gen<Gen<MyEnum>>`
- `Gens.mixedDistribution(int min, int max)` - Returns `Gen<Gen.IntGen>` for int ranges
- `Gens.mixedDistribution(long min, long max)` - Returns `Gen<Gen.LongGen>` for long ranges
- `Gens.mixedDistribution(T... list)` / `Gens.mixedDistribution(List<T> list)` - Returns `Gen<Gen<T>>` for selecting from a collection
- `Gens.mixedDistribution(int[] list)` - Returns `Gen<Gen.IntGen>` for selecting from an int array
- `Gens.bools().mixedDistribution()` - Returns `Gen<Gen<Boolean>>`

Each call to the outer `Gen` picks a *distribution strategy* (uniform, median-biased, zipfian, random-weight), then returns an inner `Gen` that selects values using that strategy. This means **each test example exercises a different distribution**, dramatically improving coverage.

**Why meta-randomness matters:** Without it, uniform random selection is unlikely to expose bugs that require specific patterns (e.g., mostly-deletes, heavily-skewed key access, or clustered ranges). A single test run with 500 examples will exercise ~500 different distributions, some of which will naturally create adversarial patterns that would take millions of uniform random iterations to hit by chance. See the "Role of Bias" section below for more details.

Composing generators:
```java
Gen<String> stringGen = Gens.ints().between(0, 10).map(i -> "Number: " + i);
Gen<Integer> evenNumbers = Gens.ints().between(0, 100).filter(i -> i % 2 == 0);
Gen<Gen<WaitingOn>> distro = rs -> AccordGens.waitingOn(...);
Gen<WaitingOn> flattened = Gens.flatten(distro); // flatten Gen<Gen<T>> to Gen<T>
```

#### Cassandra Accord-style generators (`test/unit/org/apache/cassandra/utils/AccordGenerators.java`)

Generators using Accord's `Gen<T>` type, directly usable with `Property.qt()` (no bridge needed):
- `AccordGenerators.byteArray(Gen.IntGen sizeGen)` - Random `byte[]` with size from generator; fills 8 bytes at a time via `nextLong()` for efficiency
- `AccordGenerators.byteArrayOfSize(int size)` - Random `byte[]` of fixed size
- `AccordGenerators.keys()` / `AccordGenerators.keys(partitioner)` - `PartitionKey` generators
- `AccordGenerators.routingKeysGen(partitioner)` - `TokenKey` generators
- `AccordGenerators.range(partitioner)` - `Range` generators
- `AccordGenerators.ranges(partitioner)` - `Ranges` generators
- `AccordGenerators.commands()` - Random `Command` objects
- `AccordGenerators.topologyGen(partitioner)` - Random `Topology` objects

#### Cassandra generators (`test/unit/org/apache/cassandra/utils/Generators.java`)

General-purpose generators (QuickTheories `Gen` type - use `Generators.toGen()` to convert):
- `Generators.IDENTIFIER_GEN` - Valid CQL identifiers (1-50 chars, regex word)
- `Generators.SYMBOL_GEN` - Identifiers that aren't reserved keywords
- `Generators.DNS_DOMAIN_NAME` - Valid DNS domain names
- `Generators.UTF_8_GEN` - Valid UTF-8 strings (0-1024 chars)
- `Generators.UUID_RANDOM_GEN` - Version 4 UUIDs
- `Generators.UUID_TIME_GEN` - Version 1 (time) UUIDs
- `Generators.INET_ADDRESS_GEN` - Mixed IPv4/IPv6 addresses
- `Generators.INET_4_ADDRESS_GEN` / `Generators.INET_6_ADDRESS_GEN`
- `Generators.TIMESTAMP_GEN` - Timestamps within +/- 50 years of 2020
- `Generators.DATE_GEN` - Java `Date` objects
- `Generators.TIMESTAMP_NANOS` - Nanosecond timestamps
- `Generators.SMALL_TIME_SPAN_NANOS` / `Generators.TINY_TIME_SPAN_NANOS`
- `Generators.bytes(min, max)` - Random `ByteBuffer` (heap)
- `Generators.bytesAnyType(min, max)` - Random `ByteBuffer` (heap/direct/read-only)
- `Generators.bigInt()` / `Generators.bigDecimal()`
- `Generators.timeUUID()` - `TimeUUID` objects
- `Generators.list(gen, sizeGen)` / `Generators.set(gen, sizeGen)` / `Generators.uniqueList(gen, sizeGen)`

#### Type generators (`test/unit/org/apache/cassandra/utils/AbstractTypeGenerators.java`)

For generating CQL types and values for those types:
- `AbstractTypeGenerators.typeGen()` - All `AbstractType` implementations (depth 3)
- `AbstractTypeGenerators.safeTypeGen()` - Types safe for byte-comparable round-trips
- `AbstractTypeGenerators.primitiveTypeGen()` - Only primitive types
- `AbstractTypeGenerators.getTypeSupport(type)` - Returns a `TypeSupport<T>` with a value generator + comparator for any type
- `AbstractTypeGenerators.builder()` - Fine-grained `TypeGenBuilder`:
  ```java
  AbstractTypeGenerators.builder()
      .withoutEmpty()
      .withoutPrimitive(DurationType.instance)
      .withMaxDepth(1)
      .withDefaultSizeGen(2)
      .withTypeKinds(TypeKind.PRIMITIVE, TypeKind.LIST, TypeKind.MAP)
      .build();
  ```
- `AbstractTypeGenerators.withoutUnsafeEquality()` - Remove types with problematic equality (Empty, Duration, Decimal, Counter)

#### Cassandra domain generators (`test/unit/org/apache/cassandra/utils/CassandraGenerators.java`)

Higher-level Cassandra objects (QuickTheories `Gen` type):
- `CassandraGenerators.TABLE_METADATA_GEN` - Random table schemas
- `CassandraGenerators.regularTable()` - Builder for table metadata:
  ```java
  CassandraGenerators.regularTable()
      .withKeyspaceName("ks")
      .withSimpleColumnNames()
      .withPartitionColumnsCount(1)
      .withClusteringColumnsBetween(0, 3)
      .withRegularColumnsBetween(1, 5)
      .withKnownMemtables()
      .build();
  ```
- `CassandraGenerators.regularKeyspace()` - Builder for keyspace metadata
- `CassandraGenerators.INET_ADDRESS_AND_PORT_GEN` - `InetAddressAndPort`
- `CassandraGenerators.TABLE_ID_GEN` - `TableId`
- `CassandraGenerators.CLUSTERING_GEN` - `Clustering<?>`
- `CassandraGenerators.MESSAGE_GEN` - Internode `Message<?>`
- `CassandraGenerators.compactionParamsGen()` / `compressionParamsGen()` / `cachingParamsGen()`
- `CassandraGenerators.token(partitioner)` - Tokens for a given partitioner
- `CassandraGenerators.partitionKeyDataGen(metadata)` - Random partition key bytes for a table

### Reproducing Failures

When a property test fails, the `PropertyError` message includes a `Seed = <value>`. To reproduce:

```java
// Add .withOnlySeed() to replay just the failing case (sets examples=1)
qt().withOnlySeed(3448125481938895569L).forAll(Gens.ints().all()).check(value -> {
    // will reproduce the exact same failing input, nothing more
});
```

- Use `withOnlySeed(seed)` (not `withSeed(seed)`) - it limits examples to 1 so you only run the failing case rather than continuing with more random seeds after
- If `Pure = true` (default): the seed fully determines the failing input - reproduction is guaranteed
- If `Pure = false`: side effects may prevent exact reproduction (e.g., timing, external state).  In these cases prefer `withSeed` as the actual failing seed might not be the one listed, but one of the later examples
- The `Values:` section in the error shows the specific generated inputs that triggered the failure - use these to understand the root cause
- When filing a bug or writing a regression test, always include the seed

## Patterns

### Pattern 1: Basic property with raw RandomSource
```java
qt().check(rs -> {
    int value = rs.nextInt(1, 100);
    assertThat(value / value).isEqualTo(1);
});
```

### Pattern 2: Single generator with forAll
```java
qt().forAll(Gens.ints().all()).check(value -> {
    assertThat(value / value).isEqualTo(1);
});
```

### Pattern 3: Serialization round-trip

For most Cassandra serializers, use `org.apache.cassandra.io.Serializers.testSerde()` which automates the full round-trip:
- Serializes the input
- Verifies `serializedSize()` matches actual bytes written
- Deserializes and checks equality (using `ReflectionUtils.recursiveEquals` for diff reporting)
- Verifies `skip()` consumes exactly the right number of bytes
- Tests `ByteBuffer`-based serialize/deserialize methods if overridden

You only need to provide a generator for random input and the serializer:

```java
// Shared buffer across examples for efficiency
@SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" })
DataOutputBuffer output = new DataOutputBuffer();

qt().forAll(myObjectGen).check(obj -> {
    // Unversioned serializer
    Serializers.testSerde(output, MyObject.serializer, obj);
});

qt().forAll(myObjectGen).check(obj -> {
    // Versioned serializer - test across all supported versions
    for (MessagingService.Version version : MessagingService.Version.supportedVersions())
        Serializers.testSerde(output, MyObject.serializer, obj, version.value);
});

qt().forAll(myObjectGen).check(obj -> {
    // Parameterised serializer
    Serializers.testSerde(output, MyObject.serializer, obj, param);
});
```

The `Serializers` utility supports:
- `AsymmetricUnversionedSerializer<T, T>` - basic unversioned
- `IVersionedAsymmetricSerializer<T, T>` - versioned with `int` version
- `AsymmetricVersionedSerializer<T, T, Version>` - versioned with typed version
- `ParameterisedUnversionedSerializer<T, P>` - with extra parameter
- `ParameterisedVersionedSerializer<T, P, Version>` - parameterised + versioned

For non-standard serde (e.g., `ByteComparable`), write the round-trip manually:

```java
qt().forAll(tokenGen).check(token -> {
    ByteComparable comparable = asComparableBytes(token);
    Token read = fromComparableBytes(token.getPartitioner(), comparable);
    assertThat(read).isEqualTo(token);
});
```

### Pattern 4: Algebraic laws (merge idempotency, commutativity, associativity)
```java
// Idempotency
qt().forAll(snapshotGen).check(s -> s.merge(s).equals(s));
// Commutativity
qt().forAll(snapshotGen, snapshotGen).check((a, b) -> a.merge(b).equals(b.merge(a)));
// Associativity
qt().forAll(snapshotGen, snapshotGen, snapshotGen)
    .check((a, b, c) -> a.merge(b).merge(c).equals(a.merge(b.merge(c))));
```

### Pattern 5: Impure test (shared state across examples)

By default (`pure=true`), each example gets a fresh seed derived from the previous - making every single example independently reproducible. Use `withPure(false)` when examples share mutable state that accumulates across iterations, making individual re-seeding meaningless:

```java
qt().withPure(false).withExamples(10).check(rs -> {
    // Cluster persists across examples - state from example N affects example N+1
    Cluster.Node coordinator = cluster.nextCoordinator(rs);
    RepairCoordinator repair = coordinator.repair(repairOption(rs, coordinator));
    repair.run();
    cluster.processAll();
    assertSuccess(repair);
});
```

When `pure=false`, the seed in a failure report may not fully reproduce the issue since it depends on accumulated side effects from prior examples.

### Pattern 6: Meta-randomness (distribution varies per example)
```java
Gen<Gen<Action>> ACTION_DISTRO = Gens.enums().allMixedDistribution(Action.class);
qt().check(rs -> {
    Gen<Action> gen = ACTION_DISTRO.next(rs); // pick distribution for this example
    for (int i = 0; i < 1000; i++) {
        Action action = gen.next(rs);
        // each example exercises a different distribution of actions
    }
});
```

### Pattern 7: Timeout-bounded
```java
qt().withTimeout(Duration.ofSeconds(60)).check(rs -> {
    // Long-running fuzz test bounded by wall clock
});
```

### Pattern 8: Model-based validation

For simple cases, you can inline a model comparison loop directly in `qt().check()`:

```java
qt().check(rs -> {
    TreeMap<Integer, Integer> model = new TreeMap<>();
    MyDataStructure sut = new MyDataStructure();
    for (int i = 0; i < 1000; i++) {
        int key = rs.nextInt();
        int value = rs.nextInt();
        model.put(key, value);
        sut.put(key, value);
        assertThat(sut.get(key)).isEqualTo(model.get(key));
    }
});
```

If you need multiple operation types (create/read/update/delete), conditional commands, or better failure diagnostics (command history, step-level reporting), consider using `Property.stateful()` instead - see the **cassandra-testing-stateful** skill which is purpose-built for model-based stateful testing.

## Role of Bias (Why Meta-Randomness is Critical)

Property tests with **uniform random distribution** are unlikely to find many real bugs because:

1. **Most types have large domains** - If your int range is [0, 1M], the chance of hitting 0 (a common edge case) is 1/1,000,000 per attempt
2. **Edge cases cluster at boundaries** - Off-by-one errors only manifest at specific values
3. **Relationships between operations matter** - A bug might only appear when you do 90% deletes and 10% inserts, or when the same key is accessed repeatedly
4. **Data structure behavior changes with shape** - A B-tree behaves differently when heavily left-skewed vs balanced

**Meta-randomness solves this** by varying the distribution *itself* across test examples:

```java
// BAD: Uniform - every action equally likely every time
Gen<Action> actionGen = Gens.enums().all(Action.class);

// GOOD: Meta-random - some examples will be 90% Create, others 90% Delete, etc.
Gen<Gen<Action>> actionDistro = Gens.enums().allMixedDistribution(Action.class);
qt().check(rs -> {
    Gen<Action> gen = actionDistro.next(rs); // pick distribution for THIS example
    // now use gen.next(rs) for each step...
});
```

The available distribution strategies include:
- **Uniform** - Equal probability (the baseline)
- **Median-biased** - Values cluster around a randomly-chosen median
- **Zipfian** - Power-law distribution (a few values dominate, most are rare)
- **Random-weight** - Each value gets a random weight, creating arbitrary skew

This applies to numeric ranges too:
```java
// For selecting from a range with varying distribution
Gen<Gen.IntGen> sizeDistro = Gens.mixedDistribution(1, 10000);
qt().check(rs -> {
    Gen.IntGen sizeGen = sizeDistro.next(rs);
    // some examples will mostly pick small sizes, others large, others clustered around a median
    for (int i = 0; i < 100; i++) {
        int size = sizeGen.nextInt(rs);
        // ...
    }
});
```

**Real example**: The `StatefulRangeTreeTest` bug was only found when bias caused a specific sequence (Create, Clear, Create, RangeRead) - with uniform distribution, Clear is equally weighted with other operations, making it extremely rare to see Clear followed immediately by Create. A zipfian or heavily-skewed distribution naturally produces these adversarial sequences.

## Imports

```java
import static accord.utils.Property.qt;
import accord.utils.Gens;
import accord.utils.Gen;
import accord.utils.RandomSource;
```

## Key File Locations

- `modules/accord/accord-core/src/test/java/accord/utils/Property.java` - Core `qt()` / `stateful()` framework
- `modules/accord/accord-core/src/test/java/accord/utils/Gens.java` - Accord generator utilities
- `test/unit/org/apache/cassandra/utils/AccordGenerators.java` - Cassandra-specific Accord-style generators (byte arrays, keys, ranges, topologies) - use directly with `qt().forAll()`
- `test/unit/org/apache/cassandra/utils/Generators.java` - General-purpose generators (UUID, InetAddress, DNS, timestamps, bytes, BigInteger, etc.) + `toGen()`/`fromGen()` bridge
- `test/unit/org/apache/cassandra/utils/AbstractTypeGenerators.java` - CQL type generators + `getTypeSupport()` for generating values of any type
- `test/unit/org/apache/cassandra/utils/CassandraGenerators.java` - High-level domain generators (TableMetadata, KeyspaceMetadata, Messages, Tokens, compaction/compression params)
- `test/unit/org/apache/cassandra/io/Serializers.java` - Automated serde test utility

## Important Notes

- Some older tests use `org.quicktheories.QuickTheory.qt` (a different library with different API) - **new tests should use the Accord `Property.qt()`**
- When using QuickTheories generators with `Property.qt()`, wrap them with `Generators.toGen(qtGen)` 
- Always write properties that are **true for all valid inputs**, not just specific examples
- When a test fails, always include the seed in bug reports so it can be replayed with `withSeed()`

## Deterministic Execution Requirement

**All randomness in a property test must flow through `RandomSource` for failures to be reproducible via seed.**

Before writing a property test, audit the system under test for internal randomness sources:

| Internal randomness source | Impact | Mitigation |
|---|---|---|
| `ThreadLocalRandom` | Seed replay won't reproduce same SUT behavior | Refactor SUT to accept random source, or document limitation |
| `Math.random()` | Same as above | Same as above |
| `System.nanoTime()` / `System.currentTimeMillis()` | Timing-dependent behavior varies across runs | Mock time source or accept non-reproducibility |
| `UUID.randomUUID()` | Different UUIDs each run | Generate UUIDs from `RandomSource` |
| `Collections.shuffle(list)` | Uses default `Random` | Use `Collections.shuffle(list, random)` with seeded random |
| **Non-deterministic iteration order** | `HashMap`, `HashSet`, `IdentityHashMap` iteration order varies across JVM versions, boxes, and even runs | Use `LinkedHashMap`/`LinkedHashSet` (insertion order), `TreeMap`/`TreeSet` (sorted order), or sort before iterating |

**How to check**: Do NOT rely on re-running the test on the same machine to verify determinism. Sources like `ThreadLocalRandom` often produce the same sequence within the same JVM process or even across restarts on the same box/JVM version, giving a false sense of reproducibility. The non-determinism only surfaces when running on a different machine, a different JVM version, or under different thread scheduling -- exactly the conditions of a CI failure that someone else needs to debug.

Instead, **audit the source code** of the SUT for uses of `ThreadLocalRandom`, `Math.random()`, `Random()` (unseeded), `UUID.randomUUID()`, `System.nanoTime()`, or any other randomness source that does not flow from the test's `RandomSource`. Grep for these in the SUT class and its transitive dependencies. Also check for non-deterministic iteration order (see below).

**When the SUT cannot be made deterministic** (e.g., it uses `ThreadLocalRandom` internally and refactoring is too invasive):
1. Add a comment explaining which internal randomness is not controlled
2. Consider using `withPure(false)` since per-example reproducibility is already limited
3. Document this in the test's Javadoc so future debuggers know seed replay may not fully reproduce failures

**Example of the problem**: `DynamicList.randomLevel()` uses `ThreadLocalRandom` to determine skip-list level assignments. A property test for `DynamicList` can reproduce the *sequence of operations* from a seed, but not the internal structure of the skip list, because level assignments are non-deterministic. A failure caused by a specific level assignment pattern cannot be replayed.

### Non-deterministic iteration order (subtle)

Collections with unstable iteration order are a particularly insidious source of non-reproducibility because they don't involve any explicit randomness API. `HashMap` and `HashSet` iteration order depends on internal hash table sizing, which can change between JVM versions, machines, or even runs with different heap sizes. If the SUT iterates over a `HashMap` and the iteration order affects behavior (e.g., which element gets processed first, tie-breaking, output ordering), the test becomes non-reproducible across environments even though no random API is called.

Common patterns that break reproducibility:
- `for (var entry : hashMap.entrySet())` where processing order matters
- `hashSet.stream().findFirst()` -- which element is "first" is undefined
- `new ArrayList<>(hashMap.values())` -- list order depends on iteration order
- Building output by iterating a `HashMap` and concatenating/accumulating results
- Any algorithm where the result depends on which element is visited first (e.g., selecting a "best" candidate from a map by iterating and comparing)

This is especially dangerous because it often works on the developer's machine and only breaks in CI or on a colleague's box. Unlike `ThreadLocalRandom`, there is no API call to grep for -- you need to understand whether iteration order affects the SUT's observable behavior.

If you can not change the data structures to be iteration safe, export the iteration to a list and add deterministic sorting to solve the problem.
