---
name: cassandra-testing-stateful
description: Write stateful property-based tests for Apache Cassandra using the Property.stateful() framework. Use when verifying systems behave correctly through sequences of operations, modeling interactions with stateful systems, or testing CRUD/state-machine correctness with random command sequences.
---

# Stateful Property-Based Testing with `stateful()`

Write stateful property-based tests for Apache Cassandra using the `Property.stateful()` framework. This is the "CRUD testing" or "model-based testing" approach where random command sequences exercise state machines and verify invariants hold throughout.

## When to Use

- Testing stateful data structures (trees, indexes, caches) against a simple model
- Verifying CRUD operations maintain consistency
- Testing multi-step sequences (topology changes, schema mutations, journal operations)
- Testing systems that survive restart/flush/compact cycles
- Any scenario where you need to verify a sequence of operations, not just individual properties

## Key Concepts

### Entry Point: `stateful()`

```java
import static accord.utils.Property.stateful;
import static accord.utils.Property.commands;
```

Returns a `StatefulBuilder` for configuring and running stateful property tests.

### Configuration

Inherits from `qt()` plus:

| Method | Purpose | Default |
|--------|---------|---------|
| `withExamples(int)` | Number of full test runs | 500 |
| `withSteps(int)` | Max commands per test run | 1000 |
| `withStepTimeout(Duration)` | Timeout per individual step | none |
| `withSeed(long)` | Pin seed for reproducibility | random |
| `withPure(boolean)` | Fresh seed per example | true |
| `withTimeout(Duration)` | Timeout for entire example | none |

### Core Types

- **`State`** - Model/tracking state for the test (e.g., a `TreeMap` as oracle)
- **`SystemUnderTest`** (SUT) - The actual implementation being tested (optional - often `Void` when State acts as both)
- **`Command<State, SystemUnderTest, Result>`** - A single operation applied to both model and SUT

### Command Interface

```java
interface Command<State, SystemUnderTest, Result> {
    // Guard: should this command execute given current state?
    default PreCheckResult checkPreconditions(State state) { return PreCheckResult.Ok; }
    // Apply to the model - return expected result
    Result apply(State state) throws Throwable;
    // Apply to the real system - return actual result
    Result run(SystemUnderTest sut) throws Throwable;
    // Verify model and SUT agree
    default void checkPostconditions(State state, Result expected,
                                     SystemUnderTest sut, Result actual) throws Throwable {}
    // Human-readable description for history logging
    default String detailed(State state) { return this.toString(); }
}
```

Convenience interfaces:
- **`UnitCommand<State, SUT>`** - When apply/run return void (use `applyUnit`/`runUnit`)
- **`StateOnlyCommand<State>`** - When there's no separate SUT (extends `UnitCommand<State, Void>`)
- **`SimpleCommand<State>`** - Inline lambda for `StateOnlyCommand` with a name

### Commands Builder

```java
commands(() -> stateGen)              // State-only (no SUT)
commands(() -> stateGen, Sut::new)    // State + separate SUT
```

Builder methods:
- `.add(cmd)` / `.add(gen)` / `.add((rs, state) -> cmd)` - Add command with random weight
- `.add(weight, cmd)` - Add command with fixed weight
- `.addIf(predicate, cmd)` - Conditional command (only when predicate is true)
- `.addAllIf(predicate, builder -> {...})` - Multiple conditional commands
- `.preCommands(state -> {...})` - Run before each step
- `.destroyState((state, cause) -> {...})` - Cleanup state after each example
- `.destroySut((sut, cause) -> {...})` - Cleanup SUT after each example
- `.onSuccess((state, sut, history) -> {...})` - Callback on successful example
- `.onFailure((state, sut, history, cause) -> {...})` - Callback on failure
- `.commandsTransformer((state, gen) -> newGen)` - Transform command generator per state
- `.build()` - Produce the `Commands` object

## Patterns

### Pattern 1: State-only with SimpleCommand (most common, simplest)

When the State IS the system under test (model + SUT combined).

```java
import static accord.utils.Property.stateful;
import static accord.utils.Property.commands;

stateful().check(commands(() -> State::new)
    .add(MyTest::insertCommand)
    .add(MyTest::readCommand)
    .addIf(s -> !s.isEmpty(), MyTest::deleteCommand)
    .build());

// Command factory method
private static Property.Command<State, Void, ?> insertCommand(RandomSource rs, State state) {
    int key = rs.nextInt();
    int value = rs.nextInt();
    return new Property.SimpleCommand<>(
        "Insert(" + key + ", " + value + ")",
        s -> {
            s.model.put(key, value);
            s.sut.put(key, value);
            assertThat(s.sut.get(key)).isEqualTo(value);
        });
}
```

### Pattern 2: Separate State and SUT with full Command interface

When State (model) and SUT (real implementation) are different objects.

```java
stateful().check(commands(() -> State::new, state -> new Sut(state))
    .add((rs, state) -> new Create(nextRange(rs), rs.nextInt()))
    .add((rs, state) -> new Read(nextRange(rs)))
    .addAllIf(state -> !state.isEmpty(), b -> b
        .add((rs, state) -> new Update(rs.pickOrderedSet(state.keys()), rs.nextInt()))
        .add((rs, state) -> new Delete(rs.pickOrderedSet(state.keys()))))
    .destroyState(State::close)
    .destroySut(Sut::close)
    .build());

// Full Command with apply/run/checkPostconditions
static class Read implements Command<State, Sut, List<Integer>> {
    private final Range range;
    Read(Range range) { this.range = range; }

    @Override public List<Integer> apply(State state) { return state.search(range); }
    @Override public List<Integer> run(Sut sut) { return sut.tree.search(range); }
    @Override public void checkPostconditions(State state, List<Integer> expected,
                                             Sut sut, List<Integer> actual) {
        Assertions.assertThat(actual).isEqualTo(expected);
    }
    @Override public String detailed(State state) { return "Read(" + range + ")"; }
}
```

### Pattern 3: Multistep commands

Group multiple commands as an atomic sequence in history.

```java
import static accord.utils.Property.multistep;

Gen<Command<State, Void, ?>> topologyCommand = rs -> multistep(
    new SimpleCommand<>("Stop Node", s -> s.stopNode(node)),
    new SimpleCommand<>("Replace Host", s -> s.replaceHost(node)),
    new SimpleCommand<>("Reconfigure CMS", s -> s.reconfigureCMS())
);
```

### Pattern 4: Using destroyState for final validation

```java
stateful().withExamples(50).withSteps(500).check(commands(() -> State::new)
    .add(MyTest::addTable)
    .addIf(s -> !s.tables.isEmpty(), MyTest::dropTable)
    .destroyState(state -> {
        // Run after all steps - finish pending work and validate final state
        state.finishPendingSequences();
        state.validateFinalSchema();
    })
    .build());
```

## Error Reporting

On failure, `PropertyError` reports:
- Seed for reproducibility
- Number of examples / steps configured
- The specific failing step number
- State at time of failure (via `toString()`)
- Full command history (numbered, with `detailed()` strings)
- If `withStepTimeout` is used, duration per step is appended to history

## State Design Guidelines

1. **Merged State+SUT** (most common): State holds both the model (e.g., `TreeMap`) and the SUT (e.g., `RangeTree`). Commands operate on both within `applyUnit`.

2. **Separate State and SUT**: Use when the SUT has complex lifecycle (journal, cluster). State owns the model, SUT wraps the real thing. `destroyState`/`destroySut` handle cleanup.

3. **State should implement useful `toString()`**: It appears in error reports.

4. **Commands should have descriptive `detailed(State)`**: These form the history trace shown on failure.

## Imports

```java
import static accord.utils.Property.stateful;
import static accord.utils.Property.commands;
import static accord.utils.Property.multistep;
import static accord.utils.Property.ignoreCommand;
import accord.utils.Property.Command;
import accord.utils.Property.UnitCommand;
import accord.utils.Property.StateOnlyCommand;
import accord.utils.Property.SimpleCommand;
import accord.utils.Property.PreCheckResult;
import accord.utils.Gens;
import accord.utils.Gen;
import accord.utils.RandomSource;
```

## Framework Location

- `modules/accord/accord-core/src/test/java/accord/utils/Property.java` - Core framework
- `modules/accord/accord-core/src/test/java/accord/utils/Gens.java` - Generator utilities
- `modules/accord/accord-core/src/test/java/accord/utils/README.md` - Documentation

## Important Notes

- Default examples for `stateful()` is **500** (not 1000 like `qt()`)
- Default steps per example is **1000**
- Commands with `addIf` are only included when the predicate is true for the current state
- **Prefer `addIf` over `ignoreCommand()`**: Use `addIf(predicate, cmd)` to conditionally include commands rather than having command factories return `Property.ignoreCommand()`. The framework fails the test if too many consecutive commands are ignored, making `ignoreCommand()` flaky-prone. `addIf` avoids this by excluding the command from selection entirely when the predicate is false.
- `onSuccess` and `onFailure` callbacks are useful for logging history on completion
- When writing `detailed()`, include the command parameters (key, range, etc.) for debuggability

## Deterministic Execution Requirement

Stateful tests are especially vulnerable to non-deterministic SUTs because failures depend on the exact sequence of states traversed. If the SUT uses internal randomness (e.g., `ThreadLocalRandom`, `Math.random()`), replaying a seed reproduces the same command sequence but the SUT may take a different internal path, making the failure non-reproducible.

**Before writing a stateful test, audit the SUT for internal randomness.** See the `cassandra-testing-property` skill for a full table of common randomness sources and mitigations.

**Key principle**: Every aspect of the test that affects the outcome must be derived from `RandomSource`. This includes:
- Values generated for commands (covered by the command factory receiving `RandomSource rs`)
- The SUT's internal behavior (NOT covered if the SUT uses `ThreadLocalRandom` etc.)
- Any initialization parameters (covered if `State(RandomSource rs)` constructor is used)
- **Iteration order of collections in the SUT** -- if the SUT iterates a `HashMap`/`HashSet` and the order affects behavior, the test is non-reproducible across machines/JVM versions even though no random API is called

Stateful tests are particularly sensitive to iteration order because a different traversal order can change which command's precondition is met, which element gets selected, or which state transition fires -- producing a completely different state sequence from the same seed. For example, if a command iterates `HashMap.entrySet()` to find the first element matching a condition, a different iteration order means a different element is found, leading to divergent state. See the `cassandra-testing-property` skill for a full list of common patterns that break reproducibility.

If the SUT cannot be made deterministic, document the limitation prominently in the test class Javadoc.
