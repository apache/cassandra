# Property-Based Testing for Repros

Property-based testing (PBT) is the preferred repro form when the bug is about an invariant. The framework generates random inputs, checks the property, and automatically shrinks counterexamples to minimal failures.

## When to use PBT as the repro form

- **Round-trip bugs**: `deserialize(serialize(x)) == x` fails for some inputs
- **Idempotence bugs**: `f(f(x)) != f(x)` for some operation
- **Commutativity bugs**: `f(g(x)) != g(f(x))` when they should commute
- **Ordering bugs**: sort, merge, or priority violations
- **Bounded sum bugs**: values exceed expected bounds
- **Monotonicity bugs**: a metric that should only increase decreases
- **Equivalence bugs**: two implementations of the same spec disagree
- **Model-based bugs**: a simple reference model disagrees with the real implementation

## Key advantage: automatic shrinking

When PBT finds a counterexample, it automatically shrinks it to a minimal failing case. This replaces Phase 5 manual minimization for the input/trigger dimension.

### Integrated shrinking (Hypothesis, proptest) vs type-based shrinking (QuickCheck, jqwik)

- **Integrated shrinking** (Hypothesis for Python, proptest for Rust): shrinking happens at the generator/strategy level, inside the generation process. Invariants from `filter()` and `assume()` are respected during shrinking. This produces smaller, valid counterexamples.
- **Type-based shrinking** (QuickCheck for Haskell, jqwik for Java, quickcheck for Rust): shrinking happens at the type level, after generation. May produce invalid inputs that violate generator constraints, requiring extra `assume()` guards.

Prefer integrated shrinking (Hypothesis, proptest) when available. When using type-based shrinking, add explicit `assume()` checks to filter invalid shrunk inputs.

## Framework reference

| Language | Framework | Shrinking | Seed persistence |
|----------|-----------|-----------|-----------------|
| Python | Hypothesis | Integrated | `.hypothesis/` directory |
| Rust | proptest | Integrated | `proptest-regressions/` files |
| Rust | quickcheck | Type-based | Manual seed printing |
| Java | jqwik | Type-based | `.jqwik-database` file |
| Java | QuickTheories | Type-based | Seed in failure message |
| Haskell | QuickCheck | Type-based | `--seed` flag |
| Go | gopter | Type-based | Seed in failure message |
| Scala | ScalaCheck | Type-based | Seed in failure message |
| Clojure | test.check | Type-based | `:seed` in result map |

## Common property patterns

### Round-trip (bijection)
```
for all x:
    decode(encode(x)) == x
```

### Idempotence
```
for all x:
    f(f(x)) == f(x)
```

### Commutativity
```
for all x, y:
    f(x, y) == f(y, x)
```

### Associativity
```
for all x, y, z:
    f(f(x, y), z) == f(x, f(y, z))
```

### Monotonicity
```
for all x, y where x <= y:
    f(x) <= f(y)
```

### Invariant preservation
```
for all state, operation:
    if invariant(state):
        invariant(apply(operation, state))
```

### Oracle / model comparison
```
for all inputs:
    real_implementation(inputs) == simple_model(inputs)
```

### Metamorphic relation
```
for all x, transform:
    f(transform(x)) == expected_transform(f(x))
```

## Stateful (model-based) testing

For bugs that require a sequence of operations, use stateful PBT. The framework generates random operation sequences, applies them to both a real system and a simple model, and checks that they agree after each step.

```
model = SimpleModel()
real = RealSystem()

for operation in generated_operation_sequence:
    model_result = model.apply(operation)
    real_result = real.apply(operation)
    assert model_result == real_result, f"diverged at {operation}"
```

Frameworks with stateful testing support:
- Hypothesis: `RuleBasedStateMachine`
- jqwik: `@Property` with `ActionSequence`
- proptest: manual state machine via `proptest!` macro
- QuickTheories: manual, using `withExamples()`

## Seed management

Every PBT failure has a seed. Seeds are essential for:
1. **Reproducibility**: re-run with the same seed to get the same counterexample
2. **Regression**: store the seed so the counterexample is tested in CI
3. **Debugging**: step through the exact failing scenario

Best practices:
- Always print the seed on failure (most frameworks do this automatically)
- Store regression seeds in source control (Hypothesis and proptest do this automatically)
- Include the seed in the repro README when sharing

## Danger: shrinking into a different bug

When PBT shrinks a counterexample, it may shrink into a *different* property violation than the original. This is the "shrinkers are fuzzers" problem.

Mitigation:
- Use tight, specific properties (not just "doesn't crash")
- After shrinking, verify the failure message matches the original
- If using integrated shrinking, this is less likely because the shrinker respects generator constraints
