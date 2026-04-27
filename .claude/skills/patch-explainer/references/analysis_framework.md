# Code Analysis Framework

This reference provides a structured approach for deeply analyzing code, whether examining changes (patches/diffs) or understanding existing code (classes, subsystems, repositories).

## Analysis Dimensions

Every code analysis should address these dimensions:

### 1. Purpose & Context
- **What**: What does this code do?
- **Why it exists**: What problem does it solve?
- **Where it fits**: How does it relate to surrounding components?

### 2. Structure & Organization
- **Components**: What are the key pieces?
- **Relationships**: How do components interact?
- **Hierarchy**: What are the layers/abstractions?

### 3. Behavior & Flow
- **Data flow**: How does data move through the system?
- **Control flow**: What is the execution path?
- **State transitions**: What states exist and how do they change?

### 4. Assumptions & Invariants
- **Preconditions**: What must be true before execution?
- **Postconditions**: What must be true after execution?
- **Invariants**: What must always be true?

### 5. Failure Modes & Edge Cases
- **What could go wrong**: Identify failure points
- **Error handling**: How are failures managed?
- **Edge cases**: Boundary conditions and corner cases

### 6. Concurrency & Timing
- **Threading model**: Single-threaded, multi-threaded, async?
- **Synchronization**: Locks, atomics, message passing?
- **Race conditions**: Potential data races or ordering issues?
- **Deadlocks**: Circular dependencies in locking?

## Analysis Process for Patches/Diffs

When analyzing changes (git diff, patch file, PR):

### Step 1: Identify the Change Type
Categorize the change:
- **Bug fix**: Correcting incorrect behavior
- **Feature addition**: New functionality
- **Refactoring**: Restructuring without behavior change
- **Performance optimization**: Improving efficiency
- **Technical debt**: Cleanup, documentation, tests

### Step 2: Understand the Before State
- What was the old behavior?
- What was the problem with the old code?
- What assumptions did the old code make?

### Step 3: Understand the After State
- What is the new behavior?
- How does it differ from the old behavior?
- What new assumptions are introduced?

### Step 4: Analyze the Transition
- **Why this change**: What problem does it solve?
- **How it works**: Mechanism of the change
- **Critical modifications**: Which changes are most important?
- **Side effects**: What else is affected?

### Step 5: Visualize Before/After
Create ASCII diagrams showing:
- State transitions (before vs after)
- Data flow changes
- Control flow modifications
- Component relationship changes

### Step 6: Deep Reasoning
For each significant change, ask:
- **Correctness**: Is this change correct?
- **Completeness**: Does it fully solve the problem?
- **Safety**: Does it introduce new issues?
- **Assumptions**: What must be true for this to work?
- **Failure modes**: What could go wrong?

## Analysis Process for Existing Code

When analyzing existing code (class, subsystem, repository):

### Step 1: Identify Purpose
- What is this code responsible for?
- Why does it exist?
- What problem domain does it address?

### Step 2: Map the Structure
Create hierarchy diagram showing:
- Main components/classes
- Package/module organization
- Key abstractions
- Dependency relationships

### Step 3: Trace Key Workflows
Identify and trace 2-3 most important workflows:
- Entry points
- Step-by-step execution
- Data transformations
- Exit points/results

### Step 4: Identify State & Transitions
- What state does this code manage?
- How does state change over time?
- What are the valid state transitions?
- What triggers transitions?

### Step 5: Analyze Interactions
- How do components communicate?
- What are the interfaces/APIs?
- Message passing patterns
- Synchronous vs asynchronous calls

### Step 6: Deep Reasoning
For critical paths:
- **Invariants**: What must always be true?
- **Assumptions**: What does the code assume?
- **Concurrency**: How does it handle concurrent access?
- **Failure handling**: How are errors managed?
- **Edge cases**: What are the boundary conditions?

## Levels of Granularity

Adjust depth based on scope:

### Single Function/Method
- Purpose and return value
- Parameter requirements
- Side effects
- Key algorithm steps
- Error conditions

### Single Class
- Responsibility (what it does)
- State (fields/properties)
- Key methods and their relationships
- Lifecycle (creation → use → cleanup)
- Invariants maintained

### Subsystem (Multiple Classes)
- High-level purpose
- Component breakdown
- Interface boundaries
- Main workflows (2-3 key scenarios)
- Inter-component communication

### Repository (Full Codebase)
- Architecture overview (layers/modules)
- Core abstractions
- Key subsystems and their roles
- Major data flows
- Critical patterns used throughout

## Concurrency Analysis Checklist

When analyzing concurrent code:

### Data Race Detection
- [ ] Identify all shared mutable state
- [ ] Check for unsynchronized access
- [ ] Verify read/write ordering
- [ ] Look for missing memory barriers

### Deadlock Detection
- [ ] Identify all lock acquisitions
- [ ] Check for lock ordering violations
- [ ] Look for circular dependencies
- [ ] Verify timeout mechanisms

### Thread Safety Patterns
- [ ] Immutability: Are objects immutable?
- [ ] Synchronization: Are critical sections protected?
- [ ] Lock-free: Are atomic operations used correctly?
- [ ] Thread-local: Is state properly isolated?
- [ ] Message passing: Are channels/queues used safely?

### Async/Await Analysis
- [ ] Understand execution context switches
- [ ] Identify blocking operations in async code
- [ ] Check for proper error propagation
- [ ] Verify cancellation handling

## Assumption Identification Patterns

Look for implicit assumptions in:

### Ordering Assumptions
```
// Assumes operation A completes before B
operationA();
operationB(); // Depends on A's results
```

### Null/Validity Assumptions
```
// Assumes value is never null
value.method(); // No null check
```

### State Assumptions
```
// Assumes object is in specific state
process(); // Requires initialization first
```

### Concurrency Assumptions
```
// Assumes single-threaded access
counter++; // Not atomic
```

### Resource Assumptions
```
// Assumes resource is available
file.write(data); // Doesn't check for full disk
```

## Failure Mode Identification

Common failure patterns:

### Resource Exhaustion
- Memory leaks
- File descriptor leaks
- Thread pool exhaustion
- Connection pool exhaustion

### Timing Issues
- Race conditions
- Deadlocks
- Timeouts not handled
- Retry storms

### Data Issues
- Null pointer exceptions
- Array/buffer overflows
- Type mismatches
- Encoding issues

### Error Propagation
- Exceptions swallowed
- Errors not logged
- Inconsistent error states
- Missing rollback logic

### External Dependencies
- Network failures
- Database unavailability
- Third-party API changes
- Configuration errors

## Visualization Guidelines

### When to Use Each Diagram Type

**State Machine**: When analyzing:
- Status/state fields that change
- Lifecycle of objects
- Protocol state machines
- Transaction states

**Data Flow**: When analyzing:
- Information transformation
- Pipeline processing
- Request/response patterns
- Data dependencies

**Sequence Diagram**: When analyzing:
- Component interactions
- Message protocols
- API call sequences
- Distributed system operations

**Component Diagram**: When analyzing:
- System architecture
- Module relationships
- Layer separation
- Dependency structure

**Before/After**: When analyzing:
- Code changes/patches
- Refactoring
- Bug fixes
- Architecture evolution

### Diagram Complexity Guidelines

**Simple (1-5 elements)**: Show core concept clearly
**Medium (6-15 elements)**: Show main flow with key branches
**Complex (16+ elements)**: Break into multiple focused diagrams

Prefer multiple simple diagrams over one complex diagram.

## Communication Strategy

### Structure Your Explanation

1. **Executive Summary** (1-2 paragraphs)
   - What this code does
   - Why it exists
   - Key insight in one sentence

2. **Visual Overview** (ASCII diagram)
   - High-level structure or flow
   - Main components/states
   - Critical paths highlighted

3. **Detailed Analysis** (organized by aspect)
   - Purpose & behavior
   - Structure & components
   - Assumptions & invariants
   - Failure modes

4. **Critical Insights** (bullet points)
   - Most important findings
   - Potential issues found
   - Recommendations if applicable

### Prioritization

Focus on:
- **High impact**: Changes to critical paths
- **High risk**: Concurrency, error handling, resource management
- **High complexity**: Tricky algorithms, subtle interactions
- **High value**: Key abstractions, main workflows

Skip or minimize:
- Boilerplate code
- Obvious getters/setters
- Standard patterns done correctly
- Low-risk refactoring

## Example Analysis Template

```
# Analysis of [Component/Patch Name]

## Summary
[2-3 sentences: what it is, what it does, why it matters]

## High-Level Structure
[ASCII diagram showing main components/flow]

## Key Behaviors
### Workflow 1: [Name]
[Sequence diagram or flow diagram]
- Step 1: ...
- Step 2: ...

### Workflow 2: [Name]
[Sequence diagram or flow diagram]

## State Management
[State machine diagram if applicable]

## Critical Assumptions
1. **[Assumption]**: [Explanation]
   - Why: [Why this assumption exists]
   - Risk: [What breaks if invalid]

2. **[Assumption]**: ...

## Failure Modes
1. **[Failure scenario]**: [Description]
   - Trigger: [What causes it]
   - Impact: [What goes wrong]
   - Mitigation: [How it's handled or should be]

2. **[Failure scenario]**: ...

## Concurrency Analysis (if applicable)
[Thread interaction diagram]
- Shared state: [What's shared]
- Synchronization: [How it's protected]
- Risks: [Potential races/deadlocks]

## Before/After (for patches only)
### Before
[Diagram of old behavior]

### After
[Diagram of new behavior]

### Why This Change
[Explanation of the improvement]

## Key Insights
- [Most important finding 1]
- [Most important finding 2]
- [Most important finding 3]
```
