---
name: patch-explainer
version: "1.0.0"
description: Deep code analysis with ASCII visualizations showing structure, flow, and state transitions. Use when analyzing patches/diffs, explaining classes or subsystems, understanding code architecture, reviewing changes for inconsistencies, or when asked to visualize how code works. Provides before/after diagrams, data/control flow, concurrency analysis, assumptions, and failure modes. Triggers on explain this patch/code/class, how does X work, show me the flow, visualize this change, code review requests, or proactive analysis during PR reviews.
---

# Patch Explainer

Deeply analyze and visualize code using ASCII diagrams to reveal structure, behavior, and interactions.

## What This Skill Does

Provides comprehensive code analysis with rich ASCII visualizations for:
- **Patches/diffs**: Before/after states, what changed and why
- **Classes**: Purpose, structure, state transitions, workflows
- **Subsystems**: Component interactions, data flow, architecture
- **Repositories**: Overall structure, key abstractions, major patterns
- **Code reviews**: Finding inconsistencies, analyzing assumptions and failure modes

## Analysis Approach

### For Any Code (Patches, Classes, Subsystems, Repositories)

1. **Identify the core purpose** - What does this code do and why does it exist?

2. **Create ASCII visualizations** showing:
   - Structure (components, layers, dependencies)
   - Behavior (data flow, control flow, state transitions)
   - Interactions (sequence diagrams, message passing, concurrency)
   - For patches: before/after comparisons

3. **Deep reasoning** about:
   - **Assumptions**: What must be true for this to work correctly?
   - **Failure modes**: What could go wrong at each step?
   - **Concurrency**: If multiple threads/processes, show their interactions
   - **Invariants**: What must always hold true?

4. **Focus on the "why"**:
   - Why does this code exist?
   - Why was this approach chosen?
   - For changes: Why this modification? What problem does it solve?

### Visualization Strategy

Choose diagram types based on what you're explaining:

**State machines** - When code manages states/status:
```
[Initial] --event--> [Processing] --success--> [Complete]
                          |
                       failure
                          |
                          v
                      [Failed]
```

**Data flow** - When showing information movement:
```
Input → [Transform A] → [Transform B] → Output
```

**Sequence diagrams** - When showing component interactions:
```
Client          Server          Database
  |               |                 |
  |--request----->|                 |
  |               |----query------->|
  |               |<---result-------|
  |<--response----|                 |
```

**Component structure** - When showing architecture:
```
┌─────────────────────┐
│   Application       │
└──────────┬──────────┘
           |
┌──────────┴──────────┐
│   Service Layer     │
└──────────┬──────────┘
           |
┌──────────┴──────────┐
│   Data Layer        │
└─────────────────────┘
```

**Before/after** - When explaining changes:
```
BEFORE                  AFTER
─────────────────────────────────
[A] → [B] → [C]        [A] → [Cache?] ─Yes→ [C]
                              |
                              No
                              v
                            [B] → [C]
```

### Reference Materials

**ASCII diagram templates**: See [ascii_patterns.md](references/ascii_patterns.md) for:
- State machine patterns
- Data flow diagrams
- Sequence diagrams
- Component/architecture diagrams
- Before/after comparisons
- Concurrency patterns
- Control flow patterns
- Dependency graphs

**Analysis methodology**: See [analysis_framework.md](references/analysis_framework.md) for:
- Structured analysis process for patches vs existing code
- Concurrency analysis checklist
- Assumption identification patterns
- Failure mode identification
- Analysis depth guidelines by scope (function → class → subsystem → repository)

## Analysis Workflow

### For Patches/Diffs

1. Read the patch to understand what changed
2. Identify the change type (bug fix, feature, refactor, optimization)
3. Create before/after ASCII diagrams showing:
   - Old behavior vs new behavior
   - State transitions that changed
   - Data flow modifications
4. Explain what fundamentally changed:
   - What problem did the old code have?
   - How does the new code solve it?
   - What assumptions changed?
5. Analyze failure modes:
   - What could go wrong with this change?
   - Are there edge cases not handled?
   - Concurrency implications?
6. Focus on the "why": What problem does each modification solve?

### For Classes

1. Read the class to understand its purpose
2. Create ASCII diagram showing:
   - Class structure (key fields/methods)
   - State machine if it manages state
   - How it fits with related classes
3. Explain key workflows (2-3 most important methods)
4. Identify assumptions and invariants
5. Analyze failure modes and edge cases
6. Focus on the "why": Why does this class exist? What problem does it solve?

### For Subsystems

1. Identify the components in the subsystem
2. Create ASCII diagram showing:
   - Component relationships
   - Key interfaces/boundaries
   - Main data flows
3. Trace 2-3 key workflows through the subsystem
4. Explain how components interact
5. Identify assumptions across component boundaries
6. Focus on the "why": What is this subsystem's role in the larger system?

### For Repositories

1. Understand the high-level architecture
2. Create ASCII diagram showing:
   - Major modules/packages
   - Layered architecture
   - Key abstractions
3. Identify core workflows
4. Explain main patterns used throughout
5. Highlight critical subsystems
6. Focus on the "why": What problem does this codebase solve?

## Concurrency Analysis

When analyzing concurrent code, always show:

**Thread interactions**:
```
Thread A                State                Thread B
   |                      |                      |
   |--lock()------------>|                      |
   |                     [LOCKED]               |
   |--modify()---------->|                      |
   |--unlock()---------->|                      |
   |                     [UNLOCKED]             |
   |                      |<---------lock()-----|
   |                     [LOCKED]               |
```

**Race conditions**:
```
Thread A            Thread B
   |                   |
   |--read(x=10)      |
   |                   |--read(x=10)
   |--x=x+1           |
   |                   |--x=x+1
   |--write(x=11)     |
   |                   |--write(x=11)  ⚠ Lost update!

Result: x=11 (expected: x=12)
```

Check for:
- Shared mutable state without synchronization
- Lock ordering violations (potential deadlocks)
- Race conditions in read-modify-write operations
- Missing memory barriers

## Output Structure

Structure your analysis as:

### 1. Executive Summary
Brief overview (2-3 sentences) of what this code does and why it matters.

### 2. Visual Overview
High-level ASCII diagram showing the main structure or flow.

### 3. Detailed Analysis
Organized by aspect:
- **Purpose**: What it does and why it exists
- **Structure**: Components and their relationships
- **Key Workflows**: Step-by-step execution of main scenarios
- **State Management**: States and transitions (if applicable)
- **Assumptions**: What must be true for correctness
- **Failure Modes**: What could go wrong
- **Concurrency**: Thread safety analysis (if applicable)

### 4. For Patches: Before/After
- Before state (with diagram)
- After state (with diagram)
- Why this change was made
- Critical modifications explained

### 5. Key Insights
Bullet points highlighting:
- Most important findings
- Potential issues or risks
- Recommendations (if applicable)

## Prioritization

Focus on high-impact elements:
- **Critical paths**: Main workflows that matter most
- **High-risk code**: Concurrency, error handling, resource management
- **Complex logic**: Non-obvious algorithms or subtle interactions
- **Key abstractions**: Core interfaces and contracts

Minimize or skip:
- Boilerplate code
- Simple getters/setters
- Standard patterns done correctly
- Low-risk trivial changes

## Example Scenarios

**Scenario 1**: "Explain this patch"
→ Show before/after diagrams, explain what changed and why, analyze assumptions and failure modes

**Scenario 2**: "How does SafeCommandStore work?"
→ Show class structure, explain the exclusive access pattern, trace key methods, analyze thread safety

**Scenario 3**: "Explain the coordination subsystem"
→ Show component diagram, trace PreAccept→Accept→Commit flow, explain message passing, show quorum tracking

**Scenario 4**: "What's the architecture of this codebase?"
→ Show high-level module structure, identify key abstractions, explain main patterns, highlight critical subsystems

**Scenario 5**: During code review (proactive)
→ Analyze changes for inconsistencies, check assumptions, identify potential race conditions, verify error handling
