# ASCII Diagram Patterns

This reference provides templates and examples for creating clear ASCII diagrams when visualizing code.

## State Machine Diagrams

### Simple State Transition

```
[State A] --event--> [State B] --event--> [State C]
```

### State Machine with Multiple Paths

```
           ┌─────────┐
           │ Initial │
           └────┬────┘
                │
        ┌───────┴───────┐
        │               │
    event_a         event_b
        │               │
        ▼               ▼
   ┌────────┐      ┌────────┐
   │ State1 │      │ State2 │
   └───┬────┘      └───┬────┘
       │               │
   success         failure
       │               │
       ▼               ▼
   ┌────────┐      ┌────────┐
   │  Done  │      │ Error  │
   └────────┘      └────────┘
```

### State Machine with Self-Loops

```
     retry
      ↻
   ┌──────┐
   │      ▼
┌──┴────────┐  success   ┌──────────┐
│ Pending   │──────────→ │ Complete │
└───────────┘            └──────────┘
      │
      │ timeout
      ▼
   ┌──────────┐
   │  Failed  │
   └──────────┘
```

## Data Flow Diagrams

### Linear Flow

```
Input → [Process A] → [Process B] → [Process C] → Output
```

### Branching Flow

```
                    ┌→ [Handler A] → Result A
                    │
Input → [Router] ───┼→ [Handler B] → Result B
                    │
                    └→ [Handler C] → Result C
```

### Convergent Flow

```
[Source A] ─┐
            ├→ [Combiner] → Output
[Source B] ─┘
```

### Bidirectional Flow

```
┌─────────┐  request   ┌─────────┐
│ Client  │ ─────────→ │ Server  │
└─────────┘ ←───────── └─────────┘
              response
```

## Sequence Diagrams

### Simple Interaction

```
Component A          Component B          Component C
    │                     │                     │
    │──call()────────────→│                     │
    │                     │──query()───────────→│
    │                     │←──result───────────│
    │←──response──────────│                     │
    │                     │                     │
```

### With Timing/Phases

```
Thread 1        Thread 2        Shared State
   │               │                 │
   │──lock()───────┼────────────────→│
   │               │                 │ [LOCKED]
   │──read()───────┼────────────────→│
   │←──value───────┼─────────────────│
   │               │                 │
   │──unlock()─────┼────────────────→│
   │               │                 │ [UNLOCKED]
   │               │──lock()────────→│
   │               │                 │ [LOCKED]
   │               │──write()───────→│
   │               │                 │
```

## Component/Architecture Diagrams

### Layered Architecture

```
┌─────────────────────────────────────┐
│        Application Layer            │
│  (Business Logic, Workflows)        │
└──────────────┬──────────────────────┘
               │
┌──────────────┴──────────────────────┐
│         Service Layer               │
│  (API, Coordination, Validation)    │
└──────────────┬──────────────────────┘
               │
┌──────────────┴──────────────────────┐
│          Data Layer                 │
│  (Storage, Persistence, Cache)      │
└─────────────────────────────────────┘
```

### Component Interaction

```
┌──────────────┐
│   Client     │
└──────┬───────┘
       │
       ▼
┌──────────────┐      ┌──────────────┐
│  Controller  │─────→│   Service    │
└──────┬───────┘      └──────┬───────┘
       │                     │
       │              ┌──────┴───────┐
       │              │              │
       ▼              ▼              ▼
┌──────────────┐  ┌────────┐  ┌─────────┐
│    Cache     │  │   DB   │  │  Queue  │
└──────────────┘  └────────┘  └─────────┘
```

### Nested Components

```
┌─────────────────────────────────────────┐
│              Node                       │
│  ┌─────────────────────────────────┐   │
│  │      CommandStore               │   │
│  │  ┌──────────┐  ┌──────────┐    │   │
│  │  │ Commands │  │ Listeners│    │   │
│  │  └──────────┘  └──────────┘    │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │      MessageRouter              │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

## Before/After Comparisons

### Side-by-Side

```
BEFORE                          AFTER
─────────────────────────────────────────────────
┌──────────┐                    ┌──────────┐
│  Single  │                    │  Step 1  │
│   Step   │                    └────┬─────┘
└────┬─────┘                         │
     │                                ▼
     ▼                          ┌──────────┐
┌──────────┐                    │  Step 2  │
│  Result  │                    └────┬─────┘
└──────────┘                         │
                                     ▼
                                ┌──────────┐
                                │  Result  │
                                └──────────┘
```

### With Annotations

```
OLD:  [Start] → [Process] → [End]
                    ↑
                    │
              (bottleneck!)

NEW:  [Start] → [Cache?] ─Yes→ [End]
                    │
                    No
                    ↓
               [Process] → [Cache] → [End]
                           (store)
```

## Concurrency Patterns

### Race Condition

```
Thread A                Thread B
   │                       │
   │─read(x=10)           │
   │                       │─read(x=10)
   │─compute(x+1)          │
   │                       │─compute(x+1)
   │─write(x=11)           │
   │                       │─write(x=11)  ⚠ Lost update!
   │                       │
Result: x=11 (expected: x=12)
```

### Proper Synchronization

```
Thread A                Shared State                Thread B
   │                         │                         │
   │──lock()───────────────→ │                         │
   │                         │ [LOCKED by A]           │
   │──read(x=10)─────────── │                         │
   │──write(x=11)────────→  │                         │
   │──unlock()───────────→   │                         │
   │                         │ [UNLOCKED]              │
   │                         │ ←──────────lock()───────│
   │                         │ [LOCKED by B]           │
   │                         │ ──────────read(x=11)───→│
   │                         │ ←─────────write(x=12)───│
   │                         │ ←──────────unlock()─────│
   │                         │ [UNLOCKED]              │
```

### Message Passing

```
Producer                Queue                Consumer
   │                      │                      │
   │──produce(msg1)──→   [msg1]                 │
   │──produce(msg2)──→   [msg1,msg2]            │
   │                      │   ←──consume()──────│
   │                     [msg2]  (got msg1)     │
   │──produce(msg3)──→   [msg2,msg3]            │
   │                      │   ←──consume()──────│
   │                     [msg3]  (got msg2)     │
```

## Control Flow Patterns

### Conditional Logic

```
           ┌──────────┐
           │  Check   │
           └────┬─────┘
                │
         ┌──────┴──────┐
         │             │
      condition     condition
       true?          false?
         │             │
         ▼             ▼
    ┌────────┐    ┌────────┐
    │ Path A │    │ Path B │
    └───┬────┘    └───┬────┘
        │             │
        └──────┬──────┘
               ▼
          ┌─────────┐
          │  Merge  │
          └─────────┘
```

### Loop with Exit Condition

```
    ┌──────────────┐
    │  Initialize  │
    └──────┬───────┘
           │
    ┌──────▼───────┐
    │  Loop Body   │←──┐
    └──────┬───────┘   │
           │           │
    ┌──────▼───────┐   │
    │  Continue?   │   │
    └──────┬───────┘   │
           │           │
      ┌────┴────┐      │
      │         │      │
     Yes       No      │
      │         │      │
      └─────────┘      │
           │
           ▼
    ┌──────────────┐
    │    Exit      │
    └──────────────┘
```

### Exception Handling

```
┌────────────┐
│   Start    │
└─────┬──────┘
      │
      ▼
┌────────────┐
│  Try Block │
└─────┬──────┘
      │
      ├──success──────→ [Continue]
      │
      └──exception────→ ┌─────────────┐
                        │ Catch Block │
                        └──────┬──────┘
                               │
                        ┌──────┴──────┐
                        │             │
                     recoverable   fatal
                        │             │
                        ▼             ▼
                    [Retry]       [Fail]
```

## Dependency Graphs

### Simple Dependencies

```
      ┌─────┐
      │  A  │
      └──┬──┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
  ┌───┐     ┌───┐
  │ B │     │ C │
  └─┬─┘     └─┬─┘
    │         │
    └────┬────┘
         ▼
       ┌───┐
       │ D │
       └───┘
```

### Circular Dependencies (Problem)

```
   ┌───┐
   │ A │
   └─┬─┘
     │
     ▼
   ┌───┐     ⚠ CYCLE!
   │ B │
   └─┬─┘
     │
     ▼
   ┌───┐
   │ C │
   └─┬─┘
     │
     └──→ [back to A]
```

## Tips for Effective Diagrams

1. **Use consistent spacing** - Align elements for readability
2. **Add legends** - Explain symbols when not obvious
3. **Show critical paths** - Highlight the main flow
4. **Mark problems** - Use ⚠, ✗, ❌ for issues
5. **Mark fixes** - Use ✓, ✔, ✅ for solutions
6. **Annotate** - Add brief explanations with (parentheses)
7. **Keep it simple** - Don't try to show everything at once
8. **Focus on one aspect** - Separate diagrams for different concerns (data flow vs. control flow)
