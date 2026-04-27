---
name: deep-review
description: >
  Deep file-focused code review for correctness bugs. Unlike shallow-review which runs 6
  specialists in parallel across the entire patch, deep-review focuses on user-specified
  files with full pattern catalogs (444 patterns), codebase investigation, and source-level
  context gathering. Use when: the user specifies particular files for focused review, a
  shallow review flagged areas that need deeper investigation, reviewing critical-path code
  changes, examining complex serialization/lifecycle/state-machine changes. The user
  instructs which files to focus on (typically a subset of files in the patch).
---

# Deep Code Review

Focused, thorough review of user-specified files using the full 444-pattern catalog.
Each file is reviewed with context gathering (reading source, not just diff), codebase
searches, and cross-referencing against the complete pattern database.

```
                    +------------------+
                    |   Input Patch    |
                    +--------+---------+
                             |
                    +--------v---------+
                    | User selects     |
                    | focus files      |
                    +--------+---------+
                             |
              +--------------+--------------+
              |                             |
    +---------v---------+       +-----------v-----------+
    | Phase 0: Context  |       | Phase 0: Context      |
    | (read full files, |       | (read full files,     |
    |  callers, types)  |       |  callers, types)      |
    +---------+---------+       +-----------+-----------+
              |                             |
    +---------v---------+       +-----------v-----------+
    | Phase 1: Deep     |       | Phase 1: Deep         |
    | specialist review |       | specialist review     |
    | (full checklists) |       | (full checklists)     |
    +---------+---------+       +-----------+-----------+
              |                             |
    +---------v---------+       +-----------v-----------+
    | Phase 2: Codebase |       | Phase 2: Codebase     |
    | investigation     |       | investigation         |
    +---------+---------+       +-----------+-----------+
              |                             |
              +--------------+--------------+
                             |
                    +--------v---------+
                    | Phase 3: Cross-  |
                    | file symmetry &  |
                    | merge findings   |
                    +------------------+
```

## Quick Start

1. **Receive patch** and user's file focus list
2. **For each focus file**: run Phase 0 (context), Phase 1 (deep checklists), Phase 2 (search)
3. **Cross-file symmetry** pass across all focus files
4. **Merge & report** with confidence ranking

---

## Phase 0: Context Gathering (per focus file)

Before touching any checklist, deeply understand each focus file. This is the key difference
from shallow review — you read the actual source, not just the diff.

**Prompt template**:
```
Read the full source of the focus file: `{FILE_PATH}`
Read the patch for context: `{PATCH_PATH}`

For this file, gather:
1. CLASS HIERARCHY: What does it extend? What interfaces? Read the parent class/interface.
2. SIBLING CLASSES: Are there parallel implementations? Read at least one sibling.
3. CALLERS: Who calls the modified methods? How do they use return values? (grep + read)
4. LIFECYCLE: When is this object created, used, destroyed? What state transitions?
5. INVARIANTS: Read Javadoc, assertions, and any test files for this class.
6. SERIALIZATION: If it serializes — read serialize, deserialize, serializedSize together.

Output a structured context summary for use by the deep review phases.
```

---

## Phase 1: Deep Specialist Review (per focus file)

Each focus file is reviewed against the FULL domain checklists (not the trimmed shallow
versions). The reviewer selects which domains apply based on Phase 0 context.

### Domain Selection

Based on Phase 0 context, load the relevant deep checklists:

| File contains | Load checklist |
|---|---|
| Conditions, comparisons, control flow, types | `references/deep/deep-logic.md` |
| Arithmetic, indices, ranges, ByteBuffer, I/O | `references/deep/deep-boundary.md` |
| Shared state, locks, lifecycle, state machines | `references/deep/deep-concurrency.md` |
| Serialization, resources, metrics, config | `references/deep/deep-resources.md` |
| New classes, new fields, registrations | `references/deep/deep-absence-completeness.md` |
| Refactoring, merges, shell scripts, parsing | `references/deep/deep-cross-cutting.md` |

Typically 2-4 domains are relevant per file. Load all relevant ones.

**Prompt template**:
```
You are performing a DEEP REVIEW of: `{FILE_PATH}`

CONTEXT (from Phase 0):
{CONTEXT_SUMMARY}

PATCH (the changes being reviewed):
Read: `{PATCH_PATH}`

CHECKLISTS (load each relevant one):
Read: `{SKILL_DIR}/references/deep/deep-logic.md`
Read: `{SKILL_DIR}/references/deep/deep-boundary.md`
[... load relevant domains ...]

For EVERY checklist item that could apply to this file:
1. Check whether the pattern exists in the code
2. If the pattern matches: search the codebase for confirming/refuting evidence
3. Rate confidence: High (confirmed by evidence), Medium (pattern matches but
   uncertain), Low (possible but speculative)

Report ALL findings. For each:
- Pattern ID and name
- Location (file:line)
- Confidence (High/Medium/Low)
- What's wrong (2-3 sentences with reasoning)
- Evidence gathered (what you searched for, what you found/didn't find)
- Suggested fix

Or "No finding in this domain for this file" if nothing applies.
```

---

## Phase 2: Codebase Investigation (per focus file)

For each finding from Phase 1, and for absence/completeness patterns, execute targeted
codebase searches.

**Search patterns to execute:**

### For new fields:
```bash
grep -rn "serialize\|deserialize\|serializedSize\|equals\|hashCode\|toString" {FILE_DIR}/
```

### For new registrations:
```bash
grep -rn "addListener\|register\|subscribe\|addMetric\|removeSensor\|deregister" {FILE_DIR}/
```

### For new enum constants:
```bash
grep -rn "switch.*{ENUM_TYPE}\|case.*{CONSTANT}" --include="*.java" {PROJECT_ROOT}/
```

### For interface implementations:
```bash
grep -rn "implements {INTERFACE}" --include="*.java" {PROJECT_ROOT}/
```

### For method callers:
```bash
grep -rn "{METHOD_NAME}" --include="*.java" {PROJECT_ROOT}/
```

### For parallel paths:
```bash
grep -rn "class.*extends.*{PARENT_CLASS}" --include="*.java" {PROJECT_ROOT}/
```

**Prompt template**:
```
You are investigating findings from the deep review of: `{FILE_PATH}`

FINDINGS TO INVESTIGATE:
{PHASE_1_FINDINGS}

For each finding:
1. Execute the relevant codebase searches (grep, read files)
2. Confirm or refute the finding based on evidence
3. If confirmed: search for additional instances of the same pattern
4. If refuted: explain why with evidence

For absence patterns:
1. Build the search list from the diff events
2. Execute each search
3. If no match found: report as confirmed finding
4. If match found: report as refuted (the symmetric code exists)

Report updated findings with evidence.
```

---

## Phase 3: Cross-File Symmetry & Merge

After all focus files are reviewed:

**Prompt template**:
```
Review findings across all focus files for cross-file patterns:

1. CROSS-FILE SYMMETRY: For each change in file A, is there a structurally parallel
   change needed in file B? Check:
   - Serialization families (if serialize changed, did deserialize change?)
   - Interface implementations (if one implementation changed, do siblings need it?)
   - Version-gated paths (if one version branch changed, do others?)
   - Test coverage (does the test file cover the changed behavior?)

2. FINDING REINFORCEMENT: Do findings across files reinforce each other?
   - Logic + Absence at same conceptual location → boost to High
   - Concurrency + Resources on same lifecycle → boost to High
   - Pattern flagged in multiple files → likely systematic issue

3. MERGE & DEDUP: Combine all findings, deduplicate, rank.

4. THREE-POINT TEST each finding:
   - The code construct actually exists (not inferred)
   - The bug is possible given visible context (not speculative)
   - The finding is actionable (what specifically should change)
```

---

## Report Format

```
## Deep Review: [target]

### Focus Files
- `path/to/File1.java` — [brief description of changes]
- `path/to/File2.java` — [brief description of changes]

### Context Summary
[Key facts from Phase 0: class hierarchy, lifecycle, shared state, invariants]

### Findings (ranked by confidence)

#### Finding 1: [title]
- **Location**: [file:line]
- **Confidence**: High / Medium / Low
- **Domain**: [Logic / Boundary / Concurrency / Resources / Absence / Completeness]
- **Pattern**: [pattern name from checklist]
- **What's wrong**: [2-3 sentences with reasoning]
- **Evidence**: [what was searched, what was found]
- **Suggested fix**: [specific change]

#### Finding 2: ...

### Domain Coverage (per file)
| File | Logic | Boundary | Concurrency | Resources | Absence | Completeness |
|---|---|---|---|---|---|---|
| File1.java | 2 findings | - | 1 finding | - | checked | checked |
| File2.java | - | 1 finding | - | 3 findings | checked | checked |

### Cross-File Symmetry
- [Any asymmetries found]
- [Any reinforcing patterns]
```

---

## When to Use Deep Review vs Shallow Review

| Criterion | Shallow (edge-case-explorer) | Deep (this skill) |
|---|---|---|
| Scope | Entire patch | User-specified files |
| Checklist depth | 15-20 items per domain | 80-190 items per domain |
| Source reading | Diff only | Full file + callers + siblings |
| Codebase search | Absence specialist only | All domains |
| Context gathering | Phase 0 hypotheses only | Full class hierarchy + lifecycle |
| Best for | Quick triage, first pass | Critical code, complex changes |
| Time budget | Minutes | Extended (user directs) |

## Reference Files

### Deep specialist checklists
- `references/deep/deep-logic.md` — Full logic, types, constants, filtering patterns
- `references/deep/deep-boundary.md` — Full boundary, overflow, null, I/O patterns
- `references/deep/deep-concurrency.md` — Full concurrency, state, lifecycle patterns
- `references/deep/deep-resources.md` — Full serialization, resource, crash-safety patterns
- `references/deep/deep-absence-completeness.md` — Full absence + completeness with search methodology
- `references/deep/deep-cross-cutting.md` — Refactoring, parsing, platform, dispatch patterns
