# PATTERNS.md Synthesis Format

Instructions for producing the generalized pattern catalog from individual bug analysis files.

## Process

1. Read all `bug-*.md` files in the output directory
2. Parse each file's `## Tags` section — extract `category` and `code-pattern`
3. Group bugs sharing the same `code-pattern` tag
4. For groups with 2+ bugs: synthesize a generalized pattern entry
5. For singletons (1 bug): collect in a separate section

## Generalization Rules

The goal: a reader with no knowledge of the source repository can use PATTERNS.md as a bug-detection checklist.

**Strip:**
- All ticket IDs (PROJ-1234, #567, etc.)
- Author names, reviewer names
- Project-specific subsystem labels (replace with generic structural descriptions or omit)
- Specific class/method names — replace with structural descriptions:
  - `ColumnFamilyStore.memtable` → "a shared mutable field"
  - `AccordJournal.replay()` → "the journal replay path"
  - `SSTableReader.compareTo()` → "a compareTo override"

**Keep:**
- Language-specific API names when scope is that language or broader
  - `ByteBuffer.array()` without `arrayOffset()` — keep (Java-scoped, reusable)
  - `Futures.addCallback()` racing `future.get()` — keep (Java-scoped)
- Generic distributed-systems concepts (epoch, topology change, quorum, leader election)
- The `code-pattern` tag itself (it's already designed to be generic)

**Merge Detection Rules:** If multiple bugs share a code-pattern but have slightly different Detection Rules, write one generalized rule that covers all instances. Prefer the most broadly applicable wording.

## Output Format

```markdown
# Bug Patterns

N patterns extracted from M analyzed commits.

## [Category Name] (X patterns, Y bugs)

### [code-pattern-tag] (Z bugs)

**Detection Rule**: [generalized imperative sentence]

**Code Smell**: [what to look for — greppable or visually scannable]

**Scope**: universal | <language> | distributed-systems

**Severity**: [most common across instances]

**Example**:
​```
// BEFORE
[most illustrative before snippet from any instance, ≤10 lines]
​```
​```
// AFTER
[matching after snippet]
​```

---

### [next-pattern] (Z bugs)
...

## Singletons (observed once — may not generalize)

### [code-pattern-tag]

**Detection Rule**: ...
**Code Smell**: ...
**Scope**: ...
**From**: bug-NNNN (commit <short-hash>)
```

## Ordering

1. Categories sorted by total bug count (most bugs first)
2. Patterns within each category sorted by instance count (most instances first)
3. Singletons section last, sorted alphabetically by code-pattern tag

## Quality Check

Before finalizing PATTERNS.md, verify:
- No ticket IDs appear anywhere in the file
- No project-specific class/method names appear (except in code examples where the structural pattern is clear from context)
- Every Detection Rule starts with "When" or "If"
- Every pattern with 2+ bugs has a representative Example
- Singleton patterns are clearly labeled as possibly non-generalizable
