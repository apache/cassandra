# Per-Bug Analysis Format

Every analysis file MUST follow this structure. No extra sections. No prose padding.

## Output Template

```markdown
# Bug NNNN: [title — what broke, not how it was fixed]

**Commit**: <hash> · **Ticket**: <ID or n/a>

## Detection Rule

[One imperative sentence. Starts with "When" or "If". A reusable heuristic
that would catch this CLASS of bug during review. NOT a description of this
specific bug. Two bugs with identical detection rules are the same pattern.]

## Code Smell

[What the buggy code LOOKS LIKE to a reviewer. The observable surface pattern.
1-2 sentences max. Describe what you'd grep for or see during review.]

## Tags

- scope: [universal | <language> | distributed-systems | project-specific]
- category: [see category list below]
- code-pattern: [2-4 word hyphenated tag, e.g. "null-after-lookup"]
- severity: [silent-wrong-result | crash | hang | data-loss | performance | cosmetic]
- subsystem: [free-form — infer from file paths and module structure]

## Root Cause

[2-3 sentences. What was actually wrong and why. Include the key insight.]

## Example

​```<language>
// BEFORE
[minimal buggy code — essential lines only, ≤15 lines]
​```

​```<language>
// AFTER
[minimal fixed code — same scope, ≤15 lines]
​```
```

## Field Guidelines

### Detection Rule — THE MOST IMPORTANT FIELD

Write it as a checklist item a reviewer walks through on EVERY review:

- **General enough** to catch the same bug in a different file/context
- **Specific enough** that a "yes" answer actually indicates a likely bug
- **Imperative** — starts with "When" or "If", reads as a yes/no check

Good: "When a callback is registered on a future AND the caller also blocks on future.get(), verify that all state mutations happen after get(), not in the callback."

Bad: "Check that bootstrapFinished() is called in the right place." (too specific)
Bad: "Be careful with race conditions." (too vague)

### Code Smell — WHAT IT LOOKS LIKE

Describe the visual/structural pattern a reviewer or grep would find:

Good: "A Futures.addCallback() on the same future that is later awaited with .get() in the same method, where the callback mutates shared state."

Bad: "Race condition in bootstrap code." (not visual, not greppable)

### Tags

**scope**:
- `universal`: Any language, any codebase (off-by-one, resource not closed on error path)
- `<language>` (e.g. `java`, `python`, `go`): Language-specific, not domain-specific
- `distributed-systems`: Applies to any distributed system (epoch boundaries, topology changes)
- `project-specific`: Only meaningful in this project's context

**category** (recommended list — extend if none fit):
`logic-error` | `race-condition` | `missing-null-check` | `off-by-one` | `resource-leak` | `wrong-constant` | `wrong-serialization` | `state-not-cleaned` | `deadlock` | `silent-skip` | `missing-override` | `non-atomic-write` | `wrong-filter-result`

**code-pattern** — the clustering key. Short, stable tag for grouping related bugs. Reuse existing tags when the pattern matches; invent new ones when genuinely novel.

Examples: `cast-truncates-size`, `null-after-map-lookup`, `close-not-on-all-paths`, `retry-no-escape`, `serialize-size-mismatch`, `stale-state-after-lifecycle`, `mutable-shared-buffer`, `else-if-masks-second`, `write-not-atomic`, `sentinel-not-handled`

### Root Cause — BRIEF

2-3 sentences. Don't repeat the detection rule. Focus on the mechanism.

### Example — MINIMAL

Minimum lines to show the bug and fix. Strip imports, logging, comments. 5-15 lines per side.

## What to SKIP

- Purely cosmetic commits (typo, import reorder, whitespace, formatting)
- Commits that only add tests without fixing a bug
- Pure refactors with no behavioral change
- Merge commits with no original changes

Mark skipped commits with a one-word reason.

## What NOT to Include

- No "Test" section
- No author attribution
- No long prose explanations
- No issue tracker descriptions or external context
