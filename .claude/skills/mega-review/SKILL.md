---
name: mega-review
version: "1.0.0"
description: >
  Multi-pass deep code review for large patches (1000+ LOC) that maximizes real bug detection.
  Orchestrates targeted-review, shallow-review, and deep-review in parallel across all HIGH and
  MEDIUM risk files and commits: understands the feature holistically, splits by file and commit,
  runs deep review on every HIGH/MEDIUM file, targeted review across the same scope, and
  per-commit shallow review, followed by cross-cut consistency checks. Use when: reviewing a
  large patch (feature branch, multi-commit, or single large diff, 1000+ LOC), doing a thorough
  pre-merge review, or when shallower reviews miss bugs due to patch size. Triggers on: "review
  this branch", "review these commits", "review this feature", "mega review", "thorough review",
  "full review of X commits", or when the user specifies a commit range or feature for review.
---

# Mega-Review: Multi-Pass Large Patch Review

This skill orchestrates `/targeted-review`, `/shallow-review`, and `/deep-review` with
cross-cut consistency checks to maximize bug detection on large patches (1000+ LOC). The input
can be a commit range, a feature branch, or any large diff — it does not have to span multiple
commits.

**You MUST invoke `/targeted-review`, `/shallow-review`, and `/deep-review` as part of this
workflow.** Those skills have specialist checklists, pattern catalogs, and structured review
processes that this skill does not duplicate. This skill's job is to understand the change
holistically, decompose it into reviewable chunks, and delegate the actual code review to the
appropriate sub-skills at the right granularity.

## Why This Exists

A single pass of `/shallow-review` over 7000 LOC spreads attention too thin. Real bugs hide in:
- **Offset arithmetic** that works for the simple case but breaks when offset params are added
- **Variable lifecycle issues** where a loop modifies a variable but the final operation needs the original
- **Cross-cut inconsistencies** where fixes or refactors in one subsystem introduce bugs in another
- **Algorithmic boundary conditions** in new data structure code

The key insight: some bugs are findable by mechanical checking (every System.arraycopy, every
variable swap), while others require deep understanding. This skill does BOTH.

## Architecture

```
+---------------------+
| PHASE 1: Decompose  |
|   & Classify        |    Produce decomposition map + coverage checklist.
+----------+----------+
           |
    +-------+--------+----------+
    |                |           |
+---v----+   +-------v------+  +-v-----------+
|PHASE 2 |   |   PHASE 3    |  |   PHASE 4   |
|Deep    |   |  Targeted    |  |  Shallow    |
|Review  |   |  Review      |  |  Review     |
|(HIGH + |   | (HIGH +      |  | (HIGH +     |
| MEDIUM |   |  MEDIUM      |  |  MEDIUM     |
| files) |   |  files)      |  |  commits)   |
+---+----+   +-------+------+  +-+-----------+
    |                |            |
    +----------------+------------+
                     |
          +----------v----------+
          | PHASE 5: Cross-Cut  |    Cross-commit/cross-subsystem consistency, pattern
          | Passes              |    amplification, and fix verification checks.
          +----------+----------+    One subagent per pass type (5A / 5B / 5C).
                     |
          +----------v----------+
          | PHASE 6: Merge &    |    Deduplicate, verify plausibility, rank.
          | Validate            |    Combine findings from ALL phases.
          +---------------------+
```

---

## Chunking Strategy

This skill's core principle: **the orchestrator holds global state; subagents hold only their
slice.** A large patch reviewed by a single agent suffers attention dilution — its context fills
with earlier files, earlier findings, and earlier grep output, and it starts missing things. The
fix is to reset context at every subagent boundary.

### Orchestrator responsibilities
- Holds the full patch, decomposition map, and accumulated findings from all phases
- Before spawning each subagent, assembles a **briefing packet**: the chunk + a tight 100–200
  word context summary written by the orchestrator
- After each subagent returns, integrates findings into the global picture

### What each subagent receives
- **Its chunk**: the diff for its commit / grep hits for its audit type / file diff for its deep pass
- **Orchestrator context**: subsystem name, risk level, what the code is trying to do (one paragraph)
- **Focused task**: exactly what to look for and why — not a generic "find bugs"
- **Relevant prior findings only**: findings from earlier phases that touch THIS chunk's files,
  not the full findings list from all files

### What each subagent does
Each subagent **invokes a review skill** on its chunk — it is not a generic agent. The skill
determines the review depth and structure. The chunk is the scope; the briefing packet is the
context. Subagents may read additional source files, grep for callers, or check sibling classes
to inform the review, but must not receive the full patch as input — that defeats the purpose.

### Chunk boundaries by phase

| Phase | Chunk unit | Skill to invoke | What the subagent receives |
|---|---|---|---|
| 2 (Deep) | One HIGH or MEDIUM file | `/deep-review` | That file's diff + full current source |
| 3 (Targeted) | All HIGH + MEDIUM files as a group | `/targeted-review` | Diff for those files + Phase 1 risk classification |
| 4 (Shallow) | One HIGH or MEDIUM commit | `/shallow-review` | That commit's diff only |
| 5 (Cross-cut) | One pass type (5A / 5B / 5C) | inline only — no sub-skill | Finding list relevant to that pass + grep targets |

**Phases 2, 3, and 4 run in parallel** — launch all their subagents in a single message after Phase 1 completes. Do not wait for Phase 2 before starting Phase 3 or 4.

---

## PHASE 1: Decompose & Classify

**This phase is done inline — no sub-skill invocation.** Its job is to produce a decomposition
map and coverage checklist fast, then immediately hand off to the parallel Phases 2/3/4.
Every minute spent in Phase 1 is a minute not spent finding bugs.

**Determining the input scope** — two modes depending on what the user provided:

*Mode A — git boundary given* (commit range, branch, or single SHA): proceed directly to
Step 1a.

*Mode B — no git boundary* (user describes a feature, subsystem, or set of files without
specifying a range):
1. Discover candidate files via grep or directory inspection:
   ```bash
   grep -rl "<keyword>" src/ | grep '\.java$' | sort > /tmp/file_list.txt
   # or: find src/java/org/apache/cassandra/<package> -name '*.java' | sort
   ```
2. **Show the list to the user and ask for confirmation** before going further — file count,
   file names, anything obviously missing or out of scope. Do not skip this step.
3. Produce a working diff against a sensible base (usually `main`):
   ```bash
   git diff main -- $(cat /tmp/file_list.txt | tr '\n' ' ') > /tmp/mega_review.diff
   # For files with no git history (new/untracked), read them directly — there is no diff.
   ```
4. Use this confirmed file list as the direct seed for the coverage checklist in Step 1e —
   paste the files into the `File` column verbatim.
5. In Phase 4, there are no commit boundaries: chunk by **file** instead of by commit —
   one `/shallow-review` invocation per file. Skip Step 1a and go straight to Step 1b.

**Step 1a — Get commit shape:**
```bash
git log --oneline --stat <base>..<head>
# Or for a single commit: git show --stat <SHA>
```

**Step 1b — Skim diff stats (do NOT read file bodies):**
```bash
git diff <base>..<head> --stat
# What to note: file names, extension types, rough LOC changes, new vs modified files
```

**Step 1c — Classify each commit/subsystem** by risk:
- **HIGH**: New algorithms, data structure rewrites, complex feature additions, new orchestration layers, state machine changes, bootstrap/recovery logic
- **MEDIUM**: Feature additions with new control flow, non-trivial interface changes with multiple callers
- **LOW**: Renames, moves, mechanical refactors, test-only or documentation changes

**Step 1d — Classify each source file** by type:
- **Algorithmic**: Array operations, tree traversals, binary search, merges, set operations
- **State machine**: Bootstrap, recovery, catchup, topology, lifecycle management
- **Wiring**: Delegation, API changes, parameter threading
- **Concurrency**: Shared state, synchronization
- **Documentation/Config**: Markdown, YAML, config files, skill definitions

**Step 1e — Write `/tmp/mega_coverage.md`** (the coverage checklist). Every changed non-test
file gets one row; pre-assign reviewers now based on risk:

```markdown
| File | LOC changed | Risk | Type | Assigned reviewers | Min required | Covered? |
|------|-------------|------|------|--------------------|--------------|----------|
| src/Foo.java | 320 | HIGH | Algorithmic | Phase 2 (deep), Phase 3 (targeted) | 2 | — |
| src/Bar.java | 45 | MEDIUM | Wiring | Phase 2 (deep), Phase 3 (targeted) | 2 | — |
| docs/SKILL.md | 18 | LOW | Documentation | Phase 4 (sha: def456) | 1 | — |
```

Assignment rules:
- **HIGH-risk file** → Phase 2 (deep) + Phase 3 (targeted) — min 2; also covered by Phase 4
- **MEDIUM-risk file** → Phase 2 (deep) + Phase 3 (targeted) — min 2; also covered by Phase 4
- **LOW-risk file** → Phase 4 (shallow) — min 1

**Step 1f — IMMEDIATELY start Phases 2, 3, and 4.** Do not pause to write analysis. Do not
invoke patch-explainer or any other sub-skill. The coverage checklist IS the Phase 1 output.
In the same response, spawn all Phase 2/3/4 subagents — see "Launching Phases 2/3/4" below.

---

## PHASE 2: Deep Review — invoke `/deep-review`

For every HIGH- and MEDIUM-risk file, invoke `/deep-review` scoped to that file. Each file
gets a **separate, fresh subagent** — they run in parallel and share no context with each other.
This phase runs **in parallel with Phases 3 and 4** — start it as soon as Phase 1 completes.

**Per-file input packet** (what the orchestrator provides to each `/deep-review` invocation):
- The diff for that file only: `git diff <base>..<head> -- <file>`
- The file's full current source (deep-review reads this itself, but point it at the right file)
- A 100–150 word orchestrator briefing:
  - File classification (algorithmic / state machine / wiring / concurrency)
  - What the file does in the context of the overall change (1–2 sentences from Phase 1)
  - Risk level and why it was classified HIGH or MEDIUM

---

## PHASE 3: Targeted Review — invoke `/targeted-review`

Invoke the `/targeted-review` skill scoped to **all HIGH- and MEDIUM-risk files** identified
in Phase 1 — not the full patch. Targeted-review is designed for focused review: it picks only
the bug-pattern categories whose diff signals match, then spawns tight subagents per focus area.
Feeding it the full patch dilutes that focus; feeding it the files that matter most lets it go
deep where it counts.

**Selecting the scope**:
- Include all files classified HIGH- or MEDIUM-risk in Phase 1
- Include files touched in multiple commits (cross-commit modification from the decomposition map)
- Exclude LOW-risk files, test-only files, and pure documentation/config changes

**When invoking `/targeted-review`**:
- Pass only the diff for the selected files (not the full patch)
- Include the Phase 1 risk classification and file-type notes as context so targeted-review
  subagents understand what to focus on

This phase runs **in parallel with Phases 2 and 4** — start it as soon as Phase 1 completes.
Collect all findings and tag each with `Phase 3 (targeted-review)` and its subagent focus.

---

## PHASE 4: Per-Commit Shallow Reviews — invoke `/shallow-review`

For each HIGH- and MEDIUM-risk commit, invoke `/shallow-review` scoped to that commit's diff.
Each invocation is a **fresh subagent** — it receives only that commit's diff, not the full
patch or accumulated findings from other commits.

This phase runs **in parallel with Phases 2 and 3** — start it as soon as Phase 1 completes.

**Step 4a**: Extract per-commit diff to its own file:
```bash
git diff <SHA>~1..<SHA> > /tmp/patch_<SHA>.diff
```

**Step 4b**: Assemble a briefing packet for `/shallow-review` containing ONLY:
- The commit's diff (`/tmp/patch_<SHA>.diff`)
- A 100–150 word orchestrator briefing covering:
  - What category the commit is (algorithmic, state machine, wiring, concurrency)
  - What risk level (HIGH/MEDIUM) and why
  - For "Fix" commits: "This commit says 'Fix X' — verify the fix is correct, complete,
    and does not introduce new bugs. Fix commits have a higher-than-average bug rate."

**Step 4c**: Run all HIGH- and MEDIUM-risk commit reviews in a single parallel batch. Collect
findings and tag each with the commit SHA.

---

## PHASE 5: Cross-Cut Passes

**Dispatch 5A, 5B, and 5C as parallel subagents.** Each receives a focused slice of findings
and a targeted task — not the full findings dump from all prior phases. The orchestrator
pre-filters: each subagent receives only the findings relevant to its pass.

### 5A: Pattern Amplification

Patterns come entirely from **bugs actually found in Phases 2–4** — there is no pre-loaded catalog.
If Phases 2–4 found zero bugs, 5A is skipped.

**Orchestrator's job** (done before spawning anything):
1. Collect all findings from Phases 2–4
2. For each distinct bug, characterize the structural pattern — not the specific instance, but
   the general shape: e.g. "method accepts a `start` offset parameter, but an internal
   `arraycopy` call ignores it and uses hardcoded `0`"
3. Decide the search scope based on the pattern type:
   - **Patch-only** — pattern is structural and specific to new code introduced in this PR
     (new algorithm, new data structure, new method overload family). Unlikely to exist elsewhere.
   - **Codebase-wide** — pattern is a convention violation or contract bug that could affect
     any code following the same convention (serialization encoding, API return value contracts,
     lifecycle protocols, error code handling). The same mistake recurs wherever the convention
     is followed, including in code not touched by this PR.
4. Grep at the chosen scope for locations that could match the pattern shape
5. Spawn one subagent per pattern, passing it: the pattern description + the grep hits + the
   scope used (so it knows whether to look beyond the patch if it finds new leads)

**Each subagent's job**: verify whether each grep hit is actually the same bug — read the
surrounding source as needed. Report confirmed additional instances only.

One confirmed bug → one pattern → potentially many additional instances found. This is the
highest-yield step when Phases 2–4 produce findings.

### 5B: Cross-Commit/Cross-Subsystem Consistency

**Subagent input**: The decomposition map from Phase 1 (file list, method signatures changed,
rename list) + the list of caller sites extracted by the orchestrator. Not the full patch.

1. **Interface contracts**: For each changed method signature, verify ALL callers updated
2. **Rename completeness**: `grep` for old names that should no longer appear
3. **Assumption conflicts**: When one part of the patch assumes X and another assumes Y about the same state

### 5C: Fix Verification

**Subagent input**: One subagent per "Fix" commit. Each receives: that commit's diff only +
a one-paragraph description of the bug it claims to fix.

For each fix commit:
1. Read the fix carefully
2. Try to construct an input that would still trigger the original bug
3. Try to construct an input that triggers a new bug introduced by the fix

### 5D: Optional Additional Review Passes

If Phases 2–5C revealed areas not yet covered, run additional targeted passes:

- If 5A found a pattern class in files not yet reviewed → `/deep-review` on those files with
  the specific pattern as focus guidance
- If 5B found cross-cut inconsistencies in files not yet reviewed → `/shallow-review` on the
  combined diff of those files
- If a fix commit (5C) looks suspicious → `/deep-review` on that file with guidance:
  "Verify this fix is correct and does not introduce new bugs."

---

## Coverage Gate — Verify Before Phase 6

Before merging findings, verify `/tmp/mega_coverage.md` is fully green.

**Step 1 — Mark completions**: For each file row, confirm every assigned reviewer phase ran
and reported (a "no findings" report counts). Update **Covered?**:
- `✓` — all assigned reviewers completed
- `GAP` — one or more assigned reviewers did not run

**Step 2 — Fill gaps**: For every `GAP` row, launch additional review passes now:
- File with 0 reviewers completed → `/shallow-review` on that file's diff
- HIGH- or MEDIUM-risk file with only 1 of 2 reviewers completed → `/shallow-review` on that file's diff

Run all gap-fill passes in a single parallel batch, then update the checklist.

**Step 3 — Gate**: Do not proceed to Phase 6 until every row in `/tmp/mega_coverage.md`
shows `✓`. If gaps remain after one gap-fill pass, repeat Step 2.

**Step 4 — Include the final checklist** in the Phase 6 report under a "Coverage" section.

---

## PHASE 6: Merge & Validate Findings

### Deduplication
Same location + same root cause = one finding. Boost confidence when found by multiple phases.

### Plausibility Validation

Every finding MUST pass ALL of:
1. **The code exists**: Point to exact file:line in the current code
2. **The bug is achievable**: Describe a concrete input that triggers it
3. **The result is wrong**: Explain what the code produces vs what it should produce
4. **Tests don't catch it**: Explain why (e.g., "tests only call the simple overload without offsets")

Discard findings that fail any of these.

### Confidence Ranking
- **High**: Found by Phase 2 (deep review), or found by 2+ phases independently
- **Medium**: Single phase found it, plausible trigger exists but complex to verify
- **Low**: Pattern match suggests possible bug but trigger is speculative

---

## Report Format

```
## Mega-Review: [commit range / feature name]

### Summary
- [N] commits/subsystems, [M] files, [L] LOC reviewed
- [X] bugs found: [H] high-confidence, [M] medium, [L] low

### Findings (ranked by confidence)

#### [HIGH] Finding 1: [precise title]
- **Location**: file:line
- **Found by**: Phase [N] ([audit/review type])
- **The bug**: [what the code does wrong, precisely]
- **Trigger**: [specific input that causes it]
- **Expected**: [what should happen]
- **Actual**: [what happens instead]
- **Why tests miss it**: [explanation]
- **Fix**: [specific code change]

[... more findings ...]

### Phase Coverage
| Phase | Scope | Findings |
|---|---|---|
| 1: Decompose & classify | git log + stat | (coverage map) |
| 2: Deep review | [N] HIGH+MEDIUM files | [M] bugs |
| 3: Targeted review | HIGH+MEDIUM files, [N] foci | [M] bugs |
| 4: Shallow review | [N] HIGH+MEDIUM commits | [M] bugs |
| 5A: Pattern amplification | [N] patterns | [M] additional bugs |
| 5B: Cross-cut | [checks] | [M] bugs |
```

---

## Anti-Patterns (What NOT to Report)

- **Style/naming issues** — Not bugs.
- **Performance concerns** — Not bugs (unless they cause incorrect results).
- **"Might be wrong" without a trigger** — If you can't describe a concrete input that causes
  incorrect output, it's not a finding.
- **Documentation mismatches** — Only report if the mismatch causes callers to use the API wrong.
