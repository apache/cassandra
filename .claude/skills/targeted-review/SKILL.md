---
name: targeted-review
version: "1.0.0"
description: >
  Findings-driven targeted code review. Understands the patch via patch-explainer and
  surrounding code via codebase-analysis, then consults a catalog of bug patterns
  (~11 categories under references/categories/). Picks only categories whose diff-signals
  match the patch and items whose "look for" hints match actual code shapes, groups them
  by review focus (function/file/feature), and spawns parallel subagents — each with a
  tight scope and selective checklist. Use for medium-to-large patches (50-1000 LOC) as
  a focused alternative to whole-patch ensemble or whole-file deep review. Triggers on:
  "review this patch/diff/change", "find bugs in this change", "scoped review", "what
  could go wrong here", "review using findings", or proactive PR review.
---

# Targeted Review

A meta-review skill. It does not encode bug patterns directly — it orchestrates other
skills and a categorized findings catalog into a tight, evidence-driven review.

## What it does

```
                +------------------+
                |   Input patch    |
                +--------+---------+
                         |
             +-----------+-----------+
             |                       |
   +---------v---------+   +---------v-----------+
   | patch-explainer   |   | codebase-analysis   |
   | (sub-skill)       |   | (sub-skill, scoped) |
   | what changed,     |   | invariants, callers,|
   | flow, assumptions |   | parallel paths      |
   +---------+---------+   +---------+-----------+
             |                       |
             +-----------+-----------+
                         |
         +---------------v----------------+
         | Pick categories + items        |
         | × 3-5 independent iterations  |
         | (each draw surfaces different  |
         |  items from the same catalog)  |
         +---------------+----------------+
                         |
                +--------v---------+
                | Pool & prioritize|
                | (items from 2+   |
                |  iterations →    |
                |  higher priority)|
                +--------+---------+
                         |
                +--------v---------+
                | Group items by   |
                | review focus     |
                | (file/func/feat) |
                +--------+---------+
                         |
            +------------+------------+
            |            |            |
    +-------v---+  +-----v-----+  +---v-------+
    | review    |  | review    |  | review    |
    | subagent  |  | subagent  |  | subagent  |
    | scope A   |  | scope B   |  | scope C   |
    +-------+---+  +-----+-----+  +---+-------+
            |            |            |
            +------------+------------+
                         |
                +--------v---------+
                | Merge & report   |
                +------------------+
```

## When to use this skill

- A patch or diff is on the table for review (single commit, multi-commit branch, or staged changes)
- The user wants more focus than `shallow-review` provides (six fixed lenses on the whole patch) but doesn't want to commit to `deep-review`'s file-by-file deep pass
- A `*-findings/` corpus exists or the categorized catalog under `references/categories/` is sufficient — the categories alone are usable; project corpora improve coverage
- Medium-to-large patches where dispatching focused subagents avoids overwhelming a single reviewer

## When NOT to use this skill

- Trivial 1-3 line changes — direct review is faster
- Pure refactors with no behavior change — `shallow-review` symmetry pass is enough
- The user wants a single comprehensive write-up of one file — use `deep-review` instead

---

## Workflow

You will go through seven phases. Phases 1a and 1b run in parallel. Phases 2-6 are sequential.

### Phase 1a: Understand the patch — call the patch-explainer skill

Invoke the `patch-explainer` skill on the patch. Use the Skill tool. You want patch-explainer
to produce its standard analysis (purpose, before/after diagrams, flow, assumptions,
failure modes).

If the patch is already provided as a file path, point patch-explainer at it. If you only
have a commit ref, ask for `git show <ref>` or read it from `git diff`. For multi-commit
branches, it is fine to feed the cumulative diff.

Capture patch-explainer's full output. You will refer to it in Phases 3-5.

### Phase 1b: Understand the surroundings — call the codebase-analysis skill

In parallel with 1a, invoke the `codebase-analysis` skill in **targeted mode** on the
affected subsystem only — not the whole repo. Pass it:

- The list of files touched by the patch
- The subsystem name if obvious (e.g., "the journal replay subsystem")
- A directive to focus on: invariants, lifecycle, parallel implementations, and recent
  bug history in the affected area

You want codebase-analysis to produce a scoped `analysis/system-analysis.md` that gives
you the context the patch sits inside — what the surrounding code expects, where parallel
paths exist, what historical bugs touched this area.

If invoking the full skill is too heavy for a small patch, instead spawn a research
subagent with this prompt: "Read these files: {touched files}. Summarize: (a) the
invariants the surrounding code maintains, (b) any parallel implementations or sibling
classes, (c) callers of the modified methods, (d) any recent bug-fix commits in this
area. Under 400 words." This is a lighter alternative that retains the spirit of
codebase-analysis. Prefer the full skill when the area is unfamiliar or large.

### Phase 2: Pick categories — 3-5 independent iterations

Category selection and item picking are speculative: you're making probabilistic judgments
about which patterns might apply, and a single pass will reliably miss some. Run **3-5
independent iterations** of Phases 2+3, each starting from scratch with the same patch
and context but without looking back at what prior iterations chose. Treat each iteration
like a fresh read — you are more likely to notice different things each time.

For each iteration, read `references/categories/INDEX.md` — it lists all 11 categories
with their descriptions and diff signals in one file (~200 lines). For each category, ask:
does the patch contain ANY of the diff signals listed?
- If yes → mark as **load**
- If no → skip

Typically 3-7 categories load per iteration. If you would load more than 8, re-examine.
If fewer than 2, the patch is probably too small for this skill.

### Phase 3: Pick items within each loaded category (one pass per iteration)

Within each iteration, read the **full** content of each loaded category file. For each
finding (`### F-NN: title` + body + `**Look for:** {hint}`), ask:

1. Does the "Look for" hint match a code shape that **actually appears in this patch**?
2. Does the patch context (from patch-explainer + codebase-analysis) make this finding
   **plausibly applicable** — not just a keyword match?

If yes to both → keep the item.
If no → drop.

Be selective: a category typically has 25-40 findings; keep 3-12 per category per
iteration. Repeat for all iterations before moving to Phase 3b.

### Phase 3b: Pool and prioritize across iterations

After all iterations are complete, merge the item lists:

- An item selected in **2 or more iterations** is a strong candidate — mark it **priority**.
- An item selected in only one iteration is still in scope — mark it **speculative**.
- Discard nothing yet; let the subagents decide.

The iteration count next to an item is a confidence signal for the subagents, not a
gate. A speculative item can still turn into a real bug; a priority item can still be
a false alarm.

### Phase 4: Group items by review focus

Now you have a pooled list of selected items across categories and iterations. Group them
by **review focus** — what part of the change a single reviewer should examine. A focus
is one of:

- A specific function or method (`MyClass.replay()`)
- A specific file or small group (`FooSerializer.java` and its sibling `FooDeserializer.java`)
- A feature, behavior, or concept ("the new fsync coordination", "the version-gated dispatch path")
- A specific cross-cutting concern in the patch ("any place we read the new `dirty` flag")

Aim for 2-5 foci per patch. Each focus should have **5-15 checklist items** drawn from
across the categories — not too few (subagent has nothing to do) and not too many
(subagent gets overwhelmed). Include each item's priority annotation (priority /
speculative) so the subagent knows where to focus first.

If multiple foci would receive the same checklist item, that's fine — the same pattern
can apply at multiple sites. Tag each item with the category it came from so the
subagent knows the rationale.

### Phase 5: Spawn focused review subagents

For each focus, spawn one review subagent in parallel using the Agent tool. Use the
template in `references/subagent-prompt-template.md`. The subagent prompt must contain:

- The patch (path, or pasted, or ref)
- A 100-word summary of patch-explainer's findings (relevant slice only — not the full output)
- A 100-word summary of codebase-analysis's findings (relevant slice only)
- The focus statement (what part of the change to review)
- The checklist items, each with: category, priority annotation, finding text, and
  look-for hint
- Instructions on how to gather evidence (read source, grep, check parallel paths) and
  what confidence to assign

Each subagent should produce: a list of findings, with location, confidence, and what
evidence was checked.

Run all subagents in a single tool call (parallel). They are independent.

### Phase 6: Merge and report

When all subagents return, merge findings:

1. **Deduplicate** — same code location + similar reasoning → one finding, both subagents
   credited
2. **Rank** by confidence — High first, then Medium, then Low
3. **Cross-reinforce** — if a finding was raised by multiple subagents, or if multiple
   findings cluster on the same lifecycle/state, boost confidence
4. **Three-point check** each surviving finding:
   - The code construct actually exists in the diff (not inferred from absence alone)
   - The bug is plausible given the visible context (not speculative)
   - The finding is actionable (the reader can tell what to change)

Use the format in `references/report-format.md`.

---

## Reference files

| Reference | When to read |
|---|---|
| `references/subagent-prompt-template.md` | Phase 5, when spawning each review subagent |
| `references/report-format.md` | Phase 6, when merging and presenting findings |
| `references/categories/INDEX.md` | Phase 2 — single-file scan of all category descriptions + diff signals |
| `references/categories/<name>.md` | Phase 3 — full body of each loaded category, picked from INDEX |

The categories in `references/categories/` are project-agnostic patterns mined from real
bug fixes across distributed-systems projects (Cassandra, Kafka, Iceberg). They describe
generalized code-level shapes, not project-specific issues.

## How this differs from related skills

| Skill | Scope | Pattern source | Subagent dispatch |
|---|---|---|---|
| `shallow-review` (cassandra-edge-case-explorer) | Whole patch | 6 fixed lenses (~100 items total) | One subagent per lens, fixed |
| `deep-review` | User-named files | 444-pattern catalog | One subagent per file, full catalog |
| `targeted-review` (this) | Whole patch, focus-decomposed | ~11 categories (300+ findings) — selectively loaded | One subagent per focus, selectively populated checklist |
| `mega-review` | Large branches | Orchestrates the above | Multi-pass |

Use `targeted-review` when:
- The patch spans 50-1000 LOC
- You want to spend tokens on **what matters for THIS patch**, not on a fixed catalog
- You have a `*-findings/` corpus or are reviewing systems-level code that benefits from
  the curated categories

## Pitfalls and guardrails

- **Don't skip Phase 1.** Calling patch-explainer and codebase-analysis is what lets you
  pick categories and items intelligently. Without that grounding, you'll loose-match
  on keywords and overwhelm subagents.
- **Don't look at prior iterations while picking.** The whole value of multiple iterations
  is independent draws. If you anchor on the first iteration's selections, you get one
  draw repeated instead of several independent ones.
- **Don't load every category "just in case".** The whole value is selection. If you
  match more than 8 categories in an iteration, you've probably been too generous.
- **Don't pass the full catalog to subagents.** Each subagent gets only the items
  selected for its focus. Passing all items defeats the purpose.
- **Don't substitute findings for understanding.** A finding that "matches a code shape"
  is a hypothesis to verify — the subagent confirms or refutes by reading the code, not
  by trusting the pattern. Mark uncertain findings as Low confidence.
- **The checklist is the menu, not the order.** A subagent should also report bugs it
  notices that aren't on the checklist — the checklist primes attention, it doesn't cap
  it.
