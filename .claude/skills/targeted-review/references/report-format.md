# Report format

Use this format in Phase 6, after all review subagents have returned. The aim is a
report that a human reviewer can act on without re-doing the work — every finding
should answer "where, what, why-I-believe-it, what-to-do".

---

## Format

```
# Targeted Review: {patch identifier — commit, branch, or short description}

## Patch summary
{2-3 sentences: what the patch does, drawn from patch-explainer's executive summary}

## Categories considered
- Loaded: {list — e.g., serialization-and-versioning, concurrency-and-locking, ...}
- Skipped: {list with one-word reason — e.g., "lifecycle-and-ordering (no init/register
  changes)", "io-and-crash-safety (no persistence touched)"}

## Foci dispatched
- Focus A: {one-line statement} — {N items, K findings}
- Focus B: {one-line statement} — {N items, K findings}
- Focus C: ...

## Findings (ranked by confidence)

### Finding 1: {short descriptive title}
- **Location**: {file:line}
- **Confidence**: High / Medium / Low
- **Found by**: {focus name(s)}
- **Category**: {category-id, or "off-checklist"}
- **What's wrong**: {2-3 sentences with reasoning}
- **Evidence**: {what was searched / read / found}
- **Suggested fix**: {specific change}

### Finding 2: ...

## Cross-cutting observations
{Any patterns that emerged from multiple foci — e.g., "two foci independently flagged
missing equals/hashCode updates for a new field", or "the new fsync coordination is
tested at one call site but not at the parallel one in BackupWriter"}

## What was NOT reviewed
{Brief list of categories or aspects skipped, so the reader knows the boundary of this
review. Keep terse — it's a closure note, not a full audit.}

## Recommended follow-ups
{Optional — if findings cluster around a particular concern that would benefit from
deeper investigation, name it. E.g., "The serialization symmetry findings warrant a
dedicated deep-review pass on FooSerializer.java and its sibling Bar deserializer."}
```

---

## Ranking and dedup

Within "Findings (ranked by confidence)":

1. **Sort** by confidence (High → Medium → Low), then by impact (correctness > efficiency
   > style)

2. **Deduplicate** — if two foci flagged the same root cause at the same location,
   merge into one finding and credit both. If they reached different conclusions, keep
   both with a note on the disagreement.

3. **Boost** confidence when:
   - Multiple foci independently flagged the same site (mark as "Found by: Focus A, B")
   - A finding ties to a category that has historically high incidence in this corpus
     (e.g., logic errors are the largest category — a logic finding deserves attention)
   - Codebase-analysis surfaced the same concern as a known invariant or past bug

4. **Three-point test** each finding before including:
   - The code construct exists in the diff (not inferred from absence alone)
   - The bug is plausible given visible context (not pure speculation)
   - The finding is actionable (the reader can tell what to change)

   If a finding fails the test, drop it or downgrade to a "uncertain — investigation
   note" at the bottom.

## Tone and concision

- Lead each finding with location and confidence — the reader should be able to scan and
  decide priority in one pass.
- 2-3 sentence reasoning, not paragraphs. The diff is on screen; don't restate it.
- Evidence should be concrete: "Grepped for `equals(` in {Class}.java — no override
  found, and parent class compares only by id" is useful. "I checked and it seems
  wrong" is not.
- Suggested fix should be specific. "Add a null check" is too vague. "Wrap line 142 in
  `if (config.getFoo() != null)`" is right.

## Length

Aim for the whole report to fit on one screen of scrolling for small patches and 2-3
screens for larger ones. If the report is longer, group findings under sub-headings by
focus or by category. The reader should be able to print it out and walk through
findings during review.
