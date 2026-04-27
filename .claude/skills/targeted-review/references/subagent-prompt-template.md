# Review subagent prompt template

Use this template when spawning a review subagent in Phase 5. Each subagent reviews ONE
focus with a tailored checklist drawn from the categorized findings.

---

## Template

```
You are reviewing a focused part of a patch for correctness bugs. You will gather
evidence from the actual source — not just pattern-match against a checklist.

PATCH
-----
{patch path or ref, or pasted diff}

PATCH SUMMARY (from patch-explainer; trimmed)
---------------------------------------------
{2-5 bullet points on what changed and why, only the parts relevant to your focus}

SURROUNDING CONTEXT (from codebase-analysis; trimmed)
-----------------------------------------------------
{2-5 bullet points: invariants, lifecycle, parallel implementations, callers — only what
is relevant to your focus}

YOUR FOCUS
----------
{One sentence: what part of the change you are reviewing.
Examples:
- "Review the new replay() method in JournalReplay.java and its interaction with fsync"
- "Review the version-gated decoding in MessageDeserializer.decode()"
- "Review the new field 'dirtyVersion' wherever it is read or written"
- "Review the registration symmetry of the new MetricsListener" }

CHECKLIST
---------
For each item below: check whether the pattern actually applies to your focus area.
Gather evidence (read the source, grep for callers, check parallel paths) before
reporting.

{For each selected item:}
- [{category-id}] {Finding text}
  Look for: {hint}

{Repeat for each item in this focus's checklist — typically 5-15 items.}

INSTRUCTIONS
------------
1. Read the patch and the relevant source files. Do not rely on summaries alone.

2. For EACH checklist item, decide one of:
   - APPLIES — the pattern matches a real construct in the focus area, and the bug is
     plausible given context. Report it.
   - DOES NOT APPLY — the pattern keyword matched but the code does not actually have
     the bug shape, OR the surrounding code prevents the failure. Skip silently.
   - UNCERTAIN — the pattern might apply but you cannot confirm without more digging.
     Report at Low confidence with the open question.

3. Also report bugs you notice in the focus area that are NOT on the checklist. The
   checklist is a starting point, not a fence.

4. For evidence: prefer reading the actual source over inferring. Use Grep to find
   callers, parallel paths, or symmetric code. Use Read to inspect files referenced in
   the diff.

5. Confidence rubric:
   - HIGH: code construct exists in diff, you have evidence the bug occurs, you can
     point to specific failure inputs or scenarios.
   - MEDIUM: code construct exists, the bug is plausible, but you couldn't fully verify
     the failure path or you're inferring some context.
   - LOW: pattern match without strong evidence; flag for the human to investigate.

OUTPUT
------
For each finding:

  ## Finding: {short descriptive title}
  - Location: {file:line}
  - Category: {category-id from checklist, or "off-checklist"}
  - Confidence: High / Medium / Low
  - What's wrong: {1-3 sentences explaining the bug}
  - Evidence: {what you searched / read / found, briefly}
  - Suggested fix: {specific change, if obvious}

If no findings in this focus: say so explicitly with one sentence on what you checked.

End with a coverage note:
  Checklist coverage: {N/M items applied}, {K off-checklist findings}
```

---

## How to populate the template

For each focus you defined in Phase 4:

1. **Patch summary**: extract from patch-explainer's output, the parts relevant to this
   focus — typically the before/after for the relevant function/file plus any noted
   assumptions or failure modes that touch the focus area.

2. **Surrounding context**: extract from codebase-analysis output, the parts about
   invariants, callers, parallel paths that touch the focus area.

3. **Focus statement**: be specific and narrow. "Review the journal replay subsystem"
   is too broad. "Review JournalReplay.replay() and its handling of fsync ordering with
   the new dirty flag" is right.

4. **Checklist items**: each item has three parts — the category id (e.g.,
   `concurrency-and-locking`), the finding text (one sentence describing the bug
   pattern), and the look-for hint (a concrete code-shape search). Tag each so the
   reviewer knows the rationale.

## Sizing guidance

- **Too few items (<3)**: the focus is too narrow. Either merge with another focus, or
  drop the focus and review inline.
- **Too many items (>15)**: the focus is too broad. Split it (e.g., one focus per
  function, not one per file).
- **Sweet spot**: 5-12 items per focus. Each subagent should be able to apply each item
  thoughtfully, not skim.
