# Edge Case Explorer -- Eval Prompt

Run this to test the skill against a known bug-introducing patch.

## Setup: Find an introducing patch

```bash
# Pick a random bug from archaeology, trace to the introducing commit
BUG_DIR=/path/to/bug-archaeology
REPO=/path/to/cassandra

# 1. Pick a bug file
BUGFILE=$(ls $BUG_DIR/bug-*.md | shuf -n1)
FIX_HASH=$(grep -oP 'Commit\*\*: \K\w+' "$BUGFILE" | head -1)

# 2. Find the primary changed Java file
PRIMARY=$(git -C $REPO diff --name-only "${FIX_HASH}~1" $FIX_HASH | grep '\.java$' | grep -v 'test/' | head -1)

# 3. Find the first removed (buggy) line number
LINE=$(git -C $REPO diff "${FIX_HASH}~1" $FIX_HASH -- "$PRIMARY" | \
  awk '/^@@ -([0-9]+)/{line=$0; gsub(/.*-/,"",line); gsub(/,.*/,"",line)} /^-[^-]/{if(length($0)>6) {print line; exit}}' )

# 4. Blame to find introducing commit
INTRO=$(git -C $REPO blame -L "$LINE,$LINE" "${FIX_HASH}~1" --porcelain -- "$PRIMARY" | head -1 | cut -c1-40)

# 5. Extract introducing diff
git -C $REPO diff "${INTRO}~1" $INTRO -- "$PRIMARY" > /tmp/eval_patch.diff
echo "Bug: $(head -1 "$BUGFILE")"
echo "Introducing commit: $INTRO"
echo "Diff: $(wc -l < /tmp/eval_patch.diff) lines"
```

## Eval: Ensemble Mode (default)

Launch 5 specialist agents in parallel, each reading only their checklist + the patch.
Use sonnet model for speed.

```
# For each specialist, launch an agent with:
#   1. Their specialist checklist file
#   2. The patch to review
#   3. A focused prompt (see SKILL.md for templates)

Agent(model=sonnet, prompt="""
You are a {SPECIALIST} specialist. Review ONLY for {DOMAIN_KEYWORDS}.
Read your checklist: `{SKILL_DIR}/references/general/pass2-specialists/pass2-{name}.md`
Then read the patch: `/tmp/eval_patch.diff`
Report your single best finding in <=50 words: Pattern ID, Location, What's wrong.
Or "No finding in my domain".
""")

# Specialists to launch (all 5 in parallel):
# 1. LOGIC      -- pass2-logic.md
# 2. BOUNDARY   -- pass2-boundary.md
# 3. CONCURRENCY -- pass2-concurrency.md
# 4. RESOURCES  -- pass2-resources.md
# 5. ABSENCE    -- detection-signatures.md
```

### Key constraints for eval agents

- Each agent reads exactly 2 files: their checklist + the patch
- Use sonnet model for faster turnaround
- Ask for concise output (<=50 words per finding)
- Run all 5 in parallel (single message with 5 Agent tool calls)

## Scoring

After all specialists report, merge findings and compare against the actual bug:

```bash
# The actual bug description is in the bug file
grep -A5 '## Root Cause' "$BUGFILE"
```

Score the **ensemble** (merged set of all specialist findings) as:
- **Exact**: At least one specialist identified the specific bug that was later fixed
- **Partial**: At least one specialist flagged the right area/pattern but described a different specific issue
- **Different bug**: Specialists found real bugs, but not the target
- **Miss**: No specialist found anything relevant

Also track **per-specialist** hits to identify which domains need improvement.

## Comparison Eval: Single vs Ensemble

To compare, also run the same patch through a single generalist agent:

```
You are reviewing a code patch for potential bugs. You have NO access to the broader
codebase -- only the patch and the reference files below.

Read (in order):
1. `{SKILL_DIR}/references/general/pass1-strategic-patterns.md`
2. `{SKILL_DIR}/references/general/pass2-tactical-checklist.md`
3. `{SKILL_DIR}/references/general/detection-signatures.md`

Then read: `/tmp/eval_patch.diff`

Report top 2-3 bugs found. Keep response under 400 words.
```

Score both and compare hit rates.

## Prodding (for misses)

If the ensemble missed, tell each specialist what the bug actually was and ask:

```
You missed this bug: [description of actual bug].
Which checklist question in YOUR specialist file would have helped you find it?
If none exists, propose a new question to add to your checklist.
```

Use the feedback to improve the specialist checklist files.

## Batch Eval

For batch evaluation across N bugs:
1. Extract N patches using the setup script
2. For each patch, launch 5 specialists (one message per patch)
3. Wait for results, merge, score
4. Tabulate: exact/partial/different/miss rates, per-specialist hit rates
5. Compare against single-agent baseline on the same patches

Target: >90% hit rate (exact + partial), <5% miss rate.
