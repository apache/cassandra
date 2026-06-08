# Bug-Archaeology -- Eval Prompt

Run this to test a review skill against a known bug-introducing patch.

## Setup: Find an Introducing Patch

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

## Choosing a Review Skill

The user must specify which skill to use. Pick based on patch characteristics:

| Skill | When to use | LOC |
|-------|-------------|-----|
| `/shallow-review` | Quick first pass, broad surface scan | Any |
| `/deep-review` | Focused on specific files, complex logic | Any |
| `/targeted-review` | Findings-driven, medium-to-large changes | Medium+ |
| `/mega-review` | Large feature branches, multi-commit ranges | 1000+ |

## Eval: Running the Review

Invoke the chosen skill against the extracted patch. Provide the patch path as context.

### shallow-review
```
/shallow-review

Patch: /tmp/eval_patch.diff
```

### deep-review
```
/deep-review

Patch: /tmp/eval_patch.diff
Focus file: <PRIMARY file from setup>
```

### targeted-review
```
/targeted-review

Patch: /tmp/eval_patch.diff
```

### mega-review
```
/mega-review

Patch: /tmp/eval_patch.diff
```

## Scoring

After the skill reports, compare findings against the actual bug:

```bash
grep -A5 '## Root Cause' "$BUGFILE"
```

Score the review output as:
- **Exact**: The skill identified the specific bug that was later fixed
- **Partial**: The skill flagged the right area/pattern but described a different specific issue
- **Different bug**: The skill found real bugs, but not the target
- **Miss**: The skill found nothing relevant

For `shallow-review`, also track **per-specialist** hits (Logic, Boundary, Concurrency, Resources, Absence, Completeness) to identify which domains need improvement.

## Comparison Eval

To compare skill effectiveness, run the same patch through multiple skills and record scores side by side:

| Patch | shallow | deep | targeted | mega |
|-------|---------|------|----------|------|
| bug-0001 | Exact | Partial | Exact | Exact |
| bug-0042 | Miss | Exact | Partial | Exact |

This identifies which skill is most reliable for which bug categories.

## Prodding (for Misses)

If a skill missed, tell it what the bug actually was and ask:

```
You missed this bug: [description of actual bug].
Which checklist or pattern in the skill would have helped you find it?
If none exists, propose a new item to add.
```

Use the feedback to improve the relevant skill's reference files.

## Batch Eval

For batch evaluation across N bugs:
1. Extract N patches using the setup script
2. For each patch, invoke the chosen skill (one invocation per patch)
3. Wait for results, score each
4. Tabulate: exact/partial/different/miss rates
5. For shallow-review: also tabulate per-specialist hit rates
6. Optionally compare two skills on the same patches

Target: >90% hit rate (exact + partial), <5% miss rate.
