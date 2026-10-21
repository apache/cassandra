---
name: heatmap
version: "1.0.0"
description: Use git heatmap analysis to identify high-churn files and lines as candidates for thorough review or bug hunting. Works for PR reviews, security audits, bug hunts, or any code analysis task.
---

# Heatmap-Guided Review

Use the bundled `heatmap.py` script to identify the hottest (most frequently and recently changed) code in a repository, then focus review effort on those areas. Hot code is statistically more likely to contain bugs: it changes often, accumulates complexity, and is where active development risk concentrates.

## When to use

Invoke this skill whenever you want to prioritize where to look in a codebase — during PR reviews, bug hunts, security audits, onboarding exploration, or any task where you need to decide which files and lines deserve the closest attention.

## Prerequisites

- Python 3 must be available.
- The target must be a git repository.
- The heatmap script is bundled with this skill at `~/.claude/skills/heatmap/heatmap.py`. All commands below use `HEATMAP` as a placeholder:
  ```bash
  HEATMAP="$HOME/.claude/skills/heatmap/heatmap.py"
  ```
  If `heatmap` is available on PATH, you may use that instead.

## Workflow

### Step 1: Determine the repository path and scope

Figure out the git repository root. If the user provided a repo path, use that. If working in a repo already, use the current directory. If reviewing a PR, determine which files were changed.

```bash
HEATMAP="$HOME/.claude/skills/heatmap/heatmap.py"
# Find the repo root
git -C <path> rev-parse --show-toplevel
```

### Step 2: Run repo-level heatmap

Get the hottest files in the repository. Use `--json` for machine-readable output. Adjust `--top` based on context (20-50 for a broad scan, more for large repos).

```bash
python3 "$HEATMAP" repo --json --top 50 <repo_path>
```

This returns JSON array of objects with: `path`, `heat`, `commits`, `last_modified_days`.

### Step 3: Cross-reference with the task context

**For PR reviews:** Intersect the heatmap results with the files changed in the PR. Files that are both changed in the PR AND high on the heatmap are the highest-priority review targets — they are already complex/volatile and the PR is adding more changes to them.

```bash
# Get PR changed files
git diff --name-only <base_branch>...HEAD
```

**For bug hunts:** The hottest files ARE the candidates — frequent recent changes correlate with bugs. Focus on the top 10-20.

**For security audits:** Filter heatmap results to security-sensitive paths (auth, crypto, input parsing, network, serialization).

**For general exploration:** Use the heatmap as a map of where active development is happening.

### Step 4: Run line-level heatmap on top candidates

For each high-priority file (typically 3-8 files), run the line-level heatmap to find the hottest regions within those files.

```bash
python3 "$HEATMAP" file --json <file_path_relative_to_repo> <repo_path>
```

This returns JSON array of objects with: `line`, `heat`, `content`.

### Step 5: Identify hot zones

From the line-level results, identify contiguous regions of high heat. These "hot zones" are where to focus review. Look for:

- **Clusters of hot lines** — contiguous blocks of high-heat code indicate areas under active rework. These are prime bug candidates because:
  - Multiple recent changes suggest the logic is not yet settled
  - Each change is an opportunity for regression
  - Complex interactions between recent changes may not be fully tested

- **Hot lines surrounded by cold code** — surgical edits in otherwise stable code may indicate bug fixes, workarounds, or special-case handling that deserves scrutiny.

- **Hot function/method boundaries** — if a method signature or its first few lines are hot, the contract may have changed recently, affecting all callers.

### Step 6: Produce the review focus list

Output a ranked list of review targets, structured as:

```
## Heatmap Review Targets

### Priority 1: <file_path> (heat: X, commits: Y)
- **Hot zones**: lines A-B (description of what this code does)
- **Why it matters**: <context — e.g., "most changed file in repo AND modified in this PR">
- **What to look for**: <specific guidance based on the code — race conditions, edge cases, etc.>

### Priority 2: ...
```

### Step 7: Deep review

For each priority target, read the hot zones and perform the actual review. The heatmap tells you WHERE to look; your expertise tells you WHAT to look for. Common patterns in hot code:

- **State management bugs**: Hot code often manages complex state. Check for inconsistent updates, missing synchronization, or partial failures.
- **Edge cases**: Frequent changes often mean edge cases keep being discovered. Look for more.
- **Regression risk**: If code was recently fixed, check whether the fix is complete and doesn't break other paths.
- **Missing tests**: Hot code that lacks test coverage is the highest-risk combination.

## Output format

Always present findings as a prioritized list with:
1. File path and heat metrics
2. Specific line ranges to focus on
3. What the hot code does (brief)
4. What risks to look for (specific to the code, not generic)

## Tips

- Heat is relative — compare files against each other, not against an absolute threshold.
- Files with high heat but few commits have large individual changes (risky). Files with high heat and many commits are under constant churn (also risky, differently).
- `last_modified_days` close to 0 means very recent changes — highest chance of unfound bugs.
- Use `--since` to adjust the time window. Default is 2 years. For recent bug hunts, try `--since "6 months ago"`.
- The `--no-color` flag is useful when piping output, but prefer `--json` for programmatic use.
