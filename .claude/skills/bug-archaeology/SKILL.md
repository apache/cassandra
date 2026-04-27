---
name: bug-archaeology
version: "1.0.0"
description: >
  Mine bug patterns from any git repository. Discovers bug-fix commits via git log
  heuristics, analyzes each in parallel with subagents, writes individual analysis files,
  and synthesizes a generalized PATTERNS.md with repo-specific details stripped.
  Invoke explicitly with /bug-archaeology.
---

# Bug Archaeology

Systematic extraction of reusable bug-detection heuristics from a repository's commit history.

## Arguments

All optional. Parse from the user's invocation string.

| Arg | Default | Description |
|-----|---------|-------------|
| `--range` | `HEAD~200..HEAD` | Git revision range |
| `--path` | (all files) | Restrict to commits touching this path glob |
| `--since` / `--until` | (none) | Date filter for `git log` |
| `--limit` | 200 | Max bug-fix commits to analyze |
| `--batch-size` | 10 | Commits per subagent |
| `--output` | `./bug-archaeology/` | Output directory |

## Workflow

Three sequential phases. Each phase completes fully before the next starts.

### Phase 1: Discovery

Find bug-fix commits via git log heuristics.

```bash
git log --no-merges --pretty=format:"%H %s" [--since=X] [--until=X] <range> [-- <path>] \
  | grep -iE '(fix|bug|patch|resolve|repair|correct|workaround|hotfix|regression|#[0-9]+|[A-Z]+-[0-9]+)'
```

**Filter out** commits whose message matches ONLY cosmetic keywords (`typo`, `whitespace`, `import`, `format`, `style`, `cleanup`, `rename`) with no bug keywords.

**Truncate** to `--limit`.

**Write** `QUEUE.txt` to the output directory:
```
0001 abc123def First line of commit message
0002 def456abc Another commit message
```

**Resumption**: If `PROGRESS.md` exists, read it and exclude commits already marked `done` or `skipped`. Re-include `pending` and `error` entries.

### Phase 2: Analysis (parallelized)

Spawn subagents to analyze commits in batches. Each commit is independent.

**Subagent prompt template** — adapt as needed:
```
Read the file at references/per-bug-format.md for the output format specification.

Analyze these commits from the repository at <REPO_PATH>:
<list of "NNNN hash message" lines>

For each commit:
1. Run: git show <hash>
2. If the commit is cosmetic, test-only, refactor-only, or a merge with no
   original changes — mark it as skipped with a one-word reason.
3. Otherwise, write the analysis to <OUTPUT>/bug-<NNNN>-<slug>.md
   following the format in per-bug-format.md exactly.

Return a status line per commit: NNNN <hash> done|skipped [reason-or-filename]
```

**Parallelism**: Launch up to 5 subagent batches concurrently. Wait for all to complete before the next wave.

**Progress tracking**: After each wave, append results to `PROGRESS.md`:
```markdown
| # | Commit | Status | File / Reason |
|---|--------|--------|---------------|
| 0001 | abc123d | done | bug-0001-null-after-lookup.md |
| 0002 | def456a | skipped | cosmetic |
```

**Errors**: Mark as `error` with a one-line reason. Don't retry — the user re-invokes with `--resume`.

### Phase 3: Synthesis

After all analysis is complete, produce `PATTERNS.md`. Read `references/synthesis-format.md` for the full specification.

**Process**:
1. Read all `bug-*.md` files in the output directory
2. Parse the `Tags` section from each — extract `category` and `code-pattern`
3. Group bugs by `code-pattern` tag
4. For each group of 2+ bugs with the same code-pattern:
   - Extract the shared Detection Rule (generalize if they differ across instances)
   - Pick the most representative Example
5. **Generalize** — strip repo-specific details:
   - Replace specific class/method names with structural descriptions
   - Remove ticket IDs, author names, project-specific subsystem labels
   - Keep language-specific API names only if scope is that language or broader
6. Group patterns by `category` and write `PATTERNS.md`
7. Patterns with only 1 instance go in a "Singletons" section at the end

## Reference Files

| File | Loaded by | Purpose |
|------|-----------|---------|
| `references/per-bug-format.md` | Subagents (Phase 2) | Per-bug output template and field guidelines |
| `references/synthesis-format.md` | Orchestrator (Phase 3) | PATTERNS.md structure and generalization rules |

## Evaluating the Skill

Full instructions are in `references/EVAL-PROMPT.md`. Summary:

1. **Pick a bug** from the archaeology output and trace its introducing commit via `git blame`.
2. **Extract the patch** with `git diff ${INTRO}~1 $INTRO`.
3. **Choose a review skill** — the user must specify one:
   - `/shallow-review` — quick 6-specialist parallel scan, any patch size
   - `/deep-review` — focused on specific files, complex logic
   - `/targeted-review` — findings-driven, medium-to-large patches
   - `/mega-review` — large feature branches or 1000+ LOC diffs
4. **Run the chosen skill** against the extracted patch.
5. **Score** the output: Exact / Partial / Different bug / Miss.
6. **Prod misses** — tell the skill what it missed and ask which checklist item would have caught it; use the answer to improve that skill's reference files.

Target: >90% hit rate (exact + partial), <5% miss rate across a batch of N bugs.
