# Correctness Skills

A collection of skills for finding, understanding, reproducing, and preventing bugs.

General approach and here is to try and codify whatever an engineer that cares about correctness would do to validate the code. Sometimes we do not have 
a sufficient amount of time to write a spec, or do one more pass of the review, or review a patch we have not been involved in. This is a collection of 
things I have been using to check if my own work is up to a standard, but also to pursue some of the ideas I would otherwise not have time to pursue: 
explore a speculative idea, write a TLA+ specification for a subsystem, dig into the files with high commit density, etc. 

This started with a small pull request review prompt, and grew as different "what if" ideas were popping up. While listening to a podcast, I heard a person 
on it mentioned that they kept a log of issues popping up again and again and they were encoding them, either in form of code review checklist, or in a form
of tooling, which is where [bug-archaeology](#bug-archaeology) came from. I have indexed 3000 bugs from Apache Cassandra codebase, and made a library/checklist of 
things that can be used to verify the code in the future. 

Even though the tool was yielding good results and finding issues in Apache Cassandra codebase, I was not able to quantitatively confirm its quality. At which
point I have introduced evals, and started iterating. Compare the tool's output either to its own previous run, or to a simpler prompt or some popular skill
that does the same thing. Iterate, improve, and refine until you can score better. Without evals, results are purely anecdotal. Evals give you a way to quantify
and iterate.

Recent eval that I have taken a note of was; finding _human confirmed_ issues in the codebase the tool _was not indexed on_:

```
Volume & signal-to-noise

  ┌─────────────────────────────┬──────┬────────┐
  │           Metric            │<this>│<other> │
  ├─────────────────────────────┼──────┼────────┤
  │ Total findings (20 commits) │ 151  │ ~193   │
  ├─────────────────────────────┼──────┼────────┤
  │ Avg findings/commit         │ 7.6  │ 9.7    │
  ├─────────────────────────────┼──────┼────────┤
  │ High+Critical share         │ 37%  │ 20%    │
  ├─────────────────────────────┼──────┼────────┤
  │ Low findings share          │ ~30% │ 53%    │
  └─────────────────────────────┴──────┴────────┘

  <this> finds fewer things but a far higher fraction are High/Critical. <popular open source skill> finds more but is noisier (over half Low severity).

  ---
  Coverage asymmetry

  - Both found: 3 bugs (all resource leaks with visible lifecycle asymmetry)
  - <this> only: 1 bug (TOCTOU double-call in SHA 19 — Concurrency specialist)
  - <popular open source skill> only: 2 bugs 
```

Original "archaeology" later got split into shallow- and deep- review skills, which helped to get consistently high score on larger patches, too. 
New(er) targeted-review is an experiment that came after trying (and failing) to generalize existing bugs/issues into semgrep scripts: essentially, 
patterns were at first too noisy, and later were catching only very specific permutations of issues, so this approach allows to do "semantic" checks
by allowing the model to make a checklist of most probable patterns and then going through the patch once again. Evals for this one are somewhat
more difficult to quantify, as they are designed to trigger only for specific issues (which they do). 

A `mega-review` patch is a final iteration that chunks up larger patches and performs multiple steps/iterations over each one of them based on a 
set of rules.

For writing repros, you will often have to guide the model and suppress its attempts to go deep into internals to conjure up a repro that does exactly 
what it wants but does not reveal the real issue. Introduce guidelines and close the loop by setting strict and clear exit criteria.

**Example prompt**

Once the skills are installed, try this multi-pass review workflow on a subsystem or commit SHA:

> Review all the code related to \<subsystem\> (or just give it an SHA). Do the first pass
> using `/shallow-review`. Then analyze the code using `/patch-explainer`, identify core
> components that might be most prone to critical mistakes, and use `/deep-review` skill on
> the files related to these components. Do a third `/deep-review` pass from the files
> identified as "hot" using `/heatmap` skill, and a final pass of `/targeted-review` for the
> files that contain the most tricky logic. 

This chains four skills — broad scan, structural understanding, targeted deep review, and
churn-guided deep review — to progressively narrow focus onto the code most likely to
contain correctness bugs.

## Where Do I Start?

Pick your entry point based on what you're looking at right now:

| You have…                        | Start with                              |
|----------------------------------|-----------------------------------------|
| A small patch to review          | [shallow-review](#shallow-review), then [deep-review](#deep-review) for flagged areas |
| A medium-to-large patch (50-1000 LOC) | [targeted-review](#targeted-review) |
| A large patch or feature branch (1000+ LOC) | [mega-review](#mega-review)  |
| A bug report to reproduce        | [write-reproducer](#write-reproducer)   |
| Code you don't understand yet    | [patch-explainer](#patch-explainer)     |
| A protocol or algorithm to verify| [tla-plus](#tla-plus)                   |
| A repo and no idea where to look | [heatmap](#heatmap)                     |
| A repo's bug history to learn from | [bug-archaeology](#bug-archaeology)   |
| A Cassandra cluster test to write| [cassandra-injvm-dtest](#cassandra-injvm-dtest) |

## Skills

### shallow-review

Quick, broad bug scan. Six specialist agents review the same patch in parallel, each through a different lens: logic & types, boundaries & I/O, concurrency & state, resources & serialization, absence analysis, and API completeness. Findings are merged and deduplicated. Good as a first pass — it's fast and catches surface-level issues across a wide area.

→ `shallow-review/`

### deep-review

Focused, thorough review of specific files using the full 444-pattern catalog. Starts with a heatmap pass to identify the highest-churn files and lines, then concentrates review effort there — reading source (not just diffs), searching the codebase for context, and cross-referencing against the complete pattern database. Use it when shallow-review flags something worth digging into, or when reviewing critical-path code changes.

→ `deep-review/`

### targeted-review

Findings-driven review for medium-to-large patches (50–1000 LOC). Runs patch-explainer and codebase-analysis in parallel to ground the review, then makes 3–5 independent passes over the bug-pattern catalog to pick only the categories whose diff signals match. Items selected across multiple passes are promoted; the result is grouped by review focus and dispatched to parallel subagents — each with a tight, evidence-selected checklist rather than a fixed set of lenses. Higher signal-to-noise than shallow-review on complex changes, less exhaustive than deep-review on large ones.

→ `targeted-review/`

### mega-review

Multi-pass deep review for large patches (1000+ LOC) or feature branches. Decomposes the patch into HIGH/MEDIUM/LOW-risk files and commits, then runs deep-review per file, targeted-review across the high-risk scope, and shallow-review per commit — all in parallel. A cross-cut phase follows to amplify patterns found across files, verify fix correctness, and check cross-subsystem consistency. A coverage gate ensures every file was reviewed before findings are merged. Use when a single-pass review would spread attention too thin.

→ `mega-review/`

### write-reproducer

Turns a bug description into a minimal, self-contained, runnable reproduction. Covers the full workflow: failure characterization, scope selection, writing the repro, verifying it fails for the right reason, and minimizing to the smallest possible trigger.

→ `write-reproducer/`

### patch-explainer

Deep code analysis with ASCII visualizations. Produces diagrams showing structure, data/control flow, state transitions, before/after comparisons, concurrency interactions, assumptions, and failure modes. Use it to build understanding before reviewing, or to explain a change to someone else.

→ `patch-explainer/`

### tla-plus

Create, run, and verify TLA+ and PlusCal formal specifications. Model distributed systems, protocols, concurrent algorithms, and state machines. Can compose specs from code, find divergences between spec and implementation, and surface race conditions and invariant violations through exhaustive model checking.

> **Note:** This skill requires `tla2tools.jar` to run the TLC model checker. Place it in `tla-plus/lib/` before use.

→ `tla-plus/`

### heatmap

Git heatmap analysis that identifies high-churn files and lines — the places where bugs statistically concentrate. Use it to decide where to focus review effort in a large codebase, during bug hunts, security audits, or when onboarding onto unfamiliar code.

→ `heatmap/`

### bug-archaeology

Mines bug patterns from a repository's commit history. Discovers bug-fix commits via git log heuristics, analyzes each with subagents, and synthesizes a generalized `PATTERNS.md` with repo-specific details stripped. Use it to learn what kinds of bugs a codebase tends to produce — then feed those patterns into reviews.

→ `bug-archaeology/`

### cassandra-injvm-dtest

Guide for writing Apache Cassandra in-JVM distributed tests. Covers cluster creation, configuration, instance lifecycle, query execution, message filtering for fault injection, and debugging classloader isolation issues. Domain-specific, but included here since correctness testing in distributed databases is its own discipline.

→ `cassandra-injvm-dtest/`

## Typical Workflows

**Reviewing a patch for correctness:**
heatmap → patch-explainer → shallow-review → deep-review (on hot files)

**Reviewing a medium-to-large patch:**
targeted-review → deep-review (on files flagged HIGH)

**Reviewing a large patch or feature branch:**
mega-review (orchestrates targeted-review, shallow-review, and deep-review automatically)

**Investigating a bug report:**
patch-explainer (understand the area) → write-reproducer → shallow-review (on the fix)

**Verifying a protocol change:**
patch-explainer → tla-plus (model the protocol) → deep-review (on the implementation)

**Learning a new codebase's failure modes:**
bug-archaeology → heatmap → deep-review (on the overlap between hot code and historical bug patterns)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE.txt](../../LICENSE.txt) for details.
