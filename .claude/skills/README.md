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

For writing repros, you will often times have to guide the model and suppress its attempts to go deep into internals to conjure up a repro that does exactly 
what it wants but does not reveal the real issue. Introduce guidelines and close the loop by setting strict and clear exit criteria. 

## Where Do I Start?

Pick your entry point based on what you're looking at right now:

| You have…                        | Start with                              |
|----------------------------------|-----------------------------------------|
| A patch or diff to review        | [shallow-review](#shallow-review), then [deep-review](#deep-review) for flagged areas |
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

**Investigating a bug report:**
patch-explainer (understand the area) → write-reproducer → shallow-review (on the fix)

**Verifying a protocol change:**
patch-explainer → tla-plus (model the protocol) → deep-review (on the implementation)

**Learning a new codebase's failure modes:**
bug-archaeology → heatmap → deep-review (on the overlap between hot code and historical bug patterns)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
