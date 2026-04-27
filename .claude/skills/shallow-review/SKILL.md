---
name: shallow-review
description: >
  Shallow (quick) ensemble bug-finding review using 6 specialist agents in parallel. Each
  specialist reviews the same patch through a different lens: Logic & Types, Boundaries &
  I/O, Concurrency & State, Resources & Serialization, Absence Analysis, and API
  Completeness. Findings are merged and deduplicated. Best for: quick first-pass review of
  patches, triage of diffs, broad surface-level bug scan. For deeper file-focused review
  with full pattern catalogs and codebase investigation, use the deep-review skill instead.
---

# Ensemble Code Review

Six specialist agents review the same patch in parallel. Each applies Phase 0 (understand
the change, form hypotheses) then a focused checklist. Their findings are merged and
deduplicated.

```
                    +------------------+
                    |   Input Patch    |
                    +--------+---------+
                             |
    +-------+------+---------+---------+-----------+
    |       |      |         |         |           |
+---v--+ +--v--+ +-v-----+ +-v------+ +v-----+ +---v-------+
|Logic | |Bound| |Concurr| |Resource| |Absenc| | Complete. |
|&Type | | &I/O| |&State | |& Serde | |(2-ph)| | & Contr.  |
+--+---+ +--+--+ +---+---+ +---+----+ +--+---+ +---+-------+
   |        |        |         |          |        |
   +--------+--------+---------+----------+--------+
                               |
                    +----------v----------+
                    |  Pass 3: Symmetry   |
                    |  (serial, optional) |
                    +----------+----------+
                               |
                    +----------v----------+
                    |   Merge & Dedup     |
                    |   (main agent)      |
                    +---------------------+
```

## Quick Start

1. **Identify target** — patch, diff, file, or subsystem
2. **Launch 6 specialists** in parallel (see prompts below)
3. **Run Pass 3 Symmetry** check (serial, after specialists complete)
4. **Merge** findings: deduplicate, rank by confidence
5. **Report** in unified format

## Phase 0: Understand Before Checking

Every specialist runs Phase 0 BEFORE touching the checklist:

1. **Summarize** the patch in 2–3 sentences (feature, fix, refactor, move?)
2. **Hypothesize** — list 3–5 things that could go wrong with this type of change
3. **Note** code shapes that warrant deeper investigation

This switches the review from bottom-up (checklist→code) to top-down
(code→hypotheses→verification). The checklist then verifies and extends the hypotheses.

---

## The Six Specialists

### 1. Logic & Types (52% of bugs)

**Priority**: wrong comparisons, missing returns, wrong constants, boolean polarity, sentinel ambiguity.

**Reference**: `references/general/specialists/logic.md`

**Prompt**:
```
You are a LOGIC specialist reviewing a patch for correctness bugs.

PHASE 0 — Understand the patch first (before reading checklist):
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that could go wrong with this type of change.
3. Note any suspicious code shapes (control flow, comparisons, constants, sentinels).

PHASE 1 — Apply checklist:
Read your checklist: `{SKILL_DIR}/references/general/specialists/logic.md`
Apply questions that match your hypotheses and code shapes. Check all 20 items
against the patch but spend the most effort where your hypotheses pointed.

Report ALL findings above a noise floor. For each: Location, Confidence
(High/Medium/Low), What's wrong (1-2 sentences).
Or "No finding in my domain" if nothing applies.
```

### 2. Boundaries & I/O (17% of bugs)

**Priority**: off-by-one, integer overflow, null/bounds checks, buffer sizing, I/O completeness.

**Reference**: `references/general/specialists/boundary.md`

**Prompt**:
```
You are a BOUNDARY specialist reviewing a patch for correctness bugs.

PHASE 0 — Understand the patch first (before reading checklist):
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that could go wrong with this type of change.
3. Note any arithmetic, indexing, range, or I/O operations.

PHASE 1 — Apply checklist:
Read your checklist: `{SKILL_DIR}/references/general/specialists/boundary.md`
Apply questions that match your hypotheses and code shapes. Check all 15 items
against the patch but spend the most effort where your hypotheses pointed.

Report ALL findings above a noise floor. For each: Location, Confidence
(High/Medium/Low), What's wrong (1-2 sentences).
Or "No finding in my domain" if nothing applies.
```

### 3. Concurrency & State (16% of bugs)

**Priority**: races, TOCTOU, live-view iteration, lock ordering, state cleanup.

**Reference**: `references/general/specialists/concurrency.md`

**Prompt**:
```
You are a CONCURRENCY specialist reviewing a patch for correctness bugs.

PHASE 0 — Understand the patch first (before reading checklist):
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that could go wrong with this type of change.
3. Note any shared state, locks, collections, lifecycle operations.

PHASE 1 — Apply checklist:
Read your checklist: `{SKILL_DIR}/references/general/specialists/concurrency.md`
Apply questions that match your hypotheses and code shapes. Check all 16 items
against the patch but spend the most effort where your hypotheses pointed.

Report ALL findings above a noise floor. For each: Location, Confidence
(High/Medium/Low), What's wrong (1-2 sentences).
Or "No finding in my domain" if nothing applies.
```

### 4. Resources & Serialization (15% of bugs)

**Priority**: resource leaks, serialization mismatches, wrong metric types, background tasks, wrapper bypass.

**Reference**: `references/general/specialists/resources.md`

**Prompt**:
```
You are a RESOURCES specialist reviewing a patch for correctness bugs.

PHASE 0 — Understand the patch first (before reading checklist):
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that could go wrong with this type of change.
3. Note any resource allocation, serialization methods, metrics, streams.

PHASE 1 — Apply checklist:
Read your checklist: `{SKILL_DIR}/references/general/specialists/resources.md`
Apply questions that match your hypotheses and code shapes. Check all 18 items
against the patch but spend the most effort where your hypotheses pointed.

Report ALL findings above a noise floor. For each: Location, Confidence
(High/Medium/Low), What's wrong (1-2 sentences).
Or "No finding in my domain" if nothing applies.
```

### 5. Absence Analysis (cross-cutting)

**Priority**: find what SHOULD be present but ISN'T — missing guards, cleanup, handlers, registrations.

**Reference**: `references/general/specialists/absence.md`

**Prompt**:
```
You are an ABSENCE specialist. Your job: find bugs that are purely absent — missing
guards, cleanup, handlers, or conditions that leave no signal in the diff.

PHASE 0 — Understand the patch first:
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that SHOULD be present but might be missing.

PHASE 1 — Build the search list:
Read your checklist: `{SKILL_DIR}/references/general/specialists/absence.md`
For each event in the diff matching items (a)-(q), add to your search list.

PHASE 2 — Execute searches and report:
For each item in your search list:
  - Use Grep and Read tools to gather evidence from the codebase
  - If match found (remove/deregister exists): discard
  - If no match found: report as finding

Report ALL findings above a noise floor. For each: what's missing, where it should
be, what evidence you searched for, Confidence (High/Medium/Low).
Or "No absence finding" if nothing applies.
```

### 6. API Completeness & Contracts

**Priority**: missing fields, missing overrides, registration symmetry, visibility, event coverage.

**Reference**: `references/general/specialists/completeness.md`

**Prompt**:
```
You are an API COMPLETENESS specialist reviewing a patch for correctness bugs.

PHASE 0 — Understand the patch first (before reading checklist):
Read the patch: `{PATCH_PATH}`
1. Summarize what this patch does in 2-3 sentences.
2. List 3-5 things that could be incomplete about this change.
3. Note any new classes, new fields, new registrations, visibility changes.

PHASE 1 — Apply checklist:
Read your checklist: `{SKILL_DIR}/references/general/specialists/completeness.md`
Apply questions that match your hypotheses and code shapes. Check all 15 items
against the patch but spend the most effort where your hypotheses pointed.

Report ALL findings above a noise floor. For each: Location, Confidence
(High/Medium/Low), What's wrong (1-2 sentences).
Or "No finding in my domain" if nothing applies.
```

---

## Pass 3: Cross-Path Symmetry (serial, after specialists complete)

**Prompt**:
```
You are a SYMMETRY specialist. For each code path modified in this patch, identify
whether a structurally parallel path (same class, interface, event family, version range)
was NOT modified. For each asymmetry:
- Where path A was changed
- Where parallel path B exists
- What was changed on A that is absent on B
- Whether the asymmetry is intentional or suspicious

Read the patch: `{PATCH_PATH}`
Search the codebase as needed using Grep and Read.
Report all asymmetries, Confidence High/Medium/Low.
```

---

## Merge & Dedup

After all specialists report:

1. **Collect** all findings from 6 specialists + symmetry
2. **Deduplicate** — same code location + related reasons → combine
3. **Rank** by confidence — findings confirmed by multiple specialists rank highest
4. **Cross-check** reinforcement:
   - Logic + Absence at same location → boost to High
   - Concurrency + Resources on same lifecycle → boost to High
   - Any finding flagged by 3+ specialists → treat as confirmed
5. **3-point test** each finding:
   - The code construct actually exists in the diff (not inferred)
   - The bug is possible given the visible context (not speculative)
   - The finding is actionable (what specifically should change)
6. **Specialist Silence Rule**: If Absence or Completeness reports nothing on a >100-line
   diff, note: "Consider verifying Phase 2 searches executed for registration symmetry,
   handler coverage, and field completeness."

---

## Report Format

```
## Review: [target]

### Findings (ranked by confidence)

#### Finding 1: [title]
- **Location**: [file:line]
- **Confidence**: High / Medium / Low
- **Flagged by**: [specialists]
- **What's wrong**: [1-2 sentences]

### Specialist Coverage
- Logic: [N findings / no finding]
- Boundary: [N findings / no finding]
- Concurrency: [N findings / no finding]
- Resources: [N findings / no finding]
- Absence: [N findings / no finding]
- Completeness: [N findings / no finding]
- Symmetry: [N asymmetries / none]
```

## Statistical Priors

| Category | Share | Specialist |
|---|---|---|
| Logic error in condition | 26% | Logic |
| Wrong constant / default | 15% | Logic |
| Missing null / bounds check | 13% | Boundary |
| Incorrect filtering / result | 11% | Logic |
| Race condition | 9% | Concurrency |
| Wrong serialization | 8% | Resources |
| State not cleaned up | 7% | Concurrency |
| Off-by-one | 4% | Boundary |
| Resource leak | 3% | Resources |

## Reference Files

### Specialist checklists (trimmed, ensemble mode)
- `references/general/specialists/logic.md` — 20 items, Logic specialist
- `references/general/specialists/boundary.md` — 15 items, Boundary specialist
- `references/general/specialists/concurrency.md` — 16 items, Concurrency specialist
- `references/general/specialists/resources.md` — 18 items, Resources specialist
- `references/general/specialists/absence.md` — Phase 1/2 search patterns, Absence specialist
- `references/general/specialists/completeness.md` — 15 items, Completeness specialist
