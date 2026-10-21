# Absence Analysis — Specialist Checklist

Core domain: purely absent code — missing guards, cleanup, handlers, conditions that leave
no visible signal in the diff. Two-phase agent: build a search list, then execute searches.

---

## Phase 1: Build Search List

For each event in the diff, add an item to your search list:

### Registration & Lifecycle
- (a) New `addListener` / `register` / `subscribe` / `addMetric` call → Search for matching remove/deregister
- (b) New field added to a class → Verify presence in serialize/deserialize/equals/toString

### Event & Handler Coverage
- (c) New enum constant or event type → Search for ALL switch/dispatch/if-else chains; verify new constant handled
- (d) New `onTimeout()` / `onError()` override with short body → Search sibling classes for expected follow-up actions

### Resource Safety
- (e) New `AutoCloseable` created without try-with-resources → Identify exception exits before close
- (f) New object created in loop body (Event/Task/Request) → Check if enclosing class tracks in-flight instances

### Parallel Path Coverage
- (g) Method or code block added to path A with clear parallel path B → Verify path B has corresponding addition
- (h) New private/package-private field or method → Search for subclasses or cross-package classes needing access

## Phase 1b: High-Signal Absence Patterns

- (i) **Missing `return`**: non-void method called without return on its own line
- (j) **Missing try-with-resources**: AutoCloseable not wrapped, especially when parallel path in same file DOES wrap
- (k) **Missing interface override**: new class implements interface but doesn't override predicate whose default is wrong
- (l) **Missing class loading**: self-registering class not referenced from startup → registrations never execute
- (m) **Missing shutdown mechanism**: background scheduled task with no disable/stop path
- (n) **Missing empty return**: disabled path returns non-empty collection instead of empty
- (o) **Missing buffer unwrap**: pool buffer returned as slice/view instead of original allocation
- (p) **Missing wrapper usage**: reads/writes bypass tee/counting/checked wrapper stream
- (q) **Missing scope check**: code moved to new scope where variables no longer guaranteed non-null
- (r) **Missing config propagation**: value parsed or accepted by an outer layer but never threaded into the builder / constructor / factory that actually applies it — the setting silently falls back to its default
- (s) **Missing rollback on failure**: counter/collection populated on success path with no symmetric decrement/clear in the cancel, reject, throw, or timeout paths
- (t) **Missing retained reference**: object passed into a registry inside a factory/builder without being stored on `this` → the shutdown path has nothing to deregister
- (u) **Missing idempotency guard**: init/close/register can fire twice (schema reload, listener callback, reset path) without a has-run / already-closed flag
- (v) **Missing version/runtime fallback**: new wire field, protocol feature, or reflective JVM access has no branch for older peers / older JDKs / missing native library
- (w) **Missing drain on early exit**: checksum suffix, record footer, or remaining-items not consumed when an error / limit / break returns early from a framed stream
- (x) **Missing metric on sibling branch**: metric-record or state-propagation call present on one if/switch branch but absent from the symmetric branch or the cache-hit fast path
- (y) **Missing directory/fsync before dependent step**: deletion, listing, or cross-file rename proceeds without first fsyncing the directory or flushing sibling buffered writers

---

## Phase 2: Execute Searches

For each item in your search list:
- Use Grep and Read tools to gather evidence from the codebase
- If match found (remove/deregister exists): discard
- If no match found: report as finding with confidence level

---

## Fix-Patch Mode

When reviewing a **bug-fix** (not a feature), flip the question:

> "What was the original code missing — and did the fix add it EVERYWHERE?"

| What the fix added | Where else is it missing? |
|---|---|
| Null/bounds check on one path | Same lookup on parallel path (fast/slow, NIO/fallback) |
| `future.completeExceptionally(e)` | Other catch blocks or early-return paths |
| Feature-flag guard before operation | Setup, teardown, secondary operations |
| Version guard on new field | Matching `serializedSize()` and `deserialize()` branches |
| `close()`/`release()` on error path | Success path (also check for double-close) |

---

## What's NOT in the Patch

### Missing Symmetric Updates
- Every `serialize()` change needs matching `deserialize()` and `serializedSize()`
- New enum constant must appear in every switch/dispatch
- New field must appear in equals/hashCode/toString/clone/compareTo
- Renamed method must have zero references to old name

### Missing Callers
- Changed method signature: do all callers pass new parameter?
- Parameter parsed or validated but never forwarded: trace every hop from parse → store → builder → constructor → downstream factory
- New required field: do all constructors include it?
- New abstract method: do all subclasses implement it?

### Missing Guards
- Feature-flag check at every site touching guarded subsystem
- Mode guard handles all operating modes, not just default
- New entity property consulted by every mutation/deletion path

### Missing Error Handling
- Early return in checksummed read: still consume trailing checksum bytes?
- Every `catch` block mirrors the full exception wrapping chain?
- Error path that skips content: still advance position-tracking state?
- Rollback / counter decrement / cache eviction: symmetric on every failure path (cancel, reject, throw, timeout)?
