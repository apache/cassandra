# Deep Cross-Cutting Patterns — Extended Checklist

Patterns that span multiple specialist domains. These are the highest-value patterns for
deep review because they require understanding the full context of the changed files.

---

## Refactoring & Merge Artifacts

### Composite type decomposition (12 known bugs)
- [ ] Does column name comparison on CQL3 tables split composite components?
- [ ] Is `type.decompose(value)` called on container type instead of inner element sub-type?
- [ ] Is collection meta/marker subtype passed where concrete element type expected?
- [ ] Does `ColumnIdentifier` bytes survive rename correctly?

### Refactored signature not updated (6 known bugs)
- [ ] After method parameter/name/type change: do ALL callers (across all modules) use the new form?
- [ ] Are callers outside the default build target checked?

### JDK version incompatibility (6 known bugs)
- [ ] Are `--add-opens` declarations present for module access?
- [ ] Are reflective field names stable across JDK versions?
- [ ] Is a newer JDK API used in code targeting an older JDK?

### Data structure element access (4 known bugs)
- [ ] Does code handling collection sub-elements work for ALL cell-kind combinations?
- [ ] Are static vs regular cells handled differently in merge/grouping?

### Gossip/state-merge notification (2 known bugs)
- [ ] Is a state update gated on change check, or does it fire duplicate notifications?

### Parallel-array mutation (1 known bug)
- [ ] When cells, liveness, and deletions are in parallel arrays: does mutation touch ALL arrays?

---

## Grammar & Parsing

### Grammar quantifier (3 known bugs)
- [ ] Is `+` (one-or-more) used where `*` (zero-or-more) is needed?
- [ ] Does the grammar reach the `QUOTED_NAME` alternative?

### Boolean parser (2 known bugs)
- [ ] Does the parser only check positive branch, treating everything else as false?
- [ ] Is garbage input silently accepted?

### Raw string split (1 known bug)
- [ ] Does split on `:` break for IPv6 addresses?

### Lenient date parser (1 known bug)
- [ ] Does the parser silently accept month 13 or day 32?

---

## Shell Scripts & Platform

### Version comparison (6 known bugs)
- [ ] Is version comparison lexicographic instead of numeric?
- [ ] Does it handle multi-digit components ("3.10" < "3.9")?
- [ ] Does it handle SNAPSHOT/pre-release labels?

### Locale/platform (6 known bugs)
- [ ] Does code assume English locale output?
- [ ] Does it assume POSIX file-sharing semantics?
- [ ] Does it assume Python 2 internal attributes?

### CQL string rendering (4 known bugs)
- [ ] Are identifiers properly quoted with `maybeQuote`?
- [ ] Are reserved words handled?
- [ ] Is `toCQLString` used (not `getString`)?

### Library contract change (3 known bugs)
- [ ] Has a dependency version change silently changed API behavior?
- [ ] Does a JAR version mismatch exist between classpath and packaging?

### Regex metacharacter (2 known bugs)
- [ ] Is `.` unescaped in `String.split()` or `grep`?
- [ ] Is `|`, `*`, `+`, `^` literal but used as regex?

---

## Missing Method Overrides

### Subclass overwrites parent field (2 known bugs)
- [ ] Does a subclass constructor re-initialize a parent-set field back to default?
- [ ] Does the parent's computed value get lost?

### Copy method missing (1 known bug)
- [ ] When a subclass adds fields: does it override `sharedCopy()`/`clone()`?
- [ ] Does the copy silently lose subclass state?

### Self-referential equals (1 known bug)
- [ ] Does `equals()` delegate to `Objects.equal(this, x)` creating identity comparison?

---

## Query Result & Index Patterns

### Secondary index reconciliation (17 known bugs)
- [ ] Does index entry creation check whether the column value wins timestamp reconciliation?
- [ ] Is queryability marking deferred, creating a window where queries fail?

### CLI format strings (3 known bugs)
- [ ] Do hardcoded fixed-width format strings truncate long values?
- [ ] Should column widths be computed dynamically?

### Column access by position (3 known bugs)
- [ ] Does result set access use position instead of column name?
- [ ] Could server-side column reordering break access?

### Versioned schema defaults (3 known bugs)
- [ ] When deserializing from older node: are nullable fields given safe defaults?
- [ ] Are defaults substituted before passing to non-null constructors?

### Token range convention (2 known bugs)
- [ ] Does `Range.contains()` use correct left-exclusive vs left-inclusive convention?

### JSON pagination (2 known bugs)
- [ ] Are comma separators present at page boundaries?
- [ ] Is the output valid JSON across pages?

### CLI option key mismatch (2 known bugs)
- [ ] Does `getOptionValue("t")` match `hasOption("T")`?

---

## Crash Safety & Atomic Writes

### Files.copy idempotency (1 known bug)
- [ ] Does `Files.copy()` use `REPLACE_EXISTING` if it can run twice?

### WRITE|CREATE truncation (1 known bug)
- [ ] Does `WRITE | CREATE` on existing file use `TRUNCATE_EXISTING`?

---

## Thread Pool & Dispatch

### Wrong executor stage (5 known bugs)
- [ ] Is a task dispatched to the correct executor for its thread-confinement contract?

### Counter scope (4 known bugs)
- [ ] Is a counter increment inside or outside the conditional brace?

### JMX during startup (2 known bugs)
- [ ] Are management interfaces registered early with status reporting?
- [ ] Is the node invisible to operators during bootstrap?

### Multiple constructors (2 known bugs)
- [ ] Do all constructors perform required tracking/registration?
- [ ] Or does only the canonical one register?

---

## Missing from §10: Shell Scripts & Platform — Specific Patterns

### Mutable Cached / Derived Value Diverges from Authoritative Source (7 known bugs)
A JMX-settable property, shadow static field, or cached derived value is updated independently from the authoritative source (DatabaseDescriptor, Config, or the schema registry), causing the two to silently diverge after any write to either side.

### CLI option parser treats boolean as flag instead of value (or vice versa) (2 known bugs)
JCommander treats `Boolean` as a flag (presence = true), or PowerShell argument forwarding silently drops prefixes during re-prefixing.

### Identifier quoting applied inconsistently (2 known bugs)
User-supplied keyspace/table name not double-quoted when interpolated into CQL, or already-quoted identifier double-quoted again.

### Shell argument forwarding splits on spaces (1 known bug)
Bare $@ without double-quoting splits arguments containing spaces in partition key values.

### Regex pre-processor does not respect quoting/escaping context (1 known bug)
A regex-based pre-processor that operates without awareness of string-literal quoting silently corrupts valid user data.

---

## Missing from §11: Refactoring — Specific Patterns

### Refactored method signature not updated at all call sites (6 known bugs)
A method's parameters, name, or type changes but some callers still pass the old arguments, binding to a different overload or failing to compile in modules outside the default build target.

### Gossip / state-merge fires notification without version comparison (2 known bugs)
A gossip merge loop applies state and fires subscriber notifications without checking that the incoming version is strictly greater, causing duplicate notifications.

### Merge result drops original input from aggregation (1 known bug)
A method that collects "extra" mutations from triggers and merges them omits the original input from the merge.

### Wrapper type introduced but raw comparisons remain (1 known bug)
After wrapping a primitive type in a richer wrapper, leftover code still uses the raw type for comparisons.

### Map.put in aggregation loop silently drops prior values (1 known bug)
Vnode token ownership aggregation uses `put` instead of merge, dropping all but last contribution.

### Refactored guard condition moved to wrong scope (1 known bug)
Moving a guard during refactoring places it in a utility method that fires for all callers, not just the intended one.

---

## Query Result and Index Patterns (§29)

### Secondary index entry written without reconciliation check (17 known bugs)
Index code creates entries without checking whether the column value wins timestamp reconciliation. Also: deferring queryability marking creates a window where queries fail on an apparently-UP node.

### Memtable not flushed before compaction, truncation, or cleanup (4 known bugs)
A durable operation depends on data being on disk, but no explicit flush is issued first.

### CLI fixed-width format strings break on long field values (3 known bugs)
Hardcoded fixed-width format strings in tools truncate or misalign output for long values.

### Column access by position instead of by name in result set iteration (3 known bugs)
Code accesses CQL result columns by position or `values()` iteration. Server-side column reordering silently breaks access.

### Versioned schema deserialization missing defaults for absent optional fields (3 known bugs)
When deserializing from an older node that omits optional fields, nullable fields are not replaced with safe defaults before passing to non-null constructors.

### Token range boundary convention mismatch (left-exclusive vs left-inclusive) (2 known bugs)
Range.contains() uses the wrong convention for the lower bound.

### Validation rule correct for user input but wrong for internal reconstruction (2 known bugs)
A validation rule rejects valid internally-stored schema data during reconstruction.

### JSON pagination missing separator between consecutive pages (2 known bugs)
Paginated JSON output omits comma separators at page boundaries, producing invalid JSON.

### CLI option parser getOptionValue uses wrong option key (2 known bugs)
`getOptionValue("t")` inside a block guarded by `hasOption("T")` reads the wrong option.

### Token ownership computation negative for wrapping ranges (2 known bugs)
Token ownership percentage lacks modular arithmetic for wrapping ranges, producing negative percentages.

### Reflection invoke() passes null instead of empty array for no-arg method (2 known bugs)
`method.invoke(null, (Object) null)` passes null as a single argument instead of an empty array.

### Tombstone markedForDeleteAt updated without matching localDeletionTime (2 known bugs)
When merging tombstone metadata, the logical deletion timestamp is updated but the GC clock is not, so the tombstone is never garbage-collected.

---

## Missing from §22: Filtering — Patterns in Cross-Cutting

### Shell script argument quoting / word-splitting vulnerability (3 known bugs)
A shell script stores arguments in a single unquoted variable, uses an unfiltered glob, or embeds multi-line command substitution into a path variable.

### Filesystem path or string identity mismatch (5 known bugs)
`String.startsWith()` used for path containment causing prefix false positives, `Set.contains(filename)` misses UUID-suffixed directories, `File` equality fails between live and snapshot directory prefixes.

### Off-heap class missing override of size/length query (1 known bug)
When a class stores data off-heap and overrides data accessors but not size-query methods, the base class falls back to heap-materializing data on the hot path.
