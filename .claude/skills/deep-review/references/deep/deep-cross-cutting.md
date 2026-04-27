# Deep Cross-Cutting Patterns — Extended Checklist
---

## Refactoring & Merge Artifacts

### Refactored signature not updated (weight: high)
-  After method parameter/name/type change: do ALL callers (across all modules) use the new form?
-  Are callers outside the default build target checked?

### JDK version incompatibility (weight: high)
-  Are `--add-opens` declarations present for module access?
-  Are reflective field names stable across JDK versions?
-  Is a newer JDK API used in code targeting an older JDK?

### Merge result drops original input from aggregation (weight: low)
A method that collects "extra" entries from triggers or plugins and merges them omits the original input from the result, silently discarding it.

### Wrapper type introduced but raw comparisons remain (weight: low)
After wrapping a primitive type in a richer wrapper, leftover code still uses the raw type for comparisons.

### Map.put in aggregation loop silently drops prior values (weight: low)
Aggregation code uses `put` instead of merge/compute, dropping all but the last contribution from each key.

### Refactored guard condition moved to wrong scope (weight: low)
Moving a guard during refactoring places it in a utility method that fires for all callers, not just the intended one.

---

## Shell Scripts & Platform

### Version comparison (weight: high)
-  Is version comparison lexicographic instead of numeric?
-  Does it handle SNAPSHOT/pre-release labels?

### Locale/platform (weight: high)
-  Does code assume English locale output?
-  Does it assume POSIX file-sharing semantics?

### Library contract change (weight: medium)
-  Has a dependency version change silently changed API behavior?
-  Does a JAR version mismatch exist between classpath and packaging?

### Regex metacharacter (weight: low)
-  Is `.` unescaped in `String.split()` or `grep`?
-  Is `|`, `*`, `+`, `^` literal but used as regex?

### Mutable cached / derived value diverges from authoritative source (weight: high)
A shadow static field or cached derived value is updated independently from the authoritative source, causing the two to silently diverge after any write to either side.

### Shell argument forwarding splits on spaces (weight: low)
Bare `$@` without double-quoting splits arguments containing spaces.

### Regex pre-processor does not respect quoting/escaping context (weight: low)
A regex-based pre-processor that operates without awareness of string-literal quoting silently corrupts valid user data.

---

## Missing Method Overrides

### Subclass overwrites parent field (weight: low)
-  Does a subclass constructor re-initialize a parent-set field back to default?
-  Does the parent's computed value get lost?

### Copy method missing (weight: low)
-  When a subclass adds fields: does it override `sharedCopy()`/`clone()`?
-  Does the copy silently lose subclass state?

### Self-referential equals (weight: low)
-  Does `equals()` delegate to `Objects.equal(this, x)` creating identity comparison?

---

## Crash Safety & Atomic Writes

### Files.copy idempotency (weight: low)
-  Does `Files.copy()` use `REPLACE_EXISTING` if it can run twice?

### Write without truncation (weight: low)
-  Does opening a file for write without `TRUNCATE_EXISTING` leave stale bytes beyond the new content?

---

## Output Formatting & CLI

### CLI fixed-width format strings break on long field values (weight: medium)
Hardcoded fixed-width format strings in tools truncate or misalign output for long values.

### Column access by position instead of by name in result set iteration (weight: medium)
Code accesses result columns by position or `values()` iteration. Server-side column reordering silently breaks access.

### Boolean parser treats absence as false without validating value (weight: low)
-  Does the parser only check the positive branch, treating everything else as false?
-  Is garbage input silently accepted?

---

## Versioning & Compatibility

### Protocol-version field sent unconditionally (weight: medium)
-  Does a serializer write a field unconditionally even when the negotiated protocol version does not contain it? Recipients misparse subsequent fields.
-  Does a method assert the messaging version equals the local current version, throwing in any mixed-version cluster?

### Versioned deserialization missing defaults for absent optional fields (weight: medium)
When deserializing from an older node that omits optional fields, nullable fields are not replaced with safe defaults before passing to non-null constructors.

### Validation rule correct for user input but wrong for internal reconstruction (weight: low)
A validation rule rejects valid internally-stored data during reconstruction.

### Reflection invoke() passes null instead of empty array for no-arg method (weight: low)
`method.invoke(null, (Object) null)` passes null as a single argument instead of an empty array.

---

## Filesystem Path & Identity

### Shell script argument quoting / word-splitting vulnerability (weight: medium)
A shell script stores arguments in a single unquoted variable, uses an unfiltered glob, or embeds multi-line command substitution into a path variable.

### Filesystem path or string identity mismatch (weight: medium)
`String.startsWith()` used for path containment causing prefix false positives, or a set lookup misses entries with suffixes (UUIDs, extensions) appended to the stored key.

### Off-heap class missing override of size/length query (weight: low)
When a class stores data off-heap and overrides data accessors but not size-query methods, the base class falls back to heap-materializing data on the hot path.

---

## Additional Patterns from Multi-Project Corpus

### Serializable class field / transient without custom readObject (weight: low)
-  Does a `Serializable` class hold a field typed as a non-serializable third-party container?
-  Is a field marked `transient` without a `readObject` method to reconstruct it? After deserialization the field is null and crashes on first access.

### Wrapper / delegate drops auxiliary argument (weight: medium)
-  When a wrapper serializer / deserializer / client forwards to an inner instance, does it propagate every argument — tracing context, consistency level, auth credentials, client options?
-  Does the wrapper fall back to a subset overload, silently dropping the auxiliary parameter?
-  For sibling clients / serializers: is the propagation step applied to each, or only the primary?

### Duplicate-code versioned directories drift out of sync (weight: low)
-  When a bug-fix is applied to one versioned copy of a subsystem (format-variant, protocol-version-gated implementation, legacy vs current): is it propagated to every sibling copy?
-  Grep for the fixed function name across peer directories (legacy/, current/, format variants) to find stale copies.

### Lookup key transformed asymmetrically between insert and get (weight: medium)
-  Is the map populated with a post-transform (normalized / lowercased / resolved) key while the lookup uses the pre-transform key (or vice versa)?
-  For case-insensitive lookups: do both `put` and `get` normalize case? For address translation: is the same mapping applied before insert and before lookup?

### Dependency-upgrade API incompatibility or silent deprecation (weight: medium)
-  After a dependency version bump: do fluent / chained API calls exist on the host's runtime version?
-  Do configuration methods called on the upgraded client still apply, or have they silently become no-ops returning stub values?
-  Does a plugin-version regression change published artifact classifiers?

### serializedSize counts length prefix only, omitting field bytes (weight: low)
-  Does `serializedSize` for a variable-length field count only the length prefix without adding the actual byte count?
-  Compare serializedSize field-by-field against serialize: does each field contribute X in serialize but Y in size?

### Guard / validation present at high-level entry but bypassed by lower-level factory path (weight: medium)
-  If validation exists on the public API entry point: is every lower-level factory / internal constructor also gated, or can callers reach the state-mutation via a back-door that skips validation?
-  Does one alternative creation path (from-bytes vs from-memory; primary vs copy-of) apply a required post-setup step the other omits?

### New error code / exception class unmapped in error-handling table (weight: medium)
-  When a new error code or exception type is introduced: is the error-handling mapping table updated (separately from the dispatch switch)?
-  Does an exception class incorrectly inherit a retriable parent, causing infinite retry loops on non-recoverable cases?
-  Does treating a transient-state error code as success silently skip retry instead of triggering state rediscovery?

### Fat-artifact missing class / optional dependency NoClassDefFoundError (weight: medium)
-  When a feature depends on a class that is compile-time-present but may be omitted from the deployed fat artifact: is there a runtime fallback or explicit classpath declaration?
-  Is an optional module / codec excluded from the packaged assembly but referenced from the startup path?

### Stub returning null instead of delegating to backing field (weight: medium)
-  Does a DTO / interface-impl stub return `null` / `0` / `false` from a method that should delegate to a field populated by the builder?
-  Does a predicate in an optimizing variant of a paired evaluator unconditionally return the conservative result, silently skipping the optimization for every input?
