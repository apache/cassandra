# Category: Validation and Input Handling

Bugs where input validators, parsers, format detectors, and pre-conversion guards either miss cases, anchor on the wrong delimiter, fail to account for downstream length/charset/OS limits, or skip pre-use checks (null, -1, empty buffer, length, type) before consuming a value.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- New or modified `validate*`, `check*`, `verify*`, `assert*`, `isValid*`, `parse*`, `tokenize*`, `decode*` methods
- Calls to `indexOf`, `lastIndexOf`, `String.split`, `Pattern.compile`, `Matcher`, `String.replace*`, `substring`, `startsWith`/`endsWith`, `getBytes`/`new String`
- Construction or modification of regex character classes, anchors, escapes, or quantifiers
- File path / URI / address parsing: `getFileName`, `Paths.get`, `URI`, `InetAddress`, `InetSocketAddress`, host:port splitting, IPv4/IPv6 literals
- Functions appending suffixes/prefixes (`-`, `_`, UUID, extension, port) before passing names to filesystem, network, or registry calls
- Code that consumes an `Optional`, nullable lookup, or map `get()` result without `.isPresent()`/`!= null`/`getOrDefault`
- Conditions calling `indexOf(...)` then `substring(...)` without a `-1` guard
- Pre-conversion / pre-deserialization filters that drop or pass through unsupported attribute types, sentinels, or empty values
- New constraint, length, charset, locale, or boundary checks; or removal of an existing one
- Conversion of user-supplied identifiers / option strings / config values to typed objects (enum, UUID, BigDecimal, Date)
- Length, capacity, or quantity bounds that interact with appended fixed-size data (suffixes, length prefixes, framing headers)

## Findings

### F-01: Validator passes but downstream limit (OS/filesystem/protocol) fails after suffix append
A name passes format validation but a fixed-length suffix is appended later (UUID, generation marker, extension), and the combined length exceeds an OS or protocol cap.
**Look for:** Name-validation methods checking length/format against a constant `MAX_NAME_LEN` while the same name is later concatenated with `"-" + uuid`, `.tmp`, or a numeric generation; verify the validator subtracts the suffix budget.

### F-02: indexOf followed by substring with no -1 guard
A delimiter is located with `indexOf`/`lastIndexOf` and the index is passed straight into `substring` without checking for `-1`, throwing `StringIndexOutOfBoundsException` on inputs that lack the delimiter.
**Look for:** `s.substring(s.indexOf(c) + 1)` or `s.substring(0, s.indexOf(c))` without a preceding `if (idx < 0)` branch.

### F-03: Pre-conversion validator omits unsupported-attribute check
A converter accepts an input with attributes the target format cannot represent (TTL, timestamp resolution, headers, multi-cell collection) and silently discards them rather than rejecting the conversion.
**Look for:** Conversion / migration / serialization functions whose validation step touches only the value's primary type and not its qualifying attributes; expect a missing branch like `if (src.hasFoo()) throw ...`.

### F-04: Address parsed by splitting on the wrong delimiter
Multi-segment addresses (IPv6, host:port with brackets, qualified namespace.table) are split on a delimiter that also appears inside one of the segments, retaining only the first part.
**Look for:** `addr.split(":")` applied to strings that may contain IPv6 literals, or `name.split(".")` applied without limit/escape; check whether IPv6 brackets, percent-encoding, or quoted segments are stripped first.

### F-05: Stripping a literal prefix with regex misinterprets metacharacters
A "strip prefix" or "strip suffix" routine uses `String.replaceFirst`/`replaceAll`/`split` directly on a literal prefix that may contain regex metacharacters (`.`, `+`, `?`, `[`, `(`, `\`).
**Look for:** `s.replaceAll(prefix, "")`, `s.split(sep)` where `prefix`/`sep` is a path, version, hostname, or platform separator; expect missing `Pattern.quote(...)` or `replace` (literal) usage.

### F-06: Filename prefix filter does not append a separator after the name
A prefix filter (`name.startsWith(prefix)`) matches longer names that share the same prefix but are different entities, opening the wrong files.
**Look for:** `name.startsWith(targetName)` or "list files for table X" filters that don't append `"-"`, `"."`, or another separator to the matched name.

### F-07: Path-prefix containment check uses raw string startsWith
A directory containment / ancestry check compares paths with `startsWith` without ensuring a path-separator boundary, so `/foo/barbaz` matches `/foo/bar`.
**Look for:** `path.startsWith(parent)` or `parent.isPrefixOf(child)` style logic without a final `/` (or platform separator) appended; also watch for `Path.startsWith` confusion with `String.startsWith`.

### F-08: Regex character class with mid-class hyphen creates an unintended range
A character class uses an unescaped `-` between two literal characters, silently producing an ASCII range that includes characters the author did not intend.
**Look for:** `[a-z\-_.+]` style classes where `-` appears between non-adjacent literals; verify hyphen is at the start, end, or escaped.

### F-09: split() on a regex metacharacter passed as literal
`String.split` is called with a single character (`.`, `|`, `+`, `*`, `(`) that the regex engine interprets specially, producing all-empty strings or no split at all.
**Look for:** `path.split(File.separator)` (where separator is `\` on Windows), `s.split(".")`, `s.split("|")` without `Pattern.quote`.

### F-10: Anchored regex matches only first occurrence, missing duplicates
A pattern uses `^` or `\A` (or implicit `matches()`) when `find()` was needed, so a config option only triggers a check on the first occurrence and a duplicate elsewhere is silently appended.
**Look for:** Detection of an existing JVM/CLI flag with `^-Dfoo` or `pattern.matcher(s).matches()` that should be `find()`.

### F-11: Format-time validation differs from runtime validation
A schema/DDL accepts a combination at definition time that runtime then rejects, or vice versa, because the two validators are written by different code paths and only one knows the new constraint.
**Look for:** Two validation entry points (`validateDdl` and `validateExecution`, or `validate` vs `validateForBuild`); one was updated for a new feature and the other was not.

### F-12: Boolean parser only matches positive form, silently returns false on typos
A property/boolean parser tests for "true"/"yes"/"1" with regex but returns `false` (instead of throwing) for any non-matching input, silently swallowing typos.
**Look for:** Parsing methods that pattern-match a positive set and have an unconditional `return false` fallthrough; expect operator typos like "treu" to pass validation.

### F-13: Compound declarative predicates accepted without satisfiability check
Predicates on the same field (constraints, range bounds, multiple `restrictions`) are validated individually but never cross-checked for satisfiability, allowing contradictory combinations to be stored and queried later with confusing results.
**Look for:** Per-restriction `validate()` calls in a loop with no follow-up "do these together make sense" pass; e.g. `x > 5 AND x < 3` accepted.

### F-14: User-supplied identifier passed to case-sensitive lookup without quoting
An identifier that contains uppercase, special characters, or reserved words is forwarded to a case-sensitive backend without being quoted, so writes go to the wrong key or lookups silently miss.
**Look for:** Calls to driver/JDBC/CQL/SQL APIs that accept an identifier; check whether identifiers needing quoting are wrapped with the API's quoting helper. Also watch for identifiers double-quoted by mistake.

### F-15: Empty buffer or empty array bypasses null guard but fails on first access
A guard checks for `null` but not for empty (`length == 0`, `remaining() == 0`, `isEmpty()`); the empty value passes the guard and the first index/array access throws or returns garbage.
**Look for:** `if (arr != null) ... arr[0]`, `if (buf != null) buf.getInt()`, `if (s != null) s.charAt(0)` without `!arr.isEmpty()`/`buf.hasRemaining()`/`!s.isEmpty()`.

### F-16: User-supplied collection passed to method that asserts non-empty
A public method documents an empty-collection-allowed contract but starts with an assertion or first-element access that rejects valid empty input.
**Look for:** Methods whose entry has `assert !c.isEmpty()` or `c.iterator().next()` while their callers may legitimately pass empty inputs (secondary index writes, no-op queries, single-replica scenarios).

### F-17: Validation rejects single-element grammar where one-or-more was intended
A grammar quantifier requires two-or-more comma-separated items, or a list rule rejects an empty list literal that should be valid.
**Look for:** `+` quantifier where `*` was intended (or `(item ',' item)+` patterns); empty-collection literals that fail to parse.

### F-18: Reserved word / reserved-character validation only on one entry path
Validation rejects reserved class/keyspace names on the user-facing CRUD path but a sibling internal/replay/migration path constructs the same entity without the check, allowing reserved-name rows to land in storage.
**Look for:** Validation guards in user-facing controllers absent from internal write, recovery, replay, migration, or factory call paths.

### F-19: Length validation uses one width while serializer uses another
A field is length-checked against one width (e.g., `Short.MAX_VALUE`) but encoded with a different width (4-byte length prefix vs 2-byte), so values that pass validation later fail at serialization time with an opaque error.
**Look for:** Length-check constants (`MAX_NAME_BYTES`, `MAX_LEN`) decoupled from the actual serializer's width; mismatched `writeShort(len)` / `readInt(len)` pairs.

### F-20: Number / decimal parser accepts adversarial exponent without bound check
`BigDecimal.toPlainString`, `setScale`, or similar methods are called on user input without bounding the exponent, allowing a small input to expand into gigabytes of digits or to exhaust memory.
**Look for:** `new BigDecimal(s).toPlainString()`, `setScale(0, ...)`, parsing of arbitrary-precision numbers from network/CLI; expect missing exponent / precision caps.

### F-21: Charset-less getBytes / new String produces platform-dependent bytes
`String.getBytes()` or `new String(bytes)` is called without an explicit `Charset`, so checksum, hash, or wire-protocol bytes vary by JVM default locale.
**Look for:** Bare `getBytes()` / `new String(b)` in code that computes digests, builds wire frames, or persists identifiers; require explicit `StandardCharsets.UTF_8`.

### F-22: Locale-sensitive case folding applied inconsistently
Identifier normalization uses locale-independent case folding for one component but the default locale for another (`toUpperCase()` vs `toUpperCase(Locale.ROOT)`), producing mismatched keys on JVMs with locale-sensitive case mappings (Turkish "I").
**Look for:** Mixed `toLowerCase()` / `toLowerCase(Locale.ROOT)` calls inside the same identifier-handling pipeline.

### F-23: Locale-sensitive number/date format breaks fixed parser
A `NumberFormat`, `DateFormat`, or `String.format` call inherits the JVM default locale and emits a comma decimal separator that a fixed regex parser then fails to round-trip.
**Look for:** Format objects constructed without `Locale.ROOT`/`Locale.US`; fixed regexes like `\d+\.\d+` that reject locale-formatted output.

### F-24: Date / timestamp parser omits a valid format variant
A timestamp-format enumeration covers most variants but omits one (space-delimited offset, ISO week-year, fractional-second precision), silently failing to parse legitimate timestamps from peers or files.
**Look for:** Hand-rolled `DateFormat`/`SimpleDateFormat` lists; check that all documented input variants are covered, including space-vs-`T` separators and `Z`-vs-offset.

### F-25: Lenient date parser silently rolls over out-of-range fields
A date parser left in lenient mode rolls "Feb 30" into March without rejecting it, accepting and silently shifting invalid input.
**Look for:** `DateFormat`/`SimpleDateFormat` constructed without `setLenient(false)`; calls accepting user-supplied date strings.

### F-26: Negative or sentinel value bypasses range / unit conversion check
A configuration accepts `-1` (or another sentinel meaning "disabled") that the type converter cannot represent, throwing on first use; or a sentinel used as a real value (e.g. timestamp `-1`) participates in arithmetic where a real comparison is expected.
**Look for:** Config setters where the legacy sentinel must be mapped to `null`/`Optional.empty()` before converting; sentinel constants like `-1`, `Long.MIN_VALUE`, `Long.MAX_VALUE` used in comparisons without an "is sentinel" guard.

### F-27: Unit-suffixed property parsed without applying unit
A property whose name advertises units (`...Mb`, `...Kb`, `...Ms`, `...Bytes`) is read as a raw integer and never multiplied by the corresponding factor, so the stored value is 1024x or 1000x off.
**Look for:** `getInt("foo.size.mb")` immediately stored to a `long bytes` field; or values written to a "kb" field while the value is in bytes.

### F-28: Mutually-exclusive options accepted together with no validation
Two configuration keys that should be mutually exclusive (auto+manual, min+max, encrypt+plaintext) have no cross-check at parse time, surfacing as confusing runtime errors later.
**Look for:** `if (configA.set() && configB.set()) throw ...` missing in the validation pass; expect new options added without updating the cross-validation matrix.

### F-29: User-supplied option flag silently overridden by hardcoded default
A constructor accepts a parameter (encryption options, throttle, headers) but never wires it through, so the default is silently used regardless of caller input.
**Look for:** Constructor parameter with no field assignment, or a setter call that re-applies the hardcoded default after the user value was assigned.

### F-30: One namespace's identifier passed to a lookup keyed by another namespace
A lookup populated using one identifier representation (post-quote, with port, with prefix) is queried using a different representation (raw, without port, stripped), so every valid entry silently misses.
**Look for:** Pairs of put/get on the same map where the key is built differently on each side; address strings with/without port, identifiers with/without quoting, paths absolute-vs-relative.

### F-31: Reserved character in user input corrupts downstream framing
A delimiter character that the format reserves (gossip separator, CSV separator, log4j pattern) is accepted in user input without escaping, so the field corrupts the encoded record.
**Look for:** Constructors / setters of structured identifiers that take an arbitrary string; check that the reserved character is rejected or escaped on entry.

### F-32: Validation that throws bypasses caller's "not found" / no-op contract
A throwing helper (`getOrThrow`) is used on user-supplied input where the contract is "missing == empty"; the raw exception escapes to the user instead of being normalized.
**Look for:** `Map.get` lookups on user input followed by `.orElseThrow()` or assertions, in code paths where absence should yield an empty result; or "not found" rethrown as a generic runtime exception.

### F-33: Empty input array / config reaches code that derives an internal size from it
External empty input is passed to a constructor that derives internal-array size from the input length, so the zero-size array later throws `ArrayIndexOutOfBoundsException` on first access.
**Look for:** Constructors that allocate `new T[input.length]` from caller-supplied data without an early `if (input.length == 0)` check.

### F-34: Boolean type guard accepts only one variant, missing a sibling
A guard checks `instanceof FrozenCollection` but not the matching `MultiCellCollection`, or vice versa; one valid variant takes the unsafe default path or is rejected.
**Look for:** `instanceof` chains that enumerate one subtype; verify a structural sibling isn't silently routed elsewhere. Common in collection / wrapped-type / decorator scenarios.

### F-35: Tokenization halts at a delimiter without consuming remaining tokens
A startup script / option parser halts at a delimiter (`--`, `;`) without forwarding remaining arguments, silently dropping user-provided options.
**Look for:** `for arg in $@` loops with `break` on a delimiter; argparse / getopt loops that don't accumulate the tail.

### F-36: Sub-name / nested name parsing uses left-to-right with fixed token count
A compound identifier with a variable-length leading component is parsed left-to-right by counting fixed tokens, misidentifying fields when the leading component itself contains the delimiter.
**Look for:** `split("/", 2)` on URLs/paths where the host or scheme can contain `/`; right-anchored parsing (`lastIndexOf`) is usually correct.

### F-37: Whitespace not trimmed from tokens before lookup
Tokens from `split(...)` are passed straight to a lookup without `trim()`, causing every entry with surrounding whitespace to fail validation.
**Look for:** CSV / config parsing pipelines that immediately do `Map.get(token)` after `split(",")`.

### F-38: Membership check uses wrong key type, silently misses every entry
`Map.get` accepts `Object` and silently returns `null` when the wrong key type (boxed-vs-primitive, wrapper-vs-raw, byte[]-vs-String) is passed.
**Look for:** Lookups where the value type doesn't match the map's declared key type; `set.contains(name)` where `name` is a wrapper but elements are raw.

### F-39: Trusting stored boolean / metadata flag without cross-validation
A migration trusts a stored boolean flag that an earlier version may have written incorrectly, carrying the stale value into a new schema.
**Look for:** Migration code paths reading a single flag from an old format without verifying it against an independent heuristic; expect "trust but verify" to be missing.

### F-40: Buffer length / framing prefix mismatch between writer and reader
A writer uses one length-prefix width (4-byte) but the reader uses another (2-byte), or one path writes a field unconditionally while the other reads it conditionally, corrupting all subsequent bytes.
**Look for:** Asymmetric `writeInt(len)` / `readShort(len)` pairs across versions; conditional vs unconditional field reads gated on differing flags.
