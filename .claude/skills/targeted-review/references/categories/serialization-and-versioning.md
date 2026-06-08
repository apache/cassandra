# Category: Serialization and Versioning

Bugs in encoding/decoding symmetry, wire and on-disk formats, version-gated paths, schema evolution, mixed-version compatibility, and switch- or enum-based decode dispatch.

## Diff signals (when to load this category)

Load this category if the patch contains ANY of:
- A `serialize`, `deserialize`, `serializedSize`, `read`, or `write` method, or any pair of these together
- A `switch` or `if/else if` ladder that dispatches on a type tag, enum, or kind discriminator
- A version comparison: `if (version >= ...)`, `protocolVersion`, `apiVersion`, `formatVersion`, `MessagingService.VERSION_*`, or any version-gated branch
- An enum used as a wire-protocol or on-disk discriminator (especially with explicit ordinal/code values, or `values()[i]` decoding)
- A `ByteBuffer` operation: `array()`, `arrayOffset()`, `position()`, `slice()`, `duplicate()`, `flip()`, `rewind()`, or any relative read/write
- A length prefix (`writeInt`, `writeShort`, `writeUnsignedVInt`) followed/preceded by a payload write/read
- A digest or checksum computation over fields, buffers, or sub-records (`MessageDigest.update`, `XXHash`, `CRC32`)
- Schema-evolution markers: nullable→non-nullable changes, new fields added to records, `taggedFields`, `nullableVersions`, `flexibleVersions`, `ignorable`
- Auto-generated message class changes (Avro, Thrift, Kafka JSON message protocol, Protobuf)
- A "compatibility", "legacy", "pre-X.Y", or "old format" code path
- Header / context / metadata propagation through wrapper serializers (`Headers headers` arg added or removed)
- `getBytes()` / `String.getBytes()` / charset-related calls in a serialization context
- Charset, locale, or byte-order assumptions (`UTF_8`, `Locale.getDefault`, `ByteOrder.nativeOrder`)
- A deserializer constructor that takes raw bytes plus a type tag, kind flag, or context object

## Findings

### F-01: Conditional write paired with unconditional read
A serializer writes a field only when present (e.g. behind an isSet flag) while the deserializer reads the field unconditionally, so when the field is absent the reader consumes bytes belonging to the next field.
**Look for:** `if (cond) out.writeXxx(...)` in serialize without a matching `if (cond)` guard in deserialize, or vice versa.

### F-02: Switch-case field bleeds across decoded variants
In a switch- or kind-based deserialization path, a field from a prior decoded value bleeds into the current object because it is not explicitly cleared when the variant indicates it should be absent.
**Look for:** A `switch(kind)` in deserialize where some branches assign a field and others do not, with no `obj.field = null` reset before dispatch.

### F-03: serializedSize disagrees with serialize
The `serializedSize` method omits a field, double-counts a field, uses the wrong width, or hits a different conditional branch than `serialize`, causing buffer underflow/overrun or framing errors.
**Look for:** Pair `serializedSize` and `serialize` side-by-side; check field count, conditional branches, and variable-length sizing helpers (`sizeof`, `sizeOfUnsignedVInt`).

### F-04: Round-trip not closed (write/read/size asymmetry)
A field is written but never read, read but never written, or read in a different order than it is written; bytes after the first asymmetry decode at wrong offsets and the round-trip silently drops or corrupts data.
**Look for:** A diff that touches one of `serialize`/`deserialize`/`serializedSize` but not all three; sequential `writeX(a); writeY(b)` not mirrored verbatim by `readX(); readY()` on the other side.

### F-05: Length-prefix width mismatch
A field is written with one length-prefix width (e.g. 4-byte int) but read with another (e.g. 2-byte short), or the prefix encoding (signed vs unsigned varint, fixed vs variable) differs between sides.
**Look for:** `writeInt`/`readShort` or `writeVInt`/`readUnsignedVInt` mismatches around the same field; or `vintSize(x)` paired with `writeUnsignedVInt(x)`.

### F-06: Mixed-version digest mismatch from divergent fields
Two software versions compute an identity digest over structurally different fields, so a hard equality check between digests always fails during a mixed-version deployment, triggering continuous re-synchronization or repair loops.
**Look for:** A `digest.update` or `hash` call inside a serializer where the field set or order has changed across versions without a version gate.

### F-07: Non-canonical encoding fed directly to a digest
Raw bytes of a value type with multiple equivalent encodings (e.g. variable-length numbers, sorted vs insertion-order maps) are fed directly to a hash without first normalizing; replicas with identical logical state produce different digests.
**Look for:** `hash.update(buffer)` where `buffer` came from a type that has a non-canonical wire form (collections, decimals, big ints).

### F-08: Wire-discriminator enum reordering shifts ordinals
An enum used as a wire-protocol discriminator has constants inserted, removed, or reordered, silently shifting the ordinals/codes of all subsequent constants and breaking peers on the older version.
**Look for:** Reorder of an enum where `ordinal()` or `values()[i]` is used in serialization, or removal of a middle constant; check for explicit `code` overrides.

### F-09: Wire-discriminator enum has unhandled cases
A type-dispatch encoder/decoder handles only some variants of a discriminator and falls through to the wrong branch (or a default that picks the wrong serializer) for newly added or rare variants.
**Look for:** A `switch` on an enum or kind with `default` that assumes an existing variant; missing case for newly added kinds.

### F-10: Buffer position/duplicate omitted before relative read
A `ByteBuffer` returned from a shared structure is read with relative `getInt`/`getLong` before being `duplicate()`d, so the side-effecting position advance corrupts subsequent reads from the same buffer.
**Look for:** `buffer.getInt()` / `buffer.get(...)` without a prior `buffer.duplicate()` when the buffer is a shared resource.

### F-11: arrayOffset ignored on heap ByteBuffer
A digest, byte copy, or array constructor uses `buffer.array()` and `buffer.position()` but omits `buffer.arrayOffset()`, so sliced or duplicated buffers read the wrong byte range.
**Look for:** `buffer.array()` next to `buffer.position()` without `buffer.arrayOffset()`; especially in checksum, digest, or `new String(bytes, ...)` paths.

### F-12: Direct buffer with array() throws UnsupportedOperationException
Calling `buffer.array()` without first calling `buffer.hasArray()` crashes for off-heap (direct) buffers.
**Look for:** `buffer.array()` without a `hasArray()` guard.

### F-13: Buffer not flipped (or wrongly rewound) before downstream consumption
After writing, code calls `rewind()` instead of `flip()` (so the limit stays at capacity and stale bytes leak), or omits `flip()` entirely so the position is at the end and zero bytes are readable.
**Look for:** `buffer.rewind()` after a write; missing `buffer.flip()` before returning a buffer for reading.

### F-14: Buffer position tracking corrupted by raw/wrapper mismatch
Position-aware reads go wrong because: a `slice()`/relative `get` is called on a shared buffer each loop without advancing position, the buffer is rewound on an input the deserializer doesn't own, or writes go through the raw stream while a counting/tracking decorator's position diverges.
**Look for:** Loops that re-`slice` the same source buffer; `position(0)`/`rewind()` on borrowed input; writes through `rawStream` while sizes accumulate via a `TrackingInputStream`/`CountingOutputStream` wrapper.

### F-15: Schema-evolved record sender omits a field; receiver NPEs
A field declared nullable (or just newly added) is omitted by the sender, but the receiver dereferences it without a null/presence guard.
**Look for:** A protocol-generated optional or nullable field touched without `isSet(field)` / `field != null` checks; auto-generated `getX()` calls in deserialize paths.

### F-16: New field added but version-gated paths don't read it
A new field is added to the format without a corresponding version guard on read/write, so older nodes encounter unexpected bytes during a rolling upgrade.
**Look for:** A new `out.writeXxx`/`in.readXxx` line not wrapped in `if (version >= NEW_VERSION)`; check both serialize and serializedSize.

### F-17: New schema field not back-stubbed in older version
A new field is added to the current schema version but the older version's definition isn't given a forward-compatible stub or default, so older nodes fail when they receive records containing the new field.
**Look for:** A schema definition change without a corresponding "legacy" or "pre-X" stub update.

### F-18: Version-equality assertion in shared serializer
A serializer asserts `version == CURRENT_VERSION` (or hard-codes a fixed version), throwing in any mixed-version cluster or when reading historical on-disk data.
**Look for:** `assert version == ...` or `Preconditions.checkArgument(version == ...)` inside serialize/deserialize.

### F-19: Mixed-version exception treated as fatal
A handler throws or escalates on receiving a message from an older protocol version instead of skipping/logging gracefully, breaking rolling upgrades.
**Look for:** An `IllegalArgumentException`/`UnsupportedOperationException` thrown from a deserialize switch when version is less than current.

### F-20: Wrong serializer chosen for sub-variant
A polymorphic dispatch uses a catch-all/default serializer for a type with multiple sub-variants, so one sub-variant is encoded with the wrong wire format and corrupts cross-version interop.
**Look for:** A `getSerializer(type)` or `serializerFor(kind)` that returns a generic serializer when a specific subtype needs its own.

### F-21: Type-info read from data instead of authoritative schema
A serialization path reads type info from the data object rather than the schema/header context; if the schema has changed since the data was written, the stale type produces corrupt or unreadable bytes.
**Look for:** `value.getType()` (instead of `column.type`) inside serialize; `column.type` (instead of the dropped-column header type) inside deserialize.

### F-22: Headers/metadata silently dropped by wrapper serde
A wrapper serializer/deserializer calls a no-headers overload of the inner serde or hardcodes empty headers, dropping caller-supplied context.
**Look for:** A wrapper that takes `(topic, headers, value)` but calls `inner.serialize(topic, value)` or `inner.serialize(topic, EMPTY_HEADERS, value)`.

### F-23: Charset/locale not pinned in serialization
`String.getBytes()` is called without a charset, or a `NumberFormat` inherits the JVM default locale; serialized bytes vary across nodes/locales and round-trip parsing fails.
**Look for:** `getBytes()` with no charset arg; `new DecimalFormat(pattern)` (no locale); `String.toLowerCase()` (no `Locale.ROOT`).

### F-24: Byte order / native alignment assumption
Native memory reads/writes use an "unaligned access OK" fast path without accounting for big-endian, or `ByteBuffer.getLong` is paired with bit shifts assuming big-endian; values are byte-swapped on the other architecture.
**Look for:** `Unsafe.getLong/putLong` used with explicit shifts; `buffer.getLong()` followed by `>> 56` style decoding without `order(BIG_ENDIAN)`.

### F-25: Hand-rolled encoding diverges from canonical serializer
A type's bytes are produced by ad-hoc `getBytes`/`put` calls instead of the canonical serializer (or vice versa), producing a different binary representation that fails round-trip.
**Look for:** Inline `buffer.putLong(uuid.getMostSignificantBits())` etc. instead of using a shared `UUIDSerializer`.

### F-26: Field width or position changed without protocol gate
A length prefix's width changed (1→2→4 bytes) or a field's relative position moved across versions; readers using the older fixed-width helper produce garbled values.
**Look for:** A constant like `LENGTH_BYTES = 2` that diverges from a parallel `writeShort`/`writeInt`; or a "skip 4 bytes" placed before a moved field.

### F-27: Sentinel rendered as data
A sentinel value (e.g. UUID-zero, `-1`, `null`) is serialized as a literal value (string `"null"`, integer `-1`) rather than being omitted or carrying the absence semantics, leaving consumers unable to distinguish absence from a real value.
**Look for:** `writeString(uuid.toString())` without an absence check; serializing `Optional` via `%d` rather than presence guard.

### F-28: Protocol version tag hardcoded or stale
A factory or message-builder hardcodes a stale or sentinel protocol version (`LATEST_PRODUCTION`, `0`, current), bypassing version-aware dispatch and selecting the wrong wire format. Auto-negotiation is also suppressed when an explicit version is supplied to a library that treats it as opt-out.
**Look for:** `new XxxRequest(... , VERSION_X, ...)` with a static version constant; `client.builder().protocolVersion(VERSION_X)` against peers with mixed support.

### F-29: New error code missing from error-handling table
A protocol upgrade introduces a new error code that the client's error-handling map does not contain, so the default branch treats a retriable condition as a fatal error.
**Look for:** A `Map<Errors, Handler>` or `switch (errorCode)` with `default → throw`; check whether new codes added in a sibling commit are missing entries.

### F-30: Compatibility shim swallows or rejects legacy data
A compatibility reader either throws on encountering an unknown/legacy field, or silently drops a field whose presence was semantically significant; records that depended on that field disappear or upgrades stall.
**Look for:** `throw new UnsupportedOperationException("legacy")` in a read path; or a quiet `// skip legacy field` that was the carrier of state.

### F-31: Skip path leaves unconsumed checksum/trailer in stream
An error-handling skip returns early without consuming a checksum suffix or sub-record trailer; the stream is left positioned on bytes that are misread as the next record's header.
**Look for:** `if (failure) return;` inside a deserialize loop where the format has a fixed-size suffix per record.

### F-32: Validation passes structure but not semantics
A validator checks the byte count or structural form but does not decode the fields and verify their inter-field constraints, so semantically invalid (but well-framed) values are accepted.
**Look for:** A `validate(buffer)` whose body only inspects buffer length / signature, never decoding content.

### F-33: Variable-length field prefix consumed conditionally
A variable-length-field length prefix must always be read regardless of any presence-flag, but the read sits behind an early-return; subsequent entries decode from misaligned bytes.
**Look for:** A read of `length = in.readUnsignedVInt()` after an `if (!present) return;` early exit.

### F-34: Deserializer crashes on legitimate empty input
An assertion `length > 0` or a length-zero short-circuit converts a valid empty value into an exception, or emits a `null` literal indistinguishable from true absence.
**Look for:** `assert length > 0;` or `if (length == 0) return null;` in a deserialize helper called for legitimately-empty buffers.

### F-35: Delimiter not escaped inside serialized field
A delimiter-based serialization format does not escape the delimiter when it appears in a field value; round-tripping ambiguous data corrupts the parse.
**Look for:** `String.join(",", parts)` or hand-rolled delimited writers that don't escape; `split(...)` on the read side.

### F-36: Field default flips compatibility
A schema-field default changes (e.g. nullable → non-nullable, default `0` → default `-1`, or enum default added later); old clients/servers send or expect the old default and the new code crashes or computes wrong state.
**Look for:** Schema-definition diffs touching `default`, `nullable`, `nullableVersions`, `ignorable`, `taggedVersions`; correlate with read paths that don't accept the old default.

### F-37: Same field written twice (outer + inner) after format move
After a version refactor that moves a field from outer to inner serializer, both serializers write it; the field appears twice on the wire and corrupts the stream.
**Look for:** Two `out.writeX(field)` calls along a serialize chain; check both call sites in the new format.

### F-38: API version stability flag stuck at "unstable"
A protocol version's stability flag is left as preview/unstable after the feature ships, so clients permanently negotiate a lower version.
**Look for:** `latestVersionUnstable: true` or `STABLE = false` constants associated with shipped protocol versions.

### F-39: Hand-maintained switch duplicated across implementations diverges
Multiple sibling implementations (read/write of nullable vs non-nullable types, two coordinator versions, refactored sub-classes) hand-maintain parallel switches; a fix to one is missed in the other and the formats drift.
**Look for:** Two `switch (type)` blocks with similar shape in sibling files; check whether a fix touches only one.

### F-40: Serialized form diverges from logical equality
A type's `serialize` output differs for two values that compare equal (or two equal serializations decode to unequal objects); comparisons over the wire produce false mismatches. Special case: non-deterministic map iteration order leaks into the wire format.
**Look for:** Custom `equals` that ignores a field that `serialize` writes; ordering-sensitive serialization (`for (Entry e : hashMap.entrySet()) out.writeX(e)`) for an unordered logical type.

### F-41: New record/handler not registered with serialization registry
A new schema entry, message verb, or serializer subtype is registered with the wrong serializer, no handler, or omitted from a hand-maintained registry; messages of that type are dropped, mis-decoded, or omitted from gossip/propagation.
**Look for:** A new sealed-type/enum constant introduced without a parallel entry in the `Serializers` map / `MessagingService.registerVerb` / serialization registry.

## Footnotes

These signals overlap with other categories — when in doubt, also load:
- **boundaries-and-arithmetic** for length-prefix arithmetic overflow, position math, vint boundaries
- **api-contracts-and-completeness** for builder-omits-field, copy-omits-field, equals/hashCode incompleteness
- **lifecycle-and-state** for buffer-recycled-before-write, registration-order bugs
- **concurrency** for non-volatile field read twice across a deserialize boundary
