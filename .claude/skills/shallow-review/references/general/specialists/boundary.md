# Boundary & I/O — Specialist Checklist

22 highest-signal questions. 17% of all bugs fall in this domain.

---

## Off-by-One & Range

1. [ ] Does this code iterate from the wrong start or stop one element too early/late? Check `<` vs `<=`, `begin` vs `begin + 1`, missing last chunk on exact boundary.
   → Also: round-robin wrap-around should use `>=` not `==`; pre-filled array loops must offset start index.

2. [ ] Does a range fail to handle `start == end` (degenerate/empty)? Does a binary search adjust by +1/-1 without handling the exact-match case?

3. [ ] Does an exponential backoff counter start at 1 when the formula assumes 0-based? `pow(base, attempts)` over-scales the first retry if attempts isn't decremented.
   → Also: cumulative-division loops accumulate floating-point residual, drifting into an extra iteration.

4. [ ] In a backward / reverse scan across a wrapping range (begin > end in token space), does the termination condition still hold? Strict `<` sentinels on wrapping ranges never terminate.

## Integer Overflow

5. [ ] Does integer-to-bytes conversion (`mebibytes * 1024 * 1024`) perform all multiplication in `int`? At least one operand needs `L` suffix. Does `(int) longValue` on a file size/offset truncate above 2GB?

6. [ ] Does integer division truncate to zero when divisor exceeds dividend? Does `(value / N) * N` produce zero when `value < N` — causing infinite loops or zero-sized allocations?

7. [ ] Does arithmetic on `Integer.MAX_VALUE` sentinel (e.g., `limit + 1`) overflow without a guard? Does a helper returning `long` compute intermediates in `int` before widening?

## Null & Bounds

8. [ ] Does this code dereference a lookup result (map, schema, metadata) without null check? Does `File.listFiles()` result get used without null check (returns null on I/O error)?
   → Also: shared field read twice without local capture → concurrent nullification between check and use.

9. [ ] Does this code index into array/list without checking emptiness? Does `subList(0, N)` or `list.get(0)` execute without a size guard?

10. [ ] Does a division use a runtime-derived divisor (collection size, mean) that can be zero?

11. [ ] Does `String.indexOf(...)` / `lastIndexOf(...)` feed directly into `substring(...)` or arithmetic without a `!= -1` guard? Does `split(...)` return a one-element array when the delimiter is absent, while the caller assumes multiple elements?

## Sentinel Values

12. [ ] When an API returns -1 (EOF), null (absent), or 0 (not found), does EVERY path check the sentinel BEFORE using the return value in arithmetic or narrowing cast?

13. [ ] Does a negative sentinel (`-1`/`-2`/large-negative) meaning "unset" or "infinite" get substituted for its effective value before arithmetic? `now + rawDuration` otherwise jumps into the past; sentinel-as-count causes infinite loops.

## ByteBuffer & I/O

14. [ ] Does `ByteBuffer.array()` call account for `arrayOffset()`? After `slice()`, offset is nonzero. Does no-arg `ByteBuffer.get()` advance position inside a comparator or shared view?

15. [ ] Does a `read()` call return fewer bytes than requested without the caller looping? Use `readFully()` for exact-count reads. NIO `Channel.read()` is never guaranteed to fill the buffer in one call.

16. [ ] Does `ByteBuffer.array()` get called on a potentially direct (off-heap) buffer without a `hasArray()` guard?

17. [ ] Does a write into a fixed-size buffer skip the `remaining() >= N` check before `put`? End-of-segment markers, trailers, or compressed output can overflow when the buffer is exactly full. Does a compression / encode loop exit on input-consumed rather than output-flushed, silently truncating the tail?

18. [ ] After writing into a ByteBuffer, is `flip()` used (sets limit = position) instead of `rewind()` (leaves limit at capacity) before handing it to a reader? `rewind()` exposes uninitialized bytes past the written region.

## Arithmetic

19. [ ] Does `n + 1 / 2` lack parentheses, evaluating as `n + 0`? Does a scaled index (`offset = i * stride`) get boundary-checked against the raw counter `i` instead of the scaled value?

20. [ ] Does a `subList`/`slice` end-index use a different origin (absolute vs relative) than the start index?

21. [ ] Does a `serializedSize()` helper double-count a conditional field (adding it unconditionally and again inside a branch), or count only the length prefix without the payload bytes? Declared size and actual written bytes then diverge, producing framing errors.

22. [ ] Does a write path leave a partial file on exception (no abort/delete, or no `TRUNCATE_EXISTING` when re-opening)? A subsequent read treats truncated or stale-tail content as complete.

---

## False Positives — Do NOT Flag

- Single-element collections passed to `size()` — only flag when size can be zero at runtime
- `ByteBuffer.array()` on buffers known to be heap-allocated (`ByteBuffer.allocate()`)
- Null checks after builders/constructors that guarantee non-null returns
