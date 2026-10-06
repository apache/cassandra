/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A trie that defines an infinite set of `ByteComparable`s. Trie sets represent sets of ranges of coverage by listing
/// the boundaries between them, and providing a way to identify the "covering state" for positions that are being
/// skipped to.
///
/// Trie sets can be constructed from ranges using [#branch], [#range] and [#ranges], and can be manipulated via
/// the set algebra methods [#union], [#intersection] and [#negation]. Sets are usually used to select a subset of a
/// trie using its `intersect` method (see [Trie#intersect], [RangeTrie#intersect], [DeletionAwareTrie#intersect]),
/// which preserves metadata in prefixes that are not part of the set (e.g. content for "a", "ab", "abc", "abe" when
/// listing the branch `[abcde;abefg]`). Additionally, plain tries also provide the [Trie#intersectSlicing] method which
/// only preserves content strictly inside the set (i.e. only "abe" out of the prefix examples above) and can be used to
/// obtain a lexicographically bounded slice of an ordered trie.
public interface TrieSet extends CursorWalkable<TrieSetCursor>
{
    /// The set covering the branch rooted at the given key.
    static TrieSet branch(ByteComparable.Version version, ByteComparable b)
    {
        return rangeInclusiveEnd(version, b, b);
    }

    /// The set between two keys, inclusive of the left key and the branch rooted at the right bound.
    static TrieSet rangeInclusiveEnd(ByteComparable.Version version, ByteComparable left, ByteComparable right)
    {
        return range(version, left, true, right, true);
    }

    /// The set between two keys, inclusive of the left key and excluding the right bound or any of its descendants.
    static TrieSet rangeExclusiveEnd(ByteComparable.Version version, ByteComparable left, ByteComparable right)
    {
        return range(version, left, true, right, false);
    }

    /// The set between two keys with the given inclusivity. Note that inclusivity here applies to the branch rooted at
    /// the respective key. For example, `(abc; cde]` does not contain "cdf", "abc" or "abcd" and includes "abd", "cde",
    /// and "cdez".
    static TrieSet range(ByteComparable.Version version, ByteComparable left, boolean leftInclusive, ByteComparable right, boolean rightInclusive)
    {
        return ranges(version, leftInclusive, rightInclusive, left, right);
    }

    /// The set between the given pairs of boundaries. This is the same as the union of the range sets produced by
    /// each pair in the `boundaries` array, done in a single step. The inclusivity parameters apply to the bound at the
    /// respective side in every pair, `leftInclusive` to all left bounds (every even position in the array), and
    /// `rightInclusive` to all right ones (every odd position in the array).
    ///
    /// The keys in the array must be given in order, taking into account the inclusivity parameter (where e.g.
    /// right-inclusive bound "a" is greater than "ab").
    ///
    /// Also see [RangesCursor] for further information.
    static TrieSet ranges(ByteComparable.Version version, boolean leftInclusive, boolean rightInclusive, ByteComparable... boundaries)
    {
        return dir -> RangesCursor.create(dir, version, leftInclusive, rightInclusive, boundaries);
    }

    /// The set between the given pairs of boundaries, start-inclusive and end-exclusive. This is the same as the union
    /// of the range sets produced by each pair in the `boundaries` array, done in a single step.
    ///
    /// The keys in the array must be given in order. If the same key appears twice it has no effect on the result.
    ///
    /// Also see [RangesCursor] for further information.
    static TrieSet ranges(ByteComparable.Version version, ByteComparable... boundaries)
    {
        return ranges(version, true, false, boundaries);
    }

    static TrieSet empty(ByteComparable.Version byteComparableVersion)
    {
        return ranges(byteComparableVersion, true, false);
    }

    static TrieSet full(ByteComparable.Version byteComparableVersion)
    {
        return ranges(byteComparableVersion, true, false, null, null);
    }

    /// Returns true if the given key is strictly contained in this set, i.e. it falls inside a covered range or branch.
    /// This excludes prefixes of set boundaries.
    default boolean strictlyContains(ByteComparable key)
    {
        return contains(key) == ContainsResult.CONTAINED;
    }

    enum ContainsResult
    {
        CONTAINED,
        PREFIX,
        NOT_CONTAINED
    }

    /// Returns whether the given key is contained in this set. Returns CONTAINED if it falls inside a covered range or
    /// branch, PREFIX if it is a prefix of a set boundary, and NOT_CONTAINED if it is not contained in the set at all.
    default ContainsResult contains(ByteComparable key)
    {
        TrieSetCursor cursor = cursor(Direction.FORWARD);
        final ByteSource bytes = key.asComparableBytes(cursor.byteComparableVersion());
        int next = bytes.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            long skipPosition = Cursor.positionForDescentWithByte(cursor.encodedPosition(), next);
            if (Cursor.compare(cursor.skipTo(skipPosition), skipPosition) != 0)
                return cursor.state().precedingIncluded(Direction.FORWARD) ? ContainsResult.CONTAINED
                                                                           : ContainsResult.NOT_CONTAINED;

            next = bytes.next();
        }
        return cursor.state().succeedingIncluded(Direction.FORWARD) ? ContainsResult.CONTAINED
                                                                    : ContainsResult.PREFIX;
    }

    default TrieSet union(TrieSet other)
    {
        // This method is currently only used for tests. Implemented by deMorgan's rule (`A u B = ~(~A x ~B)`).
        // It could be done more efficiently if we have an intersection variation that flips the state values
        // internally.
        return dir -> new RangeIntersectionCursor.TrieSet(cursor(dir).negated(),
                                                          other.cursor(dir).negated())
                      .negated();
    }

    default TrieSet intersection(TrieSet other)
    {
        // This method is currently only used for tests. Should we need it for (performance-sensitive) production uses,
        // we should switch to a more direct set-specific intersection implementation.
        return dir -> new RangeIntersectionCursor.TrieSet(cursor(dir), other.cursor(dir));
    }

    /// Represents the set inverse of the given set.
    /// E.g. the inverse of the set `[a, b]` is the set `union([null, a), (b, null])`.
    default TrieSet negation()
    {
        return dir -> cursor(dir).negated();
    }

    /// Constuct a textual representation of the trie.
    default String dump()
    {
        return cursor(Direction.FORWARD).process(new TrieDumper.Plain<>(Object::toString));
    }

    // The methods below form the non-public implementation, whose visibility is restricted to package-level.
    // The warning suppression below is necessary because we cannot limit the visibility of an interface method.
    // We need an interface to be able to implement trie methods by lambdas, which is heavily used above.

    /// Implement this method to provide the concrete trie implementation as the cursor that presents it, most easily
    /// done via a lambda as in the methods above.
    //noinspection ClassEscapesDefinedScope
    TrieSetCursor makeCursor(Direction direction);

    /// @inheritDoc This method's implementation uses [#makeCursor] to get the cursor and may apply additional cursor
    /// checks for tests that run with verification enabled.
    //noinspection ClassEscapesDefinedScope
    @Override
    default TrieSetCursor cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.TrieSet(makeCursor(direction))
                          : makeCursor(direction);
    }
}
