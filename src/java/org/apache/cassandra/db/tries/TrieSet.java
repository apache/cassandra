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

/// A trie that defines an infinite set of `ByteComparable`s. The convention of this package is that sets always
/// include all boundaries, all prefixes that lead to a boundary, and all descendants of all boundaries. This is done
/// to properly define reverse iteration where prefixes are listed before their descendants, and to allow for the
/// retrieval of metadata on paths pointing to specific keys.
///
/// Trie sets represent sets of ranges of coverage by listing the boundaries between them, and providing a way to
/// identify the "covering state" for positions that are being skipped to.
///
/// Trie sets can be constructed from ranges using [#singleton], [#range] and [#ranges], and can be manipulated via
/// the set algebra methods [#union], [#intersection] and [#weakNegation].
public interface TrieSet extends CursorWalkable<TrieSetCursor>
{
    static TrieSet singleton(ByteComparable.Version version, ByteComparable b)
    {
        return ranges(version, b, b);
    }

    static TrieSet range(ByteComparable.Version version, ByteComparable left, ByteComparable right)
    {
        return ranges(version, left, right);
    }

    static TrieSet ranges(ByteComparable.Version version, ByteComparable... boundaries)
    {
        return dir -> RangesCursor.create(dir, version, boundaries);
    }

    static TrieSet empty(ByteComparable.Version byteComparableVersion)
    {
        return dir -> TrieSetCursor.empty(dir, byteComparableVersion);
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
        int depth = cursor.depth();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (cursor.branchIncluded())
                return ContainsResult.CONTAINED; // The set covers a prefix of the key.
            if (cursor.skipTo(++depth, next) != depth || cursor.incomingTransition() != next)
                return cursor.state().precedingIncluded(Direction.FORWARD) ? ContainsResult.CONTAINED
                                                                           : ContainsResult.NOT_CONTAINED;
            next = bytes.next();
        }
        return cursor.branchIncluded() ? ContainsResult.CONTAINED : ContainsResult.PREFIX;
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

    /// Represents the set inverse of the given set plus all prefixes and descendants of all boundaries of the set.
    /// E.g. the inverse of the set `[a, b]` is the set `union([null, a], [b, null])`, and
    /// `intersection([a, b], weakNegation([a, b]))` equals `union([a, a], [b, b])`.
    ///
    /// True negation is not feasible in this design (exact points are always included together with all their descendants).
    default TrieSet weakNegation()
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
