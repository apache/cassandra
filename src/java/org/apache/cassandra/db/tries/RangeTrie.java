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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// Range trie. This type of trie represents information associated with ranges of keys. Its primary application is to
/// support deletions/tombstones.
///
/// Range tries use [RangeState]s for their content, but they also report a [RangeCursor#state] for any prefix point,
/// so that skips inside the trie can figure out if the new position is covered by some of the trie's ranges.
///
/// For simplicity and efficiency, range boundaries must be distinct from the keys they cover, and also distinct from
/// any prefix of a valid data key. Should a specific branch need to be covered, a range of boundaries that fall before
/// and after that key should be used -- this is easy to do with the prefix-free [ByteComparable] system we have in
/// place.
///
/// See [RangeCursor] and [Trie.md](./Trie.md) for further details on the implementation of range tries.
public interface RangeTrie<S extends RangeState<S>> extends BaseTrie<S, RangeCursor<S>, RangeTrie<S>>
{
    /// Returns a singleton trie mapping the given byte path to a marker.
    ///
    /// Note: Ranges are meant to use boundaries that are distinct from data and thus a singleton range would list
    /// only a boundary and always be empty in terms of covered content. However, we do want to be able to place
    /// metadata in intermediate nodes of the trie and this method makes that possible.
    static <S extends RangeState<S>> RangeTrie<S> singleton(ByteComparable key, ByteComparable.Version byteComparableVersion, S v)
    {
        Preconditions.checkArgument(v.isBoundary()); // make sure marker is returned for content()
        Preconditions.checkArgument(v.precedingState(Direction.FORWARD) == null);
        Preconditions.checkArgument(v.precedingState(Direction.REVERSE) == null);
        return dir -> new SingletonCursor.Range<>(dir, key.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    /// Returns a range trie covering a single range. This performs the same process as intersecting a covered
    /// range by a set, converting the passed marker to the proper state depending on the set's coverage and boundaries.
    /// To this end, the passed marker must be a covering state (i.e. it must not be reportable, and must have the same
    /// forward and reverse `precedingState`).
    static <S extends RangeState<S>> RangeTrie<S> range(ByteComparable left, ByteComparable right, ByteComparable.Version byteComparableVersion, S v)
    {
        return fromSet(TrieSet.range(byteComparableVersion, left, right), v);
    }

    /// Returns a range trie covering the given set. This performs the same process as intersecting a covered
    /// range by a set, converting the passed marker to the proper state depending on the set's coverage and boundaries.
    /// To this end, the passed marker must be a covering state (i.e. it must not be boundary, and must return itself
    /// as the forward and reverse `precedingState`).
    static <S extends RangeState<S>> RangeTrie<S> fromSet(TrieSet set, S v)
    {
        Preconditions.checkArgument(!v.isBoundary());
        Preconditions.checkArgument(v.precedingState(Direction.FORWARD) == v);
        Preconditions.checkArgument(v.precedingState(Direction.REVERSE) == v);
        return dir -> new RangeCursor.FromSet<>(set.cursor(dir), v);
    }

    /// Returns the state that applies to the given key. This is either the precise state at the given position, or
    /// the range that covers it (i.e. the `precedingState` of the next marker).
    default S applicableRange(ByteComparable key)
    {
        RangeCursor<S> cursor = cursor(Direction.FORWARD);
        final ByteSource bytes = key.asComparableBytes(cursor.byteComparableVersion());
        if (cursor.descendAlong(bytes))
            return cursor.state();
        else
            return cursor.precedingState();
    }

    @Override
    default RangeTrie<S> intersect(TrieSet set)
    {
        return dir -> new RangeIntersectionCursor<>(cursor(dir), set.cursor(dir));
    }

    /// Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in both sources, the resolver will be called to obtain the combination.
    /// (The resolver will not be called if there's content from only one source.)
    default RangeTrie<S> mergeWith(RangeTrie<S> other, Trie.MergeResolver<S> resolver)
    {
        return dir -> new MergeCursor.Range<>(resolver, cursor(dir), other.cursor(dir));
    }

    /// Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in more than one sources, the resolver will be called to obtain the
    /// combination. (The resolver will not be called if there's content from only one source.)
    static <S extends RangeState<S>> RangeTrie<S> merge(Collection<? extends RangeTrie<S>> sources, Trie.CollectionMergeResolver<S> resolver)
    {
        switch (sources.size())
        {
            case 0:
                throw new AssertionError();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends RangeTrie<S>> it = sources.iterator();
                RangeTrie<S> t1 = it.next();
                RangeTrie<S> t2 = it.next();
                return t1.mergeWith(t2, resolver);
            }
            default:
                return dir -> new CollectionMergeCursor.Range<>(resolver, dir, sources, RangeTrie::cursor);
        }
    }

    /// Applies these ranges to a given data trie. The meaning of the application is defined by the given mapper:
    /// whenever the trie's content falls under a range, the mapper is called to return the content that should be
    /// presented.
    ///
    /// This operation will only list positions that are present in the source trie. This means, on one hand,
    /// that it is not possible to add new content from the range trie, only to augment (usually delete) existing.
    /// On the other, that the size of the range trie does not affect the size of the output or the complexity of
    /// processing it.
    default <T> Trie<T> applyTo(Trie<T> source, BiFunction<S, T, T> mapper)
    {
        return dir -> new RangeApplyCursor<>(mapper, cursor(dir), source.cursor(dir));
    }

    static <S extends RangeState<S>> RangeTrie<S> empty(ByteComparable.Version version)
    {
        return dir -> RangeCursor.empty(dir, version);
    }

    @Override
    default RangeTrie<S> prefixedBy(ByteComparable prefix)
    {
        return dir -> new PrefixedCursor.Range<>(prefix, cursor(dir));
    }

    @Override
    default RangeTrie<S> tailTrie(ByteComparable prefix)
    {
        RangeCursor<S> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else if (c.precedingState() != null)
            return c::precedingStateCursor;
        else
            return null;
    }

    /// Returns an entry set containing all tail tree constructed at the points that contain content of
    /// the given type.
    default Iterable<Map.Entry<ByteComparable.Preencoded, RangeTrie<S>>> tailTries(Direction direction, Class<? extends S> clazz)
    {
        return () -> new TrieTailsIterator.AsEntriesRange<>(cursor(direction), clazz);
    }

    // The methods below form the non-public implementation, whose visibility is restricted to package-level.
    // The warning suppression below is necessary because we cannot limit the visibility of an interface method.
    // We need an interface to be able to implement trie methods by lambdas, which is heavily used above.

    /// Implement this method to provide the concrete trie implementation as the cursor that presents it, most easily
    /// done via a lambda as in the methods above.
    //noinspection ClassEscapesDefinedScope
    RangeCursor<S> makeCursor(Direction direction);

    /// @inheritDoc This method's implementation uses [#makeCursor] to get the cursor and may apply additional cursor
    /// checks for tests that run with verification enabled.
    //noinspection ClassEscapesDefinedScope
    @Override
    default RangeCursor<S> cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.Range<>(makeCursor(direction))
                          : makeCursor(direction);
    }
}
