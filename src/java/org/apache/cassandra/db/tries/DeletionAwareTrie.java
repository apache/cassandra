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
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// Deletion-aware trie interface that combines live data and deletion information in a unified structure.
///
/// This class implements the definitions of some simple deletion-aware tries (live singletons, deleted ranges), as
/// well as the main algebraic operations:
/// - intersecting a deletion-aware trie with a set/range, returning only the live paths and content covered by the
///   set, as well as any applicable range restricted to the bounds of the set;
/// - merging deletion-aware tries, applying the deletions of each set to the live data of the others and properly
///   combining the deletion branches of all sources.
///
/// It also provides methods of processing and iterating over the live content of the trie, as well as means
/// of obtaining the full range deletion view of the trie and a combined data-with-deletions view.
///
/// The structure of a deletion-aware trie presents the live data in its normal paths, and deleted ranges
/// in additional "deletion branches". The objective of this split is to be able to separately and efficiently
/// query the two: on one hand, to search for the closest live entry without having to walk paths leading to
/// deleted data, and on the other, to be able to find the covering deletions affecting any position in the
/// trie. With this design, both can be achieved in time proportional to the length of the key.
///
/// For efficiency there can only be at most one deletion branch defined for any path in the trie. I.e. a deletion
/// branch cannot cover another deletion branch. To additionally improve merge performance we also support a mode
/// of operation where it is known that the depth at which a deletion can be introduced is determined in advance for
/// every path for all sources (in other words, that if one source defines a deletion branch at one point, none of the
/// other sources can define a deletion branch below it); this is the mode of operation intended for use in Cassandra
/// memtables and sstables, where deletion branches are defined at the root of each partition.
///
/// This class expects deletions to always be specified as ranges, and the deletion boundaries to never coincide with
/// live keys or their prefixes (this property is provided by the prefix-free [ByteComparable] conversions).
/// While deletion branches can support point deletions, this support is somewhat restricted for efficiency reasons
/// (e.g. the coverage of positions under a point deletion is undefined).
///
/// It is also expected that a deletion-aware trie does not contain any live data that is deleted by its own deletion
/// branches. If such data exists, whether it is preserved after transformations is undefined.
///
/// See [DeletionAwareCursor] for details on cursor operations and [InMemoryDeletionAwareTrie] for the
/// concrete in-memory implementation.
///
/// @param <T> The content type for live data in the trie
/// @param <D> The deletion marker type, must extend [RangeState] for range operations
public interface DeletionAwareTrie<T, D extends RangeState<D>>
extends BaseTrie<T, DeletionAwareCursor<T, D>, DeletionAwareTrie<T, D>>
{
    /// Creates a singleton deletion-aware trie containing only live data at the specified key.
    ///
    /// This method creates a trie with a single entry mapping the given byte-comparable key to the provided
    /// content. The resulting trie contains no deletion information and behaves similarly to a regular
    /// [Trie#singleton], but is compatible with deletion-aware operations.
    ///
    /// @param b The byte-comparable key for the content
    /// @param byteComparableVersion The version to use for byte-comparable serialization
    /// @param v The content to associate with the key
    /// @return A deletion-aware trie containing the single key-value mapping
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, T v)
    {
        return dir -> new SingletonCursor.DeletionAware<>(dir, b.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    /// Creates a deletion-aware trie containing a single deletion range.
    ///
    /// This method creates a trie that represents a deletion covering the range from `prefixInDataTrie`+`left` to
    /// `prefixInDataTrie`+`right`. The deletion is presented as a deletion branch at the specified prefix, allowing
    /// the user to take advantage of predefined deletion-branch positions.
    ///
    /// Range boundaries should be distinct from live data positions; the inclusivity of the boundary point and any
    /// position below it is not specified or guaranteed to be preserved under trie transformations.
    ///
    /// @param prefixInDataTrie The position in the data trie where this deletion branch is rooted
    /// @param left The left boundary of the deletion range
    /// @param right The right boundary of the deletion range
    /// @param byteComparableVersion The version to use for byte-comparable serialization
    /// @param deletion A _covering_ range state that defines the deletion information
    /// @return A deletion-aware trie containing the deletion range
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> deletion(ByteComparable prefixInDataTrie, ByteComparable left, ByteComparable right, ByteComparable.Version byteComparableVersion, D deletion)
    {
        RangeTrie<D> rangeTrie = RangeTrie.range(left, right, byteComparableVersion, deletion);
        return deletionBranch(prefixInDataTrie, byteComparableVersion, rangeTrie);
    }

    /// Creates a deletion-aware trie from an existing range trie representing deletions.
    ///
    /// This method allows for more complex deletion patterns by accepting a pre-constructed [RangeTrie]
    /// that may contain multiple ranges, boundaries, and complex deletion states. This is useful for
    /// advanced scenarios where simple range deletions are insufficient.
    ///
    /// @param prefixInDataTrie The position in the data trie where this deletion branch is to be rooted
    /// @param byteComparableVersion The version to use for byte-comparable serialization
    /// @param rangeTrie A pre-constructed range trie representing the deletion pattern
    /// @return A deletion-aware trie containing the deletion branch
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> deletionBranch(ByteComparable prefixInDataTrie, ByteComparable.Version byteComparableVersion, RangeTrie<D> rangeTrie)
    {
        return dir -> new SingletonCursor.DeletionBranch<>(dir,
                                                           prefixInDataTrie.asComparableBytes(byteComparableVersion), byteComparableVersion,
                                                           rangeTrie);
    }

    /// @inheritDoc
    /// The returned deletion branches will be restricted to the bounds of the set; i.e. any ranges extending beyond
    /// boundaries of the set will be cut to the confines of the set.
    @Override
    default DeletionAwareTrie<T, D> intersect(TrieSet set)
    {
        return dir -> new IntersectionCursor.DeletionAware<>(cursor(dir), set.cursor(dir));
    }

    /// Specialized merge resolver for deletion-aware trie operations.
    ///
    /// This interface extends the basic [Trie.MergeResolver] to handle the additional complexity of
    /// deletion-aware merging, including deletion marker resolution and deletion application logic.
    ///
    /// During merge operations, this resolver handles three types of conflicts:
    /// - **Live Data Conflicts**: Resolved using inherited [#resolve] method
    /// - **Deletion Marker Conflicts**: Resolved using [#resolveMarkers] method
    /// - **Deletion Application**: Applied using [#applyMarker] method
    ///
    /// Additionally, this also provides the [#deletionsAtFixedPoints] flag, which significantly improves merge
    /// performance when the user can guarantee that deletion branches are only introduced at predefined positions.
    interface MergeResolver<T, D extends RangeState<D>> extends Trie.MergeResolver<T>
    {
        /// Resolves conflicts between deletion markers from different sources.
        ///
        /// It is expected that this method will return the overriding deletion marker (e.g. the one with the higher
        /// timestamp), or some combination of information from the two markers.
        ///
        /// @param left Deletion marker from the left source (order not guaranteed)
        /// @param right Deletion marker from the right source (order not guaranteed)
        /// @return The resolved deletion marker, or null if deletions cancel out
        D resolveMarkers(D left, D right);

        /// Applies a deletion marker to live content, potentially removing or modifying it.
        ///
        /// This method defines how deletions affect live data during merge operations. The
        /// implementation determines whether the content should be deleted, partially modified,
        /// or left unchanged based on the deletion marker's properties.
        ///
        /// @param marker The deletion marker to apply
        /// @param content The live content that may be affected by the deletion
        /// @return The content after deletion application, or null if completely deleted
        T applyMarker(D marker, T content);

        /// Indicates whether deletions occur at predetermined points in the trie structure.
        ///
        /// This is a critical performance optimization. When true, guarantees that if one merge source
        /// has a deletion branch at some position, the other source cannot have deletion branches
        /// below or above that position. This allows us to skip walking the data trie to look for
        /// lower-level deletion branches when merging. If the flag is false, we cannot know where
        /// in the covered branch we may have a deletion, thus to be sure to find all we _must_
        /// walk the whole data subtrie. This can be terribly expensive.
        boolean deletionsAtFixedPoints();
    }

    /// Constructs a view of the merge of this deletion-aware trie with another, applying deletions during the merge
    /// process. The view is live, i.e. any write to any of the sources will be reflected in the merged view.
    ///
    /// This merge applies each source's deletions to the other source's live data, and merges deletion branches
    /// to form a valid deletion-aware trie.
    ///
    /// The resolvers will only be called if both sources contains data for a given position, with arguments presented
    /// in arbitrary order.
    ///
    /// @param other The other deletion-aware trie to merge with.
    /// @param mergeResolver Resolver for live data conflicts between the two tries.
    /// @param deletionResolver Resolver for deletion marker conflicts. See [MergeResolver#resolveMarkers].
    /// @param deleter Function to apply deletion markers to live content. See [MergeResolver#applyMarker].
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See [MergeResolver#deletionsAtFixedPoints].
    /// @return A live view of the merged tries with deletions applied
    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other,
                                              Trie.MergeResolver<T> mergeResolver,
                                              Trie.MergeResolver<D> deletionResolver,
                                              BiFunction<D, T, T> deleter,
                                              boolean deletionsAtFixedPoints)
    {
        return dir -> new MergeCursor.DeletionAware<>(mergeResolver,
                                                      deletionResolver,
                                                      deleter,
                                                      cursor(dir),
                                                      other.cursor(dir),
                                                      deletionsAtFixedPoints);
    }

    /// Constructs a view of the merge of this deletion-aware trie with another, applying deletions during the merge
    /// process. The view is live, i.e. any write to any of the sources will be reflected in the merged view.
    ///
    /// This merge applies each source's deletions to the other source's live data, and merges deletion branches
    /// to form a valid deletion-aware trie.
    ///
    /// The resolvers will only be called if both sources contains data for a given position, with arguments presented
    /// in arbitrary order.
    ///
    /// @param other The other deletion-aware trie to merge with
    /// @param mergeResolver Unified [MergeResolver] providing the merge logic
    /// @return A live view of the merged tries with deletions applied
    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other, MergeResolver<T, D> mergeResolver)
    {
        return mergeWith(other, mergeResolver, mergeResolver::resolveMarkers, mergeResolver::applyMarker, mergeResolver.deletionsAtFixedPoints());
    }

    /// See [MergeResolver]
    interface CollectionMergeResolver<T, D extends RangeState<D>>
    extends MergeResolver<T, D>, Trie.CollectionMergeResolver<T>
    {
        /// Resolves conflicts between deletion markers from different sources.
        ///
        /// It is expected that this method will return the overriding deletion marker (e.g. the one with the higher
        /// timestamp), or some combination of information from the two markers.
        ///
        /// @param markers A collection of all the markers that apply to a position
        /// @return The resolved deletion marker, or null if deletions cancel out
        D resolveMarkers(Collection<D> markers);

        @Override
        default D resolveMarkers(D c1, D c2)
        {
            return resolveMarkers(ImmutableList.of(c1, c2));
        }
    }

    /// Constructs a view of the merge of multiple deletion-aware tries, applying deletions during the merge
    /// process. The view is live, i.e. any write to any of the sources will be reflected in the merged view.
    ///
    /// This merge applies each source's deletions to the other sources' live data, and merges deletion branches
    /// to form a valid deletion-aware trie.
    ///
    /// The resolvers will only be called if more than one source contains data for a given position, with arguments
    /// presented in arbitrary order.
    ///
    /// @param sources Collection of deletion-aware tries to merge (must not be empty)
    /// @param mergeResolver Unified [CollectionMergeResolver] providing the merge logic
    /// @return A live view of the merged tries with deletions applied
    /// @throws AssertionError if sources collection is empty.
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> merge(Collection<? extends DeletionAwareTrie<T, D>> sources,
                                  CollectionMergeResolver<T, D> mergeResolver)
    {
        return merge(sources,
                     mergeResolver,
                     mergeResolver::resolveMarkers,
                     mergeResolver::applyMarker,
                     mergeResolver.deletionsAtFixedPoints());
    }


    /// Constructs a view of the merge of multiple deletion-aware tries, applying deletions during the merge
    /// process. The view is live, i.e. any write to any of the sources will be reflected in the merged view.
    ///
    /// This merge applies each source's deletions to the other sources' live data, and merges deletion branches
    /// to form a valid deletion-aware trie.
    ///
    /// The resolvers will only be called if more than one source contains data for a given position, with arguments
    /// presented in arbitrary order.
    ///
    /// @param sources Collection of deletion-aware tries to merge (must not be empty).
    /// @param mergeResolver Resolver for live data conflicts across all sources.
    /// @param deletionResolver Resolver for deletion marker conflicts across all sources. See [CollectionMergeResolver#resolveMarkers].
    /// @param deleter Function to apply deletion markers to live content. See [MergeResolver#applyMarker].
    /// @param deletionsAtFixedPoints Optimization flag for predictable deletion patterns. See [MergeResolver#deletionsAtFixedPoints].
    /// @return A live view of the merged tries with deletions applied.
    /// @throws AssertionError if sources collection is empty.
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> merge(Collection<? extends DeletionAwareTrie<T, D>> sources,
                                  Trie.CollectionMergeResolver<T> mergeResolver,
                                  Trie.CollectionMergeResolver<D> deletionResolver,
                                  BiFunction<D, T, T> deleter,
                                  boolean deletionsAtFixedPoints)
    {
        switch (sources.size())
        {
            case 0:
                throw new AssertionError("Cannot merge empty collection of tries");
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends DeletionAwareTrie<T, D>> it = sources.iterator();
                DeletionAwareTrie<T, D> t1 = it.next();
                DeletionAwareTrie<T, D> t2 = it.next();
                return t1.mergeWith(t2, mergeResolver, deletionResolver, deleter, deletionsAtFixedPoints);
            }
            default:
                return dir -> new CollectionMergeCursor.DeletionAware<>(mergeResolver,
                                                                        deletionResolver,
                                                                        deleter,
                                                                        deletionsAtFixedPoints,
                                                                        dir,
                                                                        sources,
                                                                        DeletionAwareTrie::cursor);
        }
    }

    static <T, D extends RangeState<D>> DeletionAwareTrie<T, D> mergeDistinct(List<DeletionAwareTrie<T, D>> tries)
    {
        return merge(tries, throwingResolver());
    }

    @SuppressWarnings("unchecked")
    static <T, D extends RangeState<D>> CollectionMergeResolver<T, D> throwingResolver()
    {
        return THROWING_RESOLVER;
    }

    @SuppressWarnings("rawtypes")
    static final CollectionMergeResolver THROWING_RESOLVER = new CollectionMergeResolver()
    {
        @Override
        public Object resolve(Collection contents)
        {
            throw new AssertionError("Distinct tries expected");
        }

        @Override
        public Object applyMarker(RangeState marker, Object content)
        {
            throw new AssertionError("Distinct tries expected");
        }

        @Override
        public RangeState resolveMarkers(Collection markers)
        {
            throw new AssertionError("Distinct tries expected");
        }

        @Override
        public boolean deletionsAtFixedPoints()
        {
            return true;
        }
    };

    /// Walker interface extended to also process deletion branches.
    interface DeletionAwareWalker<T, D, R> extends Cursor.Walker<T, R>
    {
        /// Called when a deletion branch is found. Return null to skip over it, or the walker to use to descend inside
        /// it.
        ///
        /// Note that the depth given by `resetPathLength` in the deletion branch will be relative to the root of the
        /// deletion branch. See [TrieDumper] for an example of handling this.
        boolean enterDeletionsBranch();

        /// Called for every deletion marker found in the deletion branch.
        void deletionMarker(D marker);

        /// Called when the deletion branch is exited.
        void exitDeletionsBranch();
    }

    @Override
    default String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, Object::toString);
    }

    default String dump(Function<T, String> contentToString,
                        Function<D, String> rangeToString)
    {
        return process(Direction.FORWARD, new TrieDumper.DeletionAware<>(contentToString, rangeToString));
    }

    /// Process the trie using the given [DeletionAwareWalker].
    default <R> R process(Direction direction, DeletionAwareWalker<? super T, ? super D, R> walker)
    {
        return cursor(direction).process(walker);
    }


    /// Returns the state that applies to the given key. This is either the precise state at the given position, or
    /// the range that covers it (i.e. the `precedingState` of the next marker).
    default D applicableDeletion(ByteComparable key)
    {
        DeletionAwareCursor<T, D> dac = cursor(Direction.FORWARD);
        final ByteSource bytes = key.asComparableBytes(dac.byteComparableVersion());
        int next;
        int depth = dac.depth();
        RangeCursor<D> rc;
        while (true)
        {
            rc = dac.deletionBranchCursor(Direction.FORWARD);
            if (rc != null)
                break;
            next = bytes.next();
            if (next == ByteSource.END_OF_STREAM)
                return null; // no deletion branch found
            if (dac.skipTo(++depth, next) != depth || dac.incomingTransition() != next)
                return null;
        }

        if (rc.descendAlong(bytes))
            return rc.state();
        else
            return rc.precedingState();
    }


    /// Returns a view of the live content in this trie as a regular [Trie].
    default Trie<T> contentOnlyTrie()
    {
        return this::cursor;
    }

    /// Returns a view of all deletion ranges in this trie as a single [RangeTrie].
    default RangeTrie<D> deletionOnlyTrie()
    {
        // Note: We must walk the main trie to find deletion branch roots. This can be inefficient.
        return dir -> new DeletionAwareCursor.DeletionsTrieCursor<>(cursor(dir));
    }

    /// Returns a view of the combination of the live data and deletions in this trie as a regular [Trie], using
    /// the provided mapping function to covert values to a common type.
    default <Z> Trie<Z> mergedTrie(BiFunction<T, D, Z> resolver)
    {
        return dir -> new DeletionAwareCursor.LiveAndDeletionsMergeCursor<>(resolver, cursor(dir));
    }

    /// Interface used to ask a cursor to stop issuing deletions. Provided by the cursor implementing
    /// [#mergedTrieSwitchable].
    interface DeletionsStopControl
    {
        void stopIssuingDeletions(Cursor.ResettingTransitionsReceiver receiver);
    }

    /// Returns a view of the combination of the live data and deletions in this trie as a regular [Trie], using
    /// the provided mapping function to covert values to a common type.
    ///
    /// The only difference with [#mergedTrie] is that this cursor can be asked to stop visiting deletion branches
    /// via the [DeletionsStopControl] interface.
    default <Z> Trie<Z> mergedTrieSwitchable(BiFunction<T, D, Z> resolver)
    {
        return dir -> new DeletionAwareCursor.SwitchableLiveAndDeletionsMergeCursor<>(resolver, cursor(dir));
    }

    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> empty(ByteComparable.Version byteComparableVersion)
    {
        return direction -> new DeletionAwareCursor.Empty<>(direction, byteComparableVersion);
    }

    @Override
    default DeletionAwareTrie<T, D> prefixedBy(ByteComparable prefix)
    {
        return dir -> new PrefixedCursor.DeletionAware<>(prefix, cursor(dir));
    }

    /// @inheritDoc
    ///
    /// Note: if the cursor is positioned below a deletion branch root, the tail will not include any information about
    /// that deletion branch, even if it applies to the current position.
    @Override
    default DeletionAwareTrie<T, D> tailTrie(ByteComparable prefix)
    {
        DeletionAwareCursor<T, D> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return null;
    }

    /// Returns an entry set containing all tail tree constructed at the points that contain content of
    /// the given type.
    default Iterable<Map.Entry<ByteComparable.Preencoded, DeletionAwareTrie<T, D>>> tailTries(Direction direction, Class<? extends T> clazz)
    {
        return () -> new TrieTailsIterator.AsEntriesDeletionAware<>(cursor(direction), clazz);
    }

    // The methods below form the non-public implementation, whose visibility is restricted to package-level.
    // The warning suppression below is necessary because we cannot limit the visibility of an interface method.
    // We need an interface to be able to implement trie methods by lambdas, which is heavily used above.

    /// Implement this method to provide the concrete trie implementation as the cursor that presents it, most easily
    /// done via a lambda as in the methods above.
    //noinspection ClassEscapesDefinedScope
    DeletionAwareCursor<T, D> makeCursor(Direction direction);

    /// @inheritDoc This method's implementation uses [#makeCursor] to get the cursor and may apply additional cursor
    /// checks for tests that run with verification enabled.
    //noinspection ClassEscapesDefinedScope
    @Override
    default DeletionAwareCursor<T, D> cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.DeletionAware<>(makeCursor(direction))
                          : makeCursor(direction);
    }
}
