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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Basic deterministic trie interface.
///
/// Normal users of tries will only use the public methods of [BaseTrie] and this class, which provide various
/// transformations of the trie, conversion of its content to other formats (e.g. iterable of values), and several
/// forms of processing.
///
/// For any unimplemented data extraction operations one can build on the [TrieEntriesWalker] (for-each processing)
/// and [TrieEntriesIterator] (to iterator) base classes, which provide the necessary mechanisms to handle walking
/// the trie.
///
/// The internal representation of tries using this interface is defined in the [Cursor] interface, accessed via the
/// [CursorWalkable] interface's [#cursor] method. [#cursor]/[#makeCursor] is the only method of the interface without
/// an implementation; to define a trie, one needs to implement it. We usually do this by returning a lambda, and Java's
/// single abstract method functionality takes care of adding the trie plumbing around it.
///
/// Cursors are a method of presenting the internal structure of a trie without representing nodes as objects, which is
/// still useful for performing the basic operations on tries (iteration, slicing/intersection and merging). A cursor
/// will list the nodes of a trie in order, together with information about the path that was taken to reach them.
///
/// To begin traversal over a trie, one must retrieve a cursor by calling [#cursor]. Because cursors are
/// stateful, the traversal must always proceed from one thread. Should concurrent reads be required, separate calls to
/// [#cursor] must be made. Any modification that has completed before the construction of a cursor must be
/// visible, but any later concurrent modifications may be presented fully, partially or not at all; this also means that
/// if multiple are made, the cursor may see any part of any subset of them.
///
/// Note: This model only supports depth-first traversals. We do not currently have a need for breadth-first walks.
///
/// See [Trie.md](./Trie.md) for further description of the trie representation model.
///
/// @param <T> The content type of the trie.
public interface Trie<T> extends BaseTrie<T, Cursor<T>, Trie<T>>
{
    boolean DEBUG = CassandraRelevantProperties.TRIE_DEBUG.getBoolean();

    /// Returns a singleton trie mapping the given byte path to content.
    static <T> Trie<T> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, T v)
    {
        return dir -> new SingletonCursor<>(dir, b.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    @Override
    default Trie<T> intersect(TrieSet set)
    {
        return dir -> new IntersectionCursor.Plain<>(cursor(dir), set.cursor(dir));
    }

    /// A version of subtrie that supports control over the inclusivity of bounds.
    /// Neither of the two bounds can be a prefix of the other.
    ///
    /// Unlike `subtrie`, prefixes of the boundaries are reported only if they fall in the span (i.e. are prefixes of a
    /// right bound), and the branches rooted at the relevant boundary are covered if and only if the boundary itself is
    /// included.
    ///
    /// For example, `slice(20, false, 40, true)` excludes `2020` (which is a descendant of the excluded `20`) but
    /// includes `4040` (a descendant of the included `40`).
    default Trie<T> slice(ByteComparable left, boolean inclusiveLeft, ByteComparable right, boolean inclusiveRight)
    {
        return dir -> {
            Cursor<T> cursor = cursor(dir);
            return new IntersectionCursor.PlainSlice<>(cursor,
                                                       RangesCursor.create(dir, cursor.byteComparableVersion(), left, right),
                                                       inclusiveLeft,
                                                       inclusiveRight);
        };
    }

    /// Returns the values in any order. For some tries this is much faster than the ordered iterable.
    default Iterable<T> valuesUnordered()
    {
        return values();
    }

    /// Resolver of content of merged nodes, used for two-source merges (i.e. mergeWith).
    interface MergeResolver<T>
    {
        // Note: No guarantees about argument order.
        // E.g. during t1.mergeWith(t2, resolver), resolver may be called with t1 or t2's items as first argument.
        T resolve(T b1, T b2);
    }

    /// Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in both sources, the resolver will be called to obtain the combination.
    /// (The resolver will not be called if there's content from only one source.)
    default Trie<T> mergeWith(Trie<T> other, MergeResolver<T> resolver)
    {
        return dir -> new MergeCursor.Plain<>(resolver, this.cursor(dir), other.cursor(dir));
    }

    /// Resolver of content of merged nodes.
    ///
    /// The resolver's methods are only called if more than one of the merged nodes contain content, and the
    /// order in which the arguments are given is not defined. Only present non-null values will be included in the
    /// collection passed to the resolving methods.
    ///
    /// Can also be used as a two-source resolver.
    interface CollectionMergeResolver<T> extends MergeResolver<T>
    {
        T resolve(Collection<T> contents);

        @Override
        default T resolve(T c1, T c2)
        {
            return resolve(ImmutableList.of(c1, c2));
        }
    }

    /// Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in more than one sources, the resolver will be called to obtain the
    /// combination. (The resolver will not be called if there's content from only one source.)
    static <T> Trie<T> merge(Collection<? extends Trie<T>> sources, CollectionMergeResolver<T> resolver)
    {
        switch (sources.size())
        {
            case 0:
                throw new AssertionError();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends Trie<T>> it = sources.iterator();
                Trie<T> t1 = it.next();
                Trie<T> t2 = it.next();
                return t1.mergeWith(t2, resolver);
            }
            default:
                return dir -> new CollectionMergeCursor.Plain<>(resolver, dir, sources, Trie::cursor);
        }
    }

    /// Not to be used directly, call [#throwingResolver()] instead.
    static CollectionMergeResolver<Object> THROWING_RESOLVER = new CollectionMergeResolver<Object>()
    {
        @Override
        public Object resolve(Collection<Object> contents)
        {
            throw error();
        }

        private AssertionError error()
        {
            throw new AssertionError("Entries must be distinct.");
        }
    };

    /// Returns a resolver that throws whenever more than one of the merged nodes contains content.
    /// Can be used to merge tries that are known to have distinct content paths.
    @SuppressWarnings("unchecked")
    static <T> CollectionMergeResolver<T> throwingResolver()
    {
        return (CollectionMergeResolver<T>) THROWING_RESOLVER;
    }

    /// Constructs a view of the merge of two tries, where each source must have distinct keys. The view is live, i.e.
    /// any write to any of the sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in more than one sources, the merge will throw an assertion error.
    static <T> Trie<T> mergeDistinct(Trie<T> t1, Trie<T> t2)
    {
        return new Trie<T>()
        {
            @Override
            public Cursor<T> makeCursor(Direction direction)
            {
                return new MergeCursor.Plain<>(throwingResolver(), t1.cursor(direction), t2.cursor(direction));
            }

            @Override
            public Iterable<T> valuesUnordered()
            {
                return Iterables.concat(t1.valuesUnordered(), t2.valuesUnordered());
            }
        };
    }

    /// Constructs a view of the merge of multiple tries, where each source must have distinct keys. The view is live,
    /// i.e. any write to any of the sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in more than one sources, the merge will throw an assertion error.
    static <T> Trie<T> mergeDistinct(Collection<? extends Trie<T>> sources)
    {
        switch (sources.size())
        {
        case 0:
            throw new AssertionError();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return mergeDistinct(t1, t2);
        }
        default:
            return new Trie<T>()
            {
                @Override
                public Cursor<T> makeCursor(Direction direction)
                {
                    return new CollectionMergeCursor.Plain<>(Trie.throwingResolver(), direction, sources, Trie::cursor);
                }

                @Override
                public Iterable<T> valuesUnordered()
                {
                    return Iterables.concat(Iterables.transform(sources, Trie::valuesUnordered));
                }
            };
        }
    }

    @Override
    default Trie<T> prefixedBy(ByteComparable prefix)
    {
        return dir -> new PrefixedCursor.Plain<>(prefix, cursor(dir));
    }

    @Override
    default Trie<T> tailTrie(ByteComparable prefix)
    {
        Cursor<T> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return null;
    }

    /// Returns an entry set containing all tail tree constructed at the points that contain content of
    /// the given type.
    default Iterable<Map.Entry<ByteComparable.Preencoded, Trie<T>>> tailTries(Direction direction, Class<? extends T> clazz)
    {
        return () -> new TrieTailsIterator.AsEntries<>(cursor(direction), clazz);
    }

    static <T> Trie<T> empty(ByteComparable.Version byteComparableVersion)
    {
        return dir -> new Cursor.Empty<>(dir, byteComparableVersion);
    }

    // The methods below form the non-public implementation, whose visibility is restricted to package-level.
    // The warning suppression below is necessary because we cannot limit the visibility of an interface method.
    // We need an interface to be able to implement trie methods by lambdas, which is heavily used above.

    /// Implement this method to provide the concrete trie implementation as the cursor that presents it, most easily
    /// done via a lambda as in the methods above.
    //noinspection ClassEscapesDefinedScope
    Cursor<T> makeCursor(Direction direction);

    /// @inheritDoc This method's implementation uses [#makeCursor] to get the cursor and may apply additional cursor
    /// checks for tests that run with verification enabled.
    //noinspection ClassEscapesDefinedScope
    @Override
    default Cursor<T> cursor(Direction direction)
    {
        return DEBUG ? new VerificationCursor.Plain<>(makeCursor(direction), 0, 0, -1)
                     : makeCursor(direction);
    }
}
