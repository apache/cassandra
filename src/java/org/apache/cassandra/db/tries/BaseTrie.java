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

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Predicates;

import org.agrona.DirectBuffer;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Base trie interface, providing various transformations of the trie, conversion
/// of its content to other formats (e.g. iterable of values), and several forms of processing.
///
/// For any unimplemented data extraction operations one can build on the [TrieEntriesWalker] (for-each processing)
/// and [TrieEntriesIterator] (to iterator) base classes, which provide the necessary mechanisms to handle walking
/// the trie.
///
/// See [Trie.md](./Trie.md) for further description of the trie representation model.
///
/// @param <T> The content type of the trie.
/// @param <Q> The concrete subtype of the trie.
public interface BaseTrie<T, C extends Cursor<T>, Q extends BaseTrie<T, C, Q>> extends CursorWalkable<C>
{
    /// Adapter interface providing most of the methods a [Cursor.Walker], so that the latter can be used
    /// with [#process].
    /// This enables calls like
    ///     `trie.forEachValue(x -> System.out.println(x));`
    /// to be mapped directly to a single call to [#process] without extra allocations.
    interface ValueConsumer<T2> extends Cursor.Walker<T2, Void>
    {
        @Override
        default Void complete()
        {
            return null;
        }

        @Override
        default void resetPathLength(int newDepth)
        {
            // not tracking path
        }

        @Override
        default void addPathByte(int nextByte)
        {
            // not tracking path
        }

        @Override
        default void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            // not tracking path
        }
    }

    /// Call the given consumer on all content values in the trie in order.
    default void forEachValue(ValueConsumer<? super T> consumer)
    {
        forEachValue(Direction.FORWARD, consumer);
    }

    /// Call the given consumer on all content values in the trie in order.
    default void forEachValue(Direction direction, ValueConsumer<? super T> consumer)
    {
        process(direction, consumer);
    }

    /// Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
    default void forEachEntry(BiConsumer<ByteComparable.Preencoded, ? super T> consumer)
    {
        forEachEntry(Direction.FORWARD, consumer);
    }

    /// Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
    default void forEachEntry(Direction direction, BiConsumer<ByteComparable.Preencoded, ? super T> consumer)
    {
        Cursor<T> cursor = cursor(direction);
        cursor.process(new TrieEntriesWalker.WithConsumer<>(consumer, cursor.byteComparableVersion()));
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /// Process the trie using the given [Cursor.Walker].
    default <R> R process(Direction direction, Cursor.Walker<? super T, R> walker)
    {
        return cursor(direction).process(walker);
    }

    /// Process the trie using the given [ValueConsumer], skipping all branches below the top content-bearing node.
    default void forEachValueSkippingBranches(Direction direction, ValueConsumer<? super T> consumer)
    {
        processSkippingBranches(direction, consumer);
    }

    /// Call the given consumer on all `(path, content)` pairs with non-null content in the trie in order, skipping all
    /// branches below the top content-bearing node.
    default void forEachEntrySkippingBranches(Direction direction, Predicate<? super T> acceptancePredicate, BiConsumer<ByteComparable.Preencoded, ? super T> consumer)
    {
        Cursor<T> cursor = cursor(direction);
        cursor.processSkippingBranches(acceptancePredicate, new TrieEntriesWalker.WithConsumer<>(consumer, cursor.byteComparableVersion()));
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /// Call the given consumer on all `(path, content)` pairs with non-null content in the trie in order, skipping all
    /// branches below the top content-bearing node.
    default void forEachEntrySkippingBranches(Direction direction, BiConsumer<ByteComparable.Preencoded, ? super T> consumer)
    {
        forEachEntrySkippingBranches(direction, Predicates.alwaysTrue(), consumer);
    }

    /// Process the trie using the given [Cursor.Walker], skipping all branches below the top content-bearing node.
    default <R> R processSkippingBranches(Direction direction, Cursor.Walker<? super T, R> walker)
    {
        return processSkippingBranches(direction, Predicates.alwaysTrue(), walker);
    }

    /// Process the trie using the given [Cursor.Walker], skipping all branches below the top content-bearing node.
    default <R> R processSkippingBranches(Direction direction, Predicate<? super T> acceptancePredicate, Cursor.Walker<? super T, R> walker)
    {
        return cursor(direction).processSkippingBranches(acceptancePredicate, walker);
    }

    /// Map-like get by key.
    default T get(ByteComparable key)
    {
        Cursor<T> cursor = cursor(Direction.FORWARD);
        if (cursor.descendAlong(key.asComparableBytes(cursor.byteComparableVersion())))
            return cursor.content();
        else
            return null;
    }

    /// Constuct a textual representation of the trie.
    default String dump()
    {
        return dump(Object::toString);
    }

    /// Constuct a textual representation of the trie using the given content-to-string mapper.
    default String dump(Function<T, String> contentToString)
    {
        return process(Direction.FORWARD, new TrieDumper.Plain<>(contentToString));
    }

    /// Returns the ordered entry set of this trie's content as an iterable.
    default Iterable<Map.Entry<ByteComparable.Preencoded, T>> entrySet()
    {
        return this::entryIterator;
    }

    /// Returns the ordered entry set of this trie's content as an iterable.
    default Iterable<Map.Entry<ByteComparable.Preencoded, T>> entrySet(Direction direction)
    {
        return () -> entryIterator(direction);
    }

    /// Returns the ordered entry set of this trie's content in an iterator.
    default Iterator<Map.Entry<ByteComparable.Preencoded, T>> entryIterator()
    {
        return entryIterator(Direction.FORWARD);
    }

    /// Returns the ordered entry set of this trie's content in an iterator.
    default Iterator<Map.Entry<ByteComparable.Preencoded, T>> entryIterator(Direction direction)
    {
        return new TrieEntriesIterator.AsEntries<>(cursor(direction));
    }

    /// Returns the ordered entry set of this trie's content in an iterable, filtered by the given type.
    default <U extends T> Iterable<Map.Entry<ByteComparable.Preencoded, U>> filteredEntrySet(Class<U> clazz)
    {
        return filteredEntrySet(Direction.FORWARD, clazz);
    }

    /// Returns the ordered entry set of this trie's content in an iterable, filtered by the given type.
    default <U extends T> Iterable<Map.Entry<ByteComparable.Preencoded, U>> filteredEntrySet(Direction direction, Class<U> clazz)
    {
        return () -> filteredEntryIterator(direction, clazz);
    }

    /// Returns the ordered entry set of this trie's content in an iterator, filtered by the given type.
    default <U extends T> Iterator<Map.Entry<ByteComparable.Preencoded, U>> filteredEntryIterator(Direction direction, Class<U> clazz)
    {
        return new TrieEntriesIterator.AsEntriesFilteredByType<>(cursor(direction), clazz);
    }

    /// Returns the ordered set of values of this trie as an iterable.
    default Iterable<T> values()
    {
        return this::valueIterator;
    }

    /// Returns the ordered set of values of this trie as an iterable.
    default Iterable<T> values(Direction direction)
    {
        return direction.isForward() ? this::valueIterator : this::reverseValueIterator;
    }

    /// Returns the ordered set of values of this trie in an iterator.
    default Iterator<T> valueIterator()
    {
        return valueIterator(Direction.FORWARD);
    }

    /// Returns the inversely ordered set of values of this trie in an iterator.
    default Iterator<T> reverseValueIterator()
    {
        return valueIterator(Direction.REVERSE);
    }

    /// Returns the ordered set of values of this trie in an iterator.
    default Iterator<T> valueIterator(Direction direction)
    {
        return new TrieValuesIterator<>(cursor(direction));
    }

    /// Returns the ordered set of values of this trie in an iterable, filtered by the given type.
    default <U extends T> Iterable<U> filteredValues(Class<U> clazz)
    {
        return filteredValues(Direction.FORWARD, clazz);
    }

    /// Returns the ordered set of values of this trie in an iterable, filtered by the given type.
    default <U extends T> Iterable<U> filteredValues(Direction direction, Class<U> clazz)
    {
        return () -> filteredValuesIterator(direction, clazz);
    }

    /// Returns the ordered set of values of this trie in an iterator, filtered by the given type.
    default <U extends T> Iterator<U> filteredValuesIterator(Direction direction, Class<U> clazz)
    {
        return new TrieValuesIterator.FilteredByType<>(cursor(direction), clazz);
    }

    /// Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries,
    /// inclusive of both bounds, any prefix of the bounds, as well as any descendant of the bounds (if one bound is a
    /// prefix of the other, only all descendants of the longer bound).
    ///
    /// The view is live, i.e. any write to the source will be reflected in the subtrie.
    ///
    /// This method will not check its arguments for correctness. The resulting trie may throw an exception if the right
    /// bound is smaller than the left.
    ///
    /// @param left the left bound for the returned subtrie. If `null`, the resulting subtrie is not left-bounded.
    /// @param right the right bound for the returned subtrie. If `null`, the resulting subtrie is not right-bounded.
    /// @return a view of the subtrie containing all the keys of this trie falling between `left` and `right`,
    /// including both bounds, their prefixes and branches.
    default Q subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(TrieSet.rangeInclusiveEnd(cursor(Direction.FORWARD).byteComparableVersion(), left, right));
    }

    /// Returns a view of this trie that is an intersection of its content with the given set. Note that intersections
    /// return content for all positions listed by the set, including prefixes that are not actually contained, to be
    /// able to preserve branch metadata.
    ///
    /// The view is live, i.e. any write to the source will be reflected in the intersection.
    Q intersect(TrieSet set);

    /// Returns a Trie that is a view of this one, where the given prefix is prepended before the root.
    Q prefixedBy(ByteComparable prefix);

    /// Returns a trie that corresponds to the branch of this trie rooted at the given prefix.
    ///
    /// The result will include the same values as `subtrie(prefix, prefix)`, but the keys in the
    /// resulting trie will not include the prefix. In other words,
    /// ```tailTrie(prefix).prefixedBy(prefix) = subtrie(prefix, prefix)```
    /// (Note: This equivalence does not hold for content on the path leading to the branch, because the tail trie
    /// has no way of presenting such content.)
    Q tailTrie(ByteComparable prefix);

    /// Returns an entry set containing all tail tree constructed at the points that contain content of
    /// the given type.
    default Iterable<Map.Entry<ByteComparable.Preencoded, Q>> tailTries(Direction direction, Class<? extends T> clazz)
    {
        return tailTries(direction, clazz::isInstance);
    }

    /// Returns an entry set containing all tail tree constructed at the points that contain content passing
    /// the given predicate.
    Iterable<Map.Entry<ByteComparable.Preencoded, Q>> tailTries(Direction direction, Predicate<? super T> predicate);
}
