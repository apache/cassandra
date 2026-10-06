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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.common.base.Predicates;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Iterator of trie entries that constructs tail tries for the content-bearing branches that satisfy the given predicate
/// and skips over the returned branches. When a branch that satisfies the predicate is found, the iterator calls
/// [#getContent] which in turn refers to a trie-specific flavour of `mapContent`; if the latter returns null, the null
/// will not be returned by the iterator, but the branch will still be skipped.
public abstract class TrieTailsIterator<T, V, C extends Cursor<T>> extends TriePathReconstructor implements Iterator<V>
{
    final C cursor;
    private final Predicate<? super T> predicate;
    private V next;
    private boolean gotNext;
    private boolean started;

    TrieTailsIterator(C cursor, Predicate<? super T> predicate)
    {
        this.cursor = cursor;
        this.predicate = predicate;
        this.started = false;
        cursor.assertFresh();
    }

    public boolean hasNext()
    {
        while (!gotNext)
        {
            if (started)
            {
                // if we are not just starting, we have returned a branch and must skip over it
                long pos = cursor.skipTo(Cursor.positionForSkippingBranch(cursor.encodedPosition()));
                if (Cursor.isExhausted(pos))
                    return done();
                int depth = Cursor.depth(pos);
                if (depth > 0)
                {
                    resetPathLength(depth - 1);
                    addPathByte(Cursor.incomingTransition(pos));
                }
                else
                    resetPathLength(0);
            }
            else
                started = true;

            boolean gotNextContent = false;
            T nextContent = cursor.content();
            if (nextContent != null)
                gotNextContent = predicate.test(nextContent);

            while (!gotNextContent)
            {
                nextContent = cursor.advanceToContent(this);
                if (nextContent != null)
                    gotNextContent = predicate.test(nextContent);
                else
                    return done();
            }

            next = getContent(nextContent);
            gotNext = next != null;
        }

        return next != null;
    }

    private boolean done()
    {
        gotNext = true;
        next = null;
        return false;
    }

    public V next()
    {
        if (!hasNext())
            throw new NoSuchElementException();
        gotNext = false;
        V v = next;
        next = null;
        return v;
    }

    void skipPreparedNextIf(Predicate<V> shouldSkipPreparedNext)
    {
        if (gotNext && next != null && shouldSkipPreparedNext.test(next))
        {
            gotNext = false;
            next = null;
        }
    }

    protected abstract V getContent(T v);

    ByteComparable.Version byteComparableVersion()
    {
        return cursor.byteComparableVersion();
    }

    public static abstract class Plain<T, V> extends TrieTailsIterator<T, V, Cursor<T>>
    {
        Plain(Cursor<T> cursor, Predicate<? super T> predicate)
        {
            super(cursor, predicate);
        }

        /// Public constructor accepting a Trie and creating a cursor from it
        public Plain(Trie<T> trie, Predicate<? super T> predicate)
        {
            this(trie.cursor(Direction.FORWARD), predicate);
        }

        /// Public constructor accepting a Trie, Direction, and creating a cursor from it
        public Plain(Trie<T> trie, Direction direction, Predicate<? super T> predicate)
        {
            this(trie.cursor(direction), predicate);
        }

        @Override
        protected V getContent(T v)
        {
            // Fix the location of the tail trie source.
            Cursor<T> tailCursor = cursor.tailCursor(cursor.direction());
            return mapContent(v, tailCursor::tailCursor, keyBytes, keyPos);
        }

        protected abstract V mapContent(T value, Trie<T> tailTrie, byte[] bytes, int byteLength);
    }

    public static abstract class Range<S extends RangeState<S>, V> extends TrieTailsIterator<S, V, RangeCursor<S>>
    {
        Range(RangeCursor<S> cursor, Predicate<? super S> predicate)
        {
            super(cursor, predicate);
        }

        /// Public constructor accepting a RangeTrie and creating a cursor from it
        public Range(RangeTrie<S> trie, Predicate<S> predicate)
        {
            this(trie.cursor(Direction.FORWARD), predicate);
        }

        /// Public constructor accepting a RangeTrie, Direction, and creating a cursor from it
        public Range(RangeTrie<S> trie, Direction direction, Predicate<S> predicate)
        {
            this(trie.cursor(direction), predicate);
        }

        @Override
        protected V getContent(S v)
        {
            // Fix the location of the tail trie source.
            RangeCursor<S> tailCursor = cursor.tailCursor(cursor.direction());
            return mapContent(v, tailCursor::tailCursor, keyBytes, keyPos);
        }

        protected abstract V mapContent(S value, RangeTrie<S> tailTrie, byte[] bytes, int byteLength);
    }

    /// Deletion-aware tails iterator that only walks the live data trie and ignores covering deletion branches.
    /// To be used in cases where it is known that deletion branches can only start at or below the selected tail
    /// positions.
    public static abstract class DeletionAwareWithoutCoveringDeletions<T, D extends RangeState<D>, V>
    extends TrieTailsIterator<T, V, DeletionAwareCursor<T, D>>
    {
        DeletionAwareWithoutCoveringDeletions(DeletionAwareCursor<T, D> cursor, Predicate<? super T> predicate)
        {
            super(cursor, predicate);
        }

        /// Public constructor accepting a DeletionAwareTrie, Direction, and creating a cursor from it
        public DeletionAwareWithoutCoveringDeletions(DeletionAwareTrie<T, D> trie, Direction direction, Predicate<? super T> predicate)
        {
            this(trie.cursor(direction), predicate);
        }

        @Override
        protected V getContent(T v)
        {
            // Fix the location of the tail trie source.
            DeletionAwareCursor<T, D> tailCursor = cursor.tailCursor(cursor.direction());
            return mapContent(v, tailCursor::tailCursor, keyBytes, keyPos);
        }

        protected abstract V mapContent(T value, DeletionAwareTrie<T, D> tailTrie, byte[] bytes, int byteLength);
    }

    /// General deletion-aware tail trie iterator. Deletion branches are followed, covering deletions are applied to the
    /// reported branches, and deletion branch data may be used to select a tail trie.
    ///
    /// Instead of a predicate, this version takes a mapping function that is given both live and deletion data, and
    /// will select a branch when it returns a non-null value. Said value is then passed to [#mapContent].
    ///
    /// When `includeCoveringDeletions` is true, this iterator will include deletions that are introduced above the
    /// requested points as deletion branches at the roots of the returned tail tries. If it is false, covering
    /// deletions will be filtered out from the returned tail tries.
    ///
    /// Also offers [#stopIssuingDeletions], which allows it to cease reporting data coming from deletion branches.
    public static abstract class DeletionAware<T, D extends RangeState<D>, V, Q>
    extends TrieTailsIterator<V, Q, DeletionAwareCursor.SwitchableLiveAndDeletionsMergeCursor<T, D, V>>
    {
        final boolean includeCoveringDeletions;

        DeletionAware(DeletionAwareCursor<T, D> cursor, BiFunction<T, D, V> merger, boolean includeCoveringDeletions)
        {
            super(new DeletionAwareCursor.SwitchableLiveAndDeletionsMergeCursor<>(merger, cursor), Predicates.alwaysTrue());
            this.includeCoveringDeletions = includeCoveringDeletions;
        }

        /// Public constructor accepting a DeletionAwareTrie, Direction, and creating a cursor from it
        public DeletionAware(DeletionAwareTrie<T, D> trie, Direction direction, BiFunction<T, D, V> merger, boolean includeCoveringDeletions)
        {
            this(trie.cursor(direction), merger, includeCoveringDeletions);
        }

        @Override
        protected Q getContent(V v)
        {
            return mapContent(v, cursor.deletionAwareTail(includeCoveringDeletions), keyBytes, keyPos);
        }

        public void stopIssuingDeletions(Predicate<Q> shouldSkipPreparedNext)
        {
            skipPreparedNextIf(shouldSkipPreparedNext);
            cursor.stopIssuingDeletions(this);
        }

        protected abstract Q mapContent(V value, DeletionAwareTrie<T, D> tailTrie, byte[] bytes, int byteLength);
    }

    /// Iterator representing the selected content of the trie a sequence of `(path, tail)` pairs, where
    /// `tail` is the branch of the trie rooted at the selected content node (reachable by following
    /// `path`). The tail trie will have the selected content at its root.
    static class AsEntries<T> extends Plain<T, Map.Entry<ByteComparable.Preencoded, Trie<T>>>
    {
        public AsEntries(Cursor<T> cursor, Predicate<? super T> predicate)
        {
            super(cursor, predicate);
        }

        @Override
        protected Map.Entry<ByteComparable.Preencoded, Trie<T>> mapContent(T value, Trie<T> tailTrie, byte[] bytes, int byteLength)
        {
            ByteComparable.Preencoded key = toByteComparable(byteComparableVersion(), bytes, byteLength);
            return new AbstractMap.SimpleImmutableEntry<>(key, tailTrie);
        }
    }

    /// Iterator representing the selected content of the trie a sequence of `(path, tail)` pairs, where
    /// `tail` is the branch of the trie rooted at the selected content node (reachable by following
    /// `path`). The tail trie will have the selected content at its root.
    static class AsEntriesRange<S extends RangeState<S>>
    extends Range<S, Map.Entry<ByteComparable.Preencoded, RangeTrie<S>>>
    {
        public AsEntriesRange(RangeCursor<S> cursor, Predicate<? super S> predicate)
        {
            super(cursor, predicate);
        }

        @Override
        protected Map.Entry<ByteComparable.Preencoded, RangeTrie<S>> mapContent(S value, RangeTrie<S> tailTrie, byte[] bytes, int byteLength)
        {
            ByteComparable.Preencoded key = toByteComparable(byteComparableVersion(), bytes, byteLength);
            return new AbstractMap.SimpleImmutableEntry<>(key, tailTrie);
        }
    }

    /// Iterator representing the selected content of the trie a sequence of `(path, tail)` pairs, where
    /// `tail` is the branch of the trie rooted at the selected content node (reachable by following
    /// `path`). The tail trie will have the selected content at its root.
    ///
    /// When `includeCoveringDeletions` is true, this iterator will include deletions that are introduced above the
    /// requested points as deletion branches at the roots of the returned tail tries. If it is false, covering
    /// deletions will be filtered out from the returned tail tries.
    static class AsEntriesDeletionAware<T, D extends RangeState<D>>
    extends DeletionAware<T, D, T, Map.Entry<ByteComparable.Preencoded, DeletionAwareTrie<T, D>>>
    {
        public AsEntriesDeletionAware(DeletionAwareCursor<T, D> cursor,
                                      Predicate<? super T> predicate,
                                      boolean includeCoveringDeletions)
        {
            super(cursor, (t, d) -> predicate.test(t) ? t : null, includeCoveringDeletions);
        }

        @Override
        protected Map.Entry<ByteComparable.Preencoded, DeletionAwareTrie<T, D>> mapContent(T value,
                                                                                           DeletionAwareTrie<T, D> tailTrie,
                                                                                           byte[] bytes,
                                                                                           int byteLength)
        {
            ByteComparable.Preencoded key = toByteComparable(byteComparableVersion(), bytes, byteLength);
            return new AbstractMap.SimpleImmutableEntry<>(key, tailTrie);
        }
    }
}
