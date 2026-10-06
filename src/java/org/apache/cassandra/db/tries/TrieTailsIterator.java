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
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Iterator of trie entries that constructs tail tries for the content-bearing branches that satisfy the given predicate
/// and skips over the returned branches.
public abstract class TrieTailsIterator<T, V, C extends Cursor<T>> extends TriePathReconstructor implements Iterator<V>
{
    final C cursor;
    private final Predicate<T> predicate;
    private T next;
    private boolean gotNext;

    TrieTailsIterator(C cursor, Predicate<T> predicate)
    {
        this.cursor = cursor;
        this.predicate = predicate;
        assert cursor.depth() == 0;
    }

    public boolean hasNext()
    {
        if (!gotNext)
        {
            int depth = cursor.depth();
            if (depth > 0)
            {
                // if we are not just starting, we have returned a branch and must skip over it
                depth = cursor.skipTo(depth, cursor.incomingTransition() + cursor.direction().increase);
                if (depth < 0)
                    return false;
                resetPathLength(depth - 1);
                addPathByte(cursor.incomingTransition());
            }

            next = cursor.content();
            if (next != null)
                gotNext = predicate.test(next);

            while (!gotNext)
            {
                next = cursor.advanceToContent(this);
                if (next != null)
                    gotNext = predicate.test(next);
                else
                    gotNext = true;
            }
        }

        return next != null;
    }

    public V next()
    {
        gotNext = false;
        T v = next;
        next = null;
        return getContent(v);
    }

    protected abstract V getContent(T v);

    ByteComparable.Version byteComparableVersion()
    {
        return cursor.byteComparableVersion();
    }

    public static abstract class Plain<T, V> extends TrieTailsIterator<T, V, Cursor<T>>
    {
        Plain(Cursor<T> cursor, Predicate<T> predicate)
        {
            super(cursor, predicate);
        }

        /// Public constructor accepting a Trie and creating a cursor from it
        public Plain(Trie<T> trie, Predicate<T> predicate)
        {
            this(trie.cursor(Direction.FORWARD), predicate);
        }

        /// Public constructor accepting a Trie, Direction, and creating a cursor from it
        public Plain(Trie<T> trie, Direction direction, Predicate<T> predicate)
        {
            this(trie.cursor(direction), predicate);
        }

        @Override
        protected V getContent(T v)
        {
            return mapContent(v, dir -> cursor.tailCursor(dir), keyBytes, keyPos);
        }

        protected abstract V mapContent(T value, Trie<T> tailTrie, byte[] bytes, int byteLength);
    }

    public static abstract class Range<S extends RangeState<S>, V> extends TrieTailsIterator<S, V, RangeCursor<S>>
    {
        Range(RangeCursor<S> cursor, Predicate<S> predicate)
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
            return mapContent(v, dir -> cursor.tailCursor(dir), keyBytes, keyPos);
        }

        protected abstract V mapContent(S value, RangeTrie<S> tailTrie, byte[] bytes, int byteLength);
    }

    public static abstract class DeletionAware<T, D extends RangeState<D>, V> extends TrieTailsIterator<T, V, DeletionAwareCursor<T, D>>
    {
        DeletionAware(DeletionAwareCursor<T, D> cursor, Predicate<T> predicate)
        {
            super(cursor, predicate);
        }

        /// Public constructor accepting a DeletionAwareTrie and creating a cursor from it
        public DeletionAware(DeletionAwareTrie<T, D> trie, Predicate<T> predicate)
        {
            this(trie.cursor(Direction.FORWARD), predicate);
        }

        /// Public constructor accepting a DeletionAwareTrie, Direction, and creating a cursor from it
        public DeletionAware(DeletionAwareTrie<T, D> trie, Direction direction, Predicate<T> predicate)
        {
            this(trie.cursor(direction), predicate);
        }

        @Override
        protected V getContent(T v)
        {
            return mapContent(v, dir -> cursor.tailCursor(dir), keyBytes, keyPos);
        }

        protected abstract V mapContent(T value, DeletionAwareTrie<T, D> tailTrie, byte[] bytes, int byteLength);
    }

    /// Iterator representing the selected content of the trie a sequence of `(path, tail)` pairs, where
    /// `tail` is the branch of the trie rooted at the selected content node (reachable by following
    /// `path`). The tail trie will have the selected content at its root.
    static class AsEntries<T> extends Plain<T, Map.Entry<ByteComparable.Preencoded, Trie<T>>>
    {
        public AsEntries(Cursor<T> cursor, Class<? extends T> clazz)
        {
            super(cursor, clazz::isInstance);
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
        public AsEntriesRange(RangeCursor<S> cursor, Class<? extends S> clazz)
        {
            super(cursor, clazz::isInstance);
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
    static class AsEntriesDeletionAware<T, D extends RangeState<D>>
    extends DeletionAware<T, D, Map.Entry<ByteComparable.Preencoded, DeletionAwareTrie<T, D>>>
    {
        public AsEntriesDeletionAware(DeletionAwareCursor<T, D> cursor, Class<? extends T> clazz)
        {
            super(cursor, clazz::isInstance);
        }

        @Override
        protected Map.Entry<ByteComparable.Preencoded, DeletionAwareTrie<T, D>> mapContent(T value, DeletionAwareTrie<T, D> tailTrie, byte[] bytes, int byteLength)
        {
            ByteComparable.Preencoded key = toByteComparable(byteComparableVersion(), bytes, byteLength);
            return new AbstractMap.SimpleImmutableEntry<>(key, tailTrie);
        }
    }
}
