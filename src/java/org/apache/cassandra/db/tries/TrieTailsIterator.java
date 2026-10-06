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

/**
 * Iterator of trie entries that constructs tail tries for the content-bearing branches that satisfy the given predicate
 * and skips over the returned branches.
 */
public abstract class TrieTailsIterator<T, V> extends TriePathReconstructor implements Iterator<V>
{
    final Trie.Cursor<T> cursor;
    private final Predicate<T> predicate;
    private T next;
    private boolean gotNext;

    protected TrieTailsIterator(Trie<T> trie, Direction direction, Predicate<T> predicate)
    {
        this.cursor = trie.cursor(direction);
        this.predicate = predicate;
        assert cursor.depth() == 0;
    }

    TrieTailsIterator(Trie.Cursor<T> cursor, Predicate<T> predicate)
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
        return mapContent(v, cursor.tailTrie(), keyBytes, keyPos);
    }

    ByteComparable.Version byteComparableVersion()
    {
        return cursor.byteComparableVersion();
    }

    protected abstract V mapContent(T value, Trie<T> tailTrie, byte[] bytes, int byteLength);

    /**
     * Iterator representing the selected content of the trie a sequence of {@code (path, tail)} pairs, where
     * {@code tail} is the branch of the trie rooted at the selected content node (reachable by following
     * {@code path}). The tail trie will have the selected content at its root.
     */
    static class AsEntries<T> extends TrieTailsIterator<T, Map.Entry<ByteComparable.Preencoded, Trie<T>>>
    {
        public AsEntries(Trie.Cursor<T> cursor, Class<? extends T> clazz)
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
}
