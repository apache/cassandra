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

import com.google.common.base.Predicates;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie entries to iterator where each entry is passed through {@link #mapContent} (to be implemented by
 * descendants).
 */
public abstract class TrieEntriesIterator<T, V> extends TriePathReconstructor implements Iterator<V>
{
    private final Trie.Cursor<T> cursor;
    private final Predicate<T> predicate;
    T next;
    boolean gotNext;

    protected TrieEntriesIterator(Trie<T> trie, Direction direction, Predicate<T> predicate)
    {
        this(trie.cursor(direction), predicate);
    }

    TrieEntriesIterator(Trie.Cursor<T> cursor, Predicate<T> predicate)
    {
        this.cursor = cursor;
        this.predicate = predicate;
        assert cursor.depth() == 0;
        next = cursor.content();
        gotNext = next != null && predicate.test(next);
    }

    public boolean hasNext()
    {
        while (!gotNext)
        {
            next = cursor.advanceToContent(this);
            if (next != null)
                gotNext = predicate.test(next);
            else
                gotNext = true;
        }

        return next != null;
    }

    public V next()
    {
        if (!hasNext())
            throw new IllegalStateException("next without hasNext");

        gotNext = false;
        T v = next;
        next = null;
        return mapContent(v, keyBytes, keyPos);
    }

    ByteComparable.Version byteComparableVersion()
    {
        return cursor.byteComparableVersion();
    }

    protected abstract V mapContent(T content, byte[] bytes, int byteLength);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntries<T> extends TrieEntriesIterator<T, Map.Entry<ByteComparable.Preencoded, T>>
    {
        public AsEntries(Trie.Cursor<T> cursor)
        {
            super(cursor, Predicates.alwaysTrue());
        }

        @Override
        protected Map.Entry<ByteComparable.Preencoded, T> mapContent(T content, byte[] bytes, int byteLength)
        {
            return toEntry(byteComparableVersion(), content, bytes, byteLength);
        }
    }

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntriesFilteredByType<T, U extends T> extends TrieEntriesIterator<T, Map.Entry<ByteComparable.Preencoded, U>>
    {
        public AsEntriesFilteredByType(Trie.Cursor<T> cursor, Class<U> clazz)
        {
            super(cursor, clazz::isInstance);
        }

        @Override
        @SuppressWarnings("unchecked")  // checked by the predicate
        protected Map.Entry<ByteComparable.Preencoded, U> mapContent(T content, byte[] bytes, int byteLength)
        {
            return toEntry(byteComparableVersion(), (U) content, bytes, byteLength);
        }
    }

    static <T> java.util.Map.Entry<ByteComparable.Preencoded, T> toEntry(ByteComparable.Version version, T content, byte[] bytes, int byteLength)
    {
        return new AbstractMap.SimpleImmutableEntry<>(toByteComparable(version, bytes, byteLength), content);
    }
}
