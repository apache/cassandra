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

/// Convertor of trie entries to iterator where each entry is passed through [#mapContent] (to be implemented by
/// descendants).
///
/// If [#mapContent] returns null, this version of the class will pass on that null upstream. If this is not the desired
/// behaviour, see [WithNullFiltering].
public abstract class TrieEntriesIterator<T, V> extends TriePathReconstructor implements Iterator<V>
{
    protected final Cursor<T> cursor;
    private final Predicate<T> predicate;
    T next;
    boolean gotNext;

    protected TrieEntriesIterator(Trie<T> trie, Direction direction, Predicate<T> predicate)
    {
        this(trie.cursor(direction), predicate);
    }

    TrieEntriesIterator(Cursor<T> cursor, Predicate<T> predicate)
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

    /// To be implemented by descendants to map the content value and path to the required entry. If callers need to
    /// save the path, they must copy the `bytes` array, which will be overwritten when the iteration continues.
    /// If this method returns null, the null will be passed on as an entry in the iteration.
    protected abstract V mapContent(T content, byte[] bytes, int byteLength);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntries<T> extends TrieEntriesIterator<T, Map.Entry<ByteComparable.Preencoded, T>>
    {
        public AsEntries(Cursor<T> cursor)
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
        public AsEntriesFilteredByType(Cursor<T> cursor, Class<U> clazz)
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

    /// Convertor of trie entries to iterator where each entry is passed through [#mapContent] (to be implemented by
    /// descendants). This is the same as [TrieEntriesIterator], but instead of accepting a predicate to filter out entries,
    /// it skips over ones where [#mapContent] returns null.
    public static abstract class WithNullFiltering<T, V> extends TriePathReconstructor implements Iterator<V>
    {
        protected final Cursor<T> cursor;
        V next;
        boolean gotNext;

        protected WithNullFiltering(Trie<T> trie, Direction direction)
        {
            this(trie.cursor(direction));
        }

        WithNullFiltering(Cursor<T> cursor)
        {
            this.cursor = cursor;
            assert cursor.depth() == 0;
            T nextContent = cursor.content();
            if (nextContent != null)
            {
                next = mapContent(nextContent, keyBytes, keyPos);
                gotNext = next != null;
            }
            else
                gotNext = false;
        }

        public boolean hasNext()
        {
            while (!gotNext)
            {
                T nextContent = cursor.advanceToContent(this);
                if (nextContent != null)
                {
                    next = mapContent(nextContent, keyBytes, keyPos);
                    gotNext = next != null;
                }
                else
                    gotNext = true;
            }

            return next != null;
        }

        public V next()
        {
            if (!hasNext())
                throw new IllegalStateException("next without hasNext");

            return consumeNext();
        }

        protected V consumeNext()
        {
            gotNext = false;
            V v = next;
            next = null;
            return v;
        }

        protected V peekNextIfAvailable()
        {
            return next;    // null if not prepared
        }

        /// To be implemented by descendants to map the content value and path to the required entry. If callers need to
        /// save the path, they must copy the `bytes` array, which will be overwritten when the iteration continues.
        /// If this method returns null, the iteration will skip over the current position.
        protected abstract V mapContent(T content, byte[] bytes, int byteLength);
    }
}
