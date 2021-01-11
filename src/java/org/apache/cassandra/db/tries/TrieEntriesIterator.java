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

import java.util.Map;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie entries to iterator where each entry is passed through {@link #mapContent} (to be implemented by
 * descendants).
 */
public abstract class TrieEntriesIterator<T, V> extends TrieIteratorWithKey<T, V>
{
    protected TrieEntriesIterator(Trie<T> trie)
    {
        super(trie);
    }

    V contentOf(Trie.Node<T, NodeWithPosition<T>> node)
    {
        T content = node.content();
        if (content == null)
            return null;
        return mapContent(content, path, ppos);
    }

    protected abstract V mapContent(T content, byte[] bytes, int byteLength);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntries<T>
    extends TrieEntriesIterator<T, Map.Entry<ByteComparable, T>>
    {
        public AsEntries(Trie<T> trie)
        {
            super(trie);
        }

        protected Map.Entry<ByteComparable, T> mapContent(T content, byte[] bytes, int byteLength)
        {
            return toEntry(content, bytes, byteLength);
        }
    }
}
