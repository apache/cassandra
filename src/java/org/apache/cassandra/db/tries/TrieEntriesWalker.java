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

import java.util.function.BiConsumer;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Walker of trie entries, used with Trie.process to walk all content in order and provide the path through which values
 * are reached.
 */
public abstract class TrieEntriesWalker<T, V> extends TriePathReconstructor implements Trie.Walker<T, V>
{
    @Override
    public void content(T content)
    {
        content(content, keyBytes, keyPos);
    }

    protected abstract void content(T content, byte[] bytes, int byteLength);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class WithConsumer<T> extends TrieEntriesWalker<T, Void>
    {
        private final BiConsumer<ByteComparable, T> consumer;

        public WithConsumer(BiConsumer<ByteComparable, T> consumer)
        {
            this.consumer = consumer;
        }

        @Override
        protected void content(T content, byte[] bytes, int byteLength)
        {
            consumer.accept(toByteComparable(bytes, byteLength), content);
        }

        @Override
        public Void complete()
        {
            return null;
        }
    }
}
