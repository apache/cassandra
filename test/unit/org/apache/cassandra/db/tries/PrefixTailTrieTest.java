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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NavigableMap;

import com.google.common.base.Predicates;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;

public class PrefixTailTrieTest extends PrefixTailTestBase<InMemoryTrie<Object>, Trie<Object>>
{
    @Override
    InMemoryTrie<Object>[] makeArray(int length)
    {
        return new InMemoryTrie[length];
    }

    @Override
    InMemoryTrie<Object> makeInMemoryTrie()
    {
        return InMemoryTrie.shortLived(VERSION);
    }

    @Override
    void applyPrefixed(InMemoryTrie<Object> destination, ByteComparable prefix, InMemoryTrie<Object> tail, InMemoryBaseTrie.UpsertTransformer<Object, Object> upsertTransformer) throws TrieSpaceExhaustedException
    {
        destination.apply(tail.prefixedBy(prefix), upsertTransformer, Predicates.alwaysFalse());
    }

    @Override
    void apply(InMemoryTrie<Object> destination, Trie<Object> tail, InMemoryBaseTrie.UpsertTransformerWithKeyProducer<Object, Object> upsertTransformer) throws TrieSpaceExhaustedException
    {
        destination.apply(tail, upsertTransformer, Predicates.alwaysFalse());
    }

    @Override
    Trie<Object> merge(InMemoryTrie<Object>[] tries, Trie.CollectionMergeResolver<Object> resolver)
    {
        return Trie.merge(Arrays.asList(tries), resolver);
    }

    @Override
    Trie<Object> cast(InMemoryTrie<Object> inMemoryTrie)
    {
        return inMemoryTrie;
    }

    @Override
    void addToInMemoryTrie(Preencoded[] src, NavigableMap<Preencoded, ByteBuffer> content, InMemoryTrie<Object> tail)
    {
        InMemoryTrieTestBase.addToInMemoryTrie(src, content, tail, true);
    }

    @Override
    void addNthToInMemoryTrie(Preencoded[] src, NavigableMap<Preencoded, ByteBuffer> content, InMemoryTrie<Object> tail, int splits, int k)
    {
        InMemoryTrieTestBase.addNthToInMemoryTrie(src, content, tail, true, splits, k);
    }

    @Override
    Trie<ByteBuffer> processContent(Trie<Object> trie)
    {
        return TrieUtil.processContent(trie, x -> x instanceof ByteBuffer ? (ByteBuffer) x : null);
    }
}
